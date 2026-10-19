//! Plan-carried lineage marker and its lowering into the terminal node.
//!
//! OpenLineage instrumentation has three concerns with different needs (see ADR
//! 0005): lineage *extraction* needs the optimized `LogicalPlan`; the START event
//! and orchestration context need `&dyn Session` and are async; the terminal
//! COMPLETE/FAIL node needs to sit at the physical root and observe execution.
//! The [`QueryPlanner`] receives the optimized logical plan and session, so the
//! planning-time work lives there. A registered [`ExtensionPlanner`] lowers a
//! plan-carried marker into [`OpenLineageExec`] at the physical root.
//!
//! Flow, all under one `run_id`. First, [`OpenLineageQueryPlanner`] (the
//! [`QueryPlanner`]) extracts lineage from the optimized logical plan, resolves
//! async [`LineageContextProvider`], emits START with inputs only, and wraps the
//! *logical* plan in a [`LineageMarker`] carrying the query lifecycle. Physical
//! planning lowers the marker via
//! [`LineageExtensionPlanner`] into an [`OpenLineageExec`] at the root, which
//! emits COMPLETE/FAIL at end of execution.
//!
//! A `LogicalPlan::Extension` requires a registered `ExtensionPlanner` (the
//! default physical planner errors on unknown extension nodes), so the planner
//! delegates physical planning to a [`DefaultPhysicalPlanner`] configured with
//! [`LineageExtensionPlanner`].

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{DFSchemaRef, Result};
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::{QueryPlanner, SessionContext};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{
    Expr, InvariantLevel, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};

use crate::client::OpenLineageClient;
use crate::config::OpenLineageConfig;
use crate::context::LineageContextProvider;
use crate::event::RunEvent;
use crate::exec::OpenLineageExec;
use crate::facet::FacetRegistry;
use crate::resolver::DatasetResolver;
use crate::{LineageHandle, begin_lineage};

tokio::task_local! {
    /// Set while [`OpenLineageQueryPlanner::execute_ddl_with_lineage`] runs the DDL,
    /// so the *nested* `create_physical_plan` that DataFusion triggers for the body
    /// (e.g. `create_memory_table` collecting the CTAS SELECT) does not emit a
    /// second, spurious run. Task-local rather than a shared flag so it is scoped to
    /// this one execution's task tree and never suppresses a concurrent query on
    /// another task. See [`suppressing_nested_lineage`].
    static SUPPRESS_NESTED_LINEAGE: ();
}

/// True when the current task is inside an `execute_ddl_with_lineage` call, i.e.
/// any `create_physical_plan` here is the nested body of a DDL run already being
/// reported and must not emit its own events.
pub(crate) fn nested_lineage_suppressed() -> bool {
    SUPPRESS_NESTED_LINEAGE.try_with(|()| ()).is_ok()
}

// ---------------------------------------------------------------------------
// The plan-carried marker.
// ---------------------------------------------------------------------------

/// A logical no-op wrapping the real plan, carrying the per-query lineage payload
/// from [`OpenLineageQueryPlanner`] (which has `&dyn Session`) to
/// [`LineageExtensionPlanner`] (which installs the terminal node). Schema-
/// transparent: it reports its input's schema so optimization and physical
/// planning treat it as a pass-through.
#[derive(Clone)]
pub struct LineageMarker {
    input: LogicalPlan,
    /// Initial event template. A lifecycle handle resolves its outputs on success.
    complete: RunEvent,
    client: OpenLineageClient,
    producer: String,
    pub(crate) lineage: Option<Arc<crate::lineage::LineageRun>>,
}

impl LineageMarker {
    /// Wrap `input`, carrying the COMPLETE template the terminal
    /// [`OpenLineageExec`] emits at end of execution. Usually built for you by
    /// [`LineageHandle::create_physical_plan`]; public for hosts that supply their
    /// own pre-built events instead of using the managed lineage lifecycle.
    pub fn new(
        input: LogicalPlan,
        complete: RunEvent,
        client: OpenLineageClient,
        producer: String,
    ) -> Self {
        Self {
            input,
            complete,
            client,
            producer,
            lineage: None,
        }
    }
}

impl fmt::Debug for LineageMarker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LineageMarker").finish_non_exhaustive()
    }
}

// Identity is the run id plus the wrapped plan: enough to distinguish markers,
// and the payload (client/template) is behavioral rather than structural.
impl PartialEq for LineageMarker {
    fn eq(&self, other: &Self) -> bool {
        self.complete.run.run_id == other.complete.run.run_id && self.input == other.input
    }
}
impl Eq for LineageMarker {}
impl PartialOrd for LineageMarker {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.complete
            .run
            .run_id
            .partial_cmp(&other.complete.run.run_id)
    }
}
impl Hash for LineageMarker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.complete.run.run_id.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for LineageMarker {
    fn name(&self) -> &str {
        "LineageMarker"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn check_invariants(&self, _check: InvariantLevel) -> Result<()> {
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LineageMarker")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            input: inputs.pop().expect("LineageMarker has one input"),
            complete: self.complete.clone(),
            client: self.client.clone(),
            producer: self.producer.clone(),
            lineage: self.lineage.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// Lowering: marker -> OpenLineageExec.
// ---------------------------------------------------------------------------

/// Lowers a [`LineageMarker`] into an [`OpenLineageExec`] at physical-planning
/// time. Register it on the session's physical planner (see
/// [`crate::session::instrument_session_state`]).
#[derive(Debug, Default)]
pub struct LineageExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for LineageExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Not our node: let another extension planner handle it.
        let Some(marker) = node.as_any().downcast_ref::<LineageMarker>() else {
            return Ok(None);
        };
        let inner = physical_inputs
            .first()
            .expect("LineageMarker has one physical input")
            .clone();
        Ok(Some(OpenLineageExec::new_with_lineage(
            inner,
            marker.client.clone(),
            marker.complete.clone(),
            marker.producer.clone(),
            marker.lineage.clone(),
        )))
    }
}

// ---------------------------------------------------------------------------
// The reusable planning-time lineage step.
// The query planner: extract + START + inject the marker.
// ---------------------------------------------------------------------------

/// A [`QueryPlanner`] that emits OpenLineage events around a query.
///
/// It does the `&dyn Session`-bound, async work — extract lineage, resolve
/// context, emit START, mint the `run_id`, emit FAIL on a planning error — then
/// hands off to physical planning by wrapping the logical plan in a
/// [`LineageMarker`]. The registered [`LineageExtensionPlanner`] lowers that
/// marker into the terminal [`OpenLineageExec`]. Built by
/// [`crate::session::instrument_session_state`].
pub struct OpenLineageQueryPlanner {
    client: OpenLineageClient,
    context: Arc<dyn LineageContextProvider>,
    config: OpenLineageConfig,
    dataset_resolvers: Vec<Arc<dyn DatasetResolver>>,
    facets: FacetRegistry,
    /// Physical planner that knows how to lower [`LineageMarker`]; composes any
    /// extension planners the host already had.
    physical: Arc<DefaultPhysicalPlanner>,
}

impl OpenLineageQueryPlanner {
    /// Build a planner whose physical planning lowers our marker plus
    /// `extra_extension_planners` (any the host session already registered).
    pub fn new(
        client: OpenLineageClient,
        context: Arc<dyn LineageContextProvider>,
        config: OpenLineageConfig,
        extra_extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
    ) -> Self {
        let mut planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> =
            vec![Arc::new(LineageExtensionPlanner)];
        planners.extend(extra_extension_planners);
        Self {
            client,
            context,
            config,
            dataset_resolvers: Vec::new(),
            facets: FacetRegistry::default(),
            physical: Arc::new(DefaultPhysicalPlanner::with_extension_planners(planners)),
        }
    }

    /// Append a resolver, using registration order and the first resolved name.
    /// An empty chain retains the default logical-table naming behavior.
    pub fn with_dataset_resolver(mut self, resolver: Arc<dyn DatasetResolver>) -> Self {
        self.dataset_resolvers.push(resolver);
        self
    }

    /// Use the same factory configuration as the high-level session builder.
    pub fn with_facet_registry(mut self, registry: FacetRegistry) -> Self {
        self.facets = registry;
        self
    }

    /// Planning-time lineage work shared by the `QueryPlanner` path and the
    /// `SessionContext`-level DDL path (see [`crate::session::OpenLineageSqlExt`]):
    /// see the free [`begin_lineage`] function this delegates to.
    async fn begin_lineage(
        &self,
        plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Option<LineageHandle> {
        begin_lineage(
            &self.client,
            self.context.as_ref(),
            &self.config,
            plan,
            session,
        )
        .with_resolvers(&self.dataset_resolvers)
        .with_facets(&self.facets)
        .start()
        .await
    }

    /// Execute a DDL-with-input statement (CTAS / CREATE VIEW) that DataFusion
    /// runs *outside* the `QueryPlanner` hook, emitting lineage around it.
    ///
    /// `SessionContext::execute_logical_plan` dispatches these DDL variants to its
    /// own `create_memory_table` / `create_view` before any `QueryPlanner` sees the
    /// wrapper (it only ever plans the stripped SELECT body), so the planner path
    /// captures the inputs but never the created table as an output. This runs
    /// [`crate::extract_with_resolvers`] on the *full* DDL plan (capturing the output
    /// dataset, schema, and column lineage), emits START, delegates creation to
    /// `execute_logical_plan` — reusing DataFusion's registration logic, including
    /// every `if_not_exists` / `or_replace` branch — then emits COMPLETE on success
    /// or FAIL on error, under the same `run_id`.
    ///
    /// `raw_sql` is folded into the lineage when the context provider didn't supply
    /// SQL text, since this path *does* have the original statement in hand.
    ///
    /// COMPLETE here does not carry an `outputStatistics.rowCount`: DataFusion
    /// materializes the CTAS body internally and hands back an empty result, so
    /// there is no stream for an `OpenLineageExec` to count. The output edge,
    /// schema, lifecycle, and column lineage are all present; runtime row stats for
    /// this path are a documented follow-up.
    pub(crate) async fn execute_ddl_with_lineage(
        &self,
        ctx: &SessionContext,
        plan: LogicalPlan,
        raw_sql: &str,
    ) -> Result<DataFrame> {
        let Some(mut handle) = self.begin_lineage(&plan, &ctx.state()).await else {
            // No datasets touched — nothing to report; just run it.
            return ctx.execute_logical_plan(plan).await;
        };
        // This path has the SQL in hand even when the context provider omitted it.
        handle.set_sql_if_absent(raw_sql);

        // Run the DDL with nested-lineage suppression set: `execute_logical_plan`
        // dispatches CTAS/CREATE VIEW to `create_memory_table`/`create_view`, which
        // collect the SELECT body back through *this* planner; without the guard
        // that body would emit its own (input-only) run alongside this DDL run.
        let result = SUPPRESS_NESTED_LINEAGE
            .scope((), ctx.execute_logical_plan(plan))
            .await;
        match result {
            Ok(df) => {
                // No `OpenLineageExec` on this path (DataFusion materializes the CTAS
                // body internally), so emit COMPLETE directly under the run id.
                handle.complete().await;
                Ok(df)
            }
            Err(err) => {
                handle.fail(&err.to_string());
                Err(err)
            }
        }
    }
}

impl fmt::Debug for OpenLineageQueryPlanner {
    // `DefaultPhysicalPlanner` is not `Debug`, so don't try to print it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenLineageQueryPlanner")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl QueryPlanner for OpenLineageQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Extract lineage, resolve context, and emit START — or, when the query
        // touches no datasets, plan straight through without a marker so no events
        // fire.
        let Some(handle) = self.begin_lineage(logical_plan, session).await else {
            return self
                .physical
                .create_physical_plan(logical_plan, session)
                .await;
        };

        handle
            .create_physical_plan(self.physical.as_ref(), session)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::builder::complete_event;
    use std::collections::hash_map::DefaultHasher;
    use uuid::Uuid;

    use datafusion::execution::SessionState;
    use datafusion::logical_expr::LogicalPlanBuilder;

    use super::*;
    use crate::QueryLineage;
    use crate::context::LineageContext;
    use crate::transport::NoopTransport;

    // `name`/`inputs`/`schema`/`expressions`/`with_exprs_and_inputs` exist on both
    // `UserDefinedLogicalNodeCore` and the object-safe `UserDefinedLogicalNode`
    // (blanket impl), so calls go through this alias to disambiguate.
    use datafusion::logical_expr::UserDefinedLogicalNodeCore as NodeCore;

    /// A `LineageMarker` over a trivial empty-relation plan, tagged with `run_id`.
    /// The marker fields are private, so these tests live inline.
    fn marker(run_id: Uuid) -> LineageMarker {
        let input = LogicalPlanBuilder::empty(false).build().unwrap();
        let config = OpenLineageConfig::default();
        let complete = complete_event(
            run_id,
            &QueryLineage::default(),
            &LineageContext::default(),
            &config,
        );
        LineageMarker {
            input,
            complete,
            client: OpenLineageClient::new(Arc::new(NoopTransport)),
            producer: config.producer,
            lineage: None,
        }
    }

    fn hash_of(m: &LineageMarker) -> u64 {
        let mut h = DefaultHasher::new();
        m.hash(&mut h);
        h.finish()
    }

    // `OpenLineageClient::new` spawns a background drain, so these need a runtime.
    #[tokio::test]
    async fn node_core_is_schema_transparent_and_expr_free() {
        let m = marker(Uuid::now_v7());
        assert_eq!(NodeCore::name(&m), "LineageMarker");
        assert_eq!(NodeCore::inputs(&m).len(), 1);
        // Schema-transparent: it reports its single input's schema verbatim.
        assert_eq!(NodeCore::schema(&m), NodeCore::inputs(&m)[0].schema());
        assert!(NodeCore::expressions(&m).is_empty());
        assert!(NodeCore::check_invariants(&m, InvariantLevel::Always).is_ok());
        assert_eq!(format!("{m:?}"), "LineageMarker { .. }");
    }

    #[tokio::test]
    async fn with_exprs_and_inputs_rebuilds_preserving_payload() {
        let run_id = Uuid::now_v7();
        let m = marker(run_id);
        let new_input = LogicalPlanBuilder::empty(true).build().unwrap();
        let rebuilt = NodeCore::with_exprs_and_inputs(&m, vec![], vec![new_input.clone()]).unwrap();
        // The wrapped input swaps; the run-id payload is carried through.
        assert_eq!(NodeCore::inputs(&rebuilt)[0], &new_input);
        assert_eq!(rebuilt.complete.run.run_id, run_id);
    }

    #[tokio::test]
    async fn identity_keys_on_run_id_and_input() {
        let run_id = Uuid::now_v7();
        // Same run id + same (empty) plan → equal, same hash, equal ordering.
        let a = marker(run_id);
        let b = marker(run_id);
        assert_eq!(a, b);
        assert_eq!(hash_of(&a), hash_of(&b));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Equal));

        // Different run id → not equal, ordering follows the run id.
        let c = marker(Uuid::now_v7());
        assert_ne!(a, c);
        assert_eq!(
            a.partial_cmp(&c),
            a.complete.run.run_id.partial_cmp(&c.complete.run.run_id)
        );
    }

    // --- the reusable `begin_lineage` step (for hosts sequencing it themselves) ---

    use std::sync::Mutex;

    use datafusion::execution::context::SessionContext;

    use crate::context::StaticContextProvider;
    use crate::event::{RunEvent, RunEventType};
    use crate::transport::{Transport, TransportError};

    /// Records emitted events for assertions.
    #[derive(Clone, Default, Debug)]
    struct Recording {
        events: std::sync::Arc<Mutex<Vec<RunEvent>>>,
    }
    #[async_trait]
    impl Transport for Recording {
        async fn emit(&self, event: &RunEvent) -> std::result::Result<(), TransportError> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    async fn table_select() -> (SessionState, LogicalPlan) {
        let ctx = SessionContext::new();
        ctx.sql("CREATE TABLE t AS VALUES (1)").await.unwrap();
        let plan = ctx
            .state()
            .create_logical_plan("SELECT * FROM t")
            .await
            .unwrap();
        (ctx.state(), plan)
    }

    #[tokio::test]
    async fn begin_lineage_plans_and_emits_lifecycle() {
        let (state, plan) = table_select().await;
        let rec = Recording::default();
        let client = OpenLineageClient::new(std::sync::Arc::new(rec.clone()));
        let config = OpenLineageConfig::default();
        let ctx = StaticContextProvider::default();

        let handle = begin_lineage(&client, &ctx, &config, &plan, &state)
            .start()
            .await
            .expect("query touches a dataset -> a run begins");

        let run_id = handle.run_id();
        let planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            LineageExtensionPlanner,
        )]);
        let physical = handle.create_physical_plan(&planner, &state).await.unwrap();
        assert!(physical.is::<OpenLineageExec>());
        datafusion::physical_plan::collect(physical, state.task_ctx())
            .await
            .unwrap();
        client.shutdown().await;

        let events = rec.events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, RunEventType::Start);
        assert_eq!(events[1].event_type, RunEventType::Complete);
        assert!(events.iter().all(|event| event.run.run_id == run_id));
    }

    #[tokio::test]
    async fn begin_lineage_returns_none_for_no_dataset_query() {
        // A metadata-only statement touches no datasets -> no run, no START.
        let ctx_df = SessionContext::new();
        let plan = ctx_df
            .state()
            .create_logical_plan("SET a = 1")
            .await
            .unwrap();
        let rec = Recording::default();
        let client = OpenLineageClient::new(std::sync::Arc::new(rec.clone()));
        let config = OpenLineageConfig::default();
        let ctx = StaticContextProvider::default();

        let handle = begin_lineage(&client, &ctx, &config, &plan, &ctx_df.state())
            .start()
            .await;
        assert!(handle.is_none(), "no datasets -> no run begins");

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            rec.events.lock().unwrap().is_empty(),
            "no events for a no-dataset query"
        );
    }
}
