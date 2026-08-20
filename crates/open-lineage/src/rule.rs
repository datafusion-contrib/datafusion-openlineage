//! Plan-carried lineage marker and its lowering into the terminal node.
//!
//! OpenLineage instrumentation has three concerns with different needs (see ADR
//! 0005): lineage *extraction* needs the optimized `LogicalPlan`; the START event
//! and orchestration context need `&SessionState` and are async; the terminal
//! COMPLETE/FAIL node needs to sit at the physical root and observe execution.
//! Only the [`QueryPlanner`] seam has `&SessionState`, so the planning-time work
//! lives there — but the terminal node is installed the composable, DataFusion-
//! idiomatic way: a registered [`ExtensionPlanner`] lowers a plan-carried marker
//! into [`OpenLineageExec`], rather than the planner hand-wrapping the physical
//! root.
//!
//! Flow, all under one `run_id`. First, [`OpenLineageQueryPlanner`] (the
//! [`QueryPlanner`]) extracts lineage from the optimized logical plan, resolves
//! the async [`LineageContextProvider`], emits START, builds the COMPLETE
//! template, and wraps the *logical* plan in a [`LineageMarker`] carrying that
//! template. Then physical planning lowers the marker via
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
use datafusion::common::{DFSchemaRef, Result};
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::{QueryPlanner, SessionContext, SessionState};
use datafusion::logical_expr::{
    Expr, Extension, InvariantLevel, LogicalPlan, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use uuid::Uuid;

use crate::builder::{complete_event, fail_event, start_event};
use crate::client::OpenLineageClient;
use crate::config::OpenLineageConfig;
use crate::context::{LineageContext, LineageContextProvider};
use crate::event::RunEvent;
use crate::exec::OpenLineageExec;
use crate::extract::{QueryLineage, extract};

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
fn nested_lineage_suppressed() -> bool {
    SUPPRESS_NESTED_LINEAGE.try_with(|()| ()).is_ok()
}

// ---------------------------------------------------------------------------
// The plan-carried marker.
// ---------------------------------------------------------------------------

/// A logical no-op wrapping the real plan, carrying the per-query lineage payload
/// from [`OpenLineageQueryPlanner`] (which has `&SessionState`) to
/// [`LineageExtensionPlanner`] (which installs the terminal node). Schema-
/// transparent: it reports its input's schema so optimization and physical
/// planning treat it as a pass-through.
#[derive(Clone)]
pub struct LineageMarker {
    input: LogicalPlan,
    /// COMPLETE event template, built at plan time; cloned into the terminal
    /// [`OpenLineageExec`] at lowering and mutated into FAIL there on error.
    complete: RunEvent,
    client: OpenLineageClient,
    producer: String,
}

impl LineageMarker {
    /// Wrap `input`, carrying the COMPLETE template the terminal
    /// [`OpenLineageExec`] emits at end of execution. Usually built for you by
    /// [`LineageHandle::into_marker`]; public so a host can construct the marker
    /// when composing lineage into its own planner.
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
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Not our node: let another extension planner handle it.
        let Some(marker) = node.as_any().downcast_ref::<LineageMarker>() else {
            return Ok(None);
        };
        let inner = physical_inputs
            .first()
            .expect("LineageMarker has one physical input")
            .clone();
        Ok(Some(OpenLineageExec::new(
            inner,
            marker.client.clone(),
            marker.complete.clone(),
            marker.producer.clone(),
        )))
    }
}

// ---------------------------------------------------------------------------
// The reusable planning-time lineage step.
// ---------------------------------------------------------------------------

/// The per-query lineage payload produced by [`begin_lineage`], carried under one
/// `run_id` from the planning-time step to the terminal event.
///
/// A host composing lineage with another concern (e.g. running lineage's START
/// step *after* a policy gate, from its own single [`QueryPlanner`]) holds this
/// between [`begin_lineage`] and [`Self::into_marker`] / the `emit_*` methods.
pub struct LineageHandle {
    run_id: Uuid,
    lineage: QueryLineage,
    context: LineageContext,
}

impl LineageHandle {
    /// The run id START was emitted under; COMPLETE/FAIL must share it.
    pub fn run_id(&self) -> Uuid {
        self.run_id
    }

    /// Wrap `plan` in a [`LineageMarker`] carrying the COMPLETE template, so a
    /// registered [`LineageExtensionPlanner`] lowers it into the terminal
    /// [`OpenLineageExec`] at the physical root (the composable path).
    ///
    /// Borrows so the handle stays available for [`Self::emit_fail`] if the
    /// subsequent physical planning errors.
    pub fn to_marker(
        &self,
        plan: LogicalPlan,
        client: OpenLineageClient,
        config: &OpenLineageConfig,
    ) -> LogicalPlan {
        let complete = complete_event(self.run_id, &self.lineage, &self.context, config);
        LogicalPlan::Extension(Extension {
            node: Arc::new(LineageMarker::new(
                plan,
                complete,
                client,
                config.producer.clone(),
            )),
        })
    }

    /// Emit FAIL for a planning error that occurs *after* START, before any
    /// [`OpenLineageExec`] exists to observe execution. Under the same `run_id`.
    pub fn emit_fail(&self, client: &OpenLineageClient, config: &OpenLineageConfig, err: &str) {
        client.emit(fail_event(
            self.run_id,
            &self.lineage,
            &self.context,
            config,
            err,
        ));
    }

    /// Emit COMPLETE directly, under the same `run_id`, with `eventTime` refreshed
    /// to now (so run duration is meaningful). For paths that run the query
    /// *outside* a physical plan — there is no [`OpenLineageExec`] to emit the
    /// terminal event, so the caller emits it (e.g. the CTAS / `CREATE VIEW` DDL
    /// path, which materializes internally and hands back an empty result).
    pub fn emit_complete(&self, client: &OpenLineageClient, config: &OpenLineageConfig) {
        let mut event = complete_event(self.run_id, &self.lineage, &self.context, config);
        event.event_time = chrono::Utc::now().to_rfc3339();
        client.emit(event);
    }

    /// Fold SQL text into the lineage when the context provider didn't supply it
    /// (a path that has the raw statement in hand — e.g. the DDL path).
    pub fn set_sql_if_absent(&mut self, sql: &str) {
        if self.lineage.sql.is_none() {
            self.lineage.sql = Some(sql.to_string());
        }
    }
}

/// Planning-time lineage work, decoupled from the [`QueryPlanner`] trait: extract
/// lineage from `plan`, resolve the async [`LineageContextProvider`], and — unless
/// the query touches no datasets — mint a `run_id` and emit START.
///
/// Returns a [`LineageHandle`] to carry the run under one id to the terminal event
/// (via [`LineageHandle::into_marker`]), or `None` when lineage is suppressed (no
/// inputs and no outputs — `information_schema` introspection, `SET`/`SHOW`,
/// metadata probes; or a nested DDL body), in which case no START fired and the
/// caller must emit nothing.
///
/// This is the reusable step for hosts that sequence lineage into their own
/// single `QueryPlanner` (e.g. after a policy gate) rather than installing
/// [`OpenLineageQueryPlanner`] as the session planner.
pub async fn begin_lineage(
    client: &OpenLineageClient,
    context: &dyn LineageContextProvider,
    config: &OpenLineageConfig,
    plan: &LogicalPlan,
    session_state: &SessionState,
) -> Option<LineageHandle> {
    // A `create_physical_plan` nested inside `execute_ddl_with_lineage` is the
    // DDL body (e.g. the CTAS SELECT that `create_memory_table` collects); the
    // enclosing DDL run already reports it, so emit nothing here.
    if nested_lineage_suppressed() {
        return None;
    }

    let mut lineage = extract(plan, config);
    let cx = context.context(session_state).await;
    // The SQL text isn't recoverable from the plan; take it from the
    // host-supplied context (absent on non-SQL paths, e.g. ingest).
    lineage.sql = cx.sql.clone();

    // Suppress lineage for queries that touch no datasets — information_schema
    // introspection, `SET`/`SHOW`, metadata-RPC probes. They carry no input or
    // output, so a START/COMPLETE pair only adds a dangling job node to the graph.
    if lineage.inputs.is_empty() && lineage.outputs.is_empty() {
        return None;
    }

    let run_id = cx.run_id.unwrap_or_else(Uuid::now_v7);
    client.emit(start_event(run_id, &lineage, &cx, config));
    Some(LineageHandle {
        run_id,
        lineage,
        context: cx,
    })
}

// ---------------------------------------------------------------------------
// The query planner: extract + START + inject the marker.
// ---------------------------------------------------------------------------

/// A [`QueryPlanner`] that emits OpenLineage events around a query.
///
/// It does the `&SessionState`-bound, async work — extract lineage, resolve
/// context, emit START, mint the `run_id`, emit FAIL on a planning error — then
/// hands off to physical planning by wrapping the logical plan in a
/// [`LineageMarker`]. The registered [`LineageExtensionPlanner`] lowers that
/// marker into the terminal [`OpenLineageExec`]. Built by
/// [`crate::session::instrument_session_state`].
pub struct OpenLineageQueryPlanner {
    client: OpenLineageClient,
    context: Arc<dyn LineageContextProvider>,
    config: OpenLineageConfig,
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
            physical: Arc::new(DefaultPhysicalPlanner::with_extension_planners(planners)),
        }
    }

    /// Planning-time lineage work shared by the `QueryPlanner` path and the
    /// `SessionContext`-level DDL path (see [`crate::session::OpenLineageSqlExt`]):
    /// see the free [`begin_lineage`] function this delegates to.
    async fn begin_lineage(
        &self,
        plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Option<LineageHandle> {
        begin_lineage(
            &self.client,
            self.context.as_ref(),
            &self.config,
            plan,
            session_state,
        )
        .await
    }

    /// Execute a DDL-with-input statement (CTAS / CREATE VIEW) that DataFusion
    /// runs *outside* the `QueryPlanner` hook, emitting lineage around it.
    ///
    /// `SessionContext::execute_logical_plan` dispatches these DDL variants to its
    /// own `create_memory_table` / `create_view` before any `QueryPlanner` sees the
    /// wrapper (it only ever plans the stripped SELECT body), so the planner path
    /// captures the inputs but never the created table as an output. This runs
    /// [`extract`] on the *full* DDL plan (so the output dataset, its schema, and
    /// column lineage are captured), emits START, delegates the actual creation to
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
                handle.emit_complete(&self.client, &self.config);
                Ok(df)
            }
            Err(err) => {
                handle.emit_fail(&self.client, &self.config, &err.to_string());
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
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Extract lineage, resolve context, and emit START — or, when the query
        // touches no datasets, plan straight through without a marker so no events
        // fire.
        let Some(handle) = self.begin_lineage(logical_plan, session_state).await else {
            return self
                .physical
                .create_physical_plan(logical_plan, session_state)
                .await;
        };

        // Carry the COMPLETE template into the physical phase via the plan itself;
        // the extension planner lowers it into an OpenLineageExec at the root that
        // emits COMPLETE/FAIL at end of execution, under this same run id.
        let wrapped = handle.to_marker(logical_plan.clone(), self.client.clone(), &self.config);

        match self
            .physical
            .create_physical_plan(&wrapped, session_state)
            .await
        {
            Ok(plan) => Ok(plan),
            Err(err) => {
                // Planning failed outright — no execution to observe, emit FAIL now.
                handle.emit_fail(&self.client, &self.config, &err.to_string());
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;

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
    async fn begin_lineage_emits_start_and_yields_a_marker() {
        let (state, plan) = table_select().await;
        let rec = Recording::default();
        let client = OpenLineageClient::new(std::sync::Arc::new(rec.clone()));
        let config = OpenLineageConfig::default();
        let ctx = StaticContextProvider::default();

        let handle = begin_lineage(&client, &ctx, &config, &plan, &state)
            .await
            .expect("query touches a dataset -> a run begins");

        // START was emitted under the handle's run id.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let events = rec.events.lock().unwrap();
        let start = events
            .iter()
            .find(|e| e.event_type == RunEventType::Start)
            .expect("START emitted");
        assert_eq!(start.run.run_id, handle.run_id());

        // The handle wraps a plan in a LineageMarker for the extension planner.
        let wrapped = handle.to_marker(plan.clone(), client.clone(), &config);
        assert!(matches!(&wrapped, LogicalPlan::Extension(e) if e.node.name() == "LineageMarker"));
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

        let handle = begin_lineage(&client, &ctx, &config, &plan, &ctx_df.state()).await;
        assert!(handle.is_none(), "no datasets -> no run begins");

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            rec.events.lock().unwrap().is_empty(),
            "no events for a no-dataset query"
        );
    }
}
