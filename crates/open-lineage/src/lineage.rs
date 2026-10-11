//! Host-managed lineage with output identities resolved only after success.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use chrono::{DateTime, Utc};

use datafusion::common::Result;
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use uuid::Uuid;

use crate::builder::event_template;
use crate::extract::extract_with_names;
use crate::facet::{PreparedFacets, QueryContext};
use crate::resolver::DatasetNames;
use crate::{
    DatasetAccess, DatasetResolver, FacetRegistry, LineageContext, LineageContextProvider,
    OpenLineageClient, OpenLineageConfig, QueryLineage, RunEventType,
};

/// A query whose outputs are reported only on COMPLETE.
///
/// Input identities and symlinks are pinned before START. This handle owns the
/// original client, configuration, and plan for the entire run. Finish it once
/// with [`Self::complete`], [`Self::fail`], or transfer it to automatic execution
/// with [`Self::create_physical_plan`]. Dropping an unfinished handle emits FAIL.
#[must_use = "complete, fail, or create a physical plan for the run"]
pub struct LineageHandle {
    // Taken only when ownership passes to a completion future or plan marker.
    run: Option<Arc<LineageRun>>,
}

impl LineageHandle {
    fn run(&self) -> &Arc<LineageRun> {
        self.run.as_ref().expect("active lineage handle")
    }

    /// The id shared by START and the terminal event.
    pub fn run_id(&self) -> Uuid {
        self.run().run_id
    }

    /// Resolve outputs from the completed execution, then enrich and emit COMPLETE.
    ///
    /// Call after consuming results or committing the write. The completion time
    /// is captured when this method is called, before output resolver I/O. Input
    /// resolvers and facet factories are not called again. Cancelling the returned
    /// future (even before polling it) emits FAIL without outputs.
    /// Resolver I/O must be bounded and read-only, as required by [`DatasetResolver`].
    pub fn complete(mut self) -> impl Future<Output = ()> + Send {
        let completion = self.run.take().unwrap().begin_completion(Utc::now());
        async move {
            if let Some(completion) = completion {
                let (event, datasets) = completion.event().await;
                completion.emit(event, &datasets);
            }
        }
    }

    /// Emit FAIL without outputs and without invoking any output resolver.
    pub fn fail(self, error: &str) {
        self.run().fail(error);
    }

    /// Plan this run for automatic execution, guarding planning errors and cancellation.
    ///
    /// The supplied planner must lower [`crate::LineageMarker`] through
    /// [`crate::LineageExtensionPlanner`]. This method wraps the original logical
    /// plan and keeps the handle armed until planning succeeds. Planning errors
    /// emit FAIL with the original error, which is returned unchanged. Dropping
    /// the future emits FAIL even if a planner retains clones of the marker.
    ///
    /// On success, the returned plan owns terminal reporting. Its result stream
    /// awaits output resolution before completing. Plan clones share one terminal
    /// state; dropping all plans and streams before completion emits FAIL.
    pub async fn create_physical_plan(
        self,
        planner: &dyn PhysicalPlanner,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let marked_plan = self.marker();
        match planner
            .create_physical_plan(&marked_plan, session_state)
            .await
        {
            Ok(plan) => {
                self.disarm();
                Ok(plan)
            }
            Err(error) => {
                self.fail(&error.to_string());
                Err(error)
            }
        }
    }

    fn marker(&self) -> LogicalPlan {
        let run = self.run();
        let template = event_template(
            RunEventType::Complete,
            run.run_id,
            &run.inputs,
            &run.context,
            &run.config,
            None,
        );
        let mut marker = crate::LineageMarker::new(
            run.plan.clone(),
            template,
            run.client.clone(),
            run.config.producer.clone(),
        );
        marker.lineage = Some(run.clone());
        LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node: Arc::new(marker),
        })
    }

    fn disarm(mut self) {
        self.run.take();
    }

    /// Supply SQL text for subsequent events if the context omitted it.
    pub fn set_sql_if_absent(&mut self, sql: &str) {
        // A public handle cannot be cloned; SQL is set before transferring it.
        let run = Arc::get_mut(self.run.as_mut().unwrap()).expect("unshared lineage handle");
        if run.inputs.sql.is_none() {
            run.inputs.sql = Some(sql.to_owned());
        }
    }
}

impl Drop for LineageHandle {
    fn drop(&mut self) {
        if let Some(run) = &self.run {
            run.fail("query execution cancelled before completion");
        }
    }
}

const ACTIVE: u8 = 0;
const COMPLETING: u8 = 1;
const FINISHED: u8 = 2;

/// Immutable run payload and one terminal state, shared only inside the library.
/// Logical/physical plan clones must not fork the run's lifecycle.
pub(crate) struct LineageRun {
    pub(crate) has_outputs: bool,
    run_id: Uuid,
    plan: LogicalPlan,
    names: DatasetNames,
    inputs: QueryLineage,
    context: LineageContext,
    resolvers: Vec<Arc<dyn DatasetResolver>>,
    facets: Arc<PreparedFacets>,
    client: OpenLineageClient,
    config: OpenLineageConfig,
    terminal: AtomicU8,
}

impl LineageRun {
    pub(crate) fn begin_completion(
        self: &Arc<Self>,
        event_time: DateTime<Utc>,
    ) -> Option<Completion> {
        self.terminal
            .compare_exchange(ACTIVE, COMPLETING, Ordering::SeqCst, Ordering::SeqCst)
            .ok()?;
        Some(Completion {
            run: self.clone(),
            event_time,
        })
    }

    pub(crate) fn fail(&self, error: &str) {
        if self.terminal.swap(FINISHED, Ordering::SeqCst) == FINISHED {
            return;
        }
        self.facets.emit(
            &self.client,
            event_template(
                RunEventType::Fail,
                self.run_id,
                &self.inputs,
                &self.context,
                &self.config,
                Some(error.to_owned()),
            ),
        );
    }
}

impl Drop for LineageRun {
    fn drop(&mut self) {
        self.fail("query plan dropped before completion");
    }
}

/// Owns completion across asynchronous resolution, including an unpolled future.
pub(crate) struct Completion {
    run: Arc<LineageRun>,
    event_time: DateTime<Utc>,
}

impl Completion {
    pub(crate) async fn event(&self) -> (crate::RunEvent, Vec<crate::ResolvedDataset>) {
        let run = &self.run;
        let mut names = run.names.clone();
        names
            .resolve_access(&run.plan, &run.resolvers, Some(DatasetAccess::Write))
            .await;
        let mut lineage = extract_with_names(&run.plan, &names);
        lineage.sql = run.inputs.sql.clone();
        let mut event = event_template(
            RunEventType::Complete,
            run.run_id,
            &lineage,
            &run.context,
            &run.config,
            None,
        );
        event.event_time = self.event_time.to_rfc3339();
        (event, names.datasets())
    }

    pub(crate) fn emit(self, event: crate::RunEvent, datasets: &[crate::ResolvedDataset]) {
        if self.run.terminal.swap(FINISHED, Ordering::SeqCst) != FINISHED {
            self.run
                .facets
                .emit_with_datasets(&self.run.client, event, datasets);
        }
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        self.run.fail("query cancelled during output resolution");
    }
}

/// Configure a query's lineage lifecycle. Nothing is emitted until [`LineageBuilder::start`].
///
/// Resolvers are optional; without them, logical table references supply identities.
/// Facets default to the built-in registry. Outputs are always deferred until COMPLETE.
///
/// ```no_run
/// # use datafusion_openlineage::*;
/// # async fn example(client: &OpenLineageClient, context: &dyn LineageContextProvider,
/// # config: &OpenLineageConfig, plan: &datafusion::logical_expr::LogicalPlan,
/// # state: &datafusion::execution::SessionState, resolvers: &[std::sync::Arc<dyn DatasetResolver>], facets: &FacetRegistry) {
/// let handle = begin_lineage(client, context, config, plan, state)
///     .with_resolvers(resolvers)
///     .with_facets(facets)
///     .start()
///     .await;
/// # }
/// ```
pub fn begin_lineage<'a>(
    client: &'a OpenLineageClient,
    context: &'a dyn LineageContextProvider,
    config: &'a OpenLineageConfig,
    plan: &'a LogicalPlan,
    session_state: &'a SessionState,
) -> LineageBuilder<'a> {
    LineageBuilder {
        client,
        context,
        config,
        plan,
        session_state,
        resolvers: Vec::new(),
        facets: FacetRegistry::default(),
    }
}

/// Options for one query. Dropping this builder does not emit an event.
#[must_use = "call start().await to begin lineage"]
pub struct LineageBuilder<'a> {
    client: &'a OpenLineageClient,
    context: &'a dyn LineageContextProvider,
    config: &'a OpenLineageConfig,
    plan: &'a LogicalPlan,
    session_state: &'a SessionState,
    resolvers: Vec<Arc<dyn DatasetResolver>>,
    facets: FacetRegistry,
}

impl LineageBuilder<'_> {
    /// Set the ordered resolver chain, replacing any previously configured chain.
    pub fn with_resolvers(mut self, resolvers: &[Arc<dyn DatasetResolver>]) -> Self {
        self.resolvers = resolvers.to_vec();
        self
    }

    /// Set the facet registry, replacing the default built-in registry.
    pub fn with_facets(mut self, facets: &FacetRegistry) -> Self {
        self.facets = facets.clone();
        self
    }

    /// Resolve inputs, prepare facets once, and emit START without outputs.
    ///
    /// Returns `None` for dataset-free queries and nested instrumented DDL bodies.
    /// Output-only queries still emit START. Factories see input identities and
    /// the original plan; dataset builders receive final output origins on COMPLETE.
    /// A manually managed host must await completion after executing and consuming
    /// results, or call `fail` on error. Dropping an unfinished handle emits FAIL.
    /// Finish or drop all handles and plans before shutting down the client.
    pub async fn start(self) -> Option<LineageHandle> {
        let Self {
            client,
            context,
            config,
            plan,
            session_state,
            resolvers,
            facets,
        } = self;
        if crate::rule::nested_lineage_suppressed() {
            return None;
        }
        let mut names = DatasetNames::new(&config.job_namespace);
        names
            .resolve_access(plan, &resolvers, Some(DatasetAccess::Read))
            .await;
        let mut inputs = extract_with_names(plan, &names);
        if inputs.inputs.is_empty() && inputs.outputs.is_empty() {
            return None;
        }
        let has_outputs = !inputs.outputs.is_empty();
        inputs.outputs.clear();
        let context = context.context(session_state).await;
        inputs.sql = context.sql.clone();
        let run_id = context.run_id.unwrap_or_else(Uuid::now_v7);
        let facets = facets
            .prepare(&QueryContext {
                run_id,
                session_state,
                logical_plan: plan,
                lineage: &inputs,
                context: &context,
                datasets: &names.datasets(),
                config,
            })
            .await;
        facets.emit(
            client,
            event_template(RunEventType::Start, run_id, &inputs, &context, config, None),
        );
        Some(LineageHandle {
            run: Some(Arc::new(LineageRun {
                has_outputs,
                run_id,
                plan: plan.clone(),
                names,
                inputs,
                context,
                resolvers,
                facets,
                client: client.clone(),
                config: config.clone(),
                terminal: AtomicU8::new(ACTIVE),
            })),
        })
    }
}
