//! Late output identities, stable inputs, and facet contexts through the public API.
mod common;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, DataFusionError, Result};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;
use datafusion_openlineage::facet::{
    DatasetFacetContext, Facet, FacetBuilder, FacetBuilderFactory, FacetBuilders, FacetError,
    FacetSink, QueryContext, scope,
};
use datafusion_openlineage::facets::SymlinkIdentifier;
use datafusion_openlineage::{
    DatasetAccess, DatasetName, DatasetResolutionContext, DatasetResolver, FacetRegistry,
    LineageExtensionPlanner, OpenLineageClient, RunEventType, StaticContextProvider, begin_lineage,
};
use serde::Serialize;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Debug, Default)]
struct LateResolver {
    committed: AtomicBool,
    reads: AtomicUsize,
    writes: AtomicUsize,
}

#[async_trait]
impl DatasetResolver for LateResolver {
    async fn resolve(&self, cx: &DatasetResolutionContext<'_>) -> Option<DatasetName> {
        match cx.access {
            DatasetAccess::Read => {
                self.reads.fetch_add(1, Ordering::Relaxed);
                // Calling this again after the write would incorrectly change the input.
                Some(DatasetName {
                    namespace: "s3://input".into(),
                    name: if self.committed.load(Ordering::Relaxed) {
                        "wrong-new-input"
                    } else {
                        "pinned-input"
                    }
                    .into(),
                })
            }
            DatasetAccess::Write => {
                self.writes.fetch_add(1, Ordering::Relaxed);
                assert!(
                    self.committed.load(Ordering::Relaxed),
                    "output resolved before success"
                );
                Some(DatasetName {
                    namespace: "s3://output".into(),
                    name: "actual-new-location".into(),
                })
            }
        }
    }
    async fn symlinks(&self, cx: &DatasetResolutionContext<'_>) -> Vec<SymlinkIdentifier> {
        vec![SymlinkIdentifier {
            namespace: "catalog://test".into(),
            name: cx.table_ref.table().into(),
            type_: "TABLE".into(),
        }]
    }
}

#[derive(Debug, Default)]
struct Factory(AtomicUsize);
#[async_trait]
impl FacetBuilderFactory for Factory {
    fn name(&self) -> &'static str {
        "test.late_output"
    }
    async fn create(&self, cx: &QueryContext<'_>) -> Result<FacetBuilders, FacetError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        assert!(cx.lineage.outputs.is_empty());
        assert!(
            cx.datasets
                .iter()
                .all(|dataset| dataset.access == DatasetAccess::Read)
        );
        Ok(FacetBuilders::default().with(Builder))
    }
}
#[derive(Serialize)]
struct Evidence {
    name: String,
    origins: usize,
}
impl Facet for Evidence {
    type Scope = scope::OutputDataset;
    const NAME: &'static str = "test_evidence";
    const SCHEMA_URL: &'static str = "https://example.test/evidence.json";
}
#[derive(Debug)]
struct Builder;
impl FacetBuilder for Builder {
    type Scope = scope::OutputDataset;
    fn build(
        &self,
        cx: &DatasetFacetContext<'_>,
        sink: &mut FacetSink<Self::Scope>,
    ) -> Result<(), FacetError> {
        assert_eq!(cx.event.event_type, RunEventType::Complete);
        assert_eq!(cx.access, DatasetAccess::Write);
        assert_eq!(cx.origins.len(), 1);
        assert!(
            cx.origins[0]
                .table_provider()
                .unwrap()
                .downcast_ref::<MemTable>()
                .is_some()
        );
        sink.insert(Evidence {
            name: cx.dataset.name.clone(),
            origins: cx.origins.len(),
        })
    }
}

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    ctx.register_table(
        "dst",
        Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap()),
    )
    .unwrap();
    ctx
}

#[tokio::test]
async fn complete_resolves_only_outputs_and_preserves_input_column_lineage_and_facets() {
    let ctx = context();
    let plan = ctx
        .state()
        .create_logical_plan("INSERT INTO dst SELECT a FROM dst")
        .await
        .unwrap();
    let config = common::config("late-output", true);
    let transport = common::RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let resolver = Arc::new(LateResolver::default());
    let factory = Arc::new(Factory::default());
    let handle = begin_lineage(
        &client,
        &StaticContextProvider::default(),
        &config,
        &plan,
        &ctx.state(),
    )
    .with_resolvers(&[resolver.clone() as Arc<dyn DatasetResolver>])
    .with_facets(&FacetRegistry::default().with_factory(factory.clone()))
    .start()
    .await
    .unwrap();
    assert_eq!(resolver.reads.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.writes.load(Ordering::Relaxed), 0);
    resolver.committed.store(true, Ordering::Relaxed);
    let run_id = handle.run_id();
    handle.complete().await;
    client.shutdown().await;
    let events = transport.events();
    assert_eq!(events.len(), 2);
    assert!(events[0].outputs.is_empty());
    assert_eq!(events[0].run.run_id, run_id);
    assert_eq!(events[1].run.run_id, run_id);
    for event in &events {
        assert_eq!(event.inputs[0].name, "pinned-input");
        assert_eq!(
            event.inputs[0]
                .facets
                .symlinks
                .as_ref()
                .unwrap()
                .identifiers[0]
                .name,
            "dst"
        );
    }
    let output = &events[1].outputs[0];
    assert_eq!(output.namespace, "s3://output");
    assert_eq!(output.name, "actual-new-location");
    assert_eq!(
        output.facets.symlinks.as_ref().unwrap().identifiers[0].name,
        "dst"
    );
    let field = &output.facets.column_lineage.as_ref().unwrap().fields["a"].input_fields[0];
    assert_eq!(field.namespace, "s3://input");
    assert_eq!(field.name, "pinned-input");
    assert_eq!(
        output.output_facets.as_ref().unwrap().extra["test_evidence"]["name"],
        output.name
    );
    assert_eq!(factory.0.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.reads.load(Ordering::Relaxed), 1);
    assert_eq!(resolver.writes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn output_only_queries_emit_start_but_failure_never_resolves_or_reports_outputs() {
    for success in [false, true] {
        let ctx = context();
        let plan = ctx
            .state()
            .create_logical_plan("INSERT INTO dst VALUES (7)")
            .await
            .unwrap();
        let config = common::config("late-output", true);
        let transport = common::RecordingTransport::default();
        let client = OpenLineageClient::new(Arc::new(transport.clone()));
        let resolver = Arc::new(LateResolver::default());
        let handle = begin_lineage(
            &client,
            &StaticContextProvider::default(),
            &config,
            &plan,
            &ctx.state(),
        )
        .with_resolvers(&[resolver.clone() as Arc<dyn DatasetResolver>])
        .with_facets(&FacetRegistry::default())
        .start()
        .await
        .unwrap();
        if success {
            resolver.committed.store(true, Ordering::Relaxed);
            handle.complete().await;
        } else {
            handle.fail("write failed");
        }
        client.shutdown().await;
        let events = transport.events();
        assert_eq!(events.len(), 2);
        assert!(events[0].inputs.is_empty() && events[0].outputs.is_empty());
        assert_eq!(events[1].outputs.len(), usize::from(success));
        assert_eq!(
            events[1].event_type,
            if success {
                RunEventType::Complete
            } else {
                RunEventType::Fail
            }
        );
        assert_eq!(
            resolver.writes.load(Ordering::Relaxed),
            usize::from(success)
        );
    }
}

#[tokio::test]
async fn dataset_free_queries_remain_suppressed() {
    let ctx = context();
    let plan = ctx.state().create_logical_plan("SELECT 1").await.unwrap();
    let transport = common::RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    assert!(
        begin_lineage(
            &client,
            &StaticContextProvider::default(),
            &common::config("test", true),
            &plan,
            &ctx.state()
        )
        .with_facets(&FacetRegistry::default())
        .start()
        .await
        .is_none()
    );
    client.shutdown().await;
    assert!(transport.events().is_empty());
}

#[tokio::test]
async fn default_builder_has_no_side_effects_until_started_and_needs_no_options() {
    let ctx = context();
    let state = ctx.state();
    let plan = state
        .create_logical_plan("INSERT INTO dst VALUES (7)")
        .await
        .unwrap();
    let config = common::config("default", true);
    let transport = common::RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let provider = StaticContextProvider::default();
    drop(begin_lineage(&client, &provider, &config, &plan, &state));
    let handle = begin_lineage(&client, &provider, &config, &plan, &state)
        .start()
        .await
        .unwrap();
    handle.complete().await;
    client.shutdown().await;
    let events = transport.events();
    assert_eq!(
        events.len(),
        2,
        "constructing and dropping a builder must not emit START"
    );
    assert!(events[0].outputs.is_empty());
    assert_eq!(events[1].outputs[0].name, "dst");
    assert!(
        events
            .iter()
            .all(|event| event.run.facets.processing_engine.is_some())
    );
}

#[derive(Debug, Default)]
struct PausingResolver {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    writes: AtomicUsize,
}

#[async_trait]
impl DatasetResolver for PausingResolver {
    async fn resolve(&self, cx: &DatasetResolutionContext<'_>) -> Option<DatasetName> {
        assert_eq!(cx.access, DatasetAccess::Write);
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.entered.notify_one();
        self.release.notified().await;
        Some(DatasetName {
            namespace: "s3://committed".into(),
            name: "actual-output".into(),
        })
    }
}

#[tokio::test]
async fn automatic_execution_awaits_outputs_and_handles_cancellation_during_resolution() {
    use datafusion::physical_plan::collect;
    use datafusion_openlineage::{OpenLineage, OpenLineageSqlExt};
    for cancel in [false, true] {
        let transport = common::RecordingTransport::default();
        let client = OpenLineageClient::new(Arc::new(transport.clone()));
        let resolver = Arc::new(PausingResolver::default());
        let ctx = SessionContext::new().with_lineage(
            OpenLineage::builder()
                .client(client.clone())
                .dataset_resolver(resolver.clone()),
        );
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        ctx.register_table(
            "dst",
            Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap()),
        )
        .unwrap();
        let plan = ctx
            .sql("INSERT INTO dst VALUES (7)")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert_eq!(
            resolver.writes.load(Ordering::Relaxed),
            0,
            "planning must not resolve outputs"
        );
        let task_ctx = ctx.task_ctx();
        let task = tokio::spawn(async move { collect(plan, task_ctx).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            resolver.entered.notified(),
        )
        .await
        .unwrap();
        let resolution_started = chrono::Utc::now();
        assert!(!task.is_finished(), "collect must await output resolution");
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            resolver.release.notify_one();
            task.await.unwrap().unwrap();
        }
        drop(ctx);
        client.shutdown().await;
        let events = transport.events();
        assert_eq!(events.len(), 2);
        assert!(events[0].outputs.is_empty());
        assert_eq!(events[0].run.run_id, events[1].run.run_id);
        if cancel {
            assert_eq!(events[1].event_type, RunEventType::Fail);
            assert!(events[1].outputs.is_empty());
        } else {
            assert_eq!(events[1].event_type, RunEventType::Complete);
            let ended_at = chrono::DateTime::parse_from_rfc3339(&events[1].event_time).unwrap();
            assert!(ended_at <= resolution_started);
            assert_eq!(events[1].outputs[0].name, "actual-output");
            assert_eq!(
                events[1].outputs[0]
                    .output_facets
                    .as_ref()
                    .unwrap()
                    .output_statistics
                    .as_ref()
                    .unwrap()
                    .row_count,
                Some(1)
            );
        }
    }
}

#[tokio::test]
async fn ddl_resolves_only_after_creation_and_cancellation_emits_fail() {
    use datafusion_openlineage::{OpenLineage, OpenLineageSqlExt};
    for cancel in [false, true] {
        let transport = common::RecordingTransport::default();
        let client = OpenLineageClient::new(Arc::new(transport.clone()));
        let resolver = Arc::new(PausingResolver::default());
        let ctx = SessionContext::new().with_lineage(
            OpenLineage::builder()
                .client(client.clone())
                .dataset_resolver(resolver.clone()),
        );
        let query_ctx = ctx.clone();
        let task = tokio::spawn(async move {
            query_ctx
                .sql_with_lineage("CREATE TABLE created AS VALUES (7)")
                .await
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            resolver.entered.notified(),
        )
        .await
        .unwrap();
        let resolution_started = chrono::Utc::now();
        assert!(
            ctx.table_exist("created").unwrap(),
            "DDL output resolution must follow actual creation"
        );
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            resolver.release.notify_one();
            task.await.unwrap().unwrap();
        }
        drop(ctx);
        client.shutdown().await;
        let events = transport.events();
        assert_eq!(events.len(), 2, "nested DDL must not emit another run");
        assert!(events[0].outputs.is_empty());
        assert_eq!(events[0].run.run_id, events[1].run.run_id);
        assert_eq!(
            events[1].event_type,
            if cancel {
                RunEventType::Fail
            } else {
                RunEventType::Complete
            }
        );
        assert_eq!(events[1].outputs.len(), usize::from(!cancel));
        if !cancel {
            let ended_at = chrono::DateTime::parse_from_rfc3339(&events[1].event_time).unwrap();
            assert!(ended_at <= resolution_started);
        }
    }
}

#[tokio::test]
async fn manual_completion_keeps_original_identity_and_execution_end_time() {
    use chrono::{DateTime, Utc};

    let ctx = context();
    let state = ctx.state();
    let plan = state
        .create_logical_plan("INSERT INTO dst VALUES (7)")
        .await
        .unwrap();
    let mut config = common::config("original-job", true);
    let original_producer = config.producer.clone();
    let transport = common::RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let resolver = Arc::new(PausingResolver::default());
    let handle = begin_lineage(
        &client,
        &StaticContextProvider::default(),
        &config,
        &plan,
        &state,
    )
    .with_resolvers(&[resolver.clone() as Arc<dyn DatasetResolver>])
    .start()
    .await
    .unwrap();
    ctx.execute_logical_plan(plan)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let before_complete = Utc::now();
    let completion = handle.complete();
    let after_complete = Utc::now();
    // Neither changing the host's config nor delaying the first poll may change
    // the completed run's identity or execution-end timestamp.
    config.job_namespace = "other-job".into();
    config.producer = "https://example.test/other-producer".into();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let task = tokio::spawn(completion);
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        resolver.entered.notified(),
    )
    .await
    .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let resolver_released = Utc::now();
    resolver.release.notify_one();
    task.await.unwrap();
    client.shutdown().await;

    let events = transport.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].event_type, RunEventType::Complete);
    let ended_at = DateTime::parse_from_rfc3339(&events[1].event_time).unwrap();
    assert!(ended_at >= before_complete && ended_at <= after_complete);
    assert!(ended_at < resolver_released);
    for event in &events {
        assert_eq!(event.job.namespace, "original-job");
        assert_eq!(event.producer, original_producer);
        assert_ne!(event.job.namespace, config.job_namespace);
        assert_ne!(event.producer, config.producer);
        assert_eq!(
            event
                .run
                .facets
                .processing_engine
                .as_ref()
                .unwrap()
                .base
                .producer,
            original_producer,
        );
    }
    assert_eq!(events[0].run.run_id, events[1].run.run_id);
}

#[tokio::test]
async fn dropping_manual_run_or_completion_emits_one_failure() {
    for phase in ["execution", "unpolled completion", "output resolution"] {
        let ctx = context();
        let state = ctx.state();
        let plan = state
            .create_logical_plan("INSERT INTO dst VALUES (7)")
            .await
            .unwrap();
        let transport = common::RecordingTransport::default();
        let client = OpenLineageClient::new(Arc::new(transport.clone()));
        let resolver = Arc::new(PausingResolver::default());
        let handle = begin_lineage(
            &client,
            &StaticContextProvider::default(),
            &common::config("manual-cancel", true),
            &plan,
            &state,
        )
        .with_resolvers(&[resolver.clone() as Arc<dyn DatasetResolver>])
        .start()
        .await
        .unwrap();
        match phase {
            "execution" => drop(handle),
            "unpolled completion" => drop(handle.complete()),
            _ => {
                let task = tokio::spawn(handle.complete());
                tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    resolver.entered.notified(),
                )
                .await
                .unwrap();
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            }
        }
        client.shutdown().await;
        let events = transport.events();
        assert_eq!(events.len(), 2, "{phase}");
        assert_eq!(events[0].event_type, RunEventType::Start);
        assert_eq!(events[1].event_type, RunEventType::Fail);
        assert_eq!(events[0].run.run_id, events[1].run.run_id);
        assert!(events.iter().all(|event| event.outputs.is_empty()));
        assert_eq!(
            resolver.writes.load(Ordering::Relaxed),
            usize::from(phase == "output resolution"),
        );
    }
}

#[tokio::test]
async fn marker_clones_share_one_terminal_outcome() {
    use datafusion::physical_plan::collect;

    for fail_first in [false, true] {
        let ctx = context();
        let state = ctx.state();
        let plan = state
            .create_logical_plan("SELECT * FROM dst")
            .await
            .unwrap();
        let transport = common::RecordingTransport::default();
        let client = OpenLineageClient::new(Arc::new(transport.clone()));
        let handle = begin_lineage(
            &client,
            &StaticContextProvider::default(),
            &common::config("cloned-marker", true),
            &plan,
            &state,
        )
        .start()
        .await
        .unwrap();
        let planner = RetainingPlanner::new(PlanningOutcome::Success);
        let succeeds = handle.create_physical_plan(&planner, &state).await.unwrap();
        let marker_clone = planner.retained_plan();
        let cancelled = planner
            .inner
            .create_physical_plan(&marker_clone, &state)
            .await
            .unwrap();
        if fail_first {
            drop(cancelled.execute(0, state.task_ctx()).unwrap());
            collect(succeeds, state.task_ctx()).await.unwrap();
        } else {
            collect(succeeds, state.task_ctx()).await.unwrap();
            drop(cancelled.execute(0, state.task_ctx()).unwrap());
        }
        drop(cancelled);
        drop(marker_clone);
        drop(planner);
        client.shutdown().await;
        let events = transport.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, RunEventType::Start);
        assert_eq!(
            events[1].event_type,
            if fail_first {
                RunEventType::Fail
            } else {
                RunEventType::Complete
            },
        );
        assert_eq!(events[0].run.run_id, events[1].run.run_id);
    }
}

#[tokio::test]
async fn abandoning_planned_run_emits_failure_only_after_last_owner_drops() {
    let ctx = context();
    let state = ctx.state();
    let plan = state
        .create_logical_plan("SELECT * FROM dst")
        .await
        .unwrap();
    let transport = common::RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let planner = RetainingPlanner::new(PlanningOutcome::Success);
    let physical = begin_lineage(
        &client,
        &StaticContextProvider::default(),
        &common::config("abandoned-marker", true),
        &plan,
        &state,
    )
    .start()
    .await
    .unwrap()
    .create_physical_plan(&planner, &state)
    .await
    .unwrap();
    let marker_clone = planner.retained_plan();
    drop(physical);
    drop(planner);
    // Allow the client worker to deliver anything already queued.
    tokio::task::yield_now().await;
    assert!(
        transport
            .events()
            .iter()
            .all(|event| event.event_type == RunEventType::Start)
    );
    drop(marker_clone);
    client.shutdown().await;
    let events = transport.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].event_type, RunEventType::Fail);
}

enum PlanningOutcome {
    Success,
    Error,
    Pending,
}

/// Model a custom planner that keeps a logical-plan clone after its call returns
/// or is cancelled. That clone must not prevent an immediate planning FAIL.
struct RetainingPlanner {
    inner: DefaultPhysicalPlanner,
    retained: Mutex<Option<LogicalPlan>>,
    entered: tokio::sync::Notify,
    outcome: PlanningOutcome,
}

impl RetainingPlanner {
    fn new(outcome: PlanningOutcome) -> Self {
        Self {
            inner: DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                LineageExtensionPlanner,
            )]),
            retained: Mutex::new(None),
            entered: tokio::sync::Notify::new(),
            outcome,
        }
    }

    fn retained_plan(&self) -> LogicalPlan {
        self.retained.lock().unwrap().as_ref().unwrap().clone()
    }
}

#[async_trait]
impl PhysicalPlanner for RetainingPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(
            matches!(logical_plan, LogicalPlan::Extension(e) if e.node.name() == "LineageMarker")
        );
        *self.retained.lock().unwrap() = Some(logical_plan.clone());
        self.entered.notify_one();
        match self.outcome {
            PlanningOutcome::Success => {
                self.inner
                    .create_physical_plan(logical_plan, session_state)
                    .await
            }
            PlanningOutcome::Error => Err(DataFusionError::Plan("custom planning failed".into())),
            PlanningOutcome::Pending => std::future::pending().await,
        }
    }

    fn create_physical_expr(
        &self,
        expr: &Expr,
        input_dfschema: &DFSchema,
        session_state: &dyn Session,
        planning_ctx: &PhysicalPlanningContext,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.inner
            .create_physical_expr(expr, input_dfschema, session_state, planning_ctx)
    }
}

// Client shutdown waits for all sender clones. Observe delivery while the
// planner still retains a marker, rather than dropping the clone to flush it.
async fn wait_for_terminal(transport: &common::RecordingTransport) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while transport.events().len() < 2 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("terminal event must not wait for retained marker clones to drop");
}

#[tokio::test]
async fn planning_failure_reports_original_error_despite_retained_marker() {
    let ctx = context();
    let state = ctx.state();
    let plan = state
        .create_logical_plan("INSERT INTO dst VALUES (7)")
        .await
        .unwrap();
    let transport = common::RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let resolver = Arc::new(PausingResolver::default());
    let handle = begin_lineage(
        &client,
        &StaticContextProvider::default(),
        &common::config("planning-error", true),
        &plan,
        &state,
    )
    .with_resolvers(&[resolver.clone() as Arc<dyn DatasetResolver>])
    .start()
    .await
    .unwrap();
    let run_id = handle.run_id();
    let planner = RetainingPlanner::new(PlanningOutcome::Error);
    let error = handle
        .create_physical_plan(&planner, &state)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, DataFusionError::Plan(message) if message == "custom planning failed")
    );
    assert!(planner.retained.lock().unwrap().is_some());
    wait_for_terminal(&transport).await;
    let events = transport.events();
    assert_eq!(events[0].event_type, RunEventType::Start);
    assert_eq!(events[1].event_type, RunEventType::Fail);
    assert_eq!(
        events[1].run.facets.error_message.as_ref().unwrap().message,
        error.to_string()
    );
    assert!(
        events
            .iter()
            .all(|event| event.run.run_id == run_id && event.outputs.is_empty())
    );
    assert_eq!(resolver.writes.load(Ordering::Relaxed), 0);
    drop(planner);
    client.shutdown().await;
    assert_eq!(
        transport.events().len(),
        2,
        "late clone cleanup must not emit again"
    );
}

#[tokio::test]
async fn cancelling_planning_reports_failure_even_with_retained_marker() {
    for poll in [false, true] {
        let ctx = context();
        let state = ctx.state();
        let plan = state
            .create_logical_plan("INSERT INTO dst VALUES (7)")
            .await
            .unwrap();
        let transport = common::RecordingTransport::default();
        let client = OpenLineageClient::new(Arc::new(transport.clone()));
        let resolver = Arc::new(PausingResolver::default());
        let handle = begin_lineage(
            &client,
            &StaticContextProvider::default(),
            &common::config("planning-cancelled", true),
            &plan,
            &state,
        )
        .with_resolvers(&[resolver.clone() as Arc<dyn DatasetResolver>])
        .start()
        .await
        .unwrap();
        let run_id = handle.run_id();
        let planner = RetainingPlanner::new(PlanningOutcome::Pending);
        let mut planning = Box::pin(handle.create_physical_plan(&planner, &state));
        if poll {
            tokio::select! {
                _ = &mut planning => panic!("planner must remain pending"),
                _ = planner.entered.notified() => {},
            }
        }
        drop(planning);
        assert_eq!(planner.retained.lock().unwrap().is_some(), poll);
        wait_for_terminal(&transport).await;
        let events = transport.events();
        assert_eq!(events[0].event_type, RunEventType::Start);
        assert_eq!(events[1].event_type, RunEventType::Fail);
        assert!(
            events[1]
                .run
                .facets
                .error_message
                .as_ref()
                .unwrap()
                .message
                .contains("cancelled")
        );
        assert!(
            events
                .iter()
                .all(|event| event.run.run_id == run_id && event.outputs.is_empty())
        );
        assert_eq!(resolver.writes.load(Ordering::Relaxed), 0);
        drop(planner);
        client.shutdown().await;
        assert_eq!(
            transport.events().len(),
            2,
            "late clone cleanup must not emit again"
        );
    }
}
