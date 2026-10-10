//! Public API and lifecycle contracts for built-in and engine facet builders.

mod common;

use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::RecordingTransport;
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion_openlineage::facet::{
    DatasetFacetContext, Facet, FacetBuilder, FacetBuilderFactory, FacetBuilders, FacetError,
    FacetRegistry, FacetScope, FacetSink, JobFacetContext, PROCESSING_ENGINE_FACTORY, QueryContext,
    RunFacetContext, scope,
};
use datafusion_openlineage::{
    DatasetAccess, DatasetName, DatasetResolutionContext, DatasetResolver, OpenLineage,
    OpenLineageClient, OpenLineageConfig, OpenLineageSqlExt, RunEventType, StaticContextProvider,
    begin_lineage_with_facets,
};
use serde::Serialize;
use uuid::Uuid;

fn config() -> OpenLineageConfig {
    common::config("facet-tests", true)
}

fn table() -> Arc<MemTable> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
}

fn register(ctx: &SessionContext) {
    ctx.register_table("src", table()).unwrap();
    ctx.register_table("dst", table()).unwrap();
}

#[derive(Debug, Default)]
struct Calls {
    factories: AtomicUsize,
    dataset_checks: AtomicUsize,
    builds: Mutex<Vec<(Uuid, RunEventType, &'static str, String)>>,
    origins: Mutex<Vec<(String, DatasetAccess, usize, usize)>>,
}

#[derive(Serialize)]
struct Probe<S> {
    event_type: RunEventType,
    target: String,
    rows: Option<i64>,
    #[serde(skip)]
    scope: PhantomData<S>,
}

impl<S: FacetScope> Facet for Probe<S> {
    type Scope = S;
    const NAME: &'static str = "test_observed";
    const SCHEMA_URL: &'static str = "https://example.com/facets/v1/probe.json";
}

fn probe<S>(event_type: RunEventType, target: &str, rows: Option<i64>) -> Probe<S> {
    Probe {
        event_type,
        target: target.into(),
        rows,
        scope: PhantomData,
    }
}

#[derive(Debug)]
struct Probes(Arc<Calls>);

#[async_trait]
impl FacetBuilderFactory for Probes {
    fn name(&self) -> &'static str {
        "test.probes"
    }

    async fn create(&self, cx: &QueryContext<'_>) -> Result<FacetBuilders, FacetError> {
        self.0.factories.fetch_add(1, Ordering::Relaxed);
        tokio::task::yield_now().await;
        for dataset in cx.datasets {
            let providers = dataset
                .origins
                .iter()
                .filter(|origin| {
                    origin
                        .table_provider()
                        .is_some_and(|provider| provider.downcast_ref::<MemTable>().is_some())
                })
                .count();
            self.0.origins.lock().unwrap().push((
                dataset.name.name.clone(),
                dataset.access,
                dataset.origins.len(),
                providers,
            ));
        }
        Ok(FacetBuilders::default()
            .with(RunProbe {
                run_id: cx.run_id,
                calls: self.0.clone(),
            })
            .with(JobProbe)
            .with(DatasetProbe(self.0.clone()))
            .with(InputProbe)
            .with(OutputProbe))
    }
}

#[derive(Debug)]
struct RunProbe {
    run_id: Uuid,
    calls: Arc<Calls>,
}

impl FacetBuilder for RunProbe {
    type Scope = scope::Run;
    fn build(
        &self,
        cx: &RunFacetContext<'_>,
        sink: &mut FacetSink<Self::Scope>,
    ) -> Result<(), FacetError> {
        assert_eq!(self.run_id, cx.event.run.run_id);
        assert!(
            cx.event.run.facets.processing_engine.is_some(),
            "built-ins run first"
        );
        if cx.event.event_type == RunEventType::Fail {
            assert!(
                cx.event.run.facets.error_message.is_some(),
                "failure is finalized before callbacks"
            );
        }
        self.calls.builds.lock().unwrap().push((
            self.run_id,
            cx.event.event_type,
            "run",
            String::new(),
        ));
        sink.insert(probe(cx.event.event_type, "run", None))
    }
}

#[derive(Debug)]
struct JobProbe;
impl FacetBuilder for JobProbe {
    type Scope = scope::Job;
    fn build(
        &self,
        cx: &JobFacetContext<'_>,
        sink: &mut FacetSink<Self::Scope>,
    ) -> Result<(), FacetError> {
        sink.insert(probe(cx.event.event_type, "job", None))
    }
}

#[derive(Debug)]
struct DatasetProbe(Arc<Calls>);
impl FacetBuilder for DatasetProbe {
    type Scope = scope::Dataset;
    fn applies_to(&self, cx: &DatasetFacetContext<'_>) -> bool {
        self.0.dataset_checks.fetch_add(1, Ordering::Relaxed);
        cx.dataset.name == "dst" && cx.access == DatasetAccess::Write
    }
    fn build(
        &self,
        cx: &DatasetFacetContext<'_>,
        sink: &mut FacetSink<Self::Scope>,
    ) -> Result<(), FacetError> {
        assert_eq!(cx.dataset.name, "dst");
        assert_eq!(cx.access, DatasetAccess::Write);
        assert!(!cx.origins.is_empty());
        self.0.builds.lock().unwrap().push((
            cx.event.run.run_id,
            cx.event.event_type,
            "dataset",
            cx.dataset.name.clone(),
        ));
        sink.insert(probe(cx.event.event_type, &cx.dataset.name, None))
    }
}

#[derive(Debug)]
struct InputProbe;
impl FacetBuilder for InputProbe {
    type Scope = scope::InputDataset;
    fn build(
        &self,
        cx: &DatasetFacetContext<'_>,
        sink: &mut FacetSink<Self::Scope>,
    ) -> Result<(), FacetError> {
        assert_eq!(cx.access, DatasetAccess::Read);
        sink.insert(probe(cx.event.event_type, &cx.dataset.name, None))
    }
}

#[derive(Debug)]
struct OutputProbe;
impl FacetBuilder for OutputProbe {
    type Scope = scope::OutputDataset;
    fn applies_to(&self, cx: &DatasetFacetContext<'_>) -> bool {
        cx.event.event_type == RunEventType::Complete
    }
    fn build(
        &self,
        cx: &DatasetFacetContext<'_>,
        sink: &mut FacetSink<Self::Scope>,
    ) -> Result<(), FacetError> {
        assert_eq!(cx.access, DatasetAccess::Write);
        let rows = cx
            .dataset
            .output_facets
            .as_ref()
            .and_then(|facets| facets.output_statistics.as_ref())
            .and_then(|stats| stats.row_count);
        sink.insert(probe(cx.event.event_type, &cx.dataset.name, rows))
    }
}

#[tokio::test]
async fn scopes_targets_and_callbacks_follow_actual_emission() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let calls = Arc::new(Calls::default());
    let ctx = SessionContext::new().with_lineage(
        OpenLineage::builder()
            .client(client.clone())
            .config(config())
            .facet_factory(Arc::new(Probes(calls.clone()))),
    );
    register(&ctx);

    let plan = ctx
        .sql("INSERT INTO dst SELECT src.a FROM src JOIN dst ON src.a = dst.a")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    assert_eq!(calls.factories.load(Ordering::Relaxed), 1);
    assert_eq!(calls.dataset_checks.load(Ordering::Relaxed), 3);
    assert!(
        calls
            .builds
            .lock()
            .unwrap()
            .iter()
            .all(|(_, event, _, _)| *event == RunEventType::Start)
    );
    assert_eq!(
        calls.builds.lock().unwrap().len(),
        2,
        "creating the COMPLETE template runs no builders"
    );

    collect(plan, ctx.task_ctx()).await.unwrap();
    drop(ctx);
    client.shutdown().await;
    let events = transport.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].event_type, RunEventType::Complete);
    assert_eq!(calls.dataset_checks.load(Ordering::Relaxed), 6);
    assert_eq!(calls.builds.lock().unwrap().len(), 4);
    for event in &events {
        assert_eq!(event.inputs.len(), 2);
        assert_eq!(event.outputs.len(), 1);
        assert_eq!(event.job.facets.extra["test_observed"]["target"], "job");
        assert_eq!(
            event.run.facets.extra["test_observed"]["event_type"],
            serde_json::to_value(event.event_type).unwrap()
        );
        for input in &event.inputs {
            assert!(!input.facets.extra.contains_key("test_observed"));
            assert!(input.output_facets.is_none());
            assert_eq!(
                input.input_facets.as_ref().unwrap().extra["test_observed"]["target"],
                input.name
            );
        }
        let output = &event.outputs[0];
        assert_eq!(output.facets.extra["test_observed"]["target"], "dst");
        assert!(output.input_facets.is_none());
        assert_eq!(
            output.facets.extra["test_observed"]["_producer"],
            config().producer
        );
        assert_eq!(
            output.facets.extra["test_observed"]["_schemaURL"],
            Probe::<scope::Dataset>::SCHEMA_URL
        );
    }
    assert!(events[0].outputs[0].output_facets.is_none());
    assert_eq!(
        events[1].outputs[0].output_facets.as_ref().unwrap().extra["test_observed"]["rows"],
        2
    );
    assert!(
        calls
            .origins
            .lock()
            .unwrap()
            .iter()
            .all(|(_, _, count, providers)| *count == 1 && *providers == 1)
    );
}

#[tokio::test]
async fn concurrent_queries_have_independent_prepared_state_and_failure_events() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let calls = Arc::new(Calls::default());
    let ctx = SessionContext::new().with_lineage(
        OpenLineage::builder()
            .client(client.clone())
            .config(config())
            .facet_factory(Arc::new(Probes(calls.clone()))),
    );
    register(&ctx);
    let good = ctx.sql("SELECT a FROM src").await.unwrap();
    let bad = ctx.sql("SELECT a / (a - a) FROM src").await.unwrap();
    let (good, bad) = tokio::join!(good.collect(), bad.collect());
    assert!(good.is_ok());
    assert!(bad.is_err());
    drop(ctx);
    client.shutdown().await;
    let events = transport.events();
    assert_eq!(events.len(), 4);
    assert_eq!(calls.factories.load(Ordering::Relaxed), 2);
    for terminal in events
        .iter()
        .filter(|event| event.event_type != RunEventType::Start)
    {
        assert!(
            events
                .iter()
                .any(|start| start.event_type == RunEventType::Start
                    && start.run.run_id == terminal.run.run_id)
        );
        assert_eq!(
            terminal.run.facets.extra["test_observed"]["event_type"],
            serde_json::to_value(terminal.event_type).unwrap()
        );
    }
}

#[tokio::test]
async fn ddl_and_stream_cancellation_use_the_same_dispatch() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let calls = Arc::new(Calls::default());
    let ctx = SessionContext::new().with_lineage(
        OpenLineage::builder()
            .client(client.clone())
            .config(config())
            .facet_factory(Arc::new(Probes(calls.clone()))),
    );
    register(&ctx);
    ctx.sql_with_lineage("CREATE VIEW v AS SELECT a FROM src")
        .await
        .unwrap();
    assert!(
        ctx.sql_with_lineage("CREATE VIEW v AS SELECT a FROM src")
            .await
            .is_err()
    );
    let stream = ctx
        .sql("SELECT a FROM src")
        .await
        .unwrap()
        .execute_stream()
        .await
        .unwrap();
    drop(stream);
    drop(ctx);
    client.shutdown().await;
    let events = transport.events();
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        [
            RunEventType::Start,
            RunEventType::Complete,
            RunEventType::Start,
            RunEventType::Fail,
            RunEventType::Start,
            RunEventType::Fail,
        ]
    );
    assert_eq!(calls.factories.load(Ordering::Relaxed), 3);
    assert!(
        events
            .iter()
            .all(|event| event.run.facets.extra.contains_key("test_observed"))
    );
    assert!(
        calls
            .origins
            .lock()
            .unwrap()
            .iter()
            .any(|(name, access, count, providers)| name == "v"
                && *access == DatasetAccess::Write
                && *count == 1
                && *providers == 0)
    );
}

#[derive(Debug)]
struct SameIdentity;
#[async_trait]
impl DatasetResolver for SameIdentity {
    async fn resolve(&self, _: &DatasetResolutionContext<'_>) -> Option<DatasetName> {
        Some(DatasetName {
            namespace: "catalog://canonical".into(),
            name: "shared".into(),
        })
    }
}

#[tokio::test]
async fn custom_planner_preserves_all_origins_and_dispatches_planning_failure() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let calls = Arc::new(Calls::default());
    let ctx = SessionContext::new();
    register(&ctx);
    let state = ctx.state();
    let plan = state
        .create_logical_plan("INSERT INTO dst SELECT src.a FROM src JOIN dst ON src.a = dst.a")
        .await
        .unwrap();
    let registry = FacetRegistry::default().with_factory(Arc::new(Probes(calls.clone())));
    let handle = begin_lineage_with_facets(
        &client,
        &StaticContextProvider::default(),
        &config(),
        &plan,
        &state,
        &[Arc::new(SameIdentity)],
        &registry,
    )
    .await
    .unwrap();
    handle.emit_fail(&client, &config(), "physical planning failed");
    drop(handle);
    client.shutdown().await;
    let events = transport.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].event_type, RunEventType::Fail);
    assert_eq!(events[0].inputs.len(), 1);
    assert_eq!(events[0].inputs[0].name, "shared");
    let origins = calls.origins.lock().unwrap();
    assert!(origins.contains(&("shared".into(), DatasetAccess::Read, 2, 2)));
    assert!(origins.contains(&("shared".into(), DatasetAccess::Write, 1, 1)));
}

#[tokio::test]
async fn builtin_registration_can_be_disabled_without_affecting_execution() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let ctx = SessionContext::new().with_lineage(
        OpenLineage::builder()
            .client(client.clone())
            .disable_facet_factory(PROCESSING_ENGINE_FACTORY),
    );
    register(&ctx);
    ctx.sql("SELECT a FROM src")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    drop(ctx);
    client.shutdown().await;
    let events = transport.events();
    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .all(|event| event.run.facets.processing_engine.is_none())
    );
}

#[derive(Debug)]
struct FailingFactory {
    panic: bool,
}
#[async_trait]
impl FacetBuilderFactory for FailingFactory {
    fn name(&self) -> &'static str {
        if self.panic {
            "test.panic"
        } else {
            "test.error"
        }
    }
    async fn create(&self, _: &QueryContext<'_>) -> Result<FacetBuilders, FacetError> {
        if self.panic {
            panic!("factory panic");
        }
        Err(FacetError::Other("lookup failed".into()))
    }
}

#[derive(Debug)]
struct FailingBuilders(Arc<AtomicUsize>);
#[async_trait]
impl FacetBuilderFactory for FailingBuilders {
    fn name(&self) -> &'static str {
        "test.failing_builders"
    }
    async fn create(&self, _: &QueryContext<'_>) -> Result<FacetBuilders, FacetError> {
        Ok(FacetBuilders::default()
            .with(PartialFailure)
            .with(NeverBuild(self.0.clone()))
            .with(PanickingBuilder))
    }
}

#[derive(Debug)]
struct PartialFailure;
impl FacetBuilder for PartialFailure {
    type Scope = scope::Run;
    fn build(
        &self,
        cx: &RunFacetContext<'_>,
        sink: &mut FacetSink<Self::Scope>,
    ) -> Result<(), FacetError> {
        sink.insert(probe(cx.event.event_type, "partial must not escape", None))?;
        Err(FacetError::Other("failed after staging".into()))
    }
}
#[derive(Debug)]
struct NeverBuild(Arc<AtomicUsize>);
impl FacetBuilder for NeverBuild {
    type Scope = scope::Run;
    fn applies_to(&self, _: &RunFacetContext<'_>) -> bool {
        false
    }
    fn build(
        &self,
        _: &RunFacetContext<'_>,
        _: &mut FacetSink<Self::Scope>,
    ) -> Result<(), FacetError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        unreachable!("applies_to returned false")
    }
}
#[derive(Debug)]
struct PanickingBuilder;
impl FacetBuilder for PanickingBuilder {
    type Scope = scope::Run;
    fn applies_to(&self, _: &RunFacetContext<'_>) -> bool {
        panic!("predicate panic");
    }
    fn build(
        &self,
        _: &RunFacetContext<'_>,
        _: &mut FacetSink<Self::Scope>,
    ) -> Result<(), FacetError> {
        unreachable!()
    }
}

#[tokio::test]
async fn extension_failures_do_not_fail_queries_or_leak_partial_facets() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let calls = Arc::new(Calls::default());
    let skipped_builds = Arc::new(AtomicUsize::new(0));
    let ctx = SessionContext::new().with_lineage(
        OpenLineage::builder()
            .client(client.clone())
            .facet_factory(Arc::new(FailingFactory { panic: false }))
            .facet_factory(Arc::new(FailingFactory { panic: true }))
            .facet_factory(Arc::new(FailingBuilders(skipped_builds.clone())))
            .facet_factory(Arc::new(Probes(calls.clone())))
            .facet_factory(Arc::new(Probes(calls.clone()))),
    ); // Duplicate name runs once.
    register(&ctx);
    ctx.sql("SELECT a FROM src")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    drop(ctx);
    client.shutdown().await;
    assert_eq!(calls.factories.load(Ordering::Relaxed), 1);
    assert_eq!(skipped_builds.load(Ordering::Relaxed), 0);
    let events = transport.events();
    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .all(|event| event.run.facets.extra["test_observed"]["target"] == "run")
    );
}

#[tokio::test]
async fn suppressed_queries_do_not_prepare_factories() {
    let calls = Arc::new(Calls::default());
    let ctx = SessionContext::new()
        .with_lineage(OpenLineage::builder().facet_factory(Arc::new(Probes(calls.clone()))));
    ctx.sql("SELECT 1").await.unwrap().collect().await.unwrap();
    assert_eq!(calls.factories.load(Ordering::Relaxed), 0);
}

#[derive(Debug)]
struct PlanningFailure;

#[async_trait]
impl datafusion::datasource::TableProvider for PlanningFailure {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }

    async fn scan(
        &self,
        _: &dyn datafusion::catalog::Session,
        _: Option<&Vec<usize>>,
        _: &[datafusion::logical_expr::Expr],
        _: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Err(datafusion::error::DataFusionError::Plan(
            "cannot plan this provider".into(),
        ))
    }
}

#[tokio::test]
async fn physical_planning_failure_dispatches_fail_before_any_execution_wrapper_exists() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let calls = Arc::new(Calls::default());
    let ctx = SessionContext::new().with_lineage(
        OpenLineage::builder()
            .client(client.clone())
            .facet_factory(Arc::new(Probes(calls.clone()))),
    );
    ctx.register_table("broken", Arc::new(PlanningFailure))
        .unwrap();
    assert!(
        ctx.sql("SELECT a FROM broken")
            .await
            .unwrap()
            .collect()
            .await
            .is_err()
    );
    drop(ctx);
    client.shutdown().await;
    let events = transport.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event_type, RunEventType::Start);
    assert_eq!(events[1].event_type, RunEventType::Fail);
    assert_eq!(
        events[1].run.facets.extra["test_observed"]["event_type"],
        "FAIL"
    );
    assert!(
        events[1]
            .run
            .facets
            .error_message
            .as_ref()
            .unwrap()
            .message
            .contains("cannot plan this provider")
    );
    assert_eq!(calls.factories.load(Ordering::Relaxed), 1);
}
