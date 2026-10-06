//! Dataset resolution contracts, using providers with no external services.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use common::RecordingTransport;
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::{MemTable, TableProvider, TableType, provider_as_source};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableSource};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::TableReference;
use datafusion_openlineage::builder::start_event;
use datafusion_openlineage::{
    DatasetAccess, DatasetName, DatasetResolutionContext, DatasetResolver, LineageContext,
    OpenLineage, OpenLineageClient, OpenLineageConfig, OpenLineageSqlExt, QueryLineage,
    RunEventType, StaticContextProvider, begin_lineage_with_resolvers, extract,
    extract_with_resolvers,
};

fn config() -> OpenLineageConfig {
    common::config("jobs", true)
}

fn name(table: &str) -> DatasetName {
    DatasetName::from_table_ref("s3://warehouse", table)
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]))
}

#[derive(Debug)]
struct StorageTable {
    inner: MemTable,
    name: DatasetName,
}

impl StorageTable {
    fn new(table: &str) -> Self {
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![3, 4])),
            ],
        )
        .unwrap();
        Self {
            inner: MemTable::try_new(schema(), vec![vec![batch]]).unwrap(),
            name: name(table),
        }
    }
}

#[async_trait]
impl TableProvider for StorageTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, op).await
    }
}

#[derive(Debug, Default)]
struct ProviderResolver {
    calls: Mutex<Vec<(TableReference, DatasetAccess, bool)>>,
}

#[async_trait]
impl DatasetResolver for ProviderResolver {
    async fn resolve(&self, cx: &DatasetResolutionContext<'_>) -> Option<DatasetName> {
        assert_eq!(cx.default_namespace, "jobs");
        self.calls
            .lock()
            .unwrap()
            .push((cx.table_ref.clone(), cx.access, cx.source.is_some()));
        // Exercise a resolver that actually yields, without network or sleeps.
        tokio::task::yield_now().await;
        let provider = cx.table_provider()?;
        Some(provider.downcast_ref::<StorageTable>()?.name.clone())
    }
}

#[derive(Debug, Default)]
struct DecliningResolver(AtomicUsize);

#[async_trait]
impl DatasetResolver for DecliningResolver {
    async fn resolve(&self, _: &DatasetResolutionContext<'_>) -> Option<DatasetName> {
        self.0.fetch_add(1, Ordering::Relaxed);
        None
    }
}

#[derive(Debug)]
struct UnexpectedResolver;

#[async_trait]
impl DatasetResolver for UnexpectedResolver {
    async fn resolve(&self, _: &DatasetResolutionContext<'_>) -> Option<DatasetName> {
        panic!("resolver after a successful match must not be called")
    }
}

fn context() -> SessionContext {
    let ctx = SessionContext::new();
    register_tables(&ctx);
    ctx
}

fn register_tables(ctx: &SessionContext) {
    ctx.register_table("src", Arc::new(StorageTable::new("source")))
        .unwrap();
    ctx.register_table("dst", Arc::new(StorageTable::new("destination")))
        .unwrap();
}

async fn plan(ctx: &SessionContext, sql: &str) -> LogicalPlan {
    let plan = ctx.state().create_logical_plan(sql).await.unwrap();
    ctx.state().optimize(&plan).unwrap()
}

#[tokio::test]
async fn builder_resolvers_apply_to_start_complete_fail_and_column_lineage() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let declining = Arc::new(DecliningResolver::default());
    let resolver = Arc::new(ProviderResolver::default());
    let ctx = SessionContext::new().with_lineage(
        OpenLineage::builder()
            .client(client.clone())
            .config(config())
            .dataset_resolver(declining.clone())
            .dataset_resolver(resolver.clone())
            .dataset_resolver(Arc::new(UnexpectedResolver)),
    );
    register_tables(&ctx);

    ctx.sql("INSERT INTO dst SELECT a, b FROM src WHERE b > 0")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let failed = ctx
        .sql("INSERT INTO dst SELECT a / (b - b), b FROM src")
        .await
        .unwrap()
        .collect()
        .await;
    assert!(failed.is_err());
    drop(ctx);
    client.shutdown().await;

    let events = transport.events();
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![
            RunEventType::Start,
            RunEventType::Complete,
            RunEventType::Start,
            RunEventType::Fail
        ]
    );
    assert_eq!(events[0].run.run_id, events[1].run.run_id);
    assert_eq!(events[2].run.run_id, events[3].run.run_id);
    assert_ne!(events[0].run.run_id, events[2].run.run_id);
    for event in &events {
        assert_eq!(event.job.namespace, "jobs");
        assert_eq!(event.inputs.len(), 1);
        assert_eq!(event.outputs.len(), 1);
        assert_eq!(event.inputs[0].namespace, "s3://warehouse");
        assert_eq!(event.inputs[0].name, "source");
        assert_eq!(event.outputs[0].namespace, "s3://warehouse");
        assert_eq!(event.outputs[0].name, "destination");
        let columns = event.outputs[0].facets.column_lineage.as_ref().unwrap();
        for field in columns.fields.values() {
            assert!(!field.input_fields.is_empty());
            for input in &field.input_fields {
                assert_eq!(input.namespace, event.inputs[0].namespace);
                assert_eq!(input.name, event.inputs[0].name);
            }
        }
    }
    let columns = events[0].outputs[0].facets.column_lineage.as_ref().unwrap();
    assert!(columns.fields["a"].input_fields.iter().any(|input| {
        input.field.as_deref() == Some("b")
            && input
                .transformations
                .iter()
                .any(|t| t.subtype.as_deref() == Some("FILTER"))
    }));
    // One read and one write lookup per query, shared by table/column extraction
    // and START/terminal events; a new query resolves again.
    assert_eq!(declining.0.load(Ordering::Relaxed), 4);
    assert_eq!(resolver.calls.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn no_match_preserves_existing_events_and_allows_execution() {
    let resolver = Arc::new(DecliningResolver::default());
    let resolvers: Vec<Arc<dyn DatasetResolver>> = vec![resolver.clone()];
    let ctx = context();
    let plan = plan(&ctx, "INSERT INTO dst SELECT a, b FROM src").await;
    let cfg = config();
    let expected = extract(&plan, &cfg);
    for actual in [
        extract_with_resolvers(&plan, &cfg, &[]).await,
        extract_with_resolvers(&plan, &cfg, &resolvers).await,
    ] {
        let event = |lineage: &QueryLineage| {
            let event = start_event(uuid::Uuid::nil(), lineage, &LineageContext::default(), &cfg);
            serde_json::json!({"inputs": event.inputs, "outputs": event.outputs})
        };
        assert_eq!(event(&actual), event(&expected));
    }
    let ctx = ctx.with_lineage(
        OpenLineage::builder()
            .config(cfg)
            .dataset_resolver(resolver),
    );
    register_tables(&ctx);
    ctx.sql("SELECT a FROM src")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

fn scan(table_ref: &str, source: Arc<dyn TableSource>) -> LogicalPlan {
    LogicalPlanBuilder::scan(table_ref, source, None)
        .unwrap()
        .build()
        .unwrap()
}

#[tokio::test]
async fn source_identity_and_canonical_deduplication_are_independent() {
    let source = provider_as_source(Arc::new(StorageTable::new("first")));
    let other = provider_as_source(Arc::new(StorageTable::new("second")));
    let input = LogicalPlanBuilder::from(scan("t", source.clone()))
        .union(scan("t", other)) // Same reference, different source.
        .unwrap()
        .union(scan("t", source.clone())) // Exact repeated request.
        .unwrap()
        .union(scan("other_catalog.schema.t", source)) // Another reference to the first dataset.
        .unwrap()
        .build()
        .unwrap();
    let plan = LogicalPlanBuilder::insert_into(
        input,
        "dst",
        provider_as_source(Arc::new(StorageTable::new("destination"))),
        InsertOp::Append,
    )
    .unwrap()
    .build()
    .unwrap();
    let resolver = Arc::new(ProviderResolver::default());
    let resolvers: Vec<Arc<dyn DatasetResolver>> = vec![resolver.clone()];
    let lineage = extract_with_resolvers(&plan, &config(), &resolvers).await;
    assert_eq!(
        lineage
            .inputs
            .iter()
            .map(|input| input.name.clone())
            .collect::<Vec<_>>(),
        vec![name("first"), name("second")]
    );
    for sources in lineage.outputs[0]
        .column_lineage
        .as_ref()
        .unwrap()
        .fields
        .values()
    {
        assert_eq!(sources.direct.len(), 2);
        assert!(
            sources
                .direct
                .keys()
                .any(|source| source.dataset == name("first"))
        );
        assert!(
            sources
                .direct
                .keys()
                .any(|source| source.dataset == name("second"))
        );
    }
    assert_eq!(resolver.calls.lock().unwrap().len(), 4);
}

#[derive(Debug)]
struct AccessResolver;

#[async_trait]
impl DatasetResolver for AccessResolver {
    async fn resolve(&self, cx: &DatasetResolutionContext<'_>) -> Option<DatasetName> {
        Some(name(match cx.access {
            DatasetAccess::Read => "before",
            DatasetAccess::Write => "after",
        }))
    }
}

#[tokio::test]
async fn same_source_can_resolve_differently_for_reads_and_writes() {
    let source = provider_as_source(Arc::new(StorageTable::new("table")));
    let plan = LogicalPlanBuilder::insert_into(
        scan("table", source.clone()),
        "table",
        source,
        InsertOp::Append,
    )
    .unwrap()
    .build()
    .unwrap();
    let lineage = extract_with_resolvers(&plan, &config(), &[Arc::new(AccessResolver)]).await;
    assert_eq!(lineage.inputs[0].name, name("before"));
    assert_eq!(lineage.outputs[0].name, name("after"));
    for sources in lineage.outputs[0]
        .column_lineage
        .as_ref()
        .unwrap()
        .fields
        .values()
    {
        assert!(
            sources
                .direct
                .keys()
                .all(|source| source.dataset == name("before"))
        );
    }
}

#[derive(Debug)]
struct DdlResolver;

#[async_trait]
impl DatasetResolver for DdlResolver {
    async fn resolve(&self, cx: &DatasetResolutionContext<'_>) -> Option<DatasetName> {
        assert!(cx.source.is_none());
        assert!(cx.table_provider().is_none());
        assert_eq!(cx.access, DatasetAccess::Write);
        Some(name(cx.table_ref.table()))
    }
}

#[tokio::test]
async fn ddl_targets_have_no_source_and_share_resolvers_with_inputs() {
    for sql in [
        "CREATE TABLE created AS SELECT a FROM src",
        "CREATE VIEW created AS SELECT a FROM src",
    ] {
        let transport = RecordingTransport::default();
        let client = OpenLineageClient::new(Arc::new(transport.clone()));
        let resolver = Arc::new(ProviderResolver::default());
        let ctx = SessionContext::new().with_lineage(
            OpenLineage::builder()
                .client(client.clone())
                .config(config())
                .dataset_resolver(resolver.clone())
                .dataset_resolver(Arc::new(DdlResolver)),
        );
        register_tables(&ctx);
        ctx.sql_with_lineage(sql).await.unwrap();
        drop(ctx);
        client.shutdown().await;
        let events = transport.events();
        assert_eq!(events.len(), 2, "nested DDL body must not emit its own run");
        assert_eq!(events[0].event_type, RunEventType::Start);
        assert_eq!(events[1].event_type, RunEventType::Complete);
        assert_eq!(events[0].run.run_id, events[1].run.run_id);
        for event in events {
            assert_eq!(event.outputs[0].namespace, "s3://warehouse");
            assert_eq!(event.outputs[0].name, "created");
            assert_eq!(event.inputs[0].name, "source");
            let input = &event.outputs[0]
                .facets
                .column_lineage
                .as_ref()
                .unwrap()
                .fields["a"]
                .input_fields[0];
            assert_eq!(input.name, "source");
            assert_eq!(input.namespace, "s3://warehouse");
        }
        let calls = resolver.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!((calls[0].1, calls[0].2), (DatasetAccess::Write, false));
        assert_eq!((calls[1].1, calls[1].2), (DatasetAccess::Read, true));
    }

    let plan = plan(
        &context(),
        "CREATE EXTERNAL TABLE external_table (a INT) STORED AS PARQUET LOCATION '/unused'",
    )
    .await;
    let lineage = extract_with_resolvers(&plan, &config(), &[Arc::new(DdlResolver)]).await;
    assert_eq!(lineage.outputs[0].name, name("external_table"));
}

struct CustomSource;

impl TableSource for CustomSource {
    fn schema(&self) -> SchemaRef {
        schema()
    }
}

#[derive(Debug)]
struct CustomSourceResolver;

#[async_trait]
impl DatasetResolver for CustomSourceResolver {
    async fn resolve(&self, cx: &DatasetResolutionContext<'_>) -> Option<DatasetName> {
        assert!(cx.table_provider().is_none());
        cx.source?.downcast_ref::<CustomSource>()?;
        Some(name("custom"))
    }
}

#[tokio::test]
async fn custom_table_sources_can_resolve_without_a_table_provider() {
    let plan = scan("logical", Arc::new(CustomSource));
    let lineage = extract_with_resolvers(&plan, &config(), &[Arc::new(CustomSourceResolver)]).await;
    assert_eq!(lineage.inputs[0].name, name("custom"));
}

#[tokio::test]
async fn metadata_and_constant_queries_do_not_call_resolvers() {
    for sql in [
        "SELECT table_name FROM information_schema.tables",
        "SELECT 1",
    ] {
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
        let plan = plan(&ctx, sql).await;
        let lineage =
            extract_with_resolvers(&plan, &config(), &[Arc::new(UnexpectedResolver)]).await;
        assert!(lineage.inputs.is_empty());
        assert!(lineage.outputs.is_empty());
    }
}

#[tokio::test]
async fn custom_planner_entry_point_retains_resolved_identities() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let ctx = context();
    let plan = plan(&ctx, "SELECT a FROM src").await;
    let cfg = config();
    let handle = begin_lineage_with_resolvers(
        &client,
        &StaticContextProvider::default(),
        &cfg,
        &plan,
        &ctx.state(),
        &[Arc::new(ProviderResolver::default())],
    )
    .await
    .unwrap();
    handle.emit_complete(&client, &cfg);
    client.shutdown().await;
    let events = transport.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].run.run_id, events[1].run.run_id);
    for event in events {
        assert_eq!(event.inputs[0].namespace, "s3://warehouse");
        assert_eq!(event.inputs[0].name, "source");
        assert_eq!(
            event.inputs[0].facets.schema.as_ref().unwrap().fields.len(),
            2,
            "projection must not shrink the reported dataset schema"
        );
    }
}
