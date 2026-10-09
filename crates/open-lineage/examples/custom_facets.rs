//! Run with `cargo run -p datafusion-openlineage --example custom_facets`.
//! Adds an engine run facet and a completion-only facet to one input dataset.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion_openlineage::facet::{
    DatasetFacetContext, Facet, FacetBuilder, FacetBuilderFactory, FacetBuilders, FacetError,
    FacetSink, QueryContext, RunFacetContext, scope,
};
use datafusion_openlineage::{
    ConsoleTransport, DataFusionConfig, OpenLineage, OpenLineageClient, OpenLineageConfig,
    OpenLineageSqlExt, RunEventType,
};
use serde::Serialize;

#[derive(Debug)]
struct EngineFactory;

#[async_trait]
impl FacetBuilderFactory for EngineFactory {
    fn name(&self) -> &'static str {
        "example.engine"
    }

    async fn create(&self, cx: &QueryContext<'_>) -> Result<FacetBuilders, FacetError> {
        // Capture owned state once; asynchronous provider metadata lookups can
        // also happen here. cx.datasets retains resolved identities and origins.
        Ok(FacetBuilders::default()
            .with(EngineBuilder {
                query_id: cx.run_id.to_string(),
            })
            .with(OrdersReadBuilder))
    }
}

#[derive(Serialize)]
struct EngineFacet<'a> {
    query_id: &'a str,
    stage: RunEventType,
}
impl Facet for EngineFacet<'_> {
    type Scope = scope::Run;
    const NAME: &'static str = "example_execution";
    const SCHEMA_URL: &'static str = "https://example.com/schemas/v1/ExecutionRunFacet.json";
}

#[derive(Debug)]
struct EngineBuilder {
    query_id: String,
}
impl FacetBuilder for EngineBuilder {
    type Scope = scope::Run;
    fn build(
        &self,
        cx: &RunFacetContext<'_>,
        facets: &mut FacetSink<Self::Scope>,
    ) -> Result<(), FacetError> {
        facets.insert(EngineFacet {
            query_id: &self.query_id,
            stage: cx.event.event_type,
        })
    }
}

#[derive(Serialize)]
struct ReadFacet {
    policy: &'static str,
}
impl Facet for ReadFacet {
    type Scope = scope::InputDataset;
    const NAME: &'static str = "example_read_policy";
    const SCHEMA_URL: &'static str =
        "https://example.com/schemas/v1/ReadPolicyInputDatasetFacet.json";
}

#[derive(Debug)]
struct OrdersReadBuilder;
impl FacetBuilder for OrdersReadBuilder {
    type Scope = scope::InputDataset;
    fn applies_to(&self, cx: &DatasetFacetContext<'_>) -> bool {
        cx.event.event_type == RunEventType::Complete
            && cx.dataset.namespace == "example"
            && cx.dataset.name == "orders"
    }
    fn build(
        &self,
        _: &DatasetFacetContext<'_>,
        facets: &mut FacetSink<Self::Scope>,
    ) -> Result<(), FacetError> {
        facets.insert(ReadFacet {
            policy: "analytics",
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let client = OpenLineageClient::new(Arc::new(ConsoleTransport));
    let ctx = SessionContext::new().with_lineage(
        OpenLineage::builder()
            .client(client.clone())
            .config(OpenLineageConfig {
                job_namespace: "example".into(),
                ..OpenLineageConfig::for_datafusion()
            })
            .facet_factory(Arc::new(EngineFactory)),
    );
    ctx.sql("CREATE TABLE orders AS VALUES (1), (2)").await?;
    ctx.sql("SELECT * FROM orders").await?.collect().await?;
    drop(ctx);
    client.shutdown().await;
    Ok(())
}
