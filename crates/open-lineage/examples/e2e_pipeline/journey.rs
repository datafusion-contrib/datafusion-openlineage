//! The transport-agnostic core of the end-to-end DataFusion lineage journey.
//!
//! Shared between the runnable demo (`main.rs`, which picks a transport from the
//! environment) and the always-on integration test
//! (`tests/e2e_pipeline.rs`, which records events in memory and asserts on them).
//! Keeping the journey here — rather than forking ~500 lines — means the test
//! covers exactly the pipeline the demo runs.
//!
//! The journey instruments real [`SessionContext`]s and executes a multi-stage
//! `INSERT INTO <out> SELECT … FROM <ins>` pipeline (bronze → silver → gold)
//! over Parquet files on disk, letting the crate extract lineage from the query
//! plans. See [`run_pipeline`] for the entry point; the module-level doc on
//! `main.rs` explains the data flow and why `INSERT INTO` is used over CTAS.
//!
//! This module is `#[path]`-included by the test and `mod`-included by the
//! example; each consumer uses a different subset, so `dead_code` is allowed.
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::memory::MemorySchemaProvider;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionState;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion::sql::TableReference;
use datafusion_openlineage::DataFusionConfig;
use datafusion_openlineage::config::OpenLineageConfig;
use datafusion_openlineage::context::{LineageContext, LineageContextProvider};
use datafusion_openlineage::facets::{BaseFacet, ParentJob, ParentRun, ParentRunFacet};
use datafusion_openlineage::{OpenLineageClient, instrument_session_state};
use uuid::Uuid;

/// One whole pipeline run that the per-stage queries hang off of as children.
///
/// Holds a single parent run id + parent job identity (the "orchestrator") so
/// every stage's emitted run carries a `parent` facet pointing back here — this
/// is what lets the UI present the stages as one correlated pipeline rather than
/// a handful of disconnected jobs.
#[derive(Debug, Clone)]
pub struct Pipeline {
    /// The parent run id stamped onto every stage's `parent` run facet.
    pub parent_run_id: Uuid,
    /// The parent job's namespace (the orchestrator's namespace).
    pub parent_namespace: String,
    /// The parent job's name (the orchestrator's job).
    pub parent_job: String,
    /// The `producer` URI stamped on emitted events.
    pub producer: String,
}

impl Pipeline {
    /// The `parent` run facet every stage stamps onto its run.
    fn parent_facet(&self) -> ParentRunFacet {
        ParentRunFacet {
            base: BaseFacet::new(&self.producer, "1-1-0/ParentRunFacet.json"),
            run: ParentRun {
                run_id: self.parent_run_id.to_string(),
            },
            job: ParentJob {
                namespace: self.parent_namespace.clone(),
                name: self.parent_job.clone(),
            },
            root: None,
        }
    }
}

/// Per-stage [`LineageContextProvider`]: names the stage's job and links it to
/// the parent pipeline run. The SQL text is supplied per query (the plan walk
/// can't recover it) so the `sql` job facet is populated.
#[derive(Debug)]
struct StageContext {
    pipeline: Pipeline,
    job_name: String,
    job_namespace: String,
    sql: String,
}

#[async_trait]
impl LineageContextProvider for StageContext {
    async fn context(&self, _state: &SessionState) -> LineageContext {
        LineageContext {
            job_namespace: Some(self.job_namespace.clone()),
            job_name: Some(self.job_name.clone()),
            parent_run: Some(self.pipeline.parent_facet()),
            sql: Some(self.sql.clone()),
            ..Default::default()
        }
    }
}

/// The OpenLineage namespace all *datasets* share. Dataset identity is
/// `(config.job_namespace, table_name)`; the name is DataFusion's qualified
/// `catalog.schema.table` (e.g. `lake.bronze.raw_orders`), so the namespace just
/// names the engine/instance the catalog lives in. It must match between the
/// stage that writes a dataset and the stage that reads it, or the graph
/// wouldn't connect across layers — hence one shared value for every stage.
/// Jobs, by contrast, are organized per-layer via the context's `job_namespace`.
pub const DATASET_NAMESPACE: &str = "datafusion";

/// The data catalog every stage operates in. Datasets are named
/// `{CATALOG}.{layer}.{table}` (e.g. `lake.bronze.raw_orders`) — a real
/// three-part `catalog.schema.table` identity, not a flat name.
pub const CATALOG: &str = "lake";

/// The medallion layers, each a schema under [`CATALOG`].
const LAYERS: [&str; 3] = ["bronze", "silver", "gold"];

/// Register a fresh `{CATALOG}` catalog with one schema per [`LAYERS`] entry so
/// qualified `lake.bronze.raw_orders`-style names resolve. Each stage gets its
/// own catalog (its tables live only in that session); cross-stage data flows
/// through the persisted Parquet, re-registered by name.
fn register_catalog(ctx: &SessionContext) {
    let catalog = Arc::new(MemoryCatalogProvider::new());
    for layer in LAYERS {
        catalog
            .register_schema(layer, Arc::new(MemorySchemaProvider::new()))
            .expect("fresh catalog has no duplicate schema");
    }
    ctx.register_catalog(CATALOG, catalog);
}

/// Build a fresh instrumented context for one stage, with the `{CATALOG}`
/// catalog registered. `job_layer` (bronze/silver/gold) organizes the stage's
/// *job* namespace; datasets use [`DATASET_NAMESPACE`] + their qualified name.
/// Shares the process-wide emit `client`.
fn stage_context(
    client: &OpenLineageClient,
    pipeline: &Pipeline,
    job_layer: &str,
    job_name: &str,
    sql: &str,
) -> SessionContext {
    let provider = StageContext {
        pipeline: pipeline.clone(),
        job_name: job_name.to_string(),
        job_namespace: job_layer.to_string(),
        sql: sql.to_string(),
    };
    let config = OpenLineageConfig {
        producer: pipeline.producer.clone(),
        job_namespace: DATASET_NAMESPACE.to_string(),
        ..OpenLineageConfig::for_datafusion()
    };
    let base = SessionStateBuilder::new_with_default_features().build();
    let state = instrument_session_state(base, client.clone(), Arc::new(provider), config);
    let ctx = SessionContext::new_with_state(state);
    register_catalog(&ctx);
    ctx
}

/// The qualified `catalog.schema.table` identity of a table in a layer. This is
/// exactly what becomes the OpenLineage dataset name, so writes and downstream
/// reads must agree on it.
pub fn qualified(layer: &str, table: &str) -> String {
    format!("{CATALOG}.{layer}.{table}")
}

/// Directory under the lake root for a layer's table Parquet output.
fn parquet_dir(root: &Path, layer: &str, table: &str) -> std::path::PathBuf {
    root.join(layer).join(table)
}

/// `register_parquet` a previously-written stage output under its qualified
/// `catalog.schema.table` name so the next stage reads it as that same dataset.
async fn register_upstream(
    ctx: &SessionContext,
    layer: &str,
    table: &str,
    root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let dir = parquet_dir(root, layer, table);
    ctx.register_parquet(
        TableReference::full(CATALOG, layer, table),
        dir.to_str().expect("utf-8 lake path"),
        ParquetReadOptions::default(),
    )
    .await?;
    Ok(())
}

/// One lineage-bearing transform: the layer + bare table name it writes, the job
/// name, the (empty) `CREATE TABLE` DDL, the `INSERT INTO … SELECT …` that fills
/// it, and the upstream tables to register as inputs. Table references in the
/// SQL are the qualified `lake.<layer>.<table>` names (see [`qualified`]).
struct Transform<'a> {
    layer: &'a str,
    table: &'a str,
    job_name: &'a str,
    /// `CREATE TABLE <out> (cols)` — pure DDL with no query, so it emits nothing.
    out_ddl: &'a str,
    /// `INSERT INTO <out> SELECT … FROM <ins>` — the instrumented statement; its
    /// plan carries the inputs **and** the output dataset + column lineage.
    insert: &'a str,
    /// Upstream tables to register as named inputs: `(layer, table)`.
    inputs: &'a [(&'a str, &'a str)],
}

/// Run a [`Transform`] end to end: build the stage's instrumented context (with
/// the insert as its `sql` facet), register upstream Parquet inputs, create the
/// empty output table, run the `INSERT`, and persist the result to Parquet.
///
/// Why `INSERT INTO` and not `CREATE TABLE AS SELECT`: DataFusion executes a CTAS
/// write itself, outside the instrumented planner. CTAS *can* be captured via
/// `OpenLineageSqlExt::sql_with_lineage` (output dataset, schema, column lineage),
/// but because DataFusion materializes the body internally that path carries no
/// runtime row statistics. An `INSERT INTO` of an existing table lowers to a
/// `Dml(Insert)` node the planner sees directly, so the write becomes an output
/// edge *with* runtime row statistics — which this stats-showcasing example wants.
async fn run_transform(
    client: &OpenLineageClient,
    pipeline: &Pipeline,
    root: &Path,
    t: &Transform<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = stage_context(client, pipeline, t.layer, t.job_name, t.insert);
    for (layer, table) in t.inputs {
        register_upstream(&ctx, layer, table, root).await?;
    }
    ctx.sql(t.out_ddl).await?.collect().await?;
    ctx.sql(t.insert).await?.collect().await?;
    persist(&ctx, t.layer, t.table, root).await?;
    eprintln!("  ✓ {}", qualified(t.layer, t.table));
    Ok(())
}

/// Write the just-built `lake.<layer>.<table>` (in `ctx`'s catalog) to Parquet
/// so later stages can read it back as a registered input.
///
/// The `COPY` runs in a **fresh, uninstrumented** context: the written table's
/// provider is moved over and copied there, so the persist read emits no
/// lineage. Reading it back through the instrumented `ctx` (even just to collect
/// it) would route a scan through that context's planner and emit a spurious
/// "job reads its own output" self-edge into the graph.
async fn persist(
    ctx: &SessionContext,
    layer: &str,
    table: &str,
    root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let name = TableReference::full(CATALOG, layer, table);
    let provider = ctx.table_provider(name.clone()).await?;

    let writer = SessionContext::new();
    register_catalog(&writer);
    writer.register_table(name, provider)?;

    let dir = parquet_dir(root, layer, table);
    writer
        .sql(&format!(
            "COPY {} TO '{}' STORED AS PARQUET",
            qualified(layer, table),
            dir.to_str().expect("utf-8 lake path")
        ))
        .await?
        .collect()
        .await?;
    Ok(())
}

/// Run the whole bronze → silver → gold journey against `client`, writing
/// intermediate Parquet under `root`. Returns the [`Pipeline`] that every
/// emitted run is correlated to (its `parent_run_id` is what the test asserts
/// against).
///
/// Does **not** shut the client down — the caller owns that, after dropping any
/// other client clones (see [`OpenLineageClient::shutdown`]). The per-stage
/// instrumented contexts are local to [`run_transform`] and dropped on return,
/// so by the time this returns no context holds a client clone.
pub async fn run_pipeline(
    client: &OpenLineageClient,
    root: &Path,
) -> Result<Pipeline, Box<dyn std::error::Error>> {
    let pipeline = Pipeline {
        parent_run_id: Uuid::now_v7(),
        parent_namespace: "pipelines".to_string(),
        parent_job: "retail_analytics".to_string(),
        producer: OpenLineageConfig::default().producer,
    };
    eprintln!(
        "→ pipeline run: {} ({}/{})",
        pipeline.parent_run_id, pipeline.parent_namespace, pipeline.parent_job
    );

    seed_bronze(client, &pipeline, root).await?;
    build_silver(client, &pipeline, root).await?;
    build_gold(client, &pipeline, root).await?;

    Ok(pipeline)
}

// ---------------------------------------------------------------------------
// Stage 0 — bronze: raw landing data, seeded from literals and persisted.
// ---------------------------------------------------------------------------

async fn seed_bronze(
    client: &OpenLineageClient,
    pipeline: &Pipeline,
    root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("\n── stage: bronze (seed raw data) ──");

    // INSERT … VALUES (no inputs) so each raw table is captured as an output.
    run_transform(
        client,
        pipeline,
        root,
        &Transform {
            layer: "bronze",
            table: "raw_orders",
            job_name: "bronze.write_raw_orders",
            out_ddl: "CREATE TABLE lake.bronze.raw_orders \
               (order_id INT, customer_id INT, amount DOUBLE, country VARCHAR, order_date VARCHAR)",
            insert: "INSERT INTO lake.bronze.raw_orders VALUES \
               (1, 101, 19.99,  'us', '2026-06-01'), \
               (2, 102, 5.50,   'de', '2026-06-01'), \
               (3, 101, 120.00, 'us', '2026-06-02'), \
               (4, 103, 42.10,  'fr', '2026-06-02'), \
               (5, 102, 8.75,   'de', '2026-06-03'), \
               (6, 104, 250.00, 'us', '2026-06-03')",
            inputs: &[],
        },
    )
    .await?;

    run_transform(
        client,
        pipeline,
        root,
        &Transform {
            layer: "bronze",
            table: "raw_customers",
            job_name: "bronze.write_raw_customers",
            out_ddl: "CREATE TABLE lake.bronze.raw_customers \
               (customer_id INT, name VARCHAR, email VARCHAR, home_country VARCHAR)",
            insert: "INSERT INTO lake.bronze.raw_customers VALUES \
               (101, 'Ada Lovelace', 'ada@example.com',   'US'), \
               (102, 'Carl Gauss',   'carl@example.com',  'DE'), \
               (103, 'Marie Curie',  'marie@example.com', 'FR'), \
               (104, 'Alan Turing',  'alan@example.com',  'US')",
            inputs: &[],
        },
    )
    .await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Stage 1 — silver: cleaned + enriched. Drives TRANSFORMATION / FILTER / JOIN
// column lineage.
// ---------------------------------------------------------------------------

async fn build_silver(
    client: &OpenLineageClient,
    pipeline: &Pipeline,
    root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("\n── stage: silver (clean + enrich) ──");

    // orders: cast/rename + a WHERE filter (TRANSFORMATION + FILTER).
    run_transform(
        client,
        pipeline,
        root,
        &Transform {
            layer: "silver",
            table: "orders",
            job_name: "silver.clean_orders",
            out_ddl: "CREATE TABLE lake.silver.orders \
               (order_id INT, customer_id INT, amount_usd DOUBLE, country VARCHAR, order_date VARCHAR)",
            insert: "INSERT INTO lake.silver.orders SELECT \
                 order_id, \
                 customer_id, \
                 CAST(amount AS DOUBLE) AS amount_usd, \
                 upper(country) AS country, \
                 order_date \
               FROM lake.bronze.raw_orders \
               WHERE amount > 0",
            inputs: &[("bronze", "raw_orders")],
        },
    )
    .await?;

    // orders_enriched: join orders to customers (JOIN column lineage).
    run_transform(
        client,
        pipeline,
        root,
        &Transform {
            layer: "silver",
            table: "orders_enriched",
            job_name: "silver.enrich_orders",
            out_ddl: "CREATE TABLE lake.silver.orders_enriched \
               (order_id INT, customer_id INT, customer_name VARCHAR, amount_usd DOUBLE, \
                country VARCHAR, order_date VARCHAR)",
            insert: "INSERT INTO lake.silver.orders_enriched SELECT \
                 o.order_id, \
                 o.customer_id, \
                 c.name AS customer_name, \
                 o.amount_usd, \
                 o.country, \
                 o.order_date \
               FROM lake.silver.orders o \
               JOIN lake.bronze.raw_customers c ON o.customer_id = c.customer_id",
            inputs: &[("silver", "orders"), ("bronze", "raw_customers")],
        },
    )
    .await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Stage 2 — gold: aggregates. Fans out to two outputs; drives AGGREGATION /
// GROUP_BY column lineage.
// ---------------------------------------------------------------------------

async fn build_gold(
    client: &OpenLineageClient,
    pipeline: &Pipeline,
    root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("\n── stage: gold (aggregate) ──");

    // revenue_by_country: SUM + GROUP BY (AGGREGATION + GROUP_BY).
    run_transform(
        client,
        pipeline,
        root,
        &Transform {
            layer: "gold",
            table: "revenue_by_country",
            job_name: "gold.revenue_by_country",
            out_ddl: "CREATE TABLE lake.gold.revenue_by_country \
               (country VARCHAR, order_count BIGINT, revenue_usd DOUBLE)",
            insert: "INSERT INTO lake.gold.revenue_by_country SELECT \
                 country, \
                 count(*) AS order_count, \
                 sum(amount_usd) AS revenue_usd \
               FROM lake.silver.orders_enriched \
               GROUP BY country",
            inputs: &[("silver", "orders_enriched")],
        },
    )
    .await?;

    // daily_orders: a second gold output so gold branches (> 1 dataset).
    run_transform(
        client,
        pipeline,
        root,
        &Transform {
            layer: "gold",
            table: "daily_orders",
            job_name: "gold.daily_orders",
            out_ddl: "CREATE TABLE lake.gold.daily_orders \
               (order_date VARCHAR, order_count BIGINT, revenue_usd DOUBLE)",
            insert: "INSERT INTO lake.gold.daily_orders SELECT \
                 order_date, \
                 count(*) AS order_count, \
                 sum(amount_usd) AS revenue_usd \
               FROM lake.silver.orders_enriched \
               GROUP BY order_date",
            inputs: &[("silver", "orders_enriched")],
        },
    )
    .await?;

    Ok(())
}
