//! End-to-end demo: a live, instrumented DataFusion pipeline emitting OpenLineage.
//!
//! Unlike the static JSON seed (`examples/seed/`), this runs the *real*
//! instrumentation path — it builds instrumented [`SessionContext`]s, executes
//! genuine multi-stage SQL transformations over Parquet files on disk, and lets
//! the crate extract lineage from the query plans and POST it to a running
//! `headwaters`. Run the service, run this, open the UI: the bronze →
//! silver → gold graph appears, covering schema facets, column-level lineage
//! (with TRANSFORMATION / FILTER / JOIN / AGGREGATION / GROUP_BY subtypes),
//! input statistics, run history, and parent/child run correlation.
//!
//! Datasets are named with real three-part `catalog.schema.table` identities
//! (`lake.bronze.raw_orders`, `lake.silver.orders`, `lake.gold.revenue_by_country`,
//! …): the layers are schemas under a `lake` catalog built from
//! [`MemoryCatalogProvider`]/[`MemorySchemaProvider`], not flat underscore names.
//!
//! The journey itself lives in [`journey`] (shared with the always-on
//! integration test `tests/e2e_pipeline.rs`); this binary only chooses a
//! transport and drives [`journey::run_pipeline`].
//!
//! ## Running
//!
//! ```sh
//! just dev      # Postgres + headwaters on :8091 (in one shell)
//! cargo run -p datafusion-openlineage --example e2e_pipeline   # in another
//! just ui-dev   # then open the UI and explore the graph
//! ```
//!
//! Point it at a non-default service with `OPENLINEAGE_URL`
//! (default `http://localhost:8091/api/v1/lineage`); set `OPENLINEAGE_API_KEY`
//! to authenticate, or `OPENLINEAGE_URL=console` for a service-free dry run that
//! logs events via `tracing`. Parquet is written under a temp dir, logged at
//! startup.
//!
//! Re-running appends new runs to each stage's job history without duplicating
//! datasets — handy for exercising the run-history views.
//!
//! ## How lineage is captured
//!
//! Each lineage-bearing query is an `INSERT INTO <out> SELECT … FROM <ins>`: the
//! SELECT's scans of registered Parquet become input datasets (with schema +
//! input statistics) and the insert target becomes the output dataset (with
//! schema + column lineage). `INSERT INTO` is used rather than `CREATE TABLE AS
//! SELECT` because DataFusion executes a CTAS write itself, outside the planner:
//! CTAS lineage is available via `OpenLineageSqlExt::sql_with_lineage` but without
//! runtime row statistics (the body is materialized internally), whereas an insert
//! of an existing table lowers to a `Dml` node the planner sees directly — so this
//! statistics-focused example uses `INSERT INTO`. The output is then
//! persisted to Parquet (via an uninstrumented context, so the read emits
//! nothing) for the next stage to pick up.

use std::sync::Arc;

use datafusion_openlineage::{CloudClientTransport, ConsoleTransport, OpenLineageClient};
use url::Url;

mod journey;

/// Build the emit client from the environment.
///
/// When `OPENLINEAGE_URL` is set, the standard env path (incl.
/// `OPENLINEAGE_ENDPOINT` / `OPENLINEAGE_API_KEY`) is delegated to
/// [`OpenLineageClient::from_env`]. On top of that the demo adds two
/// conveniences: defaulting to the local service when `OPENLINEAGE_URL` is unset,
/// and an `OPENLINEAGE_URL=console` dry run that logs each event as JSON via
/// `tracing` instead of POSTing it (so the demo runs with no service).
fn client_from_env() -> OpenLineageClient {
    match std::env::var("OPENLINEAGE_URL") {
        Ok(raw) if raw.eq_ignore_ascii_case("console") => {
            eprintln!("→ transport: console (events logged, not sent)");
            OpenLineageClient::new(Arc::new(ConsoleTransport))
        }
        Ok(raw) if !raw.is_empty() => {
            eprintln!("→ transport: POST {raw} (+ OPENLINEAGE_ENDPOINT)");
            OpenLineageClient::from_env().expect("build OpenLineage client from environment")
        }
        // Unset: default to the local service, bypassing env so we don't mutate it.
        _ => {
            let url = Url::parse("http://localhost:8091/api/v1/lineage").unwrap();
            eprintln!("→ transport: POST {url} (default)");
            OpenLineageClient::new(Arc::new(CloudClientTransport::unauthenticated(url)))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // So the `console` transport's `tracing` events are visible; defaults to
    // showing them, overridable via `RUST_LOG`.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "openlineage=info".into()),
        )
        .init();

    let client = client_from_env();

    // A scratch lake root for this run. Logged so you can inspect the Parquet.
    let root = std::env::temp_dir().join("headwaters-e2e");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;
    eprintln!("→ lake root: {}", root.display());

    journey::run_pipeline(&client, &root).await?;

    // Drain the queue before exit so nothing is lost. `shutdown` awaits the
    // background drain task — no post-hoc sleep needed.
    client.shutdown().await;
    eprintln!("✓ done — explore the graph in the UI (job namespaces: bronze / silver / gold)");
    Ok(())
}
