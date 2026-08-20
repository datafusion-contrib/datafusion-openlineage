//! Always-on end-to-end coverage of the instrumentation path.
//!
//! This drives the *same* bronze → silver → gold journey the live demo runs
//! (the shared core in `examples/e2e_pipeline/journey.rs`), but with an
//! in-memory recording transport instead of a network sink — so it needs no
//! running `headwaters`, no Docker, and runs in the default `cargo nextest`
//! suite. Its value over the unit tests in `lineage.rs` / `column_lineage.rs`
//! (which call `extract` / `start_event` directly and never execute a plan) is
//! that it exercises the *runtime* path end-to-end: the
//! [`OpenLineageQueryPlanner`](datafusion_openlineage::OpenLineageQueryPlanner)
//! START emission, the
//! [`OpenLineageExec`](datafusion_openlineage::OpenLineageExec) terminal node's
//! COMPLETE emission with runtime statistics, and the async
//! [`OpenLineageClient`] drain. Assertions are on the *set* of captured events,
//! never on arrival order.
//!
//! The journey core is `#[path]`-included rather than depended on, because it
//! lives under `examples/` (each example/test is its own crate); see the module
//! doc on `examples/e2e_pipeline/main.rs`.

#[path = "../examples/e2e_pipeline/journey.rs"]
mod journey;

mod common;

use std::sync::Arc;

use common::RecordingTransport;
use datafusion_openlineage::event::{RunEvent, RunEventType};
use serde_json::Value;

/// Whether any output dataset named `dataset` (in any captured event) has a
/// `columnLineage` mapping for output field `field` that draws from
/// `(src_table, src_column)` with a transformation of `(type_, subtype)`.
///
/// Mirrors `column_lineage.rs::has_source`, but searches across the whole
/// captured event set and keys on qualified dataset names.
fn has_column_source(
    events: &[Value],
    dataset: &str,
    field: &str,
    src_table: &str,
    src_column: &str,
    type_: &str,
    subtype: &str,
) -> bool {
    events.iter().any(|e| {
        e["outputs"].as_array().is_some_and(|outs| {
            outs.iter().any(|o| {
                o["name"] == dataset
                    && o["facets"]["columnLineage"]["fields"][field]["inputFields"]
                        .as_array()
                        .is_some_and(|fields| {
                            fields.iter().any(|f| {
                                f["name"] == src_table
                                    && f["field"] == src_column
                                    && f["transformations"].as_array().is_some_and(|ts| {
                                        ts.iter()
                                            .any(|t| t["type"] == type_ && t["subtype"] == subtype)
                                    })
                            })
                        })
            })
        })
    })
}

/// Every output dataset name across all captured events.
fn output_names(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .flat_map(|e| e["outputs"].as_array().cloned().unwrap_or_default())
        .filter_map(|o| o["name"].as_str().map(str::to_string))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn e2e_pipeline_emits_full_lineage() {
    let transport = RecordingTransport::default();
    let seen = transport.events.clone();
    let client = datafusion_openlineage::OpenLineageClient::new(Arc::new(transport));

    // A unique per-process lake root so a concurrent `just demo` or a second
    // test run can't clobber it (the demo uses a fixed `headwaters-e2e` dir).
    let root = std::env::temp_dir().join(format!("headwaters-e2e-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let pipeline = journey::run_pipeline(&client, &root)
        .await
        .expect("pipeline runs");

    // The journey's per-stage contexts are dropped inside `run_pipeline`, so by
    // here `client` is the only sender; `shutdown` awaits the drain so every
    // queued event is delivered before we read `seen`.
    client.shutdown().await;
    let _ = std::fs::remove_dir_all(&root);

    let events = seen.lock().unwrap();

    // --- Lifecycle (rule.rs START + exec.rs COMPLETE) ---------------------
    let starts: Vec<&RunEvent> = events
        .iter()
        .filter(|e| e.event_type == RunEventType::Start)
        .collect();
    let completes: Vec<&RunEvent> = events
        .iter()
        .filter(|e| e.event_type == RunEventType::Complete)
        .collect();
    // Six lineage-bearing INSERTs: 2 bronze + 2 silver + 2 gold.
    assert!(
        starts.len() >= 6,
        "at least 6 START events, got {}",
        starts.len()
    );
    assert_eq!(
        starts.len(),
        completes.len(),
        "every START has a matching COMPLETE"
    );
    // At least one run id carries both a START and a COMPLETE (the full
    // plan→execute lifecycle, which only the runtime path produces).
    assert!(
        starts
            .iter()
            .any(|s| completes.iter().any(|c| c.run.run_id == s.run.run_id)),
        "a run id appears as both START and COMPLETE"
    );

    // Serialize once for the JSON-shaped assertions below.
    let json: Vec<Value> = events
        .iter()
        .map(|e| serde_json::to_value(e).unwrap())
        .collect();

    // --- Dataset identity (extract.rs + naming.rs) ------------------------
    let outputs = output_names(&json);
    for expected in [
        "lake.bronze.raw_orders",
        "lake.bronze.raw_customers",
        "lake.silver.orders",
        "lake.silver.orders_enriched",
        "lake.gold.revenue_by_country",
        "lake.gold.daily_orders",
    ] {
        assert!(
            outputs.iter().any(|n| n == expected),
            "output dataset {expected} present; got {outputs:?}"
        );
    }
    // Every dataset (in + out) shares the dataset namespace and carries a
    // dataSource facet naming the table (the journey's plain INSERTs are
    // appends, so no lifecycleStateChange facet — that's covered in conformance).
    for e in &json {
        for side in ["inputs", "outputs"] {
            for d in e[side].as_array().cloned().unwrap_or_default() {
                assert_eq!(
                    d["namespace"],
                    journey::DATASET_NAMESPACE,
                    "dataset uses the shared namespace: {d}"
                );
                assert_eq!(
                    d["facets"]["dataSource"]["name"], d["name"],
                    "dataset carries a dataSource facet naming it: {d}"
                );
                assert!(
                    d["facets"]["dataSource"]["uri"]
                        .as_str()
                        .is_some_and(|u| u.starts_with("datafusion:")),
                    "dataSource uri is a datafusion: URI: {d}"
                );
            }
        }
    }

    // --- Schema facets (builder.rs + facets.rs) ---------------------------
    let silver_orders_schema = json.iter().find_map(|e| {
        e["outputs"].as_array().and_then(|outs| {
            outs.iter().find_map(|o| {
                (o["name"] == "lake.silver.orders").then(|| o["facets"]["schema"]["fields"].clone())
            })
        })
    });
    let schema_fields = silver_orders_schema.expect("silver.orders has a schema facet");
    let field_names: Vec<&str> = schema_fields
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|f| f["name"].as_str())
        .collect();
    assert!(
        field_names.contains(&"amount_usd") && field_names.contains(&"country"),
        "silver.orders schema carries renamed columns: {field_names:?}"
    );

    // --- Column-lineage subtypes (column.rs, end-to-end) ------------------
    // TRANSFORMATION: CAST(amount) -> amount_usd.
    assert!(
        has_column_source(
            &json,
            "lake.silver.orders",
            "amount_usd",
            "lake.bronze.raw_orders",
            "amount",
            "DIRECT",
            "TRANSFORMATION",
        ),
        "silver.orders.amount_usd has a TRANSFORMATION from raw_orders.amount"
    );
    // FILTER: WHERE amount > 0 is an indirect influence on the output.
    assert!(
        has_column_source(
            &json,
            "lake.silver.orders",
            "amount_usd",
            "lake.bronze.raw_orders",
            "amount",
            "INDIRECT",
            "FILTER",
        ),
        "silver.orders reflects the WHERE amount > 0 FILTER influence"
    );
    // JOIN: orders_enriched joins on customer_id.
    assert!(
        has_column_source(
            &json,
            "lake.silver.orders_enriched",
            "customer_name",
            "lake.bronze.raw_customers",
            "name",
            "DIRECT",
            "IDENTITY",
        ),
        "orders_enriched.customer_name comes from raw_customers.name"
    );
    assert!(
        json.iter()
            .any(|e| e["outputs"].as_array().is_some_and(|outs| {
                outs.iter().any(|o| {
                    o["name"] == "lake.silver.orders_enriched"
                        && o["facets"]["columnLineage"]["fields"]
                            .as_object()
                            .is_some_and(|fields| {
                                fields.values().any(|f| {
                                    f["inputFields"].as_array().is_some_and(|ins| {
                                        ins.iter().any(|i| {
                                            i["transformations"].as_array().is_some_and(|ts| {
                                                ts.iter().any(|t| t["subtype"] == "JOIN")
                                            })
                                        })
                                    })
                                })
                            })
                })
            })),
        "orders_enriched carries a JOIN influence"
    );
    // AGGREGATION + GROUP_BY: revenue_by_country.
    assert!(
        has_column_source(
            &json,
            "lake.gold.revenue_by_country",
            "revenue_usd",
            "lake.silver.orders_enriched",
            "amount_usd",
            "DIRECT",
            "AGGREGATION",
        ),
        "revenue_usd is an AGGREGATION of orders_enriched.amount_usd"
    );
    assert!(
        has_column_source(
            &json,
            "lake.gold.revenue_by_country",
            "country",
            "lake.silver.orders_enriched",
            "country",
            "INDIRECT",
            "GROUP_BY",
        ),
        "revenue_by_country reflects the GROUP BY country influence"
    );

    // --- Parent-run correlation (context.rs + facets.rs) ------------------
    let parent_id = pipeline.parent_run_id.to_string();
    for e in &json {
        let parent = &e["run"]["facets"]["parent"];
        assert_eq!(
            parent["run"]["runId"], parent_id,
            "every event correlates to the pipeline parent run: {e}"
        );
        assert_eq!(parent["job"]["namespace"], "pipelines");
        assert_eq!(parent["job"]["name"], "retail_analytics");
    }

    // --- Runtime statistics on COMPLETE (exec.rs) -------------------------
    // Only the executed terminal node can populate outputStatistics.rowCount;
    // this is the assertion the plan-only unit tests can't make.
    assert!(
        json.iter().any(|e| {
            e["eventType"] == "COMPLETE"
                && e["outputs"].as_array().is_some_and(|outs| {
                    outs.iter().any(|o| {
                        o["outputFacets"]["outputStatistics"]["rowCount"]
                            .as_i64()
                            .is_some()
                    })
                })
        }),
        "a COMPLETE event carries outputStatistics.rowCount"
    );
}
