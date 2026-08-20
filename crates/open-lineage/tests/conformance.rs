//! Offline OpenLineage spec conformance.
//!
//! Drives the real emit path over a SQL matrix and validates every captured
//! `RunEvent` against the vendored OpenLineage JSON Schemas in `tests/schemas/`
//! (see that directory's `README.md` for provenance). No network, no Docker —
//! this is the always-on gate that catches drift between what the crate emits
//! and the published spec.
//!
//! Two granularities:
//!   * every event is validated against the core `OpenLineage.json` envelope;
//!   * specific facets (columnLineage, input/output statistics) are validated
//!     against their individual facet schemas, since the core schema treats
//!     facet bodies as open `additionalProperties` and won't deep-check them.

mod common;

use std::sync::{Arc, OnceLock};

use common::RecordingTransport;
use datafusion::prelude::SessionContext;
use datafusion_openlineage::config::OpenLineageConfig;
use datafusion_openlineage::event::RunEvent;
use datafusion_openlineage::{OpenLineageClient, instrument_session_state_simple};
use jsonschema::{Registry, Resource, Validator};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Vendored schemas, loaded once.
// ---------------------------------------------------------------------------

const CORE_SCHEMA: &str = include_str!("schemas/openlineage/OpenLineage.json");
/// Retrieval URI the facet schemas `$ref` to reach the core definitions.
const CORE_URI: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json";

const SCHEMA_FACET: &str = include_str!("schemas/openlineage/facets/SchemaDatasetFacet.json");
const COLUMN_LINEAGE_FACET: &str =
    include_str!("schemas/openlineage/facets/ColumnLineageDatasetFacet.json");
const OUTPUT_STATS_FACET: &str =
    include_str!("schemas/openlineage/facets/OutputStatisticsOutputDatasetFacet.json");
const INPUT_STATS_FACET: &str =
    include_str!("schemas/openlineage/facets/InputStatisticsInputDatasetFacet.json");
const DATA_SOURCE_FACET: &str =
    include_str!("schemas/openlineage/facets/DatasourceDatasetFacet.json");
const LIFECYCLE_FACET: &str =
    include_str!("schemas/openlineage/facets/LifecycleStateChangeDatasetFacet.json");

/// The vendored core schema, registered under the retrieval URI the facet
/// schemas `$ref`. Built once; owned `Value` contents make it `'static`.
fn registry() -> &'static Registry<'static> {
    static REGISTRY: OnceLock<Registry<'static>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let core: Value = serde_json::from_str(CORE_SCHEMA).expect("core schema parses");
        Registry::new()
            .add(CORE_URI, Resource::from_contents(core))
            .expect("register core schema")
            .prepare()
            .expect("prepare registry")
    })
}

fn validator_for(schema_src: &str) -> Validator {
    let schema: Value = serde_json::from_str(schema_src).expect("schema parses");
    jsonschema::options()
        .with_registry(registry())
        .build(&schema)
        .expect("schema compiles")
}

fn core_validator() -> &'static Validator {
    static V: OnceLock<Validator> = OnceLock::new();
    V.get_or_init(|| validator_for(CORE_SCHEMA))
}

/// Assert `value` validates against `validator`, printing every error on failure.
fn assert_valid(validator: &Validator, value: &Value, label: &str) {
    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|e| e.to_string())
        .collect();
    assert!(
        errors.is_empty(),
        "{label} failed OpenLineage schema validation:\n  - {}\n--- instance ---\n{}",
        errors.join("\n  - "),
        serde_json::to_string_pretty(value).unwrap()
    );
}

/// A facet schema file wraps its facet under a single top-level property (e.g.
/// `{"properties": {"columnLineage": {...}}}`). Wrap the facet body the same way
/// so it validates against the file as published.
fn wrapped(key: &str, body: &Value) -> Value {
    serde_json::json!({ key: body })
}

// ---------------------------------------------------------------------------
// Emit harness.
// ---------------------------------------------------------------------------

/// The DataFusion-flavored config these conformance runs emit under.
fn config() -> OpenLineageConfig {
    common::config("conformance-ns", true)
}

/// Run `setup` (DDL to seed tables) then `query`, fully draining it, and return
/// every event emitted. `query` may legitimately fail (FAIL-path coverage); we
/// only require it planned and ran.
async fn capture(setup: &[&str], query: &str) -> Vec<RunEvent> {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));

    let base = SessionContext::new();
    let state = instrument_session_state_simple(base.state(), client, config());
    let ctx = SessionContext::new_with_state(state);

    for stmt in setup {
        ctx.sql(stmt).await.expect("setup DDL").collect().await.ok();
    }
    if let Ok(df) = ctx.sql(query).await {
        let _ = df.collect().await; // ok or err — both are valid coverage
    }

    // Let the background emit worker drain.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    transport.events.lock().unwrap().clone()
}

// ---------------------------------------------------------------------------
// 1. Every event over a SQL matrix validates against the core envelope.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_matrix_events_conform_to_core_schema() {
    // (setup, query) pairs spanning read / write / DDL / failure paths. Each
    // setup runs in its own session; collectively they exercise projection,
    // joins, aggregation, CTAS, INSERT, DELETE, and a runtime failure.
    let cases: Vec<(Vec<&str>, &str)> = vec![
        (
            vec!["CREATE TABLE t (a INT, b INT) AS VALUES (1, 2), (3, 4)"],
            "SELECT a, b FROM t",
        ),
        (
            vec!["CREATE TABLE t (a INT, b INT, c INT) AS VALUES (1, 2, 3)"],
            "SELECT a FROM t",
        ),
        (
            vec!["CREATE TABLE t (a INT) AS VALUES (1), (2), (3)"],
            "SELECT count(*), sum(a) FROM t",
        ),
        (
            vec!["CREATE TABLE t (a INT, g INT) AS VALUES (1, 0), (2, 0), (3, 1)"],
            "SELECT g, sum(a) AS s FROM t GROUP BY g",
        ),
        (
            vec!["CREATE TABLE t (a INT) AS VALUES (1), (2)"],
            "SELECT l.a FROM t l JOIN t r ON l.a = r.a",
        ),
        (
            vec!["CREATE TABLE src (a INT) AS VALUES (1), (2), (3)"],
            "CREATE TABLE dst AS SELECT a, a + 1 AS b FROM src",
        ),
        (
            vec![
                "CREATE TABLE src (a INT) AS VALUES (1), (2), (3)",
                "CREATE TABLE dst (a INT) AS VALUES (0)",
            ],
            "INSERT INTO dst SELECT a FROM src",
        ),
        (
            vec!["CREATE TABLE t (a INT) AS VALUES (1), (2)"],
            "DELETE FROM t WHERE a = 1",
        ),
        (
            // Division by zero: a runtime failure that must still emit a
            // spec-valid FAIL event (errorMessage facet attached).
            vec!["CREATE TABLE t (a INT) AS VALUES (1), (0)"],
            "SELECT 10 / a AS r FROM t",
        ),
    ];

    let mut total = 0usize;
    for (setup, query) in cases {
        let events = capture(&setup, query).await;
        assert!(
            !events.is_empty(),
            "query produced at least one event: {query}"
        );
        for event in &events {
            let json = serde_json::to_value(event).unwrap();
            assert_valid(core_validator(), &json, &format!("event for `{query}`"));
            total += 1;
        }
    }
    assert!(total >= 9, "exercised a representative number of events");
}

// ---------------------------------------------------------------------------
// 2. The columnLineage facet validates against its own schema.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn column_lineage_facet_conforms() {
    let validator = validator_for(COLUMN_LINEAGE_FACET);

    // An INSERT with a derived column produces a columnLineage facet on the
    // output. (DataFusion intercepts in-memory `CREATE TABLE AS` before physical
    // planning, so via plain `sql()` its output never reaches the instrumented
    // planner; the `OpenLineageSqlExt::sql_with_lineage` entry point handles that
    // case. An INSERT is used here because it flows an output dataset end-to-end
    // through the ordinary planner path with no special entry point.)
    let events = capture(
        &[
            "CREATE TABLE src (a INT, b INT) AS VALUES (1, 2), (3, 4)",
            "CREATE TABLE dst (a INT, s INT) AS VALUES (0, 0)",
        ],
        "INSERT INTO dst SELECT a, a + b AS s FROM src",
    )
    .await;

    let mut checked = false;
    for event in &events {
        for output in &event.outputs {
            if let Some(cl) = &output.facets.column_lineage {
                let body = serde_json::to_value(cl).unwrap();
                assert_valid(
                    &validator,
                    &wrapped("columnLineage", &body),
                    "columnLineage facet",
                );
                checked = true;
            }
        }
    }
    assert!(checked, "a columnLineage facet was emitted and validated");
}

// ---------------------------------------------------------------------------
// 3. Input/output statistics facets validate against their schemas.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn output_statistics_facet_conforms() {
    let validator = validator_for(OUTPUT_STATS_FACET);

    let events = capture(
        &[
            "CREATE TABLE src (a INT) AS VALUES (1), (2), (3)",
            "CREATE TABLE dst (a INT) AS VALUES (0)",
        ],
        "INSERT INTO dst SELECT a FROM src",
    )
    .await;

    let mut checked = false;
    for event in &events {
        for output in &event.outputs {
            if let Some(stats) = output
                .output_facets
                .as_ref()
                .and_then(|f| f.output_statistics.as_ref())
            {
                let body = serde_json::to_value(stats).unwrap();
                assert_valid(
                    &validator,
                    &wrapped("outputStatistics", &body),
                    "outputStatistics facet",
                );
                checked = true;
            }
        }
    }
    assert!(
        checked,
        "an outputStatistics facet was emitted and validated"
    );
}

#[tokio::test]
async fn data_source_facet_conforms() {
    let validator = validator_for(DATA_SOURCE_FACET);

    // A read emits a dataSource facet on the input; an INSERT emits one on the
    // output too. One query exercises both sides.
    let events = capture(
        &[
            "CREATE TABLE src (a INT) AS VALUES (1), (2)",
            "CREATE TABLE dst (a INT) AS VALUES (0)",
        ],
        "INSERT INTO dst SELECT a FROM src",
    )
    .await;

    let mut checked = false;
    for event in &events {
        for ds in event.inputs.iter().chain(event.outputs.iter()) {
            if let Some(source) = &ds.facets.data_source {
                let body = serde_json::to_value(source).unwrap();
                assert_valid(
                    &validator,
                    &wrapped("dataSource", &body),
                    "dataSource facet",
                );
                checked = true;
            }
        }
    }
    assert!(checked, "a dataSource facet was emitted and validated");
}

#[tokio::test]
async fn lifecycle_state_change_facet_conforms() {
    let validator = validator_for(LIFECYCLE_FACET);

    // An INSERT OVERWRITE rewrites the output table, which maps to the spec's
    // `OVERWRITE` state change (a plain append carries no lifecycle facet). The
    // state change rides on the output dataset.
    let events = capture(
        &[
            "CREATE TABLE src (a INT) AS VALUES (1), (2)",
            "CREATE TABLE dst (a INT) AS VALUES (0)",
        ],
        "INSERT OVERWRITE dst SELECT a FROM src",
    )
    .await;

    let mut checked = false;
    for event in &events {
        for output in &event.outputs {
            if let Some(lifecycle) = &output.facets.lifecycle_state_change {
                let body = serde_json::to_value(lifecycle).unwrap();
                assert_valid(
                    &validator,
                    &wrapped("lifecycleStateChange", &body),
                    "lifecycleStateChange facet",
                );
                checked = true;
            }
        }
    }
    assert!(
        checked,
        "a lifecycleStateChange facet was emitted and validated"
    );
}

#[tokio::test]
async fn schema_facet_conforms() {
    let validator = validator_for(SCHEMA_FACET);

    let events = capture(
        &["CREATE TABLE t (a INT, b INT, c INT) AS VALUES (1, 2, 3)"],
        "SELECT a, b FROM t",
    )
    .await;

    let mut checked = false;
    for event in &events {
        for input in &event.inputs {
            if let Some(schema) = &input.facets.schema {
                let body = serde_json::to_value(schema).unwrap();
                assert_valid(&validator, &wrapped("schema", &body), "schema facet");
                checked = true;
            }
        }
    }
    assert!(checked, "a schema facet was emitted and validated");
}

// Reference the input-statistics schema constant so an accidental rename of the
// vendored file is caught at compile time even though file-source statistics are
// covered by the more involved Parquet test in `lineage.rs`.
#[test]
fn input_statistics_schema_compiles() {
    let _ = validator_for(INPUT_STATS_FACET);
}
