//! Integration tests for column-level lineage extraction.
//!
//! Each case pins a behavior of the positional bottom-up resolution in
//! `src/column.rs` — several of them are regressions for the bugs that got the
//! old name-based extraction removed (fabricated datasets from aliases,
//! same-name clobbering, facet on the wrong dataset side).

mod common;

use datafusion::prelude::SessionContext;
use datafusion_openlineage::builder::start_event;
use datafusion_openlineage::config::OpenLineageConfig;
use datafusion_openlineage::context::LineageContext;
use datafusion_openlineage::extract::extract;
use serde_json::Value;
use uuid::Uuid;

/// Column-lineage extraction doesn't depend on the engine identity, so this uses
/// the plain (non-DataFusion) config in `test-ns`.
fn config() -> OpenLineageConfig {
    common::config("test-ns", false)
}

/// Plan + optimize `sql`, extract lineage, and return the START event as JSON.
async fn event_for(ctx: &SessionContext, sql: &str) -> Value {
    let plan = ctx.state().create_logical_plan(sql).await.unwrap();
    let optimized = ctx.state().optimize(&plan).unwrap();
    let lineage = extract(&optimized, &config());
    serde_json::to_value(start_event(
        Uuid::now_v7(),
        &lineage,
        &LineageContext::default(),
        &config(),
    ))
    .unwrap()
}

/// The `columnLineage` facet of the first output dataset.
fn facet(event: &Value) -> &Value {
    &event["outputs"][0]["facets"]["columnLineage"]
}

/// The `inputFields` of one output field in the facet.
fn input_fields<'a>(event: &'a Value, field: &str) -> &'a Vec<Value> {
    facet(event)["fields"][field]["inputFields"]
        .as_array()
        .unwrap_or_else(|| panic!("no inputFields for output field `{field}`: {event}"))
}

/// Whether `input_fields` contains `(table, column)` with a transformation of
/// the given type/subtype.
fn has_source(
    input_fields: &[Value],
    table: &str,
    column: &str,
    type_: &str,
    subtype: &str,
) -> bool {
    input_fields.iter().any(|f| {
        f["name"] == table
            && f["field"] == column
            && f["transformations"].as_array().is_some_and(|ts| {
                ts.iter()
                    .any(|t| t["type"] == type_ && t["subtype"] == subtype)
            })
    })
}

/// Every dataset name referenced by the facet's inputFields.
fn referenced_datasets(event: &Value) -> Vec<String> {
    facet(event)["fields"]
        .as_object()
        .unwrap()
        .values()
        .flat_map(|f| f["inputFields"].as_array().unwrap())
        .map(|f| f["name"].as_str().unwrap().to_string())
        .collect()
}

// ---------------------------------------------------------------------------
// Regressions for the removed name-based extraction
// ---------------------------------------------------------------------------

/// Old bug #1: aliases/CTEs fabricated datasets that don't exist. The alias
/// name must never appear; every source must reference the physical table.
#[tokio::test]
async fn aliases_and_ctes_do_not_fabricate_datasets() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT) AS VALUES (1)")
        .await
        .unwrap();

    for sql in [
        "CREATE TABLE out AS SELECT a FROM t x",
        "CREATE TABLE out AS WITH c AS (SELECT a FROM t) SELECT a FROM c",
    ] {
        let event = event_for(&ctx, sql).await;
        let datasets = referenced_datasets(&event);
        assert!(!datasets.is_empty(), "column lineage present: {event}");
        assert!(
            datasets.iter().all(|d| d == "t"),
            "only the physical table may appear, got {datasets:?} for {sql}"
        );
        assert!(
            has_source(input_fields(&event, "a"), "t", "a", "DIRECT", "IDENTITY"),
            "identity provenance survives the alias: {event}"
        );
    }
}

/// Old bug #2: the map was keyed by bare output-column name, so a deeper
/// same-named projection clobbered the real top-level mapping.
#[tokio::test]
async fn same_named_columns_at_different_depths_resolve_to_true_source() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    // out.a is t.a renamed twice through colliding names (a -> b -> a).
    let event = event_for(
        &ctx,
        "CREATE TABLE out AS SELECT a FROM (SELECT b AS a FROM (SELECT a AS b FROM t))",
    )
    .await;
    let fields = input_fields(&event, "a");
    assert!(
        has_source(fields, "t", "a", "DIRECT", "IDENTITY"),
        "a resolves to t.a: {event}"
    );
    assert!(
        !has_source(fields, "t", "b", "DIRECT", "IDENTITY")
            && !has_source(fields, "t", "b", "DIRECT", "TRANSFORMATION"),
        "t.b must not leak in via the colliding name: {event}"
    );
}

/// Old bug #3: the facet was attached to inputs, where the spec says consumers
/// won't look. It must be on the output dataset — and only there.
#[tokio::test]
async fn facet_is_on_outputs_not_inputs() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE src (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    let event = event_for(&ctx, "CREATE TABLE dst AS SELECT a + b AS c FROM src").await;

    assert!(
        event["inputs"][0]["facets"]["columnLineage"].is_null(),
        "no columnLineage on inputs: {event}"
    );
    let facet = facet(&event);
    assert!(facet.is_object(), "columnLineage on the output: {event}");
    assert!(facet["_producer"].is_string(), "facet carries _producer");
    assert!(facet["_schemaURL"].is_string(), "facet carries _schemaURL");
    assert!(
        has_source(
            input_fields(&event, "c"),
            "src",
            "a",
            "DIRECT",
            "TRANSFORMATION"
        ) && has_source(
            input_fields(&event, "c"),
            "src",
            "b",
            "DIRECT",
            "TRANSFORMATION"
        ),
        "computed column derives from both sources: {event}"
    );
}

// ---------------------------------------------------------------------------
// Resolution through relational operators
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transitive_resolution_through_stacked_projections() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT) AS VALUES (1)")
        .await
        .unwrap();
    let event = event_for(
        &ctx,
        "CREATE TABLE out AS SELECT x + 1 AS y FROM (SELECT a AS x FROM t)",
    )
    .await;
    assert!(
        has_source(
            input_fields(&event, "y"),
            "t",
            "a",
            "DIRECT",
            "TRANSFORMATION"
        ),
        "y <- t.a through the intermediate rename: {event}"
    );
}

#[tokio::test]
async fn self_join_disambiguates_sides_by_position() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    let event = event_for(
        &ctx,
        "CREATE TABLE out AS SELECT l.a AS la, r.b AS rb FROM t l JOIN t r ON l.a = r.b",
    )
    .await;

    let la = input_fields(&event, "la");
    assert!(
        has_source(la, "t", "a", "DIRECT", "IDENTITY"),
        "la <- t.a: {event}"
    );
    let rb = input_fields(&event, "rb");
    assert!(
        has_source(rb, "t", "b", "DIRECT", "IDENTITY"),
        "rb <- t.b: {event}"
    );
    // Join keys influence every output field indirectly.
    for field in [la, rb] {
        assert!(
            has_source(field, "t", "a", "INDIRECT", "JOIN")
                && has_source(field, "t", "b", "INDIRECT", "JOIN"),
            "join keys are INDIRECT/JOIN on every field: {event}"
        );
    }
    // Table-level dedup still holds: one input dataset.
    assert_eq!(event["inputs"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn aggregate_marks_group_keys_and_aggregations() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    let event = event_for(
        &ctx,
        "CREATE TABLE out AS SELECT a, sum(b) AS s FROM t GROUP BY a",
    )
    .await;

    let a = input_fields(&event, "a");
    assert!(
        has_source(a, "t", "a", "DIRECT", "IDENTITY"),
        "group key value: {event}"
    );
    assert!(
        has_source(a, "t", "a", "INDIRECT", "GROUP_BY"),
        "group key shapes rows: {event}"
    );
    let s = input_fields(&event, "s");
    assert!(
        has_source(s, "t", "b", "DIRECT", "AGGREGATION"),
        "sum arg: {event}"
    );
    assert!(
        has_source(s, "t", "a", "INDIRECT", "GROUP_BY"),
        "group key on agg field: {event}"
    );
}

#[tokio::test]
async fn join_predicate_and_filter_are_indirect() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    ctx.sql("CREATE TABLE u (c INT, d INT) AS VALUES (2, 3)")
        .await
        .unwrap();
    let event = event_for(
        &ctx,
        "CREATE TABLE out AS SELECT t.a FROM t JOIN u ON t.b = u.c WHERE u.d > 0",
    )
    .await;

    let a = input_fields(&event, "a");
    assert!(has_source(a, "t", "a", "DIRECT", "IDENTITY"), "{event}");
    assert!(
        has_source(a, "t", "b", "INDIRECT", "JOIN"),
        "left join key: {event}"
    );
    assert!(
        has_source(a, "u", "c", "INDIRECT", "JOIN"),
        "right join key: {event}"
    );
    assert!(
        has_source(a, "u", "d", "INDIRECT", "FILTER"),
        "filter column: {event}"
    );
}

#[tokio::test]
async fn insert_keys_facet_by_target_field_names() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    ctx.sql("CREATE TABLE dst (x INT, y INT) AS VALUES (0, 0)")
        .await
        .unwrap();

    // Reordered column list: y gets t.a, x gets t.b.
    let event = event_for(&ctx, "INSERT INTO dst (y, x) SELECT a, b FROM t").await;
    assert!(
        has_source(input_fields(&event, "y"), "t", "a", "DIRECT", "IDENTITY"),
        "y <- t.a: {event}"
    );
    assert!(
        has_source(input_fields(&event, "x"), "t", "b", "DIRECT", "IDENTITY"),
        "x <- t.b: {event}"
    );

    // Partial column list: the default-filled column has no sources and is
    // omitted from the facet.
    let event = event_for(&ctx, "INSERT INTO dst (x) SELECT a FROM t").await;
    assert!(
        has_source(input_fields(&event, "x"), "t", "a", "DIRECT", "IDENTITY"),
        "x <- t.a: {event}"
    );
    assert!(
        facet(&event)["fields"]["y"].is_null(),
        "default-filled y is omitted: {event}"
    );
}

/// UPDATE plans the same positionally-aligned projection over the target as
/// INSERT does; SET expressions resolve, untouched columns are identities.
#[tokio::test]
async fn update_resolves_set_expressions() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE dst (x INT, y INT) AS VALUES (0, 0)")
        .await
        .unwrap();
    let event = event_for(&ctx, "UPDATE dst SET x = y + 1 WHERE y > 0").await;
    assert!(
        has_source(
            input_fields(&event, "x"),
            "dst",
            "y",
            "DIRECT",
            "TRANSFORMATION"
        ),
        "x <- dst.y via SET: {event}"
    );
    assert!(
        has_source(input_fields(&event, "y"), "dst", "y", "DIRECT", "IDENTITY"),
        "untouched y carries through: {event}"
    );
    assert!(
        has_source(input_fields(&event, "x"), "dst", "y", "INDIRECT", "FILTER"),
        "WHERE column: {event}"
    );
}

// ---------------------------------------------------------------------------
// Smoke coverage for the remaining node handlers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn union_merges_sources_positionally() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    let event = event_for(
        &ctx,
        "CREATE TABLE out AS SELECT a FROM t UNION ALL SELECT b FROM t",
    )
    .await;
    let a = input_fields(&event, "a");
    assert!(
        has_source(a, "t", "a", "DIRECT", "IDENTITY")
            && has_source(a, "t", "b", "DIRECT", "IDENTITY"),
        "union field carries both branches' sources: {event}"
    );
}

#[tokio::test]
async fn window_function_args_are_aggregation_keys_are_indirect() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    let event = event_for(
        &ctx,
        "CREATE TABLE out AS SELECT a, sum(b) OVER (PARTITION BY a) AS w FROM t",
    )
    .await;
    let w = input_fields(&event, "w");
    assert!(
        has_source(w, "t", "b", "DIRECT", "AGGREGATION"),
        "window arg: {event}"
    );
    assert!(
        has_source(w, "t", "a", "INDIRECT", "WINDOW"),
        "partition key: {event}"
    );
}

#[tokio::test]
async fn distinct_on_keys_are_indirect() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    // The optimizer rewrites DISTINCT ON into an aggregate (`first_value(b)
    // GROUP BY a`), so the resolved lineage flows through the Aggregate
    // handler: b is an aggregated value, a a group key.
    let event = event_for(
        &ctx,
        "CREATE TABLE out AS SELECT DISTINCT ON (a) b FROM t ORDER BY a",
    )
    .await;
    let b = input_fields(&event, "b");
    assert!(has_source(b, "t", "b", "DIRECT", "AGGREGATION"), "{event}");
    assert!(
        has_source(b, "t", "a", "INDIRECT", "GROUP_BY"),
        "DISTINCT ON key: {event}"
    );
}

#[tokio::test]
async fn unnest_is_a_transformation() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    let event = event_for(
        &ctx,
        "CREATE TABLE out AS SELECT unnest(make_array(a, b)) AS u FROM t",
    )
    .await;
    let u = input_fields(&event, "u");
    assert!(
        has_source(u, "t", "a", "DIRECT", "TRANSFORMATION")
            && has_source(u, "t", "b", "DIRECT", "TRANSFORMATION"),
        "unnested values derive from both array elements: {event}"
    );
}

// ---------------------------------------------------------------------------
// Scope and degradation
// ---------------------------------------------------------------------------

/// Pure SELECTs have no output dataset, and the spec defines the facet on
/// outputs — so no `columnLineage` key may appear anywhere.
#[tokio::test]
async fn pure_select_emits_no_column_lineage() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    let event = event_for(&ctx, "SELECT a + b AS c FROM t").await;
    assert!(
        !serde_json::to_string(&event)
            .unwrap()
            .contains("columnLineage"),
        "no columnLineage anywhere on a read-only query: {event}"
    );
}

/// An all-literal INSERT writes a dataset but no column derives from a source;
/// an empty facet says nothing, so none is emitted. Table-level lineage stays.
#[tokio::test]
async fn literal_insert_emits_no_column_lineage() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE dst (x INT) AS VALUES (0)")
        .await
        .unwrap();
    let event = event_for(&ctx, "INSERT INTO dst VALUES (1), (2)").await;
    assert_eq!(
        event["outputs"].as_array().unwrap().len(),
        1,
        "table-level output: {event}"
    );
    assert!(
        !serde_json::to_string(&event)
            .unwrap()
            .contains("columnLineage"),
        "no facet for literal-only writes: {event}"
    );
}

/// Unresolvable plans degrade by dropping the whole facet — never guessing —
/// while table-level lineage is unaffected.
#[tokio::test]
async fn unhandled_node_degrades_whole_facet_keeps_table_level() {
    let ctx = SessionContext::new();
    // A recursive CTE's work-table scan cannot be soundly attributed (its
    // TableScan carries the CTE's own name, which is not a dataset).
    let event = event_for(
        &ctx,
        "CREATE TABLE out AS WITH RECURSIVE nums AS \
         (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 3) \
         SELECT n FROM nums",
    )
    .await;
    assert_eq!(
        event["outputs"].as_array().unwrap().len(),
        1,
        "table-level output: {event}"
    );
    assert!(
        !serde_json::to_string(&event)
            .unwrap()
            .contains("columnLineage"),
        "degraded: no columnLineage facet: {event}"
    );
}
