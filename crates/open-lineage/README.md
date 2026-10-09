# datafusion-openlineage

[OpenLineage](https://openlineage.io) integration for [Apache DataFusion](https://datafusion.apache.org).

Wrap a DataFusion `SessionState`'s query planner and every query emits OpenLineage
run events — `START` at plan time, `COMPLETE` / `FAIL` at end of execution, all under
one run id — describing the query's input and output datasets, their schemas, and
column-level lineage.

## What you get

- **Table-level lineage** — input datasets (with full table schemas) and output
  datasets, extracted from the optimized `LogicalPlan`.
- **Column-level lineage** — sound, positional bottom-up resolution over the
  optimized plan (handles aliases, CTEs, self-joins, projections, joins,
  aggregations, window functions). Degrades cleanly rather than guessing.
- **Run lifecycle** — `START` / `COMPLETE` / `FAIL` correlated by a single run id,
  with terminal events fired at *end of execution* (a query that plans but errors
  mid-stream reports `FAIL`, not `COMPLETE`).
- **Runtime statistics** — rows/bytes read and written, harvested from DataFusion
  metrics and attached to the terminal event.
- **Typed facet extensions** — query-scoped factories and builders for job, run,
  dataset, input, and output facets, including automatic built-in registration.
- **Non-blocking emission** — events go through a bounded queue drained by a
  background task; lineage never stalls or fails a query.

Events are emitted against OpenLineage spec **`2-0-2`**, with facets pinned to the
latest published facet versions (see
[`tests/schemas/openlineage/README.md`](tests/schemas/openlineage/README.md)).

## Quickstart

```rust,no_run
use datafusion::execution::SessionStateBuilder;
use datafusion_openlineage::OpenLineage;

# fn wire() -> Result<(), Box<dyn std::error::Error>> {
let state = SessionStateBuilder::new_with_default_features().build();
// Reads OPENLINEAGE_URL / OPENLINEAGE_API_KEY / OPENLINEAGE_NAMESPACE; a no-op
// client if OPENLINEAGE_URL is unset.
let state = OpenLineage::builder().from_env()?.instrument(state);
// Build a SessionContext from `state` and run queries as usual.
# let _ = state;
# Ok(())
# }
```

Inject orchestration metadata (parent run, job name, custom facets, SQL text) per
query with a [`LineageContextProvider`]: `OpenLineage::builder().context(provider)`.
The lower-level `instrument_session_state` / `instrument_session_state_simple`
free functions remain for advanced cases (e.g. sharing one client across many
sessions, each with its own context provider).

## Dataset resolvers

By default, dataset identity is the logical table reference under the configured
job namespace. Register a `DatasetResolver` to supply canonical names from your
catalog or table-provider metadata:

```rust,no_run
use std::{collections::HashMap, sync::Arc};
use async_trait::async_trait;
use datafusion::{common::TableReference, prelude::SessionContext};
use datafusion_openlineage::{
    DatasetName, DatasetResolutionContext, DatasetResolver, OpenLineage,
    OpenLineageSqlExt,
};
use datafusion_openlineage::facets::SymlinkIdentifier;

// This example uses metadata supplied by the host. A resolver can also inspect
// context.source or downcast context.table_provider() to a provider it knows.
#[derive(Debug)]
struct KnownDatasets(HashMap<TableReference, DatasetName>);

#[async_trait]
impl DatasetResolver for KnownDatasets {
    async fn resolve(&self, context: &DatasetResolutionContext<'_>) -> Option<DatasetName> {
        self.0.get(context.table_ref).cloned()
    }

    // Optional: the default implementation returns an empty vector.
    async fn symlinks(&self, context: &DatasetResolutionContext<'_>) -> Vec<SymlinkIdentifier> {
        vec![SymlinkIdentifier {
            namespace: "catalog://warehouse".into(),
            name: context.table_ref.to_string(),
            type_: "TABLE".into(),
        }]
    }
}

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let resolver = KnownDatasets(HashMap::from([(
    TableReference::full("warehouse", "analytics", "orders"),
    DatasetName {
        namespace: "s3://warehouse".into(),
        name: "production/analytics/orders".into(),
    },
)]));
let context = SessionContext::new().with_lineage(
    OpenLineage::builder()
        .from_env()?
        .dataset_resolver(Arc::new(resolver)),
);
# let _ = context;
# Ok(())
# }
```

Repeated `.dataset_resolver(...)` calls append resolvers. They run in registration
order; the first `Some(DatasetName)` wins. `None` tries the next resolver, and if
none matches, the existing naming behavior is preserved. The job namespace and
job name are unaffected.

Only the winning resolver's `symlinks` method is called, with the same context.
A nonempty vector adds the standard `symlinks` facet to that input or output
dataset; an empty vector omits the facet and does not try another resolver.
Aliases are optional and never replace the canonical identity used by column
lineage. When multiple references resolve to the same dataset within an access
mode, their aliases are merged and duplicate namespace/name/type triples removed.

The context includes the logical table reference, optional `TableSource`,
`DatasetAccess::Read` or `Write`, and the fallback namespace. Scans provide their
source; DML writes provide their target; DDL targets have no source. The
`table_provider()` helper unwraps DataFusion's default table source and returns
`None` for custom sources, which can be inspected through `context.source`.

Resolvers may await metadata I/O. Keep lookups bounded, log lookup failures, and
return `None` when identity is unavailable. Resolvers should not prepare or
execute writes. For unavailable aliases, `symlinks` should return an empty vector.
Resolvers supply identities and symlinks. Use [facet builders](#facet-builders)
for additional metadata. The existing synthetic `dataSource` facet remains
unchanged.

Resolution runs before START. Each distinct table reference/source/access
combination is resolved once per extraction and shared by table and column
lineage. Symlinks are fetched once for each successful resolution. The run retains
both names and symlinks for COMPLETE or FAIL. Metadata-only scans
of `information_schema` remain excluded.

For hosts with custom planners, `extract_with_resolvers(plan, config, resolvers)`
provides asynchronous extraction without installing a query planner.
`begin_lineage_with_resolvers(client, context, config, plan, state, resolvers)`
also emits START and returns the existing `LineageHandle`. These APIs let the host
choose when to resolve a write target whose identity becomes available during
target preparation. The synchronous `extract` and the original `begin_lineage`
retain their existing behavior without resolvers.

## Facet builders

Built-in integrations and engine extensions use the same public `facet` module:

- `Facet` describes a serializable payload, its scope, map key, and schema URI.
- `FacetBuilder` separates `applies_to(context)` from `build(context, sink)`.
- `FacetBuilderFactory` asynchronously prepares owned builders once per query.
- `FacetBuilders::default().with(builder)` collects builders of different scopes.
- `FacetSink<S>::insert` accepts only payloads whose `Facet::Scope` is `S`.

Register a factory using `OpenLineage::builder().facet_factory(Arc::new(factory))`.
Factory registration appends to the default built-ins. The standard
`processing_engine` run facet is provided by a built-in factory using this same
API. To disable it, call
`.disable_facet_factory(facet::PROCESSING_ENGINE_FACTORY)`. Factory names are
stable identifiers; disabled names are skipped regardless of registration order,
and duplicate enabled names use the first registration with a warning. A
replacement for a disabled factory should use its own name.

| `Facet::Scope` | Context | Destination |
| --- | --- | --- |
| `scope::Job` | `JobFacetContext` | `job.facets` |
| `scope::Run` | `RunFacetContext` | `run.facets` |
| `scope::Dataset` | `DatasetFacetContext` | Current input/output's `facets` |
| `scope::InputDataset` | `DatasetFacetContext` | Current input's `inputFacets` |
| `scope::OutputDataset` | `DatasetFacetContext` | Current output's `outputFacets` |

Every context exposes `event.event_type` and the captured `LineageContext`.
Dataset contexts also expose the current `dataset`, its read/write `access`, and
all logical `origins` that resolved to that canonical identity. Use namespace,
name, and access to select a dataset; each sink is already bound to the exact
event entry being visited. No contribution is broadcast to other datasets.
`DatasetOrigin::table_provider()` unwraps a default DataFusion table source;
custom `TableSource` implementations remain available through `origin.source`.
DDL targets have no source, and self-joins/aliases may share one emitted dataset.

Factory preparation receives `QueryContext`: the session, logical plan, extracted
lineage, resolved datasets with origins, run ID, orchestration context, and
configuration. Retain owned metadata or `Arc` handles in the returned builders.
Bound asynchronous I/O and return an empty `FacetBuilders` when the integration
does not apply. No factory runs for queries whose lineage is suppressed.

After preparation, dispatch happens at actual emission:

1. Assemble START, then evaluate predicates and invoke matching builders.
2. Plan and execute the query.
3. Finalize COMPLETE or FAIL, including available statistics/error details, then
   evaluate predicates and invoke matching builders again.

Job/run builders run once per event. Dataset builders run once per eligible
dataset per event. `build` is never called when `applies_to` is false, and creating
the COMPLETE template does not run callbacks. Builders are reused within a query,
including planning failures, DDL, and stream cancellation. They run synchronously
on the planning/completion path and must remain fast and non-blocking. A planned
query may never execute, so terminal callbacks are not cleanup guarantees.

All matching builders run, in registration order within each scope; factories run
in registration order with built-ins first. Builders within one factory observe
the same event snapshot. The next factory sees successfully merged contributions
from earlier factories. The library stages each builder's contributions and
discards them on errors or unwinding panics while continuing with other builders
and event emission. Abort-on-panic processes cannot recover from panics.

Payloads must serialize to JSON objects and omit `_producer` and `_schemaURL`:
the sink supplies the configured producer and `Facet::SCHEMA_URL`. Use immutable,
absolute schema URIs and project-prefixed names for custom facets. The sink checks
basic shape and metadata; it does not fetch or validate external JSON Schemas.
Known facet names populate the existing typed fields. Name collisions preserve
existing values and produce warnings; malformed typed facets discard that
builder invocation's additions.

For custom planners, pass a `FacetRegistry` to
`begin_lineage_with_facets(client, context, config, plan, state, resolvers, registry)`.
The returned `LineageHandle` carries the same prepared builders into its direct
terminal methods or its plan marker. Existing entry points retain default
built-ins. Direct event-builder helpers use the built-in processing-engine
builder but do not prepare engine factories.

See [`examples/custom_facets.rs`](examples/custom_facets.rs) for a complete
factory with a run facet and a completion-only facet for one input:

```sh
cargo run -p datafusion-openlineage --example custom_facets
```

The API preserves provider associations for future shared integrations. Delta and
Iceberg integrations are not bundled yet. Scan/commit reports still require a
provider reporting API or execution hook that associates reports with the query's
actual operations; a facet builder cannot infer those reports from table identity.

## Transports

The event sink is the pluggable `Transport` trait, which lives in the
engine-agnostic [`openlineage-client`](../openlineage-client) crate (re-exported
here). A transport can target an OpenLineage REST API, a Kafka topic, or anything
else — see that crate to write your own.

| Transport               | Feature | Use                                                        |
| ----------------------- | ------- | ---------------------------------------------------------- |
| `CloudClientTransport`  | `http`  | POST to a (possibly authenticated) OpenLineage endpoint.   |
| `ConsoleTransport`      | —       | Log each event as JSON via `tracing`. Development.         |
| `NoopTransport`         | —       | Drop events. The safe default when lineage isn't wired up. |

`http` is on by default and forwards to `openlineage-client/http`, which pulls in
`olai-http` (bearer-token, Databricks, and AWS/GCP credential auth out of the box).
Disable default features to drop the HTTP stack and bring your own `Transport`.

## Correctness testing

Two layers (see [`PUBLISHING.md`](PUBLISHING.md) for how they run):

1. **Offline spec conformance** (`tests/conformance.rs`, always on) — drives the real
   emit path over a SQL matrix and validates every emitted event against the vendored
   OpenLineage JSON Schemas. No Docker, no network.
2. **Reference-backend acceptance** (`tests/marquez_acceptance.rs`, opt-in) — spins up
   [Marquez](https://marquezproject.ai), the OpenLineage reference implementation, via
   testcontainers, emits over the real HTTP transport, and asserts Marquez ingests and
   reconstructs the lineage through its own REST API. Gated behind the `marquez-it`
   feature **and** `#[ignore]`; requires Docker:

   ```sh
   cargo test -p datafusion-openlineage --features marquez-it -- --ignored
   ```

## License

Apache-2.0.
