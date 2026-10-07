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
This API supplies identities and symlinks; it does not supply arbitrary dataset
facets or refresh metadata after execution. The existing synthetic `dataSource`
facet remains unchanged.

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
