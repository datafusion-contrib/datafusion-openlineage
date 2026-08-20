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
