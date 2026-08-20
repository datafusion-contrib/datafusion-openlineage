# Vendored OpenLineage JSON Schemas

These are the official [OpenLineage](https://openlineage.io) JSON Schemas, vendored
so `tests/conformance.rs` can validate every emitted `RunEvent` against the spec
**offline** — no network, no Docker. They are test fixtures only; they are not part
of the published crate.

## Provenance

Downloaded verbatim from the OpenLineage repository at release tag **`1.50.0`**:

- Core run-event schema:
  `https://github.com/OpenLineage/OpenLineage/blob/1.50.0/spec/OpenLineage.json`
  (`$id`: `https://openlineage.io/spec/2-0-2/OpenLineage.json`)
- Facet schemas, from `spec/facets/<Facet>.json` at the same tag, in `facets/`.

## How references resolve

The core `OpenLineage.json` uses only internal `$ref`s (`#/$defs/...`), so it
validates the event envelope standalone. Each facet schema carries two external
`$ref`s back to `https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/{Run,Job,Dataset}Facet`;
the conformance test registers the vendored core schema under that retrieval URI so
those resolve locally.

## Emitted versions match the vendored spec

The crate stamps a specific facet schema version into each facet's `_schemaURL`
(see `src/builder.rs`, `src/exec.rs`, `src/extract.rs`, `src/context.rs`). As of
this vendoring, **every emitted version matches the latest published version**
(`1.50.0`), so `conformance.rs` validates events against exactly the versions we
advertise — not a newer or older approximation.

| Facet                                | Emitted = Vendored (1.50.0) |
| ------------------------------------ | --------------------------- |
| ProcessingEngineRunFacet             | 1-1-1                       |
| JobTypeJobFacet                      | 2-0-3                       |
| ColumnLineageDatasetFacet            | 1-2-0                       |
| OutputStatisticsOutputDatasetFacet   | 1-0-2                       |
| InputStatisticsInputDatasetFacet     | 1-0-0                       |
| SQLJobFacet                          | 1-1-0                       |
| ErrorMessageRunFacet                 | 1-0-1                       |
| ParentRunFacet                       | 1-1-0                       |
| SchemaDatasetFacet                   | 1-2-0                       |
| DatasourceDatasetFacet               | 1-0-1                       |
| LifecycleStateChangeDatasetFacet     | 1-0-1                       |

If a future spec release advances any of these, bump the emitted constant in the
source **and** re-vendor the schema here in the same change, so the two never
drift.

## Refreshing

Re-run the download against a newer tag, then re-run `cargo test -p
datafusion-openlineage --test conformance`. If a new validation failure appears,
the crate's emitted shape has diverged from the spec — fix the emitter, don't relax
the test.
