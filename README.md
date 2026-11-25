# datafusion-openlineage

[![CI](https://github.com/datafusion-contrib/datafusion-openlineage/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/datafusion-contrib/datafusion-openlineage/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/datafusion-contrib/datafusion-openlineage/graph/badge.svg)](https://codecov.io/gh/datafusion-contrib/datafusion-openlineage)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/datafusion-contrib/datafusion-openlineage/badge)](https://scorecard.dev/viewer/?uri=github.com/datafusion-contrib/datafusion-openlineage)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

[OpenLineage](https://openlineage.io) integration for
[Apache DataFusion](https://datafusion.apache.org). Instrument a DataFusion
session and every query emits OpenLineage run events (`START`, then `COMPLETE`
or `FAIL`) describing its input and output datasets, their schemas, column-level
lineage, and runtime statistics.

## Crates

| Crate | | Description |
|---|---|---|
| [`datafusion-openlineage`](crates/open-lineage) | [![crates.io](https://img.shields.io/crates/v/datafusion-openlineage.svg)](https://crates.io/crates/datafusion-openlineage) [![docs.rs](https://img.shields.io/docsrs/datafusion-openlineage)](https://docs.rs/datafusion-openlineage) | Lineage extraction and run lifecycle for DataFusion sessions. |
| [`openlineage-client`](crates/openlineage-client) | [![crates.io](https://img.shields.io/crates/v/openlineage-client.svg)](https://crates.io/crates/openlineage-client) [![docs.rs](https://img.shields.io/docsrs/openlineage-client)](https://docs.rs/openlineage-client) | Engine-agnostic OpenLineage event model, `Transport` trait, and non-blocking emit client. |

## Quickstart

```toml
[dependencies]
datafusion-openlineage = "0.0"
```

```rust,no_run
use datafusion::execution::SessionStateBuilder;
use datafusion_openlineage::OpenLineage;

fn wire() -> Result<(), Box<dyn std::error::Error>> {
    let state = SessionStateBuilder::new_with_default_features().build();
    // Reads OPENLINEAGE_URL / OPENLINEAGE_API_KEY / OPENLINEAGE_NAMESPACE.
    // Without OPENLINEAGE_URL the client is a no-op.
    let state = OpenLineage::builder().from_env()?.instrument(state);
    // Build a SessionContext from `state` and run queries as usual.
    let _ = state;
    Ok(())
}
```

See the [`datafusion-openlineage` README](crates/open-lineage/README.md) for
dataset resolvers, facet builders, and driving the query lifecycle yourself.
To try it end to end, `just demo` runs a bronze → silver → gold pipeline
(`OPENLINEAGE_URL=console just demo` prints the events instead of sending them).

## Compatibility

| | |
|---|---|
| DataFusion | 56 |
| OpenLineage spec | `2-0-2` (facet schemas from OpenLineage `1.50.0`) |
| MSRV | Rust 1.95 |

The crates are still at `0.0.x`, so Cargo treats every release as potentially
breaking. Breaking changes are called out in each crate's `CHANGELOG.md`, and
each DataFusion major upgrade is one of them.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the
development setup, checks, and PR conventions, and
[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md). Report security issues privately as
described in [SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
