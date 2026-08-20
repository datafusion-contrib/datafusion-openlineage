# openlineage-client

A transport-agnostic [OpenLineage](https://openlineage.io) event model and emit
client for Rust.

This crate owns the emission side of OpenLineage: the `RunEvent` model and its
typed facets, a pluggable `Transport` sink, and a non-blocking `OpenLineageClient`
that hands events to a background drain task. Engine integrations such as
[`datafusion-openlineage`](https://docs.rs/datafusion-openlineage) build on top
of it — and so can any other emitter or transport.

## The transport seam

How events are published is deliberately unspecified. The `Transport` trait is the
only seam:

```rust
use async_trait::async_trait;
use openlineage_client::{RunEvent, Transport, TransportError};

#[derive(Debug)]
struct MyTransport;

#[async_trait]
impl Transport for MyTransport {
    async fn emit(&self, event: &RunEvent) -> Result<(), TransportError> {
        // POST to a REST API, publish to Kafka, write to a file — your choice.
        Ok(())
    }
    // `emit_batch` and `flush` have default impls; override them when the backend
    // can deliver in bulk or buffers internally.
}
```

Built-in transports: `NoopTransport`, `ConsoleTransport`, and — behind the `http`
feature (on by default) — `CloudClientTransport`, which POSTs to an (optionally
authenticated) endpoint.

## Non-blocking emission

`OpenLineageClient::emit` never blocks and never applies back-pressure: it hands
the event to a bounded channel drained by a background task. If the queue is full
the event is dropped with a warning — lineage must never slow or break the host
workload. The drain coalesces queued events into batched deliveries when the
upstream is slow. Call `shutdown().await` before exit to drain the queue and flush
the transport.

```rust,no_run
use std::sync::Arc;
use openlineage_client::{ConsoleTransport, OpenLineageClient};

# async fn run(event: openlineage_client::RunEvent) {
let client = OpenLineageClient::new(Arc::new(ConsoleTransport));
client.emit(event);          // non-blocking
client.shutdown().await;     // drain + flush before exit
# }
```

## Environment

`OpenLineageClient::from_env` and `OpenLineageConfig::from_env` read the standard
OpenLineage environment variables (`OPENLINEAGE_URL`, `OPENLINEAGE_ENDPOINT`,
`OPENLINEAGE_API_KEY`, `OPENLINEAGE_NAMESPACE`, `OPENLINEAGE_TIMEOUT_MS`, and the
`OPENLINEAGE_PARENT_*` parent-run variables).

## License

Apache-2.0.
