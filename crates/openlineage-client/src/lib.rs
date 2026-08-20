//! Transport-agnostic [OpenLineage](https://openlineage.io) event model and emit client.
//!
//! This crate owns the emission side of OpenLineage: the [`RunEvent`] model and
//! its typed facets, a pluggable [`Transport`] sink, and a non-blocking
//! [`OpenLineageClient`] that hands events to a background drain task. It has
//! **no DataFusion (or any engine) dependency** — engine integrations such as
//! [`datafusion-openlineage`](https://docs.rs/datafusion-openlineage) build on
//! top of it, and so can any other emitter.
//!
//! # The transport seam
//!
//! How events are published is deliberately unspecified. The [`Transport`] trait
//! is the only seam: an implementation might POST to a spec-compliant OpenLineage
//! REST endpoint, publish to a Kafka topic, or do anything else. The crate ships
//! [`NoopTransport`], [`ConsoleTransport`], and — behind the `http` feature —
//! [`CloudClientTransport`], which posts to an (optionally authenticated) HTTP
//! endpoint via `olai-http`. To target something else, implement [`Transport`].
//!
//! # Non-blocking emission
//!
//! [`OpenLineageClient::emit`] never blocks and never applies back-pressure: it
//! hands the event to a bounded channel drained by a background task. If the
//! queue is full the event is dropped with a warning — lineage must never slow or
//! break the host workload. Call [`OpenLineageClient::shutdown`] before exit to
//! drain queued events and flush the transport.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use openlineage_client::{ConsoleTransport, OpenLineageClient};
//!
//! # async fn run(event: openlineage_client::RunEvent) {
//! let client = OpenLineageClient::new(Arc::new(ConsoleTransport));
//! client.emit(event);          // non-blocking
//! client.shutdown().await;     // drain + flush before exit
//! # }
//! ```
//!
//! # Environment
//!
//! [`OpenLineageClient::from_env`] and [`OpenLineageConfig::from_env`] read the
//! standard OpenLineage environment variables, so an integration can wire itself
//! up from the environment the rest of the ecosystem already uses:
//!
//! | Variable | Read by | Meaning |
//! |---|---|---|
//! | `OPENLINEAGE_URL` | [`OpenLineageClient::from_env`] | Base URL of the endpoint (unset → no-op client) |
//! | `OPENLINEAGE_ENDPOINT` | [`OpenLineageClient::from_env`] | Path appended to the URL (default `/api/v1/lineage`) |
//! | `OPENLINEAGE_API_KEY` | [`OpenLineageClient::from_env`] | Bearer token, if set |
//! | `OPENLINEAGE_NAMESPACE` | [`OpenLineageConfig::from_env`] | Default job namespace |
//! | `OPENLINEAGE_TIMEOUT_MS` | [`OpenLineageConfig::from_env`] | Per-request transport timeout |
//! | `OPENLINEAGE_PARENT_ID` / `OPENLINEAGE_PARENT_*` | [`LineageContext::from_env`] | Parent run facet |
#![deny(missing_docs)]

pub mod client;
pub mod config;
pub mod context;
pub mod event;
pub mod facets;
pub mod naming;
pub mod transport;

#[cfg(feature = "http")]
pub mod cloud;

pub use client::{ClientError, OpenLineageClient, OpenLineageClientBuilder};
pub use config::{DEFAULT_NAMESPACE, DEFAULT_PRODUCER, DEFAULT_REQUEST_TIMEOUT, OpenLineageConfig};
pub use context::LineageContext;
pub use event::{Dataset, Job, Run, RunEvent, RunEventType};
pub use naming::DatasetName;
pub use transport::{ConsoleTransport, NoopTransport, Transport, TransportError};

#[cfg(feature = "http")]
pub use cloud::CloudClientTransport;
