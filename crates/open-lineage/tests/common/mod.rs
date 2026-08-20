//! Shared scaffolding for the `datafusion-openlineage` integration tests.
//!
//! Each integration test is its own crate and includes this module via `mod
//! common;`, so a given test compiles helpers it doesn't use (e.g.
//! `column_lineage.rs` only needs `config`). `allow(dead_code)` keeps those
//! per-crate "unused" warnings — which are false positives across the suite —
//! from tripping `-D warnings`.
#![allow(dead_code)]

//!
//! The recording/failing transports and the config builder were independently
//! re-implemented in `lineage.rs`, `conformance.rs`, `e2e_pipeline.rs`, and
//! `column_lineage.rs`; this collapses them to one copy. Network-bound or
//! feature-gated transports (e.g. the Marquez `TeeTransport`) deliberately stay
//! in their own test — they forward over HTTP and aren't reusable here.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion_openlineage::DataFusionConfig;
use datafusion_openlineage::config::OpenLineageConfig;
use datafusion_openlineage::event::RunEvent;
use datafusion_openlineage::transport::{Transport, TransportError};

/// A network-free [`Transport`] that records every emitted event for the test to
/// assert on. Cloning shares the same buffer, so a clone handed to the client
/// still sees what the client emits.
///
/// `events` is public so callers can `transport.events.lock().unwrap()` for
/// in-place assertions; [`events`](Self::events) is the convenience snapshot.
#[derive(Debug, Default, Clone)]
pub struct RecordingTransport {
    pub events: Arc<Mutex<Vec<RunEvent>>>,
}

impl RecordingTransport {
    /// A snapshot of the events captured so far, in arrival order.
    pub fn events(&self) -> Vec<RunEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait]
impl Transport for RecordingTransport {
    async fn emit(&self, event: &RunEvent) -> Result<(), TransportError> {
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

/// A transport that always errors — to prove emit failures are swallowed and
/// never surface to the caller.
#[derive(Debug)]
pub struct FailingTransport;

#[async_trait]
impl Transport for FailingTransport {
    async fn emit(&self, _event: &RunEvent) -> Result<(), TransportError> {
        Err(TransportError::Other("boom".to_string()))
    }
}

/// A config in `namespace`. With `datafusion = true` it carries the DataFusion
/// producer/engine identity ([`OpenLineageConfig::for_datafusion`]); otherwise it
/// starts from [`OpenLineageConfig::default`] (column-lineage extraction doesn't
/// depend on the engine identity).
pub fn config(namespace: &str, datafusion: bool) -> OpenLineageConfig {
    let base = if datafusion {
        OpenLineageConfig::for_datafusion()
    } else {
        OpenLineageConfig::default()
    };
    OpenLineageConfig {
        job_namespace: namespace.to_string(),
        ..base
    }
}
