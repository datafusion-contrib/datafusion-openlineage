//! Pluggable sink for OpenLineage events.
//!
//! Mirrors OpenLineage's own `Transport` SPI naming. The default
//! [`NoopTransport`] is used when no endpoint is configured; [`ConsoleTransport`]
//! is handy for development and tests.

use async_trait::async_trait;

use crate::event::RunEvent;

/// Error returned when a [`Transport`] fails to send an event.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The event could not be serialized to JSON.
    #[error("failed to serialize lineage event: {0}")]
    Serialize(#[from] serde_json::Error),
    /// A transport-specific delivery failure (e.g. network or backend error).
    #[error("transport error: {0}")]
    Other(String),
}

/// A sink that delivers OpenLineage events to a backend.
///
/// The only required method is [`emit`](Transport::emit). [`emit_batch`](Transport::emit_batch)
/// and [`flush`](Transport::flush) have default implementations; override them
/// when the backend can deliver events in bulk or buffers internally.
///
/// Implementations must never apply back-pressure that could stall the host
/// workload — emission happens on a background drain task, but a transport that
/// blocks indefinitely (e.g. on a hung upstream) can still starve every queued
/// event. Bound your own IO with a timeout.
#[async_trait]
pub trait Transport: std::fmt::Debug + Send + Sync {
    /// Delivers a single OpenLineage event to the backend.
    ///
    /// # Errors
    /// Returns a [`TransportError`] if the event cannot be serialized or
    /// delivered.
    async fn emit(&self, event: &RunEvent) -> Result<(), TransportError>;

    /// Delivers a batch of events. The default implementation calls
    /// [`emit`](Transport::emit) for each event in order, stopping at the first
    /// error. Override when the backend has a bulk endpoint (e.g. a batch POST).
    ///
    /// # Errors
    /// Returns a [`TransportError`] if any event cannot be serialized or
    /// delivered.
    async fn emit_batch(&self, events: &[RunEvent]) -> Result<(), TransportError> {
        for event in events {
            self.emit(event).await?;
        }
        Ok(())
    }

    /// Flushes any internally-buffered events, blocking until they are delivered
    /// (or fail). Called by [`OpenLineageClient::shutdown`](crate::OpenLineageClient::shutdown)
    /// after the drain queue empties. The default is a no-op — override it only
    /// for transports that buffer beyond a single [`emit`](Transport::emit) call.
    ///
    /// # Errors
    /// Returns a [`TransportError`] if buffered events cannot be delivered.
    async fn flush(&self) -> Result<(), TransportError> {
        Ok(())
    }
}

/// Drops events. The safe default when lineage is not configured.
#[derive(Debug, Default)]
pub struct NoopTransport;

#[async_trait]
impl Transport for NoopTransport {
    async fn emit(&self, _event: &RunEvent) -> Result<(), TransportError> {
        Ok(())
    }
}

/// Logs each event as pretty JSON via `tracing`. For development/tests.
#[derive(Debug, Default)]
pub struct ConsoleTransport;

#[async_trait]
impl Transport for ConsoleTransport {
    async fn emit(&self, event: &RunEvent) -> Result<(), TransportError> {
        let json = serde_json::to_string_pretty(event)?;
        tracing::info!(target: "openlineage", "{json}");
        Ok(())
    }
}
