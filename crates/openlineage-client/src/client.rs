//! The OpenLineage client: a non-blocking emit front-end over a [`Transport`].
//!
//! Emission must never break or slow the host query. [`OpenLineageClient::emit`]
//! is non-blocking: it hands the event to a bounded channel drained by a
//! background task that delivers events to the transport (coalescing queued
//! events into batches when the upstream is slow) and swallows + logs any error.
//! If the channel is full the event is dropped with a warning (back-pressure must
//! not stall planning). [`OpenLineageClient::shutdown`] drains the queue and
//! flushes the transport before exit.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::event::RunEvent;
use crate::transport::{NoopTransport, Transport};

/// Default bound on the in-flight event queue.
const DEFAULT_QUEUE_SIZE: usize = 1024;

/// Cap on events coalesced into a single `emit_batch` by the drain task. Bounds
/// the per-delivery payload size while still amortizing a slow upstream.
const MAX_BATCH: usize = 256;

/// Error returned when an [`OpenLineageClient`] cannot be constructed.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The client could not be built from the given configuration or
    /// environment — for example, no Tokio runtime was available to host the
    /// drain task, or an endpoint URL was malformed.
    #[error("invalid OpenLineage configuration: {0}")]
    Config(String),
}

/// Non-blocking front-end for emitting OpenLineage events.
///
/// Must be constructed from within a Tokio runtime (it spawns a background
/// drain task); see [`OpenLineageClient::try_new`] for the non-panicking form.
#[derive(Debug, Clone)]
pub struct OpenLineageClient {
    tx: mpsc::Sender<RunEvent>,
    /// Events dropped on a full queue (back-pressure) or by transport failure.
    /// Shared across clones so the count is process-global.
    dropped: Arc<AtomicU64>,
    /// The background drain task, shared across clones. [`Self::shutdown`] takes
    /// it to await a final flush of queued events before process exit.
    drain: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// A handle to the transport, kept so [`Self::shutdown`] can flush it after
    /// the drain queue empties (the drain task owns its own clone).
    transport: Arc<dyn Transport>,
}

impl OpenLineageClient {
    /// Start a client that drains events into `transport` on a background task.
    ///
    /// # Panics
    /// Panics if called outside a Tokio runtime. Use [`Self::try_new`] to get a
    /// clear error instead.
    pub fn new(transport: Arc<dyn Transport>) -> Self {
        Self::with_queue_size(transport, DEFAULT_QUEUE_SIZE)
    }

    /// Fallible [`Self::new`]: returns [`ClientError::Config`] instead of
    /// panicking when no Tokio runtime is available to host the drain task.
    pub fn try_new(transport: Arc<dyn Transport>) -> Result<Self, ClientError> {
        Self::try_with_queue_size(transport, DEFAULT_QUEUE_SIZE)
    }

    /// [`Self::new`] with an explicit in-flight queue bound.
    ///
    /// # Panics
    /// Panics if called outside a Tokio runtime; see [`Self::try_with_queue_size`].
    pub fn with_queue_size(transport: Arc<dyn Transport>, queue_size: usize) -> Self {
        Self::try_with_queue_size(transport, queue_size)
            .expect("OpenLineageClient must be constructed within a Tokio runtime")
    }

    /// Fallible [`Self::with_queue_size`]: returns [`ClientError::Config`]
    /// instead of panicking when no Tokio runtime is available.
    ///
    /// # Errors
    /// Returns [`ClientError::Config`] if called outside a Tokio runtime.
    pub fn try_with_queue_size(
        transport: Arc<dyn Transport>,
        queue_size: usize,
    ) -> Result<Self, ClientError> {
        let handle = Handle::try_current().map_err(|_| {
            ClientError::Config(
                "OpenLineageClient must be constructed within a Tokio runtime".to_string(),
            )
        })?;
        let (tx, mut rx) = mpsc::channel::<RunEvent>(queue_size);
        let dropped = Arc::new(AtomicU64::new(0));
        let drain_dropped = dropped.clone();
        // The drain task owns one clone of the transport; `shutdown` keeps another
        // so it can flush after the queue empties.
        let drain_transport = transport.clone();
        let drain = handle.spawn(async move {
            // Drain-coalescing: block for the first event, then opportunistically
            // pull whatever else is already queued (up to MAX_BATCH) and deliver
            // it in one `emit_batch`. Under light load this is one event per call;
            // when the upstream is slow and the queue backs up, it coalesces into
            // batch deliveries — the cheapest throughput win exactly when needed.
            let mut batch: Vec<RunEvent> = Vec::new();
            while let Some(first) = rx.recv().await {
                batch.clear();
                batch.push(first);
                while batch.len() < MAX_BATCH {
                    match rx.try_recv() {
                        Ok(event) => batch.push(event),
                        Err(_) => break,
                    }
                }
                if let Err(err) = drain_transport.emit_batch(&batch).await {
                    let n = drain_dropped.fetch_add(batch.len() as u64, Ordering::Relaxed)
                        + batch.len() as u64;
                    tracing::warn!(
                        target: "openlineage",
                        error = %err,
                        batch = batch.len(),
                        dropped_total = n,
                        "failed to emit lineage events; dropping batch"
                    );
                }
            }
        });
        Ok(Self {
            tx,
            dropped,
            drain: Arc::new(Mutex::new(Some(drain))),
            transport,
        })
    }

    /// Returns a builder for configuring a client's transport and queue size.
    pub fn builder() -> OpenLineageClientBuilder {
        OpenLineageClientBuilder::default()
    }

    /// A client whose transport drops everything ([`NoopTransport`]).
    pub fn noop() -> Self {
        Self::new(Arc::new(NoopTransport))
    }

    /// Construct from the standard OpenLineage environment.
    ///
    /// If `OPENLINEAGE_URL` is set, builds an HTTP transport (requires the
    /// `http` feature); otherwise returns a no-op client. `OPENLINEAGE_API_KEY`,
    /// if present, is sent as a bearer token.
    pub fn from_env() -> Result<Self, ClientError> {
        match std::env::var("OPENLINEAGE_URL") {
            Ok(url) if !url.is_empty() => Self::http_from_env(&url),
            _ => Ok(Self::noop()),
        }
    }

    #[cfg(feature = "http")]
    fn http_from_env(url: &str) -> Result<Self, ClientError> {
        use crate::cloud::CloudClientTransport;

        let endpoint =
            std::env::var("OPENLINEAGE_ENDPOINT").unwrap_or_else(|_| "/api/v1/lineage".to_string());
        let full = url.trim_end_matches('/').to_string() + &endpoint;
        let endpoint_url = url::Url::parse(&full)
            .map_err(|e| ClientError::Config(format!("invalid OPENLINEAGE_URL/ENDPOINT: {e}")))?;

        // Honor OPENLINEAGE_TIMEOUT_MS for the per-request transport timeout.
        let timeout = crate::config::OpenLineageConfig::from_env().request_timeout;
        let cloud = match std::env::var("OPENLINEAGE_API_KEY") {
            Ok(token) if !token.is_empty() => CloudClientTransport::with_token(endpoint_url, token),
            _ => CloudClientTransport::unauthenticated(endpoint_url),
        }
        .with_timeout(timeout);
        Ok(Self::new(Arc::new(cloud)))
    }

    #[cfg(not(feature = "http"))]
    fn http_from_env(_url: &str) -> Result<Self, ClientError> {
        Err(ClientError::Config(
            "OPENLINEAGE_URL is set but the `http` feature is disabled".to_string(),
        ))
    }

    /// Emit an event without blocking. On a full queue the event is dropped
    /// with a warning — lineage never applies back-pressure to the query.
    pub fn emit(&self, event: RunEvent) {
        if let Err(err) = self.tx.try_send(event) {
            let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::warn!(
                target: "openlineage",
                error = %err,
                dropped_total = n,
                "lineage queue full or closed; dropping event"
            );
        }
    }

    /// Total events dropped so far — on a full/closed queue (back-pressure) or
    /// by transport failure. Process-global (shared across clones).
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Flush queued events, flush the transport, and stop the background drain
    /// task.
    ///
    /// Awaits the drain task to completion so events still queued at process exit
    /// are delivered rather than lost, then calls [`Transport::flush`] so a
    /// transport that buffers internally (e.g. a Kafka producer) delivers its
    /// tail before exit. The drain task ends once the event channel closes, which
    /// requires every sender to be dropped — so call this after (or while)
    /// dropping all other clones of the client; this consumes the clone it is
    /// called on. Idempotent across clones: only the clone holding the drain
    /// handle awaits it, the rest return immediately.
    pub async fn shutdown(self) {
        // Drop our sender so this clone no longer keeps the channel open.
        let Self {
            tx,
            drain,
            transport,
            ..
        } = self;
        drop(tx);
        let handle = drain.lock().unwrap().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        if let Err(err) = transport.flush().await {
            tracing::warn!(
                target: "openlineage",
                error = %err,
                "transport flush failed during shutdown",
            );
        }
    }
}

/// Builder for [`OpenLineageClient`].
///
/// Defaults to a [`NoopTransport`] and the default queue size if left unset.
#[derive(Default)]
pub struct OpenLineageClientBuilder {
    transport: Option<Arc<dyn Transport>>,
    queue_size: Option<usize>,
}

impl OpenLineageClientBuilder {
    /// Sets the transport events are drained into.
    pub fn transport(mut self, transport: Arc<dyn Transport>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Sets the bound on the in-flight event queue.
    pub fn queue_size(mut self, queue_size: usize) -> Self {
        self.queue_size = Some(queue_size);
        self
    }

    /// Builds the client, spawning its background drain task.
    ///
    /// # Panics
    /// Panics if called outside a Tokio runtime.
    pub fn build(self) -> OpenLineageClient {
        let transport = self.transport.unwrap_or_else(|| Arc::new(NoopTransport));
        OpenLineageClient::with_queue_size(transport, self.queue_size.unwrap_or(DEFAULT_QUEUE_SIZE))
    }
}
