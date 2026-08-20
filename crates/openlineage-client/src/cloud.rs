//! Default HTTP transport backed by [`olai_http::CloudClient`].
//!
//! `CloudClient` handles auth (bearer token, Databricks, AWS/GCP credentials,
//! or unauthenticated) and retry/backoff, so this transport works against
//! deployed, authenticated OpenLineage endpoints out of the box. It does not add
//! its own retry loop; it does bound each request with a timeout so one hung
//! upstream request cannot stall the shared background drain.

use std::time::Duration;

use async_trait::async_trait;
use olai_http::CloudClient;
use url::Url;

use crate::config::DEFAULT_REQUEST_TIMEOUT;
use crate::event::RunEvent;
use crate::transport::{Transport, TransportError};

/// Posts OpenLineage events to an HTTP endpoint via [`CloudClient`].
///
/// Single events go to `endpoint`; batches go to `batch_endpoint` (by default the
/// single endpoint with `/batch` appended, matching the OpenLineage HTTP API).
#[derive(Clone)]
pub struct CloudClientTransport {
    client: CloudClient,
    endpoint: Url,
    batch_endpoint: Url,
    timeout: Duration,
}

impl std::fmt::Debug for CloudClientTransport {
    // `CloudClient` is not `Debug`, so don't try to print it.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudClientTransport")
            .field("endpoint", &self.endpoint)
            .field("batch_endpoint", &self.batch_endpoint)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl CloudClientTransport {
    /// Use a pre-built [`CloudClient`].
    ///
    /// The batch endpoint defaults to `endpoint` with `/batch` appended; the
    /// request timeout defaults to [`DEFAULT_REQUEST_TIMEOUT`]. Override either
    /// with [`Self::with_batch_endpoint`] / [`Self::with_timeout`].
    pub fn new(client: CloudClient, endpoint: Url) -> Self {
        let batch_endpoint = default_batch_endpoint(&endpoint);
        Self {
            client,
            endpoint,
            batch_endpoint,
            timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }

    /// Authenticate with a static bearer token (e.g. `OPENLINEAGE_API_KEY`).
    pub fn with_token(endpoint: Url, token: impl ToString) -> Self {
        Self::new(CloudClient::new_with_token(token), endpoint)
    }

    /// No authentication.
    pub fn unauthenticated(endpoint: Url) -> Self {
        Self::new(CloudClient::new_unauthenticated(), endpoint)
    }

    /// Override the per-request timeout (default [`DEFAULT_REQUEST_TIMEOUT`]).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Override the batch endpoint (default: `endpoint` with `/batch` appended).
    pub fn with_batch_endpoint(mut self, batch_endpoint: Url) -> Self {
        self.batch_endpoint = batch_endpoint;
        self
    }
}

/// Append `/batch` to the endpoint's path, falling back to the endpoint itself
/// if the URL cannot be a base (so a degenerate URL still has a usable target).
fn default_batch_endpoint(endpoint: &Url) -> Url {
    let mut batch = endpoint.clone();
    if let Ok(mut segments) = batch.path_segments_mut() {
        segments.pop_if_empty().push("batch");
    }
    batch
}

#[async_trait]
impl Transport for CloudClientTransport {
    async fn emit(&self, event: &RunEvent) -> Result<(), TransportError> {
        self.client
            .post(self.endpoint.clone())
            .timeout(self.timeout)
            .json(event)
            .send()
            .await
            .map_err(|e| TransportError::Other(e.to_string()))?;
        Ok(())
    }

    async fn emit_batch(&self, events: &[RunEvent]) -> Result<(), TransportError> {
        // A single event is cheaper to deliver on the single-event endpoint.
        match events {
            [] => Ok(()),
            [event] => self.emit(event).await,
            events => {
                self.client
                    .post(self.batch_endpoint.clone())
                    .timeout(self.timeout)
                    .json(events)
                    .send()
                    .await
                    .map_err(|e| TransportError::Other(e.to_string()))?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_endpoint_appends_batch_segment() {
        let endpoint = Url::parse("http://localhost:8091/api/v1/lineage").unwrap();
        let batch = default_batch_endpoint(&endpoint);
        assert_eq!(batch.as_str(), "http://localhost:8091/api/v1/lineage/batch");
    }

    #[test]
    fn batch_endpoint_handles_trailing_slash() {
        let endpoint = Url::parse("http://localhost:8091/api/v1/lineage/").unwrap();
        let batch = default_batch_endpoint(&endpoint);
        assert_eq!(batch.as_str(), "http://localhost:8091/api/v1/lineage/batch");
    }

    fn endpoint() -> Url {
        Url::parse("http://localhost:8091/api/v1/lineage").unwrap()
    }

    #[test]
    fn new_defaults_batch_endpoint_and_timeout() {
        let t = CloudClientTransport::unauthenticated(endpoint());
        assert_eq!(t.endpoint.as_str(), "http://localhost:8091/api/v1/lineage");
        assert_eq!(
            t.batch_endpoint.as_str(),
            "http://localhost:8091/api/v1/lineage/batch",
            "batch endpoint defaults to endpoint + /batch"
        );
        assert_eq!(t.timeout, DEFAULT_REQUEST_TIMEOUT);
    }

    #[test]
    fn with_token_carries_the_same_endpoint_defaults() {
        let t = CloudClientTransport::with_token(endpoint(), "secret");
        assert_eq!(
            t.batch_endpoint.as_str(),
            "http://localhost:8091/api/v1/lineage/batch"
        );
        assert_eq!(t.timeout, DEFAULT_REQUEST_TIMEOUT);
    }

    #[test]
    fn with_timeout_and_with_batch_endpoint_override_defaults() {
        let custom_batch = Url::parse("http://localhost:8091/bulk").unwrap();
        let t = CloudClientTransport::unauthenticated(endpoint())
            .with_timeout(Duration::from_secs(5))
            .with_batch_endpoint(custom_batch.clone());
        assert_eq!(t.timeout, Duration::from_secs(5));
        assert_eq!(t.batch_endpoint, custom_batch);
        // The single-event endpoint is untouched by the batch override.
        assert_eq!(t.endpoint, endpoint());
    }

    #[tokio::test]
    async fn emit_batch_of_nothing_is_a_no_op() {
        // The empty slice must short-circuit to Ok without touching the network —
        // so this passes even though the endpoint isn't listening.
        let t = CloudClientTransport::unauthenticated(endpoint());
        assert!(t.emit_batch(&[]).await.is_ok());
    }
}
