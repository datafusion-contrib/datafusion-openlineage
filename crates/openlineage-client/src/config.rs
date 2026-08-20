//! Static configuration shared across emitted events.

use std::time::Duration;

/// Identifies the emitting integration on every emitted event/facet, and tunes
/// the emit transport.
#[derive(Debug, Clone)]
pub struct OpenLineageConfig {
    /// `producer` URI stamped on events and facets (the emitting code). Engine
    /// integrations should override this with their own identity.
    pub producer: String,
    /// Default job namespace when the context provides none
    /// (from `OPENLINEAGE_NAMESPACE`, falling back to `"default"`).
    pub job_namespace: String,
    /// Engine name for the `processing_engine` run facet. Empty unless an engine
    /// integration sets it.
    pub engine_name: String,
    /// Engine version for the `processing_engine` run facet. Empty unless an
    /// engine integration sets it.
    pub engine_version: String,
    /// This crate's version, for `openlineageAdapterVersion`.
    pub adapter_version: String,
    /// Per-request timeout applied by HTTP transports. One slow/hung upstream
    /// request can otherwise stall the shared background drain; bounding it lets
    /// the drain keep making progress. See `OPENLINEAGE_TIMEOUT_MS`.
    pub request_timeout: Duration,
}

/// Default `producer` URI for this crate. Engine integrations override it with a
/// producer that names the engine (e.g. `datafusion-openlineage`).
pub const DEFAULT_PRODUCER: &str =
    "https://github.com/open-lakehouse/headwaters/openlineage-client";

/// Default job namespace when none is configured or supplied per query.
pub const DEFAULT_NAMESPACE: &str = "default";

/// Default per-request transport timeout. A balance between tolerating a briefly
/// slow upstream and not letting one hung request starve every session's events.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

impl OpenLineageConfig {
    /// Build a config from the standard OpenLineage environment conventions.
    ///
    /// Reads `OPENLINEAGE_NAMESPACE` for the default job namespace (falling back
    /// to [`DEFAULT_NAMESPACE`]) and `OPENLINEAGE_TIMEOUT_MS` for the request
    /// timeout (falling back to [`DEFAULT_REQUEST_TIMEOUT`]); the producer/adapter
    /// identity is fixed for this crate and the engine identity is left empty (an
    /// engine integration such as `datafusion-openlineage` stamps it). This is
    /// the documented entry point for env-driven configuration — pair it with
    /// [`OpenLineageClient::from_env`](crate::OpenLineageClient::from_env), which
    /// reads `OPENLINEAGE_URL` / `OPENLINEAGE_ENDPOINT` / `OPENLINEAGE_API_KEY`
    /// for the transport, so an integration can wire itself up entirely from the
    /// environment the rest of the OpenLineage ecosystem already uses.
    ///
    /// [`Default`] is equivalent and reads the same environment; `from_env`
    /// exists to make the env dependency explicit and discoverable at call sites.
    pub fn from_env() -> Self {
        Self::with_env(
            std::env::var("OPENLINEAGE_NAMESPACE").ok(),
            std::env::var("OPENLINEAGE_TIMEOUT_MS").ok(),
        )
    }

    /// [`Self::from_env`] with the `OPENLINEAGE_NAMESPACE` / `OPENLINEAGE_TIMEOUT_MS`
    /// values injected, so the fallback logic is unit-testable without mutating
    /// process-global env. An absent or empty namespace falls back to
    /// [`DEFAULT_NAMESPACE`]; an absent/empty/unparsable timeout falls back to
    /// [`DEFAULT_REQUEST_TIMEOUT`].
    pub fn with_env(namespace: Option<String>, timeout_ms: Option<String>) -> Self {
        let request_timeout = timeout_ms
            .as_deref()
            .filter(|s| !s.is_empty())
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_REQUEST_TIMEOUT);
        Self {
            job_namespace: namespace
                .filter(|ns| !ns.is_empty())
                .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string()),
            request_timeout,
            ..Self::fixed()
        }
    }

    /// The fixed (non-environment) fields: producer and adapter identity, with the
    /// fallback namespace and default request timeout. Shared by [`Self::from_env`]
    /// and [`Default`].
    ///
    /// The engine identity (`engine_name` / `engine_version`) is left empty here —
    /// this crate is engine-agnostic. An engine integration fills it in (see e.g.
    /// `datafusion_openlineage::OpenLineageConfig::for_datafusion`).
    fn fixed() -> Self {
        Self {
            producer: DEFAULT_PRODUCER.to_string(),
            job_namespace: DEFAULT_NAMESPACE.to_string(),
            engine_name: String::new(),
            engine_version: String::new(),
            adapter_version: env!("CARGO_PKG_VERSION").to_string(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }
}

impl Default for OpenLineageConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_falls_back_when_unset() {
        let cfg = OpenLineageConfig::with_env(None, None);
        assert_eq!(cfg.job_namespace, DEFAULT_NAMESPACE);
    }

    #[test]
    fn empty_namespace_falls_back() {
        let cfg = OpenLineageConfig::with_env(Some(String::new()), None);
        assert_eq!(cfg.job_namespace, DEFAULT_NAMESPACE);
    }

    #[test]
    fn namespace_is_taken_from_env() {
        let cfg = OpenLineageConfig::with_env(Some("analytics".to_string()), None);
        assert_eq!(cfg.job_namespace, "analytics");
    }

    #[test]
    fn timeout_parses_from_env() {
        let cfg = OpenLineageConfig::with_env(None, Some("5000".to_string()));
        assert_eq!(cfg.request_timeout, Duration::from_millis(5000));
    }

    #[test]
    fn timeout_falls_back_when_unparsable() {
        let cfg = OpenLineageConfig::with_env(None, Some("not-a-number".to_string()));
        assert_eq!(cfg.request_timeout, DEFAULT_REQUEST_TIMEOUT);
    }

    #[test]
    fn fixed_identity_is_engine_neutral() {
        let cfg = OpenLineageConfig::with_env(Some("x".to_string()), None);
        assert_eq!(cfg.producer, DEFAULT_PRODUCER);
        assert_eq!(cfg.adapter_version, env!("CARGO_PKG_VERSION"));
        // Engine identity is filled in by an engine integration, not this crate.
        assert!(cfg.engine_name.is_empty());
        assert!(cfg.engine_version.is_empty());
    }
}
