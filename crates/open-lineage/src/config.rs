//! DataFusion engine identity for [`OpenLineageConfig`].
//!
//! [`OpenLineageConfig`] itself is engine-agnostic and lives in
//! [`openlineage_client`]. This module adds [`DataFusionConfig`], an extension
//! trait that stamps the DataFusion `processing_engine` identity (name +
//! [`datafusion::DATAFUSION_VERSION`]) and a DataFusion-flavored producer.

pub use openlineage_client::OpenLineageConfig;

/// The `producer` URI stamped on events emitted by this integration.
pub const DATAFUSION_PRODUCER: &str =
    "https://github.com/open-lakehouse/headwaters/datafusion-openlineage";

/// Builds [`OpenLineageConfig`] with the DataFusion `processing_engine` identity.
pub trait DataFusionConfig {
    /// A config with the DataFusion producer/engine identity and default
    /// namespace.
    fn for_datafusion() -> Self;

    /// [`Self::for_datafusion`] but reading `OPENLINEAGE_NAMESPACE` /
    /// `OPENLINEAGE_TIMEOUT_MS` from the environment for the namespace and
    /// transport timeout (the engine identity is still fixed).
    fn for_datafusion_from_env() -> Self;
}

impl DataFusionConfig for OpenLineageConfig {
    fn for_datafusion() -> Self {
        stamp_datafusion(OpenLineageConfig {
            producer: DATAFUSION_PRODUCER.to_string(),
            ..OpenLineageConfig::with_env(None, None)
        })
    }

    fn for_datafusion_from_env() -> Self {
        stamp_datafusion(OpenLineageConfig {
            producer: DATAFUSION_PRODUCER.to_string(),
            ..OpenLineageConfig::from_env()
        })
    }
}

/// Fill in the DataFusion `processing_engine` identity: this integration is the
/// adapter, DataFusion is the engine, so report DataFusion's version as the
/// engine version (the crate version stays the adapter version).
fn stamp_datafusion(mut cfg: OpenLineageConfig) -> OpenLineageConfig {
    cfg.engine_name = "DataFusion".to_string();
    cfg.engine_version = datafusion::DATAFUSION_VERSION.to_string();
    cfg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datafusion_identity_is_stable() {
        let cfg = OpenLineageConfig::for_datafusion();
        assert_eq!(cfg.producer, DATAFUSION_PRODUCER);
        assert_eq!(cfg.engine_name, "DataFusion");
        // `engine_version` is DataFusion's version, stamped by this crate.
        assert!(!cfg.engine_version.is_empty());
        // Note: `adapter_version` is owned and stamped by `openlineage-client`
        // (its own CARGO_PKG_VERSION) and covered by that crate's tests — it is
        // *not* this crate's version, so we don't assert it here.
    }
}
