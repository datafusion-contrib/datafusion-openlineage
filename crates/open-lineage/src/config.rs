//! DataFusion engine identity for [`OpenLineageConfig`].
//!
//! [`OpenLineageConfig`] itself is engine-agnostic and lives in
//! [`openlineage_client`]. This module adds [`DataFusionConfig`], an extension
//! trait that stamps the DataFusion `processing_engine` identity (name +
//! [`datafusion::DATAFUSION_VERSION`]), this integration's adapter version, and
//! a DataFusion-flavored producer.

pub use openlineage_client::OpenLineageConfig;

/// The `producer` URI stamped on events emitted by this integration.
pub const DATAFUSION_PRODUCER: &str =
    "https://github.com/datafusion-contrib/datafusion-openlineage";

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
    cfg.adapter_version = env!("CARGO_PKG_VERSION").to_string();
    cfg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datafusion_identity_is_stable() {
        for cfg in [
            OpenLineageConfig::for_datafusion(),
            OpenLineageConfig::for_datafusion_from_env(),
        ] {
            assert_eq!(cfg.producer, DATAFUSION_PRODUCER);
            assert_eq!(cfg.engine_name, "DataFusion");
            assert_eq!(cfg.engine_version, datafusion::DATAFUSION_VERSION);
            assert_eq!(cfg.adapter_version, env!("CARGO_PKG_VERSION"));
        }
    }
}
