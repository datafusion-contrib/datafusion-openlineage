//! The DataFusion-specific context-provider seam.
//!
//! The context *data* ([`LineageContext`]) and its environment conventions live
//! in [`openlineage_client`]; this module adds the DataFusion-flavored
//! [`LineageContextProvider`] trait, whose [`context`](LineageContextProvider::context)
//! method receives the query's [`SessionState`]. A different engine would define
//! its own provider over its own request type.

use async_trait::async_trait;
use datafusion::execution::context::SessionState;

pub use openlineage_client::LineageContext;

/// Supplies per-query [`LineageContext`]. Implemented by the host integration.
#[async_trait]
pub trait LineageContextProvider: std::fmt::Debug + Send + Sync {
    /// Returns the [`LineageContext`] for a query, given its session state.
    async fn context(&self, session_state: &SessionState) -> LineageContext;
}

/// Returns a fixed [`LineageContext`] for every query. Use
/// `StaticContextProvider::default()` for callers with no orchestration context.
#[derive(Debug, Default)]
pub struct StaticContextProvider(pub LineageContext);

#[async_trait]
impl LineageContextProvider for StaticContextProvider {
    async fn context(&self, _session_state: &SessionState) -> LineageContext {
        self.0.clone()
    }
}
