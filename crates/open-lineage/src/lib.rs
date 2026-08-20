//! OpenLineage integration for Apache DataFusion.
//!
//! Instrument a [`SessionState`](datafusion::execution::context::SessionState)
//! with [`OpenLineage::builder`] to emit [OpenLineage](https://openlineage.io)
//! run events (START / COMPLETE / FAIL) describing each query's input/output
//! datasets and column-level lineage. Planning-time work (lineage extraction,
//! context, START) runs in a [`QueryPlanner`](crate::rule::OpenLineageQueryPlanner);
//! the terminal COMPLETE/FAIL node is installed by a registered
//! [`ExtensionPlanner`](crate::rule::LineageExtensionPlanner) that lowers a
//! plan-carried marker — see the [`rule`] module and ADR 0005.
//!
//! The event model, the pluggable [`Transport`] seam, and the non-blocking
//! [`OpenLineageClient`] live in the engine-agnostic
//! [`openlineage_client`] crate; this crate re-exports them and adds the
//! DataFusion-specific glue. Orchestration metadata (parent run, job ids, custom
//! facets) is injected per query via a [`LineageContextProvider`].
//!
//! # Quickstart
//!
//! ```no_run
//! use datafusion::execution::SessionStateBuilder;
//! use datafusion_openlineage::OpenLineage;
//!
//! # fn wire() -> Result<(), Box<dyn std::error::Error>> {
//! let state = SessionStateBuilder::new_with_default_features().build();
//! // Reads OPENLINEAGE_URL / _API_KEY / _NAMESPACE / parent-run env vars.
//! let state = OpenLineage::builder().from_env()?.instrument(state);
//! # let _ = state;
//! # Ok(())
//! # }
//! ```
#![deny(missing_docs)]

// Event builders. Reachable for integration tests that assert on the emitted
// event shape, but not part of the advertised API — callers instrument a
// session rather than building events by hand.
#[doc(hidden)]
pub mod builder;
pub mod column;
pub mod config;
pub mod context;
pub mod exec;
pub mod extract;
pub mod rule;
pub mod session;

// Re-export the engine-agnostic emission surface so the flat
// `datafusion_openlineage::{RunEvent, Transport, OpenLineageClient, ...}` paths
// keep working and callers need not depend on `openlineage-client` directly.
pub use openlineage_client::{
    ClientError, ConsoleTransport, Dataset, DatasetName, Job, LineageContext, NoopTransport,
    OpenLineageClient, OpenLineageClientBuilder, OpenLineageConfig, Run, RunEvent, RunEventType,
    Transport, TransportError, client, event, facets, naming, transport,
};
#[cfg(feature = "http")]
pub use openlineage_client::{CloudClientTransport, cloud};

pub use config::DataFusionConfig;
pub use context::{LineageContextProvider, StaticContextProvider};
pub use exec::OpenLineageExec;
pub use extract::{QueryLineage, extract};
pub use rule::{
    LineageExtensionPlanner, LineageHandle, LineageMarker, OpenLineageQueryPlanner, begin_lineage,
};
pub use session::{
    OpenLineage, OpenLineageBuilder, OpenLineageSqlExt, instrument_session_state,
    instrument_session_state_simple,
};
