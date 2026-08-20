//! OpenLineage run-event envelope.
//!
//! See <https://openlineage.io/docs/spec/object-model> and the core
//! `OpenLineage.json` spec.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::facets::{DatasetFacets, InputDatasetFacets, JobFacets, OutputDatasetFacets, RunFacets};

/// URL of the OpenLineage run-event schema this crate emits against.
pub const RUN_EVENT_SCHEMA_URL: &str =
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";

/// Lifecycle stage of a run, per the OpenLineage spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum RunEventType {
    /// The run has started.
    Start,
    /// The run is in progress.
    Running,
    /// The run finished successfully.
    Complete,
    /// The run was aborted before completing.
    Abort,
    /// The run failed.
    Fail,
    /// A lifecycle event that doesn't fit the other variants.
    Other,
}

/// A single OpenLineage run event.
///
/// All events for one query share a constant [`Run::run_id`]; `eventType`
/// distinguishes START from COMPLETE/FAIL.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEvent {
    /// Lifecycle stage this event reports.
    pub event_type: RunEventType,
    /// RFC3339 timestamp.
    pub event_time: String,
    /// The run this event belongs to.
    pub run: Run,
    /// The job the run is an execution of.
    pub job: Job,
    /// Datasets consumed by the run.
    #[serde(default)]
    pub inputs: Vec<Dataset>,
    /// Datasets produced by the run.
    #[serde(default)]
    pub outputs: Vec<Dataset>,
    /// URI identifying the software that emitted the event.
    pub producer: String,
    /// URL of the OpenLineage schema this event conforms to.
    #[serde(rename = "schemaURL")]
    pub schema_url: String,
}

/// A single execution of a [`Job`], stable across all its events.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Run {
    /// Unique identifier shared by every event of this run.
    pub run_id: Uuid,
    /// Run-level facets carrying additional metadata.
    #[serde(default)]
    pub facets: RunFacets,
}

/// A process definition that runs are executions of.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Namespace the job belongs to.
    pub namespace: String,
    /// Job name, unique within its namespace.
    pub name: String,
    /// Job-level facets carrying additional metadata.
    #[serde(default)]
    pub facets: JobFacets,
}

/// A dataset consumed or produced by a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dataset {
    /// Namespace the dataset belongs to.
    pub namespace: String,
    /// Dataset name, unique within its namespace.
    pub name: String,
    /// Dataset-level facets carrying additional metadata.
    #[serde(default)]
    pub facets: DatasetFacets,
    /// Input-only facets (e.g. runtime statistics). Serialized as `inputFacets`
    /// and only present on input datasets.
    #[serde(
        rename = "inputFacets",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub input_facets: Option<InputDatasetFacets>,
    /// Output-only facets (e.g. runtime statistics). Serialized as
    /// `outputFacets` and only present on output datasets.
    #[serde(
        rename = "outputFacets",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub output_facets: Option<OutputDatasetFacets>,
}
