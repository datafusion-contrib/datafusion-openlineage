//! OpenLineage facet types.
//!
//! Every facet embeds a [`BaseFacet`] carrying the spec-mandated `_producer` and
//! `_schemaURL` fields (underscore-prefixed to avoid collisions with the facet
//! payload). Shapes follow the OpenLineage spec; see
//! <https://openlineage.io/docs/spec/facets/>.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Common fields present on every OpenLineage facet.
///
/// Flattened into each concrete facet so the serialized JSON carries the
/// required `_producer` and `_schemaURL` keys.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseFacet {
    /// URI identifying the software that produced this facet. Maps to the
    /// `_producer` spec field.
    #[serde(rename = "_producer")]
    pub producer: String,
    /// URI of the JSON schema this facet conforms to. Maps to the `_schemaURL`
    /// spec field.
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
}

impl BaseFacet {
    /// Build a [`BaseFacet`] for `producer` pointing at the facet schema named
    /// `schema` (e.g. `1-2-0/SchemaDatasetFacet.json`).
    pub fn new(producer: &str, schema: &str) -> Self {
        Self {
            producer: producer.to_string(),
            schema_url: format!("https://openlineage.io/spec/facets/{schema}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Run facets
// ---------------------------------------------------------------------------

/// Bag of run facets. Known facets are typed; anything supplied by a
/// [`crate::context::LineageContext`] flows through `extra`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunFacets {
    /// Identifies the engine executing the run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing_engine: Option<ProcessingEngineRunFacet>,
    /// Links the run to its parent run and job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<ParentRunFacet>,
    /// The nominal (scheduled) time window for the run. Maps to the
    /// `nominalTime` spec field.
    #[serde(rename = "nominalTime", skip_serializing_if = "Option::is_none")]
    pub nominal_time: Option<NominalTimeRunFacet>,
    /// Failure details for a failed run. Maps to the `errorMessage` spec field.
    #[serde(rename = "errorMessage", skip_serializing_if = "Option::is_none")]
    pub error_message: Option<ErrorMessageRunFacet>,
    /// Additional custom run facets passed through verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// The facet a query engine is expected to populate to identify itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingEngineRunFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// Version of the processing engine.
    pub version: String,
    /// Name of the processing engine (e.g. `spark`, `airflow`).
    pub name: String,
    /// Version of the OpenLineage adapter or integration in use. Maps to the
    /// `openlineageAdapterVersion` spec field.
    #[serde(rename = "openlineageAdapterVersion")]
    pub openlineage_adapter_version: String,
}

/// Links this run to a parent (and optionally root) run/job — set by orchestrators.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentRunFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// The immediate parent run.
    pub run: ParentRun,
    /// The immediate parent job.
    pub job: ParentJob,
    /// The root run/job at the top of the parent chain, when distinct from the
    /// immediate parent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root: Option<RootParent>,
}

/// Reference to a parent run by its run identifier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentRun {
    /// Unique identifier of the parent run. Maps to the `runId` spec field.
    #[serde(rename = "runId")]
    pub run_id: String,
}

/// Reference to a parent job by namespace and name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentJob {
    /// Namespace the parent job belongs to.
    pub namespace: String,
    /// Name of the parent job.
    pub name: String,
}

/// The root run and job at the top of a multi-level parent chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootParent {
    /// The root run.
    pub run: ParentRun,
    /// The root job.
    pub job: ParentJob,
}

/// The nominal (scheduled) start and end times of a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NominalTimeRunFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// Scheduled start time as an ISO-8601 timestamp. Maps to the
    /// `nominalStartTime` spec field.
    #[serde(rename = "nominalStartTime")]
    pub nominal_start_time: String,
    /// Scheduled end time as an ISO-8601 timestamp, if known. Maps to the
    /// `nominalEndTime` spec field.
    #[serde(rename = "nominalEndTime", skip_serializing_if = "Option::is_none")]
    pub nominal_end_time: Option<String>,
}

/// Failure details attached to a failed run (the `errorMessage` run facet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMessageRunFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// Human-readable error message.
    pub message: String,
    /// Programming language the error originated from. Maps to the
    /// `programmingLanguage` spec field.
    #[serde(rename = "programmingLanguage")]
    pub programming_language: String,
    /// Full stack trace, if available. Maps to the `stackTrace` spec field.
    #[serde(rename = "stackTrace", skip_serializing_if = "Option::is_none")]
    pub stack_trace: Option<String>,
}

// ---------------------------------------------------------------------------
// Job facets
// ---------------------------------------------------------------------------

/// Bag of job facets. Known facets are typed; anything else flows through
/// `extra`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobFacets {
    /// The SQL query backing the job, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<SqlJobFacet>,
    /// Classification of the job (batch/stream, integration, type). Maps to the
    /// `jobType` spec field.
    #[serde(rename = "jobType", skip_serializing_if = "Option::is_none")]
    pub job_type: Option<JobTypeJobFacet>,
    /// Additional custom job facets passed through verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// The SQL query executed by a job (the `sql` job facet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlJobFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// The SQL query text.
    pub query: String,
}

/// Classifies a job by processing mode, integration, and type (the `jobType`
/// job facet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobTypeJobFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// Processing mode, typically `BATCH` or `STREAMING`. Maps to the
    /// `processingType` spec field.
    #[serde(rename = "processingType")]
    pub processing_type: String,
    /// The integration that produced the job (e.g. `SPARK`, `AIRFLOW`).
    pub integration: String,
    /// Integration-specific job type (e.g. `QUERY`, `JOB`). Maps to the
    /// `jobType` spec field.
    #[serde(rename = "jobType")]
    pub job_type: String,
}

/// Free-text description of a job (the `documentation` job facet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentationJobFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// Free-text description of the job.
    pub description: String,
}

/// Who owns a job (the `ownership` job facet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwnershipJobFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// The owners of the job.
    pub owners: Vec<Owner>,
}

/// One owner entry of an [`OwnershipJobFacet`]: a name plus an optional kind
/// (e.g. `MAINTAINER`, or a custom value like `team` / `user`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Owner {
    /// Identifier of the owner (e.g. a user or team name).
    pub name: String,
    /// Kind of owner (e.g. `MAINTAINER`, `team`, `user`). Maps to the `type`
    /// spec field.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
}

/// Business tags on a job (the `tags` job facet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagsJobFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// The tags applied to the job.
    pub tags: Vec<TagsJobFacetFields>,
}

/// One tag of a [`TagsJobFacet`]: a key with an optional value and an optional
/// source naming the system that assigned the tag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagsJobFacetFields {
    /// The tag key.
    pub key: String,
    /// The tag value, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// The system that assigned the tag, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

// ---------------------------------------------------------------------------
// Dataset facets
// ---------------------------------------------------------------------------

/// Bag of dataset facets. Known facets are typed; anything else flows through
/// `extra`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatasetFacets {
    /// The dataset's field-level schema.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaDatasetFacet>,
    /// The physical data source backing the dataset. Maps to the `dataSource`
    /// spec field.
    #[serde(rename = "dataSource", skip_serializing_if = "Option::is_none")]
    pub data_source: Option<DataSourceDatasetFacet>,
    /// Column-level lineage. Per spec this facet describes how an **output**
    /// dataset's fields are derived, so it is only ever attached to outputs.
    #[serde(rename = "columnLineage", skip_serializing_if = "Option::is_none")]
    pub column_lineage: Option<ColumnLineageDatasetFacet>,
    /// Alternate identifiers (symlinks) for the dataset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symlinks: Option<SymlinksDatasetFacet>,
    /// The lifecycle state change this run applied to the dataset (e.g. it was
    /// created or overwritten). Maps to the `lifecycleStateChange` spec field;
    /// only meaningful on outputs.
    #[serde(
        rename = "lifecycleStateChange",
        skip_serializing_if = "Option::is_none"
    )]
    pub lifecycle_state_change: Option<LifecycleStateChangeDatasetFacet>,
    /// Additional custom dataset facets passed through verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Output-only dataset facets (serialized under `outputFacets`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OutputDatasetFacets {
    /// Statistics about the data written to the output. Maps to the
    /// `outputStatistics` spec field.
    #[serde(rename = "outputStatistics", skip_serializing_if = "Option::is_none")]
    pub output_statistics: Option<OutputStatisticsOutputDatasetFacet>,
    /// Additional custom output dataset facets passed through verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl OutputDatasetFacets {
    /// Returns `true` when no output facets are present.
    pub fn is_empty(&self) -> bool {
        self.output_statistics.is_none() && self.extra.is_empty()
    }
}

/// Runtime statistics about the data written to an output dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputStatisticsOutputDatasetFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// Number of rows written. Maps to the `rowCount` spec field.
    #[serde(rename = "rowCount", skip_serializing_if = "Option::is_none")]
    pub row_count: Option<i64>,
    /// Size in bytes of the data written.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    /// Number of files written. Maps to the `fileCount` spec field.
    #[serde(rename = "fileCount", skip_serializing_if = "Option::is_none")]
    pub file_count: Option<i64>,
}

/// Input-only dataset facets (serialized under `inputFacets`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InputDatasetFacets {
    /// Statistics about the data read from the input. Maps to the
    /// `inputStatistics` spec field.
    #[serde(rename = "inputStatistics", skip_serializing_if = "Option::is_none")]
    pub input_statistics: Option<InputStatisticsInputDatasetFacet>,
    /// Additional custom input dataset facets passed through verbatim.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl InputDatasetFacets {
    /// Returns `true` when no input facets are present.
    pub fn is_empty(&self) -> bool {
        self.input_statistics.is_none() && self.extra.is_empty()
    }
}

/// Runtime statistics about the data read from an input dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputStatisticsInputDatasetFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// Number of rows read. Maps to the `rowCount` spec field.
    #[serde(rename = "rowCount", skip_serializing_if = "Option::is_none")]
    pub row_count: Option<i64>,
    /// Size in bytes of the data read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    /// Number of files read. Maps to the `fileCount` spec field.
    #[serde(rename = "fileCount", skip_serializing_if = "Option::is_none")]
    pub file_count: Option<i64>,
}

/// The field-level schema of a dataset (the `schema` dataset facet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDatasetFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// The dataset's fields, in order.
    pub fields: Vec<SchemaField>,
}

/// One field of a [`SchemaDatasetFacet`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    /// Name of the field.
    pub name: String,
    /// Data type of the field. Maps to the `type` spec field.
    #[serde(rename = "type")]
    pub type_: String,
    /// Optional human-readable description of the field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// The physical data source backing a dataset (the `dataSource` dataset facet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceDatasetFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// Name of the data source.
    pub name: String,
    /// URI of the data source.
    pub uri: String,
}

/// Alternate identifiers for a dataset (the `symlinks` dataset facet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymlinksDatasetFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// The alternate identifiers for the dataset.
    pub identifiers: Vec<SymlinkIdentifier>,
}

/// One alternate identifier of a [`SymlinksDatasetFacet`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymlinkIdentifier {
    /// Namespace of the alternate identifier.
    pub namespace: String,
    /// Name of the alternate identifier.
    pub name: String,
    /// Kind of identifier (e.g. `TABLE`). Maps to the `type` spec field.
    #[serde(rename = "type")]
    pub type_: String,
}

/// The lifecycle state change a run applied to a dataset (the
/// `lifecycleStateChange` dataset facet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleStateChangeDatasetFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// The state change applied. One of the spec's enum values: `ALTER`,
    /// `CREATE`, `DROP`, `OVERWRITE`, `RENAME`, `TRUNCATE`.
    #[serde(rename = "lifecycleStateChange")]
    pub lifecycle_state_change: String,
}

/// How an output dataset's fields derive from input dataset fields.
///
/// Keyed by output field name; the spec defines this facet on output datasets
/// (consumers ignore it on inputs). Fed by the positional plan resolution in
/// `crate::column`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnLineageDatasetFacet {
    /// Common facet fields (`_producer`, `_schemaURL`).
    #[serde(flatten)]
    pub base: BaseFacet,
    /// Output field name -> the input fields it derives from.
    pub fields: std::collections::BTreeMap<String, FieldLineage>,
    /// Dataset-level (whole-row) influences not tied to a single output column.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dataset: Vec<InputField>,
}

/// Provenance of a single output column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldLineage {
    /// The input fields the output column derives from. Maps to the
    /// `inputFields` spec field.
    #[serde(rename = "inputFields")]
    pub input_fields: Vec<InputField>,
}

/// A source `(dataset, field)` an output column derives from, with how.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputField {
    /// Namespace of the source dataset.
    pub namespace: String,
    /// Name of the source dataset.
    pub name: String,
    /// Name of the source field within the dataset, if column-specific.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// How the source influences the output.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transformations: Vec<Transformation>,
}

/// How an input influences an output: `DIRECT` (the source value flows into the
/// output) or `INDIRECT` (influence via filter/group/join/sort). `subtype` is
/// conventionally one of `IDENTITY`, `TRANSFORMATION`, `AGGREGATION`, `FILTER`,
/// `JOIN`, `GROUP_BY`, `SORT`, `WINDOW`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transformation {
    /// Whether the influence is `DIRECT` or `INDIRECT`. Maps to the `type` spec
    /// field.
    #[serde(rename = "type")]
    pub type_: TransformationType,
    /// More specific classification (e.g. `IDENTITY`, `AGGREGATION`, `FILTER`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtype: Option<String>,
    /// Human-readable description of the transformation.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// Whether the transformation masks or obfuscates the source value.
    #[serde(default)]
    pub masking: bool,
}

/// Kind of influence an input field has on an output field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TransformationType {
    /// The source value flows directly into the output.
    Direct,
    /// The source influences the output indirectly (e.g. via filter, group,
    /// join, or sort).
    Indirect,
}
