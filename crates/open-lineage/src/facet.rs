//! Typed, query-scoped facet builders shared by built-in and engine integrations.
//!
//! Register a [`FacetBuilderFactory`] with [`crate::OpenLineageBuilder::facet_factory`].
//! Factories prepare owned state once before START. Their builders run immediately
//! before each emitted event, after runtime statistics or error details are added.
//! Dataset builders run separately for each dataset; a sink is bound to that target.
//! No callbacks run while constructing the COMPLETE template.

mod builtin;

use std::collections::HashSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::LogicalPlan;
use futures::FutureExt;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::config::OpenLineageConfig;
use crate::context::LineageContext;
use crate::event::{Dataset, RunEvent};
use crate::extract::QueryLineage;
use crate::resolver::{DatasetAccess, DatasetOrigin, ResolvedDataset};

/// Built-in factory supplying the standard `processing_engine` run facet.
pub const PROCESSING_ENGINE_FACTORY: &str = "datafusion.processing_engine";

/// Failures contributing facets. They are logged and never fail the query.
#[derive(Debug, thiserror::Error)]
pub enum FacetError {
    /// The payload could not be serialized.
    #[error("facet serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    /// The name, schema URI, or payload is invalid.
    #[error("invalid facet: {0}")]
    Invalid(String),
    /// An integration-specific failure.
    #[error("{0}")]
    Other(String),
}

/// OpenLineage attachment locations, encoded as zero-sized marker types.
pub mod scope {
    /// The event's `job.facets` map.
    #[derive(Debug)]
    pub struct Job;
    /// The event's `run.facets` map.
    #[derive(Debug)]
    pub struct Run;
    /// An input or output dataset's `facets` map.
    #[derive(Debug)]
    pub struct Dataset;
    /// An input dataset's `inputFacets` map.
    #[derive(Debug)]
    pub struct InputDataset;
    /// An output dataset's `outputFacets` map.
    #[derive(Debug)]
    pub struct OutputDataset;
}

mod sealed {
    pub trait Sealed {}
}

/// A supported attachment location. Implemented only by this crate's scope types.
pub trait FacetScope: sealed::Sealed + Sized + Send + Sync + 'static {
    /// Read-only context supplied for this scope.
    type Context<'a>;

    /// Internal routing used by [`FacetBuilders::with`].
    #[doc(hidden)]
    fn register<B: FacetBuilder<Scope = Self>>(builders: &mut FacetBuilders, builder: B);
}

/// The context associated with scope `S`.
pub type FacetContext<'a, S> = <S as FacetScope>::Context<'a>;

/// Read-only event and query metadata for singleton job and run targets.
pub struct EventFacetContext<'a> {
    /// The event being emitted, including its lifecycle type and available metrics.
    pub event: &'a RunEvent,
    /// Orchestration context captured for this query.
    pub query: &'a LineageContext,
}

/// Context for a job facet builder.
pub type JobFacetContext<'a> = EventFacetContext<'a>;
/// Context for a run facet builder.
pub type RunFacetContext<'a> = EventFacetContext<'a>;

/// One dataset occurrence in the event being emitted.
pub struct DatasetFacetContext<'a> {
    /// The event being emitted, including its lifecycle type.
    pub event: &'a RunEvent,
    /// Orchestration context captured for this query.
    pub query: &'a LineageContext,
    /// The exact input or output whose facet map the sink will update.
    pub dataset: &'a Dataset,
    /// Whether this occurrence is an input or output.
    pub access: DatasetAccess,
    /// Logical sources associated with this canonical identity and access mode.
    pub origins: &'a [DatasetOrigin],
}

/// Information available during asynchronous, per-query factory preparation.
pub struct QueryContext<'a> {
    /// Run ID shared by all events for this query.
    pub run_id: Uuid,
    /// The session planning the query.
    pub session_state: &'a SessionState,
    /// Logical plan used for lineage extraction.
    pub logical_plan: &'a LogicalPlan,
    /// Extracted table and column lineage.
    pub lineage: &'a QueryLineage,
    /// Orchestration context captured for this query.
    pub context: &'a LineageContext,
    /// Resolved identities with their logical source/provider associations.
    pub datasets: &'a [ResolvedDataset],
    /// Producer and engine configuration for this session.
    pub config: &'a OpenLineageConfig,
}

/// A standard or custom facet payload. The library attaches base metadata.
///
/// Serialize an object without `_producer` or `_schemaURL`. Custom facet names
/// should use a project prefix and schemas should have immutable, versioned URIs.
/// Implementations can use any of the five scopes; no trait objects are needed
/// for payloads because serialization happens inside [`FacetSink::insert`].
pub trait Facet: Serialize {
    /// The only attachment location to which this payload may be contributed.
    type Scope: FacetScope;
    /// Key in the target facet map.
    const NAME: &'static str;
    /// Absolute URI of the schema for this payload, including base metadata.
    const SCHEMA_URL: &'static str;
}

/// A query-scoped contributor. All matching builders run; matching never stops
/// dispatch to other builders.
///
/// Callbacks are synchronous, including during stream cleanup. Keep them fast,
/// non-blocking and side-effect-free where possible; prepare asynchronous
/// metadata in the factory. A planned query might never execute, so terminal
/// callbacks are not resource-cleanup guarantees.
pub trait FacetBuilder: Debug + Send + Sync + 'static {
    /// Target scope, also restricting the sink's accepted payloads.
    type Scope: FacetScope;

    /// Whether to call [`Self::build`] for this event and target. Defaults to true.
    fn applies_to(&self, _cx: &FacetContext<'_, Self::Scope>) -> bool {
        true
    }

    /// Stage zero or more facets. Called only when `applies_to` returned true,
    /// with the same context. An error discards this invocation's additions.
    fn build(
        &self,
        cx: &FacetContext<'_, Self::Scope>,
        facets: &mut FacetSink<Self::Scope>,
    ) -> Result<(), FacetError>;
}

/// Prepares builders once per query, before START, after identity resolution.
///
/// Returned builders own their state or shared handles; they cannot borrow the
/// temporary context. Bound any metadata I/O. An error or unwinding panic skips
/// this factory for the query; other factories and event emission continue.
#[async_trait]
pub trait FacetBuilderFactory: Debug + Send + Sync + 'static {
    /// Stable, globally distinctive registration name (e.g. `acme.delta`).
    fn name(&self) -> &'static str;

    /// Return no builders when the integration does not apply to this query.
    async fn create(&self, cx: &QueryContext<'_>) -> Result<FacetBuilders, FacetError>;
}

/// A collection of builders, grouped by scope. Construct with `default().with(...)`.
#[derive(Debug, Default)]
pub struct FacetBuilders {
    job: Vec<Box<dyn FacetBuilder<Scope = scope::Job>>>,
    run: Vec<Box<dyn FacetBuilder<Scope = scope::Run>>>,
    dataset: Vec<Box<dyn FacetBuilder<Scope = scope::Dataset>>>,
    input: Vec<Box<dyn FacetBuilder<Scope = scope::InputDataset>>>,
    output: Vec<Box<dyn FacetBuilder<Scope = scope::OutputDataset>>>,
}

impl FacetBuilders {
    /// Append a builder to its associated scope, retaining registration order.
    pub fn with<B: FacetBuilder>(mut self, builder: B) -> Self {
        B::Scope::register(&mut self, builder);
        self
    }
}

macro_rules! scope_impl {
    ($scope:ident, $context:ident, $field:ident) => {
        impl sealed::Sealed for scope::$scope {}
        impl FacetScope for scope::$scope {
            type Context<'a> = $context<'a>;
            fn register<B: FacetBuilder<Scope = Self>>(builders: &mut FacetBuilders, builder: B) {
                builders.$field.push(Box::new(builder));
            }
        }
    };
}
scope_impl!(Job, JobFacetContext, job);
scope_impl!(Run, RunFacetContext, run);
scope_impl!(Dataset, DatasetFacetContext, dataset);
scope_impl!(InputDataset, DatasetFacetContext, input);
scope_impl!(OutputDataset, DatasetFacetContext, output);

/// A staging collector bound by the dispatcher to one target and scope.
///
/// Payloads of another scope are rejected at compile time:
///
/// ```compile_fail
/// use datafusion_openlineage::facet::{Facet, FacetError, FacetSink, scope};
/// fn wrong_scope<F: Facet<Scope = scope::Job>>(
///     sink: &mut FacetSink<scope::Run>, facet: F,
/// ) -> Result<(), FacetError> {
///     sink.insert(facet)
/// }
/// ```
pub struct FacetSink<S: FacetScope> {
    producer: String,
    pending: Map<String, Value>,
    scope: PhantomData<S>,
}

impl<S: FacetScope> FacetSink<S> {
    /// Serialize and stage a facet of this sink's scope.
    ///
    /// The library adds `_producer` and `_schemaURL`. Non-object payloads,
    /// reserved base-metadata fields, invalid schema URIs and duplicate names
    /// within one invocation return an error. Existing event facets are preserved
    /// when staged contributions are merged. Standard names populate typed fields.
    pub fn insert<F: Facet<Scope = S>>(&mut self, facet: F) -> Result<(), FacetError> {
        if F::NAME.is_empty() || url::Url::parse(F::SCHEMA_URL).is_err() {
            return Err(FacetError::Invalid(
                "a name and absolute schema URI are required".into(),
            ));
        }
        let Value::Object(mut payload) = serde_json::to_value(facet)? else {
            return Err(FacetError::Invalid("payload must be an object".into()));
        };
        if payload.contains_key("_producer") || payload.contains_key("_schemaURL") {
            return Err(FacetError::Invalid(
                "base metadata is supplied by the sink".into(),
            ));
        }
        if self.pending.contains_key(F::NAME) {
            return Err(FacetError::Invalid(format!("duplicate facet {}", F::NAME)));
        }
        payload.insert("_producer".into(), self.producer.clone().into());
        payload.insert("_schemaURL".into(), F::SCHEMA_URL.into());
        self.pending.insert(F::NAME.into(), payload.into());
        Ok(())
    }
}

/// Factory configuration shared by session instrumentation and custom planners.
///
/// Defaults include [`PROCESSING_ENGINE_FACTORY`]. Engine factories append to
/// built-ins. Disabled names are skipped; duplicate enabled names use the first
/// registration and log a warning. No Delta/Iceberg integration is implied.
#[derive(Debug, Clone)]
pub struct FacetRegistry {
    factories: Vec<Arc<dyn FacetBuilderFactory>>,
    disabled: HashSet<String>,
}

impl Default for FacetRegistry {
    fn default() -> Self {
        Self {
            factories: builtin::factories(),
            disabled: HashSet::new(),
        }
    }
}

impl FacetRegistry {
    /// Append an integration without replacing built-in factories.
    pub fn with_factory(mut self, factory: Arc<dyn FacetBuilderFactory>) -> Self {
        self.factories.push(factory);
        self
    }

    /// Skip all registrations with this name, independent of call order.
    pub fn without_factory(mut self, name: impl Into<String>) -> Self {
        self.disabled.insert(name.into());
        self
    }

    pub(crate) async fn prepare(&self, cx: &QueryContext<'_>) -> Arc<PreparedFacets> {
        let mut prepared = PreparedFacets {
            query: cx.context.clone(),
            datasets: cx.datasets.to_vec(),
            factories: Vec::new(),
        };
        let mut seen = HashSet::new();
        for factory in &self.factories {
            let name = factory.name();
            if self.disabled.contains(name) {
                continue;
            }
            if !seen.insert(name) {
                tracing::warn!(target: "openlineage", factory = name, "duplicate facet factory skipped");
                continue;
            }
            // Include future construction in the unwind boundary as well as polling.
            let result = AssertUnwindSafe(async { factory.create(cx).await })
                .catch_unwind()
                .await;
            match result {
                Ok(Ok(builders)) => prepared.factories.push((name, builders)),
                Ok(Err(error)) => {
                    tracing::warn!(target: "openlineage", factory = name, %error, "facet factory failed")
                }
                Err(_) => {
                    tracing::warn!(target: "openlineage", factory = name, "facet factory panicked")
                }
            }
        }
        Arc::new(prepared)
    }
}

pub(crate) struct PreparedFacets {
    query: LineageContext,
    datasets: Vec<ResolvedDataset>,
    factories: Vec<(&'static str, FacetBuilders)>,
}

impl PreparedFacets {
    pub(crate) fn emit(&self, client: &crate::OpenLineageClient, mut event: RunEvent) {
        for (name, builders) in &self.factories {
            builders.enrich(&mut event, &self.query, &self.datasets, name);
        }
        client.emit(event);
    }
}

enum Target {
    Job,
    Run,
    Dataset(DatasetAccess, usize),
    Input(usize),
    Output(usize),
}

fn collect<S: FacetScope>(
    builders: &[Box<dyn FacetBuilder<Scope = S>>],
    cx: &FacetContext<'_, S>,
    producer: &str,
    factory: &str,
) -> Vec<Map<String, Value>> {
    let mut contributions = Vec::new();
    for builder in builders {
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut sink = FacetSink::<S> {
                producer: producer.to_string(),
                pending: Map::new(),
                scope: PhantomData,
            };
            if builder.applies_to(cx) {
                builder.build(cx, &mut sink)?;
            }
            Ok::<_, FacetError>(sink.pending)
        }));
        match result {
            Ok(Ok(pending)) if !pending.is_empty() => contributions.push(pending),
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                tracing::warn!(target: "openlineage", factory, %error, "facet builder failed; discarding its additions")
            }
            Err(_) => {
                tracing::warn!(target: "openlineage", factory, "facet builder panicked; discarding its additions")
            }
        }
    }
    contributions
}

impl FacetBuilders {
    fn enrich(
        &self,
        event: &mut RunEvent,
        query: &LineageContext,
        datasets: &[ResolvedDataset],
        factory: &str,
    ) {
        // Stage against one immutable snapshot per factory. No event clone and no
        // mutable alias of the dataset being inspected is handed to user code.
        let mut pending = Vec::new();
        let cx = EventFacetContext { event, query };
        for facets in collect(&self.job, &cx, &event.producer, factory) {
            pending.push((Target::Job, facets));
        }
        for facets in collect(&self.run, &cx, &event.producer, factory) {
            pending.push((Target::Run, facets));
        }
        for (access, occurrences) in [
            (DatasetAccess::Read, &event.inputs),
            (DatasetAccess::Write, &event.outputs),
        ] {
            for (index, dataset) in occurrences.iter().enumerate() {
                let origins = datasets
                    .iter()
                    .find(|resolved| {
                        resolved.access == access
                            && resolved.name.namespace == dataset.namespace
                            && resolved.name.name == dataset.name
                    })
                    .map(|resolved| resolved.origins.as_slice())
                    .unwrap_or_default();
                let cx = DatasetFacetContext {
                    event,
                    query,
                    dataset,
                    access,
                    origins,
                };
                for facets in collect(&self.dataset, &cx, &event.producer, factory) {
                    pending.push((Target::Dataset(access, index), facets));
                }
                match access {
                    DatasetAccess::Read => {
                        for facets in collect(&self.input, &cx, &event.producer, factory) {
                            pending.push((Target::Input(index), facets));
                        }
                    }
                    DatasetAccess::Write => {
                        for facets in collect(&self.output, &cx, &event.producer, factory) {
                            pending.push((Target::Output(index), facets));
                        }
                    }
                }
            }
        }
        for (target, facets) in pending {
            let result = match target {
                Target::Job => merge(&mut event.job.facets, facets),
                Target::Run => merge(&mut event.run.facets, facets),
                Target::Dataset(access, index) => {
                    let dataset = match access {
                        DatasetAccess::Read => &mut event.inputs[index],
                        DatasetAccess::Write => &mut event.outputs[index],
                    };
                    merge(&mut dataset.facets, facets)
                }
                Target::Input(index) => {
                    merge_optional(&mut event.inputs[index].input_facets, facets)
                }
                Target::Output(index) => {
                    merge_optional(&mut event.outputs[index].output_facets, facets)
                }
            };
            if let Err(error) = result {
                tracing::warn!(target: "openlineage", factory, %error, "invalid standard facet; discarding builder additions");
            }
        }
    }
}

fn merge<T: Serialize + DeserializeOwned>(
    target: &mut T,
    additions: Map<String, Value>,
) -> Result<(), serde_json::Error> {
    let mut value = serde_json::to_value(&*target)?;
    let object = value
        .as_object_mut()
        .expect("facet bags serialize as objects");
    for (name, payload) in additions {
        match object.entry(name) {
            serde_json::map::Entry::Vacant(entry) => {
                entry.insert(payload);
            }
            serde_json::map::Entry::Occupied(entry) => {
                tracing::warn!(target: "openlineage", facet = entry.key(), "facet collision; preserving existing value");
            }
        }
    }
    // Deserializing the whole candidate routes known names into typed fields,
    // preserves extras, and makes validation atomic for this builder invocation.
    *target = serde_json::from_value(value)?;
    Ok(())
}

fn merge_optional<T: Serialize + DeserializeOwned + Default>(
    target: &mut Option<T>,
    additions: Map<String, Value>,
) -> Result<(), serde_json::Error> {
    if let Some(target) = target {
        merge(target, additions)
    } else {
        let mut candidate = T::default();
        merge(&mut candidate, additions)?;
        *target = Some(candidate);
        Ok(())
    }
}

/// Preserve standalone event-builder behavior without asynchronous preparation.
pub(crate) fn enrich_standalone(
    event: &mut RunEvent,
    query: &LineageContext,
    config: &OpenLineageConfig,
) {
    builtin::builders(config).enrich(event, query, &[], PROCESSING_ENGINE_FACTORY);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::facets::{InputDatasetFacets, RunFacets};
    use serde_json::json;

    #[derive(Serialize)]
    #[serde(transparent)]
    struct Payload(Value);

    impl Facet for Payload {
        type Scope = scope::Run;
        const NAME: &'static str = "test_payload";
        const SCHEMA_URL: &'static str = "https://example.com/v1/facet.json";
    }

    fn sink() -> FacetSink<scope::Run> {
        FacetSink {
            producer: "https://example.com/engine".into(),
            pending: Map::new(),
            scope: PhantomData,
        }
    }

    #[test]
    fn sink_validates_payload_and_owns_base_metadata() {
        for value in [
            Value::Null,
            json!(42),
            json!([]),
            json!({"_producer":"override"}),
            json!({"_schemaURL":"override"}),
        ] {
            let mut sink = sink();
            assert!(sink.insert(Payload(value)).is_err());
            assert!(sink.pending.is_empty());
        }
        let mut sink = sink();
        sink.insert(Payload(json!({"answer":42}))).unwrap();
        let value = &sink.pending[Payload::NAME];
        assert_eq!(value["_producer"], "https://example.com/engine");
        assert_eq!(value["_schemaURL"], Payload::SCHEMA_URL);
        assert!(sink.insert(Payload(json!({"answer":0}))).is_err());
        assert_eq!(sink.pending[Payload::NAME]["answer"], 42);
    }

    #[derive(Serialize)]
    struct BadSchema {}
    impl Facet for BadSchema {
        type Scope = scope::Run;
        const NAME: &'static str = "test_bad";
        const SCHEMA_URL: &'static str = "relative/path.json";
    }

    #[test]
    fn relative_schema_uris_are_rejected() {
        assert!(sink().insert(BadSchema {}).is_err());
    }

    fn additions(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn standard_facets_route_to_typed_fields_and_collisions_preserve_existing_values() {
        let mut target = RunFacets::default();
        let nominal = json!({
            "_producer":"https://example.com/engine",
            "_schemaURL":"https://example.com/nominal.json",
            "nominalStartTime":"2026-10-09T00:00:00Z"
        });
        merge(
            &mut target,
            additions(json!({"nominalTime":nominal, "test_first":{"value":1}})),
        )
        .unwrap();
        assert!(target.nominal_time.is_some());
        assert!(!target.extra.contains_key("nominalTime"));
        merge(
            &mut target,
            additions(
                json!({"nominalTime":{}, "test_first":{"value":2}, "test_second":{"value":3}}),
            ),
        )
        .unwrap();
        assert_eq!(target.extra["test_first"]["value"], 1);
        assert_eq!(target.extra["test_second"]["value"], 3);
        let encoded = serde_json::to_string(&target).unwrap();
        assert_eq!(encoded.matches("\"nominalTime\":").count(), 1);
        assert_eq!(
            target.nominal_time.unwrap().nominal_start_time,
            "2026-10-09T00:00:00Z"
        );
    }

    #[test]
    fn invalid_standard_facets_discard_the_whole_invocation() {
        let mut target = RunFacets::default();
        assert!(
            merge(
                &mut target,
                additions(json!({"nominalTime":{}, "test_partial":{"value":1}}))
            )
            .is_err()
        );
        assert!(target.nominal_time.is_none());
        assert!(target.extra.is_empty());
        let mut input: Option<InputDatasetFacets> = None;
        assert!(merge_optional(&mut input, additions(json!({"inputStatistics":{}}))).is_err());
        assert!(input.is_none());
    }
}
