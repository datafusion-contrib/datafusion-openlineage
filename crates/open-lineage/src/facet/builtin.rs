//! Built-in integrations use the same public factory/builder/sink contracts.

use super::*;

pub(super) fn factories() -> Vec<Arc<dyn FacetBuilderFactory>> {
    vec![Arc::new(ProcessingEngineFactory)]
}

pub(super) fn builders(config: &OpenLineageConfig) -> FacetBuilders {
    FacetBuilders::default().with(ProcessingEngineBuilder {
        version: config.engine_version.clone(),
        name: config.engine_name.clone(),
        adapter_version: config.adapter_version.clone(),
    })
}

#[derive(Debug)]
struct ProcessingEngineFactory;

#[async_trait]
impl FacetBuilderFactory for ProcessingEngineFactory {
    fn name(&self) -> &'static str {
        PROCESSING_ENGINE_FACTORY
    }

    async fn create(&self, cx: &QueryContext<'_>) -> Result<FacetBuilders, FacetError> {
        Ok(builders(cx.config))
    }
}

#[derive(Debug)]
struct ProcessingEngineBuilder {
    version: String,
    name: String,
    adapter_version: String,
}

#[derive(Serialize)]
struct ProcessingEnginePayload<'a> {
    version: &'a str,
    name: &'a str,
    #[serde(rename = "openlineageAdapterVersion")]
    adapter_version: &'a str,
}

impl Facet for ProcessingEnginePayload<'_> {
    type Scope = scope::Run;
    const NAME: &'static str = "processing_engine";
    const SCHEMA_URL: &'static str =
        "https://openlineage.io/spec/facets/1-1-1/ProcessingEngineRunFacet.json";
}

impl FacetBuilder for ProcessingEngineBuilder {
    type Scope = scope::Run;

    fn build(
        &self,
        _cx: &RunFacetContext<'_>,
        sink: &mut FacetSink<Self::Scope>,
    ) -> Result<(), FacetError> {
        sink.insert(ProcessingEnginePayload {
            version: &self.version,
            name: &self.name,
            adapter_version: &self.adapter_version,
        })
    }
}
