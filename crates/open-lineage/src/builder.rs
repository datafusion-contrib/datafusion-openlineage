//! Construct [`RunEvent`]s from extracted lineage + context + config.

use chrono::Utc;
use uuid::Uuid;

use crate::config::OpenLineageConfig;
use crate::context::LineageContext;
use crate::event::{Dataset, Job, RUN_EVENT_SCHEMA_URL, Run, RunEvent, RunEventType};
use crate::extract::{QueryLineage, input_dataset_facets, output_dataset_facets};
use crate::facets::{
    BaseFacet, ErrorMessageRunFacet, JobFacets, JobTypeJobFacet, ProcessingEngineRunFacet,
    RunFacets, SqlJobFacet,
};

const PROCESSING_ENGINE_FACET: &str = "1-1-1/ProcessingEngineRunFacet.json";
const SQL_FACET: &str = "1-1-0/SQLJobFacet.json";
const JOB_TYPE_FACET: &str = "2-0-3/JobTypeJobFacet.json";
const ERROR_FACET: &str = "1-0-1/ErrorMessageRunFacet.json";

/// Build the START event for a query.
pub fn start_event(
    run_id: Uuid,
    lineage: &QueryLineage,
    cx: &LineageContext,
    config: &OpenLineageConfig,
) -> RunEvent {
    base_event(RunEventType::Start, run_id, lineage, cx, config, None)
}

/// Build the COMPLETE event for a query.
pub fn complete_event(
    run_id: Uuid,
    lineage: &QueryLineage,
    cx: &LineageContext,
    config: &OpenLineageConfig,
) -> RunEvent {
    base_event(RunEventType::Complete, run_id, lineage, cx, config, None)
}

/// Build the FAIL event for a query, attaching an `errorMessage` run facet.
pub fn fail_event(
    run_id: Uuid,
    lineage: &QueryLineage,
    cx: &LineageContext,
    config: &OpenLineageConfig,
    error: &str,
) -> RunEvent {
    base_event(
        RunEventType::Fail,
        run_id,
        lineage,
        cx,
        config,
        Some(error.to_string()),
    )
}

fn base_event(
    event_type: RunEventType,
    run_id: Uuid,
    lineage: &QueryLineage,
    cx: &LineageContext,
    config: &OpenLineageConfig,
    error: Option<String>,
) -> RunEvent {
    let run_facets = RunFacets {
        processing_engine: Some(ProcessingEngineRunFacet {
            base: BaseFacet::new(&config.producer, PROCESSING_ENGINE_FACET),
            version: config.engine_version.clone(),
            name: config.engine_name.clone(),
            openlineage_adapter_version: config.adapter_version.clone(),
        }),
        parent: cx.parent_run.clone(),
        nominal_time: None,
        error_message: error.map(|message| ErrorMessageRunFacet {
            base: BaseFacet::new(&config.producer, ERROR_FACET),
            message,
            programming_language: "Rust".to_string(),
            stack_trace: None,
        }),
        extra: cx.run_facets.clone(),
    };

    let job_facets = JobFacets {
        sql: lineage.sql.as_ref().map(|query| SqlJobFacet {
            base: BaseFacet::new(&config.producer, SQL_FACET),
            query: query.clone(),
        }),
        job_type: Some(JobTypeJobFacet {
            base: BaseFacet::new(&config.producer, JOB_TYPE_FACET),
            processing_type: "BATCH".to_string(),
            integration: "DATAFUSION".to_string(),
            job_type: "QUERY".to_string(),
        }),
        extra: cx.job_facets.clone(),
    };

    let inputs = lineage
        .inputs
        .iter()
        .map(|input| Dataset {
            namespace: input.name.namespace.clone(),
            name: input.name.name.clone(),
            facets: input_dataset_facets(input, config),
            // Runtime read statistics are filled in by OpenLineageExec at end of
            // execution (single-input queries only; see exec.rs).
            input_facets: None,
            output_facets: None,
        })
        .collect();

    let outputs = lineage
        .outputs
        .iter()
        .map(|output| Dataset {
            namespace: output.name.namespace.clone(),
            name: output.name.name.clone(),
            facets: output_dataset_facets(output, config),
            input_facets: None,
            // Runtime statistics are filled in by OpenLineageExec at end of
            // execution, once the row count is known.
            output_facets: None,
        })
        .collect();

    RunEvent {
        event_type,
        event_time: Utc::now().to_rfc3339(),
        run: Run {
            run_id,
            facets: run_facets,
        },
        job: Job {
            namespace: cx
                .job_namespace
                .clone()
                .unwrap_or_else(|| config.job_namespace.clone()),
            name: cx
                .job_name
                .clone()
                .unwrap_or_else(|| "datafusion_query".to_string()),
            facets: job_facets,
        },
        inputs,
        outputs,
        producer: config.producer.clone(),
        schema_url: RUN_EVENT_SCHEMA_URL.to_string(),
    }
}
