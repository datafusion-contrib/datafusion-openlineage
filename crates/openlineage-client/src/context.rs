//! Top-level orchestration context contributed per emitted run.
//!
//! Different orchestration systems (Airflow, Dagster, Databricks Jobs) model
//! run/job identity differently. Rather than hardcode a field set, an emitting
//! integration contributes a [`LineageContext`] per query. The mechanism by
//! which a context is produced is engine-specific (e.g. DataFusion derives it
//! from a `SessionState`), so the *provider* trait lives with the engine
//! integration — this crate owns only the context data and its env conventions.

use serde_json::{Map, Value};
use uuid::Uuid;

use crate::config::OpenLineageConfig;
use crate::facets::{BaseFacet, ParentJob, ParentRun, ParentRunFacet};

/// Top-level context an integration contributes to each emitted run.
///
/// All fields are optional; unset values fall back to [`OpenLineageConfig`]
/// defaults and plan-derived identity.
#[derive(Debug, Default, Clone)]
pub struct LineageContext {
    /// Correlate with an orchestrator-owned run id (else a fresh UUIDv7 is used).
    pub run_id: Option<Uuid>,
    /// Namespace of the emitting job (else the configured default namespace).
    pub job_namespace: Option<String>,
    /// Name of the emitting job (else a plan-derived job name).
    pub job_name: Option<String>,
    /// Standard OpenLineage `parent` run facet.
    pub parent_run: Option<ParentRunFacet>,
    /// Arbitrary extra run facets merged into the emitted event.
    pub run_facets: Map<String, Value>,
    /// Arbitrary extra job facets merged into the emitted event.
    pub job_facets: Map<String, Value>,
    /// The SQL text of the query, if the host has it. Populates the `sql` job
    /// facet. A plan walk cannot recover this, so the integration supplies it
    /// from the request boundary.
    pub sql: Option<String>,
}

impl LineageContext {
    /// Build a context from the established OpenLineage parent-run environment
    /// conventions, returning `None`-filled fields when nothing is set.
    ///
    /// Reads `OPENLINEAGE_PARENT_ID` (slash form `{namespace}/{name}/{runId}`),
    /// falling back to the discrete `OPENLINEAGE_PARENT_JOB_NAMESPACE` /
    /// `OPENLINEAGE_PARENT_JOB_NAME` / `OPENLINEAGE_PARENT_RUN_ID` variables.
    pub fn from_env(config: &OpenLineageConfig) -> Self {
        let parent_run = parent_from_env(config);
        LineageContext {
            parent_run,
            ..Default::default()
        }
    }
}

fn parent_from_env(config: &OpenLineageConfig) -> Option<ParentRunFacet> {
    let (namespace, name, run_id) = if let Ok(parent_id) = std::env::var("OPENLINEAGE_PARENT_ID") {
        // Format: {namespace}/{name}/{runId}
        let parts: Vec<&str> = parent_id.splitn(3, '/').collect();
        match parts.as_slice() {
            [ns, n, rid] => (ns.to_string(), n.to_string(), rid.to_string()),
            _ => return None,
        }
    } else {
        let ns = std::env::var("OPENLINEAGE_PARENT_JOB_NAMESPACE").ok()?;
        let n = std::env::var("OPENLINEAGE_PARENT_JOB_NAME").ok()?;
        let rid = std::env::var("OPENLINEAGE_PARENT_RUN_ID").ok()?;
        (ns, n, rid)
    };

    Some(ParentRunFacet {
        base: BaseFacet::new(&config.producer, "1-1-0/ParentRunFacet.json"),
        run: ParentRun { run_id },
        job: ParentJob { namespace, name },
        root: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARENT_VARS: [&str; 4] = [
        "OPENLINEAGE_PARENT_ID",
        "OPENLINEAGE_PARENT_JOB_NAMESPACE",
        "OPENLINEAGE_PARENT_JOB_NAME",
        "OPENLINEAGE_PARENT_RUN_ID",
    ];

    fn clear_parent_vars() {
        for v in PARENT_VARS {
            unsafe { std::env::remove_var(v) };
        }
    }

    // Process env is global, so the parent-from-env cases run in one serialized
    // test rather than racing as separate `#[test]`s. Each step clears the vars
    // first, then sets only what it exercises.
    #[test]
    fn parent_from_env_covers_all_forms() {
        let config = OpenLineageConfig::default();

        // No vars set → no parent facet.
        clear_parent_vars();
        assert!(parent_from_env(&config).is_none(), "unset env yields None");

        // Slash form `{namespace}/{name}/{runId}`.
        clear_parent_vars();
        unsafe { std::env::set_var("OPENLINEAGE_PARENT_ID", "airflow/dag.task/run-7") };
        let p = parent_from_env(&config).expect("slash form parses");
        assert_eq!(p.job.namespace, "airflow");
        assert_eq!(p.job.name, "dag.task");
        assert_eq!(p.run.run_id, "run-7");

        // Malformed slash form (too few segments) → None.
        clear_parent_vars();
        unsafe { std::env::set_var("OPENLINEAGE_PARENT_ID", "only-one-part") };
        assert!(
            parent_from_env(&config).is_none(),
            "a non-triple PARENT_ID is rejected"
        );

        // Discrete fallback variables.
        clear_parent_vars();
        unsafe {
            std::env::set_var("OPENLINEAGE_PARENT_JOB_NAMESPACE", "dagster");
            std::env::set_var("OPENLINEAGE_PARENT_JOB_NAME", "asset.build");
            std::env::set_var("OPENLINEAGE_PARENT_RUN_ID", "run-9");
        }
        let p = parent_from_env(&config).expect("discrete vars parse");
        assert_eq!(p.job.namespace, "dagster");
        assert_eq!(p.job.name, "asset.build");
        assert_eq!(p.run.run_id, "run-9");

        // A partial discrete set (missing run id) → None.
        clear_parent_vars();
        unsafe {
            std::env::set_var("OPENLINEAGE_PARENT_JOB_NAMESPACE", "dagster");
            std::env::set_var("OPENLINEAGE_PARENT_JOB_NAME", "asset.build");
        }
        assert!(
            parent_from_env(&config).is_none(),
            "missing run id falls through to None"
        );

        // from_env wires parent_run through and leaves the rest defaulted.
        clear_parent_vars();
        unsafe { std::env::set_var("OPENLINEAGE_PARENT_ID", "ns/job/run") };
        let ctx = LineageContext::from_env(&config);
        assert!(ctx.parent_run.is_some());
        assert!(ctx.run_id.is_none() && ctx.sql.is_none());

        clear_parent_vars();
    }
}
