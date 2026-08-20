//! Acceptance test against the **OpenLineage reference implementation**, Marquez.
//!
//! This is the strongest correctness signal the crate has: it does not assert
//! against our own model of the spec, it emits real events over the real HTTP
//! transport into a real Marquez backend and then reads them back through
//! Marquez's own REST API. If Marquez ingests our events (HTTP 2xx) and
//! reconstructs the datasets / job / run-state / column-lineage we intended, the
//! events are correct by the reference's definition.
//!
//! Gated twice so it never runs by accident:
//! * the whole file is `#[cfg(feature = "marquez-it")]`, and
//! * the test is `#[ignore]`d.
//!
//! Run it explicitly, with Docker available:
//!
//! ```text
//! cargo test -p datafusion-openlineage --features marquez-it -- --ignored
//! ```
#![cfg(feature = "marquez-it")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion_openlineage::config::OpenLineageConfig;
use datafusion_openlineage::event::{RunEvent, RunEventType};
use datafusion_openlineage::transport::{Transport, TransportError};
use datafusion_openlineage::{
    CloudClientTransport, OpenLineageClient, instrument_session_state_simple,
};
use serde_json::Value;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use url::Url;

/// Versions match `environments/services/marquez.yaml`.
const MARQUEZ_IMAGE: &str = "marquezproject/marquez";
const MARQUEZ_TAG: &str = "0.50.0";
const POSTGRES_TAG: &str = "16";
/// The Marquez image's baked config hardcodes the db user/password/name to this.
const MARQUEZ_DB: &str = "marquez";
const NAMESPACE: &str = "marquez-acceptance";

/// Records events locally too, so a failure can be diagnosed against what we
/// believe we sent (the network transport delivers in parallel to Marquez).
#[derive(Debug, Default, Clone)]
struct TeeTransport {
    inner: Option<CloudClientTransport>,
    seen: Arc<Mutex<Vec<RunEvent>>>,
}

#[async_trait]
impl Transport for TeeTransport {
    async fn emit(&self, event: &RunEvent) -> Result<(), TransportError> {
        self.seen.lock().unwrap().push(event.clone());
        if let Some(inner) = &self.inner {
            inner.emit(event).await?;
        }
        Ok(())
    }
}

/// Bring up Postgres + Marquez on a shared network and return
/// `(net-holders, base_url)` where `base_url` is the host-mapped Marquez API.
/// The returned containers must be kept alive for the duration of the test.
async fn start_marquez() -> (
    ContainerAsync<GenericImage>,
    ContainerAsync<GenericImage>,
    Url,
) {
    // A unique-ish network name without Math.random: derive from the pid.
    let network = format!("ol-marquez-{}", std::process::id());

    let postgres = GenericImage::new("postgres", POSTGRES_TAG)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_network(network.clone())
        .with_container_name(format!("ol-pg-{}", std::process::id()))
        .with_env_var("POSTGRES_USER", MARQUEZ_DB)
        .with_env_var("POSTGRES_PASSWORD", MARQUEZ_DB)
        .with_env_var("POSTGRES_DB", MARQUEZ_DB)
        .start()
        .await
        .expect("postgres started");

    let pg_host = format!("ol-pg-{}", std::process::id());
    let marquez = GenericImage::new(MARQUEZ_IMAGE, MARQUEZ_TAG)
        .with_exposed_port(5000.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/healthcheck")
                .with_port(5001.tcp())
                .with_expected_status_code(200u16),
        ))
        .with_network(network)
        .with_env_var("MARQUEZ_PORT", "5000")
        .with_env_var("MARQUEZ_ADMIN_PORT", "5001")
        .with_env_var("POSTGRES_HOST", pg_host)
        .with_env_var("POSTGRES_PORT", "5432")
        .with_env_var("SEARCH_ENABLED", "false")
        .start()
        .await
        .expect("marquez started");

    let host = marquez.get_host().await.expect("marquez host");
    let port = marquez
        .get_host_port_ipv4(5000.tcp())
        .await
        .expect("marquez api port");
    let base = Url::parse(&format!("http://{host}:{port}/")).expect("base url");
    (postgres, marquez, base)
}

/// GET `base + path` and parse JSON, retrying briefly to absorb Marquez's
/// asynchronous ingest of the events we just posted.
async fn get_json_eventually(
    http: &reqwest::Client,
    base: &Url,
    path: &str,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    let url = base.join(path).expect("join path");
    let mut last = Value::Null;
    for _ in 0..30 {
        if let Ok(resp) = http.get(url.clone()).send().await
            && resp.status().is_success()
            && let Ok(json) = resp.json::<Value>().await
        {
            if predicate(&json) {
                return json;
            }
            last = json;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("condition never met for GET {path}; last response:\n{last:#}");
}

#[tokio::test]
#[ignore = "requires Docker; run with --features marquez-it -- --ignored"]
async fn marquez_ingests_and_reconstructs_lineage() {
    let (_pg, _marquez, base) = start_marquez().await;

    // Real HTTP transport into the running Marquez, tee'd for local diagnostics.
    let endpoint = base.join("api/v1/lineage").unwrap();
    let transport = TeeTransport {
        inner: Some(CloudClientTransport::unauthenticated(endpoint)),
        seen: Arc::new(Mutex::new(Vec::new())),
    };
    let seen = transport.seen.clone();
    let client = OpenLineageClient::new(Arc::new(transport));

    let cfg = OpenLineageConfig {
        job_namespace: NAMESPACE.to_string(),
        ..Default::default()
    };
    let ctx = SessionContext::new_with_state(instrument_session_state_simple(
        SessionContext::new().state(),
        client.clone(),
        cfg,
    ));

    // Seed inputs/outputs, then an INSERT that derives a column — the path that
    // flows an output dataset + column lineage through the instrumented planner.
    ctx.sql("CREATE TABLE src (a INT, b INT) AS VALUES (1, 2), (3, 4)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("CREATE TABLE dst (a INT, s INT) AS VALUES (0, 0)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("INSERT INTO dst SELECT a, a + b AS s FROM src")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // `shutdown()` awaits the drain task, which only ends once every sender is
    // dropped. The instrumented planner + exec nodes inside `ctx` hold client
    // clones, so the context must be dropped first or the drain never closes.
    drop(ctx);
    client.shutdown().await;

    // Sanity: locally we recorded a START and a COMPLETE for the INSERT.
    {
        let local = seen.lock().unwrap();
        let kinds: Vec<RunEventType> = local.iter().map(|e| e.event_type).collect();
        assert!(
            kinds.contains(&RunEventType::Start),
            "emitted START: {kinds:?}"
        );
        assert!(
            kinds.contains(&RunEventType::Complete),
            "emitted COMPLETE: {kinds:?}"
        );
        assert!(
            local
                .iter()
                .any(|e| e.outputs.iter().any(|o| o.name == "dst")),
            "an event names `dst` as an output"
        );
    }

    let http = reqwest::Client::new();

    // 1. Marquez reconstructed the output dataset in our namespace.
    let dataset = get_json_eventually(
        &http,
        &base,
        &format!("api/v1/namespaces/{NAMESPACE}/datasets/dst"),
        |j| j.get("name").and_then(Value::as_str) == Some("dst"),
    )
    .await;
    assert_eq!(dataset["name"], "dst", "dataset reconstructed: {dataset:#}");

    // 2. The input dataset is present too.
    let src = get_json_eventually(
        &http,
        &base,
        &format!("api/v1/namespaces/{NAMESPACE}/datasets/src"),
        |j| j.get("name").and_then(Value::as_str) == Some("src"),
    )
    .await;
    assert_eq!(src["name"], "src");

    // 3. The datasets list for the namespace contains both.
    let datasets = get_json_eventually(
        &http,
        &base,
        &format!("api/v1/namespaces/{NAMESPACE}/datasets"),
        |j| {
            let names: Vec<&str> = j["datasets"]
                .as_array()
                .map(|a| a.iter().filter_map(|d| d["name"].as_str()).collect())
                .unwrap_or_default();
            names.contains(&"src") && names.contains(&"dst")
        },
    )
    .await;
    let names: Vec<&str> = datasets["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|d| d["name"].as_str())
        .collect();
    assert!(
        names.contains(&"src") && names.contains(&"dst"),
        "both datasets listed: {names:?}"
    );

    // 4. The lineage graph links the job to its datasets. Marquez addresses
    //    nodes as `dataset:<namespace>:<name>`; the graph for `dst` must be
    //    non-empty (it has at least the producing job as a neighbour).
    let node_id = format!("dataset:{NAMESPACE}:dst");
    let graph = get_json_eventually(
        &http,
        &base,
        &format!("api/v1/lineage?nodeId={}", urlencoding(&node_id)),
        |j| {
            j.get("graph")
                .map(|g| !g.as_array().unwrap_or(&vec![]).is_empty())
                .unwrap_or(false)
        },
    )
    .await;
    let graph_nodes = graph["graph"].as_array().expect("graph array");
    assert!(
        !graph_nodes.is_empty(),
        "lineage graph for dst is non-empty: {graph:#}"
    );
}

/// Minimal percent-encoding for the characters that appear in a Marquez node id
/// (`:`), avoiding a dependency just for the query string.
fn urlencoding(s: &str) -> String {
    s.replace(':', "%3A")
}
