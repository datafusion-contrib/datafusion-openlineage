//! Behavioral tests for the non-blocking client: smoke delivery, flush-on-shutdown,
//! drain coalescing, and slow-upstream isolation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use openlineage_client::{
    ConsoleTransport, OpenLineageClient, Run, RunEvent, RunEventType, Transport, TransportError,
};
use uuid::Uuid;

fn event() -> RunEvent {
    RunEvent {
        event_type: RunEventType::Start,
        event_time: "2024-01-01T00:00:00Z".to_string(),
        run: Run {
            run_id: Uuid::now_v7(),
            facets: Default::default(),
        },
        job: openlineage_client::Job {
            namespace: "ns".to_string(),
            name: "job".to_string(),
            facets: Default::default(),
        },
        inputs: Vec::new(),
        outputs: Vec::new(),
        producer: "test".to_string(),
        schema_url: "https://openlineage.io/spec/2-0-2/OpenLineage.json".to_string(),
    }
}

/// Records every batch's length and whether `flush` was called.
#[derive(Debug, Default)]
struct RecordingTransport {
    batches: std::sync::Mutex<Vec<usize>>,
    flushed: AtomicBool,
}

#[async_trait]
impl Transport for RecordingTransport {
    async fn emit(&self, _event: &RunEvent) -> Result<(), TransportError> {
        self.batches.lock().unwrap().push(1);
        Ok(())
    }

    async fn emit_batch(&self, events: &[RunEvent]) -> Result<(), TransportError> {
        self.batches.lock().unwrap().push(events.len());
        Ok(())
    }

    async fn flush(&self) -> Result<(), TransportError> {
        self.flushed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn smoke_emit_and_shutdown_drops_nothing() {
    let client = OpenLineageClient::new(Arc::new(ConsoleTransport));
    client.emit(event());
    let dropped = client.dropped_count();
    client.shutdown().await;
    assert_eq!(dropped, 0);
}

#[tokio::test]
async fn shutdown_flushes_transport_once() {
    let transport = Arc::new(RecordingTransport::default());
    let client = OpenLineageClient::new(transport.clone());
    client.emit(event());
    client.shutdown().await;
    assert!(
        transport.flushed.load(Ordering::SeqCst),
        "shutdown must flush the transport"
    );
}

#[tokio::test]
async fn drain_coalesces_a_burst_into_batches() {
    // A transport that delays the first delivery long enough for the rest of the
    // burst to queue, so the drain coalesces them into a batch > 1.
    #[derive(Debug)]
    struct SlowFirst {
        seen: Arc<std::sync::Mutex<Vec<usize>>>,
        calls: AtomicU64,
    }
    #[async_trait]
    impl Transport for SlowFirst {
        async fn emit(&self, _event: &RunEvent) -> Result<(), TransportError> {
            self.emit_batch(std::slice::from_ref(&event())).await
        }
        async fn emit_batch(&self, events: &[RunEvent]) -> Result<(), TransportError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                // Hold the drain on the first delivery so the rest of the burst
                // accumulates in the queue behind it.
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            self.seen.lock().unwrap().push(events.len());
            Ok(())
        }
    }

    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let transport = Arc::new(SlowFirst {
        seen: seen.clone(),
        calls: AtomicU64::new(0),
    });
    let client = OpenLineageClient::new(transport.clone());
    for _ in 0..50 {
        client.emit(event());
    }
    client.shutdown().await;

    let batches = seen.lock().unwrap().clone();
    let total: usize = batches.iter().sum();
    assert_eq!(total, 50, "every event must be delivered exactly once");
    assert!(
        batches.iter().any(|&n| n > 1),
        "a burst behind a slow delivery must coalesce into a batch > 1, got {batches:?}"
    );
}

#[tokio::test]
async fn emit_never_blocks_on_a_slow_upstream() {
    // A transport that hangs forever. emit() must remain non-blocking, and once
    // the bounded queue fills, events are dropped rather than stalling the caller.
    #[derive(Debug)]
    struct Hang;
    #[async_trait]
    impl Transport for Hang {
        async fn emit(&self, _event: &RunEvent) -> Result<(), TransportError> {
            std::future::pending::<()>().await;
            Ok(())
        }
    }

    let client = OpenLineageClient::with_queue_size(Arc::new(Hang), 8);
    // Emit far more than the queue holds; each emit must return immediately.
    for _ in 0..1000 {
        client.emit(event());
    }
    // The hung transport means the queue fills and excess events drop — proving
    // back-pressure never reaches the caller.
    assert!(
        client.dropped_count() > 0,
        "a hung upstream must cause drops, not block the hot path"
    );
}
