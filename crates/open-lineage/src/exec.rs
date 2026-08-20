//! Physical-plan wrapper that emits COMPLETE / FAIL at end of execution.
//!
//! [`crate::rule::OpenLineageQueryPlanner`] emits START at plan time and carries
//! a COMPLETE template through the plan in a [`crate::rule::LineageMarker`];
//! [`crate::rule::LineageExtensionPlanner`] lowers that marker into an
//! [`OpenLineageExec`] at the physical root. This node observes the result
//! streams and emits exactly one terminal event once every output partition has
//! finished:
//!
//! - COMPLETE when all partitions drain successfully;
//! - FAIL (with an `errorMessage` run facet) if any partition yields an error
//!   or is dropped before its stream is exhausted (e.g. a cancelled query).
//!
//! Completion is tracked with a `Drop`-based guard so cancellation is handled
//! without special-casing, and the terminal event fires under the *same*
//! `runId` the START used.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use chrono::Utc;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;

use crate::client::OpenLineageClient;
use crate::event::{RunEvent, RunEventType};
use crate::facets::{
    BaseFacet, ErrorMessageRunFacet, InputStatisticsInputDatasetFacet,
    OutputStatisticsOutputDatasetFacet,
};

const ERROR_FACET: &str = "1-0-1/ErrorMessageRunFacet.json";
const OUTPUT_STATS_FACET: &str = "1-0-2/OutputStatisticsOutputDatasetFacet.json";
const INPUT_STATS_FACET: &str = "1-0-0/InputStatisticsInputDatasetFacet.json";

/// Shared completion state across the partitions of one query run.
///
/// The `Mutex` fields are locked with `.unwrap()`: the only way to poison one
/// is to panic while holding it, which the short critical sections here never
/// do (they touch plain data, never call back into user code). Recovering from
/// poisoning is therefore intentionally not handled.
struct RunState {
    client: OpenLineageClient,
    /// COMPLETE event template (cloned and mutated into FAIL on error).
    complete: RunEvent,
    producer: String,
    /// The wrapped plan, read for native runtime metrics on completion. Tracked
    /// through `with_new_children` rewrites so metrics come from the node that
    /// actually executed.
    inner: std::sync::Mutex<Arc<dyn ExecutionPlan>>,
    /// Outstanding partitions yet to finish. Initialized lazily from the
    /// executing node's partition count on the first `execute()` (see
    /// [`RunState::init_partitions`]) so it stays correct across
    /// `with_new_children` rewrites that change the partitioning.
    remaining: AtomicUsize,
    /// Guards one-time initialization of `remaining`.
    init: std::sync::Once,
    /// Set if any partition observed an error or was dropped early.
    failed: AtomicBool,
    /// First error message observed, for the FAIL facet.
    error: std::sync::Mutex<Option<String>>,
    /// Whether this run has an output dataset. The write-result `count`-batch
    /// sniffing in [`TrackedStream`] only applies to writes, so it is gated on
    /// this: a read whose result happens to be a single `UInt64` `count` column
    /// must not be mistaken for a rows-written signal. Mirrors the
    /// `outputs`-non-empty guard in [`RunState::attach_output_statistics`].
    has_outputs: bool,
    /// Rows written, summed from DataFusion's write-result `count` batches
    /// (`Some` once any write count is observed).
    rows_written: std::sync::Mutex<Option<u64>>,
    /// Guards against emitting more than once (e.g. zero-partition plans).
    emitted: AtomicBool,
}

impl RunState {
    /// Initialize the outstanding-partition counter from the count of
    /// partitions that will actually execute. Called on every `execute()`;
    /// the `Once` ensures only the first call wins, so concurrent partition
    /// executes observe a stable total. `count` is the executing node's
    /// `output_partitioning().partition_count()`.
    fn init_partitions(&self, count: usize) {
        self.init.call_once(|| {
            // A plan may report zero partitions; guard so we still emit once.
            self.remaining.store(count.max(1), Ordering::SeqCst);
        });
    }

    fn record_error(&self, message: String) {
        self.failed.store(true, Ordering::SeqCst);
        let mut slot = self.error.lock().unwrap();
        if slot.is_none() {
            *slot = Some(message);
        }
    }

    /// Accumulate rows written, observed from a write-result `count` batch.
    fn record_rows_written(&self, rows: u64) {
        let mut slot = self.rows_written.lock().unwrap();
        *slot = Some(slot.unwrap_or(0) + rows);
    }

    /// Mark one partition finished; emit the terminal event when the last one
    /// completes. Safe to call once per partition (including from `Drop`).
    fn partition_finished(&self) {
        // `remaining` starts at the partition count; the partition that brings
        // it to zero emits.
        if self.remaining.fetch_sub(1, Ordering::SeqCst) != 1 {
            return;
        }
        self.emit_terminal();
    }

    fn emit_terminal(&self) {
        if self.emitted.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut event = self.complete.clone();
        // The template's `eventTime` was set at plan time; refresh it to the
        // moment execution actually ended so run duration is meaningful.
        event.event_time = Utc::now().to_rfc3339();
        if self.failed.load(Ordering::SeqCst) {
            event.event_type = RunEventType::Fail;
            let message = self
                .error
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| "query failed".to_string());
            event.run.facets.error_message = Some(ErrorMessageRunFacet {
                base: BaseFacet::new(&self.producer, ERROR_FACET),
                message,
                programming_language: "Rust".to_string(),
                stack_trace: None,
            });
        } else {
            self.attach_output_statistics(&mut event);
            self.attach_input_statistics(&mut event);
        }
        self.client.emit(event);
    }

    /// Attach an `outputStatistics` facet to each output dataset of the COMPLETE
    /// event.
    ///
    /// The row count comes from DataFusion's write-result `count` batch (the
    /// authoritative rows-written signal, captured as the stream drained). Size
    /// is taken from a `bytes_scanned`-style plan metric when available. Reads
    /// (SELECT) have no output dataset, so this is a no-op there.
    fn attach_output_statistics(&self, event: &mut RunEvent) {
        if event.outputs.is_empty() {
            return;
        }

        let row_count = self.rows_written.lock().unwrap().map(|n| n as i64);

        // `bytes_scanned` is the closest widely-emitted size metric; absent for
        // many plans, in which case `size` is simply omitted.
        let size = self
            .inner
            .lock()
            .unwrap()
            .metrics()
            .and_then(|m| m.aggregate_by_name().sum_by_name("bytes_scanned"))
            .map(|v| v.as_usize() as i64);

        if row_count.is_none() && size.is_none() {
            return;
        }

        let stats = OutputStatisticsOutputDatasetFacet {
            base: BaseFacet::new(&self.producer, OUTPUT_STATS_FACET),
            row_count,
            size,
            file_count: None,
        };
        for output in &mut event.outputs {
            let facets = output.output_facets.get_or_insert_with(Default::default);
            facets.output_statistics = Some(stats.clone());
        }
    }

    /// Attach an `inputStatistics` facet to the input dataset — but only when
    /// there is exactly ONE input.
    ///
    /// Scan metrics (`output_rows`, `bytes_scanned`) live on the per-node
    /// `MetricsSet` of the scan nodes, not the root, so we walk the executed
    /// plan tree and aggregate them. With a single input that aggregate is
    /// unambiguously that dataset's read stats. With multiple inputs we cannot
    /// attribute a summed total to the right source without matching each scan
    /// node back to its dataset — which needs location-based dataset naming
    /// (object-store URL + symlinks). That is deferred; see the design doc, so
    /// we skip rather than emit a misleading aggregate.
    fn attach_input_statistics(&self, event: &mut RunEvent) {
        if event.inputs.len() != 1 {
            return;
        }

        let inner = self.inner.lock().unwrap().clone();
        // A dataset scanned more than once (self-join, self-union, correlated
        // subquery) collapses to a single input dataset, but the metric
        // aggregate sums over every leaf scan and would over-count. Only a
        // single leaf scan can be unambiguously attributed to the lone input,
        // so skip rather than emit a doubled total.
        if count_leaf_scans(&inner) != 1 {
            tracing::trace!(
                target: "openlineage",
                "skipping inputStatistics: dataset scanned by multiple leaf nodes"
            );
            return;
        }
        let (rows, bytes) = aggregate_scan_metrics(&inner);
        let row_count = rows.map(|n| n as i64);
        let size = bytes.map(|n| n as i64);
        if row_count.is_none() && size.is_none() {
            return;
        }

        let stats = InputStatisticsInputDatasetFacet {
            base: BaseFacet::new(&self.producer, INPUT_STATS_FACET),
            row_count,
            size,
            file_count: None,
        };
        let facets = event.inputs[0]
            .input_facets
            .get_or_insert_with(Default::default);
        facets.input_statistics = Some(stats);
    }
}

/// Sum scan metrics across the whole executed plan tree.
///
/// `metrics()` is per-node, so we recurse. Returns aggregated
/// (`output_rows`, `bytes_scanned`); either may be `None` if no node reported it.
fn aggregate_scan_metrics(plan: &Arc<dyn ExecutionPlan>) -> (Option<usize>, Option<usize>) {
    let mut rows: Option<usize> = None;
    let mut bytes: Option<usize> = None;

    if let Some(metrics) = plan.metrics() {
        let metrics = metrics.aggregate_by_name();
        // Only count rows from leaf scans: intermediate nodes also report
        // `output_rows` and would double-count. Leaf-ness (no children) is the
        // right discriminator — `bytes_scanned` is parquet-specific, so gating
        // on it dropped `output_rows` for memory/CSV/JSON/custom sources.
        if plan.children().is_empty() {
            if let Some(r) = metrics.output_rows() {
                *rows.get_or_insert(0) += r;
            }
            if let Some(b) = metrics.sum_by_name("bytes_scanned") {
                *bytes.get_or_insert(0) += b.as_usize();
            }
        }
    }

    for child in plan.children() {
        let (cr, cb) = aggregate_scan_metrics(child);
        if let Some(r) = cr {
            *rows.get_or_insert(0) += r;
        }
        if let Some(b) = cb {
            *bytes.get_or_insert(0) += b;
        }
    }

    (rows, bytes)
}

/// Count the leaf scan nodes in the executed plan tree.
///
/// Used to detect when a single input dataset is read by more than one scan
/// (self-join/union/correlated subquery), where the summed scan-metric
/// aggregate would over-count that dataset's read statistics.
fn count_leaf_scans(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let children = plan.children();
    if children.is_empty() {
        return 1;
    }
    children.iter().map(|child| count_leaf_scans(child)).sum()
}

/// Wraps the root physical plan, emitting a terminal lineage event when
/// execution finishes.
pub struct OpenLineageExec {
    inner: Arc<dyn ExecutionPlan>,
    state: Arc<RunState>,
}

impl OpenLineageExec {
    /// Wrap `inner`, emitting COMPLETE (or FAIL on error) once all partitions
    /// finish. `complete` is the pre-built COMPLETE event (sharing the run id
    /// used by START); `producer` builds the error facet on failure.
    pub fn new(
        inner: Arc<dyn ExecutionPlan>,
        client: OpenLineageClient,
        complete: RunEvent,
        producer: String,
    ) -> Arc<Self> {
        let has_outputs = !complete.outputs.is_empty();
        let state = Arc::new(RunState {
            client,
            complete,
            producer,
            has_outputs,
            inner: std::sync::Mutex::new(inner.clone()),
            // Initialized lazily on the first `execute()` from the partition
            // count of the node that actually runs (which may differ from
            // `inner` here after a `with_new_children` rewrite).
            remaining: AtomicUsize::new(0),
            init: std::sync::Once::new(),
            failed: AtomicBool::new(false),
            error: std::sync::Mutex::new(None),
            rows_written: std::sync::Mutex::new(None),
            emitted: AtomicBool::new(false),
        });
        Arc::new(Self { inner, state })
    }

    fn with_new_inner(&self, inner: Arc<dyn ExecutionPlan>) -> Arc<Self> {
        // Keep the shared run state pointed at the node that will execute, so
        // metrics are harvested from the right plan on completion.
        *self.state.inner.lock().unwrap() = inner.clone();
        Arc::new(Self {
            inner,
            state: self.state.clone(),
        })
    }
}

impl fmt::Debug for OpenLineageExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenLineageExec").finish_non_exhaustive()
    }
}

impl DisplayAs for OpenLineageExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "OpenLineageExec")
            }
            DisplayFormatType::TreeRender => write!(f, "OpenLineageExec"),
        }
    }
}

impl ExecutionPlan for OpenLineageExec {
    fn name(&self) -> &str {
        "OpenLineageExec"
    }

    // No `as_any` override: as of DataFusion 54 it is no longer an
    // `ExecutionPlan` method — the trait now requires `Any`, so downcasting
    // goes through `Any::downcast_ref` on `&dyn ExecutionPlan`, which resolves
    // to *this* wrapper (not the inner plan). That is exactly the behavior the
    // former override existed to guarantee: visitors can't downcast past the
    // wrapper to the inner type and silently drop this lineage node.

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // We wrap a single root; rewrap whatever child we're given so the node
        // stays installed across optimizer/child rewrites.
        let child = children.pop().unwrap_or_else(|| self.inner.clone());
        Ok(self.with_new_inner(child))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Lazily fix the outstanding-partition count from the node that is
        // actually executing, so `with_new_children` rewrites that change the
        // partitioning don't desync the counter (reading `self.properties()`,
        // which delegates to `inner`, rather than the count captured at
        // construction). `init_partitions` floors the count at 1 so that if a
        // plan reporting zero partitions is nonetheless executed, the terminal
        // event still fires exactly once. (A zero-partition plan that is never
        // executed emits nothing — correctly, there was no execution.)
        self.state
            .init_partitions(self.properties().output_partitioning().partition_count());

        // An execute-time error (e.g. object-store auth / credential vending)
        // means this partition's stream never exists, so its `TrackedStream`
        // would never run its terminal path. Record the failure and settle the
        // partition here before propagating, or the run is stuck RUNNING with
        // no COMPLETE/FAIL ever emitted.
        let inner = match self.inner.execute(partition, context) {
            Ok(inner) => inner,
            Err(err) => {
                self.state.record_error(err.to_string());
                self.state.partition_finished();
                return Err(err);
            }
        };
        Ok(Box::pin(TrackedStream {
            schema: inner.schema(),
            inner,
            state: self.state.clone(),
            done: false,
        }))
    }
}

/// Wraps a partition's stream, recording errors and signalling completion on
/// terminal (exhaustion, error, or drop).
struct TrackedStream {
    schema: SchemaRef,
    inner: SendableRecordBatchStream,
    state: Arc<RunState>,
    done: bool,
}

impl TrackedStream {
    fn finish(&mut self) {
        if !self.done {
            self.done = true;
            self.state.partition_finished();
        }
    }
}

impl Stream for TrackedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Only sniff for a write-result `count` batch when this run
                // actually writes (has an output dataset); otherwise a read
                // returning a lone `UInt64 count` column would be misread as
                // rows-written.
                if self.state.has_outputs
                    && let Some(rows) = write_count(&batch)
                {
                    self.state.record_rows_written(rows);
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => {
                self.state.record_error(e.to_string());
                self.finish();
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                self.finish();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for TrackedStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for TrackedStream {
    fn drop(&mut self) {
        // A stream dropped before exhaustion means the partition was cancelled
        // or abandoned: count it as a failure for the run.
        if !self.done {
            self.state
                .record_error("query stream dropped before completion".to_string());
            self.finish();
        }
    }
}

/// Recognize DataFusion's write-result batch — a single `count` UInt64 column
/// whose value is the number of rows written — and return that count.
fn write_count(batch: &RecordBatch) -> Option<u64> {
    use datafusion::arrow::array::{Array, UInt64Array};

    if batch.num_columns() != 1 || batch.schema().field(0).name() != "count" {
        return None;
    }
    let counts = batch.column(0).as_any().downcast_ref::<UInt64Array>()?;
    Some(counts.iter().flatten().sum())
}
