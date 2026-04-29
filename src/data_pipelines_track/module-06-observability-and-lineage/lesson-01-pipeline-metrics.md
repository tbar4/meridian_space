# Lesson 1 — Pipeline Metrics

**Module:** Data Pipelines — M06: Observability and Lineage
**Position:** Lesson 1 of 3
**Source:** *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 13 (Monitoring Kafka — Service-Level Objectives, Lag Monitoring, Metric Basics); *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 2 (DataOps as an Undercurrent of the Data Engineering Lifecycle)

---

## Context

The pipeline at the start of this module is correct under load (M4), correct across restarts (M5), correct in event time (M3), and correctly orchestrated (M2). It produces output that downstream subscribers can trust. There is one remaining gap: the pipeline's correctness is invisible to operations. Every internal property the previous modules built — the watermark progress, the per-channel occupancy gradient, the per-operator latency, the duplicate-absorption rate at the dedup sinks, the DLQ growth rate — is a property the engineers know exists but cannot easily *see* during incident response. The SDA-2026-0245 incident from this module's mission framing took three hours to diagnose because the operational dashboard had pipeline-level metrics (throughput, error rate) but not stage-level metrics (per-operator latency, channel occupancy), and the on-call engineer had to instrument the pipeline live to find the slow stage.

The fix is observability. **Metrics** (this lesson) supply the aggregate signals the dashboard summarizes — the four golden signals adapted for streaming pipelines, the SLI/SLO/SLA discipline applied to event-time-aware operators, and the per-stage breakdowns that turn "the pipeline is slow" into "stage 4 is the bottleneck." **Lineage** (Lesson 2) supplies the per-event traceability — given a wrong output, walk backward through the pipeline to find the contributing inputs and the operator that introduced the error. **Debugging under load** (Lesson 3) is the discipline that turns metrics and lineage into actionable diagnosis during incidents — sampling strategies for tracing, canary observations for regression detection, and the dashboard reading patterns for the three common failure shapes.

The pipeline is the same pipeline; this module wraps it in a layer that makes its correctness operationally legible. The capstone integrates all three pieces into the operational dashboard and the runbook that ops uses during incidents.

---

## Core Concepts

### The Four Golden Signals (for Streaming Pipelines)

Google's *Site Reliability Engineering* book identifies four golden signals for any user-facing service: **latency**, **traffic**, **errors**, **saturation**. The streaming pipeline analogues are similar but with one important addition: **lag** as a fifth signal that subsumes the unique time dimension of event-time pipelines.

* **Throughput** (traffic). Events per second crossing a stage. Per-stage and per-pipeline. Counter type. The simplest signal — confirms the pipeline is running and indicates bottleneck symptoms when a downstream throughput is below an upstream's.
* **Latency** (latency). Per-event processing time, measured as `emit_time - enqueue_time` per stage. Histogram type so percentiles (P50, P95, P99) are queryable. The signal that catches operator-internal slowdowns.
* **Errors** (errors). Errors per second per operator, split by `error_kind` to match the L4 DLQ classifier from M5. Counter type with a label per kind. A sudden change in the error rate is operationally meaningful — typically a partner-side change or a deploy regression.
* **Lag** (the streaming-specific signal). The gap between event time and processing time. `lag = now - sensor_timestamp` for an in-flight observation. Gauge type per source plus an aggregated pipeline-level value. The master diagnostic for "is the pipeline keeping up with real time" — a question that throughput alone cannot answer.
* **Saturation** (saturation). Per-channel occupancy from M4 L3. Gauge type per edge. The signal that identifies the pressure-gradient bottleneck.

The five together cover every operational question the SDA pipeline gets asked during an incident. The dashboard's primary panels show all five at once; the runbook reads them in a fixed order.

### SLI / SLO / SLA

The acronyms are precise and the distinction matters operationally.

**SLI** — Service-Level Indicator. The metric you measure. For SDA's conjunction-alert path, the canonical SLI is `P99 of (alert_emit_time - sensor_timestamp)` over the last hour. A number, not a target.

**SLO** — Service-Level Objective. The target the SLI must meet. For SDA, the SLO is `P99 alert latency < 30 seconds, 99.9% of the time over a 30-day window`. A target, internally agreed.

**SLA** — Service-Level Agreement. The contractual obligation to downstream consumers. For SDA's pipeline, the alert subscriber's SLA is "alerts arrive within 60 seconds of event time, with 99.5% reliability." Less aggressive than the SLO, by design — the SLO is the internal goal; the SLA is the external promise; the gap is the buffer that protects the SLA when the SLO momentarily slips.

The discipline is to track all three and alert on SLO violations *before* they become SLA violations. The SLO calculator from this lesson's code examples computes the SLI's value over the rolling window and compares against the SLO target; an alert fires when the SLO is at risk (SLI trending toward the target's edge with non-trivial probability of crossing). Module 5's checkpoint-age and DLQ-rate metrics from L3-L4 contribute to the SLO compliance picture; if checkpoints stall or DLQ entries spike, the alert latency SLI is at risk.

### Metric Types: Counter, Gauge, Histogram

Choosing the right metric type matters because a wrong choice produces a misleading dashboard. The three primary types in Prometheus-style metrics:

**Counter**. Monotonically increasing. Reset on process restart. Used for *cumulative event counts*: `events_total`, `errors_total{kind=...}`, `dlq_entries_total`, `retractions_emitted_total`. The dashboard derives rates as `rate(counter[window])` — the per-second rate over the window.

**Gauge**. Instantaneous value, can go up or down. Used for *current state*: `channel_occupancy{edge=...}`, `pending_windows{operator=...}`, `pipeline_watermark_seconds`, `consumer_lag_seconds`. The dashboard shows the current value or a recent average.

**Histogram**. Distribution of values, used for *percentile queries*: `latency_seconds{stage=...}`, `checkpoint_pause_duration_ms`, `source_lag_seconds{source=...}`. The dashboard queries P50, P95, P99 from the histogram's buckets.

Wrong-type bugs show up as misleading dashboards. A counter for "current backlog" is wrong because the dashboard shows ever-growing values that don't reflect the actual current state. A gauge for "events processed this minute" is wrong because the gauge does not survive process restarts and the rate calculation is broken across them. A histogram for an integer enum (like `priority_classifier_decision`) is wrong because the buckets cannot meaningfully represent the values. Choose the type that matches the question being asked.

### Per-Stage vs Pipeline-Level

Two scopes of metrics, both required.

**Pipeline-level** metrics summarize end-to-end behavior. `pipeline_throughput_total`, `pipeline_lag_seconds`, `slo_compliance_ratio`. Useful for ops dashboards aimed at "is the pipeline healthy?" Aggregate signals; tell you something is wrong but not where.

**Per-stage** metrics localize to specific operators. `operator_throughput_total{stage=normalize}`, `operator_latency_seconds{stage=correlator}`, `channel_occupancy{edge=correlator->alert_sink}`. Useful during diagnosis aimed at "which operator is slow?" Localize the problem to the smallest component.

The two complement each other. The dashboard's primary panels are pipeline-level (the on-call engineer's first look). Drill-down panels are per-stage (the on-call engineer's investigation tool). Both are required because pipeline-level alone tells you something is wrong but does not tell you where; per-stage alone is too granular for the first-look summary.

### Lag as the Master Diagnostic

Lag — `now - sensor_timestamp` for the most recent emission — answers the single most important operational question for an event-time pipeline: *is the pipeline keeping up with real time?* Throughput tells you how many events per second the pipeline is processing; latency tells you how long each event takes; neither tells you whether the pipeline is current with the world it is observing.

A pipeline can have high throughput and low latency and still be hours behind in lag. The shape: a partner API outage causes the source to back up; the source's input queue grows; when the partner recovers, the source drains the backlog at full speed (high throughput) and processes each event quickly (low latency) — but the events being processed are hours old in event time. Lag is the metric that surfaces this.

For SDA's pipeline, lag is split into source lag (`ingest_timestamp - sensor_timestamp` per source) and pipeline lag (`now - ingest_timestamp` for in-flight events) per Module 3 L1. The dashboard shows both, distinguishing "is the source slow?" from "is the pipeline slow?" The split is what makes the lag diagnostic actionable rather than just descriptive.

---

## Code Examples

### Instrumenting an Operator with the Prometheus Crate

Every operator in the pipeline gets per-stage metrics for the four golden signals. The `prometheus` Rust crate gives a registry-and-metric-handle pattern; the orchestrator's startup code creates the registry and shares it across operators.

```rust,ignore
use anyhow::Result;
use std::time::Instant;
use tokio::sync::mpsc;

pub async fn run_operator_instrumented(
    name: &str,
    mut input: mpsc::Receiver<Observation>,
    output: mpsc::Sender<Observation>,
) -> Result<()> {
    // Counter for throughput — increments on every successful process.
    let events_total = metrics::counter!("operator_events_total", "stage" => name.to_string());
    // Counter for errors, labeled by error kind.
    let errors_total = metrics::counter!("operator_errors_total", "stage" => name.to_string());
    // Histogram for per-event latency (Prometheus default buckets work for SDA).
    let latency_seconds = metrics::histogram!("operator_latency_seconds", "stage" => name.to_string());

    while let Some(obs) = input.recv().await {
        let started = Instant::now();
        match process(obs.clone()).await {
            Ok(processed) => {
                output.send(processed).await
                    .map_err(|_| anyhow::anyhow!("downstream dropped"))?;
                events_total.increment(1);
                latency_seconds.record(started.elapsed().as_secs_f64());
            }
            Err(e) => {
                errors_total.increment(1);
                tracing::warn!(?e, stage = %name, "operator error");
                // ... DLQ classification per M5 L4 ...
            }
        }
    }
    Ok(())
}
```

Three things to notice. The metric handles are created at the top of the function and reused on every event; the `metrics::counter!` macro is cheap (single hashmap lookup) but not free, and creating per-event handles would dominate the hot path. The latency histogram records `elapsed().as_secs_f64()` rather than millis — the Prometheus convention is seconds with float precision, which lets the histogram's buckets (default: 0.005s, 0.01s, ..., 10s) cover the realistic latency range of any pipeline operator. The errors counter is incremented in the `Err` arm, which is the single place where error counting belongs; the DLQ classification logic from M5 L4 hooks into the same arm via the operator's classifier function.

### A Lag-Computing Sink Operator

The lag operator sits at or near the pipeline's sink end. It computes both source lag and pipeline lag for every emitted event and exports them as histograms.

```rust,ignore
use std::time::{Duration, SystemTime};

pub fn observe_lag(obs: &Observation) {
    let now = SystemTime::now();

    // Source lag: the gap between when the sensor recorded the event
    // and when the pipeline received it. Driven by the source's
    // upstream behavior (network paths, partner APIs).
    let source_lag = obs.ingest_timestamp
        .duration_since(obs.sensor_timestamp)
        .unwrap_or(Duration::ZERO);

    // Pipeline lag: the gap between when the pipeline received the
    // event and right now. Driven by the pipeline's own processing
    // time across all stages.
    let pipeline_lag = now.duration_since(obs.ingest_timestamp)
        .unwrap_or(Duration::ZERO);

    let kind = format!("{:?}", obs.source_kind);
    metrics::histogram!("source_lag_seconds", "source" => kind.clone())
        .record(source_lag.as_secs_f64());
    metrics::histogram!("pipeline_lag_seconds", "source" => kind)
        .record(pipeline_lag.as_secs_f64());
}
```

The split is the operationally important part. When the dashboard panel for `source_lag_seconds{source="optical"}` shows a 30-second P99 spike while `pipeline_lag_seconds` is unchanged, the on-call engineer knows the partner-side optical archive is the cause; pipeline operators are processing fine, the events are just arriving late. Conversely, a `pipeline_lag_seconds` spike with stable source lag points at the pipeline itself — a slow operator, a full channel, a stalled checkpoint. The split answers "is it ours or theirs" without ambiguity.

### An SLO Compliance Calculator

The SLO calculator computes the SLI's current value from the histogram and compares against the target. It runs as a small auxiliary task that emits a gauge for `slo_compliance_ratio` — a value between 0.0 and 1.0 indicating what fraction of the rolling window's events met the SLO target.

```rust,ignore
use anyhow::Result;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct SloDefinition {
    /// The target — e.g., Duration::from_secs(30) for the alert latency SLO.
    pub target: Duration,
    /// The percentile being measured — typically 0.99 for P99 SLOs.
    pub percentile: f64,
    /// The rolling window — e.g., Duration::from_secs(3600) for hourly SLO.
    pub window: Duration,
}

pub struct SloCompliance {
    name: String,
    definition: SloDefinition,
    /// In-process histogram of recently-observed values for the SLI.
    /// Production code uses Prometheus's histogram and queries it via
    /// the metrics registry; we use an in-memory histogram for clarity.
    histogram: Histogram,
}

impl SloCompliance {
    pub fn new(name: impl Into<String>, definition: SloDefinition) -> Self {
        Self {
            name: name.into(),
            definition,
            histogram: Histogram::new(),
        }
    }

    /// Record an SLI sample (e.g., one observation's emit-to-sensor lag).
    pub fn record(&mut self, value: Duration) {
        self.histogram.record(value);
    }

    /// Compute the SLO compliance ratio: fraction of window's events
    /// that met the SLO target. Used by the alerting rule and the
    /// dashboard's SLO panel.
    pub fn compliance(&self) -> f64 {
        let total = self.histogram.count_in_window(self.definition.window);
        if total == 0 { return 1.0; }
        let breaching = self.histogram.count_above(
            self.definition.target,
            self.definition.window,
        );
        let compliant = total - breaching;
        compliant as f64 / total as f64
    }

    /// Emit the compliance ratio as a gauge for the dashboard.
    pub fn export(&self) {
        let ratio = self.compliance();
        metrics::gauge!("slo_compliance_ratio", "name" => self.name.clone()).set(ratio);
    }
}

// (Histogram is a stand-in for the production Prometheus histogram
// query; the real implementation uses prometheus::Histogram.)
struct Histogram { /* ... */ }
impl Histogram {
    fn new() -> Self { Histogram {} }
    fn record(&mut self, _value: Duration) { /* ... */ }
    fn count_in_window(&self, _window: Duration) -> u64 { 0 }
    fn count_above(&self, _threshold: Duration, _window: Duration) -> u64 { 0 }
}
```

The compliance ratio (e.g., 0.999 for "99.9% of events met the SLO") is exactly the number the SLO target compares against — the SLO is "compliance ≥ 0.999 over rolling 30 days," and the alert fires when the gauge drops below the threshold for a sustained period. The SLO calculator's value is operational: the dashboard panel showing the compliance ratio over time is what the ops engineer reads to confirm the pipeline is meeting its commitments without drilling into latency histograms directly. The SLI/SLO/SLA discipline becomes legible when the compliance ratio is on the dashboard alongside the raw latency.

---

## Key Takeaways

* The **five signals** for streaming pipelines are throughput, latency, errors, lag, and saturation. Lag is the streaming-specific signal that subsumes the event-time dimension; it is the master diagnostic for "is the pipeline keeping up with real time?"
* **SLI / SLO / SLA** are the three layers of service-quality measurement. SLI is what you measure (a number), SLO is the target you commit to internally, SLA is the contract you make externally. Track all three; alert on SLO violations before they become SLA violations.
* The **three metric types** match different question shapes. Counter for cumulative counts (events, errors). Gauge for current state (occupancy, lag, watermark). Histogram for percentile queries (latency, pause duration). Wrong-type bugs produce misleading dashboards.
* **Per-stage** and **pipeline-level** metrics are both required. Pipeline-level for the first-look "is something wrong?" check; per-stage for the diagnosis "which operator?" question. The dashboard is structured to show pipeline-level prominently and per-stage as drill-down.
* **Lag split into source-lag and pipeline-lag** answers "is the problem ours or theirs?" without ambiguity. The split is the M3 L1 framing made operational; the dashboard panel that distinguishes them is what turns the lag diagnostic from descriptive to actionable.

---

{{#quiz lesson-01-quiz.toml}}
