# Lesson 3 — Debugging Under Load

**Module:** Data Pipelines — M06: Observability and Lineage
**Position:** Lesson 3 of 3
**Source:** *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 13 (Diagnosing Cluster Problems, The Art of Under-Replicated Partitions); *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (operational considerations); *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 2 (DataOps as an Undercurrent)

---

## Context

Metrics from Lesson 1 tell you the pipeline is broken. Lineage from Lesson 2 tells you which events were affected. Neither tells you *why* — for a specific event, what was the operator doing at the moment it produced the wrong output? What was on the worker thread? What did the operator's internal state look like when it processed this input?

Distributed tracing is the answer. **Spans** — discrete intervals of work with a name, start time, end time, and structured attributes — captured per event as it flows through operators. **Trace context** — a correlation ID propagated across operator boundaries via the envelope so the same event's spans across multiple operators link into a single trace. **Sampling** — the policy that decides which events get traced (because tracing every event at SDA's volumes is the same volume problem as full lineage from L2).

Together with metrics and lineage, tracing completes the operational debugging stack: metrics for "is something wrong?" — lineage for "which events?" — tracing for "why?" The capstone wires all three into the operational dashboard and the runbook. This lesson establishes the tracing piece and the discipline that turns the three together into an actionable diagnostic process during incidents. The lesson closes the Data Pipelines track; M6 is the final module, and the SDA Fusion Service curriculum is complete.

---

## Core Concepts

### Distributed Tracing for Pipelines

A *span* is a discrete unit of work. The streaming-pipeline analogue is per-event-per-operator: when an operator processes an event, the operator opens a span, does its work, closes the span. The span has a `name` (the operator's name), a `start_time` and `end_time`, and a set of structured attributes (the event_id, any operator-specific values like the window the event landed in). At the end of the operator's processing, the span is *exported* — sent to a tracing backend (Jaeger, Zipkin, OpenTelemetry collector) where it can be queried.

The Rust ecosystem's `tracing` crate is the de-facto standard. Each operator's processing function uses `#[tracing::instrument]` to wrap its body in a span automatically; the span's name defaults to the function name; attributes can be added via the macro's parameters or via runtime `record!` calls.

```rust,ignore
#[tracing::instrument(skip(obs), fields(observation_id = %obs.observation_id))]
async fn process_observation(obs: Observation) -> Result<ConjunctionRisk> {
    // ... operator body ...
}
```

The `skip` directive omits the entire `obs` payload from the span (it would balloon span size with the payload contents); the `fields` directive adds the specific attribute we care about for the trace. Production code is conservative about which attributes to record — too few and the trace is opaque; too many and the trace volume becomes the dominant operational cost.

### Trace Context Propagation

A trace covers a single event's path across multiple operators. For the spans across operators to link into one trace, the trace context (a unique 128-bit trace_id) must be propagated as the event flows through. The pattern is to attach the trace_id to the envelope:

```rust,ignore
pub struct Observation {
    // ... existing fields ...
    /// Optional trace_id for distributed tracing. None means
    /// 'this event is not being traced' (sampling decision).
    pub trace_id: Option<TraceId>,
}
```

When operator A processes an event with `trace_id: Some(t)`, it creates a span with that trace_id; the span is associated with the existing trace rather than a new one. When A emits to operator B, it copies the trace_id forward; B's span joins the same trace. The result is a single trace covering every operator's per-event work, queryable in the tracing backend as one tree.

The integration with `tracing::instrument` is via its parent-span detection: if the operator's body sets the current span's parent to the upstream's span, the spans link correctly. Production OpenTelemetry tooling handles this via context propagation across async boundaries; the SDA pipeline's wiring uses `tracing::Span::current()` and explicit `parent_span!` annotations.

### Sampling Strategies

Tracing every event at SDA's volumes is impractical (the same volume problem as full lineage). Two strategies for managing the cost.

**Head-based sampling.** The decision to trace is made at the source operator, before the event flows through the pipeline. The same hash-based deterministic approach from L2's lineage sampling applies: the source uses `hash(observation_id) % 100 < sample_pct` to decide. Cheap (single hash per event), reproducible (same event_id always produces the same decision), and produces a uniform sample. The downside: errors that occur mid-pipeline don't get extra tracing — the decision was made before the error appeared.

**Tail-based sampling.** The decision is made at the sink, after the event's processing is complete and the operator knows whether the path was interesting (errored, slow, or otherwise notable). Tail-based sampling captures more of the *interesting* events at the cost of buffering all events' spans until the decision is made. For SDA's scale, tail-based requires a separate tail-sampler service (the OpenTelemetry tail-sampling-processor) that buffers spans and applies policies at egress. More complex but catches errors that head-based misses.

The SDA pipeline uses head-based sampling at 1% by default, tail-based for the alert path specifically (where slow or errored alerts are operationally meaningful and head-based might miss them). The combination — head-based for general visibility, tail-based for SLO-relevant paths — is the production pattern.

### Canary Observations

Synthetic observations injected at the source on a regular cadence. The canary has a known `observation_id` and known properties; it flows through the pipeline like any other event; the canary-watcher at the sink confirms it arrived within an expected latency. The pattern catches three problems that other observability mechanisms miss.

**Pipeline-wide regression detection.** A code change that breaks the pipeline's overall correctness (an operator that drops events; a serialization mismatch; a wiring error) shows up as canaries not arriving. The metric `canary_arrived_total` counter falling behind `canary_emitted_total` is the regression signal.

**End-to-end latency under load.** Real events have variable processing time depending on the input; the canary's known content makes its expected processing time known. A spike in canary latency without a corresponding spike in real-event latency is informative — it points at the processing path itself rather than at variable input characteristics.

**Cold-start verification after deploys.** After a deploy, the first canaries through the new pipeline confirm the deploy succeeded. The canary's first arrival is the deploy-success signal.

For SDA, canaries are emitted every 30 seconds at the source. Each carries a deterministic content (a synthetic radar observation with a known timestamp and known orbital coordinates). The canary-watcher at the alert sink emits `canary_arrival_latency_seconds` as a histogram and `canary_missed_total` as a counter; alerts fire when the latency exceeds 60 seconds (twice the SLO) or when missed count grows.

### Diagnostic Patterns

The on-call engineer reads metrics, lineage, and tracing in a fixed order during an incident.

**Symptom: "lag is rising." Investigation:**
1. Check pipeline lag and source lag separately (M6 L1's split). If source lag is the dominant component, the cause is upstream of ingestion — investigate partner-side health.
2. If pipeline lag is the dominant component, check per-stage latency histograms (M6 L1's per-stage metrics). The slowest stage's P99 stands out.
3. Check the slow stage's incoming channel occupancy (M4 L3's gradient). A persistently full channel just upstream of the slow stage confirms the diagnosis.
4. Open the tracing UI for a recent slow-path event; the span tree shows where the per-event time is being spent.

**Symptom: "alert subscriber reports a wrong alert." Investigation:**
1. Get the alert's `event_id` from the subscriber.
2. Backward-walk the lineage (M6 L2) to find contributing inputs.
3. Open each contributing input's trace; the per-operator spans show what each operator did with it.
4. Compare against the canary's expected behavior — is the difference in the input or in the operator's processing?

**Symptom: "DLQ entries spiking from operator X." Investigation:**
1. Read the latest DLQ entries' `error_kind` distribution (M5 L4 metadata).
2. Pull a few `original_payload` samples; deserialize manually to confirm the partner-side change hypothesis.
3. Trace forward (M6 L2) from the partner's last-known-good observation_id to find when the change started.
4. Notify partner; once fixed, run `sda-reprocess` against the affected DLQ window.

The discipline is to have the order memorized and the tools at hand. The dashboard is structured to support this workflow; the runbook documents each pattern with concrete pointers to which dashboard panels and which queries to run.

---

## Code Examples

### Adding Tracing Spans to Operators

The minimal instrumentation. `#[tracing::instrument]` wraps the operator's per-event work in a span. The span's attributes include the event_id (for correlation with lineage and DLQ entries) and any operator-specific structured fields the engineer wants visible in the trace.

```rust,ignore
use tracing::{info, instrument};

#[instrument(
    skip(obs, output),
    fields(
        observation_id = %obs.observation_id,
        source_kind = ?obs.source_kind,
    ),
)]
async fn correlator_process(
    obs: Observation,
    output: &mpsc::Sender<ConjunctionRisk>,
) -> Result<()> {
    // The span is automatically created by the macro; it covers the
    // entire body of this function.
    info!("correlator received observation");
    let risk = compute_conjunction_risk(&obs).await?;
    output.send(risk).await?;
    info!("correlator emitted risk");
    Ok(())
}
```

The `skip` directive omits the bulky payload (`obs` and the output sender); `fields` adds the lightweight identifier. The `info!` calls inside the span add log lines that are correlated with the span automatically (the `tracing` crate's structured-log integration). Production code uses `info!` sparingly on the hot path — every log line is a serialization cost; `debug!` calls are the right verbosity for inner-loop instrumentation, with the log level configurable per operator at startup.

### A Head-Based Sampling Layer

The sampling decision is made at the source operator. The lesson's hash-based approach from L2 applies here unchanged: the same observation_id deterministically samples to the same decision, which makes the trace data reproducible across replicas.

```rust,ignore
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

pub struct TracingSampler {
    sample_rate_pct: u8,  // 0..=100
}

impl TracingSampler {
    pub fn new(sample_rate_pct: u8) -> Self {
        Self { sample_rate_pct: sample_rate_pct.min(100) }
    }

    pub fn should_trace(&self, observation_id: &Uuid) -> bool {
        let mut h = DefaultHasher::new();
        observation_id.hash(&mut h);
        let bucket = (h.finish() % 100) as u8;
        bucket < self.sample_rate_pct
    }
}

pub async fn run_source_with_tracing(
    sampler: &TracingSampler,
    mut output: mpsc::Sender<Observation>,
) -> Result<()> {
    loop {
        let mut obs = read_next_observation().await?;
        // Decide whether this event will be traced; if yes, generate a
        // trace_id and attach it. If no, leave trace_id as None.
        if sampler.should_trace(&obs.observation_id) {
            obs.trace_id = Some(TraceId::new_v4());
        }
        output.send(obs).await?;
    }
}
```

The deterministic hash means that for any given event_id, the same trace decision is made every time across every replica and across pipeline restarts. Investigators can reliably ask "do we have a trace for event X?" and get a stable answer; "yes, here it is" or "no, that event was not sampled." Random sampling produces a different answer per call.

### A Canary Injector and Watcher

The canary system is two operators: an injector at the source that emits a synthetic observation every 30 seconds, and a watcher at the sink that confirms each canary arrived within the expected latency.

```rust,ignore
use std::time::{Duration, SystemTime};
use tokio::time::interval;

pub async fn run_canary_injector(
    output: mpsc::Sender<Observation>,
    cadence: Duration,
) -> Result<()> {
    let mut tick = interval(cadence);
    loop {
        tick.tick().await;
        let canary = build_canary_observation();
        let canary_id = canary.observation_id;
        metrics::counter!("canary_emitted_total").increment(1);
        output.send(canary).await
            .map_err(|_| anyhow::anyhow!("downstream dropped"))?;
        tracing::info!(canary_id = %canary_id, "canary emitted");
    }
}

pub async fn run_canary_watcher(
    mut input: mpsc::Receiver<Observation>,
    expected_max_latency: Duration,
) -> Result<()> {
    while let Some(obs) = input.recv().await {
        if !is_canary(&obs) { continue; }
        let now = SystemTime::now();
        let latency = now.duration_since(obs.sensor_timestamp)
            .unwrap_or(Duration::ZERO);
        metrics::histogram!("canary_arrival_latency_seconds")
            .record(latency.as_secs_f64());
        if latency > expected_max_latency {
            metrics::counter!("canary_late_total").increment(1);
            tracing::warn!(
                canary_id = %obs.observation_id,
                latency_secs = latency.as_secs_f64(),
                "canary arrived late",
            );
        }
    }
    Ok(())
}

fn build_canary_observation() -> Observation {
    // The canary has deterministic content with a recent timestamp.
    // The orbital coordinates are a fixed test track that never
    // produces real conjunctions, so the canary cannot pollute alerts.
    Observation {
        observation_id: Uuid::new_v4(),
        sensor_timestamp: SystemTime::now(),
        source_kind: SourceKind::Canary,
        // ... deterministic test values for the rest of the envelope ...
        ingest_timestamp: SystemTime::now(),
        target: ObservationTarget::test_canary_target(),
        uncertainty: Uncertainty { sigma: 0.1 },
        trace_id: None,  // canaries are tagged separately, not via the sampler
        lineage: None,
    }
}

fn is_canary(obs: &Observation) -> bool {
    matches!(obs.source_kind, SourceKind::Canary)
}
```

The canary uses its own `SourceKind::Canary` variant so downstream operators can distinguish canaries from real observations — useful when an operator's behavior should differ for canaries (e.g., the alert sink should drop canary-derived alerts rather than emit them). The `is_canary` function provides a single point for that filtering. The metrics emitted (`canary_emitted_total`, `canary_arrival_latency_seconds`, `canary_late_total`) feed the dashboard's canary panel and the alert that fires when canaries fall behind.

---

## Key Takeaways

* **Distributed tracing** completes the observability stack alongside metrics and lineage. Spans (per-event-per-operator) capture the *what* and *when*; trace_id propagation links spans across operators into one trace per event; the tracing backend (Jaeger, Zipkin, OpenTelemetry collector) makes per-event behavior queryable.
* **Head-based sampling** (decide at source, deterministic hash, 1% default) is the standard. Tail-based sampling catches more interesting events at the cost of buffering; the SDA pipeline uses tail-based for SLO-relevant paths specifically.
* **Canary observations** are synthetic events injected every 30 seconds with known content; the canary-watcher at the sink confirms expected arrival. Catches pipeline-wide regressions, end-to-end-latency-under-load issues, and cold-start verification after deploys.
* **The diagnostic patterns** match three common symptom shapes: rising lag (split source lag vs pipeline lag, find the bottleneck stage, drill into spans), wrong alert (backward lineage walk + per-operator trace inspection), DLQ spikes (read DLQ metadata, partner-side hypothesis, forward lineage walk to find when the change started, sda-reprocess after fix).
* **The complete stack** — metrics for is-it-wrong, lineage for which-events, tracing for why — is what turns operational observability from descriptive to actionable. The dashboard is structured to support the read-in-fixed-order workflow; the runbook documents each pattern with concrete dashboard pointers.

---

{{#quiz lesson-03-quiz.toml}}
