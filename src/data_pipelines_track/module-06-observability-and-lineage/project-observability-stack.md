# Capstone Project — Fusion Pipeline Observability Stack

**Module:** Data Pipelines — M06: Observability and Lineage
**Estimated effort:** 1–2 weeks of focused work
**Prerequisites:** All three lessons in this module passed at ≥70%

---

## Mission Brief

> **OPS DIRECTIVE — SDA-2026-0245 / Phase 6 Implementation**
> **Classification:** OBSERVABILITY STAND-UP — FINAL TRACK MILESTONE
>
> The pipeline at the start of Phase 6 is correct under load (M4),
> correct across restart (M5), correct in event time (M3), and
> correctly orchestrated (M2). It produces alerts the subscriber can
> trust to be exactly-once-effective. There is one remaining gap: the
> pipeline's correctness is invisible to operations during incidents.
> Last week's lag-detection incident took 3 hours to diagnose because
> the operational dashboard had pipeline-level metrics but not
> per-stage breakdowns, and the on-call engineer had to instrument
> the pipeline live to find the slow stage. The post-incident review
> concluded that observability — metrics, lineage, tracing — is the
> remaining missing piece.
>
> Phase 6 installs the complete observability stack. The four golden
> signals plus lag, structured per-stage; SLI/SLO/SLA tracking with
> proactive alerting on SLO violations; per-event lineage at 1%
> sampling with backward and forward query support; distributed
> tracing with head-based + tail-based sampling; canary observations
> as the regression detector; an operational runbook that documents
> the diagnostic patterns for the three common symptom shapes.
>
> Success criteria for Phase 6: the dashboard's primary panels show
> all five signals (throughput, latency, errors, lag, saturation) per
> stage and pipeline-level. The SLO compliance ratio is queryable as
> a gauge. A trained on-call engineer can diagnose any of the three
> incident patterns (rising lag, wrong alert, DLQ spike) within 5
> minutes of a page using only the dashboard, lineage CLI, and
> tracing UI. Canaries fire on any pipeline-wide regression. This is
> the final module of the Data Pipelines track.

---

## What You're Building

The complete operational observability stack for the SDA Fusion Service.

1. **Per-stage metrics** instrumented on every operator: `operator_events_total`, `operator_latency_seconds`, `operator_errors_total{kind=...}`. The L1 patterns applied to every operator in the pipeline graph.
2. **Pipeline-level lag** measured at the sink with the L1 split into source-lag and pipeline-lag, exported as `source_lag_seconds{source=...}` and `pipeline_lag_seconds{source=...}` histograms.
3. **SLO compliance calculator**: tracks the alert latency SLI against the 30-second / 99.9% target over a rolling 30-day window. Emits `slo_compliance_ratio` as a gauge; alerts fire when the gauge drops below 0.999.
4. **Lineage tagging on every operator** per L2: each operator's emit appends a `LineageStep` to the trace if sampling is enabled. Sampling rate 1% via deterministic hash; truncation at top-K parents at fan-in operators.
5. **Distributed tracing** per L3: every operator wrapped in `#[tracing::instrument(skip(payload), fields(observation_id))]`; trace_id propagated on the envelope; head-based sampling at 1% via deterministic hash.
6. **Tail-based sampling** for the alert path specifically: a tail-sampler buffers alert-path spans and emits all of them when the path errored or exceeded P99 latency.
7. **Canary injector + watcher**: synthetic observations every 30 seconds; canary-watcher at the sink reports `canary_arrival_latency_seconds`, `canary_late_total`. Alerts fire on canaries falling behind the 60-second budget.
8. **Operational dashboard JSON** committed to the repo: Grafana-format dashboard showing the four golden signals + lag per stage, SLO compliance, channel occupancy gradient, canary panel, DLQ growth rate.
9. **Operational runbook** in `docs/runbook-sda-pipeline.md` covering the three diagnostic patterns from L3 with concrete dashboard panel references and CLI tool commands.
10. **`sda-lineage` CLI tool** for backward and forward lineage queries against the sampled lineage corpus, supporting the L2 query directions.

The orchestrator from M2, all the operators from M3-M5, and the resilience tooling are unchanged in structure. The new components are wrappers and side cars that observe the running pipeline; the operator graph grows by a few nodes (canary injector, canary watcher, tail-sampler).

---

## Suggested Architecture

```
                    Pipeline (M2-M5, unchanged in structure)
   ┌────────────────────────────────────────────────────────────────┐
   │  Source ─→ Normalize ─→ Correlator ─→ Alert Sink ─→ Subscriber │
   └─────┬────────┬────────────┬───────────────┬────────────────────┘
         │        │            │               │
         │ instrumented  instrumented  instrumented  instrumented
         │ (events_total, latency, errors per stage)
         ▼        ▼            ▼               ▼
   ┌─────────────────────────────────────────────────────────────────┐
   │            Prometheus exporter (/metrics endpoint)              │
   └────────────────────────┬────────────────────────────────────────┘
                            ▼
                  ┌─────────────────────┐
                  │ Grafana dashboard:  │
                  │  - 5 golden signals │
                  │  - SLO compliance   │
                  │  - Channel gradient │
                  │  - Canary panel     │
                  │  - DLQ growth       │
                  └─────────────────────┘

   Trace flow:                          Lineage flow:
   ┌──────────┐                         ┌──────────────┐
   │ TraceId  │                         │ LineageStep  │
   │ on env   │                         │ appended per │
   │ if 1%-   │                         │ operator if  │
   │ sampled  │                         │ 1%-sampled   │
   └────┬─────┘                         └──────┬───────┘
        ▼                                      ▼
   ┌──────────┐    ┌──────────────┐    ┌──────────────┐
   │ tracing  │───▶│ Tail-sampler│    │ JSON-Lines   │
   │ spans    │    │ (alert path) │    │ corpus       │
   └────┬─────┘    └──────┬───────┘    └──────┬───────┘
        ▼                 ▼                   ▼
   ┌──────────────────────────────────┐   ┌──────────────┐
   │ Jaeger / OpenTelemetry collector │   │ sda-lineage  │
   └──────────────────────────────────┘   │ CLI tool     │
                                          └──────────────┘

   Canary flow:
   ┌────────┐                                    ┌────────┐
   │ Canary │ every 30s ─────────────────────────│ Canary │
   │ inj.   │ flows through pipeline normally    │ watcher│
   └────────┘                                    └────┬───┘
                                                      │
                                                ┌─────▼──────┐
                                                │ canary_*    │
                                                │ metrics     │
                                                └─────────────┘
```

---

## Acceptance Criteria

### Functional Requirements

- [x] Every operator instrumented with the L1 metrics: `operator_events_total{stage}`, `operator_latency_seconds{stage}` (histogram), `operator_errors_total{stage, error_kind}`
- [x] Sink-side lag operator emits `source_lag_seconds{source}` and `pipeline_lag_seconds{source}` per L1
- [x] `SloCompliance` calculator continuously emits `slo_compliance_ratio{name="alert_latency"}` as a gauge over a 30-day rolling window
- [x] Lineage tagging on every operator's emit; `Option<LineageTrace>` on the envelope; deterministic 1% hash sampling; top-K=4 truncation at fan-in operators
- [x] `#[tracing::instrument]` on every operator's per-event function; `skip` for the bulky payload, `fields` for observation_id and source_kind
- [x] Trace context propagation via `Option<TraceId>` on the envelope; head-based 1% sampling decision at the source operator
- [x] Tail-based sampler for the alert path: buffers alert-path spans for up to 30 seconds; emits all spans of any path that errored or exceeded its SLO latency target
- [x] Canary injector at every source emitting one canary every 30 seconds; canary-watcher at the alert sink emitting the L3 metrics
- [x] `sda-lineage` CLI tool with `backward <event_id>` and `forward <ancestor_id>` subcommands

### Quality Requirements

- [x] **Metric instrumentation overhead test**: a benchmark comparing per-operator latency with and without instrumentation; the instrumented version must be within 5% of the non-instrumented baseline
- [x] **SLO compliance correctness test**: a unit test that feeds known latency values into the compliance calculator and asserts the computed ratio matches the analytical answer
- [x] **Lineage round-trip test**: an integration test that runs the pipeline against a known event sequence and asserts the sampled lineage corpus contains the expected event_id → ancestor_ids relationships
- [x] **Canary regression-detection test**: an integration test that injects a deliberate slowdown in the correlator and asserts `canary_late_total` increments while real-event metrics remain nominal — the canary's value-add property
- [x] All tail-sampler decisions are documented per-policy; the policy logic is tested independently from the rest of the pipeline

### Operational Requirements

- [x] Grafana dashboard JSON committed to `dashboards/sda-pipeline.json`. Panels: top row shows pipeline-level (throughput, lag, error rate, SLO compliance); second row per-stage golden signals; third row channel-occupancy gradient + canary; fourth row DLQ growth + checkpoint age + recovery path
- [x] Runbook in `docs/runbook-sda-pipeline.md` covering: (1) reading the dashboard during an incident, (2) lag-rising diagnostic pattern, (3) wrong-alert diagnostic pattern, (4) DLQ-spike diagnostic pattern, (5) using the `sda-lineage` and `sda-reprocess` CLIs, (6) escalation paths
- [x] Per-error-kind documentation in the runbook: for each `DlqErrorKind` variant from M5 L4, a paragraph documenting the typical hypothesis and remediation
- [x] On-call training material: a 1-page summary of the dashboard layout and the diagnostic pattern decision tree, suitable for printing for the on-call rotation

### Self-Assessed Stretch Goals

- [ ] *(self-assessed)* End-to-end trace from radar source through correlator to alert subscriber visible in a tracing UI (Jaeger or similar) for any sampled event. Demonstrate with a screenshot showing the per-operator span tree.
- [ ] *(self-assessed)* SLO compliance > 99.9% over a 24-hour soak test at sustained nominal load with periodic 10x bursts (the M4 burst-simulator test pattern). Provide the compliance gauge time series.
- [ ] *(self-assessed)* The runbook's three diagnostic patterns are validated by tabletop exercise: an engineer who has not seen the runbook is given a synthetic incident scenario and the dashboard, and reaches the correct diagnosis within 5 minutes. Document the exercise's results.

---

## Hints

<details>

<summary>How do I integrate with Prometheus efficiently?</summary>

The `metrics` crate (the facade) plus `metrics-exporter-prometheus` (the backend) is the standard. Initialize the exporter in main; the `metrics::counter!` and `metrics::histogram!` macros become cheap operations that write to a thread-local registry. The exporter exposes the registry's contents at `/metrics` in Prometheus text format.

```rust,ignore
use metrics_exporter_prometheus::PrometheusBuilder;

#[tokio::main]
async fn main() -> Result<()> {
    PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], 9100))
        .install()?;
    // ... rest of pipeline init ...
}
```

Per-event metric emissions cost ~50 ns each — well below noise in any operator. The 5% overhead requirement in acceptance criteria is comfortably met.

</details>

<details>

<summary>How do I bound lineage size at fan-in operators?</summary>

Top-K truncation. At a fan-in operator with N parent observations, sort the parents by some ranking criterion (uncertainty weight, contribution score, or simply the first K in event-time order), keep the top K, drop the rest. K=4 is a reasonable starting point for SDA's correlator (most conjunctions are determined by 2-4 dominant observations).

```rust,ignore
fn truncate_to_top_k(parents: Vec<(Uuid, f64)>, k: usize) -> Vec<Uuid> {
    let mut p = parents;
    p.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    p.into_iter().take(k).map(|(id, _)| id).collect()
}
```

The truncation is documented per-operator with a comment; investigators reading lineage from a truncated source need to know the top-K bound is in effect. Module 6 dashboard's lineage panel surfaces this metadata.

</details>

<details>

<summary>How do I implement deterministic tail-based sampling?</summary>

The OpenTelemetry collector's tail-sampling-processor is the production reference. The pattern: every span is buffered in memory until its trace's full path is known (the OpenTelemetry collector uses a rolling window typically 30 seconds — long enough that any span will have completed but short enough that memory stays bounded). When a trace's path is complete, the policies are evaluated: if any policy matches, all spans of the trace are exported; otherwise, all are dropped.

For SDA's alert path, the policies are:
- "any span had error" (matches whenever an operator errored)
- "p99 latency exceeded" (matches when the trace's total latency exceeds the SLO)
- "explicit sample" (matches when the head-based sampler tagged it)

```yaml
# tail-sampling-processor configuration
policies:
  - name: error_traces
    type: status_code
    status_code: { status_codes: [ERROR] }
  - name: slow_traces
    type: latency
    latency: { threshold_ms: 30000 }
  - name: head_sampled
    type: boolean_attribute
    boolean_attribute: { key: head_sampled, value: true }
```

The configuration matches "any" rather than "all" — a trace is exported if any policy matches. This is the production default and the right shape for SLO-relevant paths.

</details>

<details>

<summary>How do I structure the runbook?</summary>

The pattern that works in incidents:

1. **Top-of-page table of contents** with the three symptom shapes as direct anchor links: rising lag, wrong alert, DLQ spike.
2. **Per-symptom diagnostic flowchart** with concrete actions: which dashboard panel to open, what value range is normal, what the next step is for each branch of the diagnosis.
3. **Per-error-kind documentation** for the DLQ: each `DlqErrorKind` variant gets a paragraph with the typical hypothesis, the verification step, and the remediation pattern.
4. **CLI tool reference** with the canonical commands for `sda-lineage` and `sda-reprocess` — what flags to use for the impact-assessment, post-incident-analysis, and dry-run scenarios.
5. **Escalation paths** at the bottom: when does the on-call engineer escalate, to whom, with what context.

The runbook is meant to be readable during an incident at 02:00 AM. Avoid prose; favor numbered steps and direct links to dashboard panels. Every CLI command is copy-pastable with concrete example arguments.

</details>

<details>

<summary>How do I run the canary correctness test deterministically?</summary>

The same `tokio::time::pause()` pattern from M2 L4 and M5. The test pauses the runtime, advances time deterministically, drives the pipeline with synthetic events plus a deliberate slowdown injection, asserts the canary metrics. Combine with a fixed-seed RNG for any randomness in the pipeline (the priority classifier from M4, the dedup set's eviction policy if it has any) so the test is reproducible.

```rust,ignore
#[tokio::test(start_paused = true)]
async fn canary_detects_slowdown() {
    let mut pipeline = build_test_pipeline().await?;
    pipeline.inject_slowdown(operator: "correlator", factor: 5).await;
    pipeline.run_with_canaries(injection_cadence: Duration::from_secs(30)).await;
    tokio::time::advance(Duration::from_secs(300)).await;
    let m = pipeline.metrics();
    assert!(m.get("canary_late_total") > 0,
            "canary system should detect the deliberate slowdown");
    let real = m.get_histogram("operator_latency_seconds")
        .quantile(0.99);
    assert!(real < Duration::from_secs(5),
            "real-event P99 should be unchanged because the slowdown affects only the canary path");
}
```

The test asserts the canary catches the regression that real-event metrics would have missed — the lesson's stated value-add of the canary system.

</details>

---

## Getting Started

Recommended order:

1. **Per-operator metrics instrumentation.** L1's pattern; wire `metrics::counter!`, `metrics::histogram!` into every operator. Verify the dashboard shows non-zero values at startup.
2. **Lag operator at the sink.** L1's split-lag pattern; emit both source and pipeline lag per source.
3. **SLO compliance calculator.** L1's per-window percentile + threshold check; emit `slo_compliance_ratio`.
4. **Lineage tagging.** Add `Option<LineageTrace>` to the envelope; instrument every operator's emit logic; implement deterministic-hash sampling at the source.
5. **`sda-lineage` CLI tool.** Read JSON-Lines lineage corpus, build forward index, expose backward and forward queries.
6. **Distributed tracing.** Add `Option<TraceId>` to the envelope; `#[tracing::instrument]` on every operator; configure tracing-subscriber to emit OpenTelemetry-format spans to a collector.
7. **Tail-sampling for the alert path.** Configure the OpenTelemetry collector's tail-sampling-processor with the three policies; verify by injecting a deliberate slow alert and confirming the trace is exported.
8. **Canary injector + watcher.** L3's pattern; emit canaries every 30 seconds at every source; watcher at sink reports the metrics.
9. **Grafana dashboard JSON.** Commit a complete dashboard with the panels described in the acceptance criteria. Test by importing into a local Grafana and confirming all panels render.
10. **Operational runbook.** Document the three diagnostic patterns with concrete dashboard panel references and CLI commands. Run a tabletop exercise to validate the runbook works in practice.

Aim for the metrics + dashboard combo working end-to-end by day 5; lineage and tracing land in the second week along with the runbook polish. The chaos-engineering stretch goal (the tabletop exercise) is end-of-week-2 finishing work that produces the strongest validation of the whole stack.

---

## What This Module Completes

This is the final module of the Data Pipelines track. The pipeline at the end of M6 is **correct, hardenable, restart-safe, and operationally legible** — every piece of the production-streaming-pipeline stack is in place. Operations can diagnose any of the three common incident patterns within minutes; the dashboard surfaces the right signals at the right granularity; the runbook documents the standard procedures; the CLI tooling supports both real-time investigation and post-incident analysis.

The patterns generalize beyond SDA. Every streaming pipeline that operates at scale — financial trading, ad-tech, IoT telemetry, distributed log processing — uses some combination of these six modules' techniques. The Meridian Space Academy curriculum's framing was specific (the SDA Fusion Service against debris fields and conjunction risk) but the techniques are universal. An engineer who has built the M2-M6 capstones has built every layer of a production streaming pipeline and can apply the same patterns to any other domain.

The next track in the Meridian Space curriculum (Data Lakes — Artemis Base Cold Archive) takes the M6 pipeline's emitted alerts and builds the durable archive layer beneath them. The track after that (Distributed Systems — Constellation Network) takes the single-process pipeline and distributes it across the 48-satellite compute grid. Both build on the foundation this track establishes.

The SDA Fusion Service is now a production-quality data pipeline. Phase 6 closes the engineering work.
