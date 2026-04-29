# Module 06 — Observability and Lineage

**Track:** Data Pipelines — Space Domain Awareness Fusion
**Position:** Module 6 of 6 *(final module of the track)*
**Source material:** *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 13 (Monitoring Kafka — Service-Level Objectives, Lag Monitoring, Diagnosing Cluster Problems); *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (operational considerations, provenance and audit trails); *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 2 (DataOps and Data Lineage as Undercurrents of the Data Engineering Lifecycle)
**Quiz pass threshold:** 70% on all three lessons to unlock the project

---

## Mission Context

> **OPS ALERT — SDA-2026-0245**
> **Classification:** OBSERVABILITY STAND-UP
> **Subject:** Make pipeline correctness visible to operations during incidents
>
> The Phase 5 pipeline is correct under load (M4), correct across restart (M5), correct in event time (M3), and correctly orchestrated (M2). It produces alerts the subscriber can trust. Last week's lag-detection incident took 3 hours to diagnose because the dashboard had pipeline-level metrics but not stage-level metrics, and the on-call engineer had to instrument the pipeline live to find the slow stage. Operations cannot observe the pipeline's correctness during an incident; the pipeline IS correct, but its correctness is invisible at exactly the moments when visibility matters most.

This module is the final piece. **Metrics** supply the aggregate signals that summarize behavior — the four golden signals plus lag, the SLI/SLO/SLA tracking discipline, the per-stage breakdowns that turn "the pipeline is slow" into "stage 4 is the bottleneck." **Lineage** supplies per-event traceability — given a wrong output, walk backward to find the contributing inputs; given a known-bad input, walk forward to find the affected outputs. **Distributed tracing** supplies per-event explanations — for a specific event, what was each operator doing when it processed it. The three together complete the observability stack: metrics for "is something wrong?", lineage for "which events?", tracing for "why?".

The pipeline at the end of this module is **correct, hardenable, restart-safe, and operationally legible**. Operations can diagnose any of the three common incident patterns within minutes; the dashboard surfaces the right signals at the right granularity; the runbook documents the standard procedures; the CLI tooling supports both real-time investigation and post-incident analysis. This is the production-quality data pipeline the SDA Fusion Service has been building toward for six modules.

The mental model the module installs is the three-component observability stack: (1) metrics emit aggregate signals at the right scope (per-stage, pipeline-level), (2) lineage emits per-event ancestry both backward-queryable (from output to inputs) and forward-queryable (from input to affected outputs), (3) tracing emits per-event-per-operator spans linked into traces via a propagated `trace_id`. Each component answers a different operational question; together they cover every diagnostic need that arises during an incident.

---

## Learning Outcomes

After completing this module, you will be able to:

1. Define the four golden signals for streaming pipelines (throughput, latency, errors, lag, saturation) and choose the right metric type (counter, gauge, histogram) for each measurement
2. Distinguish SLI, SLO, and SLA, and configure proactive alerting on SLO violations before they become SLA breaches
3. Implement per-event lineage tagging on the envelope with deterministic-hash sampling and top-K truncation at fan-in operators
4. Reason about lineage's two query directions — backward for post-incident analysis, forward for impact assessment — and build the indices that make each query efficient
5. Add distributed tracing spans to operators with `tracing::instrument`, propagate trace_id across operator boundaries via the envelope, and choose between head-based and tail-based sampling per the operational profile of each path
6. Inject canary observations as a pipeline-wide regression detector that catches what aggregate metrics smooth over
7. Apply the three diagnostic patterns (rising lag, wrong alert, DLQ spike) using metrics + lineage + tracing in a fixed reading order during incidents

---

## Lesson Summary

### Lesson 1 — Pipeline Metrics

The four golden signals for streaming pipelines (throughput, latency, errors, saturation) plus lag as the streaming-specific master diagnostic. SLI/SLO/SLA as three layered quality measurements. The three metric types matched to question shapes — wrong-type bugs produce misleading dashboards. Per-stage and pipeline-level metrics both required: pipeline-level for first-look "is something wrong?", per-stage for "which operator?". Lag split into source-lag and pipeline-lag answers "is it ours or theirs."

*Key question:* The dashboard shows nominal throughput, latency, and errors but lag is at 240s and rising. Per the lesson's diagnostic order, what is the on-call engineer's first action?

### Lesson 2 — Data Lineage

Event-level lineage as a first-class pipeline output. The `Option<LineageTrace>` envelope extension; per-operator append on emit; the storage cost (2^N at fan-in depth N) and three management strategies (1% sampling, top-K truncation, externalization to a graph database). Deterministic hash sampling for cross-replica reproducibility. Two query directions: backward for post-incident analysis, forward for impact assessment, with the forward index making impact queries O(K). OpenLineage as the complementary cross-system standard at the dataset-and-job level.

*Key question:* A radar later found to have a calibration bug between 14:00-15:00 yesterday produced ~6,000 observations. The pipeline emitted ~80 alerts during the same window. Which alerts were affected, and which lineage query direction answers this efficiently?

### Lesson 3 — Debugging Under Load

Distributed tracing as the third layer of observability. Spans per-event-per-operator; trace_id propagation via the envelope; head-based sampling (deterministic hash, 1% default) and tail-based sampling (buffers spans, decides at sink). Canary observations as the regression detector for pipeline-wide subtle changes that aggregate metrics smooth over. The three diagnostic patterns the runbook documents: rising lag (split, per-stage, channel gradient, span tree); wrong alert (lineage backward + per-operator trace); DLQ spike (metadata, partner-side hypothesis, lineage forward, sda-reprocess after fix).

*Key question:* The on-call engineer is paged at 02:00 AM with rising lag. Per the diagnostic discipline, what should the first action be, and why is reading the metrics layer the right starting point?

---

## Capstone Project — Fusion Pipeline Observability Stack

Wrap the M5 pipeline in the complete observability stack. Per-stage metrics for the four golden signals plus lag; SLO compliance calculator with proactive alerting; lineage tagging at 1% sampling with top-K truncation; distributed tracing with head-based + tail-based sampling; canary injector and watcher; a Grafana dashboard JSON; an operational runbook documenting the three diagnostic patterns. The deliverable is the production-quality SDA Fusion Service: ingest → orchestrate → window → backpressure → exactly-once → observe. Acceptance criteria, suggested architecture, runbook structure, and the full project brief are in `project-observability-stack.md`.

The orchestrator from M2, the windowed correlator from M3, the priority-aware shedding from M4, and the resilience tooling from M5 are all unchanged in structure. The new components are wrappers and side cars that observe the running pipeline; the operator graph grows by a few nodes (canary injector, canary watcher, tail-sampler).

---

## File Index

```
module-06-observability-and-lineage/
├── README.md                                  ← this file
├── lesson-01-pipeline-metrics.md              ← Pipeline metrics
├── lesson-01-quiz.toml                        ← Quiz (5 questions)
├── lesson-02-data-lineage.md                  ← Data lineage
├── lesson-02-quiz.toml                        ← Quiz (5 questions)
├── lesson-03-debugging-under-load.md          ← Debugging under load
├── lesson-03-quiz.toml                        ← Quiz (5 questions)
└── project-observability-stack.md             ← Capstone project brief
```

---

## Prerequisites

* Modules 1 through 5 completed — the `Observation` envelope, the orchestrator with supervisor, the watermark-aware windowed correlator, the per-edge `FlowPolicy` discipline, and the at-least-once + idempotent + checkpointed + DLQ'd pipeline are all assumed
* Foundation Track completed — async Rust, channels, runtime intuitions
* Familiarity with the `tracing` crate's `instrument` macro and the `metrics` crate's counter/gauge/histogram primitives
* Working familiarity with Prometheus metric types and PromQL-style rate/percentile queries; comfort reading a Grafana dashboard JSON

## What Comes Next

This is the final module of the Data Pipelines track. The pipeline at the end of M6 is correct, hardenable, restart-safe, and operationally legible — every piece of the production-streaming-pipeline stack is in place. The patterns the six modules installed generalize beyond SDA: every streaming pipeline that operates at scale (financial trading, ad-tech, IoT telemetry, distributed log processing) uses some combination of these techniques. The Meridian Space Academy curriculum's framing was specific; the techniques are universal.

The next track in the curriculum (Data Lakes — Artemis Base Cold Archive) takes the M6 pipeline's emitted alerts and builds the durable archive layer beneath them. The track after that (Distributed Systems — Constellation Network) takes the single-process pipeline and distributes it across the 48-satellite compute grid. Both build on the foundation this track establishes.

The SDA Fusion Service is now a production-quality data pipeline. The Data Pipelines track is complete.
