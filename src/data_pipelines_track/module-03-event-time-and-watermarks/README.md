# Module 03 — Event Time and Watermarks

**Track:** Data Pipelines — Space Domain Awareness Fusion
**Position:** Module 3 of 6
**Source material:** *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (Reasoning About Time, Windowing, Knowing When You're Ready, Out-of-Order Events); *Streaming Data* — Andrew Psaltis, Chapter 4 (Analyzing Streaming Data, Windowing Patterns, Watermarks); *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 14 (Stream Processing Concepts: Time, Out-of-Sequence Events); *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 7 (Late-Arriving Data)
**Quiz pass threshold:** 70% on all four lessons to unlock the project

---

## Mission Context

> **OPS ALERT — SDA-2026-0142**
> **Classification:** CORRELATION TIER UPGRADE
> **Subject:** Replace processing-time dedup with event-time windowed correlation
>
> The Phase 2 orchestrator from Module 2 is in production. Its dedup operator at the top of the correlation tier is processing-time-based — buckets observations by arrival time, not by sensor_timestamp. Internal review of conjunction alerts from the past quarter found a 2.3% rate of cross-sensor mismatches traceable to optical-vs-radar arrival skew straddling the 5-second dedup window. Two of the missed correlations were later determined to be real conjunctions. The fix is structural: replace processing-time dedup with event-time windowed correlation that respects sensor_timestamp regardless of arrival skew, with watermarks driving window close and allowed-lateness handling the long tail of partner-API delays.

This module is the correctness foundation of the rest of the track. The orchestrator built in Module 2 stays unchanged — only one operator's implementation changes (dedup → correlator). What does change is the conceptual machinery: every event-time-aware operator from this point on consumes a watermark stream, maintains windowed state with explicit close triggers, and handles the rare late event explicitly rather than silently dropping it. The patterns this module installs — event-time-on-the-envelope, per-source max-lateness, min-of-inputs watermark propagation, allowed-lateness retention, retract-and-correct downstream emission — are the canonical streaming-system shape that Module 4 (backpressure and flow control), Module 5 (delivery guarantees and fault tolerance), and Module 6 (observability and lineage) all build on.

The mental model the module installs is the four-piece event-time discipline: (1) the envelope carries event time and ingest time as separate first-class fields, (2) operators bucket by event time using one of four canonical window shapes, (3) watermarks are guarantees that drive window close, propagated through fan-ins by the min rule, (4) late events past the watermark are handled explicitly per a per-output strategy. Every event-time pipeline that succeeds in production is some combination of these four; pipelines that skip any of them have correctness bugs that look like flakiness.

---

## Learning Outcomes

After completing this module, you will be able to:

1. Distinguish event time from ingest time on the observation envelope and choose the right time for any aggregation question
2. Reason about per-source clock heterogeneity (GPS-locked, NTP-synced, drifting) and carry source-specific quality forward through the pipeline
3. Choose among the four canonical window shapes (tumbling, hopping, sliding, session) based on the question being asked, not on the perceived complexity of each
4. Implement per-event sliding windows with bounded memory and correct event-time eviction, plus session windows with the production-safety double-bound
5. Define watermarks precisely as guarantees, generate them at sources from per-source max-lateness, and propagate them through fan-in operators via the min-of-inputs rule
6. Handle late events with the appropriate strategy — drop, allowed-lateness, or retract — and recognize which strategy fits which output
7. Compose the operator-level retract-and-correct pattern with M2's at-least-once-plus-idempotent delivery to produce effective exactly-once at the sink

---

## Lesson Summary

### Lesson 1 — Event Time vs Processing Time

The two operationally distinct timestamps every observation carries: `sensor_timestamp` (when the event happened in the world) and `ingest_timestamp` (when the pipeline received it). Sensor-clock heterogeneity carried forward as `ClockQuality` so per-source skew can widen the correlator's matching window. Out-of-order arrival as the rule. Lag (split into source lag and pipeline lag) as the master diagnostic for "is the problem ours or theirs."

*Key question:* A radar observation arrives 80 ms after its event time; an optical observation of the same event arrives 30 seconds after its event time. Should they be correlated, and which window-assignment strategy gets that right?

### Lesson 2 — Windowing

The four canonical window shapes — tumbling, hopping, sliding, session — and the question shape each fits. The conjunction-risk question fits per-event sliding windows. BTreeMap-keyed-on-window-end as the data structure that makes close-up-to-watermark a cheap O(log N) prefix query. Session windows' production-safety double-bound (gap timeout AND max session duration) as the safety valve that prevents unbounded growth.

*Key question:* The correlator must answer "for each new observation, find others within W of its event_time." Which window shape does that question fit, and what is the cost profile?

### Lesson 3 — Watermarks

The watermark as a per-event-time guarantee, not an estimate. Heuristic watermarks computed as `max_observed_event_time - max_lateness`, with per-source documented bounds (radar 100ms, optical 30s, ISL 10s for SDA). The min-of-inputs rule for propagation through fan-in operators, and the operational consequence: the slowest source dominates the downstream watermark. Watermarks interleaved with data items on the same channel via `enum SourceItem { Observation(_), Watermark(_) }` to preserve their relationship to the data they bound.

*Key question:* Three sources at watermarks T-100ms, T-30s, T-10s. What is the downstream watermark, and what is the operational consequence of that answer?

### Lesson 4 — Late Data

The three strategies for events that arrive after their watermark: drop (cheap, lossy), accumulate-with-allowed-lateness (medium cost, eventual completeness), retract-and-correct (highest cost, strongest correctness). Two-tier window state (active and retained). Retract-then-insert ordering with sequence numbers for downstream correction. Retract-aware sinks with strict-greater UPSERT semantics that absorb duplicates and out-of-order retransmits.

*Key question:* A late observation invalidates a previously-emitted conjunction alert. The alert subscriber has already triggered an avoidance maneuver. Which late-data strategy should the correlator use, and why?

---

## Capstone Project — Conjunction Window Engine

Replace the M2 dedup operator with a windowed event-time correlator. Per-source watermarks, min-of-inputs fan-in propagation, per-key sliding windows of 30 seconds with 5 seconds of allowed lateness, retract-then-insert emission on late events, and a sequence-keyed retraction-aware SQLite sink. The replay-correctness test (byte-identical output under random arrival order) is the canary for every windowing bug. Acceptance criteria, suggested architecture, and the full project brief are in `project-conjunction-window-engine.md`.

The orchestrator from Module 2 is unchanged; only one operator's implementation changes. The patterns established here repeat in every subsequent module's project.

---

## File Index

```
module-03-event-time-and-watermarks/
├── README.md                                  ← this file
├── lesson-01-event-vs-processing-time.md      ← Event time vs processing time
├── lesson-01-quiz.toml                        ← Quiz (5 questions)
├── lesson-02-windowing.md                     ← Windowing
├── lesson-02-quiz.toml                        ← Quiz (5 questions)
├── lesson-03-watermarks.md                    ← Watermarks
├── lesson-03-quiz.toml                        ← Quiz (5 questions)
├── lesson-04-late-data.md                     ← Late data
├── lesson-04-quiz.toml                        ← Quiz (5 questions)
└── project-conjunction-window-engine.md       ← Capstone project brief
```

---

## Prerequisites

* Module 1 (Stream Processing Foundations) and Module 2 (Pipeline Orchestration Internals) completed — the `Observation` envelope, the `OperatorGraph` builder, the supervisor pattern, and the at-least-once-plus-idempotent delivery frame are all assumed
* Foundation Track completed — async Rust, channels, BTreeMap and VecDeque algorithmic intuitions
* Familiarity with `tokio::sync::mpsc`, `std::time::SystemTime`, and `serde` for the watermark envelope extension
* Comfort with the `tokio::select!` pattern from Module 2's cancel-safety lesson

## What Comes Next

Module 4 (Backpressure and Flow Control) hardens the channel boundaries against burst load. The bounded-channel-per-edge invariant from M2 plus the watermark machinery from M3 are both inputs to that work — burst load affects watermark advance, and watermark stall affects late-event handling. The flow-policy machinery developed in M4 plugs in upstream of the windowed correlator without changing its windowing logic.
