# Capstone Project — Backpressure-Aware Fusion Broker

**Module:** Data Pipelines — M04: Backpressure and Flow Control
**Estimated effort:** 1–2 weeks of focused work
**Prerequisites:** All three lessons in this module passed at ≥70%

---

## Mission Brief

> **OPS DIRECTIVE — SDA-2026-0188 / Phase 4 Implementation**
> **Classification:** BURST-LOAD HARDENING
>
> Last week's anti-satellite test added 1,800 newly tracked debris
> objects to the catalog within 90 seconds. The Phase 3 pipeline
> survived but with twelve minutes of catch-up time and four dropped
> conjunction alerts during the spike. The postmortem traced the
> dropped alerts to a single edge — the alert-emitter's incoming
> channel — where the buffer was sized for nominal traffic, the
> upstream correlator was IO-bound and could not slow further, and
> the alert-emitter had no mechanism to triage which observations to
> drop and which to preserve. The pipeline's response to the burst
> was uniform load shed without policy: the four critical alerts that
> got dropped were no more important to the system than the four
> hundred non-critical observations dropped alongside them.
>
> Phase 4 hardens the pipeline against this failure mode. Every edge
> in the operator graph gets an explicit FlowPolicy. A priority
> classifier distinguishes high-priority observations (previously-
> unseen objects, sustained-trajectory updates) from low-priority
> ones (redundant samples for objects already being tracked). Under
> shed conditions, low-priority observations are dropped before
> high-priority. The audit script becomes a CI test that fails the
> build if a new edge is added without a documented FlowPolicy. A
> burst simulator drives the pipeline at 10x normal rate for five
> minutes and asserts no high-priority alerts are dropped during
> the spike.
>
> Success criteria for Phase 4: a deliberate 10x burst test passes
> with zero high-priority alerts dropped, with the pipeline memory
> bounded throughout the spike, and with the operational dashboard
> identifying the bottleneck operator within 30 seconds of the
> spike's onset.

---

## What You're Building

Harden the Phase 3 pipeline (M3's conjunction window engine running on M2's orchestrator) against burst load. The deliverable is:

1. Every channel in the topology has an explicit `FlowPolicy` set at construction time and documented with a one-line comment
2. A priority classifier (`fn classify_priority(obs: &Observation) -> Priority`) that distinguishes High and Low based on whether the observation is a fresh sighting, a sustained-trajectory update, or a redundant sample
3. A `PriorityShedSink` that maintains separate sub-channels per priority and drops Low-priority items first when the shared channel approaches capacity
4. The audit script from L3 wired into the `cargo test` suite as a CI gate that fails on any new unbounded channel or undocumented FlowPolicy
5. The `BurstSimulator` from L3 running as an integration test that drives 10x rate for 5 minutes and asserts the high-priority retention property
6. Per-channel occupancy gauges (the `InstrumentedSender` from L3) on every edge, plus a `flow_policy_drops_total{policy, priority}` counter
7. An updated operational README documenting how to read the channel-occupancy gradient, what each FlowPolicy means, and what the burst simulator's pass/fail criteria are

The orchestrator from M2 and the windowed correlator from M3 are unchanged in structure. Only the channels' policies change, and a new operator (the priority classifier) sits between the normalize and the correlator.

---

## Suggested Architecture

```
   ┌───────┐   FP=Backpressure                                FP=Backpressure
   │ Radar │═════════>┌────────┐                            ┌──────────────┐
   │  Src  │═════════>│ Norm   │   FP=Backpressure          │   Windowed   │
   └───────┘          │ FanIn  │═════>┌─────────────┐═════>│  Correlator  │
   ┌───────┐  ═══════>│        │      │  Priority   │═════>│  (M3)        │
   │ Optical│         └────────┘      │ Classifier  │      └──────┬───────┘
   │  Src  │                          │  + Shed     │             │ FP=Timed(200ms)
   └───────┘                          └─────────────┘             ▼
   ┌───────┐                                                ┌──────────────┐
   │  ISL  │                                                │  Alert Sink  │
   │  Src  │                                                │ (priority    │
   └───────┘                                                │  preserving) │
                                                            └──────────────┘
                                                                  │
                                                                  ▼ shed_drop
                                                            ┌──────────────┐
                                                            │     DLQ      │
                                                            └──────────────┘

   Plus a sidecar:
   ┌─────────────────────────────────────┐
   │  /metrics endpoint exporting:       │
   │   - channel_occupancy{edge}         │
   │   - flow_policy_drops_total{policy} │
   │   - per-priority counters           │
   └─────────────────────────────────────┘
```

The priority classifier operator sits between the fan-in normalize and the correlator. Its outgoing edge is a `PriorityShedSink` — a sink that holds two sub-channels (one per priority) and feeds the correlator from a `select!` that prefers High when both have items. Under shed conditions (the underlying correlator's incoming channel is filling), Low items are dropped first while High items still flow.

---

## Acceptance Criteria

### Functional Requirements

- [x] Every channel in the operator graph has a `FlowPolicy` set and documented with a comment naming the choice and the reasoning
- [x] No `unbounded_channel` anywhere in the data path; the audit script fails CI if one is introduced
- [x] No `tokio::spawn` inside any operator's per-event hot path; the audit script's heuristic check fails CI on new occurrences
- [x] The priority classifier returns `Priority::{High, Low}` based on the documented rules; the rules are encoded in code with a `// Reason: ...` comment per branch
- [x] The `PriorityShedSink` maintains separate sub-channels and drops Low first when the shared underlying channel is over a configured high-water mark (e.g., 80% capacity)
- [x] The alert-emitter uses `FlowPolicy::Timed(200ms)` and routes timed-out alerts to a DLQ rather than blocking past the SLO

### Quality Requirements

- [x] **Audit script test**: a unit test runs the audit on a synthetic graph that contains an unbounded channel, asserts the audit fails with a recognizable error message
- [x] **Burst simulator test**: an integration test drives the pipeline at 10x normal rate for 5 minutes; asserts (a) zero High-priority alerts dropped, (b) memory plateau reached within 30 seconds (no unbounded growth), (c) `flow_policy_drops_total{priority="low"}` increments while `flow_policy_drops_total{priority="high"}` stays at zero
- [x] **Bottleneck identification test**: a deterministic test injects a synthetic slow operator, runs the simulator, asserts the gradient-reading helper identifies the right operator as the bottleneck within 30 seconds
- [x] All channel buffer sizes have a documented `BurstProfile` in the topology declaration; the orchestrator's startup log emits the (edge, buffer_size, profile) triple

### Operational Requirements

- [x] `/metrics` endpoint extends Phase 3's with: `channel_occupancy{edge}` (gauge per edge), `flow_policy_drops_total{policy, priority}` (counter), `priority_classifier_decisions_total{priority}` (counter)
- [x] Operational runbook section "Diagnosing a Backpressure Incident" documenting the gradient-reading discipline, with a worked example using the burst simulator's output as the canonical case
- [x] The audit script runs as part of `cargo test --release`; failing it fails the CI build

### Self-Assessed Stretch Goals

- [ ] *(self-assessed)* The 10x burst test holds P99 high-priority alert latency under 5 seconds throughout the spike (the 30-second SLA is comfortably met). Provide a histogram from a real 5-minute run.
- [ ] *(self-assessed)* A circuit breaker (Module 2 L4) wraps the optical-archive call: if the archive's failure rate exceeds 50% over 30 seconds, the breaker opens for 30 seconds; the dashboard shows breaker state. Demonstrated with a `wiremock` simulating intermittent 503 responses.
- [ ] *(self-assessed)* The pipeline's checkpoint operator (foreshadowed for M5) uses the Lesson 2 `CreditChannel` to pause ingestion during the flush; the burst simulator runs through a checkpoint and continues without observable High-priority alert loss.

---

## Hints

<details>

<summary>How should I encode the priority classification rules?</summary>

A small enum and a function with explicit branches. Each branch carries a comment naming the operational rationale.

```rust,ignore
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority { High, Low }

pub fn classify_priority(obs: &Observation, recent_objects: &RecentSet) -> Priority {
    // Reason: previously-unseen objects are time-critical; their first
    // observation defines the orbital track and a missed alert can mean
    // a missed conjunction.
    if !recent_objects.contains(&obs.target_object_id) {
        return Priority::High;
    }

    // Reason: sustained-trajectory updates from the high-cadence radar
    // refine existing tracks but redundant samples (>4 within 30s) add
    // little to track quality and are sheddable.
    if obs.source_kind == SourceKind::Radar
       && recent_objects.sample_count(&obs.target_object_id, Duration::from_secs(30)) > 4
    {
        return Priority::Low;
    }

    Priority::High
}
```

The `RecentSet` is a small auxiliary structure that the classifier owns; it tracks the last N seconds of (object_id, sensor_timestamp) pairs and supports `contains` and `sample_count` lookups. Bound it by both time and count (the L2 sliding-window pattern from M3 applies here).

</details>

<details>

<summary>How should the PriorityShedSink select between sub-channels?</summary>

A `tokio::select!` over two recv futures, with a bias toward High when both are ready. The biased mode of select! gives deterministic precedence — without it, the macro picks randomly among ready arms.

```rust,ignore
pub async fn run_priority_sink(
    mut high_rx: mpsc::Receiver<Observation>,
    mut low_rx: mpsc::Receiver<Observation>,
    output: mpsc::Sender<Observation>,
) -> Result<()> {
    loop {
        tokio::select! {
            biased;
            recv = high_rx.recv() => match recv {
                Some(obs) => output.send(obs).await
                    .map_err(|_| anyhow!("downstream dropped"))?,
                None => break,
            },
            recv = low_rx.recv() => match recv {
                Some(obs) => output.send(obs).await
                    .map_err(|_| anyhow!("downstream dropped"))?,
                None => continue,
            },
        }
    }
    Ok(())
}
```

The `biased;` directive is what gives High priority deterministic preference; without it, ties (both channels ready) resolve randomly, which means the High channel only gets ~50% of the throughput. Documented in the operator's comment.

</details>

<details>

<summary>How do I make the burst simulator deterministic?</summary>

Drive synthetic events with a fixed-seed RNG; sample channel occupancy at deterministic wall-clock intervals; assert against the resulting time series rather than against transient peaks. The key insight: real-world burst behavior is not deterministic, but the simulator's value is the regression-detection property — same inputs produce same outputs, so a regression is visible as a change in the time series.

Use `tokio::time::pause()` and `tokio::time::advance()` to drive the simulation in fast-forward without real wall-clock waits. The simulator runs in milliseconds rather than minutes, fits in CI, runs deterministically every time.

</details>

<details>

<summary>How do I size the High and Low sub-channels?</summary>

Total capacity should match the underlying channel's capacity — say, 4096 in total. Split: 75% High (3072), 25% Low (1024). The High side is sized to the steady-state High rate plus burst headroom; the Low side is sized small because it is the first to shed under load and additional capacity does not add value.

The split is documented in the `BurstProfile` for the edge and visible at startup in the orchestrator's structured log. Operational tuning revisits the split if dashboard data shows the High side hitting capacity under normal load (then it is undersized) or the Low side being persistently empty (then it is oversized and the capacity could be reallocated).

</details>

<details>

<summary>How do I integrate the audit script with cargo test?</summary>

A `#[test]` function that builds the production graph and runs `audit_or_fail` on it. The test fails the CI build whenever a new edge or operator violates the audit rules.

```rust,ignore
#[test]
fn pipeline_passes_backpressure_audit() {
    let graph = build_production_pipeline_graph();
    let result = audit_or_fail(&graph);
    assert!(
        result.is_ok(),
        "backpressure audit failed:\n{}",
        result.unwrap_err()
    );
}
```

The `build_production_pipeline_graph` is the same function the binary calls for its actual topology — sharing the construction code between binary and test ensures the test exercises what production runs, not a stale fixture.

</details>

---

## Getting Started

Recommended order:

1. **`FlowPolicy` enum and `ConfigurableSink`.** Lesson 1's primitive; wire it through every existing edge in the topology with `FlowPolicy::Backpressure` as the explicit choice on most edges.
2. **Audit script.** Lesson 3's primitive; encode it as a `cargo test` function. It will pass at this stage; the value is preventing future regressions.
3. **`InstrumentedSender` + per-channel occupancy gauge.** Lesson 3's primitive; wrap every `mpsc::Sender` in the production graph. Verify the dashboard shows occupancy values at startup.
4. **Priority classifier.** Encode the classification rules; unit-test them against synthetic inputs.
5. **`PriorityShedSink` with sub-channel split.** The biased-select pattern; integration-test it with a synthetic load mix.
6. **`BurstSimulator` integration test.** Drive 10x rate for 5 simulated minutes; assert High-priority retention.
7. **Operational runbook + structured log emit at startup.** The "what each policy means" reference for ops.
8. **CI integration**: audit script in `cargo test`, burst simulator in nightly CI.

Aim for the 10x burst simulator passing with the priority classifier in place by day 7. The audit script and runbook are finishing work that pays back later.

---

## What This Module Sets Up

In Module 5 you will make the windowed correlator's state crash-safe via checkpointing. The credit-based-flow primitive from this module's Lesson 2 is the mechanism that pauses the upstream during the checkpoint flush. The bounded-channel-everywhere invariant the audit enforces is what lets the checkpointed state size be bounded and predictable. The exactly-once-via-idempotency frame from M2 plus the retract-aware sink from M3 plus the priority shedding from M4 compose into a pipeline that is correct, hardenable under load, and crash-safe under restart — the full M5 deliverable.

In Module 6 you will surface the per-channel occupancy gradient and the per-priority drop counters as the operational dashboard's primary panels. The audit script becomes part of the deploy gate; the BurstSimulator becomes part of the SLO compliance test. The work in this module is the operational foundation for that observability stack.

The patterns the module installs — explicit per-edge FlowPolicy, priority-aware load shedding, audit-as-CI-test — generalize beyond SDA. They are the standard streaming-pipeline hardening techniques for any system that must survive bursts; the module's specifics are where the techniques meet a real workload.
