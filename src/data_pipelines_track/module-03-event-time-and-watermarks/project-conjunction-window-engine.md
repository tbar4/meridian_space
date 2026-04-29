# Capstone Project — Conjunction Window Engine

**Module:** Data Pipelines — M03: Event Time and Watermarks
**Estimated effort:** 1–2 weeks of focused work
**Prerequisites:** All four lessons in this module passed at ≥70%

---

## Mission Brief

> **OPS DIRECTIVE — SDA-2026-0142 / Phase 3 Implementation**
> **Classification:** CORRELATION TIER UPGRADE
>
> The Phase 2 orchestrator from Module 2 is in production and stable.
> The dedup operator at the top of the correlation tier is processing-
> time-based, which is correct for ingestion deduplication but wrong
> for cross-sensor correlation. Internal review of conjunction alerts
> from the past quarter found a 2.3% rate of cross-sensor mismatches
> traceable to optical-vs-radar arrival skew straddling the 5-second
> dedup window. Two of the missed correlations were later determined
> to be real conjunctions, surfaced only by post-hoc batch reprocessing.
> The fix is structural: replace processing-time dedup with event-time
> windowed correlation that respects sensor_timestamp regardless of
> arrival skew.
>
> Success criteria for Phase 3: cross-sensor correlations are computed
> by event time using the watermark protocol; the per-source max-lateness
> values from the M3 lesson (radar 100ms, optical 30s, ISL 10s) are
> respected; allowed-lateness of 5s captures the long tail of optical
> archive delays; conjunction alerts emit a retraction when a late
> observation invalidates a previously-emitted alert.

---

## What You're Building

Replace the dedup operator from M2's pipeline with a windowed event-time
correlator that emits `ConjunctionRisk` envelopes when two orbital
objects' observations within the same event-time window indicate a
close approach.

The deliverable is:

1. A `WatermarkSource` trait extending the M1 `ObservationSource` to interleave watermarks with observations on the channel
2. Wrapped versions of M1's three sources (radar, optical, ISL) that emit per-source watermarks per the lesson's max-lateness table
3. A min-of-inputs watermark-propagating fan-in normalize operator
4. A `WindowedCorrelator` operator that holds per-key sliding windows of observations, emits `ConjunctionRisk` envelopes when pairs cross a configured proximity threshold, and supports allowed-lateness retention
5. A retraction-aware alert sink that emits `WindowEmit::{Insert, Retract}` records via a sequence-number-keyed downstream
6. A test harness that drives the pipeline with synthetic out-of-order events and verifies correctness across replay

The orchestrator from M2 is unchanged; only one operator's implementation changes (dedup → correlator). The `OperatorGraph` declaration is updated to reflect the new operator. Refresh the operational README to document the new metrics (watermark trail per source, allowed-lateness eviction count, retractions emitted).

---

## Suggested Architecture

```
                                                          ┌─────────────────┐
   ┌───────────────┐  SourceItem                          │  Alert Sink     │
   │ Radar Source  │═══════════════╗                      │  (retract-aware)│
   │  + watermarks │               ║                      │                 │
   └───────────────┘               ║                      └────────▲────────┘
                                   ║                               │
   ┌───────────────┐  SourceItem   ▼                               │
   │ Optical Src   │═══════>┌────────────┐    SourceItem    ┌──────────────┐
   │  + watermarks │═══════>│  Normalize │═════════════════>│   Windowed   │
   └───────────────┘        │  Fan-In    │                  │  Correlator  │
                            │  min-WM    │                  │  (sliding,   │
   ┌───────────────┐  ═════>└────────────┘                  │   allowed-   │
   │  ISL Source   │                                        │   lateness)  │
   │  + watermarks │                                        └──────────────┘
   └───────────────┘
```

Each source runs its own task (orchestrated by M2's `OperatorGraph`). Each source emits `enum SourceItem { Observation(_), Watermark(_) }` on its outgoing channel. The fan-in normalize operator consumes from all three and emits a single SourceItem stream with min-of-inputs watermark propagation. The windowed correlator consumes that stream, holds per-orbital-object sliding windows, computes pairwise close-approach proximity within each window, and emits to the alert sink. The orchestrator's restart, retry, and circuit-breaker machinery from M2 wraps all of this without modification.

---

## Acceptance Criteria

### Functional Requirements

- [x] `WatermarkSource` trait with method `next() -> Result<Option<SourceItem>>`; the existing M1 `ObservationSource` is wrapped via the lesson's `run_source_with_watermarks` helper
- [x] Each source's `max_lateness` matches the lesson's table: radar 100ms, optical 30s, ISL 10s
- [x] Watermarks emitted at a wall-clock cadence (default 1 second) AND on-demand whenever the observed event_time advances; idle sources still advance their watermark
- [x] Fan-in normalize operator computes min-of-inputs watermark; output watermark held until every input has reported at least once
- [x] Windowed correlator uses sliding windows keyed on `(object_a_id, object_b_id)`; window size 30s, allowed lateness 5s
- [x] `ConjunctionRisk` emit triggered when two observations of distinct objects within the same window indicate proximity below threshold
- [x] Late events into already-emitted windows trigger a retraction-then-insert pair on the alert channel; sequence numbers on every emit
- [x] Alert sink uses an embedded SQLite (or equivalent) keyed on (window_id, sequence) with strict-greater UPSERT semantics

### Quality Requirements

- [x] **Replay test for correctness**: deterministic test injecting a fixed event sequence in event-time order, then re-running with the same events injected in random arrival order. The final state of the alert sink must be byte-identical between the two runs.
- [x] **Watermark progress test**: a unit test feeds watermarks into the fan-in operator and asserts the output watermark advances per the min-of-inputs rule and not before all inputs have reported
- [x] **Allowed-lateness test**: a unit test injects a late event into a retained window and asserts the retraction-then-insert pair is emitted; injects a late event after the lateness budget has expired and asserts it is dropped silently with the corresponding metric incremented
- [x] **Memory bound test**: under sustained load, the correlator's per-key window state stays bounded by `(window_size + allowed_lateness) × per_key_event_rate`; an integration test asserts steady-state memory for a synthetic workload

### Operational Requirements

- [x] `/metrics` extends M2's: `source_watermark_seconds{source}` (gauge), `pipeline_watermark_seconds` (gauge — the min-of-inputs at fan-in), `pending_windows{tier}` (gauge — split between active and retained), `late_events_dropped_total` (counter), `retractions_emitted_total` (counter)
- [x] Lag dashboard split into source lag and pipeline lag per the M3 L1 framing; the pipeline lag panel makes "is the problem ours or theirs" answerable in seconds
- [x] Operational runbook section "Reading the Watermark Dashboard" documenting how to interpret a watermark stall (which source's max-lateness is dominant; what the per-source values are; what tightens what)

### Self-Assessed Stretch Goals

- [ ] *(self-assessed)* Sustain 50K observations/sec input with a 30-second window, P99 emit latency < 1 second after watermark advance. Provide a `criterion` benchmark and a flame graph showing where the per-event cost lives.
- [ ] *(self-assessed)* Replay-correctness test runs against a corpus of 10 randomly-shuffled arrival orders and produces byte-identical final state in every case
- [ ] *(self-assessed)* The operational dashboard exposes a "watermark stall" alert that fires when the pipeline watermark fails to advance for > 60 seconds, distinguishing source-side stalls from pipeline-side stalls in the alert text

---

## Hints

<details>

<summary>How do I generate watermarks for an event-driven UDP source like the radar where there is no natural "tick"?</summary>

Two interleaved triggers. **On observation**: each observation updates the source's `max_observed_event_time` and (every Nth observation, or whenever wall-clock has advanced past the watermark interval) emits a watermark of `max_observed_event_time - max_lateness`. **On idle**: a `tokio::time::interval` ticking at the watermark interval emits a watermark even when no observations have arrived recently — important during quiet periods so the downstream operator's windows do not stall waiting for the source to wake up.

```rust,ignore
use tokio::select;

let mut interval = tokio::time::interval(watermark_interval);
loop {
    select! {
        obs = source.next() => {
            // Emit Observation, update max_observed_event_time,
            // possibly emit Watermark.
        }
        _ = interval.tick() => {
            // Emit Watermark even if no observations arrived;
            // the source's clock is what advances the watermark
            // during idle periods.
        }
    }
}
```

The select-against-interval pattern is the same one Module 2 used for the source-side retry timer. Reusable.

</details>

<details>

<summary>How do I efficiently store retained windows alongside active windows?</summary>

Two `BTreeMap<SystemTime, WindowState>` — one for active, one for retained — and a small dispatch on ingest. The `on_watermark` step uses `BTreeMap::split_off` for both maps to do prefix range eviction in O(log N). The combined cost per watermark advance is two prefix splits + one drain over the closed-active set; for hundreds of windows, this is sub-millisecond.

A single `BTreeMap` with a per-window enum tag (Active or Retained) also works and saves one allocation; both designs are fine. The two-map design makes the operations more obvious in the code and is the one the lesson uses.

</details>

<details>

<summary>How do I deterministically test replay correctness?</summary>

Two ingredients: a fixed input set of observations with known event_times, and a way to inject them in a specific arrival order. The test runs the same input set through the pipeline twice — once in event-time order, once in a deliberately scrambled order — and asserts the final state of the alert sink is identical between the two runs.

```rust,ignore
#[tokio::test]
async fn replay_correctness_under_scrambled_arrival() {
    let observations = build_test_observations();  // 1000 observations, event-time ordered
    let scrambled = {
        let mut o = observations.clone();
        // Shuffle deterministically with a fixed seed.
        use rand::seq::SliceRandom;
        use rand::rngs::StdRng;
        use rand::SeedableRng;
        o.shuffle(&mut StdRng::seed_from_u64(42));
        o
    };
    let result_ordered = run_pipeline_to_completion(&observations).await;
    let result_scrambled = run_pipeline_to_completion(&scrambled).await;
    assert_eq!(result_ordered, result_scrambled,
               "pipeline output should be invariant to arrival order");
}
```

The `run_pipeline_to_completion` helper drains the alert sink at the end and returns the stored state (the SQLite contents serialized to a comparable form). The assertion is the correctness property the watermark protocol is supposed to give you; if it fails, the bug is in the operator's allowed-lateness logic or the retract-then-insert ordering.

</details>

<details>

<summary>How small can the safety margin be on retained-window eviction?</summary>

The eviction happens at `watermark - allowed_lateness`. With watermark = `max_observed_event_time - max_lateness`, the eviction occurs when the latest event-time has advanced past `window_end + allowed_lateness + max_lateness`. So the *real* event-time-domain retention is `allowed_lateness + max_lateness`, not just `allowed_lateness`. The pipeline lag adds a third term: `allowed_lateness + max_lateness + pipeline_lag`.

For SDA's defaults: `5 + 30 + 1 = 36` seconds of total state retention. A budget of `~40s` for the correlator's worst-case-per-window memory budget is a reasonable plan-for-the-tail value. Operations should monitor the actual eviction lag (`now - retained_window_evict_event_time`) and alert when it grows past, say, 120 seconds — that signals either pipeline lag growth or a stuck source.

</details>

<details>

<summary>How do I avoid duplicate retractions when the operator restarts?</summary>

The operator's emit history is in-memory; on restart, it has no idea what it already emitted. M2's at-least-once-plus-idempotent composition saves us here: a duplicate emit (the same window_id, sequence) is absorbed by the sink's strict-greater UPSERT comparison. The restart re-emits the same sequence numbers it emitted before; the sink stores the result the same way it was already stored; no observable change. A late event arriving for a previously-retained window after the operator restarts and that window is no longer in retained state is silently dropped — same as if the operator had not restarted but the lateness had simply expired.

For the full crash-safety story (where the restart should resume from a checkpoint of in-flight window state), see Module 5. This module's pipeline survives restarts but loses some allowed-lateness windows during the restart window — acceptable for SDA's tolerance.

</details>

---

## Getting Started

Recommended order:

1. **`SourceItem` enum and the watermark-emitting source wrapper.** Implement once; reuse across all three sources. Unit-test by feeding fixed observations and asserting the watermarks emitted match the documented formula.
2. **Min-of-inputs fan-in.** Implement against three synthetic input channels in tests; do the unit test for the "hold until all inputs report" case explicitly.
3. **Sliding-window state per key.** Reuse the L2 SlidingWindow primitive; add the per-key dispatch in the correlator. Test eviction with manual event-time scenarios.
4. **Allowed-lateness retention.** Add the retained tier; wire watermark advance to move active→retained and to evict expired retained.
5. **Conjunction-risk computation.** The actual proximity logic — given two observations of distinct objects within the same window, decide whether they form a `ConjunctionRisk`. The math is out of scope for this curriculum; a stub `compute_proximity(obs_a, obs_b) -> f64` returning a deterministic number based on inputs is sufficient for testing the pipeline structure.
6. **Retract-and-correct emit logic.** When a late event lands in a retained window, emit `Retract { sequence: N }` followed by `Insert { sequence: N+1, result: corrected }`.
7. **Retraction-aware sink.** Embedded SQLite with the UPSERT pattern from L4. Test replay-after-restart correctness by killing and restarting the pipeline mid-stream.
8. **Replay-correctness integration test.** The byte-identical-output-across-arrival-orders test from the hint.
9. **Refresh the operational README and the dashboard.**

Aim for the sliding-window correlator with min-of-inputs watermark propagation working end-to-end by day 7; allowed-lateness and retractions can land in days 8-10. The replay-correctness test is the canary that catches every windowing bug; if it fails, stop and diagnose before adding features.

---

## What This Module Sets Up

In Module 4 you will harden the channel boundaries against burst load. The bounded-channel-per-edge invariant from M2 plus the watermark machinery from M3 are both inputs to that work — burst load affects watermark advance, and watermark stall affects late-event handling. The flow-policy machinery developed in M4 plugs in upstream of the correlator without changing its windowing logic.

In Module 5 you will make the correlator's window state crash-safe. The two-tier (active/retained) state structure is exactly what the checkpoint code serializes. The sequence numbers on emits are exactly what the at-least-once-with-checkpoint recovery uses to replay safely. M3's correctness foundation is what M5's durability layer rests on.

In Module 6 you will surface the watermark progress as the master observability metric. The per-source watermark gauges, the pipeline watermark gauge, and the retained-window count are the diagnostic dashboard's first-row panels. The lag-distinct-from-watermark distinction is the framing that makes the dashboard usable.

The correlator built here is the canonical event-time-windowed operator. Every subsequent module's project either extends it directly or applies the same patterns to a different operator.
