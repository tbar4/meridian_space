# Lesson 3 — Watermarks

**Module:** Data Pipelines — M03: Event Time and Watermarks
**Position:** Lesson 3 of 4
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 ("Knowing When You're Ready to Receive Events" — the watermark/punctuation discussion); *Streaming Data* — Andrew Psaltis, Chapter 4 (Out-of-Order Events and the Watermark Mechanism); *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 14 (Out-of-Sequence Events)

---

## Context

Lesson 2's windowed operator builds up state per active window and waits for a *close trigger* — the signal that says "this window is done; emit its result and discard its state." We deferred the close trigger to this lesson. The naive answer is wall-clock time: a 5-second window starting at T closes when wall-clock time exceeds T+5. This is wrong in event-time semantics. A late-arriving observation with `sensor_timestamp ≤ T+5` may still arrive after wall-clock-time T+5 because the optical archive's polling cadence delayed it. Closing on wall-clock loses that observation; the window's emitted result is wrong. The right close trigger has to be a per-event-time signal, not a per-wall-clock signal.

The mechanism is the **watermark**: a declaration, made by a source or computed by an operator, that "no event with `event_time` less than X will arrive at this point in the pipeline after this watermark." A watermark is a *guarantee*, not an estimate. With a watermark, the windowed operator can close any window whose end ≤ the watermark's value and be confident no more relevant events will arrive. Without a watermark, the operator either holds windows forever (correct but useless — no result is ever emitted) or closes them too early on a wall-clock bound (fast but wrong). The watermark is the necessary fourth piece of event-time semantics, alongside event-time-on-the-envelope (Lesson 1) and event-time-windowing (Lesson 2).

This lesson develops watermarks in three pieces. What a watermark is precisely (and the perfect-vs-heuristic distinction). How sources generate watermarks for their own observations. How operators propagate watermarks through the pipeline (the min-of-inputs rule that the rest of the track depends on). The forward references stay tight. Lesson 4 handles late events that arrive *after* a watermark has already declared their window closed. The capstone wires watermarks from sources through normalize through the windowed correlator. Module 6 surfaces watermark progress as the master observability metric — "the pipeline is currently complete through event-time T."

---

## Core Concepts

### A Watermark, Defined Precisely

A watermark is a value of the form `Watermark(t)` whose meaning is: *no event with `event_time < t` will arrive after this point*. The watermark is a *guarantee*, not a hope or an estimate. When an operator receives a watermark, it can act on that guarantee — close windows, emit results, evict state — confident that the guarantee will hold.

A watermark's value monotonically advances: each new watermark from a given source has a value greater than or equal to the previous one. (Equality is permitted but uninteresting; in practice watermarks strictly advance.) A source that emits a non-monotonic watermark has violated the protocol; the operator may treat this as a bug and either ignore the regressed value or fail loudly. The orchestrator's structured-logging discipline applies — log a structured event for every regressed watermark, alert on persistent regression.

The watermark is a *separate item* from observations on the same channel. The convention this track uses is to interleave two kinds of items on the source-to-operator channels: data items (`Observation`) and control items (`Watermark(SystemTime)`). Operators consume both, processing data items as they arrive and updating their watermark state on watermark items. The alternative — a separate side channel for watermarks — exists in some streaming frameworks but introduces its own coordination problems (a fast data channel out-pacing a slow watermark channel produces apparent regressions). In-band interleaving keeps the ordering coherent.

### Perfect vs Heuristic Watermarks

A **perfect watermark** is one where the source can prove the guarantee — there is some monotonic property of the source that lets it declare with certainty when no earlier event will arrive. The clearest example is a single-stream source whose events arrive in event-time order: every event is a watermark, because the source can emit "watermark = this event's event_time" and be sure no earlier events will follow.

Most production sources cannot offer perfect watermarks. They offer **heuristic watermarks**: an estimate of the maximum lateness an event can have, used to compute a bound. The source picks a *max-lateness estimate* (call it M) and emits, periodically, a watermark of value `current_time - M`. The estimate is documented per-source based on the source's known properties. If M is too small, late events arrive *past* the watermark — the watermark's guarantee was wrong, and the late event must be dropped or held in allowed-lateness state (Lesson 4). If M is too large, the watermark advances too slowly and downstream windows close later than necessary, increasing pipeline latency.

For SDA, the per-source max-lateness values are:

| Source | Max lateness | Reasoning |
|---|---|---|
| Radar (UDP) | 100 ms | GPS-locked clocks; fiber path round-trip; no buffering |
| Optical (HTTP poll) | 30 s | Polling cadence is 30 s; an event recorded just after a poll waits one full cycle |
| ISL beacon (TCP) | 10 s | Onboard buffering before downlink; downlink-to-ground propagation |

The estimates are conservative: real lateness is typically much less, but the bound covers the tail of the lateness distribution. Lesson 4 covers what happens when the estimate is wrong.

### Generation at Sources

A source emits watermarks alongside its observations. The pattern is the same for each source kind, parameterized by the source's max-lateness estimate.

```rust,ignore
pub enum SourceItem {
    Observation(Observation),
    Watermark(SystemTime),
}
```

Each source's emit loop interleaves `Observation` items with periodic `Watermark` items. The frequency of watermark emission is operationally important: too rarely, downstream windows are held longer than necessary because the operator does not know the watermark has advanced; too frequently, the watermark items add bandwidth overhead. A watermark every 1 second of wall-clock time is a reasonable starting point for SDA — well below the optical source's 30-second polling cadence, well above the per-event rate.

The source-side watermark value is `max(observed event_times) - max_lateness`. A source that has just produced an observation with `sensor_timestamp = T` emits the watermark `T - M` (where M is its max-lateness estimate). The watermark trails the source's most recent observed event-time by exactly M, which is the guarantee shape the watermark protocol expects.

### Propagation Through Operators

When an operator has multiple inputs (a fan-in normalize, a join, a correlation), it must compute its *output watermark* from its *input watermarks*. The rule is the **minimum**: the output watermark is the minimum of the most recent watermark from each input. The reason for *min*, not *max*: we can only guarantee what the *worst* upstream guarantees. If the radar input has watermark T and the optical input has watermark T-30, we cannot guarantee that no event with event_time < T will arrive — because the optical input might still produce one. The strongest claim we can make is "no event with event_time < T-30 will arrive," so that is the output watermark.

The min-rule has a counterintuitive consequence: the slowest source dominates the downstream watermark. A pipeline with three sources at watermarks T, T+10, and T+20 has a downstream watermark of T — the slow source's value. Improving any of the faster sources does nothing for the downstream watermark; only improving the slowest source does. This is the operational property that makes per-source max-lateness estimates so important: tightening any one estimate lowers that source's watermark trail-time, which lowers the downstream watermark trail-time only if that source was the dominant one.

Implementation in code: the operator tracks the most recent watermark per input channel, recomputes the minimum on every watermark item, and emits a new output watermark when the minimum advances.

### The Aggressive-vs-Conservative Tradeoff

The watermark designer's main lever is the per-source max-lateness M. Aggressive M (small) → fast watermark → fast window close → low pipeline latency, but late events arriving past the watermark are dropped or pushed into allowed-lateness state. Conservative M (large) → slow watermark → slow window close → higher pipeline latency, but few late events are missed.

The right setting is operational, not theoretical. For the SDA pipeline's 30-second conjunction-detection SLA, the source-side max-lateness values above produce a downstream watermark that trails real time by ~30 seconds (dominated by the optical source). That gives windowed correlators 30 seconds to close before the SLA is at risk. The aggressive-vs-conservative tradeoff is per-pipeline; we set defaults that match SDA and document them in the per-source code.

---

## Code Examples

### A Source That Emits Both Observations and Watermarks

The radar UDP source from M1 emits only `Observation`s. We extend it to interleave watermark items on a wall-clock cadence. The pattern is the same for every source; the per-source max-lateness M is the only parameter that changes.

```rust,ignore
use std::time::{Duration, Instant, SystemTime};
use anyhow::Result;
use tokio::sync::mpsc;
use tokio::time;

pub enum SourceItem {
    Observation(Observation),
    Watermark(SystemTime),
}

/// Wraps an existing source and interleaves periodic watermarks based
/// on observed event-time and the source's documented max-lateness.
pub async fn run_source_with_watermarks<S>(
    mut source: S,
    output: mpsc::Sender<SourceItem>,
    max_lateness: Duration,
    watermark_interval: Duration,
) -> Result<()>
where
    S: ObservationSource,
{
    let mut last_watermark_emit = Instant::now();
    let mut max_observed_event_time = SystemTime::UNIX_EPOCH;

    loop {
        match source.next().await? {
            Some(obs) => {
                if obs.sensor_timestamp > max_observed_event_time {
                    max_observed_event_time = obs.sensor_timestamp;
                }
                output.send(SourceItem::Observation(obs)).await
                    .map_err(|_| anyhow::anyhow!("downstream dropped"))?;

                // Emit a watermark on cadence, regardless of event rate.
                if last_watermark_emit.elapsed() >= watermark_interval {
                    let wm = max_observed_event_time
                        .checked_sub(max_lateness)
                        .unwrap_or(SystemTime::UNIX_EPOCH);
                    output.send(SourceItem::Watermark(wm)).await
                        .map_err(|_| anyhow::anyhow!("downstream dropped"))?;
                    last_watermark_emit = Instant::now();
                }
            }
            None => return Ok(()),
        }
    }
}
```

The watermark value is `max_observed_event_time - max_lateness` — the most recent event-time the source has seen, minus the source's documented worst-case lateness. The watermark monotonically advances because `max_observed_event_time` does and `max_lateness` is constant. The cadence (`watermark_interval`) is wall-clock-driven so the watermark advances even if the source has been silent for a stretch — important so a downstream operator's windows do not sit idle waiting for events that are not coming. A real production source also emits watermarks during idle gaps via a `tokio::time::interval`; we elide that for clarity but the capstone implementation includes it.

### A Fan-In Operator That Computes Min-of-Inputs

The normalize operator from M1 fanned three sources into one channel. With watermarks, the fan-in must compute its output watermark as the minimum of the most recent watermarks from each input. The implementation tracks per-input watermarks in a `Vec<Option<SystemTime>>` and recomputes the min on each watermark item.

```rust,ignore
use std::time::SystemTime;
use anyhow::Result;
use tokio::sync::mpsc;

/// Fan-in normalize operator that consumes from N upstream channels
/// (each carrying SourceItem) and emits a single SourceItem stream
/// downstream with a properly-propagated min-of-inputs watermark.
pub async fn normalize_fan_in(
    mut inputs: Vec<mpsc::Receiver<SourceItem>>,
    output: mpsc::Sender<SourceItem>,
) -> Result<()> {
    use tokio::select;

    let n = inputs.len();
    let mut input_watermarks: Vec<Option<SystemTime>> = vec![None; n];
    let mut last_emitted_watermark: Option<SystemTime> = None;

    // Simplified select: real implementation uses select_all from
    // futures::future for arbitrary N. Here we sketch the per-input
    // handling for clarity.
    for input_idx in 0..n {
        // ... in a real implementation, all inputs are polled
        // concurrently via select_all; this loop is illustrative.
        while let Some(item) = inputs[input_idx].recv().await {
            match item {
                SourceItem::Observation(obs) => {
                    let normalized = normalize(obs);
                    output.send(SourceItem::Observation(normalized)).await
                        .map_err(|_| anyhow::anyhow!("downstream dropped"))?;
                }
                SourceItem::Watermark(wm) => {
                    input_watermarks[input_idx] = Some(wm);
                    // Compute min only when every input has reported at
                    // least one watermark. Until then, the operator's
                    // output watermark is undefined.
                    if input_watermarks.iter().all(|w| w.is_some()) {
                        let new_wm = input_watermarks
                            .iter()
                            .map(|w| w.unwrap())
                            .min()
                            .unwrap();
                        if Some(new_wm) > last_emitted_watermark {
                            output.send(SourceItem::Watermark(new_wm)).await
                                .map_err(|_| anyhow::anyhow!("downstream dropped"))?;
                            last_emitted_watermark = Some(new_wm);
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
```

Three subtle points. The output watermark is undefined until every input has reported at least one watermark. A fan-in with three sources where one source has not yet sent a watermark cannot propagate a min — there is no upper bound on what that source's watermark might be once it arrives, so any min computed without it is unsafe. The fix is structural: hold downstream emission until every input is heard from. Second, the operator emits a new output watermark only when the min strictly advances. Re-emitting the same watermark would be correct but wasteful; the strict-advance check throttles the per-event watermark traffic to what is operationally meaningful. Third, the per-input bookkeeping is intentionally simple — a `Vec<Option<SystemTime>>` indexed by input position, no fancier structure needed. Production code that joins many inputs uses the same pattern with `Vec` lengths in the dozens; the constant-factor cost of the `.min()` recomputation on each watermark item is negligible at any realistic input count.

### Wiring Watermarks Into the Tumbling Window Operator

Lesson 2's `TumblingWindow::close_up_to(watermark)` becomes the consumer of watermark items. The operator no longer has its own close logic; it reacts to the watermark stream the upstream produced.

```rust,ignore
use std::time::SystemTime;
use anyhow::Result;
use tokio::sync::mpsc;

/// Drive a TumblingWindow operator from a single SourceItem stream
/// that interleaves Observations with Watermarks. Observations are
/// ingested into the window state; watermarks trigger close-up-to.
pub async fn run_tumbling_with_watermarks(
    mut window_op: TumblingWindow,
    mut input: mpsc::Receiver<SourceItem>,
) -> Result<()> {
    while let Some(item) = input.recv().await {
        match item {
            SourceItem::Observation(obs) => {
                window_op.ingest(obs);
            }
            SourceItem::Watermark(wm) => {
                window_op.close_up_to(wm).await?;
            }
        }
    }
    Ok(())
}
```

The pattern is the structural property the lesson promised. The operator does not decide its own close; it consumes a watermark stream that supplies the close trigger. The same shape applies to sliding-window operators (which evict per-key state on watermark advance), session-window operators (which close sessions whose `session_end + gap` is past the watermark), and any other event-time-windowed operator. The watermark is the universal close trigger.

---

## Key Takeaways

* A **watermark** is a per-event-time guarantee: `Watermark(t)` means "no event with event_time < t will arrive after this point." It is the only correct close trigger for event-time windows; wall-clock-based close drops late events whose event_time precedes the wall-clock cutoff.
* **Heuristic watermarks** (the production default) are computed as `max_observed_event_time - max_lateness`, where `max_lateness` is a per-source documented bound. Tighter `max_lateness` → faster watermark → lower pipeline latency, at the cost of more events arriving past the watermark.
* The **min-of-inputs rule** propagates watermarks through fan-in operators: the output watermark is the minimum of the most recent watermarks from each input. We can only guarantee what the *slowest* upstream guarantees. The slowest source dominates the downstream watermark.
* **Watermarks are interleaved with data items** on the same channel (`enum SourceItem { Observation(_), Watermark(_) }`). In-band ordering keeps the watermark's relationship to the data items it bounds coherent; a separate side-channel can produce apparent regressions.
* The windowed operator does not decide its own close. It **consumes a watermark stream** and closes any window whose end ≤ the watermark. This decoupling is what makes windowed operators composable across the pipeline and re-usable across window shapes.

---

{{#quiz lesson-03-quiz.toml}}
