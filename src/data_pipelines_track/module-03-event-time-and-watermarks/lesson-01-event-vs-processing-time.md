# Lesson 1 — Event Time vs Processing Time

**Module:** Data Pipelines — M03: Event Time and Watermarks
**Position:** Lesson 1 of 4
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 ("Reasoning About Time" — the three different times in stream processing); *Streaming Data* — Andrew Psaltis, Chapter 4 (Analyzing Streaming Data); *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 14 (Stream Processing Concepts: Time)

---

## Context

Module 1's `Observation` envelope carried two timestamps, and Module 1 said the distinction would matter later. This is later. The dedup operator in M1 (and the orchestrator that wraps it in M2) assigns observations to windows by *processing time* — the wall-clock instant at which the pipeline received the observation. That works for ingestion-tier deduplication but it is wrong for correlation-tier reasoning. Two radars observe the same orbital event at the same instant. Their observations leave the radars within microseconds of each other. They arrive at the pipeline 100 milliseconds apart because one radar's link to the ground network has a long-haul fiber detour. A processing-time correlator concludes "two separate events." An event-time correlator concludes "one event, two views." The conjunction risk computation depends on the latter being right.

The mental shift this lesson installs is that *every observation has multiple timestamps that are operationally non-interchangeable*. Sensor timestamp is when the event happened in the world. Ingest timestamp is when the pipeline received it. The pipeline's wall clock is when *now* is. Each of these answers a different question. Throughput metrics (events per second across the pipeline) want processing time. SLO compliance (P99 ingest-to-emit latency) wants ingest time as the start. Conjunction-window assignment (which observations should be considered together for risk computation) wants sensor timestamp. Confusing them produces incorrect aggregates that are individually plausible but collectively inconsistent.

This module's job is to take the orchestrator from Module 2 and replace its processing-time dedup with an event-time windowed correlator. The lessons proceed in dependency order. This lesson establishes the time vocabulary. Lesson 2 builds windows on top of it. Lesson 3 introduces watermarks — the mechanism by which the pipeline decides "I have seen all events for window W with sufficient confidence to emit the window's result." Lesson 4 handles the late events that arrive after a watermark has already declared the window closed. The capstone project replaces M2's dedup operator with the result.

---

## Core Concepts

### The Three Times

DDIA Chapter 11 makes the case precisely: an event in a streaming system can carry up to three distinct timestamps, and a production pipeline that does not distinguish them will have correctness bugs that look like flakiness.

* **Event time** — when the event actually happened in the source system. For an SDA observation, this is the instant the sensor's hardware recorded the detection. The radar's GPS-disciplined clock captures this to nanosecond precision. The optical telescope's NTP-disciplined clock captures it to about ten milliseconds. The ISL beacon's onboard satellite clock captures it with drift up to seconds between syncs.
* **Ingest time** (sometimes called *server time*) — when the pipeline received the event. This is what `SystemTime::now()` returns in the source operator's recv loop. It is monotonic-ish across observations from the same sensor but not across sensors.
* **Processing time** — the wall-clock instant at which a given operator processes the event. Different operators process the same event at different processing times because the event flows through them sequentially. For most aggregation purposes this and ingest time are interchangeable; the distinction matters when you are reasoning about a single operator's local clock.

For SDA's purposes, ingest time and processing time can be collapsed: every observation's `ingest_timestamp` is captured at the ingestion-tier source operator (Module 1), and downstream operators inherit that timestamp without modification. The two distinct times the pipeline carries forward are *event time* (`sensor_timestamp`) and *ingest time* (`ingest_timestamp`). The lessons that follow refer to these by their envelope field names.

### Where Each Time Belongs

The decision is per-question, not per-pipeline. A useful rule of thumb is "what is the question being asked, and what time would make the answer right?"

| Question | The right time |
|---|---|
| How many events did we see this minute? | Processing/ingest time (operator's local view) |
| What is the P99 latency from sensor to alert? | Both — `(emit_time - sensor_timestamp)` for end-to-end, `(emit_time - ingest_timestamp)` for pipeline-only |
| Which observations are part of this orbital event? | Event time (`sensor_timestamp`) — we want all observations that physically co-occurred |
| Is the pipeline keeping up with real time? | The lag — `(now - sensor_timestamp)` summarized over the recent window |
| Did we receive any observation in the last second? | Ingest time |
| For a 5-second event-time window starting at T, when can we close it? | This is the watermark question — Lesson 3 |

The trap is using the wrong time for the question and getting an answer that looks plausible. A conjunction correlator that buckets by ingest time produces correct-looking output most of the time — the typical optical-vs-radar arrival skew is small enough that most observations of the same event do land in the same processing-time bucket. The bug shows up only when a sensor source has a delay that pushes an observation across a bucket boundary. That is when correlations are missed. The bug is rare and silent and load-dependent and exactly the kind of thing that gets diagnosed only after a high-profile incident.

### Clock Skew and Source Quality

Every sensor's clock has its own accuracy story, and the pipeline must accept the heterogeneity rather than pretend it away.

* **GPS-disciplined clocks** — radar arrays. Accurate to about 100 nanoseconds against UTC. Locked to a constellation that disciplines drift continuously. The radar's `sensor_timestamp` is the most trustworthy event time in the system.
* **NTP-disciplined clocks** — ground-based optical telescopes. Accurate to about 10 milliseconds in steady state, occasionally worse when the NTP server is degraded. NTP self-reports its sync state, which is data the source operator can include in the envelope as a *quality flag*.
* **Onboard satellite clocks for ISL beacons** — disciplined by the satellite's own GPS or by occasional ground-loop syncs. Drift between syncs can grow to seconds. The beacon's envelope reports `time_since_last_sync_s` so the pipeline can estimate the drift and treat the timestamp accordingly.

The pipeline cannot fix bad clocks at the source, but it can carry the per-source quality forward so downstream operators can reason about it. A correlator that knows an ISL beacon's timestamp is uncertain to within ±5 seconds can either widen the matching window for that source or downweight its contribution; a correlator that treats every timestamp as ground truth produces incorrect correlations during sync drift.

### Out-of-Order Arrival

Even with perfectly synchronized clocks, observations arrive at the pipeline out of event-time order. The optical archive's 30-second polling interval means optical observations can lag radar observations of the same event by up to 30 seconds of event time. The ISL beacon's 10-second buffering before downlink means beacon observations can lag both. A correlator that assumes events arrive in event-time order produces correctness bugs the first time a slow source's observation arrives after a faster source's observation that was *generated later*.

Out-of-order arrival is the rule, not the exception. The system must accept it. The mechanism is the watermark protocol covered in Lesson 3, which generalizes "wait for late events" into a tractable operator-level abstraction. The lesson here is conceptual: design every event-time operator under the assumption that observations arrive in arbitrary order with respect to event time, and verify the assumption with a replay test that injects deliberate out-of-order traffic.

### Lag as the Master Diagnostic

The diagnostic metric that operations depends on most is *lag*: `lag = ingest_timestamp - sensor_timestamp` (or `now - sensor_timestamp` for currently-streaming events). Lag answers "how far behind real time is the pipeline?" — the single most operationally important question for an event-driven system.

Lag's two components separate cleanly. *Source lag* (`ingest_timestamp - sensor_timestamp`) is how long the pipeline took to receive the event after the sensor recorded it. Source lag changes when sensors get slower, when network paths degrade, when partner APIs back up. *Pipeline lag* (`now - ingest_timestamp` for a still-flowing event, or for an emitted event the difference between emit-time and ingest-time) is how long the pipeline itself takes once it has the event. Pipeline lag changes when operators slow down, when channels back up, when the pipeline is overloaded. Distinguishing the two is what lets ops answer "is the problem ours or theirs?" without playing detective.

A naive lag metric (just `now - sensor_timestamp` summarized as a histogram) is a useful single-number diagnostic and the right thing to put on the dashboard. A diagnostic-grade metric (split by source for source lag, by stage for pipeline lag) is the right thing to have available when you need it. Module 6 builds these out fully; for this module we instrument lag at one point — the sink — and use it as the SLO indicator.

---

## Code Examples

### Source-Side Event-Time Capture with Quality Flags

The radar source already populates `sensor_timestamp` from its frame data. We extend each source's emission with a quality flag describing how trustworthy the timestamp is. This is small, additive, and the foundation of every event-time decision downstream.

```rust,ignore
use std::time::{Duration, SystemTime};

/// How trustworthy is this observation's sensor_timestamp?
/// Carried alongside the timestamp so downstream operators can
/// widen matching windows or downweight observations from
/// sources with degraded clocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClockQuality {
    /// GPS-disciplined; accurate to ~100ns against UTC.
    GpsLocked,
    /// NTP-disciplined; accurate to ~10ms in steady state.
    NtpSynced { last_sync_age_s: u32 },
    /// Onboard clock with measurable drift since last discipline.
    OnboardDrift { time_since_sync_s: u32 },
    /// Source could not provide quality; treat conservatively.
    Unknown,
}

impl ClockQuality {
    /// Worst-case event-time uncertainty for this source. Used by
    /// the correlator to widen its matching window.
    pub fn max_skew(&self) -> Duration {
        match *self {
            ClockQuality::GpsLocked => Duration::from_micros(1),
            ClockQuality::NtpSynced { last_sync_age_s } => {
                // NTP accuracy degrades roughly linearly with sync age,
                // capped at the original 10ms accuracy + 1ms per minute.
                let extra_ms = (last_sync_age_s / 60) as u64;
                Duration::from_millis(10 + extra_ms)
            }
            ClockQuality::OnboardDrift { time_since_sync_s } => {
                // Conservative: 100us drift per second since last sync.
                Duration::from_micros((time_since_sync_s as u64) * 100)
            }
            ClockQuality::Unknown => Duration::from_secs(5),
        }
    }
}
```

The `max_skew` method gives the correlator a single number to expand its event-time matching window for observations from this source. A radar observation gets a 1-microsecond skew (effectively zero); an ISL beacon two minutes past its last sync gets 12 milliseconds; a beacon at the bound of its sync interval (an hour without sync, hypothetically) gets 360 milliseconds. The correlator widens its window by the max-skew of any participating observation, ensuring legitimate correlations are not missed because of clock differences. The pattern is the same one Kafka Streams calls *grace period for out-of-order events* and Flink calls *allowed lateness in source time domain*.

### Computing and Emitting Lag

The lag operator sits at the sink end of the pipeline (or near it). It computes the lag for every event flowing through and exports it as a histogram. The operator is stateless and zero-overhead in the hot path.

```rust,ignore
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Compute end-to-end lag for an observation emitted at the sink.
/// Returns (source_lag, pipeline_lag).
/// source_lag = ingest_timestamp - sensor_timestamp (how late the sensor's
///              event was when the pipeline first saw it)
/// pipeline_lag = now - ingest_timestamp (how long the pipeline itself
///                took to process the event)
pub fn compute_lag(obs: &Observation) -> (Duration, Duration) {
    let now = SystemTime::now();
    let source_lag = obs
        .ingest_timestamp
        .duration_since(obs.sensor_timestamp)
        .unwrap_or_else(|_| {
            // Negative source lag means the source's clock is ahead of
            // ours — possible during deploy if the source is GPS-locked
            // and our host's NTP is degraded. Surface as zero rather
            // than silently subtracting.
            Duration::ZERO
        });
    let pipeline_lag = now
        .duration_since(obs.ingest_timestamp)
        .unwrap_or(Duration::ZERO);
    (source_lag, pipeline_lag)
}

/// Operator that observes lag at the sink and exports it.
/// The Prometheus histogram split by source kind makes it actionable:
/// a source-specific lag spike points at the source; a uniform spike
/// points at the pipeline.
pub fn observe_lag(obs: &Observation) {
    let (source_lag, pipeline_lag) = compute_lag(obs);
    let kind = format!("{:?}", obs.source_kind);
    metrics::histogram!("source_lag_seconds", "source" => kind.clone())
        .record(source_lag.as_secs_f64());
    metrics::histogram!("pipeline_lag_seconds", "source" => kind)
        .record(pipeline_lag.as_secs_f64());
}
```

The negative-lag handling is a real concern — the pipeline's host clock and a source's clock can disagree, and the difference can be in either direction. The right behavior is to surface the disagreement as a separate signal (a `clock_skew_observed_total` counter) rather than producing absurd lag values. The implementation collapses to zero for simplicity here; production code should split this out and alert on persistent negative lag. The Prometheus split by source is the operationally important part: when the lag dashboard shows the radar's source lag has doubled while the optical's is unchanged, you know what to investigate.

### A Pitfall: Window Assignment Using Processing Time

The buggy operator below assigns observations to 5-second windows by calling `Instant::now()` at processing time. A teammate proposes it during code review with the rationale "this is simpler and the difference is small." It is wrong. The example shows the failure mode the lesson keeps warning about.

```rust,ignore
// BUG: assigns to windows by processing time, not by event time.
// Observations of the same event from different sources land in
// different windows because they arrive at slightly different times.
pub async fn buggy_window_assigner(
    mut input: mpsc::Receiver<Observation>,
    output: mpsc::Sender<(WindowId, Observation)>,
) -> Result<()> {
    let start = Instant::now();
    while let Some(obs) = input.recv().await {
        let elapsed_secs = start.elapsed().as_secs();
        let window_id = WindowId(elapsed_secs / 5); // 5-second windows
        output.send((window_id, obs)).await?;
    }
    Ok(())
}

// CORRECT: assigns by sensor_timestamp. Observations of the same
// physical event land in the same window regardless of arrival skew.
pub async fn correct_window_assigner(
    mut input: mpsc::Receiver<Observation>,
    output: mpsc::Sender<(WindowId, Observation)>,
    epoch: SystemTime,
) -> Result<()> {
    while let Some(obs) = input.recv().await {
        let event_offset = obs
            .sensor_timestamp
            .duration_since(epoch)
            .unwrap_or_default();
        let window_id = WindowId(event_offset.as_secs() / 5);
        output.send((window_id, obs)).await?;
    }
    Ok(())
}
```

Two observations that physically co-occurred — same orbital event, same instant of detection — but arriving at the pipeline 100 milliseconds apart land in *different* processing-time windows whenever the 100-ms gap straddles a 5-second boundary. About 2% of correlated events per the optical-radar arrival distribution, in the SDA pipeline's actual deployment, would be mis-correlated by the buggy operator. That is enough to produce a phantom-conjunction rate that operations notices but cannot easily explain — every correlator failure produces a defensible-looking output, and only the aggregate statistics reveal the bias. The fix is exactly the operator above: bucket by `sensor_timestamp`, not by elapsed processing time. Lesson 2 develops this into a full windowing operator with bounded memory and explicit close conditions.

---

## Key Takeaways

* Every observation carries **two operationally distinct timestamps**: `sensor_timestamp` (when the event happened in the world) and `ingest_timestamp` (when the pipeline received it). Processing time at any given operator can be collapsed to ingest time for SDA's purposes; the distinction that matters is event time vs ingest time.
* The right time depends on the question. **Throughput and SLO metrics** want ingest/processing time. **Window assignment for correlation** wants event time. Confusing them produces output that looks correct in aggregate but is silently wrong in ways that only the aggregate-of-aggregates statistics reveal.
* Sensor clocks are heterogeneous: GPS-locked (radar, ~100ns), NTP-synced (optical, ~10ms), onboard-with-drift (ISL, up to seconds). Carry **clock quality forward in the envelope** so downstream operators can widen matching windows or downweight observations from degraded sources.
* **Out-of-order arrival is the rule** in event-time pipelines. Late observations from slow sources arrive after observations of later events from faster sources. Every event-time operator must be designed with this assumption; the watermark protocol in Lesson 3 makes the rule operational.
* **Lag** (`now - sensor_timestamp`) is the master diagnostic for an event-driven pipeline. Split into **source lag** and **pipeline lag** to answer "is the problem ours or theirs?" without ambiguity. Module 6 builds the full observability stack on top of this primitive.

---

{{#quiz lesson-01-quiz.toml}}
