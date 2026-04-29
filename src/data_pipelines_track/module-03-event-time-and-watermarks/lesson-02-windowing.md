# Lesson 2 — Windowing

**Module:** Data Pipelines — M03: Event Time and Watermarks
**Position:** Lesson 2 of 4
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (Windowing); *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 14 (Time Windows in Stream Processing); *Streaming Data* — Andrew Psaltis, Chapter 4 (Windowing patterns and their implementation costs)

---

## Context

Lesson 1 established that observations carry an event time and that correlation logic must bucket them by event time rather than by arrival time. This lesson is about how to do that bucketing efficiently and correctly. A *window* is a bounded slice of the event stream — defined by the rule "all events whose event-time falls within this range" — that the operator can hold in memory, compute over, and emit a result for. Windows are how unbounded streams admit aggregation: you cannot compute "all conjunction risks ever," but you can compute "all conjunction risks where the contributing observations fell within this 30-second event-time slice."

The choice of window shape is a correctness decision, not a performance optimization. The conjunction risk computation cares about pairs of orbital objects whose closest-approach time falls within a small window. If we use the wrong window shape — say, fixed 30-second buckets aligned to clock minutes — every legitimate conjunction whose closest approach happens to straddle a bucket boundary is silently missed. Choosing the right shape requires understanding the four canonical kinds (tumbling, hopping, sliding, session), the cost profile of each, and the question each is shaped to answer. This lesson takes them in turn and develops a sliding-window operator that the capstone correlator builds on.

The forward references stay concrete. The window operator built here holds events in memory until it can declare a window "closed" and emit its result. Lesson 3 supplies the close mechanism — watermarks. Lesson 4 handles late events that arrive after a window has been closed. The capstone project replaces M2's processing-time dedup with this lesson's sliding-window correlator. The pattern of "windowed accumulation, watermark-driven emit, allowed-lateness for stragglers" is the standard streaming-system shape; we are building the SDA-specific instance of it.

---

## Core Concepts

### Tumbling Windows

The simplest window shape. Fixed size, non-overlapping, every event lands in exactly one window. A 5-second tumbling window over an event-time stream produces one window for `[0, 5)`, the next for `[5, 10)`, the next for `[10, 15)`, and so on. The boundaries are typically aligned to a stable epoch (00:00:00 UTC, or the pipeline's start time, or some other fixed reference) so that a given event_time always maps to the same window across replicas.

Tumbling windows are the right choice for *aggregates over disjoint time slices*: events-per-minute counts, hourly throughput summaries, "how many observations did each sensor produce this minute." They are the wrong choice for any question of the form "find pairs of events within a small time gap" — the gap can straddle a tumbling-window boundary, and events in adjacent windows are never seen together by the operator. The conjunction-risk computation is exactly that kind of question, which is why M3 needs sliding windows rather than tumbling.

Memory cost: bounded by the largest single window's events. Once a window closes (Lesson 3), its state is freed. State per active window scales with the window size and the per-key event rate.

### Hopping Windows

A generalization of tumbling. Fixed size, fixed *step* (sometimes called *advance* or *slide*), step typically smaller than size — which produces overlap. A 30-second window with 5-second step produces windows for `[0, 30)`, `[5, 35)`, `[10, 40)`, ..., overlapping by 25 seconds each. Every event lands in `size / step` windows simultaneously (`30 / 5 = 6` here).

Hopping windows are useful for "rolling aggregates emitted on a regular cadence" — every 5 seconds, emit the count of events in the last 30 seconds. The emit cadence (step) is decoupled from the aggregation breadth (size). Some streaming systems call this a sliding window; the terminology is unfortunately not standardized. We use *hopping* for fixed-size-fixed-step and reserve *sliding* for the per-event-driven shape below.

Memory cost: scales with `size / step`. Each event is held in `size / step` windows simultaneously. For a 30-second window with 1-second step, every event is in 30 windows. The per-event memory is small (a reference, not a copy), but the constant factor is real and shows up at scale.

### Sliding Windows (Per-Event)

Every event creates its own window of `[event_time - W, event_time]` for some window size `W`. Maximum overlap. For a 30-second sliding window, an event at time T anchors a window covering T-30 to T, and the next event at T+1 anchors a window covering T-29 to T+1. The "matching set" for any given event is whatever other events fall within W of its event_time.

This is the right shape for the conjunction-risk question. The natural framing of the problem is "for each new observation, what other observations are within 30 seconds of event_time?" — exactly what a per-event sliding window computes. Production systems implement sliding windows as a deque per key, with the front evicted as new events arrive that push the deque's tail past the window boundary. The capstone operator builds this.

Memory cost: bounded by the most-active key's event rate × W. For a 30-second window over a key that sees 100 events per second, the deque holds 3000 entries — trivial. For a key that sees 100,000 events per second the deque is much larger; mitigations are key-level rate limits, sampling, or shorter windows. Module 4's load-shedding work develops these mitigations.

### Session Windows

Variable-length, defined by gap of inactivity. A session window collects events while they keep arriving within a *gap timeout* of each other; when the gap is exceeded, the window closes. The classic use is "user web session" — collect events while the user is active, close the session when they go idle for 5 minutes. For SDA, the natural use is "satellite pass" — a satellite's beacons during a single overhead pass form a session, with the gap between passes (when the satellite is below the horizon) closing the session.

Session windows are the only window shape with state proportional to the active session's duration, not to a fixed configuration. A satellite that orbits for 90 minutes during a long pass produces a 90-minute session; a quiet pass might be 10 seconds. The gap timeout is what bounds the session — without it, a continuous event stream produces an unbounded session that never closes.

Memory cost: the dominant risk. A misconfigured gap timeout (too long) or a continuous source (a beacon that emits without gaps) produces unbounded growth. Production code adds a hard *maximum session duration* alongside the gap timeout: even if events keep arriving, force-close after N minutes. The double-bound makes session windows safe to deploy.

### Window State and the Close Trigger

For every window class, the operator maintains state per active window: the events that have been assigned to it and any aggregations being computed (running counts, partial join results, etc.). The state grows as events arrive and shrinks when windows close. The *close trigger* — the condition under which the operator declares "this window is done; emit its result and discard its state" — is the single most important correctness decision in a windowed operator.

The naive close trigger is wall-clock time: a 5-second tumbling window starting at T closes when wall-clock-time reaches T+5. This is wrong in event-time semantics because late events with `sensor_timestamp ≤ T+5` may still arrive after wall-clock-time T+5 — a tumbling window closed by wall clock would miss them. The correct close trigger is the *watermark* — Lesson 3's full topic. The intuition we install here is structural: the windowed operator does not decide on its own when to close a window; it consumes a watermark signal that tells it "no event with event_time ≤ X will arrive after this point," and it closes any window whose end ≤ X.

Until Lesson 3, this lesson's operator implementations declare windows closed via a placeholder mechanism (an explicit `close_windows_up_to(T)` method). The capstone operator wires that placeholder to a real watermark stream.

---

## Code Examples

### Tumbling Window Operator

The simplest implementation. A `BTreeMap<WindowEnd, WindowState>` keyed on the window's end time. Each event is bucketed by its sensor_timestamp; close-up-to-watermark walks the map's prefix and emits.

```rust,ignore
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};
use anyhow::Result;
use tokio::sync::mpsc;

/// Tumbling-window operator. Emits aggregated state for every window
/// whose end has been crossed by the watermark. Close-trigger is supplied
/// externally via close_up_to(); Lesson 3 wires this to a watermark.
pub struct TumblingWindow {
    /// Windows by end time. BTreeMap gives O(log N) range-prefix iteration
    /// in close_up_to(), which is the hot path on watermark advance.
    windows: BTreeMap<SystemTime, Vec<Observation>>,
    window_size: Duration,
    epoch: SystemTime,
    output: mpsc::Sender<WindowResult>,
}

impl TumblingWindow {
    pub fn new(
        window_size: Duration,
        epoch: SystemTime,
        output: mpsc::Sender<WindowResult>,
    ) -> Self {
        Self {
            windows: BTreeMap::new(),
            window_size,
            epoch,
            output,
        }
    }

    /// Assign an observation to its window and accumulate it.
    pub fn ingest(&mut self, obs: Observation) {
        let offset = obs.sensor_timestamp.duration_since(self.epoch).unwrap_or_default();
        let window_idx = offset.as_secs() / self.window_size.as_secs().max(1);
        let window_end = self.epoch + self.window_size * (window_idx + 1) as u32;
        self.windows.entry(window_end).or_default().push(obs);
    }

    /// Close every window whose end ≤ watermark. Emit result, free state.
    pub async fn close_up_to(&mut self, watermark: SystemTime) -> Result<()> {
        // BTreeMap::split_off gives us O(log N) access to the prefix
        // we want to drain. The remainder stays in self.windows.
        let still_open = self.windows.split_off(&(watermark + Duration::from_nanos(1)));
        let to_close = std::mem::replace(&mut self.windows, still_open);
        for (window_end, observations) in to_close {
            let result = WindowResult { window_end, count: observations.len() };
            self.output.send(result).await
                .map_err(|_| anyhow::anyhow!("window result downstream dropped"))?;
        }
        Ok(())
    }

    /// For diagnostics: number of windows currently held in state.
    pub fn pending_window_count(&self) -> usize { self.windows.len() }
}

#[derive(Debug, Clone)]
pub struct WindowResult {
    pub window_end: SystemTime,
    pub count: usize,
}
```

The BTreeMap-keyed-on-window-end choice is load-bearing for this operator. The hot path on watermark advance is "find every window whose end ≤ watermark," which is a prefix range query; BTreeMap does this in O(log N) via `split_off`. A HashMap-keyed-on-window-id would require a full scan on every watermark advance — fine for a few windows, untenable when the operator holds hundreds. The cost of BTreeMap inserts (O(log N) instead of HashMap's O(1) amortized) is paid back many times over by the cheap range query on close. The pattern generalizes: for any operator whose hot path is "find everything ≤ X," choose a data structure that gives you that operation cheaply. A naive `HashMap.iter().filter()` is O(N) and quietly catastrophic at scale.

### Sliding Window Operator (Per-Event)

The conjunction-risk operator's foundation. A `VecDeque<Observation>` per key. New events are appended to the back; on each new event, evict from the front any events whose `sensor_timestamp` is older than `event_time - W`. The deque always represents the current sliding window for that key.

```rust,ignore
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

/// Per-key sliding window operator. Each new event for a key emits
/// the set of other events within W of its event_time as a candidate
/// match list. The conjunction correlator (capstone project) builds
/// on this primitive.
pub struct SlidingWindow {
    deques: HashMap<ObjectId, VecDeque<Observation>>,
    window: Duration,
}

impl SlidingWindow {
    pub fn new(window: Duration) -> Self {
        Self { deques: HashMap::new(), window }
    }

    /// Process an observation. Evict expired entries, append the new
    /// observation, return the current contents of the window for
    /// downstream matching.
    pub fn step(&mut self, obs: Observation) -> &VecDeque<Observation> {
        let key = obs.target_object_id.clone();
        let deque = self.deques.entry(key.clone()).or_default();
        let cutoff = obs.sensor_timestamp - self.window;
        // Evict from front while the front's event_time is older than
        // the cutoff. Front is the oldest; the deque is event-time
        // ordered by construction (we only append to the back, and
        // each appended event has a sensor_timestamp ≥ all existing
        // ones, modulo out-of-order arrival within the window).
        while let Some(front) = deque.front() {
            if front.sensor_timestamp < cutoff {
                deque.pop_front();
            } else {
                break;
            }
        }
        deque.push_back(obs);
        // Return a reference; caller decides what matching to compute.
        &*deque
    }

    /// Drop a key's deque entirely — used when the watermark indicates
    /// no more events will arrive for this key (e.g., a satellite
    /// has decayed). Not used in the steady-state hot path.
    pub fn close_key(&mut self, key: &ObjectId) -> Option<VecDeque<Observation>> {
        self.deques.remove(key)
    }

    pub fn pending_keys(&self) -> usize { self.deques.len() }
}
```

The eviction-on-each-event pattern keeps the deque always sized to the current window for that key — no separate "garbage collect old entries" pass needed. Two design choices that are subtle. The eviction cutoff is `obs.sensor_timestamp - self.window`, not `now - self.window`; we are doing event-time windowing, so the cutoff is in event time, not wall-clock. The deque is event-time ordered by the assumption that within a single key, observations arrive in roughly event-time order — which is true for a single-source single-key stream and approximately true for a multi-source stream (out-of-order is possible within tens of milliseconds). Strict event-time ordering is *not* required; the eviction loop simply advances while the front is older than the cutoff and stops. A late event whose sensor_timestamp is older than the cutoff is silently dropped — that is the *late event* problem that Lesson 4 covers in full.

### A Session Window with Hard-Cap Safety

The ISL beacon's per-satellite pass. Events arrive while the satellite is overhead; the session closes when the satellite goes below the horizon (gap exceeds threshold) or, as a safety valve, when the session has been open for longer than a configured maximum.

```rust,ignore
use std::time::{Duration, Instant};

pub struct SessionWindow {
    session_start: Instant,
    last_event: Instant,
    gap_timeout: Duration,
    max_session: Duration,
    events: Vec<Observation>,
}

impl SessionWindow {
    pub fn new(first: Observation, gap_timeout: Duration, max_session: Duration) -> Self {
        let now = Instant::now();
        Self {
            session_start: now,
            last_event: now,
            gap_timeout,
            max_session,
            events: vec![first],
        }
    }

    /// Add an event to the session if the gap is acceptable. Returns
    /// Err with the new event if the session has timed out and the
    /// caller should start a fresh session.
    pub fn try_add(&mut self, obs: Observation) -> Result<(), Observation> {
        let now = Instant::now();
        let gap_ok = now.duration_since(self.last_event) < self.gap_timeout;
        let max_ok = now.duration_since(self.session_start) < self.max_session;
        if gap_ok && max_ok {
            self.last_event = now;
            self.events.push(obs);
            Ok(())
        } else {
            Err(obs)
        }
    }

    pub fn close(self) -> Vec<Observation> { self.events }

    pub fn open_duration(&self) -> Duration { self.last_event.duration_since(self.session_start) }
}
```

The double-bound pattern (gap timeout AND max session duration) is the safety property that makes session windows production-safe. A gap-only design hits unbounded memory the first time a source emits without any gap (a stuck-open beacon, a misconfigured emitter); the max-session bound is the safety valve that always fires. The cost of the bound is occasional artificial session breaks for legitimately-long sessions — acceptable for SDA's beacon-pass use case (passes are bounded by orbital mechanics) and adjustable per use case. The session window operator is the third canonical shape; the SDA capstone uses sliding windows (the second shape), but the pattern generalizes.

---

## Key Takeaways

* Window shape is a **correctness decision**, not a performance optimization. The four canonical shapes — **tumbling** (disjoint, fixed-size), **hopping** (overlapping, fixed-step), **sliding** (per-event), **session** (gap-defined) — each fit different question shapes. The conjunction-risk question fits sliding windows; aggregations fit tumbling; emit-on-cadence fits hopping; satellite-pass fits session.
* The window operator does not decide its own close trigger. It consumes a **watermark signal** from upstream that says "no event with event_time ≤ X will arrive" and closes any window whose end ≤ X. This decouples the window's accumulation semantics from the close logic; Lesson 3 supplies the watermark.
* **BTreeMap keyed on window end** gives the close-up-to-watermark hot path O(log N) prefix iteration. Choose data structures whose hot-path operations are cheap; a HashMap with O(N) iteration is quietly catastrophic at scale.
* **Sliding windows are bounded by the most-active key's event rate × W**. Per-key deques with on-each-event front eviction keep memory always sized to the current window; no separate GC pass needed.
* **Session windows need a hard maximum** alongside the gap timeout. A gap-only design hits unbounded memory the first time a source streams continuously; the max-session bound is the safety valve.

---

{{#quiz lesson-02-quiz.toml}}
