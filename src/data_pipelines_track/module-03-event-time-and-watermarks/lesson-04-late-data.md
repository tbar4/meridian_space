# Lesson 4 — Late Data

**Module:** Data Pipelines — M03: Event Time and Watermarks
**Position:** Lesson 4 of 4
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (Out-of-Order Events and corrections); *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 7 (Late-Arriving Data); *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 14 (Out-of-Sequence Events and Reprocessing)

---

## Context

A watermark is a guarantee, but the guarantee is built on an estimate — the per-source `max_lateness`. The estimate is calibrated to cover the typical worst case, not every possible case. Eventually a source's actual lateness exceeds its estimate. The optical archive's polling cadence runs slow because the partner's API is degraded; an observation arrives 35 seconds after its event time when `max_lateness` was set to 30. The radar's fiber path takes a 200-ms detour through a ground-station relay; an observation arrives 250 ms after its event time when `max_lateness` was 100 ms. In every case, the observation arrives *after* the watermark for its window has already passed. The window has already been closed and emitted. The observation is *late*.

Three things can happen with a late event. **Drop it.** Cheap, lossy, the default in many systems. **Accumulate it into a re-opened window.** Hold the window's state in a holding pattern past its watermark close, accept events into it for a bounded *allowed lateness* period, re-emit the window's result on each accepted late event. Medium cost, requires the downstream to handle re-emissions. **Retract and re-emit.** Emit a "negation" of the previously-emitted result, then emit a corrected one. Most expensive, requires the downstream to handle retractions, gives the strongest correctness guarantee.

The choice is operational. For SDA's conjunction-alert pipeline, the cost of a missed alert (a real conjunction not flagged) is collision risk; the cost of a phantom alert (a false conjunction emitted then never corrected) is a needless avoidance maneuver burning satellite fuel. The right choice depends on which cost is being weighed against what. For high-rate dashboards (events-per-second, throughput summaries), drop is fine — individual late events do not change the aggregate. For batched analytics where eventual completeness matters, accumulate-with-bound is the right shape. For human-actionable alerts, retract-and-correct is necessary because a phantom alert that is never withdrawn produces lasting downstream effects. This lesson covers all three, develops the implementation patterns, and closes the module by tying back to the orchestrator's at-least-once-plus-idempotency frame from Module 2.

---

## Core Concepts

### The Three Strategies

**Drop.** When a late event arrives — its sensor_timestamp falls within an already-closed window — discard it. The window's emitted result remains the canonical answer. The lost event contributes nothing. Cost: minimal. Cost paid: events that should have contributed to the result do not. Best for high-rate streams where individual events are statistically insignificant.

**Accumulate (Allowed Lateness).** Each window's state is held for a bounded `allowed_lateness` period past its watermark-driven close. Late events arriving within `[window_end, window_end + allowed_lateness]` in event time are accepted into the window's state, and the window's result is re-emitted with the additional event included. After the allowed-lateness period expires (the watermark advances past `window_end + allowed_lateness`), the window's state is finally evicted; events arriving past that point are dropped. Cost: state held longer; downstream must handle re-emission of previously-emitted results. Best for analytics-style pipelines where some delay is acceptable.

**Retract.** When a late event arrives within a previously-emitted window's lateness period, emit two records: a *retraction* of the previously-emitted result, then an *insertion* of the corrected result. The downstream consumer treats retraction-then-insertion as an atomic correction. Cost: highest — requires retraction-aware downstream. Best for human-actionable outputs where a wrong result that is never corrected is worse than a delayed-but-correct one.

The three are a progression: drop is a special case of accumulate-with-zero-allowed-lateness; accumulate is a special case of retract that does not bother distinguishing the previous result from the next; retract is the most general. Most production pipelines use a mix: drop for non-critical metrics, accumulate for batched reporting, retract for human-actionable alerts.

### Allowed Lateness, Concretely

The accumulate strategy parameterizes `allowed_lateness` per operator. A windowed correlator with `allowed_lateness = 5s` holds each closed window's state for 5 seconds (in event time, not wall clock) past its watermark close. In code, this means: when the watermark advances past `window_end`, the operator emits the window's result but does *not* free the window's state; the state stays in a *retained* state. Late events arriving while the watermark is in `[window_end, window_end + allowed_lateness]` are added to the retained state and trigger a re-emission. When the watermark advances past `window_end + allowed_lateness`, the state is finally evicted.

The memory cost of allowed lateness scales as `(allowed_lateness / window_size) × window_state`. A 30-second window with 5-second allowed lateness holds ~1.17x the steady-state window memory; a 30-second window with 30-second allowed lateness doubles it. The per-key cardinality multiplies through the same ratio. For SDA's correlator at typical scales (tens of thousands of orbital objects, 30-second windows, 5-second allowed lateness), the additional memory is bounded and tractable.

The operational tuning is per-pipeline. Aggressive allowed-lateness (small, e.g. 1s) → low memory cost, fast window finalization → late events past 1s are silently lost. Conservative allowed-lateness (large, e.g. 60s) → higher memory cost, slow finalization → almost all late events captured. The right choice depends on the lateness distribution of the slowest source. SDA's setting of 5s on top of optical's 30s max_lateness covers the long tail of optical archive delays without doubling memory.

### Retractions

A retraction is a downstream-visible "undo." For a window that previously emitted result R1, the operator emits a retraction of R1 followed by an insertion of R2 (the corrected result). The downstream subscriber sees the pair as: invalidate the previously-stated R1; the correct value is now R2.

The implementation requires a *sequence number* on each emit so the downstream can match retractions to the correct previous emit. The convention this track uses: each window has a `(window_id, sequence)` pair on every emission, with sequence starting at 0 for the first emit and incrementing on each correction. The downstream uses `(window_id, sequence)` as the primary key with last-write-wins semantics — at any point in time, the latest sequence for a given window_id is the canonical answer.

The retraction emission shape:

```rust,ignore
pub enum WindowEmit {
    Insert { window_id: WindowId, sequence: u32, result: WindowResult },
    Retract { window_id: WindowId, sequence: u32 },
}
```

A retract-then-insert pair on window 17 looks like: `Retract { window_id: 17, sequence: 1 }` followed by `Insert { window_id: 17, sequence: 2, result: corrected }`. The downstream's stored state, keyed on window_id, is updated to sequence 2's result. The sequence number prevents a delayed retraction from being applied to a *newer* result emitted in the meantime: if the retraction is for sequence 1 but the downstream has already stored sequence 2, the retract is a no-op.

### The Idempotency Requirement Downstream

Retractions only work when the downstream is *idempotent on (window_id, sequence)*. A SQL sink with `ON CONFLICT (window_id) DO UPDATE SET ... WHERE EXCLUDED.sequence > stored.sequence` is idempotent. A Kafka topic where the consumer dedups on `(window_id, sequence)` is idempotent. An HTTP webhook that does not respect any keying is *not* idempotent — duplicate retractions or out-of-order retract/insert pairs produce wrong final state at the subscriber.

The pattern composes with Module 2's at-least-once-plus-idempotency frame: the pipeline emits at least once (with retries on transient failures) and the downstream is idempotent (on `(window_id, sequence)`), giving effective exactly-once delivery of the corrected stream. Module 5 covers the full machinery for cross-pipeline exactly-once, including transactional Kafka producers; this lesson establishes the pattern at the operator level.

### Choosing the Strategy

A decision table for SDA's pipeline:

| Output | Strategy | Reasoning |
|---|---|---|
| `events_per_minute` dashboard counter | Drop | Late events negligible at this granularity; dashboard precision is loose |
| `conjunction_risk_summary` analytics emit | Accumulate (5s) | Eventual completeness matters; some delay acceptable |
| `conjunction_alert` to subscriber | Retract | Phantom alerts cause real-world action (avoidance maneuvers); must be correctable |
| `audit_log` of every observation | Drop | The audit log is the input record, not a derived computation; latency is irrelevant |

The decision is per-output, not per-pipeline. The same windowed correlator can produce different outputs with different strategies — a retract-emitting alert stream alongside a drop-emitting metrics stream. The implementation factors out the strategy as a per-output configuration that the operator dispatches on.

---

## Code Examples

### Allowed-Lateness Window Operator

The L2 tumbling-window operator extended with allowed-lateness retention. Window state is held in two tiers: *active* (window not yet closed) and *retained* (window closed by watermark, held for late events for `allowed_lateness`).

```rust,ignore
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};
use anyhow::Result;
use tokio::sync::mpsc;

pub struct AllowedLatenessWindow {
    /// Active windows: state still being accumulated, watermark hasn't passed.
    active: BTreeMap<SystemTime, WindowState>,
    /// Retained windows: emitted once, held for allowed_lateness in case
    /// late events arrive. Will be re-emitted on each accepted late event.
    retained: BTreeMap<SystemTime, WindowState>,
    window_size: Duration,
    allowed_lateness: Duration,
    epoch: SystemTime,
    output: mpsc::Sender<WindowEmit>,
}

#[derive(Debug, Clone)]
struct WindowState {
    observations: Vec<Observation>,
    sequence: u32,
}

impl AllowedLatenessWindow {
    pub fn new(
        window_size: Duration,
        allowed_lateness: Duration,
        epoch: SystemTime,
        output: mpsc::Sender<WindowEmit>,
    ) -> Self {
        Self {
            active: BTreeMap::new(),
            retained: BTreeMap::new(),
            window_size,
            allowed_lateness,
            epoch,
            output,
        }
    }

    /// Ingest an observation, dispatching on whether it lands in an
    /// active window or a retained (late) window.
    pub async fn ingest(&mut self, obs: Observation) -> Result<()> {
        let window_end = self.window_end_for(obs.sensor_timestamp);
        if let Some(state) = self.active.get_mut(&window_end) {
            state.observations.push(obs);
            return Ok(());
        }
        if let Some(state) = self.retained.get_mut(&window_end) {
            // Late event into a retained window — accept and re-emit.
            state.observations.push(obs);
            state.sequence += 1;
            self.emit(window_end, state.clone(), EmitKind::Insert).await?;
            return Ok(());
        }
        // Fresh window.
        self.active.insert(
            window_end,
            WindowState { observations: vec![obs], sequence: 0 },
        );
        Ok(())
    }

    /// Watermark advance: close every active window whose end ≤ watermark
    /// (move to retained); evict every retained window whose end +
    /// allowed_lateness ≤ watermark (final eviction, no more late events
    /// accepted for this window).
    pub async fn on_watermark(&mut self, watermark: SystemTime) -> Result<()> {
        // Move closed-by-watermark windows from active to retained, emitting
        // the initial result on the way through.
        let still_active = self.active.split_off(&(watermark + Duration::from_nanos(1)));
        let to_close = std::mem::replace(&mut self.active, still_active);
        for (window_end, state) in to_close {
            self.emit(window_end, state.clone(), EmitKind::Insert).await?;
            self.retained.insert(window_end, state);
        }
        // Evict retained windows whose lateness budget is exhausted.
        let retain_cutoff = watermark.checked_sub(self.allowed_lateness)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        let still_retained = self.retained.split_off(&(retain_cutoff + Duration::from_nanos(1)));
        let to_evict = std::mem::replace(&mut self.retained, still_retained);
        // Evicted windows are silently dropped; their state is gone.
        // Lesson 4 also discusses the retraction strategy for cases where
        // even-after-eviction late events need to update results.
        drop(to_evict);
        Ok(())
    }

    fn window_end_for(&self, ts: SystemTime) -> SystemTime {
        let offset = ts.duration_since(self.epoch).unwrap_or_default();
        let idx = offset.as_secs() / self.window_size.as_secs().max(1);
        self.epoch + self.window_size * (idx + 1) as u32
    }

    async fn emit(&self, window_end: SystemTime, state: WindowState, _kind: EmitKind) -> Result<()> {
        let result = WindowResult {
            window_end,
            count: state.observations.len(),
        };
        self.output.send(WindowEmit::Insert {
            window_id: WindowId(window_end),
            sequence: state.sequence,
            result,
        }).await
            .map_err(|_| anyhow::anyhow!("downstream dropped"))
    }
}

enum EmitKind { Insert, Retract }
```

The two-tier state — active and retained — is the structural pattern. Active windows accumulate; the watermark advance moves them to retained and triggers their first emit; retained windows can still receive late events and re-emit; final eviction at `watermark - allowed_lateness` frees the state for good. The two-tier structure makes the lateness behavior explicit rather than implicit; an operator with a single tier and ad-hoc "late event" handling tends to grow correctness bugs as the pattern complicates. The cost of two tiers is a few extra BTreeMap operations per watermark advance — negligible at any realistic scale.

### A Retracting Operator

The retract strategy emits explicit `Retract` records before each `Insert` of a corrected result. The downstream is responsible for processing the pair atomically. The operator's emit logic factors slightly differently than the accumulate-only version above.

```rust,ignore
async fn emit_retract_then_insert(
    output: &mpsc::Sender<WindowEmit>,
    window_end: SystemTime,
    prev_sequence: u32,
    new_state: &WindowState,
) -> Result<()> {
    let window_id = WindowId(window_end);
    // Retract the previously-emitted sequence.
    output.send(WindowEmit::Retract {
        window_id,
        sequence: prev_sequence,
    }).await
        .map_err(|_| anyhow::anyhow!("downstream dropped"))?;
    // Then the corrected result at the new sequence.
    output.send(WindowEmit::Insert {
        window_id,
        sequence: new_state.sequence,
        result: WindowResult {
            window_end,
            count: new_state.observations.len(),
        },
    }).await
        .map_err(|_| anyhow::anyhow!("downstream dropped"))?;
    Ok(())
}
```

The retraction must be emitted *before* the corrected insert. Otherwise the downstream sees insert-then-retract — the corrected result lands first, gets retracted, and the downstream's stored state ends up empty. The two-step pattern depends on the channel preserving FIFO ordering, which `mpsc::Sender::send` does. The sequence on the retract is the *previous* sequence; the sequence on the insert is the new one. The downstream, keyed on `(window_id, sequence)`, processes the two records in order and ends up with the corrected state. This is the pattern that makes retract-and-correct safe under at-least-once delivery: the downstream's last-write-wins semantics absorb duplicate or out-of-order retract/insert pairs as long as the sequence ordering is respected.

### A Retraction-Aware Sink

The sink that consumes the retractor's output. It uses an embedded SQLite as its idempotent state store. The schema is `(window_id, sequence, result_blob)`; UPSERT on conflict by window_id with the higher sequence winning.

```rust,ignore
use rusqlite::{params, Connection};
use std::time::SystemTime;

const UPSERT_SQL: &str = r#"
    INSERT INTO window_results (window_id, sequence, result_blob)
    VALUES (?1, ?2, ?3)
    ON CONFLICT (window_id) DO UPDATE
    SET sequence = excluded.sequence, result_blob = excluded.result_blob
    WHERE excluded.sequence > window_results.sequence
"#;

const RETRACT_SQL: &str = r#"
    DELETE FROM window_results
    WHERE window_id = ?1 AND sequence = ?2
"#;

pub fn apply_emit(conn: &Connection, emit: WindowEmit) -> rusqlite::Result<()> {
    match emit {
        WindowEmit::Insert { window_id, sequence, result } => {
            let blob = serde_json::to_vec(&result).unwrap();
            conn.execute(UPSERT_SQL, params![
                window_id.0.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as i64,
                sequence,
                blob,
            ])?;
        }
        WindowEmit::Retract { window_id, sequence } => {
            conn.execute(RETRACT_SQL, params![
                window_id.0.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as i64,
                sequence,
            ])?;
        }
    }
    Ok(())
}
```

The `WHERE excluded.sequence > window_results.sequence` clause is what makes the UPSERT idempotent: a duplicate insert at sequence N (delivered twice by at-least-once) produces no change because the comparison is strict. A retraction's `WHERE` clause matches on both window_id and sequence, so a stale retraction (window_id matches but sequence is below the current stored value) deletes nothing — exactly the right behavior. The composition with the at-least-once-with-retries delivery layer (Module 2's `with_retry`) gives the exactly-once-effective property the lesson promises.

---

## Key Takeaways

* Late events arrive when a source's actual lateness exceeds its `max_lateness` estimate. The pipeline has **three strategies**: drop (cheap, lossy), accumulate-with-allowed-lateness (medium cost, eventual completeness), retract-and-correct (highest cost, strongest correctness guarantee). The choice is per-output, not per-pipeline.
* **Allowed lateness** holds window state for a bounded period past the watermark close. The state lives in a *retained* tier alongside the *active* tier; late events into retained windows trigger re-emit; final eviction at `watermark - allowed_lateness` frees the state. Memory scales as `(allowed_lateness / window_size) × steady_state_memory`.
* **Retractions** emit a `Retract` of the previously-emitted result before each corrected `Insert`. The downstream is keyed on `(window_id, sequence)` with last-write-wins semantics; sequence numbers prevent delayed retractions from clobbering newer results. Retract-then-insert ordering is FIFO-channel-dependent and must not be reordered.
* Retractions only work with **idempotent downstream**: SQL `ON CONFLICT DO UPDATE` keyed on window_id with strict-greater sequence comparison, Kafka consumers that dedup on `(window_id, sequence)`, or any sink whose effect on the world is keyed on the same identifier the operator emits. Non-idempotent downstreams produce wrong final state under retry.
* The lateness machinery composes with Module 2's at-least-once-plus-idempotency frame: the operator emits at least once with retries on transient failures, the downstream is idempotent on `(window_id, sequence)`, giving **effective exactly-once** delivery of the corrected stream. Module 5 generalizes this pattern to cross-pipeline boundaries.

---

{{#quiz lesson-04-quiz.toml}}
