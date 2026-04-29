# Lesson 2 — Exactly-Once via Idempotency

**Module:** Data Pipelines — M05: Delivery Guarantees and Fault Tolerance
**Position:** Lesson 2 of 4
**Source:** *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 8 (Exactly-Once Semantics — Idempotent Producer, Transactions); *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (Idempotent Operations and Atomicity)

---

## Context

Lesson 1 established at-least-once delivery — every message reaches the consumer at least once, with duplicates as the operational cost. This lesson is the second half of the composition. The application makes its operations *idempotent*: processing the same message twice produces the same effect on the world as processing it once. The pair — at-least-once delivery + idempotent processing — gives effective exactly-once semantics. The pipeline's net effect is exactly-once even though the underlying transport admits duplicates. This is the standard production approach to exactly-once; true transport-level exactly-once requires coordination (transactional Kafka, two-phase commit) that costs more than the application-layer pattern in throughput, complexity, and failure modes.

The pattern is not new to this module. Module 2 L3 introduced idempotency keys carried on the envelope. Module 3 L4 added retract-aware sinks with strict-greater UPSERT semantics. This lesson develops the topic in depth: what makes an operation idempotent, where the natural keys come from, how to bound the dedup state, what Kafka's idempotent producer does at the broker level, and where the exactly-once guarantee holds versus where it doesn't. The capstone in this module composes all of it — at-least-once delivery from L1, idempotent processing from this lesson, checkpointing from L3, dead-letter queues from L4 — into a pipeline that survives the SDA-2026-0207 incident's failure mode without dropping or duplicating alerts.

---

## Core Concepts

### Idempotency, Defined

A function `f` is idempotent if `f(f(x)) = f(x)` — applying it twice with the same input produces the same result as applying it once. The classic example is setting a field to a specific value: setting `x = 5` is idempotent; setting `x += 1` is not. The streaming-system version is about *operator effects on durable state*: an operator that writes "the value of window 17 is `result`" is idempotent on the (window_id, result) pair; an operator that writes "increment the count of window 17" is not.

Idempotency is a property of the operation, not of the framework or the pipeline. The pipeline can be at-least-once at the transport layer; the application layer is what determines whether duplicates produce identical effects or accumulate. Each operator's effect on the world has its own idempotency story, and the system's overall behavior depends on every operator's individual choice.

The discipline this lesson installs: every operator that writes to durable state — a database, a topic, an external service — must declare in its design which operations are idempotent and on what key. The operator graph from M2 carries forward an explicit `idempotency_key_field` per stage, documented in the operator's specification. The capstone uses this metadata to assert at startup that every sink is configured idempotently against its expected duplicate-source.

### Natural Keys and Derived Keys

The natural key for SDA observations is the envelope's `observation_id` — a UUID generated at the source, carried unchanged through every stage, present on every observation. Every operator-level dedup logic uses this key. The orchestrator from M2 enforces that operators preserve `observation_id` across transformations (a normalize that re-generates the UUID is a bug; the supervisor's audit asserts this).

For derived events — the `ConjunctionRisk` that the M3 correlator emits from two observations — there is no natural ID. The standard derivation is a deterministic hash of the contributing inputs:

```
derived_id = hash(left_observation_id || right_observation_id || window_id)
```

Sorting the input IDs before hashing makes the result symmetric (same two observations produce the same derived_id regardless of order). The hash is content-addressable and reproducible: any two operator instances correlating the same pair of observations produce the same derived_id, which is what makes downstream dedup work.

For events derived from a sliding window over many inputs (analytics aggregates), the natural derived key is `(window_id, sequence)` — Module 3 L4's retract sequence numbering. The window is the deterministic identifier; the sequence number distinguishes the original emit from corrected emits.

### Bounded Dedup State

The sink-side dedup is implemented by a *seen-set*: a record of recently-seen IDs that the sink consults before applying each operation. A new ID is processed; a previously-seen ID is silently dropped. The set must be bounded — an unbounded seen-set is the silent-OOM pattern Module 4's audit catches.

The bound is by **time** or by **count**, ideally by both. Time-based: keep IDs seen in the last N seconds; evict older entries on each insert. Count-based: keep at most M IDs; evict the oldest when at capacity. The double-bound is the production-safety pattern: time alone fails when a burst causes more entries to land in the window than memory permits; count alone fails when a slow stream has its old entries evicted before the duplicates that would be deduped against them.

The window size is operationally determined: it must be larger than the maximum re-delivery window (the longest gap between an original send and a duplicate retry). For Kafka with default settings, this is typically minutes; for SDA's pipeline with 30-second consumer commit cadence and bounded checkpoint duration, 5 minutes is comfortable. Set the window too narrow and duplicates leak through; set it too wide and memory grows. Module 4's BurstProfile pattern applies here — document the chosen window with the rationale.

### Kafka's Idempotent Producer

Kafka offers a producer-side idempotency mechanism that prevents duplicates from producer retries. With `enable.idempotence=true`, the producer attaches a *producer ID* (PID) and a *sequence number* to every message. The broker tracks the highest sequence number seen per PID per partition; on a retry that arrives with a sequence number it has already accepted, the broker drops the duplicate silently. The producer continues to retry on transient errors, but the broker dedups before persisting.

The mechanism is single-partition, single-producer scoped — the dedup is per (PID, partition). Across partitions, the producer's idempotence does not provide a cross-partition consistency guarantee; that requires Kafka's transactional producer (a separate mechanism with `transactional.id` set). For SDA's pipeline, partition-scoped idempotence is sufficient because every observation is keyed on `observation_id` and routed to a partition by hash(observation_id), so duplicate retries land on the same partition where they get dedupped.

The throughput impact is small (<5%) and the configuration is straightforward. Production pipelines should enable `enable.idempotence=true` by default; the lesson's at-least-once L1 deliberately turned it off to demonstrate the bare semantics. Enabling it complements the application-layer dedup: producer-side dedup catches retries before they reach the broker; application-layer dedup catches duplicates from other sources (consumer crashes, rebalances). The two layers are belt-and-suspenders, and both are worth having.

### Where the Guarantee Holds

Effective exactly-once via at-least-once + idempotency holds at three boundaries.

**Within the pipeline.** Every operator that produces output to a downstream operator is implicitly part of the dedup chain because every operator preserves `observation_id`. The sink-side dedup at the end of the pipeline catches every duplicate that traveled from any source. This works as long as the chain is intact — no operator silently regenerates IDs, no operator emits a fresh ID for a copy of an existing observation.

**At external sink boundaries.** SQL writes use `INSERT ... ON CONFLICT (observation_id) DO NOTHING` (or `DO UPDATE` per Module 3's retract-aware shape). Kafka producers use `enable.idempotence=true`. HTTP requests carry an `Idempotency-Key` header (the standard HTTP convention) that the downstream service uses to dedup. Every external write has its own idempotency story, and the operator's responsibility is to know what it is and configure it.

**Where the guarantee does NOT hold.** Operations whose effect on the world is inherently non-idempotent — sending an email, charging a credit card, firing a satellite avoidance maneuver. For these, idempotency must be added at the boundary by storing recently-seen IDs and ignoring duplicates *at the consumer side of the boundary*, not just within the pipeline. The conjunction-alert subscriber from M5 L1's example is exactly this case; the capstone wires up the subscriber's seen-set as an integrated piece of the alert delivery path.

The lesson's framing is precise: the pipeline can guarantee effective exactly-once delivery TO a boundary; whether the action AT the boundary is itself exactly-once depends on the boundary's idempotency. Boundary owners must implement their own dedup; the pipeline's responsibility is to deliver the keys correctly and document the contract.

---

## Code Examples

### A Sliding-Window Dedup Set with Double-Bound Eviction

The sink-side primitive that absorbs duplicates from at-least-once delivery. Bounded by both time (seen IDs older than `window` are evicted) and count (no more than `capacity` IDs held at once).

```rust,ignore
use std::collections::{BTreeSet, VecDeque};
use std::time::{Duration, SystemTime};
use anyhow::Result;
use uuid::Uuid;

/// Bounded seen-set keyed on observation_id. Maintains:
///   - seen: a BTreeSet for O(log N) lookup of duplicate-or-not
///   - order: a VecDeque<(Uuid, SystemTime)> for FIFO eviction
///
/// Invariants:
///   - len(seen) == len(order)
///   - order is strictly time-ordered by insert time
///   - on every insert, expired entries are evicted from order's front
pub struct DedupSet {
    seen: BTreeSet<Uuid>,
    order: VecDeque<(Uuid, SystemTime)>,
    window: Duration,
    capacity: usize,
}

impl DedupSet {
    pub fn new(window: Duration, capacity: usize) -> Self {
        Self {
            seen: BTreeSet::new(),
            order: VecDeque::with_capacity(capacity),
            window,
            capacity,
        }
    }

    /// Record an observation; return true if the ID was novel (caller
    /// should process it), false if the ID is a duplicate (caller
    /// should drop).
    pub fn record(&mut self, id: Uuid, now: SystemTime) -> bool {
        // Evict by time first.
        let cutoff = now.checked_sub(self.window).unwrap_or(SystemTime::UNIX_EPOCH);
        while let Some(&(front_id, front_ts)) = self.order.front() {
            if front_ts < cutoff || self.order.len() >= self.capacity {
                self.order.pop_front();
                self.seen.remove(&front_id);
            } else {
                break;
            }
        }
        // Now check: if seen, return false (duplicate).
        if self.seen.contains(&id) { return false; }
        // Otherwise insert.
        self.seen.insert(id);
        self.order.push_back((id, now));
        true
    }

    pub fn len(&self) -> usize { self.seen.len() }
    pub fn is_empty(&self) -> bool { self.seen.is_empty() }
}
```

The `record` method is the only public mutation; the boolean return is the dispatch signal for the sink ("true → write, false → skip"). The order-of-operations matters: time-eviction first, then capacity-eviction, then duplicate check. Doing the duplicate check before eviction would leave time-expired entries in the seen set briefly, which is harmless but wastes memory. Doing eviction-then-insert means the seen set's size is always bounded by `capacity` after any single insert, regardless of how the inserts came in. The pattern is the L4 retract-aware sink's ancestor: same shape, same eviction discipline, different check direction (this records true-once-then-false; that one writes-once-then-overwrites).

### A SQL Sink with Idempotent UPSERT

The boundary-side idempotency. The sink writes observations to a Postgres table; the UPSERT with `DO NOTHING` is the idempotent operation.

```rust,ignore
use anyhow::Result;
use sqlx::{Pool, Postgres};

const UPSERT_OBSERVATION: &str = r#"
    INSERT INTO observations (observation_id, source_kind, sensor_timestamp, payload)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (observation_id) DO NOTHING
"#;

pub struct PostgresSink {
    pool: Pool<Postgres>,
}

impl PostgresSink {
    pub async fn write(&self, obs: &Observation) -> Result<()> {
        // The sql query is idempotent on observation_id (the primary key
        // with the ON CONFLICT clause). A duplicate row at the same
        // observation_id is silently ignored — the existing row is
        // preserved unchanged. The sink never produces a duplicate
        // effect on the world even under heavy at-least-once duplication.
        let payload = serde_json::to_value(obs)?;
        sqlx::query(UPSERT_OBSERVATION)
            .bind(obs.observation_id)
            .bind(format!("{:?}", obs.source_kind))
            .bind(obs.sensor_timestamp)
            .bind(payload)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
```

The `ON CONFLICT DO NOTHING` is the SQL-standard idempotent insert. A duplicate row at the same `observation_id` is silently rejected; the sink's `write` returns Ok regardless of whether the row was new or pre-existing. The duplicate cost is one network round-trip plus a brief lock on the existing row — meaningfully cheap, and well within the at-least-once duplicate budget. Alternative shapes: `DO UPDATE SET ... WHERE EXCLUDED.sequence > rows.sequence` for the M3 L4 retract-aware case; `DO UPDATE SET ... WHERE EXCLUDED.value > rows.value` for last-write-wins on a different field. The pattern at every external sink: choose the operation, document the idempotency, configure the sink correctly.

### A Non-Idempotent Operation Made Idempotent

Increment is the canonical non-idempotent operation. The pattern to make it idempotent is to wrap with a seen-set check and only increment on first sight.

```rust,ignore
use std::collections::HashSet;
use std::sync::Mutex;
use uuid::Uuid;

pub struct IdempotentCounter {
    count: Mutex<u64>,
    seen: Mutex<HashSet<Uuid>>,
}

impl IdempotentCounter {
    pub fn new() -> Self {
        Self {
            count: Mutex::new(0),
            seen: Mutex::new(HashSet::new()),
        }
    }

    /// Increment if the observation_id has not been seen before;
    /// no-op for duplicates. Returns the post-increment count.
    pub fn record_unique(&self, id: Uuid) -> u64 {
        let mut seen = self.seen.lock().unwrap();
        if !seen.insert(id) {
            // Already seen; return current count without incrementing.
            return *self.count.lock().unwrap();
        }
        let mut count = self.count.lock().unwrap();
        *count += 1;
        *count
    }

    pub fn count(&self) -> u64 {
        *self.count.lock().unwrap()
    }
}
```

The `record_unique` pattern is the standard wrapping. The `seen.insert(id)` returns `false` if the ID was already present, in which case the function returns early; otherwise it increments. The two locks are taken in a fixed order (seen before count) which prevents deadlock; in production code you would typically combine them into a single struct to make the locking simpler. The seen-set must be bounded (per the previous example) — an unbounded seen-set in a long-running counter eventually OOMs the process. The capstone's counter-style metrics use the bounded seen-set pattern at every sink that does increment-style operations; the pipeline as a whole is exactly-once-effective despite at-least-once delivery.

---

## Key Takeaways

* **Idempotency is a property of the operation, not the framework**. Every operator that writes to durable state must declare which operations are idempotent and on what key. The orchestrator's metadata carries the per-stage `idempotency_key_field`; the audit asserts it at startup.
* **Natural keys** come from the envelope (the SDA pipeline uses `observation_id` end-to-end). **Derived keys** are deterministic hashes of contributing inputs (sorted, content-addressable). Every operator preserves the natural key and computes derived keys reproducibly.
* The **bounded dedup set** uses time AND count for production safety. Double-bound is the pattern: time alone fails under bursts, count alone fails under slow streams. Window size is operationally determined by maximum re-delivery window plus margin.
* **Kafka's idempotent producer** (`enable.idempotence=true`) catches duplicates from producer retries at the broker level via PID + sequence numbers. Partition-scoped, sub-5% throughput cost. Default-on for production pipelines.
* The **at-least-once + idempotent = exactly-once-effective** composition holds within the pipeline and at idempotent boundaries (SQL UPSERT, idempotent producers, HTTP `Idempotency-Key` headers). At non-idempotent boundaries (alerts that trigger external actions), the boundary's owner must implement dedup; the pipeline's responsibility is to deliver the keys correctly and document the contract.

---

{{#quiz lesson-02-quiz.toml}}
