# Lesson 3 — Retries and Idempotency

**Module:** Data Pipelines — M02: Pipeline Orchestration Internals
**Position:** Lesson 3 of 4
**Source:** *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 7 (Reliable Data Delivery — Send Acknowledgments, Configuring Producer Retries, Idempotent Producer); *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (failure handling in stream processing)

---

## Context

Network calls in a streaming pipeline fail. The optical-archive REST endpoint goes down for thirty seconds during a partner deploy. The Kafka broker the alert sink is producing into has a leader election. The conjunction-emitter HTTP subscriber returns 503 because its database is being patched. None of these are "the pipeline is broken" — they are normal transient conditions in a system whose dependencies have their own operational lifecycle. The pipeline must keep running through them, which means it must retry.

Naive retries are themselves a source of incidents. A pipeline that retries every failure with no backoff turns a one-second downstream blip into a thundering-herd amplification of a hundred operator instances all reconnecting at once, each triggering more downstream work, each failing again. A pipeline that retries permanent errors (a 4xx, a deserialization failure, a schema-mismatch exception) loops forever on a poison pill. A pipeline that retries idempotent operations gets the right answer; one that retries non-idempotent operations produces duplicates whose downstream cost is sometimes invisible (a duplicate row in an analytics table) and sometimes catastrophic (a duplicate "fire thrusters" command to a satellite).

This lesson assembles three pieces of discipline. **What to retry** — distinguishing transient from permanent errors so retries help rather than hurt. **How to retry** — exponential backoff with jitter, so a hundred instances retrying after the same failure do not all retry at the same instant. **How to make retries safe** — idempotency, the property that lets at-least-once delivery (the default Kafka producer guarantee) compose into effective exactly-once processing (covered fully in Module 5; previewed here as the tooling we install now). The mission framing is concrete: an SDA-2026-NNNN incident two months ago saw a junior engineer add naive retries to the optical poller, which during a 30-second archive outage hammered the recovering archive with five thousand reconnect attempts per second and extended the outage to ninety minutes. The architectural fix from that postmortem is what this lesson encodes.

---

## Core Concepts

### Transient vs Permanent Errors

The first decision in a retry policy is whether a given error is a thing retrying might fix. **Transient** errors are conditions that are likely to resolve on their own within a useful timescale: timeouts, 5xx responses, connection-refused, broker-not-available, leader-election-in-progress. **Permanent** errors are conditions that will not resolve no matter how many times you ask: 4xx responses (the request is malformed), deserialization failures (the bytes are not what you expected), schema-mismatch errors (the producer and consumer disagree about the type), validation failures (the observation references a satellite that has been decommissioned). **Discardable** errors are a third category — invariant violations that should drop the event entirely without retry or DLQ, like a malformed UUID in a field that is required to be a UUID and is generated at the source.

The classification logic lives in the operator, not in the framework. A general retry wrapper that retries *every* error is the wrong shape. A wrapper that takes a `RetryDisposition::Retry | Permanent | Discard` and expects the operator to classify is the right shape. Lesson 4 introduces the dead-letter-queue as the destination for `Permanent` errors (not retried; not discarded; routed somewhere the engineer can examine them), so for now we focus on the retry path itself.

A useful default for unknown errors is `Permanent`. If you do not know whether retrying helps, assume it does not — better to surface the unknown error to the operational dashboard than to loop on a poison pill. Engineers add explicit `Retry` disposition for errors they have classified; everything else falls through to `Permanent` and gets attention.

### Exponential Backoff with Jitter

When a transient error occurs, retrying instantly is rarely right. The downstream that is failing is usually doing so because it is overloaded, recovering from a fault, or being deployed; an instant retry adds load to a system that needs the opposite. The basic shape of a sensible retry policy is **exponential backoff**: wait some initial delay, double it on each retry, cap at some maximum. The exponential growth means an outage of any duration eventually backs off to a low retry rate; the cap prevents runaway delays past the point where recovery is plausible.

Exponential backoff alone has a subtle problem at scale. A hundred operator instances all hit the same downstream failure at the same instant. They all back off the same delay. They all retry at the same instant. The downstream is still recovering, fails again, and the cycle repeats. The retry traffic looks like a square wave. The fix is **jitter**: a random component added to (or replacing) the deterministic delay. With jitter, the hundred instances retry at randomly distributed times within a window, smoothing the load.

The two jitter formulas worth knowing are *full jitter* and *decorrelated jitter*. **Full jitter**: `delay = random_uniform(0, backoff_cap)` — discard the deterministic backoff entirely; the cap is the only thing that matters. **Decorrelated jitter**: `delay = min(cap, random_uniform(initial, prev_delay * 3))` — keep the previous attempt's delay as the lower bound's anchor; the cap is the upper. Decorrelated jitter is the AWS Architecture blog's recommendation and the one we use here. It is more aggressive than full jitter (the median delay is higher) which produces less retry pressure on a slow-recovering downstream.

### Idempotency Keys

Retries can produce duplicates. A producer that retries after a partial failure may have actually succeeded on the original attempt, the success ack just got lost; the retry is a duplicate. A consumer that processes-then-commits may crash between the process and the commit; on restart it reads the message again and processes it twice. At-least-once delivery — the natural guarantee of any retry-capable system — admits duplicates by definition. To compose it into something stronger, we need the *operations* downstream of delivery to be **idempotent**: applying them twice with the same input produces the same result as applying them once.

Idempotency is a property of the operation, not the framework. Setting a record's value to a specific number is idempotent; incrementing a counter is not. Inserting a row keyed on `observation_id` with `ON CONFLICT DO NOTHING` is idempotent; a plain `INSERT` is not. A POST request with an `Idempotency-Key` header that the server respects is idempotent; the same POST without the header is not. The pipeline's operators must each be designed with the question "what does this operation do if I call it twice with the same input?" answered.

The natural idempotency key for SDA is the envelope's `observation_id` — a UUID generated at the source, carried through every stage, present on every observation. For derived events (a `ConjunctionRisk` produced by the correlator from two observations), the natural key is a hash of the inputs' observation_ids plus the window ID; deterministic, content-derived, identical across retries of the same input set. The tooling installed here will be reused in Module 5 when we discuss exactly-once delivery in depth.

### Where to Carry the Key

The envelope carries the key. The downstream sink uses the key for dedup. The middle operators do not need the key for their own correctness (a stateless map operator is idempotent regardless), but they must propagate it to the downstream. The key must not change as the envelope passes through stages — a stage that "enriches" the observation by attaching a catalog entry must not regenerate `observation_id`; it must preserve it. This is the rule that makes the rest of the system composable: the producer-side at-least-once guarantee plus the sink-side dedup, with the same key visible at both ends, gives the pipeline effective exactly-once.

External system boundaries also need the key. An HTTP request to a downstream service includes the `observation_id` as the `Idempotency-Key` header; the downstream service uses it to dedup retries server-side. A Kafka producer with `enable.idempotence=true` (the underlying mechanism is producer ID + sequence number, but the conceptual model is the same) ensures the broker drops duplicate messages. A database write uses `INSERT ... ON CONFLICT (observation_id) DO NOTHING`. The pattern is the same in every case: the key crosses the boundary, the downstream uses it.

### At-Least-Once + Idempotent = Effective Exactly-Once

This is the conceptual frame the rest of the track depends on. True exactly-once delivery — the network actually delivering each message exactly once — is impossible without either coordination (two-phase commit, transactional Kafka producers across topics) or strong assumptions about the network. Pragmatic exactly-once is achieved by combining two things that are individually achievable: **at-least-once delivery** at the transport layer (achievable with retries) and **idempotent processing** at the application layer (achievable with operation design). The two together produce a system in which every event is processed *as if* it were delivered exactly once, even though under the covers the transport layer may have delivered some events many times.

This frame is foundational for Module 5, where checkpointing, dead-letter queues, and exactly-once Kafka producers each get full treatment. Mention here so that every retry decision in this lesson is made with that downstream landscape in mind: we are not trying to avoid duplicates; we are trying to make sure duplicates are safe.

---

## Code Examples

### A Retry Wrapper with Decorrelated-Jitter Backoff

The wrapper takes a closure that performs the operation, plus a policy struct. It loops, dispatching on the operator's `RetryDisposition`. The backoff is computed per attempt with decorrelated jitter using the previous delay as the anchor.

```rust,ignore
use anyhow::Result;
use rand::Rng;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub initial: Duration,
    pub cap: Duration,
    pub max_attempts: u32,
}

#[derive(Debug)]
pub enum RetryDisposition<T> {
    /// Operation succeeded; return the value.
    Ok(T),
    /// Transient error; the wrapper should retry per the policy.
    Retry(anyhow::Error),
    /// Permanent error; the wrapper should not retry. Caller decides
    /// whether to DLQ (Lesson 4) or propagate.
    Permanent(anyhow::Error),
    /// Discard with no retry, no DLQ. The event is invariant-violating
    /// in a way that does not warrant operational attention.
    Discard,
}

/// Retry the given operation per the policy. Returns Ok on success,
/// Err on permanent failure or attempt-budget exhaustion.
pub async fn with_retry<T, F, Fut>(policy: RetryPolicy, mut op: F) -> Result<Option<T>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = RetryDisposition<T>>,
{
    let mut attempt: u32 = 0;
    let mut prev_delay = policy.initial;
    loop {
        attempt += 1;
        match op().await {
            RetryDisposition::Ok(v) => return Ok(Some(v)),
            RetryDisposition::Discard => return Ok(None),
            RetryDisposition::Permanent(e) => {
                return Err(e.context(format!("permanent failure on attempt {attempt}")));
            }
            RetryDisposition::Retry(e) if attempt >= policy.max_attempts => {
                return Err(e.context(format!(
                    "exhausted retry budget after {attempt} attempts"
                )));
            }
            RetryDisposition::Retry(_) => {
                // Decorrelated jitter: random_uniform(initial, prev_delay * 3),
                // capped at policy.cap. This produces a per-instance schedule
                // that is uncorrelated with other instances retrying the same
                // downstream — no thundering herd.
                let upper_bound = (prev_delay.as_millis() as u64).saturating_mul(3);
                let upper = upper_bound.max(policy.initial.as_millis() as u64);
                let delay_ms = rand::thread_rng()
                    .gen_range(policy.initial.as_millis() as u64..=upper);
                let delay = Duration::from_millis(delay_ms).min(policy.cap);
                prev_delay = delay;
                sleep(delay).await;
            }
        }
    }
}
```

Two things worth noting. The wrapper accepts a `FnMut() -> Future` rather than a single future — this matters because each retry needs to be a fresh operation. A future is single-use; calling `await` on it again is forbidden. The closure's job is to construct a fresh future on each invocation. The second point: the `Discard` arm returns `Ok(None)` rather than `Err(_)`. This distinguishes the "this event was an invariant violation we chose to drop" case from the "this operation failed" case at the type level. The caller can dispatch on the `Option` and update a `discards_total` metric without using errors as control flow.

### Classifying Errors at the Boundary

The HTTP-source-side operator is the canonical place where transient and permanent errors arrive interleaved. The classification logic looks at the response status code and the error variant; it returns the right `RetryDisposition` for the wrapper to act on.

```rust,ignore
use reqwest::{Client, Error as ReqwestError, StatusCode};

async fn poll_optical_archive(
    client: &Client,
    endpoint: &str,
    since: chrono::DateTime<chrono::Utc>,
) -> RetryDisposition<Vec<RawObservation>> {
    let resp = match client.get(endpoint).query(&[("since", since.to_rfc3339())]).send().await {
        Ok(r) => r,
        Err(e) if e.is_timeout() || e.is_connect() => {
            return RetryDisposition::Retry(e.into());
        }
        Err(e) => {
            // Other reqwest errors (URL parse, header mismatch) are
            // configuration bugs — permanent, not transient.
            return RetryDisposition::Permanent(e.into());
        }
    };

    match resp.status() {
        s if s.is_success() => match resp.json::<Vec<RawObservation>>().await {
            Ok(v) => RetryDisposition::Ok(v),
            Err(e) => {
                // Body is malformed JSON or has the wrong schema. Retrying
                // does not help; the next response will be the same shape.
                RetryDisposition::Permanent(e.into())
            }
        },
        s if s.is_server_error() => {
            // 500-class: transient. Retry.
            RetryDisposition::Retry(anyhow::anyhow!("optical archive {s}"))
        }
        StatusCode::TOO_MANY_REQUESTS => {
            // 429: explicitly transient, with the server asking us to slow.
            // Real code would honor a Retry-After header here.
            RetryDisposition::Retry(anyhow::anyhow!("optical archive 429"))
        }
        s if s.is_client_error() => {
            // 400-class: permanent. The request is malformed or unauthorized.
            // Retrying produces the same error.
            RetryDisposition::Permanent(anyhow::anyhow!("optical archive {s}"))
        }
        s => RetryDisposition::Permanent(anyhow::anyhow!("optical archive unexpected {s}")),
    }
}
```

The matching is exhaustive on the categories that matter — transient timeouts and connect failures, transient 5xx and 429, permanent 4xx, permanent everything-else. The `Permanent` for malformed JSON is a real consideration: if a partner's API change rolls out without coordination, the field they renamed produces a deserialization error on every request, and the pipeline starts pumping DLQ entries. That is the right behavior — alerting on DLQ growth is how we discover the partner's breaking change. Retrying the deserialization in a tight loop instead would mask the partner's bug and produce the same effect as a self-inflicted DDoS.

### Idempotent Sink-Side Write

The dedup sink keeps a sliding-window set of recently-seen observation IDs and writes downstream only on novel observations. This is the application-layer half of the at-least-once-plus-idempotent composition.

```rust,ignore
use anyhow::Result;
use std::collections::{BTreeSet, VecDeque};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use uuid::Uuid;

/// A sink that drops duplicate observations within a rolling window.
/// The window's capacity bounds memory; observations seen outside the
/// window may be re-emitted (acceptable for the SDA pipeline's downstream
/// alert subscriber, which itself is idempotent on alert ID).
pub struct DedupSink {
    seen: BTreeSet<Uuid>,
    /// Insertion order, used for FIFO eviction.
    order: VecDeque<(Uuid, SystemTime)>,
    window: Duration,
    capacity: usize,
    downstream: mpsc::Sender<Observation>,
}

impl DedupSink {
    pub fn new(window: Duration, capacity: usize, downstream: mpsc::Sender<Observation>) -> Self {
        Self { seen: BTreeSet::new(), order: VecDeque::new(), window, capacity, downstream }
    }

    pub async fn write(&mut self, obs: Observation) -> Result<()> {
        let now = SystemTime::now();
        // Evict expired entries first.
        while let Some(&(id, ts)) = self.order.front() {
            if now.duration_since(ts).unwrap_or_default() > self.window
                || self.order.len() >= self.capacity
            {
                self.order.pop_front();
                self.seen.remove(&id);
            } else {
                break;
            }
        }
        if self.seen.contains(&obs.observation_id) {
            // Duplicate; drop silently. (Production: increment a metric.)
            return Ok(());
        }
        self.seen.insert(obs.observation_id);
        self.order.push_back((obs.observation_id, now));
        self.downstream.send(obs).await
            .map_err(|_| anyhow::anyhow!("dedup sink downstream dropped"))
    }
}
```

The dedup window is bounded both by time and by count — the size cap is the safety valve in case the time-based eviction gets behind during a burst. A real production sink would also persist the seen set across restarts (via a small embedded store) so that a process restart does not produce a duplicate-observation surge while the seen set rebuilds; Module 5's checkpointing lesson supplies that machinery. Until then, a process restart causes a one-window worth of potential duplicates downstream — acceptable for SDA's alert subscriber, which has its own idempotency on alert ID, and worth flagging as a deliberate cost. The pattern at every layer is the same: choose a key, choose a bound, dedup against the bound.

---

## Key Takeaways

* Errors are either **transient** (retry helps), **permanent** (retry makes it worse), or **discardable** (drop without operational attention). The classification is the operator's responsibility, not the framework's. Default unknown errors to permanent; that surfaces them rather than masking them.
* **Exponential backoff with jitter** is the right retry shape. Decorrelated jitter (random_uniform(initial, prev_delay * 3), capped) keeps a hundred operator instances from synchronizing their retries during a downstream outage. Naive fixed-delay retries amplify outages into self-inflicted DDoS events.
* **Idempotency is a property of the operation**, not of the framework. The pipeline composes at-least-once delivery (achievable with retries) plus idempotent operations (achievable by design) into effective exactly-once processing. This frame is the tooling that Module 5 will fully develop.
* The **idempotency key** is the envelope's `observation_id` for SDA, propagated unchanged through every stage. External boundaries — Kafka producers, HTTP requests, database writes — each have their own way of consuming the key (idempotent producer, `Idempotency-Key` header, `ON CONFLICT DO NOTHING`); the pattern is the same at every boundary.
* The **dedup sink** is the canonical exactly-once-effective endpoint: a sliding-window set keyed on observation_id, bounded by both time and count, with documented behavior on cold start and known cost on duplicate-burst conditions.

---

{{#quiz lesson-03-quiz.toml}}
