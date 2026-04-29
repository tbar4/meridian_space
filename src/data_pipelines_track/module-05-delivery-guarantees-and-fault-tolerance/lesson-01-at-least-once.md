# Lesson 1 — At-Least-Once Delivery

**Module:** Data Pipelines — M05: Delivery Guarantees and Fault Tolerance
**Position:** Lesson 1 of 4
**Source:** *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 7 (Reliable Data Delivery — `acks`, `enable.idempotence`, retries, in-flight requests, consumer commit ordering); *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (Fault tolerance in stream processing)

---

## Context

Modules 1 through 4 produced a pipeline that handles steady-state load, propagates backpressure cleanly, computes correct event-time results, and survives most operational failures. It has one structural blind spot: it loses data on a process restart. The windowed correlator from Module 3 holds in-process state. The orchestrator's supervisor restarts a panicked operator with a fresh Task, which means a fresh empty state. In-flight observations between the source and the correlator are buffered in tokio channels that do not survive a process exit. When a deploy rolls a new pipeline binary, the previous binary's in-flight buffer is gone and the windows it had been accumulating are gone.

The mission framing for this module is the SDA-2026-0207 incident two months ago. A maintenance window required restarting the pipeline to apply a security patch. The orchestrator's graceful-drain logic worked correctly — every operator drained its incoming channel before exiting — but the alert subscriber had already received fourteen alerts that the new pipeline did not know about, and the new pipeline emitted six alerts that the subscriber had already acted on. Two false-positive collision-avoidance maneuvers were executed as a consequence. The postmortem identified two missing pieces: durable state on the producer side (so restart resumes from where the previous instance left off), and idempotent processing on the consumer side (so duplicate deliveries do not produce duplicate effects).

This module installs both pieces. **Lesson 1** establishes the delivery-semantics vocabulary — the three levels of guarantee (at-most-once, at-least-once, exactly-once) — and the producer-and-consumer-side configuration choices that produce at-least-once. **Lesson 2** develops idempotency as the application-layer property that composes with at-least-once delivery to produce effective exactly-once processing. **Lesson 3** introduces checkpointing — the durable state mechanism that lets a restarted operator resume without losing window state. **Lesson 4** covers dead-letter queues for events that cannot be processed regardless of how many times they are retried. The capstone wires all four into the M4 pipeline; by the end of M5, the alert pipeline is correct under load AND across restarts.

---

## Core Concepts

### The Three Levels

Every message-delivery system makes one of three guarantees about a given message.

**At-most-once.** The message is delivered zero or one times. Loss is possible (the message can be dropped); duplication is not. The simplest configuration: send and forget. UDP without retries falls in this category.

**At-least-once.** The message is delivered one or more times. Loss is not possible (every message reaches the consumer at least once); duplication is possible (the same message can be delivered multiple times under retry). The default for any system that retries on failure. Kafka's standard producer/consumer pair, configured with `acks=all` and `retries`, falls in this category.

**Exactly-once.** The message is delivered exactly one time. No loss, no duplication. The strongest guarantee, and the most expensive — true exactly-once delivery is impossible without coordination between producer and consumer (two-phase commit, transactional Kafka, or strong assumptions about the network). Most systems labeled "exactly-once" are actually "at-least-once delivery + idempotent processing = effective exactly-once at the application layer."

The choice is operational and determined by what the consumer's failure mode looks like under each. A throughput counter is fine with at-most-once — losing 0.1% of events does not change the per-minute aggregate. An audit log requires at-least-once — every observation must be recorded, even at the cost of duplicates that the audit reader can dedupe. A conjunction alert that triggers a real-world action requires effective exactly-once — neither a missed alert (collision risk) nor a phantom alert (unnecessary maneuver) is acceptable.

### At-Least-Once on the Producer Side

The Kafka producer's reliability is configured by three settings working together.

**`acks`.** Controls when the producer considers a send "successful." `acks=0` means "send and assume success" (at-most-once: a network drop is silently lost). `acks=1` means "wait for the partition leader to acknowledge" (still loses if the leader dies before replicating). `acks=all` means "wait for the full in-sync replica set to acknowledge" (durable on the broker side; the message will not be lost barring catastrophic broker failures). For at-least-once, `acks=all` is required.

**`retries`.** How many times the producer retries a failed send. Failures here are transport-level: network errors, broker timeouts, leader-elections-in-progress. With `retries=0` and `acks=all` you get at-most-once with strong durability when delivery succeeds; with `retries > 0` you get at-least-once. Production setting is typically a high number (`Integer.MAX_VALUE` is common — the producer retries until either success or `delivery.timeout.ms` elapses). For at-least-once, retries must be enabled.

**`max.in.flight.requests.per.connection`.** How many sends the producer can have outstanding to a given broker at once. With idempotence disabled, values > 1 risk re-ordering on retry: send A is in flight, send B is in flight; A fails and is retried; B succeeds before A's retry; the broker sees B-then-A. For ordered at-least-once, set this to 1 (degenerate single-credit case from Module 4 L2). For unordered at-least-once, higher values give more throughput at the cost of order.

The combination `acks=all` + `retries > 0` produces at-least-once. Duplicates are possible because the producer might retry a send that actually succeeded but whose acknowledgment was lost in transit; the broker sees the same message twice. The application must tolerate that — the lesson's title is the guarantee, not "exactly-once."

### At-Least-Once on the Consumer Side

The consumer's reliability is about the ordering of *processing* and *committing*. The consumer reads a batch of messages, processes them, then commits the offset back to the broker. If the consumer crashes between reading and processing, the next consumer instance starts at the previously-committed offset and re-reads (and re-processes) the unprocessed batch — duplicate processing, but no loss.

The critical ordering is **process-then-commit**. The consumer must finish processing a message before committing its offset. The wrong ordering (commit-then-process) loses messages: if the consumer commits and then crashes before processing, the next instance starts at the committed offset and never re-reads the messages that the first instance had committed but not yet processed. The messages are silently lost.

The Kafka consumer's `enable.auto.commit=true` configuration is the canonical version of the wrong ordering. Auto-commit fires on a timer, regardless of whether the application has actually processed the messages whose offsets it commits. For any reliability discipline beyond the loosest, `enable.auto.commit=false` and explicit `commitSync` after processing is required.

The duplicate-on-restart property is acceptable because the application is idempotent on `observation_id` (the topic of Lesson 2). Messages re-read after a restart go through the dedup logic and are silently dropped. The combination of producer-side at-least-once + consumer-side process-then-commit + sink-side idempotency is the effective-exactly-once shape this module is building toward.

### Where Duplicates Come From

Three sources of duplication under at-least-once.

**Producer-side retries after partial success.** The producer sends; the broker writes the message to its log; the broker's acknowledgment is lost in transit; the producer's retry timer fires; the producer sends again; the broker writes the message again. The same logical message is now in the log twice with different offsets. Consumers see both copies. This is the duplicate that the producer-side idempotent producer (`enable.idempotence=true`, Lesson 2) is designed to prevent — using a producer ID + sequence number that the broker uses to dedup.

**Consumer-side crashes between process and commit.** The consumer processes a batch; the consumer crashes before committing; the next consumer instance reads the same batch and processes it again. The application's effect on the world has been applied twice. Idempotency on the application's effect (e.g., UPSERT-by-natural-key in the sink) is what absorbs this.

**Rebalances during processing.** A consumer group rebalance reassigns partitions among consumer instances. If a partition is reassigned mid-batch (the original consumer was processing but had not committed when the rebalance fired), the new consumer reads from the previously-committed offset and re-processes. Same shape as the crash case.

The lesson's framing: duplicates are not a bug, they are a *configurable cost*. Rare under good operational conditions, frequent during deploys or partition migrations, always possible. The application is responsible for handling them.

### Operational Cost of Duplicates

The cost is proportional to the duplicate rate × the per-event cost of duplicate processing downstream.

For a sink that does an UPSERT keyed on `observation_id` with a strict-greater check (the M3 L4 retract-aware shape): the duplicate is absorbed by the comparison, the cost is one wasted SQL round-trip per duplicate. At a typical duplicate rate of 0.1% during a healthy steady-state, this is invisible at SDA's scale.

For a sink that increments a counter without an idempotency check: every duplicate is a miscount. The counter ends up high by the duplicate rate × window count. The audit dashboard reports inflated numbers. This is the canonical "we have at-least-once + non-idempotent sink" failure mode and the reason Module 2 L3 made idempotency a first-class topic.

For a sink that triggers an external action (an alert subscriber that fires a satellite-avoidance maneuver): every duplicate is a real-world wrong action. The cost is real fuel burn or a real hardware adjustment. This is the case the SDA-2026-0207 incident reflected, and the case where exactly-once-effective via idempotency on alert ID is necessary, not optional.

The cost shape determines how aggressively the application tightens the at-least-once bound (idempotent producer, smaller in-flight, faster commit cadence) and how robust the sink's idempotency must be.

---

## Code Examples

### A Kafka Producer Configured for At-Least-Once

The `rdkafka` crate's producer configuration. The settings encode the lesson's at-least-once shape exactly.

```rust,ignore
use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub fn build_at_least_once_producer(brokers: &str) -> Result<FutureProducer> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        // acks=all: wait for the full in-sync replica set to ack.
        // The message is durable on the broker side before send returns.
        .set("acks", "all")
        // retries: keep retrying transient transport failures.
        // i32::MAX is the conventional "retry until delivery.timeout.ms".
        .set("retries", "2147483647")
        // delivery.timeout.ms: how long the producer keeps retrying
        // before giving up. 2 minutes is reasonable for the SDA pipeline;
        // longer encourages the producer to ride out longer broker
        // transient failures.
        .set("delivery.timeout.ms", "120000")
        // enable.idempotence is OFF deliberately for this lesson — we
        // want raw at-least-once. Lesson 2 turns it on for the
        // exactly-once-effective producer.
        .set("enable.idempotence", "false")
        // max.in.flight.requests: 1 forces the strongest ordering
        // guarantee at the cost of throughput. Production might use
        // 5 (the broker-enforced max for idempotent mode) when ordering
        // can be reconstructed via observation_id at the consumer.
        .set("max.in.flight.requests.per.connection", "1")
        .create()?;
    Ok(producer)
}

pub async fn send_observation(
    producer: &FutureProducer,
    topic: &str,
    obs: &Observation,
) -> Result<()> {
    let payload = serde_json::to_vec(obs)?;
    let key = obs.observation_id.to_string();
    let record = FutureRecord::to(topic)
        .key(&key)
        .payload(&payload);

    // The future resolves when the full in-sync replica set has acked.
    // On any broker-acknowledged-but-network-lost case, the producer
    // retries automatically per the configuration above; the resolution
    // happens on the eventual successful retry.
    producer.send(record, Duration::from_secs(30)).await
        .map_err(|(e, _)| anyhow::anyhow!("send failed after retries: {e}"))?;
    Ok(())
}
```

The `enable.idempotence=false` is deliberate here — we are demonstrating raw at-least-once. The producer's retry-on-failure is what gives the at-least guarantee; duplicates are the cost. Lesson 2 turns idempotence on and explains the producer-ID + sequence-number mechanism that the broker uses to dedup at the producer side. This lesson's pipeline accepts the producer-side duplicates and absorbs them at the consumer-side sink.

### A Process-Then-Commit Consumer

The Kafka consumer pattern that gives at-least-once on the consumer side. `enable.auto.commit=false`, explicit `commit_message` after the batch is processed.

```rust,ignore
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Message};

pub fn build_at_least_once_consumer(brokers: &str, group: &str) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        // The critical setting: commits must be explicit, not auto.
        // Auto-commit fires on a timer regardless of processing
        // progress, which is the canonical 'commit-then-process'
        // bug shape.
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;
    Ok(consumer)
}

pub async fn run_consumer_loop(
    consumer: &StreamConsumer,
    sink: &impl ObservationSink,
) -> Result<()> {
    use rdkafka::consumer::CommitMode;
    use tokio_stream::StreamExt;

    let mut stream = consumer.stream();
    while let Some(result) = stream.next().await {
        let message: BorrowedMessage = result?;
        let payload = message.payload()
            .ok_or_else(|| anyhow::anyhow!("empty payload"))?;
        let obs: Observation = serde_json::from_slice(payload)?;

        // PROCESS first: feed the sink. The sink's idempotency
        // (Lesson 2) absorbs duplicates from rare retries.
        sink.write(obs).await?;

        // COMMIT only after process succeeds. A crash between process
        // and commit causes the next consumer instance to re-read
        // and re-process; the sink dedups.
        consumer.commit_message(&message, CommitMode::Sync)?;
    }
    Ok(())
}
```

The `commit_message(..., CommitMode::Sync)` blocks until the broker confirms the commit — the next message is not read until the previous commit is durable. The async variant (`CommitMode::Async`) commits in the background, which is faster but introduces a window where a crash between the async commit's queue and its broker confirmation can lose the commit; for SDA's reliability budget we use sync commits. The duplicate window is exactly the time between `sink.write` returning and `commit_message` returning — typically sub-millisecond.

### A Crash-Injection Test Harness

The test harness that verifies the at-least-once guarantee under deliberate crash conditions. The same harness is used in Lesson 2 to verify exactly-once-effective when idempotency is added.

```rust,ignore
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// A test-only sink that counts writes and panics on the Nth call.
pub struct CrashingSink {
    writes: Arc<AtomicU32>,
    panic_at: u32,
}

impl CrashingSink {
    pub fn new(panic_at: u32) -> Self {
        Self {
            writes: Arc::new(AtomicU32::new(0)),
            panic_at,
        }
    }

    pub fn writes(&self) -> u32 { self.writes.load(Ordering::SeqCst) }
}

#[async_trait::async_trait]
impl ObservationSink for CrashingSink {
    async fn write(&self, _obs: Observation) -> Result<()> {
        let n = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.panic_at {
            // Simulate process crash mid-write. In a real test this
            // would be a tokio::process restart or similar; for
            // illustration, a panic captured by the orchestrator.
            panic!("simulated crash at write #{n}");
        }
        Ok(())
    }
}

#[tokio::test]
async fn crash_between_process_and_commit_replays_messages() {
    // Setup: Kafka producer feeds 10 observations to topic. Consumer
    // processes them with the CrashingSink that panics on write #5.
    let producer = build_at_least_once_producer("localhost:9092").unwrap();
    for i in 0..10 {
        send_observation(&producer, "test-topic", &test_obs(i)).await.unwrap();
    }

    // First consumer instance: panics at write 5. Writes 1-4 succeeded
    // and were committed (commit happens after each process); write 5
    // panicked before commit.
    let sink_a = CrashingSink::new(5);
    let _ = run_consumer_with_sink("test-group", &sink_a).await;
    assert_eq!(sink_a.writes(), 5); // 4 succeeded + the panicked one

    // Second consumer instance: starts from offset 4 (last committed).
    // Re-reads message 5 and processes it, plus 6-10. Total writes
    // for the system: 5 (from instance A) + 6 (from instance B) = 11.
    // The duplicate is the M5's at-least-once cost.
    let sink_b = CrashingSink::new(0); // does not crash this time
    let _ = run_consumer_with_sink("test-group", &sink_b).await;
    assert_eq!(sink_b.writes(), 6);  // 5 through 10
}
```

The test asserts the at-least-once contract: every observation reaches a sink at least once, and message 5 reaches a sink exactly twice. With idempotent processing in Lesson 2, the duplicate's *effect* on the world is unchanged from a single processing — but the raw write count at the sink is still 11. The lesson's framing: at-least-once delivery is the transport guarantee; idempotency at the application layer is what turns it into exactly-once-effective.

---

## Key Takeaways

* The **three levels** of delivery guarantee are at-most-once, at-least-once, and exactly-once. True exactly-once delivery requires coordination beyond what most systems implement; pragmatic exactly-once is achieved by composing at-least-once delivery with idempotent processing at the application layer.
* **At-least-once on the producer side** requires `acks=all` (wait for the full in-sync replica set), `retries > 0` (retry transient failures until delivery.timeout.ms elapses), and a deliberate choice on `max.in.flight.requests.per.connection` (1 for strict ordering; higher for throughput at the cost of order under retry).
* **At-least-once on the consumer side** requires **process-then-commit** ordering with `enable.auto.commit=false`. Commit-then-process loses messages on a crash between commit and process. Auto-commit's timer-based behavior is the canonical version of the wrong ordering.
* **Three sources of duplicates** under at-least-once: producer retries after partial success, consumer crashes between process and commit, rebalances during processing. All three are absorbed by sink-side idempotency (Lesson 2's topic).
* **The cost of duplicates** is proportional to duplicate rate × per-event cost of duplicate processing downstream. For UPSERT-by-natural-key sinks the cost is one wasted SQL round-trip; for counter-style sinks it is silent miscounting; for sinks that trigger external actions it is real-world wrong actions. The cost determines how tightly the at-least-once bound is held.

---

{{#quiz lesson-01-quiz.toml}}
