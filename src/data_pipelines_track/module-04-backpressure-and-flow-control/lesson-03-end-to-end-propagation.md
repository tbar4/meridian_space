# Lesson 3 — End-to-End Backpressure Propagation

**Module:** Data Pipelines — M04: Backpressure and Flow Control
**Position:** Lesson 3 of 3
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (Buffering and Pushback in stream processing); *Network Programming with Rust* — Abhishek Chanda, sections on TCP windowing as transport-level backpressure; *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 3 (Producer `max.block.ms` and the producer-side buffer)

---

## Context

Module 1 introduced the bounded-channel-plus-await chain that propagates backpressure from sink to source. Module 2 wrapped that chain in an orchestrator. Lesson 1 of this module established per-edge `FlowPolicy`. Lesson 2 added credit-based flow as a sharper tool for cases where the bounded-channel pattern is not enough. The pipeline at this point is well-equipped to apply backpressure correctly *if* the chain is intact end-to-end.

The chain is rarely intact end-to-end. Engineers add small "convenience" pieces — a `tokio::spawn` for "fire-and-forget" logging, an `unbounded_channel` for "this side channel cannot fill," a `Vec::push` into an in-process collection — each one a place where the backpressure traversal stops. The post-Cosmos-1408 incident from this module's mission framing took two hours to diagnose because the pressure chain was structurally broken at a single `tokio::spawn` inside the correlator's per-event loop. The spawn looked harmless in code review, did not block, did not fill any visible buffer, and silently amplified any downstream slowdown into unbounded task accumulation. The fix was three lines; the diagnosis was the hard part.

This lesson is the audit. It identifies the canonical patterns that break the pressure chain, develops the diagnostic approach (read the channel-occupancy gradient — the slowest stage shows up as the channel-full upstream of itself), and discusses the two boundary cases where backpressure does not propagate naturally: across a Kafka topic between two pipeline halves, and through retry/loop topologies. The capstone integrates the audit as a CI test against the operator graph and a `BurstSimulator` that drives the M3 pipeline at 10x normal rate to verify end-to-end propagation under burst load.

---

## Core Concepts

### The Pressure Chain

A single contiguous chain of bounded buffers from source to sink. Every adjacent pair of operators connected by a bounded channel; every operator's emit using `send().await` (or its `FlowPolicy` equivalent) on its outgoing channel; no detached tasks per item; no in-process unbounded buffers. With those conditions, a slowdown at the sink propagates: the sink's incoming channel fills, the upstream operator's `send().await` suspends, that upstream's incoming channel fills, that upstream's upstream suspends, all the way back to the source. The source either suspends on its own producing primitive (the UDP `recv_from`, the HTTP `client.get`) or, for sources that cannot suspend (a UDP feed that produces whether anyone is listening or not), the kernel-level buffer fills and the kernel drops at its layer.

The chain has a shape: **operators are the links, channels are the connections**. Breaking the chain means inserting something between two operators that does not propagate the suspend signal. The next subsection enumerates the canonical breakage shapes.

### Where Pressure Breaks: Anti-Patterns

Five patterns the lesson identifies as the recurring pressure-chain breakers.

**`tokio::spawn` per-event.** An operator's hot loop does `tokio::spawn(async move { handle_event(e).await })`. The spawn returns immediately; the operator continues processing the next event without waiting for the spawned task. Under steady-state load the spawned tasks complete fast enough that the count stays bounded. Under any sustained slowdown, spawned tasks accumulate without limit. The operator's outgoing channel never fills (because the spawned tasks do the work asynchronously) and never propagates pressure. This is the M1 lesson 3 anti-pattern, and it is the single most common pressure-chain breaker because it looks harmless in code review.

**`mpsc::unbounded_channel`.** Lesson 1's footgun. Any unbounded buffer is a pressure-chain stopgap: the upstream's `send` always succeeds, so the upstream never observes the downstream's slowness. The buffer grows in proportion to the sustained gap.

**Fire-and-forget logging via channels.** A common pattern: emit a structured log via an `mpsc::Sender` to a separate logging task. If that channel is unbounded (or if the operator uses `try_send` and discards the `Err`), logging events can pile up under load without anyone noticing. The fix is not "make logging block on the hot path" — that has its own problems — but rather: log via the standard `tracing` crate's blocking machinery (which is fast), or use `try_send` + counter pattern from Lesson 1.

**`Vec::push` into ever-growing collections.** An operator accumulates events into a `Vec` for a deferred batch operation. The accumulation has no bound; under load, the `Vec` grows without limit. The pattern is structurally identical to `unbounded_channel` and has the same fix: the operator must bound the accumulation by size or time, and apply backpressure or load-shed when the bound is reached.

**Drop-and-recreate task patterns.** A supervisor that, on every event, drops the current operator task and spawns a fresh one. The motivation is usually "stateless restart for cleanliness," but the effect is that the channel between this operator and its downstream is being reconstructed faster than it can drain — the new task starts with an empty channel, the old task's in-flight items are dropped or orphaned, pressure does not propagate because the channel does not persist.

The canonical fix in every case is structural: replace the breaking pattern with bounded-and-suspending equivalents. The `tokio::spawn` per-event becomes an inline `await`. The `unbounded_channel` becomes `channel(N)`. The `Vec::push` accumulator becomes a sized `VecDeque` with explicit eviction or backpressure. The drop-and-recreate becomes a long-lived supervised operator (Module 2 L4).

### Reading the Pressure Gradient

When a pipeline is correctly chained but slow somewhere, the channel-occupancy gradient identifies the slow stage. **The slowest operator's incoming channel is consistently full**. Upstream of that operator, channels are partially filled (the stages between the source and the slow operator are running at the pipeline's bottleneck rate, with channels carrying some slack). Downstream of the slow operator, channels are mostly empty (the downstream is faster than the upstream is producing).

The pattern looks like a step function in the per-channel occupancy gauges: 100% at and just before the bottleneck, decreasing toward 0% as you move downstream. The ops engineer reading the dashboard finds the bottleneck by looking for the rightmost-100%-channel — the one whose upstream is the slowest stage and whose downstream is what is starving.

The lesson develops the audit as a diagnostic operator that exports per-channel occupancy as a Prometheus gauge. Module 6 generalizes this into the operational dashboard's primary panel; this lesson installs the foundation.

### TCP Windowing as Transport-Level Backpressure

Module 1 introduced TCP windowing as the kernel-level mechanism that propagates backpressure from a slow application back to the producer over the network. The receiver's TCP stack advertises a *receive window* — how many more bytes it can buffer; as the application reads, the window grows; when the application stops reading, the window shrinks. The sender's TCP stack respects the advertised window and pauses sending when the window is zero.

This works only if the application reads synchronously from the socket and processes each read before reading the next one. An application that reads as fast as possible into an in-process buffer and processes asynchronously breaks TCP-level backpressure: the application drains the kernel buffer as fast as the network can fill it, the receive window stays advertised at maximum, and the sender does not slow regardless of how slow the application's processing is. The backpressure chain ends at the in-process buffer, which is unbounded by definition.

The discipline is to drive the read loop and the channel send from the same task. The radar source from Module 1 does this: `recv_from().await` followed by `sink.write(obs).await`. When the sink's downstream channel is full, `send().await` suspends, the next iteration of the loop is delayed, the next `recv_from` is delayed, the kernel buffer fills, the TCP receive window shrinks, the sender's TCP stack slows. Every link in the chain — application→channel→kernel→network — propagates the pressure. Breaking any one link breaks the whole.

### Backpressure Across Kafka

The pipeline often has a Kafka topic between two halves: the ingestion half writes to a topic, a consumer half reads from it. Backpressure across the topic boundary does not work the same way as within a single process.

The producer's view: a Kafka producer maintains a *producer-side buffer* (`buffer.memory`, default 32 MB). When the topic's brokers acknowledge slowly (or the consumer is slow and the topic's retention is bursting), the producer-side buffer fills. With `max.block.ms` configured (default 60 seconds), the producer's `send` blocks waiting for buffer space — which propagates backpressure into the producer-side application. With `max.block.ms = 0` and `acks=0`, the producer drops on full buffer. The producer-side configuration determines the boundary behavior.

The consumer's view: the consumer's lag (the gap between the topic's high-watermark and the consumer's committed offset) grows when the consumer is slow. Backpressure does not propagate to the producer instantaneously — Kafka decouples the two halves intentionally. The producer keeps writing (up to its broker's retention limits) regardless of consumer lag; the consumer falls behind silently until lag is observed via metrics. The pipeline operator's responsibility is to **monitor consumer lag explicitly** and alert when it grows past a threshold. The implicit pressure-chain that works within a single process becomes an explicit observability discipline at the Kafka boundary.

For the SDA pipeline, this is mostly future work — Module 5 introduces Kafka as a checkpoint persistence layer and Module 6 develops the lag monitoring discipline. This lesson surfaces the boundary so the audit script does not flag Kafka producer/consumer pairs as pressure-chain breaks (they ARE breaks within a single process; they ARE intended at the cross-pipeline boundary; the monitoring is what restores the missing signal).

---

## Code Examples

### A Pressure-Chain Audit Script

The audit walks the operator graph from M2 and flags edges that are unbounded, operators that have detached `tokio::spawn` calls in their hot path (a heuristic check against the source code), and channels that lack a documented `FlowPolicy`. Failing edges produce a CI error.

```rust,ignore
use anyhow::{anyhow, Result};

#[derive(Debug)]
pub struct AuditFinding {
    pub edge_or_operator: String,
    pub category: AuditCategory,
    pub detail: String,
}

#[derive(Debug)]
pub enum AuditCategory {
    UnboundedChannel,
    NoFlowPolicy,
    DetachedSpawnSuspected,  // heuristic: source-grep for tokio::spawn inside operator
}

/// Audit an OperatorGraph for backpressure-chain integrity. Returns
/// the list of findings; an empty list means the audit passed.
pub fn audit(graph: &OperatorGraph) -> Vec<AuditFinding> {
    let mut findings = Vec::new();

    for edge in graph.edges() {
        if !edge.is_bounded() {
            findings.push(AuditFinding {
                edge_or_operator: format!("{} -> {}", edge.from_name, edge.to_name),
                category: AuditCategory::UnboundedChannel,
                detail: "edge uses unbounded_channel; pressure does not propagate".into(),
            });
        }
        if edge.flow_policy().is_none() {
            findings.push(AuditFinding {
                edge_or_operator: format!("{} -> {}", edge.from_name, edge.to_name),
                category: AuditCategory::NoFlowPolicy,
                detail: "edge has no documented FlowPolicy; default of Backpressure assumed but should be explicit".into(),
            });
        }
    }

    findings
}

/// CI helper: convert findings into a Result that fails the test.
pub fn audit_or_fail(graph: &OperatorGraph) -> Result<()> {
    let findings = audit(graph);
    if findings.is_empty() { return Ok(()); }
    let summary: Vec<String> = findings
        .iter()
        .map(|f| format!("[{:?}] {}: {}", f.category, f.edge_or_operator, f.detail))
        .collect();
    Err(anyhow!("backpressure audit failed:\n{}", summary.join("\n")))
}
```

The audit is intentionally conservative: an unflagged graph is *probably* correct but the audit cannot prove it. The `DetachedSpawnSuspected` heuristic is the weakest part — a source-grep for `tokio::spawn` inside operator bodies catches the obvious cases but misses cases where the spawn is hidden inside a helper function. Production audit tooling extends to AST-level inspection or annotation-based pattern matching; the lesson's version is sufficient as a CI canary that catches the regressions the postmortem identified.

### A Per-Channel Occupancy Gauge

The diagnostic operator that exports the channel-occupancy gradient. Drops in transparently between any two operators by wrapping the channel.

```rust,ignore
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;

/// A wrapper around mpsc::Sender that exports the channel's current
/// occupancy as a metric on every send. Used between operators where
/// occupancy needs to be observable for the pressure-gradient diagnostic.
pub struct InstrumentedSender<T> {
    inner: mpsc::Sender<T>,
    capacity: usize,
    edge_label: String,
}

impl<T> InstrumentedSender<T> {
    pub fn new(inner: mpsc::Sender<T>, capacity: usize, edge_label: impl Into<String>) -> Self {
        Self { inner, capacity, edge_label: edge_label.into() }
    }

    pub async fn send(&self, item: T) -> Result<()> {
        // Exporting occupancy as a Prometheus gauge labeled by edge.
        // The dashboard's primary panel filters this by edge_label
        // and shows the per-edge gradient.
        let used = self.capacity - self.inner.capacity();
        metrics::gauge!("channel_occupancy", "edge" => self.edge_label.clone())
            .set(used as f64);
        self.inner.send(item).await
            .map_err(|_| anyhow::anyhow!("downstream dropped"))
    }
}
```

The `mpsc::Sender::capacity()` method returns the *remaining* capacity (slots free), so used = total - remaining. The gauge update is per-send overhead; for SDA's volumes (tens of thousands per second), the cost is negligible — sub-microsecond per send. For higher-throughput pipelines the sample rate would be lower (every Nth send) at the cost of dashboard responsiveness. Module 6 generalizes this pattern into a structured metric for every operator-pair edge.

### A `BurstSimulator` for End-to-End Pressure Verification

The integration test that drives a synthetic 10x burst through the pipeline and asserts the per-channel occupancy gradient stabilizes at the expected bottleneck. The simulator's value is not the simulation itself but the assertion structure: under burst load, the slowest operator's incoming channel should be persistently full, every other channel should be measurably below full.

```rust,ignore
use std::time::{Duration, Instant};

pub struct BurstSimulator {
    target_rate_per_s: u64,
    duration: Duration,
}

impl BurstSimulator {
    pub fn new(target_rate_per_s: u64, duration: Duration) -> Self {
        Self { target_rate_per_s, duration }
    }

    /// Drive `target_rate_per_s` synthetic observations into the
    /// pipeline's source for `duration`, sampling channel occupancy
    /// at 10 Hz. Returns the per-edge occupancy time series.
    pub async fn drive(&self, source: impl SyntheticSource) -> Result<OccupancyReport> {
        let start = Instant::now();
        let interval = Duration::from_millis(1000 / self.target_rate_per_s.max(1));
        let mut sample_at = start + Duration::from_millis(100);
        let mut samples: Vec<OccupancySample> = Vec::new();

        while start.elapsed() < self.duration {
            source.emit_observation().await?;
            tokio::time::sleep(interval).await;
            if Instant::now() >= sample_at {
                samples.push(sample_all_edges());
                sample_at = Instant::now() + Duration::from_millis(100);
            }
        }
        Ok(OccupancyReport { samples })
    }
}

#[derive(Debug)]
pub struct OccupancyReport {
    pub samples: Vec<OccupancySample>,
}

impl OccupancyReport {
    /// Identify the persistently-full edge (>= 95% occupancy in the
    /// final third of the simulation). That edge's downstream operator
    /// is the bottleneck.
    pub fn identify_bottleneck(&self) -> Option<String> {
        let final_third_start = self.samples.len() * 2 / 3;
        let final_samples = &self.samples[final_third_start..];
        for edge_name in self.edge_names() {
            let avg_occupancy: f64 = final_samples.iter()
                .map(|s| s.edge_occupancy(&edge_name))
                .sum::<f64>() / final_samples.len() as f64;
            if avg_occupancy >= 0.95 {
                return Some(edge_name);
            }
        }
        None
    }

    fn edge_names(&self) -> Vec<String> { /* ... */ vec![] }
}
```

The simulator is more useful as a CI canary than as a load-testing tool — its value is the bottleneck-identification assertion, not the absolute throughput numbers. A regression that pushes the bottleneck from where ops expects it to be (the correlator) to somewhere else (a normalize that just got slower because of an unrelated change) is exactly the kind of thing the burst simulator catches before it becomes a production incident. The capstone wires this into CI.

---

## Key Takeaways

* The **pressure chain** is a contiguous sequence of bounded buffers from source to sink with `send().await` (or `FlowPolicy` equivalent) on every edge. A slowdown at any operator propagates upstream all the way to the source's producing primitive. The chain breaks at any unbounded buffer or detached `tokio::spawn` per event.
* The **canonical breakage patterns** are: per-event `tokio::spawn` (M1's anti-pattern revisited), `mpsc::unbounded_channel`, fire-and-forget logging on unbounded channels, `Vec::push` into unbounded accumulators, and drop-and-recreate task patterns. The fix in every case is structural: replace with bounded-and-suspending equivalents.
* **Reading the pressure gradient** identifies the slowest stage. The slowest operator's incoming channel is persistently full; channels upstream are partially full; channels downstream are mostly empty. The dashboard panel for per-channel occupancy is the primary diagnostic.
* **TCP windowing as backpressure** works only when the application reads synchronously from the socket and processes inline. An async-buffer pattern that reads as fast as possible breaks the kernel-level chain just like an unbounded `Vec` breaks the application-level chain.
* **Backpressure across Kafka does not propagate synchronously**. Kafka decouples producer and consumer intentionally. The pipeline's discipline is to **monitor consumer lag explicitly** as the substitute for the within-process pressure signal. Module 5 develops Kafka as a checkpoint store; Module 6 develops the lag monitoring.

---

{{#quiz lesson-03-quiz.toml}}
