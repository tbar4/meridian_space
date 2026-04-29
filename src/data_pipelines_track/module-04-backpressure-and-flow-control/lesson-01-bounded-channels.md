# Lesson 1 — Bounded Channels

**Module:** Data Pipelines — M04: Backpressure and Flow Control
**Position:** Lesson 1 of 3
**Source:** *Async Rust* — Maxwell Flitton & Caroline Morton, the chapters on `tokio::sync::mpsc` semantics and channel patterns; *Network Programming with Rust* — Abhishek Chanda, sections on application-level flow control over TCP; *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 3 (Producer Configuration: `buffer.memory`, `max.block.ms`, `acks`)

---

## Context

Module 1 introduced `mpsc::Sender::send().await` as the foundation of backpressure: a full bounded channel suspends the sending future until the consumer makes capacity, propagating the slowdown upstream. Module 2's orchestrator structurally enforced "every edge in the operator graph is a bounded channel." Module 3 added watermarks and windowing on top of that channel structure. The pipeline that exists at the start of this module is correct under steady-state load and tolerates transient downstream slowdowns.

It does not tolerate burst load. Last quarter's Cosmos-1408 anti-satellite test added 1,800 newly tracked debris objects to the catalog within 90 seconds. The pipeline survived in the sense that no operator panicked, but with twelve minutes of catch-up time and four dropped conjunction alerts during the spike. The postmortem traced the alerts to a single channel where the buffer was sized for nominal traffic, the upstream operator was IO-bound and could not slow further, and the downstream operator had no mechanism to triage which observations to drop and which to preserve. The pipeline's response to the burst was uniform load shed without policy — the four critical alerts that got dropped were no more important to the system than the four hundred non-critical observations dropped alongside them.

This module is the response. The orchestrator's bounded-channel invariant is correct but not sufficient; **what** to do when a bounded channel fills up is a design decision that has been left implicit, and the explicit answer is a per-edge `FlowPolicy` with three named choices. This lesson establishes the choices, develops the channel semantics behind each, and develops the discipline of sizing channel buffers on purpose rather than by reflex. Lesson 2 covers credit-based flow control as the alternative shape for cases where awaiting is not enough. Lesson 3 audits the full pipeline for places where the backpressure chain breaks. The capstone hardens the M3 pipeline against a 10x burst simulation with explicit flow policies and load-shedding.

---

## Core Concepts

### Bounded vs Unbounded Channels

`tokio::sync::mpsc::channel(N)` produces a *bounded* channel: at most N items can be in flight between the sender and the receiver. Beyond N, the next send suspends until the receiver consumes. `tokio::sync::mpsc::unbounded_channel()` produces an *unbounded* channel: items are buffered without limit, growing memory as long as the sender produces faster than the receiver consumes.

Unbounded channels are the wrong default. The reason is structural: any unbounded buffer in the pipeline is a place where backpressure does not propagate. The upstream sender writes into the unbounded channel, never suspends, and never observes the downstream's slowness. The buffer grows. The process's resident memory grows. Either a higher-level resource limit (the OOM killer; a container's memory cap; a tokio runtime memory budget) eventually intervenes, or the process keeps growing until something else gives. None of these outcomes are graceful; all are surprising at runtime, and all are diagnosed only after the symptom shows up.

The legitimate use cases for unbounded channels are narrow. *In-process notification of singleton events* — the orchestrator's "shutdown signal" that fires once and is consumed once. *Tests where the test harness controls both ends* and the bound is implicit in the test's structure. *Bounded-by-construction sources* where the application can prove the channel cannot fill. None of these apply to the data path of a streaming pipeline. The orchestrator's audit script in Lesson 3 asserts that no `unbounded_channel` appears anywhere in the operator graph; this lesson is the conceptual justification for that assertion.

### `send().await` Semantics

`send(item).await` is the default and the right choice for most operator-pair edges. The semantics: when the channel is full, the future returned by `send` does not resolve until there is capacity for the item; the calling task is suspended (cooperatively, in the M2 L1 sense) and another task on the worker thread can run. The send is **cancel-safe** — dropping the future at any point is well-defined: either the item was inserted into the channel (`Ok(())` returned) or it was not (the future was dropped before the channel had capacity). No third state.

The operational consequence is that the channel's *capacity* IS the backpressure mechanism. A channel of capacity 1024 is a slack budget: the upstream operator can produce 1024 items ahead of the downstream's processing before backpressure begins to apply. Within the budget, the upstream runs at its own rate; past the budget, the upstream's rate is capped at the downstream's rate. The relationship is exactly the dataflow-model contract: no operator runs faster than its slowest downstream.

The choice of `send().await` over `try_send` is the choice of "applied backpressure" over "load shed." Any pipeline whose default behavior under load should be "the upstream slows down" uses `send().await`. The radar source operator that calls `send(observation).await` on a full channel ends up suspended at that point; the next time it polls its UDP socket, the kernel buffer has had time to fill or drop frames at its layer. This is the right behavior for a UDP source: kernel-level drop preserves the pipeline's invariants while applying the right kind of pressure.

### `try_send()` Semantics

`try_send(item)` returns immediately. On a full channel, it returns `Err(TrySendError::Full(item))` — the item is handed back to the caller, untouched. The caller decides what to do: drop it, log it, route it to a DLQ, retry it later. The semantics are explicit load-shed: the channel's capacity is a hard limit, and an attempt to exceed it does not block.

The right use case is operator-side load shedding when the data being shed has lower marginal value than the work blocking the upstream. The metrics-export operator that emits sample metrics is the canonical case: a metric that did not get published is a small, recoverable loss; blocking the upstream operator on the metrics channel would impose a larger cost than the benefit of every metric reaching its destination. `try_send` with a `gauge!("metrics_drops_total")` increment is the right shape.

The trap that the lesson called out at the top is `try_send` *with no logging*. An operator that calls `try_send` and discards the `Err(Full)` produces silent drops that are invisible until aggregate output deficits show up downstream. The discipline is uniform: every `try_send` site has a counter increment and a structured log entry on `Full`. The orchestrator's metrics endpoint exposes the counter; an alert fires when the drop rate per second exceeds a threshold. Without the counter, `try_send` is a footgun; with the counter, it is a load-shedding tool.

### `send_timeout()` Semantics

`send_timeout(item, dur)` is the third primitive. It suspends like `send().await` but resolves with `Err(SendTimeoutError::Timeout(item))` after `dur` has elapsed without capacity becoming available. The caller decides what to do with the timed-out item: drop, DLQ, retry on a different channel.

This is the right primitive for operators with a real-time deadline. The conjunction-alert emitter has a 200 ms SLO from observation-arrival to alert-emit. If the alert subscriber's HTTP endpoint is too slow to drain the alert channel within 200 ms, `send_timeout` lets the operator make an explicit choice — drop the alert (with metrics and DLQ), route to a slow-path archive, or whatever the operational decision is — rather than blocking past the SLO. The deadline-bound choice fits naturally between `send().await` (no deadline, may block forever) and `try_send` (no wait at all).

The defaults across the SDA pipeline:

| Edge | Policy | Reasoning |
|---|---|---|
| Source → normalize | `send().await` | Apply backpressure to source; UDP drops at kernel are acceptable |
| Normalize → correlator | `send().await` | No deadline; correlator is the natural-rate consumer |
| Correlator → alert sink | `send_timeout(200ms)` | SLO-bound; timed-out alerts go to DLQ |
| Pipeline → metrics export | `try_send` + drop counter | Metrics are sheddable; observability of drops is what matters |

The discipline is per-edge, documented in the operator graph declaration with a brief comment about the choice. The orchestrator's structured log emits the policy at startup so runbooks can confirm what is configured.

### Buffer Sizing

The capacity of a bounded channel is the slack budget between the producer and the consumer — how much *burst* the channel absorbs before backpressure begins to apply. The right sizing is operational, not magical. Three considerations.

**Sustained rate vs burst rate**: a channel sized for the *sustained* rate has effectively zero slack and applies backpressure constantly under nominal load, which is wrong. A channel sized for the *burst* rate has too much slack and adds latency under load (every item in the channel is one waiting in front of the next one). The right size is bounded by the expected burst duration × the rate gap between producer and consumer, with a 2x safety factor for headroom.

**Per-item processing time at the consumer**: a channel sized for 1024 items where each item takes 100 µs to process represents 100 ms of latency at the consumer side under steady-state full-channel conditions. If the operator's SLO budget is 200 ms, that 100 ms of channel-induced latency might be more than the budget allows, and the right answer is a smaller channel.

**Memory cost per slot**: each slot in the channel holds one `Observation` (or whatever the channel's item type is). For envelopes of a few kilobytes, a channel of 1024 is negligible. For envelopes that carry larger payloads, the per-slot memory matters and the channel should be sized accordingly.

The default for SDA's pipeline is 1024 for source-to-normalize edges, 4096 for normalize-to-correlator (the correlator's per-event work is heavier and the slack absorbs more of the burst), and 256 for the alert-emit edge (low rate, tight latency). Each edge is documented with a comment in the graph declaration explaining the choice. Sizing by reflex (everything is 1024, "because that's what we used last time") is the pattern the post-Cosmos postmortem identified as having contributed to the dropped alerts.

---

## Code Examples

### Three Sinks with Three Policies

The same logical sink role with three different flow policies, each appropriate for a different edge in the topology.

```rust,ignore
use anyhow::Result;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

/// `BackpressureSink` applies upstream backpressure on a full channel.
/// The right choice for an edge where the upstream should slow rather
/// than the data should be dropped.
pub struct BackpressureSink {
    tx: mpsc::Sender<Observation>,
}

impl BackpressureSink {
    pub async fn write(&self, obs: Observation) -> Result<()> {
        // send().await suspends until capacity. Cancel-safe.
        self.tx.send(obs).await
            .map_err(|_| anyhow::anyhow!("downstream receiver dropped"))?;
        Ok(())
    }
}

/// `SheddingSink` drops on a full channel rather than block. The right
/// choice for sheddable data; ALWAYS pair with a drop counter.
pub struct SheddingSink {
    tx: mpsc::Sender<Observation>,
    drops: metrics::Counter,
}

impl SheddingSink {
    pub fn new(tx: mpsc::Sender<Observation>) -> Self {
        Self {
            tx,
            drops: metrics::counter!("shedding_sink_drops_total"),
        }
    }

    pub fn write(&self, obs: Observation) -> Result<()> {
        match self.tx.try_send(obs) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_obs)) => {
                self.drops.increment(1);
                tracing::debug!("shedding sink dropped observation; channel full");
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(anyhow::anyhow!("downstream receiver dropped"))
            }
        }
    }
}

/// `TimedSink` writes with a deadline. After the deadline, the item is
/// returned to the caller; production code routes it to a DLQ or
/// archive sink.
pub struct TimedSink {
    tx: mpsc::Sender<Observation>,
    deadline: Duration,
    dropped_with_deadline: metrics::Counter,
}

impl TimedSink {
    pub fn new(tx: mpsc::Sender<Observation>, deadline: Duration) -> Self {
        Self {
            tx,
            deadline,
            dropped_with_deadline: metrics::counter!("timed_sink_deadline_drops_total"),
        }
    }

    pub async fn write(&self, obs: Observation) -> Result<()> {
        // tokio::time::timeout wraps the send; on timeout, we get the
        // item back via Err and decide what to do with it.
        match timeout(self.deadline, self.tx.send(obs)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(anyhow::anyhow!("downstream receiver dropped")),
            Err(_elapsed) => {
                self.dropped_with_deadline.increment(1);
                // Production: route to slow-path archive sink. Here:
                // log and drop.
                tracing::warn!("timed sink missed deadline; dropping");
                Ok(())
            }
        }
    }
}
```

The three sinks are interchangeable in shape — same `write(obs) -> Result<()>` signature — but operationally different. The orchestrator's edge wiring chooses one per edge; the operator that consumes the sink does not need to know which policy is in effect. The metrics surfaced by each (`shedding_sink_drops_total`, `timed_sink_deadline_drops_total`) are the operator-visibility property the lesson keeps flagging — silent drops are the bug, instrumented drops are the tool.

### A `FlowPolicy` Enum for Per-Edge Configuration

The lesson's discipline is per-edge. Encoding the policy in an enum makes the choice visible in the operator graph declaration and lets a single sink implementation dispatch to the right semantics.

```rust,ignore
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub enum FlowPolicy {
    /// Apply backpressure: the upstream slows down rather than items
    /// being dropped. The default for most pipeline edges.
    Backpressure,
    /// Drop items on a full channel; log via the `dropped_total` metric.
    /// For sheddable data: metrics export, optional logging, etc.
    Shed,
    /// Wait up to `deadline` for capacity; drop on timeout. For SLO-bound
    /// edges where blocking past the deadline is worse than dropping.
    Timed(Duration),
}

pub struct ConfigurableSink {
    tx: mpsc::Sender<Observation>,
    policy: FlowPolicy,
}

impl ConfigurableSink {
    pub fn new(tx: mpsc::Sender<Observation>, policy: FlowPolicy) -> Self {
        Self { tx, policy }
    }

    pub async fn write(&self, obs: Observation) -> Result<()> {
        match self.policy {
            FlowPolicy::Backpressure => self.tx.send(obs).await
                .map_err(|_| anyhow::anyhow!("receiver dropped")),
            FlowPolicy::Shed => match self.tx.try_send(obs) {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => {
                    metrics::counter!("flow_drops_total", "policy" => "shed").increment(1);
                    Ok(())
                }
                Err(mpsc::error::TrySendError::Closed(_)) =>
                    Err(anyhow::anyhow!("receiver dropped")),
            },
            FlowPolicy::Timed(deadline) => match timeout(deadline, self.tx.send(obs)).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) => Err(anyhow::anyhow!("receiver dropped")),
                Err(_) => {
                    metrics::counter!("flow_drops_total", "policy" => "timed").increment(1);
                    Ok(())
                }
            },
        }
    }
}
```

Two design points. The metric label `policy` is what makes the metric useful operationally: a single counter with the policy label tells you which kind of drop is happening, and the dashboards filter by it. A separate counter per policy works too but produces dashboard duplication. Second, the dispatch on `self.policy` is per-call; the cost is a single match against a copy of an enum, which is sub-nanosecond in the hot path. The expressiveness gain over three separate sink types is worth that cost.

### Buffer Sizing With Documented Reasoning

A small helper that captures the reasoning behind a buffer size as data the operator graph carries forward into structured logs and runbook references.

```rust,ignore
use std::time::Duration;

/// The expected burst characteristics of a channel and the resulting
/// recommended buffer size. Documented per-edge in the topology
/// declaration; the orchestrator emits a startup log with the values.
#[derive(Debug, Clone, Copy)]
pub struct BurstProfile {
    pub peak_rate_per_s: u64,
    pub peak_duration_s: u64,
    pub processing_rate_per_s: u64,
    pub safety_factor: f32,
}

impl BurstProfile {
    /// Recommended buffer size: how many items the channel needs to
    /// hold to absorb the burst without applying backpressure for its
    /// duration. Past the size, backpressure begins to apply normally.
    pub fn recommended_buffer(&self) -> usize {
        let rate_gap = self.peak_rate_per_s.saturating_sub(self.processing_rate_per_s);
        let burst_items = rate_gap * self.peak_duration_s;
        ((burst_items as f32) * self.safety_factor) as usize
    }
}

// Example: source→normalize edge for the radar source.
//   peak_rate_per_s: 5000 (during a fragmentation event)
//   peak_duration_s: 60 (the burst absorbs about a minute)
//   processing_rate_per_s: 4500 (the normalizer's measured throughput)
//   safety_factor: 2.0
//   recommended_buffer() = (5000 - 4500) * 60 * 2.0 = 60,000 items
//
// 60,000 items at ~200 bytes per Observation envelope = 12 MB.
// Acceptable cost for the burst-absorption property; documented in
// the graph declaration with this comment.
const RADAR_SOURCE_BURST: BurstProfile = BurstProfile {
    peak_rate_per_s: 5000,
    peak_duration_s: 60,
    processing_rate_per_s: 4500,
    safety_factor: 2.0,
};
```

The math is not magic but it is also not "1024 because we always use 1024." Each per-edge buffer size has a `BurstProfile` constant declaring the assumptions, and the orchestrator's startup log emits the (edge_name, buffer_size, profile) triple so the runbook can reference it. The numbers are operational — they come from load tests and production observation, and they evolve as the workload changes. The discipline this lesson installs is making the assumptions visible rather than baked into magic numbers.

---

## Key Takeaways

* **Bounded channels are the default** in any production pipeline. `unbounded_channel` is a footgun for data-path edges; use it only for singleton-event signals or test scaffolding. The orchestrator's audit asserts none appear in the operator graph.
* The three send semantics are operationally distinct. **`send().await`** applies backpressure (upstream slows). **`try_send`** load-sheds on full (always pair with a drop counter). **`send_timeout(dur)`** is for SLO-bound edges where blocking past a deadline is worse than dropping. Encode the choice as a per-edge `FlowPolicy`.
* The right primitive depends on the question being asked. *Should the upstream slow down?* → `send().await`. *Is this data sheddable under load?* → `try_send` + counter. *Does this edge have a real-time deadline?* → `send_timeout` + DLQ/archive.
* **Buffer sizing is per-edge and documented**. The math: `(peak_rate - processing_rate) × peak_duration × safety_factor`. The default of 1024 for everything is the pattern that the post-Cosmos-1408 postmortem identified as a contributing cause of dropped alerts.
* **Silent drops are the bug; instrumented drops are the tool**. Every load-shedding site has a counter, a label, and a dashboard panel. An undocumented `try_send` is a bug waiting to be diagnosed; an instrumented one is operationally legible.

---

{{#quiz lesson-01-quiz.toml}}
