# Module 04 — Backpressure and Flow Control

**Track:** Data Pipelines — Space Domain Awareness Fusion
**Position:** Module 4 of 6
**Source material:** *Async Rust* — Maxwell Flitton & Caroline Morton, the chapters on `tokio::sync::mpsc` semantics and channel patterns; *Network Programming with Rust* — Abhishek Chanda, sections on application-level flow control and TCP windowing as transport-level backpressure; *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 3 (Producer Configuration and `max.in.flight.requests.per.connection`); *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (Buffering and Pushback)
**Quiz pass threshold:** 70% on all three lessons to unlock the project

---

## Mission Context

> **OPS ALERT — SDA-2026-0188**
> **Classification:** BURST-LOAD HARDENING
> **Subject:** Replace uniform load shed with priority-aware FlowPolicy
>
> Last week's anti-satellite test added 1,800 newly tracked debris objects to the catalog within 90 seconds. The Phase 3 pipeline survived but with twelve minutes of catch-up time and four dropped conjunction alerts during the spike. The postmortem traced the dropped alerts to a single edge — the alert-emitter's incoming channel — where the buffer was sized for nominal traffic, the upstream operator was IO-bound and could not slow further, and the downstream had no mechanism to triage which observations to drop. The pipeline's response to the burst was uniform load shed without policy. The four critical alerts that got dropped were no more important to the system than the four hundred non-critical observations dropped alongside them.

The pipeline at the start of this module is correct under steady-state load and tolerates transient downstream slowdowns. It is not yet correct under burst load. The mechanism for backpressure (`send().await` on bounded channels) propagates the slowdown but does not give the operator any policy choice about *what* to do when the channel fills up — the upstream just slows, regardless of what the data being slowed represents. For the post-Cosmos burst, that uniform behavior was exactly the wrong shape: the system needed to triage during the spike, preserving the high-priority alerts that could not afford the latency hit while shedding the low-priority redundant samples that contribute little under load.

This module installs the discipline. Per-edge `FlowPolicy` (Backpressure / Shed / Timed) makes the policy explicit at every edge. A priority classifier distinguishes high-priority from low-priority observations. The audit script becomes a CI test that fails the build on new unbounded channels or undocumented FlowPolicy choices. The burst simulator becomes the regression-detection canary. The patterns generalize to any streaming pipeline that must survive bursts; the module's specifics are where the techniques meet SDA's actual workload.

The mental model the module installs is the four-piece backpressure discipline: (1) every edge has an explicit per-edge FlowPolicy chosen for its operational role, (2) buffer sizing is documented per-edge with a `BurstProfile` rather than picked by reflex, (3) credit-based flow is reached for specifically when the receiver needs to pause without occupying a slot (checkpoint flushes, cross-runtime edges, graceful drains), (4) the pressure chain is continuously audited and the per-channel occupancy gradient is the first-look dashboard panel.

---

## Learning Outcomes

After completing this module, you will be able to:

1. Choose between `send().await`, `try_send()`, and `send_timeout()` based on the operator's load-shedding policy, and encode the choice as a per-edge `FlowPolicy`
2. Size channel buffers using a documented `BurstProfile` rather than by reflex, with the math (rate gap × duration × safety factor) made explicit per edge
3. Implement a load-shedding sink with priority-aware sub-channels and a biased select that gives high-priority data deterministic preference under shed conditions
4. Reach for credit-based flow control specifically when the receiver needs to pause without occupying an in-flight slot — checkpoint flushes, cross-runtime edges, graceful drains
5. Diagnose backpressure-chain breakage from the per-channel occupancy gradient: the rightmost persistently-full edge points at the slowest operator
6. Recognize the canonical pressure-chain breakage patterns (per-event `tokio::spawn`, `unbounded_channel`, fire-and-forget logging, unbounded `Vec::push` accumulators, drop-and-recreate task patterns) and their structural fixes
7. Reason about backpressure across boundary cases: TCP windowing as transport-level chain link; Kafka's deliberate decoupling that requires explicit consumer-lag monitoring as the substitute pressure signal

---

## Lesson Summary

### Lesson 1 — Bounded Channels

The three send semantics — `send().await` for backpressure, `try_send` for explicit load shed (always paired with a drop counter), `send_timeout` for SLO-bound edges where blocking past a deadline is worse than dropping — encoded as a per-edge `FlowPolicy` enum. Buffer sizing as documented `BurstProfile` rather than reflexive 1024; the math made explicit. Why `unbounded_channel` is a footgun for any data-path edge.

*Key question:* The conjunction-emitter has a 200ms SLO from observation-arrival to alert-emit, and the alert subscriber occasionally returns 503 during deploys. Which FlowPolicy is the right choice for the emitter's outgoing edge, and why?

### Lesson 2 — Credit-Based Flow Control

Credit-based flow as the alternative to bounded-channel-plus-await for cases where the receiver needs to pause the upstream *without* occupying any in-flight slot. The structural difference (decoupled credit signal and data channel), the production protocols that use it (HTTP/2 windows, AMQP prefetch, Kafka's `max.in.flight.requests.per.connection` as a single-credit degenerate case), the SDA cases (checkpoint flushes, cross-runtime edges, graceful drains), and the in-flight-bounded property the AFTER-processing return preserves.

*Key question:* The CreditHandle has a Drop impl that warns when the handle is dropped without `return_credit()` being called. What canonical failure mode does that warning surface, and why does it matter operationally?

### Lesson 3 — End-to-End Backpressure Propagation

The pressure chain as a contiguous sequence of bounded buffers from source to sink. The five canonical breakage patterns (per-event `tokio::spawn`, `unbounded_channel`, fire-and-forget unbounded logging, `Vec::push` accumulators, drop-and-recreate task patterns) and their structural fixes. Reading the per-channel occupancy gradient as first-look diagnostic. TCP windowing as transport-level chain link. Kafka's deliberate decoupling that requires explicit consumer-lag monitoring as the substitute pressure signal.

*Key question:* The dashboard shows three persistently-full edges across a six-edge topology. Which operator is the bottleneck, and why does the gradient-reading discipline give an unambiguous answer?

---

## Capstone Project — Backpressure-Aware Fusion Broker

Harden the Phase 3 pipeline against the post-Cosmos-1408 burst failure mode. Every edge gets an explicit `FlowPolicy`; a priority classifier distinguishes High from Low observations; a `PriorityShedSink` with biased select gives High deterministic preference and drops Low first under shed conditions. The audit script becomes a `cargo test` that fails the build on new unbounded channels or undocumented policies. The `BurstSimulator` drives 10x rate for 5 simulated minutes and asserts zero High-priority drops. Acceptance criteria, suggested architecture, and the full project brief are in `project-backpressure-broker.md`.

The orchestrator from M2 and the windowed correlator from M3 are unchanged in structure. Only the edges' policies change, and a new operator (the priority classifier) sits between the normalize and the correlator.

---

## File Index

```
module-04-backpressure-and-flow-control/
├── README.md                                  ← this file
├── lesson-01-bounded-channels.md              ← Bounded channels and FlowPolicy
├── lesson-01-quiz.toml                        ← Quiz (5 questions)
├── lesson-02-credit-based-flow.md             ← Credit-based flow control
├── lesson-02-quiz.toml                        ← Quiz (5 questions)
├── lesson-03-end-to-end-propagation.md        ← End-to-end backpressure propagation
├── lesson-03-quiz.toml                        ← Quiz (5 questions)
└── project-backpressure-broker.md             ← Capstone project brief
```

---

## Prerequisites

* Modules 1, 2, and 3 completed — the `Observation` envelope, the orchestrator's `OperatorGraph` and supervisor, and the watermark-aware windowed correlator are all assumed
* Foundation Track completed — async Rust, channels, scheduling intuitions
* Familiarity with `tokio::sync::mpsc::{Sender, Receiver}` semantics and `tokio::time::{timeout, sleep, pause}`
* Comfort reading Prometheus-style metric names (`channel_occupancy{edge=...}`) and reasoning about per-label counters and gauges

## What Comes Next

Module 5 (Delivery Guarantees and Fault Tolerance) makes the windowed correlator's state crash-safe via checkpointing. The credit-based-flow primitive from this module's Lesson 2 is the mechanism that pauses the upstream during the checkpoint flush. The bounded-channel-everywhere invariant the audit enforces is what lets the checkpointed state size be bounded and predictable. The exactly-once-via-idempotency frame from M2 plus the retract-aware sink from M3 plus the priority shedding from M4 compose into the pipeline M5 will harden against process restarts.
