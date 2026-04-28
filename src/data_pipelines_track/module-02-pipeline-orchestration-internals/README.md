# Module 02 — Pipeline Orchestration Internals

**Track:** Data Pipelines — Space Domain Awareness Fusion
**Position:** Module 2 of 6
**Source material:** *Async Rust* — Maxwell Flitton & Caroline Morton, the chapters on `tokio::task`, `JoinSet`, cancellation, and structured shutdown; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (operator-graph execution and failure handling); *Kafka: The Definitive Guide* — Shapira, Palino, Sivaram, Petty, Chapter 7 (Reliable Data Delivery); *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapters 2–3 (Orchestration as an Undercurrent; Plan for Failure)
**Quiz pass threshold:** 70% on all four lessons to unlock the project

---

## Mission Context

> **OPS ALERT — SDA-2026-0119**
> **Classification:** ORCHESTRATION TIER STAND-UP
> **Subject:** Replace hand-spawned topology with declarative orchestrator
>
> Phase 1 ingestion (`sda-ingest`) is in production with three sensor sources, a normalize stage, a validate stage, and a JSONL sink. The next-quarter roadmap adds five more sources, a windowed dedup, a cross-sensor correlator, an alert emitter, and an audit sink. The hand-spawned topology in `main.rs` has reached the practical limit of what one engineer can hold in their head. Two operational gaps from last quarter's incidents are also unresolved: a panicking task is silently torn down with no recovery hook (SDA-2026-0094 postmortem), and the optical poller's retry logic is unjittered fixed-delay (SDA-2026-0103 postmortem — the 90-minute outage extension after a 30-second partner blip).
>
> **Directive:** Build the orchestrator. A declarative DAG of operators, supervised by a single supervisor, with retry and circuit-breaker primitives that respect downstream characteristics, and runtime-level bulkheading for the new orbital propagator. The Phase 1 binary is refactored to use the orchestrator with no behavioral regression beyond the documented failure-handling improvements.

This module is the connective tissue of the rest of the track. The orchestrator built here is what every subsequent module's project hangs on. Module 3 changes one operator (the dedup → windowed correlator); Module 4 hardens channel boundaries against burst load; Module 5 makes operator state crash-safe via checkpointing; Module 6 wraps the assembled system in observability. None of those changes alter the orchestrator's API. The shape established here is load-bearing for the next four modules.

The mental model the module installs: an operator is a `Task`, a topology is an `OperatorGraph`, a running pipeline is a `BuiltGraph` driven by a `Supervisor`. Failures are dispatched on a four-case `TaskExit` enum. Retries use decorrelated jitter. Resource-isolation needs are met with separate runtimes. None of these patterns are SDA-specific; the orchestrator crate is meant to be reusable across any pipeline that fits the dataflow model.

---

## Learning Outcomes

After completing this module, you will be able to:

1. Articulate why a pipeline is naturally expressed as a graph of supervised tasks rather than a single async function, and explain the operational properties of each shape
2. Distinguish CPU-bound from IO-bound operators and place each on the correct part of the Tokio runtime (`spawn` vs `spawn_blocking`)
3. Reason about cancel-safety as a per-await-point property and identify operator implementations that violate it
4. Build a declarative `OperatorGraph` with build-time validation of cycles and disconnected operators
5. Implement a supervisor with bounded restart budgets that distinguishes panics from errors from clean exits
6. Design retry policies that classify errors correctly, use decorrelated-jitter backoff, and compose with idempotency to produce effective exactly-once processing
7. Apply circuit breakers and runtime-level bulkheading where retries alone are insufficient

---

## Lesson Summary

### Lesson 1 — The Task Model

What `tokio::spawn` actually does, why CPU-bound work belongs on `spawn_blocking`, what `JoinHandle::abort` actually means (cooperative, observed at the next await point), and what cancel-safety means as a per-await-point property. Closes with the `Task` wrapper struct that gives the orchestrator a uniform handle type for every operator and the `TaskExit` enum that distinguishes the four operationally meaningful exit cases.

*Key question:* If `JoinHandle::abort()` is called and the handle resolves to `Err(JoinError)` with `is_cancelled() == true`, does that mean the task has actually stopped running?

### Lesson 2 — DAG Scheduling

The `OperatorGraph` builder, Kahn's algorithm topological sort with cycle detection, the four-pass `build()` (per-role validation, topo sort, channel allocation, spawn), and `JoinSet` for managing N operator handles with whichever-finishes-first semantics. Why the bounded-channel-per-edge invariant is what makes backpressure-traversal-through-the-DAG tractable and why fan-in/fan-out are expressed as explicit router operators rather than multi-edge vertices.

*Key question:* The pipeline has three sources fanning into a single normalize operator. What does the topo-sorted spawn order look like, and which property of the order is what makes the channel-wiring code work?

### Lesson 3 — Retries and Idempotency

Three pieces of retry discipline: classifying transient vs permanent vs discardable errors (and why the classification is the operator's responsibility, not the framework's), exponential backoff with decorrelated jitter (and why fixed-delay retries amplify outages), idempotency as a property of the operation that lets at-least-once delivery compose into effective exactly-once. The `with_retry` wrapper, the `RetryDisposition` enum, and the dedup sink with sliding-window seen-set bounded by both time and count.

*Key question:* A hundred operator instances all hit the same downstream failure at the same instant. With fixed-delay retries, what happens to the downstream during recovery, and why?

### Lesson 4 — Failure Modes

What retry cannot address: panics, cascading slowdowns, resource exhaustion in shared pools. The supervisor pattern with `JoinSet::join_next` and `TaskExit` dispatch. Bounded restart budgets and why "always restart" is dangerous. Three levels of bulkheading (channel, runtime, process). Cascading failures and the discipline of addressing the cause rather than the symptom. Circuit breakers as the fail-fast complement to retries.

*Key question:* The validate operator panics on a bad input. The pipeline has no supervisor. What happens to the pipeline's apparent behavior, and what changes when the supervisor pattern is wired in?

---

## Capstone Project — Fusion Pipeline Orchestrator

Build the `sda-orchestrator` library: declarative `OperatorGraph`, supervised `JoinSet`-driven `Task` lifecycle, retry wrapper with decorrelated jitter, circuit breaker, and runtime-level bulkheading via a dedicated `tokio::runtime::Runtime` for the orbital propagator. The Phase 1 `sda-ingest` binary from Module 1 is refactored to use the new orchestrator with no behavioral regression beyond the documented failure-handling improvements. Acceptance criteria, suggested architecture, deterministic-timing test patterns, and the full project brief are in `project-fusion-orchestrator.md`.

The orchestrator is not a throwaway. The interface stays stable through Modules 3, 4, 5, and 6.

---

## File Index

```
module-02-pipeline-orchestration-internals/
├── README.md                                  ← this file
├── lesson-01-task-model.md                    ← The task model
├── lesson-01-quiz.toml                        ← Quiz (5 questions)
├── lesson-02-dag-scheduling.md                ← DAG scheduling
├── lesson-02-quiz.toml                        ← Quiz (5 questions)
├── lesson-03-retries-idempotency.md           ← Retries and idempotency
├── lesson-03-quiz.toml                        ← Quiz (5 questions)
├── lesson-04-failure-modes.md                 ← Failure modes
├── lesson-04-quiz.toml                        ← Quiz (5 questions)
└── project-fusion-orchestrator.md             ← Capstone project brief
```

---

## Prerequisites

* Module 1 (Stream Processing Foundations) completed — the `Observation` envelope, the `ObservationSource` trait, the `ChannelSink` pattern, and the bounded-channel backpressure model are assumed
* Foundation Track completed — async Rust, channels, network programming
* Familiarity with `tokio::task::JoinSet`, `tokio::sync::oneshot`, `tokio_util::sync::CancellationToken`, and `anyhow::Result`
* Working comfort reading and writing structured logs with `tracing`

## What Comes Next

Module 3 (Event Time and Watermarks) replaces the processing-time dedup operator from Module 1 with a windowed event-time correlator that computes conjunction risk from observations of the same orbital event arriving from multiple sensors. The orchestrator interface stays the same — only one operator's implementation changes. Watermark propagation becomes a property of the graph's edges, which the orchestrator's channel structure already accommodates.
