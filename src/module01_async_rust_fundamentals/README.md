# Module 01 — Async Rust Fundamentals

**Track:** Foundation — Mission Control Platform  
**Position:** Module 1 of 6  
**Source material:** *Async Rust* — Maxwell Flitton & Caroline Morton, Chapters 1, 2, 7  
**Quiz pass threshold:** 70% on all three lessons to unlock the project

---

## Mission Context

Meridian's legacy Python control plane was built for a 6-satellite constellation. It handles ground station connections sequentially: one connection at a time, blocking on each telemetry frame before moving to the next. At 48 satellites across 12 ground station sites, this architecture is the primary bottleneck in the control plane. During peak pass windows, the broker accumulates up to 40 seconds of delivery lag — unacceptable for conjunction avoidance workflows that require sub-10-second frame delivery.

This module establishes the async Rust foundation for the replacement system. Before writing any production control plane code, you need an accurate model of how async Rust executes — not at the surface level of `#[tokio::main]`, but at the level of futures, the polling contract, executor scheduling, and task lifecycle. Every architectural decision in the modules that follow depends on this model.

---

## What You Will Learn

By the end of this module you will be able to:

- Implement the `Future` trait directly and trace the polling lifecycle from first call to completion
- Explain the waker contract and identify futures that will silently stall due to missing waker registration
- Distinguish between `tokio::spawn`, `tokio::join!`, and `tokio::select!` and apply each to the correct concurrency pattern
- Configure a Tokio runtime explicitly via `Builder`, size worker and blocking thread pools for a given workload profile, and isolate high-frequency I/O workloads from blocking work
- Cancel tasks safely using `.abort()` and `tokio::time::timeout`, understanding exactly where and when the future is dropped
- Implement a graceful shutdown sequence with a bounded drain deadline, using RAII and `CancellationToken` for async cleanup

---

## Lessons

### Lesson 1 — The async/await Model: Futures, Polling, and the Executor Loop

Covers the `Future` trait, the `poll` function, `Poll::Ready` vs `Poll::Pending`, the waker contract, `Pin`, and the executor's task queue. Includes a manually implemented future to make the state machine mechanics explicit.

**Key question this lesson answers:** What actually happens at every `await` point, and what causes a task to silently stall?

→ `lesson-01-async-await-model.md` / `lesson-01-quiz.toml`

---

### Lesson 2 — The Tokio Runtime: Spawning Tasks, the Scheduler, and Thread Pools

Covers Tokio's multi-thread work-stealing scheduler, the distinction between worker threads and blocking threads, `tokio::task::spawn_blocking`, and explicit runtime configuration via `Builder`. Includes a dual-runtime pattern for isolating ingress and housekeeping workloads.

**Key question this lesson answers:** How do you configure the runtime for your actual workload rather than the defaults, and when does blocking work need to leave the async executor?

→ `lesson-02-tokio-runtime.md` / `lesson-02-quiz.toml`

---

### Lesson 3 — Task Lifecycle: Cancellation, Timeouts, and JoinHandle Management

Covers `JoinHandle<T>` semantics, cooperative cancellation with `.abort()`, RAII cleanup on cancellation, `tokio::time::timeout`, `tokio::select!` for racing futures, and a complete graceful shutdown pattern using `broadcast` and a bounded drain deadline.

**Key question this lesson answers:** How do you cleanly terminate a task — whether it completes normally, times out, or receives a shutdown signal — without leaking resources or corrupting state?

→ `lesson-03-task-lifecycle.md` / `lesson-03-quiz.toml`

---

## Capstone Project — Async Telemetry Ingestion Broker

Build the TCP ingress layer for Meridian's replacement control plane. The broker accepts concurrent connections from ground stations, reads length-prefixed telemetry frames, fans each frame out to multiple downstream handlers via a `broadcast` channel, and shuts down gracefully on Ctrl-C with a 10-second drain deadline.

Acceptance is against 7 verifiable criteria including concurrent connection handling, broadcast fan-out correctness, slow-handler isolation, and bounded graceful shutdown.

→ `project-async-telemetry-broker.md`

---

## Prerequisites

This module assumes you are comfortable with Rust ownership, borrowing, traits, and closures. It does not re-explain language fundamentals. If you are new to async Rust generally, the module starts from first principles at the trait level — but it expects you to read Rust error messages without assistance.

## What Comes Next

**Module 2 — Concurrency Primitives** builds directly on this foundation: now that you understand how the executor runs tasks, Module 2 covers how those tasks share state safely — `Mutex`, `RwLock`, atomics, and memory ordering. The ground station command queue project in Module 2 connects directly to the telemetry broker you build here.