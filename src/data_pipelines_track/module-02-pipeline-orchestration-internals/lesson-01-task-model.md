# Lesson 1 — The Task Model

**Module:** Data Pipelines — M02: Pipeline Orchestration Internals
**Position:** Lesson 1 of 4
**Source:** *Async Rust* — Maxwell Flitton & Caroline Morton, the Tokio runtime and tasks chapters (`tokio::spawn`, `JoinHandle`, `spawn_blocking`, cancellation via `select!`); *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 2 ("The Data Engineering Lifecycle: Orchestration as an Undercurrent")

---

## Context

Module 1 stood up a working pipeline by spawning tasks ad hoc — one for each radar source, one for each optical poller, one normalize stage, one channel sink. The wiring code was a single function in `main.rs` that grew thirty lines longer every time a new operator was added. That approach scales until it doesn't, and it has stopped scaling. The next quarter's roadmap adds five more sensor sources, a cross-sensor correlator, a windowed dedup stage, an alert emitter, and an audit sink. Hand-spawning that topology produces a function nobody wants to touch. We need an orchestrator: a layer that accepts a *description* of the pipeline and runs it, deciding what to spawn where, when to restart, and how to shut down cleanly.

Before we can build the orchestrator, we need a precise account of what it is orchestrating. The unit of orchestration is the **task** — an async function spawned onto the Tokio runtime, paired with a `JoinHandle` for lifecycle reference. Most engineers writing async Rust have spawned tasks; far fewer have a working mental model of what the runtime guarantees, where the cooperative-scheduling contract breaks, and what cancel-safety actually means in practice. This lesson establishes that vocabulary. The next three lessons (DAG scheduling, retries and idempotency, failure modes) each build on it.

The Reis & Housley framing is useful here: orchestration is one of the "undercurrents" of the data engineering lifecycle — present everywhere, easy to ignore until it breaks. *Fundamentals of Data Engineering* makes the point that orchestration is not a service you call out to; it is the connective tissue of the whole system. That framing matters because it shapes what the orchestrator is responsible for. Not "scheduling jobs" in the Airflow sense — the SDA Fusion Service is one long-running pipeline, not a graph of nightly batch jobs. Orchestration here means: own the lifecycle of every operator in the pipeline, supervise it, and present a single coherent abstraction to the rest of the program.

---

## Core Concepts

### Tasks as the Unit of Work

A task is an async function that has been handed to the Tokio runtime to drive. The runtime owns scheduling — picking which task to poll next, which worker thread to poll it on, and when to suspend a task that is awaiting an unready future. The application owns lifecycle reference through the `JoinHandle` returned by `tokio::spawn`. This split is the thing that makes async Rust productive at scale: the runtime is in charge of *when* work happens; the application is in charge of *what* work happens.

A task is much cheaper than an OS thread. A thread carries an OS-level kernel stack (typically 8 MiB on Linux, allocated lazily but reserved in virtual address space), a kernel-scheduled execution context, and the per-thread bookkeeping the kernel maintains. A task is a single allocation containing the future plus a small header — usually under a kilobyte. Spawning a million tasks in a single process is routine; spawning a million OS threads is not. This cheapness is why streaming pipelines are naturally expressed as tasks-per-operator: we can afford one task per source, one per stage, one per partition replica, without thinking hard about the resource cost.

A pipeline operator is one task per stage instance. The radar source task reads from its UDP socket and forwards observations to the next channel. The normalize task reads from that channel and forwards. Every stage is independently schedulable; the runtime interleaves them across worker threads as their inputs become available. Module 1's `spawn_ingestion_topology` already worked this way; the orchestrator we will build in Lesson 2 makes the pattern explicit and composable.

### CPU-Bound vs IO-Bound Tasks

Tokio is a cooperative scheduler. A task progresses by being polled; it yields control by hitting an `.await` that returns `Pending`. Between awaits, the task holds the worker thread it is running on. If a task spends 200 ms on a CPU-bound computation between awaits, no other task scheduled on that worker thread runs for 200 ms — even if that worker has a thousand pending tasks queued. This is the cooperative-scheduling contract, and violating it does not produce a clean error. It produces tail-latency spikes that look like "the runtime is mysteriously slow."

The rule: **async tasks must yield frequently**. The threshold has hardened around **10 microseconds** as the natural budget per uninterrupted run — small enough that the runtime stays responsive at hundreds of thousands of tasks per second, large enough that you are not yielding inside tight inner loops. When the work between awaits exceeds 10 µs by more than a small factor — let alone 10 ms or 200 ms — the work belongs on a thread, not a task. The mechanism is `tokio::task::spawn_blocking`, which dispatches the closure to a separate thread pool sized for blocking work (default: 512 threads, configurable). The result comes back as a `JoinHandle<T>` you can `.await` from your task, but the work itself does not block the async runtime.

For SDA, the placement decisions are concrete. UDP frame deserialization in the radar source is sub-microsecond — stays in async. JSON deserialization of an optical archive response is single-digit microseconds — stays in async. The orbital propagation library's `propagate(state_vector, dt) -> state_vector` call runs in 1–5 ms per object — `spawn_blocking`. The cross-sensor correlator's covariance update is in the hundreds of microseconds — borderline; benchmark before deciding. When in doubt, measure. When unable to measure, prefer `spawn_blocking` for any computation that touches floating-point matrix algebra or calls into a non-async C library.

### Lifecycle and JoinHandle

`tokio::spawn(future)` returns a `JoinHandle<T>` where `T` is the future's output. The handle is your sole reference to the running task. It carries three operations worth understanding precisely:

* **`.await` on the handle** waits for the task to finish and yields its `Result<T, JoinError>`. The error case captures task panics and aborts; the success case is the future's actual return value.
* **`.abort()`** signals the task to stop. This is *cooperative*: the runtime sets a cancellation flag, and the task observes it the next time it reaches an await point. A task in a tight CPU loop with no awaits ignores `.abort()` indefinitely. This is the same problem that puts CPU-bound work on `spawn_blocking` — and it has the same solution.
* **Drop of the handle** does *not* cancel the task. By default, dropping a `JoinHandle` simply detaches the task; it continues running, and you have lost your reference to it. Detached tasks are a frequent source of operational problems: they outlive the code that spawned them, they accumulate without being supervised, and they hold resources that nobody else can release. The orchestrator must never detach an operator task.

For long-running pipeline operators — the kind that drain a channel and forward to the next — the handle never resolves under normal operation. The task runs as long as the upstream channel produces. "Completion" for an operator means the upstream closed (the source ran out, or the orchestrator triggered shutdown). The handle resolving with `Ok(())` is the shutdown-success signal; resolving with `Err(JoinError)` is a panic signal; never resolving (until aborted) is the steady state.

### Cancel-Safety

A future is **cancel-safe** if dropping it at any await point leaves no observable side effects partially applied. Equivalently: the future has either completed an operation or not started it; there is no third state where a row was half-inserted, a transaction was started but not committed, or a kernel buffer was half-read. Cancel-safety is a property of an *individual await point*, not of a function as a whole. A function with one cancel-safe await and one non-cancel-safe await is non-cancel-safe.

The reason this matters for the orchestrator is that shutdown is implemented by aborting tasks, and aborting a task is effectively dropping its current future. If an operator's inner loop is non-cancel-safe at the await point that gets aborted, the operator leaves the system in an inconsistent state on shutdown. This is the underlying mechanism for "the system runs fine until we deploy a new version and then conjunction alerts go missing for ninety seconds" — the alerts that were in flight got aborted at a non-cancel-safe await.

The Tokio primitives are largely cancel-safe by design: `mpsc::Receiver::recv`, `Notify::notified`, `time::sleep`, `select!` with cancel-safe arms, `UdpSocket::recv_from`. Many third-party crates are not. Database drivers that hold a transaction across an await are usually non-cancel-safe (the transaction stays open with no committer if the future is dropped). HTTP clients that stream a response body are non-cancel-safe at the body-read await. The discipline: when you write or use a non-cancel-safe future, wrap it in `select!` against the cancellation signal and explicitly handle the cancel branch — close the transaction, drop the response, release the lock — before the task exits.

### A Task Abstraction for the Orchestrator

The plain `JoinHandle<Result<()>>` is enough to spawn and abort a task, but the orchestrator needs more. It needs a name (for logs and metrics), a restart policy (Lesson 4 builds this out), and the ability to query whether the task is alive and what failed if it isn't. We wrap the handle:

```rust,ignore
pub struct Task {
    name: String,
    handle: JoinHandle<Result<()>>,
    restart_policy: RestartPolicy,
}
```

This is the type that Lesson 2's DAG scheduler operates over and Lesson 4's supervisor restarts. The wrapper costs almost nothing at runtime — it is three fields next to an already-existing handle — but it gives the orchestrator a uniform handle type for every operator in the topology. Heterogeneous return types (some operators emit `Result<Vec<Observation>>`, some `Result<()>`) are not a concern in practice: every operator the orchestrator manages returns `Result<()>` because it runs forever until shut down, and a non-`()` return value would be discarded anyway. The standardization is on purpose.

---

## Code Examples

### Spawning a CPU-Bound Operator with `spawn_blocking`

The orbital propagator is the canonical CPU-bound operator in the SDA pipeline. Given a state vector and a propagation interval, it integrates the equations of motion forward in time — heavy floating-point work, no I/O, single-digit milliseconds per call. Putting that work in a normal async task is the standard mistake.

```rust,ignore
use anyhow::Result;
use tokio::sync::mpsc;
use tokio::task;

/// The wrong way: a CPU-bound operator in an async task.
/// This blocks the worker thread it runs on for the full duration of
/// `propagate`, starving every other task on that worker.
pub async fn propagate_inline(
    mut input: mpsc::Receiver<Observation>,
    output: mpsc::Sender<PropagatedObservation>,
) -> Result<()> {
    while let Some(obs) = input.recv().await {
        // propagate is 1-5ms of pure CPU. The tokio worker thread
        // running this task is unavailable to anything else for the
        // duration. With 8 worker threads, eight slow propagations
        // in flight stalls the entire runtime until they complete.
        let propagated = orbital::propagate(obs.target, obs.sensor_timestamp);
        output.send(PropagatedObservation { obs, propagated }).await?;
    }
    Ok(())
}

/// The right way: hand the CPU work to the blocking pool, await its
/// JoinHandle from inside the async context. The async worker thread
/// remains free to poll other tasks while the propagation runs.
pub async fn propagate_offloaded(
    mut input: mpsc::Receiver<Observation>,
    output: mpsc::Sender<PropagatedObservation>,
) -> Result<()> {
    while let Some(obs) = input.recv().await {
        // spawn_blocking returns a JoinHandle<T>. Awaiting it suspends
        // *this* task without blocking the worker thread; the runtime
        // is free to poll other operators in the meantime. The closure
        // runs on the blocking pool (default 512 threads).
        let propagated = task::spawn_blocking(move || {
            orbital::propagate(obs.target, obs.sensor_timestamp)
        })
        .await??;  // outer ? for JoinError, inner ? for the Result inside
        output.send(PropagatedObservation { obs, propagated }).await?;
    }
    Ok(())
}
```

The 10-microsecond budget rule says anything past that yields. In practice, the threshold worth respecting is much higher — closer to a millisecond — because the cost of the spawn_blocking dispatch itself (allocating a closure, signaling the blocking pool) is in the single-digit microseconds. Below that, you spend more on the offload than you save. Above it, you lose at least an order of magnitude of throughput by leaving the work inline. The two-version comparison above makes the difference visible in flame graphs: `propagate_inline` shows continuous on-CPU time with no idle worker threads even when input arrives in bursts; `propagate_offloaded` shows CPU on the blocking pool and idle async workers ready to handle the next operator's work.

### A Cancel-Unsafe Operator and How to Fix It

The orchestrator triggers shutdown by aborting operator tasks. If an operator holds a resource across a non-cancel-safe await, that resource is leaked when the task is aborted. The example below is a sink that writes observations to an embedded SQLite — a common pattern for the audit sink — and the bug it has and the fix that resolves it.

```rust,ignore
use anyhow::Result;
use rusqlite::Connection;
use tokio::select;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Cancel-unsafe: the transaction is opened, observations are written
/// inside it, and the commit happens after .await on the channel recv.
/// If the task is aborted while waiting for the next batch, the
/// transaction stays open — and SQLite holds a writer lock the rest
/// of the process cannot release until the connection is dropped.
pub async fn audit_sink_unsafe(
    mut input: mpsc::Receiver<Observation>,
    mut conn: Connection,
) -> Result<()> {
    loop {
        let txn = conn.transaction()?;       // begin
        for _ in 0..1000 {
            let obs = match input.recv().await {  // ← abort point inside txn
                Some(o) => o,
                None => return Ok(()),
            };
            txn.execute(INSERT_OBS_SQL, params![&obs.observation_id])?;
        }
        txn.commit()?;
    }
}

/// Cancel-safe: select! between the channel recv and an explicit
/// cancellation token. The cancel branch closes the transaction
/// before the task exits. Aborts of the task itself become
/// rare — shutdown comes through the token.
pub async fn audit_sink_safe(
    mut input: mpsc::Receiver<Observation>,
    mut conn: Connection,
    shutdown: CancellationToken,
) -> Result<()> {
    loop {
        let txn = conn.transaction()?;
        for _ in 0..1000 {
            select! {
                recv = input.recv() => match recv {
                    Some(obs) => {
                        txn.execute(INSERT_OBS_SQL, params![&obs.observation_id])?;
                    }
                    None => {
                        txn.commit()?;
                        return Ok(());
                    }
                },
                _ = shutdown.cancelled() => {
                    // Explicit unwind: drop the in-flight transaction
                    // before returning. SQLite will roll it back when
                    // the txn binding goes out of scope.
                    drop(txn);
                    return Ok(());
                }
            }
        }
        txn.commit()?;
    }
}
```

Two design points are worth lingering on. First, the fix is not "make the SQLite call cancel-safe" — that is not a property the underlying library can offer. The fix is to wrap the non-cancel-safe await in a `select!` whose other arm is a cancellation signal that the orchestrator controls. The task is now the one that decides how to unwind, on its own terms. Second, the cooperative shutdown via `CancellationToken` makes `JoinHandle::abort` a fallback rather than the primary mechanism. Production orchestrators emit `shutdown.cancel()` first, give every operator a grace window (typically 5–10 seconds) to drain, and only fall back to `.abort()` for operators that have not exited. Lesson 4 returns to this pattern.

### The `Task` Wrapper

This is the type the rest of the orchestrator works with. Heterogeneous operator implementations — a UDP source, an HTTP poller, a windowed correlator — all become `Task` values once spawned, with a uniform interface for the scheduler and supervisor.

```rust,ignore
use anyhow::{Context, Result};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};

/// What the supervisor should do when this task exits.
#[derive(Debug, Clone, Copy)]
pub enum RestartPolicy {
    /// Restart the operator on any non-graceful exit.
    Always,
    /// Restart up to N times within the given window.
    Bounded { max_restarts: u32, window: Duration },
    /// Never restart; failure of this operator should fail the pipeline.
    /// Reserved for operators where data integrity is at stake (e.g.,
    /// a sink whose retry would produce double-writes).
    Never,
}

/// Spawned operator handle. The orchestrator stores one of these per
/// operator in the running topology.
pub struct Task {
    name: String,
    handle: JoinHandle<Result<()>>,
    restart_policy: RestartPolicy,
    spawned_at: Instant,
}

impl Task {
    /// Spawn an operator and wrap its JoinHandle.
    pub fn spawn(
        name: impl Into<String>,
        restart_policy: RestartPolicy,
        future: impl std::future::Future<Output = Result<()>> + Send + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            handle: tokio::spawn(future),
            restart_policy,
            spawned_at: Instant::now(),
        }
    }

    pub fn name(&self) -> &str { &self.name }
    pub fn restart_policy(&self) -> RestartPolicy { self.restart_policy }
    pub fn uptime(&self) -> Duration { self.spawned_at.elapsed() }

    /// True if the task has not yet completed. Cheap; useful in
    /// the supervisor's poll loop.
    pub fn is_alive(&self) -> bool { !self.handle.is_finished() }

    /// Wait for the task to exit. Distinguishes operator-returned
    /// errors from runtime panics so the supervisor can react
    /// differently to each (Lesson 4).
    pub async fn join(self) -> TaskExit {
        match self.handle.await {
            Ok(Ok(())) => TaskExit::Ok,
            Ok(Err(e)) => TaskExit::Errored(e),
            Err(join_err) if join_err.is_panic() => {
                TaskExit::Panicked(format!("{:?}", join_err.into_panic()))
            }
            Err(_) => TaskExit::Aborted,
        }
    }

    /// Cooperative shutdown via the orchestrator's cancellation
    /// token would normally have run first; this is the fallback
    /// for operators that did not honor the token in time.
    pub fn abort(&self) { self.handle.abort(); }
}

#[derive(Debug)]
pub enum TaskExit {
    Ok,
    Errored(anyhow::Error),
    Panicked(String),
    Aborted,
}
```

The `join` method is the single most important part of this type. It collapses Tokio's two-level error reporting (`Result<Result<()>, JoinError>`) into a flat `TaskExit` enum that distinguishes the four operationally meaningful cases. A panicked operator is a bug we want to alert on. An errored operator returned `Err(_)` from its future — it is an expected failure mode (a network partition, a transient API failure) and should be retried per its policy. An aborted operator was shut down by the orchestrator and is not a failure. An `Ok` exit means the operator's input ran out — the source closed cleanly — which is normal at end-of-stream but unusual in steady state. The supervisor in Lesson 4 dispatches on `TaskExit` directly; everything past this lesson assumes the wrapper exists.

---

## Key Takeaways

* A **task** is the unit of work the runtime schedules; a `JoinHandle` is your application's sole reference to it. Drop of the handle detaches rather than cancels, and detached tasks accumulate silently — the orchestrator must own every operator's handle.
* The cooperative-scheduling contract demands tasks yield at sub-millisecond granularity. **CPU-bound work belongs on `spawn_blocking`**, not on `tokio::spawn`. Failure to honor this rule produces tail-latency spikes that look like runtime instability rather than the application bug they are.
* `JoinHandle::abort` is **cooperative**: it sets a flag observed at the next await point. CPU-bound tasks ignore aborts until they yield. Production shutdown is a two-step protocol: signal a `CancellationToken` for cooperative drain, then fall back to `.abort()` for stragglers.
* **Cancel-safety** is a per-await-point property. Non-cancel-safe awaits — database transactions, streaming HTTP bodies, custom locks — must be wrapped in `select!` against an explicit cancellation signal so the operator unwinds on its own terms. Aborting a non-cancel-safe future leaks the resource it held.
* The `Task` wrapper turns Tokio's two-level error reporting into a four-case `TaskExit` enum (`Ok`, `Errored`, `Panicked`, `Aborted`) that the supervisor in Lesson 4 dispatches on. Standardize on `Result<()>` for every operator; standardize on `Task` for every spawned handle.

---

{{#quiz lesson-01-quiz.toml}}
