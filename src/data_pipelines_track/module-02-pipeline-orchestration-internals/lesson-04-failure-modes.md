# Lesson 4 — Failure Modes

**Module:** Data Pipelines — M02: Pipeline Orchestration Internals
**Position:** Lesson 4 of 4
**Source:** *Async Rust* — Maxwell Flitton & Caroline Morton, sections on panics in async tasks and structured shutdown; *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 3 ("Plan for Failure," "Build Loosely Coupled Systems")

> **Source note:** The supervisor pattern as presented here draws on the Erlang/OTP design that influenced subsequent supervised-task systems and is well-documented across many sources beyond the cited texts. The Erlang one-for-one and rest-for-one strategies are not directly cited from a single source; they are general knowledge in the field and adapted here to the SDA orchestrator.

---

## Context

Lesson 1 established the task; Lesson 2 wired tasks into a graph; Lesson 3 made each task's network calls survive transient failures. The pipeline now tolerates the failures that retry can address. This lesson handles the three classes of failure that retry cannot. **Panics**, where an operator hits a `panic!` or an `unwrap()` on `None` and the task is torn down by the runtime. **Cascading slowdowns**, where one operator's degradation propagates through the topology in ways that look like other operators failing. **Resource exhaustion in shared pools**, where one misbehaving operator starves the rest of the pipeline of file descriptors, connections, or thread time.

The mission framing is concrete. Two months ago, a release rolled out with a new validate operator that called `unwrap()` on the `lookup` result of a thread-local catalog cache. Some keys had been evicted from the cache during a startup race; the unwrap panicked. The pipeline did not crash; the panicking task was simply removed from the runtime, its channels orphaned, its upstream blocked on a full channel and its downstream starved of input. For the next four minutes, conjunction alerts were emitted, but they were emitted on the post-validate stream from observations that had skipped the validate stage. Two of those alerts later turned out to be false positives that should have been filtered. The postmortem traced the problem to a missing supervisor — there was nothing watching the validate task and restarting it.

The discipline this lesson installs is **explicit failure-mode management**. The supervisor pattern detects an operator's exit, classifies it, and applies a policy (restart, escalate, or fail). Bulkheading separates resources so one operator's failure cannot starve the others. Circuit breakers provide local fail-fast behavior for repeatedly-failing downstreams. Together, these patterns turn the pipeline from "runs until something panics" into "runs through panics with documented recovery semantics." Reis & Housley's framing is direct: planning for failure is what distinguishes a system that operates from a system that runs.

---

## Core Concepts

### Panic vs Error in Tokio Tasks

A future returns errors via `Result`. A future *panics* when its execution hits a runtime panic — an `unwrap` on a `None`, an out-of-bounds index, a division by zero, an explicit `panic!` macro. Tokio captures the panic at the task boundary and surfaces it through the task's `JoinHandle`: awaiting the handle returns `Err(JoinError)` where `is_panic()` is true. The runtime continues running; only the panicked task is torn down. The application's view of the panic depends entirely on whether anyone is awaiting the handle. If the handle was detached (dropped), the panic is silent — logged at most, and the application has no recovery hook.

The corollary is structural: **the orchestrator must own every operator's `JoinHandle` and join on it**. Detached operators that panic disappear with no signal beyond a log line. The `Task` wrapper from Lesson 1 keeps the handle owned; the supervisor in this lesson awaits each handle through `JoinSet::join_next` and dispatches on the four-case `TaskExit` enum (`Ok`, `Errored`, `Panicked`, `Aborted`). The four cases call for different actions. A panic is a programming bug that should be alerted on, not silently retried — the same panic at the same code path will recur on restart, and an unbounded restart loop on a panic burns CPU without making progress. An error is a runtime condition (a Permanent retry-disposition that propagated past the wrapper) that may or may not warrant restart depending on policy. An abort is a deliberate shutdown signal from the orchestrator. An ok-exit means the operator's input ended cleanly, which is normal at end-of-stream.

The default Tokio behavior on a panic in a task is "ignore" — the runtime captures the panic, marks the handle as failed, and continues. There is a runtime configuration to abort the process on any task panic (`unhandled_panic = Shutdown`); we do not enable it. A pipeline that crashes on any operator panic is more brittle than one that supervises them. The supervisor decides the response; the runtime's job is to surface the panic, not to act on it.

### The Supervisor Pattern

A supervisor is a parent task that owns the `JoinHandle`s of its children, watches for any of them to exit, and applies a policy on exit. The pattern is the same one that made Erlang's OTP framework famous: structured concurrency with explicit failure dispatch. The SDA orchestrator's supervisor is a loop over `JoinSet::join_next` with a match on the resulting `TaskExit`.

The policy decides the response. **Restart** spawns a new instance of the operator (using its registered factory closure), inserting the new `Task` into the JoinSet. **Escalate** propagates the failure upward — in our single-supervisor design, this means tearing down the entire pipeline. **Ignore** lets the operator stay dead and the pipeline continues with the missing stage; rarely the right answer for SDA but useful for purely-observational sidecars (a metrics exporter, a debug logger). The policy is per-operator and registered when the operator is added to the graph.

A naive "always restart" policy is dangerous because it does not bound the restart rate. An operator that panics on its first input panics again on the same input after restart, and again, and again — a tight restart loop that burns CPU and produces a flood of metrics noise without ever making progress. The right shape is a **bounded restart budget**: at most N restarts within a time window W. Past the budget, the supervisor escalates. Erlang's documented rule of thumb is "5 restarts in 60 seconds before escalation"; the SDA orchestrator uses the same default, configurable per-operator. The policy is in `RestartPolicy::Bounded { max_restarts, window }` from Lesson 1's `Task` wrapper.

### Bulkheading

A bulkhead is a physical partition that prevents flooding from spreading between compartments of a ship; in software, it is a separation of resources so that a failure or resource exhaustion in one part of the system cannot affect other parts. Three levels are useful for the SDA orchestrator.

**Channel-level bulkheading** is what we already have: every operator pair is connected by its own bounded channel, so a slow downstream applies backpressure only to its direct upstream, not to the entire pipeline. A misbehaving correlator does not block the audit sink; the audit sink reads from a different channel that is unaffected.

**Runtime-level bulkheading** separates the Tokio runtime workers used by different sets of operators. The default Tokio runtime has a shared worker pool; a CPU-bound operator (placed on `spawn_blocking`, not `spawn`) holds a blocking-pool thread while it runs. The blocking pool's default size of 512 is sized for the assumption that blocking work is incidental, not the steady-state workload. For SDA, the orbital propagator runs a steady stream of CPU-bound calls; if it shares the blocking pool with file I/O on the audit sink, the propagator can starve the audit sink of pool slots during a fragmentation-event surge. The fix is to give the propagator its own runtime (a dedicated `tokio::runtime::Runtime` with a separate blocking pool) and have the propagator operator submit to that runtime instead. This is more complex than channel-level bulkheading and reserved for operators where the resource isolation is needed.

**Process-level bulkheading** is out of scope for this track but worth naming. Production deployments of the SDA pipeline run different stages in different processes (or even different hosts) so that an OOM in the correlator does not kill the radar source. The separation is bought via Kafka topics between stages, which Module 5 covers in depth.

### Cascading Failures

A cascading failure is when one operator's degradation produces failure-like symptoms in unrelated operators that are not themselves degrading. Three common shapes.

**Backpressure-induced timeouts**: a slow correlator's full channel suspends the upstream normalize, which suspends its upstream radar source's send. The normalize and the source are not failing, but their per-event latency goes up. If a downstream operator (an alert emitter that consumes from after the correlator) has a deadline that exceeded its budget, it times out — and the timeout looks like the alert emitter is broken even though the cause is the correlator's slowness. The correct response is not to retry the alert emitter (which makes the situation worse by adding more requests to a slow downstream) but to address the correlator.

**Resource-exhaustion cascades**: an operator with a leaked file descriptor hits the process's FD limit. Subsequent operators that try to open files (the audit sink writing to disk; the optical poller opening TCP connections) fail with EMFILE. The fault propagates with no obvious connection to its source.

**Retry-storm cascades**: an operator's retry policy (Lesson 3) is misconfigured with no jitter. A downstream blip triggers synchronized retries across a fleet of operator instances, which prevent the downstream from recovering, which extends the blip into an outage, which extends the retry storm. The original blip was 200 ms; the cascade is hours.

The architectural response to cascades is to address the cause, not the symptom. Backpressure-induced timeouts are diagnosed by tracing per-stage occupancy and processing latency together; a full channel upstream of the timing-out operator is the smoking gun. Resource exhaustion is diagnosed by tracking FD counts, connection counts, blocking-pool slot counts as Prometheus gauges. Retry storms are prevented by jitter (already done in Lesson 3) and detected by retry-rate metrics. Module 6 builds this observability tooling out fully.

### Circuit Breakers

A circuit breaker is a local pattern for protecting a downstream from a fleet of operator instances all hammering it during a failure. The breaker has three states. **Closed**: the breaker passes calls through normally and tracks the failure rate. **Open**: the breaker has observed too many failures recently and rejects calls immediately without making the downstream call at all. **Half-open**: after a cooldown, the breaker lets a single call through to test if the downstream has recovered; on success, it closes; on failure, it opens again.

The breaker complements the retry wrapper from Lesson 3. Retry handles individual call failures; the breaker handles patterns of failure. Together they produce a system that handles brief blips with a few retries, sustained outages with the breaker opening to spare the downstream from amplification, and recovery with the breaker probing to detect the downstream's return without flooding it.

The breaker's tuning is operational. The trip threshold (failures-per-window before opening) is set high enough that brief blips do not trip but low enough that genuine outages do. The cooldown (time spent open before going to half-open) is set to the longest expected recovery time; too short causes thrash, too long delays recovery detection. The half-open success criterion is one call usually; some implementations use a small batch with a quorum requirement. The SDA orchestrator's default is a 50% failure rate over a 30-second window to trip, 30-second cooldown, single-call probe — sized to match the downstream's typical recovery characteristics and adjustable per-downstream.

---

## Code Examples

### A Supervisor with Bounded Restart Budget

The supervisor loops over `JoinSet::join_next`, dispatching on the `TaskExit` enum from Lesson 1, applying the per-operator restart policy, and escalating when the budget is exhausted.

```rust,ignore
use anyhow::Result;
use std::collections::HashMap;
use std::time::Instant;
use tokio::task::JoinSet;

pub struct Supervisor {
    /// In-flight operator tasks by name. JoinSet drives the watch loop.
    set: JoinSet<(String, TaskExit)>,
    /// Per-operator restart history, used for budget enforcement.
    /// Each entry is the timestamps of recent restarts.
    restart_history: HashMap<String, Vec<Instant>>,
    /// Per-operator factories so the supervisor can respawn.
    factories: HashMap<String, OperatorFactory>,
    policies: HashMap<String, RestartPolicy>,
}

pub type OperatorFactory = Box<dyn FnMut() -> OperatorFuture + Send>;

#[derive(Debug)]
pub enum SupervisorEvent {
    /// An operator panicked; not retried. Pipeline should be torn down.
    Panicked { name: String, message: String },
    /// An operator exhausted its restart budget; pipeline should be torn down.
    BudgetExhausted { name: String },
    /// All operators exited cleanly; pipeline shut down normally.
    AllOk,
}

impl Supervisor {
    /// Run the supervisor loop. Returns when all operators have exited
    /// or when one fails in a non-recoverable way.
    pub async fn run(&mut self) -> SupervisorEvent {
        loop {
            match self.set.join_next().await {
                None => return SupervisorEvent::AllOk,
                Some(Ok((name, TaskExit::Ok))) => {
                    // End-of-stream from one operator. Other operators may
                    // still be running; let them finish naturally.
                    tracing::info!(operator = %name, "operator exited cleanly");
                }
                Some(Ok((name, TaskExit::Panicked(msg)))) => {
                    // Panic is a programming bug. Do not restart; escalate.
                    return SupervisorEvent::Panicked { name, message: msg };
                }
                Some(Ok((name, TaskExit::Errored(e)))) => {
                    // An operator returned Err. Apply its restart policy.
                    let policy = self.policies.get(&name).copied()
                        .unwrap_or(RestartPolicy::Never);
                    if !self.try_restart(&name, policy) {
                        return SupervisorEvent::BudgetExhausted { name };
                    }
                }
                Some(Ok((name, TaskExit::Aborted))) => {
                    // Aborted by the orchestrator. Not a failure.
                    tracing::info!(operator = %name, "operator aborted by orchestrator");
                }
                Some(Err(join_err)) => {
                    // join_next returned a JoinError directly — abnormal.
                    tracing::error!(?join_err, "join_next produced unexpected JoinError");
                }
            }
        }
    }

    fn try_restart(&mut self, name: &str, policy: RestartPolicy) -> bool {
        match policy {
            RestartPolicy::Never => false,
            RestartPolicy::Always => {
                self.respawn(name);
                true
            }
            RestartPolicy::Bounded { max_restarts, window } => {
                let history = self.restart_history.entry(name.to_string()).or_default();
                let cutoff = Instant::now() - window;
                history.retain(|ts| *ts >= cutoff);
                if history.len() >= max_restarts as usize {
                    tracing::error!(operator = %name, "restart budget exhausted");
                    return false;
                }
                history.push(Instant::now());
                self.respawn(name);
                true
            }
        }
    }

    fn respawn(&mut self, name: &str) {
        if let Some(factory) = self.factories.get_mut(name) {
            let future = factory();
            let name_owned = name.to_string();
            self.set.spawn(async move {
                let result = future.await;
                let exit = match result {
                    Ok(()) => TaskExit::Ok,
                    Err(e) => TaskExit::Errored(e),
                };
                (name_owned, exit)
            });
            tracing::info!(operator = %name, "operator restarted");
        }
    }
}
```

The supervisor is the type that the rest of the orchestrator hangs on. Its key property is structural: every spawned task's exit funnels through `join_next`, so every panic, error, abort, and clean exit is observed and dispatched on. There are no detached tasks in the system the supervisor knows about; if one is added by accident (a `tokio::spawn` somewhere outside the supervisor's view), it is a structural bug. The restart budget enforcement is straightforward: keep a sliding window of restart timestamps per operator, evict expired entries on each restart attempt, escalate when the budget is exhausted. The escalation just exits the supervisor's run loop with `BudgetExhausted`; the caller is the orchestrator's top-level entrypoint, which decides whether to abort the rest of the pipeline or whether to retry the supervisor itself with a longer cooldown — usually the former.

### A Circuit Breaker for the Optical Archive

The breaker wraps calls to the flaky downstream. It tracks recent failures and trips when the failure rate exceeds a threshold; while open, calls return `Err` immediately without making the downstream call. After the cooldown, it lets a single probe through.

```rust,ignore
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
enum BreakerState {
    Closed,
    Open { opened_at: Instant },
    HalfOpen,
}

pub struct CircuitBreaker {
    state: Mutex<BreakerState>,
    /// Window of (timestamp, was_failure) tuples for failure-rate calc.
    history: Mutex<Vec<(Instant, bool)>>,
    threshold: f32,    // e.g., 0.5 = trip on 50% failures
    window: Duration,
    cooldown: Duration,
}

impl CircuitBreaker {
    pub fn new(threshold: f32, window: Duration, cooldown: Duration) -> Self {
        Self {
            state: Mutex::new(BreakerState::Closed),
            history: Mutex::new(Vec::new()),
            threshold,
            window,
            cooldown,
        }
    }

    /// Returns true if the call should be allowed through. False means
    /// the breaker is open and the caller should fail-fast without
    /// touching the downstream.
    pub fn allow(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        match *state {
            BreakerState::Closed => true,
            BreakerState::HalfOpen => {
                // Already testing; do not allow concurrent probes.
                false
            }
            BreakerState::Open { opened_at } => {
                if opened_at.elapsed() >= self.cooldown {
                    *state = BreakerState::HalfOpen;
                    true
                } else {
                    false
                }
            }
        }
    }

    pub fn record_outcome(&self, was_failure: bool) {
        let now = Instant::now();
        let mut history = self.history.lock().unwrap();
        history.push((now, was_failure));
        // Drop entries outside the window.
        let cutoff = now - self.window;
        history.retain(|(ts, _)| *ts >= cutoff);
        let total = history.len();
        let failures = history.iter().filter(|(_, f)| *f).count();
        let rate = failures as f32 / total.max(1) as f32;
        let mut state = self.state.lock().unwrap();
        match *state {
            BreakerState::HalfOpen => {
                if was_failure {
                    *state = BreakerState::Open { opened_at: now };
                } else {
                    *state = BreakerState::Closed;
                    history.clear();
                }
            }
            BreakerState::Closed => {
                if total >= 5 && rate >= self.threshold {
                    *state = BreakerState::Open { opened_at: now };
                }
            }
            BreakerState::Open { .. } => {
                // Outcome from a stale in-flight call; ignore.
            }
        }
    }
}
```

The breaker integrates with the retry wrapper from Lesson 3 by gating the retry attempt: if `breaker.allow()` returns false, the retry attempt is short-circuited and the wrapper continues to the next backoff. The combination produces the layered response the lesson promises: individual transient failures are retried; sustained failure patterns trip the breaker, sparing the downstream from a fleet of synchronized retries; recovery is detected by the half-open probe without flooding the downstream. The minimum-call threshold (`total >= 5`) prevents the breaker from tripping on a single early failure during operator startup, when the failure rate is mathematically high but the sample is too small to be meaningful.

### Bulkheading the CPU-Bound Propagator

A dedicated runtime for the propagator isolates it from the main async runtime's blocking pool. The propagator submits to its own pool; the rest of the pipeline submits to the default. A starvation in one does not affect the other.

```rust,ignore
use std::sync::Arc;
use tokio::runtime::Runtime;

pub struct PropagatorPool {
    runtime: Arc<Runtime>,
}

impl PropagatorPool {
    /// Build a dedicated runtime for CPU-bound propagation. The pool size
    /// is documented per-deployment based on expected propagation rate.
    pub fn new(blocking_threads: usize) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)         // small async pool for handle wiring
            .max_blocking_threads(blocking_threads)
            .thread_name("propagator")
            .enable_all()
            .build()
            .expect("propagator runtime build");
        Self { runtime: Arc::new(runtime) }
    }

    /// Submit propagation work. The closure runs on the propagator's
    /// blocking pool, completely isolated from the main runtime's pool.
    pub async fn propagate(&self, obs: Observation) -> Result<PropagatedObservation> {
        let runtime = self.runtime.clone();
        let handle = runtime.spawn_blocking(move || {
            orbital::propagate(obs.target, obs.sensor_timestamp)
        });
        let propagated = handle.await??;
        Ok(PropagatedObservation { obs, propagated })
    }
}
```

The cost is that the propagator now lives behind an `Arc<Runtime>` rather than directly in the main runtime, and the pipeline graph has to pass the `PropagatorPool` to operators that need it. The benefit is operational isolation: a propagator surge that blocks every blocking thread it has does not touch the audit sink's blocking-pool needs. Channel-level bulkheading would not have addressed this; the audit sink and propagator share a different resource (blocking pool slots) than the channels that connect them, and the channel between them is irrelevant to the starvation. This is the case the lesson called out for runtime-level bulkheading.

---

## Key Takeaways

* **Panics surface through `JoinHandle` as `Err(JoinError)` with `is_panic()` true**. The orchestrator must own every operator's handle; detached tasks that panic disappear silently. The `Task` wrapper from Lesson 1 plus `JoinSet::join_next` is the structural mechanism.
* The **supervisor pattern** dispatches on `TaskExit`. Restart on errors per the operator's `RestartPolicy`; escalate on panics (do not retry programming bugs); ignore aborts (deliberate shutdown). Always use a **bounded restart budget** — Erlang's "5 in 60 seconds" is a sensible default — and escalate on budget exhaustion.
* **Channel-level bulkheading** is already provided by per-operator bounded channels. **Runtime-level bulkheading** separates blocking pools for resource-isolated operators (the propagator with its own `Runtime`). Process-level bulkheading is for Module 5 and beyond.
* **Cascading failures** are diagnosed by recognizing that the failing-looking operator is downstream of the actual cause. Address the cause, not the symptom — retrying a timed-out alert emitter does not fix a slow correlator. Module 6's observability tooling makes the cause visible.
* **Circuit breakers** complement retries: retries handle individual failures, breakers handle failure patterns. Closed → Open on threshold breach → Half-Open after cooldown → probe → Closed or back to Open. The combination prevents synchronized-retry amplification of downstream outages.

---

{{#quiz lesson-04-quiz.toml}}
