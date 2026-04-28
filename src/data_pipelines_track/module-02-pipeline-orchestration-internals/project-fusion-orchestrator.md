# Capstone Project — Fusion Pipeline Orchestrator

**Module:** Data Pipelines — M02: Pipeline Orchestration Internals
**Estimated effort:** 1–2 weeks of focused work
**Prerequisites:** All four lessons in this module passed at ≥70%

---

## Mission Brief

> **OPS DIRECTIVE — SDA-2026-0119 / Phase 2 Implementation**
> **Classification:** ORCHESTRATION TIER STAND-UP
>
> The Phase 1 ingestion service from Module 1 (`sda-ingest`) is in
> production and stable, but the next quarter's roadmap adds five new
> sensor sources, a windowed dedup stage, a cross-sensor correlator, an
> alert emitter, and an audit sink. The current hand-spawned topology in
> `main.rs` is at the practical limit of what one engineer can hold in
> their head. The Phase 1 postmortem also flagged two operational gaps:
> a panicking task is silently torn down with no recovery hook, and the
> retry policy on the optical poller is unjittered fixed-delay (the
> 90-minute outage extension last quarter is the canonical incident).
>
> Phase 2 builds the orchestrator that addresses both gaps. The
> deliverable is a Rust library that accepts a declarative DAG of
> operators, spawns them with their channels correctly wired, supervises
> their lifecycles, and applies retry policy with jitter. The Phase 1
> binary is refactored to use the orchestrator with no behavioral
> regression beyond the documented failure-handling improvements.
>
> Success criteria for Phase 2: the orchestrator handles every Phase 1
> source plus a synthetic 5-source future-load profile; a panic in any
> operator surfaces to operations rather than disappearing; transient
> downstream failures recover via backoff without thundering-herd
> behavior. Failure-isolated subsystems (the new orbital propagator) run
> on a dedicated runtime to avoid blocking-pool contention with the
> rest of the pipeline.

---

## What You're Building

A Rust library crate, `sda-orchestrator`, that exposes:

1. An `OperatorGraph` builder with `add_source`, `add_operator`, `add_sink`, and `connect` methods (Lesson 2)
2. A `BuiltGraph::run(supervisor_policy) -> Future` that spawns the topology with the supervisor (Lesson 4) wired in
3. The `Task` wrapper (Lesson 1), `RetryPolicy` and `with_retry` (Lesson 3), and `CircuitBreaker` (Lesson 4) types as public API
4. Cycle detection and per-role edge validation in `OperatorGraph::build()` with named-operator error messages
5. Bounded-restart-budget supervision with structured logging on every supervisor decision

Plus the refactor of the Phase 1 `sda-ingest` binary:

6. The Module 1 binary is refactored to declare its topology as an `OperatorGraph` rather than hand-spawn it; behavior is preserved end-to-end
7. The optical-archive HTTP poller wraps its requests in `with_retry` using decorrelated-jitter backoff
8. The (new for this module) orbital propagator runs on a dedicated `tokio::runtime::Runtime` for blocking-pool isolation

The deliverable is the library, the refactored binary, the test suite (including a deterministic supervisor-restart test using `tokio::time::pause`), and a 1-page operational README documenting the orchestrator's API and its failure-handling guarantees.

---

## Suggested Architecture

```
                                    OperatorGraph (declarative)
                                              │
                                              │ build()
                                              ▼
   ┌───────────────────────────────────────────────────────────────┐
   │  BuiltGraph: per-edge channels allocated, operators spawned   │
   │             in topological order with their wired ends.       │
   └───────────────────────────────┬───────────────────────────────┘
                                   │ run(policy)
                                   ▼
                          ┌───────────────────┐
                          │    Supervisor     │
                          │  (JoinSet loop)   │
                          └─────────┬─────────┘
                                    │
       ┌────────────────────────────┴────────────────────────────┐
       │                                                         │
       ▼                                                         ▼
   ┌───────┐  ┌────────────┐  ┌──────────┐  ┌──────────────┐  ┌──────┐
   │ radar │→│  normalize │→ │  dedup   │→│   correlator  │→│ sink │
   │  src  │  └────────────┘  └──────────┘  │ (Propagator-  │  │      │
   └───────┘     ↑   ↑                       │  Pool runtime)│  └──────┘
   ┌───────┐     │   │                       └──────────────┘
   │optical│─────┘   │
   │  src  │         │           Each edge: bounded mpsc::channel
   └───────┘         │           Each operator: Task wrapped, supervised
   ┌───────┐         │           Each network call: with_retry + breaker
   │  ISL  │─────────┘
   │  src  │
   └───────┘
```

The orchestrator does not know about `Observation` specifically; it operates on type-erased operator factories that consume and produce arbitrary types. The library is generic in the sense that the application (the SDA binary) wires up its own operator types and topology. Resist the temptation to bake SDA-specific assumptions into the orchestrator crate; it is meant to be reusable across pipelines.

---

## Acceptance Criteria

### Functional Requirements

- [x] `OperatorGraph` exposes `new`, `add_source`, `add_operator`, `add_sink`, `connect`, `build` matching the Lesson 2 signatures
- [x] `OperatorGraph::build()` runs all four passes (per-role validation, Kahn's topo sort with cycle detection, channel allocation, spawn) and produces actionable error messages on validation failures
- [x] `BuiltGraph::run(policy)` drives the supervisor loop and returns a `SupervisorEvent` that distinguishes clean shutdown from panic from budget exhaustion
- [x] `Task::join()` collapses Tokio's two-level error reporting into a `TaskExit` enum (`Ok`, `Errored`, `Panicked`, `Aborted`)
- [x] `RestartPolicy::{Never, Always, Bounded { max_restarts, window }}` is honored by the supervisor
- [x] `with_retry(policy, op)` retries `RetryDisposition::Retry` results with decorrelated-jitter backoff, propagates `Permanent` immediately, and discards `Discard` cleanly
- [x] `CircuitBreaker` implements Closed → Open → HalfOpen transitions with the threshold, window, and cooldown the lesson described; integration with `with_retry` is documented in the API
- [x] The `sda-ingest` binary is refactored to use `OperatorGraph` declaratively; the topology fits in a single `build_topology()` function under 80 lines

### Quality Requirements

- [x] **DAG cycle test**: a unit test attempts to build a graph with a cycle and asserts the error message names the cycle's vertices
- [x] **Disconnection test**: a unit test attempts to build a graph with an unconnected operator and asserts the per-role validation error names the operator and the missing direction
- [x] **Supervisor restart test**: a unit test injects an error from a fake operator, asserts the supervisor restarts it within the budget, then asserts budget exhaustion when the error rate exceeds the policy. Use `tokio::time::pause()` and `advance` for deterministic timing — no flaky `sleep` in tests.
- [x] **Supervisor panic test**: a unit test injects a panic from a fake operator and asserts the supervisor returns `SupervisorEvent::Panicked` without restart attempts
- [x] **Decorrelated-jitter math test**: a unit test fixes the RNG and asserts the per-attempt delays match the documented schedule
- [x] No `.unwrap()` or `.expect()` in non-startup code paths

### Operational Requirements

- [x] HTTP control plane (extending Module 1's): adds `GET /metrics` fields for `operator_restart_total{operator}`, `operator_uptime_seconds{operator}`, `circuit_breaker_state{breaker}` (encoded as 0/1/2 for Closed/Open/HalfOpen), and `retry_attempts_total{operator}`
- [x] Structured log line on every supervisor decision: spawn, restart, budget-exhausted, escalate, clean-exit. JSON formatter, one event per decision (not one per observation).
- [x] The operational README updated for Phase 2: documents the orchestrator API, the new metrics, and the new failure-handling semantics. One-page constraint preserved.

### Self-Assessed Stretch Goals

- [ ] *(self-assessed)* The optical source survives a 30-second downstream outage with no operator restarts and no observation drops at the application layer (the kernel may drop UDP frames during the outage; that is acceptable). Demonstrate via integration test using `wiremock` to simulate the outage.
- [ ] *(self-assessed)* `OperatorGraph::build()` for a 10-operator topology completes in under 100 ms (cold) and under 10 ms (warm). Provide a `criterion` benchmark.
- [ ] *(self-assessed)* The `PropagatorPool` (dedicated runtime for orbital propagation) is demonstrated to be isolated from the main runtime: an artificial 100x propagator load does not affect the main runtime's audit-sink P99 latency. Include the load-test harness.

---

## Hints

<details>

<summary>How should I represent operators in the graph without making it generic over every operator type?</summary>

A boxed factory closure is the cleanest path. The graph stores `Box<dyn FnOnce(WiredEnds) -> OperatorFuture + Send>`, where `OperatorFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>`. The closure is called once `build()` has the channels; it captures whatever the operator needs (config, addresses, references to shared state).

```rust,ignore
type OperatorFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type OperatorFactory = Box<dyn FnOnce(WiredEnds) -> OperatorFuture + Send>;

let radar_factory: OperatorFactory = Box::new(|ends| {
    Box::pin(async move {
        let radar = UdpRadarSource::bind("0.0.0.0:7001", "radar-01").await?;
        let tx = ends.tx.expect("source has tx");
        run_source_loop(radar, tx).await
    })
});
```

This keeps the graph type non-generic at the cost of a heap allocation per operator at build time — negligible for the topology sizes the SDA pipeline reaches.

</details>

<details>

<summary>How do I handle the channel-creation order problem in the topo-sort spawn pass?</summary>

The two-pass structure from the lesson:

```rust,ignore
// Pass 3: allocate every channel up-front.
let mut chan_tx: HashMap<EdgeId, mpsc::Sender<Observation>> = HashMap::new();
let mut chan_rx: HashMap<EdgeId, mpsc::Receiver<Observation>> = HashMap::new();
for e in &self.edges {
    let (tx, rx) = mpsc::channel(e.capacity);
    chan_tx.insert(e.id, tx);
    chan_rx.insert(e.id, rx);
}

// Pass 4: walk in topo order, hand each operator its channel ends.
for idx in topo_order {
    let v = &self.vertices[idx];
    let rx = v.incoming.and_then(|e| chan_rx.remove(&e));
    let tx = v.outgoing.and_then(|e| chan_tx.remove(&e));
    let future = (v.factory)(WiredEnds { rx, tx });
    let task = Task::spawn(&v.name, v.restart_policy, future);
    tasks.push(task);
}
```

The receiver halves are `remove`d (not `get`'d) because each receiver belongs to exactly one operator — moving rather than cloning. The map's emptiness at the end of pass 4 is itself a structural sanity check.

</details>

<details>

<summary>How do I test the supervisor's restart logic without flaky timing?</summary>

`tokio::time::pause()` makes time deterministic in tests: `sleep` and `Instant` are driven by `tokio::time::advance`, not by wall clock. The supervisor's window-based budget can be tested by advancing time forward over the window and observing eviction.

```rust,ignore
#[tokio::test(start_paused = true)]
async fn supervisor_restarts_within_budget() {
    let policy = RestartPolicy::Bounded {
        max_restarts: 3,
        window: Duration::from_secs(60),
    };
    let mut sup = Supervisor::for_test(policy, panic_factory());

    // First failure within budget: should restart.
    sup.simulate_exit(TaskExit::Errored(anyhow!("boom")));
    assert_eq!(sup.restart_count(), 1);

    // Three more failures exhaust the budget.
    for _ in 0..3 {
        sup.simulate_exit(TaskExit::Errored(anyhow!("boom")));
    }
    assert_eq!(sup.event(), SupervisorEvent::BudgetExhausted { .. });

    // Advance past the window; restart history evicts; new failures recover budget.
    tokio::time::advance(Duration::from_secs(61)).await;
    sup.simulate_exit(TaskExit::Errored(anyhow!("boom")));
    assert_eq!(sup.restart_count(), 5);
}
```

The `Supervisor::for_test` constructor and `simulate_exit` methods are testing-only API that you expose with `#[cfg(test)]` or behind a `testing` feature flag. The principle: the supervisor should be testable without spawning actual tasks.

</details>

<details>

<summary>How do I refactor the M1 binary without breaking the integration tests?</summary>

Two-step refactor. First, build the orchestrator library with the test harness asserting the supervisor and graph behavior. Second, refactor the binary in-place, leaving its existing integration tests unmodified — they should pass against the refactored binary because the observable behavior is unchanged.

A common temptation is to build a parallel `sda-ingest-v2` binary alongside the original. Resist this; it produces two binaries to maintain. The right approach is a single binary whose internals change. Keep the original integration test suite running on every commit during the refactor.

</details>

<details>

<summary>What restart policy should I default to per operator?</summary>

The defaults that match SDA's operational stance:

- **Sources** (radar, optical, ISL): `Bounded { max_restarts: 5, window: Duration::from_secs(60) }`. Sources are the most external part of the pipeline; they are most likely to encounter transient external failures (a network blip, a partner deploy). Restart-with-budget is the right shape.
- **Stateless operators** (normalize, validate): `Bounded { max_restarts: 5, window: Duration::from_secs(60) }`. Same defaults as sources — they have no state to corrupt and restart is cheap.
- **Stateful operators** (dedup, correlator): `Bounded { max_restarts: 3, window: Duration::from_secs(60) }` — fewer restarts because state loss on each restart costs more, and a higher recurrence rate suggests a deeper problem.
- **Sinks where data integrity is at stake** (audit log, alert emitter): `Never`. A retry on these may produce duplicate emits to downstream subscribers; better to fail the pipeline loudly. Module 5's idempotent-sink machinery will let you change this default later.

Document the choice per-operator in the topology declaration with a one-line comment explaining the reasoning.

</details>

---

## Getting Started

Recommended order:

1. **`Task` wrapper.** Define `Task::spawn`, `Task::join`, `TaskExit`, `RestartPolicy`. Write unit tests that spawn synthetic tasks (success, error, panic) and assert the right `TaskExit` variant.
2. **`OperatorGraph` builder.** Define the builder API and the per-role validation. Write tests for: well-formed graphs build, dangling edges are rejected, role mismatches are rejected.
3. **Topo sort + cycle detection.** Implement Kahn's algorithm in `build()`. Write tests for: linear chains, fan-in, fan-out, and (failing) cycles.
4. **Channel allocation and spawn.** The two-pass structure from the hint. Write an end-to-end test that builds a 3-operator graph and confirms data flows from source to sink.
5. **`Supervisor`.** Wrap the `JoinSet` loop. Write the deterministic-timing tests using `tokio::time::pause`.
6. **`with_retry` and `CircuitBreaker`.** These are independent of the orchestrator; they can be developed and tested in isolation, then wired into the optical source's polling code.
7. **`PropagatorPool`.** The dedicated-runtime wrapper for the orbital propagator. The propagator itself is mocked for this project — the real propagator is from Meridian's `orbital` crate, which is out of scope. A `mock_propagate(state, dt) -> state` function that does a deterministic-but-CPU-bound computation is sufficient.
8. **Refactor `sda-ingest`.** The topology declaration becomes a single `build_topology()` function. Keep the existing integration tests passing.
9. **Operational README.** Document the orchestrator API and the new metrics. One page, terse.

Aim for a working orchestrator and a passing-tests refactor by day 7 even if the operational polish (control-plane metrics, README) is incomplete. The orchestrator's correctness is what matters; the polish is finishing work.

---

## What This Module Sets Up

In Module 3 you will replace this module's processing-time dedup operator with an event-time windowed correlator. The orchestrator interface stays the same; only one operator's implementation changes. The watermark machinery you build there flows along the same channels the orchestrator wired up here.

In Module 4 you will harden the channel boundaries against burst-load failure modes. The bounded-channel-per-edge invariant the orchestrator enforces structurally is what makes that work tractable. You will revisit buffer sizing with rigor.

In Module 5 you will make the windowed operator's state crash-safe via checkpointing. The supervisor's restart machinery you built here is what the checkpoint recovery path hooks into. The `Never` restart policy on data-integrity-critical sinks gets revisited with idempotent-sink tooling that lets it become `Bounded` safely.

The orchestrator is not a throwaway. It is the connective tissue every subsequent module's project hangs on.
