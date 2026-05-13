# Lesson 2: Circuit Breakers and Bulkheads

## Context

During the September incident, a single misbehaving downstream — the conjunction-prediction service began returning with a P99 latency of 8 seconds instead of the usual 80ms — propagated through the entire Constellation Network in 47 seconds. The chain was straightforward in retrospect: callers held connections open waiting for replies, each new request consumed another connection, the connection pool exhausted, and every other dependency of the calling services started timing out as their thread pools blocked on the exhausted callers. By the time the on-call engineer logged in, the dashboard showed errors propagating through services that had nothing to do with the original failure. A single slow downstream had taken out half the constellation.

The fix is a category of patterns that bound failure rather than transmit it: **circuit breakers**, which cut off calls to a misbehaving downstream before they consume local resources; **bulkheads**, which isolate resource pools so that exhaustion of one pool does not starve others; and **timeouts and retries with budget**, which prevent any individual call from holding resources indefinitely. These patterns are the operational vocabulary of resilient distributed systems. Hystrix popularized them at Netflix in the early 2010s; their lineage traces back to Michael Nygard's *Release It!* (2007), which is still the canonical reference.

This lesson covers the three patterns and the failure modes they address. By the end, you should be able to identify the resource-coupling that allows a single slow dependency to take out a service, choose the right pattern to break that coupling, and recognize the configurations (overly aggressive timeouts, undersized bulkheads, retries that amplify load) that make these patterns produce more incidents than they prevent.

## Core Concepts

### The Resource Coupling Problem

The root cause of the September incident is a class of failure that has nothing to do with the network being unreliable, the clock being unreliable, or replication lag. It is a property of **shared resources**: connections, thread pool slots, file descriptors, memory. When a service makes calls to a downstream that is slow but not failed, each in-flight call holds a connection. Eventually, the local connection pool is exhausted. New requests that needed those connections now also wait, even if they were destined for other downstreams that are healthy.

This is **resource coupling**: the failure of one dependency consumes the resources required for other dependencies' health. The dependency graph in the operations diagram says service A depends on B; the resource coupling says service A's calls to C also fail when B is slow, because A's local thread pool is full of stuck calls to B.

The defenses against resource coupling are structural. You cannot fix it by tuning individual call timeouts (the cost is unbounded retries); you fix it by partitioning resources so failures are isolated.

### The Circuit Breaker Pattern

A **circuit breaker** is a wrapper around a downstream call that maintains state across calls and stops issuing them when the downstream is unhealthy. Modeled on the electrical-circuit breaker: when current exceeds the safe threshold, the breaker trips and stops conducting until manually reset.

The state machine has three states:

**Closed.** Calls pass through to the downstream. The breaker tracks success/failure rates over a rolling window. If the failure rate exceeds a threshold (typical: 50% over 10 seconds with a minimum of 20 calls), the breaker transitions to Open.

**Open.** Calls return immediately with an error — no downstream call is made. After a configurable timeout (typical: 30 seconds), the breaker transitions to Half-Open to test recovery.

**Half-Open.** A single call (or a small probe set) is allowed through. If it succeeds, the breaker transitions back to Closed. If it fails, the breaker returns to Open with the timeout restarted.

The pattern's value is twofold. First, **failing fast**: an Open breaker returns errors in microseconds instead of waiting for the downstream's timeout (often seconds). The caller can fall back to a degraded behavior, cached data, or a useful error message far faster than waiting for the slow downstream to time out. Second, **giving the downstream room to recover**: a service overloaded by retry storms cannot recover under load. Cutting off the retries by tripping the breaker reduces load on the downstream, often enough for it to recover on its own.

The catalog wraps every cross-service call in a circuit breaker. The conjunction-prediction service has its own breaker; the orbital-element-registry has another; the telemetry-ingest pipeline has a third. When one trips, only the calls to that service fail fast; the others continue to operate normally.

### Bulkheads: Isolating Resource Pools

The **bulkhead** pattern takes its name from naval architecture: a ship is divided into compartments, so flooding in one compartment does not sink the whole vessel. Applied to software: separate resource pools per dependency, so that resource exhaustion in one pool does not affect others.

The standard implementation:

- **Per-dependency thread pools.** Each downstream service gets its own thread pool. Calls to service A use pool A; calls to service B use pool B. A slow service A fills pool A but does not consume the threads available for B.
- **Per-dependency connection pools.** Same shape, applied to HTTP connections or database connections. Stuck calls to the slow downstream do not exhaust connections needed for healthy downstreams.
- **Per-tenant queue isolation.** In multi-tenant systems, separate queues per tenant prevent one heavy tenant from starving others.

The cost is resource overhead: N thread pools require N pool memory footprints, and the worst-case total resource budget is N × pool_size rather than a single shared pool. This is acceptable: the alternative is shared pools that can be exhausted by any single misbehaving consumer.

A subtler variant is **semaphore isolation**: instead of dedicated threads, each dependency gets a permit count. A call to dependency A acquires a permit from A's semaphore; if all permits are taken, the call fails fast. Semaphores are cheaper than thread pools (no thread context switching) but provide weaker isolation — a CPU-heavy operation on shared threads still affects other dependencies. Hystrix uses thread pools by default and semaphores for low-latency operations; the choice is workload-specific.

### Timeouts and the Retry Budget

The third leg of the resilience stool is **timeouts with explicit deadline propagation** and **bounded retries**.

Every call should have a timeout. The timeout should be short enough to bound resource consumption (long timeouts hold resources during slow failures) but long enough to accommodate normal latency tail. The standard heuristic is `timeout = p99.9_latency + safety_margin`. Anything tighter generates spurious failures; anything looser fails to bound resource use.

**Deadline propagation** is the related discipline: when service A receives a request with a 5-second deadline and calls service B, the call to B should carry a deadline that is `5 seconds minus elapsed-on-A`. If A has already spent 3 seconds, the call to B should time out in 2 seconds, not 5. Without deadline propagation, deep call chains can do work that has already missed its real deadline — wasting downstream resources on requests the caller has given up on. gRPC supports this natively (`Deadline` header); HTTP requires application-level convention.

**Retries** are dangerous and necessary. Necessary because transient failures (network blips, brief overloads) are real and retrying often succeeds. Dangerous because retries multiply load: a service handling 1000 RPS with 50% failure and 3 retries can produce 4× the request volume on the downstream. During a failure, this is exactly the load profile that prevents the downstream from recovering.

The mitigations:

- **Exponential backoff with jitter.** Don't retry immediately; wait `base * 2^attempt * random()`. Without jitter, simultaneous retries from many callers form synchronized waves that re-hit the downstream simultaneously.
- **Retry budget.** Limit retries to a percentage of base traffic (e.g., 10%). When the failure rate is high enough that retries exceed the budget, retries are dropped. This prevents the retry-amplification cascade.
- **Idempotency.** Every operation that is retried must be idempotent. This is the same point from Module 1 Lesson 1: at-least-once delivery is only safe under idempotency.

The catalog's RPC framework provides timeouts (mandatory), deadline propagation (gRPC-native), and a retry budget (10% of base RPS). Engineers adding new endpoints inherit all three by default; opting out is a code-review discussion.

### Composing Circuit Breakers, Bulkheads, and Timeouts

These patterns layer cleanly:

1. **Timeout** bounds the time any individual call holds a resource.
2. **Bulkhead** bounds the total resource budget per dependency.
3. **Circuit breaker** stops issuing calls to a dependency that has failed enough to trip.
4. **Retry with backoff and budget** handles transient failures without amplifying load.

A well-resilient call to a downstream looks like: enter the bulkhead (semaphore acquire); check the circuit breaker (if open, fail fast); make the call with timeout; if it fails, record the failure for the breaker's state and either retry (within budget) or fail. Each layer addresses a different failure mode.

The cost is implementation complexity: every cross-service call needs all four layers, and getting them right requires non-trivial code. The mitigation is to centralize the resilience layer in a single library or framework: every call goes through the same wrapper, the wrapper provides all four behaviors, and individual endpoints just configure thresholds. The catalog's `ResilientClient` type is this wrapper; it's a few hundred lines of code that protects every cross-service call.

### Anti-Patterns

The patterns are powerful but easy to misconfigure. The anti-patterns:

**Timeouts shorter than dependency p99 latency.** Produces spurious failures during normal operation; the system spends time in retry rather than productive work.

**Timeouts longer than upstream deadlines.** Wastes resources on requests the upstream has already given up on. Deadline propagation prevents this when used correctly.

**Bulkheads sized for steady-state, not burst.** When a dependency spikes briefly, calls back up and exceed the bulkhead. The fix is either larger bulkheads (more resources held idle in steady-state) or a queue with bounded depth (calls fail fast when the queue is full).

**Circuit breakers with thresholds tuned to traffic volume.** A breaker tuned for "trip after 100 failures" will rarely trip in low-traffic dependencies (because total volume is low) and trip too easily in high-traffic ones (because absolute failure count crosses the threshold even when failure rate is normal). The fix is rate-based thresholds with a minimum-volume floor.

**Retries without budget or backoff.** The retry storm pattern: a service degrades, retries multiply load, the degradation worsens, retries multiply more, the service fails completely. The classic example is the AWS S3 outage of February 2017, where retry amplification turned a brief disruption into hours of cascading failure.

The pattern is correct only when configured to the workload. Most operational pain from circuit breakers and bulkheads comes from misconfiguration, not from the patterns themselves.

## Code Examples

### A Simple Circuit Breaker

```rust,edition2021
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState { Closed, Open, HalfOpen }

pub struct CircuitBreaker {
    state: Mutex<State>,
    failure_threshold: u32,
    success_threshold: u32,  // probes needed in half-open to close
    open_duration: Duration,
}

struct State {
    state: BreakerState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    opened_at: Option<Instant>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, success_threshold: u32, open_duration: Duration) -> Self {
        Self {
            state: Mutex::new(State {
                state: BreakerState::Closed,
                consecutive_failures: 0,
                consecutive_successes: 0,
                opened_at: None,
            }),
            failure_threshold,
            success_threshold,
            open_duration,
        }
    }

    /// Called before a downstream call. Returns Ok(()) if the call may proceed,
    /// or Err(()) if the breaker is open and the call should fail fast.
    pub fn allow(&self) -> Result<(), ()> {
        let mut s = self.state.lock().unwrap();
        match s.state {
            BreakerState::Closed => Ok(()),
            BreakerState::HalfOpen => Ok(()),  // probe call
            BreakerState::Open => {
                let elapsed = s.opened_at.map(|t| t.elapsed()).unwrap_or_default();
                if elapsed >= self.open_duration {
                    // Transition to half-open: allow a probe.
                    s.state = BreakerState::HalfOpen;
                    s.consecutive_successes = 0;
                    Ok(())
                } else {
                    Err(())
                }
            }
        }
    }

    /// Called after a successful downstream call.
    pub fn note_success(&self) {
        let mut s = self.state.lock().unwrap();
        s.consecutive_failures = 0;
        match s.state {
            BreakerState::Closed => {}
            BreakerState::HalfOpen => {
                s.consecutive_successes += 1;
                if s.consecutive_successes >= self.success_threshold {
                    s.state = BreakerState::Closed;
                    s.opened_at = None;
                }
            }
            BreakerState::Open => {
                // Shouldn't happen - a call only completes if allow() returned Ok.
            }
        }
    }

    /// Called after a failed downstream call.
    pub fn note_failure(&self) {
        let mut s = self.state.lock().unwrap();
        s.consecutive_successes = 0;
        s.consecutive_failures += 1;
        match s.state {
            BreakerState::Closed => {
                if s.consecutive_failures >= self.failure_threshold {
                    s.state = BreakerState::Open;
                    s.opened_at = Some(Instant::now());
                }
            }
            BreakerState::HalfOpen => {
                // Probe failed - back to open.
                s.state = BreakerState::Open;
                s.opened_at = Some(Instant::now());
            }
            BreakerState::Open => {}
        }
    }

    pub fn current_state(&self) -> BreakerState {
        self.state.lock().unwrap().state
    }
}

fn main() {
    let cb = CircuitBreaker::new(3, 2, Duration::from_secs(30));
    println!("initial: {:?}", cb.current_state());

    // Three consecutive failures trip the breaker.
    cb.note_failure();
    cb.note_failure();
    cb.note_failure();
    println!("after 3 failures: {:?}", cb.current_state());

    // Calls now fail fast.
    assert!(cb.allow().is_err());
}
```

This is the consecutive-failure variant; production breakers typically use a rolling-window failure rate, which is more sophisticated but follows the same state machine.

### Semaphore-Based Bulkhead

```rust,edition2021
use std::sync::Arc;
use tokio::sync::Semaphore;
use std::time::Duration;
use anyhow::Result;

pub struct Bulkhead {
    semaphore: Arc<Semaphore>,
    acquire_timeout: Duration,
}

impl Bulkhead {
    pub fn new(max_concurrent: usize, acquire_timeout: Duration) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            acquire_timeout,
        }
    }

    pub async fn execute<F, R>(&self, f: F) -> Result<R>
    where
        F: std::future::Future<Output = R>,
    {
        // Try to acquire a permit within the timeout. If we can't, the
        // bulkhead is full - fail fast rather than queueing indefinitely.
        let permit = tokio::time::timeout(
            self.acquire_timeout,
            self.semaphore.acquire(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("bulkhead acquire timeout"))?
        .map_err(|_| anyhow::anyhow!("bulkhead semaphore closed"))?;

        let result = f.await;
        drop(permit);
        Ok(result)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let bulkhead = Bulkhead::new(2, Duration::from_millis(100));
    let result = bulkhead.execute(async { "downstream-call-result" }).await?;
    println!("got: {}", result);
    Ok(())
}
```

The semaphore version is the lightest-weight bulkhead; thread-pool bulkheads (one pool per dependency) provide stronger isolation when CPU contention is the concern.

### Composing the Layers

```rust,ignore
// Resilient call combining bulkhead, circuit breaker, timeout, and retry.

use std::time::Duration;
use anyhow::Result;

pub struct ResilientClient {
    bulkhead: Bulkhead,
    breaker: CircuitBreaker,
    call_timeout: Duration,
    max_retries: u32,
}

impl ResilientClient {
    pub async fn call<F, R, Fut>(&self, op: F) -> Result<R>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        // Outer retry loop with exponential backoff.
        for attempt in 0..=self.max_retries {
            // 1. Check the circuit breaker.
            if self.breaker.allow().is_err() {
                anyhow::bail!("circuit breaker open");
            }

            // 2. Enter the bulkhead and execute with timeout.
            let result = self
                .bulkhead
                .execute(tokio::time::timeout(self.call_timeout, op()))
                .await;

            match result {
                Ok(Ok(Ok(r))) => {
                    self.breaker.note_success();
                    return Ok(r);
                }
                Ok(Ok(Err(_))) | Ok(Err(_)) | Err(_) => {
                    self.breaker.note_failure();
                    if attempt < self.max_retries {
                        // Exponential backoff with jitter.
                        let base = Duration::from_millis(100);
                        let backoff = base * 2u32.pow(attempt);
                        let jitter = rand::random::<f64>();
                        tokio::time::sleep(backoff.mul_f64(0.5 + jitter * 0.5)).await;
                    }
                }
            }
        }
        anyhow::bail!("max retries exceeded")
    }
}
# struct Bulkhead;
# impl Bulkhead {
#     async fn execute<F: std::future::Future>(&self, _f: F) -> Result<F::Output> { unimplemented!() }
# }
# struct CircuitBreaker;
# impl CircuitBreaker {
#     fn allow(&self) -> Result<()> { Ok(()) }
#     fn note_success(&self) {}
#     fn note_failure(&self) {}
# }
# mod rand { pub fn random<T>() -> f64 { 0.5 } }
```

The composition is the production shape. Every call to a downstream goes through this layer. The configuration is per-dependency: each downstream has its own ResilientClient instance with thresholds tuned to that dependency's characteristics.

## Key Takeaways

- Resource coupling is the failure mode where one slow dependency exhausts shared resources (connections, threads), preventing healthy work on other dependencies. The structural fix is to partition resources, not to tune individual timeouts.
- Circuit breakers fail fast on calls to misbehaving downstreams, freeing local resources for other work and giving the downstream room to recover. The three-state machine (Closed, Open, Half-Open) handles the recovery probe pattern.
- Bulkheads isolate resource pools per dependency so that exhaustion in one pool does not propagate to others. Thread-pool bulkheads provide stronger isolation; semaphore bulkheads are lighter weight.
- Timeouts, retries with backoff and jitter, and retry budgets are the third leg of the resilience stool. Without backoff, simultaneous retries form synchronized waves; without budget, retries amplify load during failures.
- The patterns layer cleanly: bulkhead, circuit breaker, timeout, retry. Compose them in a centralized resilience library so every cross-service call inherits the protection without per-endpoint work.

> **Source note:** This lesson synthesizes from the Hystrix design (Netflix, 2012, archived), Michael Nygard's *Release It!* (2nd ed, 2018, Pragmatic Bookshelf) — the canonical reference for these patterns — and DDIA Chapter 9's brief treatment of "Degraded performance and partial functionality." Specific operational parameters (10% retry budget, p99.9 + safety margin timeouts) are illustrative and should be calibrated to the workload. The AWS S3 February 2017 outage reference is from the public post-incident report; specific details should be verified before publication. *Foundations of Scalable Systems* was unavailable; the resilience-pattern material would normally cite that text and should be cross-referenced.

{{#quiz quiz-2.toml}}
