# Module 04 Project — Ground Station Failover

## Mission Brief

> **Incident ticket CN-2702-003**
> **Severity:** P2
> **Reporter:** Constellation Operations, Atlantic Watch
> **Status:** Open

The Atlantic ground station primary uplink failed at 11:23Z. The backup uplink should have taken over within 30 seconds. Instead, the failover procedure stalled for 8 minutes because: (1) the failure detector took 4 minutes to declare the primary dead (the detector was tuned for cross-region paths, not the Atlantic intra-region path); (2) the circuit breaker on the satellite-command service was configured with a 50-failure threshold, but at the Atlantic traffic rate that's a full day of failures; (3) when the failover finally completed, a retry storm from the queued commands overloaded the backup uplink for another 5 minutes.

You are building **Ground Station Failover**, a Rust crate that integrates the patterns from this module into an operational failover system: phi accrual failure detection, circuit breakers and bulkheads on outgoing commands, retry budgets with backoff, and a chaos-injection harness for verifying the system works under adversarial conditions.

## Repository Layout

```
ground-station-failover/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── detector.rs         # FixedHeartbeatDetector + PhiAccrualDetector
│   ├── breaker.rs          # CircuitBreaker with Closed/Open/HalfOpen state
│   ├── bulkhead.rs         # Semaphore-based bulkhead
│   ├── client.rs           # ResilientClient composing breaker + bulkhead + timeout + retry
│   ├── failover.rs         # PrimaryBackupFailover: monitors primary, promotes backup
│   └── chaos.rs            # Latency / drop / failure injection harness
├── tests/
│   ├── detection_calibration.rs
│   ├── breaker_state_machine.rs
│   ├── bulkhead_isolation.rs
│   ├── failover_end_to_end.rs
│   └── retry_budget.rs
└── README.md
```

## Required API

```rust,ignore
// detector.rs
pub trait FailureDetector: Send + Sync {
    fn note_heartbeat(&self, node: &str);
    fn suspicion_level(&self, node: &str) -> f64;
    fn is_alive(&self, node: &str, threshold: f64) -> bool;
}

pub struct PhiAccrualDetector { /* ... */ }
pub struct FixedHeartbeatDetector { /* ... */ }

// breaker.rs
pub enum BreakerState { Closed, Open, HalfOpen }

pub struct CircuitBreaker { /* ... */ }
impl CircuitBreaker {
    pub fn new(failure_rate_threshold: f64, min_volume: u32, open_duration: Duration) -> Self;
    pub fn allow(&self) -> Result<(), ()>;
    pub fn note_success(&self);
    pub fn note_failure(&self);
    pub fn state(&self) -> BreakerState;
}

// bulkhead.rs
pub struct Bulkhead { /* ... */ }
impl Bulkhead {
    pub fn new(max_concurrent: usize, acquire_timeout: Duration) -> Self;
    pub async fn execute<F: Future<Output = R>, R>(&self, f: F) -> Result<R>;
}

// client.rs
pub struct ResilientClient { /* ... */ }
impl ResilientClient {
    pub async fn call<F, R, Fut>(&self, op: F) -> Result<R>
    where F: Fn() -> Fut, Fut: Future<Output = Result<R>>;
}

// failover.rs
pub struct PrimaryBackupFailover { /* ... */ }
impl PrimaryBackupFailover {
    pub fn new(primary: ResilientClient, backup: ResilientClient, detector: Arc<dyn FailureDetector>) -> Self;
    pub async fn run(&self);  // monitor + promote on detected failure
    pub fn current_active(&self) -> ActiveTier;
}

pub enum ActiveTier { Primary, Backup }
```

## Acceptance Criteria

- [x] `cargo build --release` completes without warnings under `#![deny(warnings)]`.
- [x] `cargo test --release` passes all integration tests with zero flakes across 50 consecutive runs.
- [x] `cargo clippy -- -D warnings` produces no lints.
- [x] **Detector calibration test:** with synthetic inter-heartbeat times drawn from a normal distribution (mean 50ms, stddev 10ms), the phi accrual detector reports phi < 2 in steady state. After the next heartbeat is 300ms late, phi rises above the configured declare threshold.
- [x] **Detection latency test:** when the primary stops sending heartbeats, the detector declares it dead within 3× the configured base timeout. (For phi accrual: within phi reaching the declare threshold from the calibrated baseline.)
- [x] **Breaker state machine test:** under a sequence of 100 failures, the breaker transitions Closed → Open. After the open duration elapses, a probe transitions Open → HalfOpen. A successful probe transitions HalfOpen → Closed; a failed probe transitions HalfOpen → Open.
- [x] **Rate-based threshold test:** the breaker uses a failure-rate threshold with a minimum-volume floor. With 5 failures out of 5 calls, the breaker does NOT trip (volume below minimum). With 50 failures out of 100 calls, the breaker DOES trip (rate exceeds threshold and volume above minimum).
- [x] **Bulkhead isolation test:** two bulkheads in the same process; one is saturated with slow operations. The other bulkhead's operations continue at normal latency. Test asserts the second bulkhead's p99 latency is within 10% of its baseline.
- [x] **Failover end-to-end test:** with primary configured to fail (all calls error), backup configured to succeed, and detector configured with phi threshold 8: the failover system detects the failure, promotes the backup, and the next 100 commands succeed against the backup with no errors propagated to the caller.
- [x] **Retry budget test:** under 50% downstream failure rate, the total retry-attempt count is bounded to 10% of the base call rate. The retry-budget mechanism rejects retries that would exceed the budget rather than amplifying load.
- [x] **Chaos injection harness:** the `chaos` module provides programmatic injection of latency, error responses, and dropped requests, with dynamic configuration (the chaos can be turned on, ramped, and turned off during a test).
- [ ] *(self-assessed)* The README explains the failure modes the system is designed to handle and explicitly names the failure modes it is NOT designed to handle (e.g., Byzantine failures, data corruption, deliberate adversarial behavior).
- [ ] *(self-assessed)* The phi accrual implementation is verified against a published reference; if it deviates, the deviations are documented and justified.
- [ ] *(self-assessed)* The retry-budget mechanism uses a sliding window (not a fixed bucket) and the window size is configurable. The default is documented and justified.

## Expected Output

`cargo test --release failover_end_to_end -- --nocapture`:

```text
[t=0.000s] Failover system initialized; primary=Atlantic-A, backup=Atlantic-B
[t=0.000s] Detector: phi-accrual, declare threshold=8.0
[t=0.000s] Initial state: PRIMARY active
[t=0.050s] Primary heartbeats: arriving at 50ms intervals, phi=0.4
[t=2.000s] Chaos: Atlantic-A enters failure mode (all calls error)
[t=2.050s] Primary heartbeats: STOPPED
[t=2.450s] Detector: phi=3.1 (suspected)
[t=2.700s] Detector: phi=6.4 (suspected, approaching declare)
[t=2.850s] Detector: phi=8.3 (DECLARED dead)
[t=2.851s] Failover: promoting backup Atlantic-B
[t=2.852s] Failover: state=BACKUP active
[t=2.852s] Subsequent 100 commands directed to backup
[t=4.852s] All 100 commands succeeded against Atlantic-B
PASS: failover completed in 852ms; no caller-visible errors after promotion
```

## Hints

<details>
<summary>1. Detector tuning is workload-specific</summary>

The phi accrual paper gives example thresholds (typically 8.0), but the right value for the constellation is empirical. Run the detector against synthetic inter-heartbeat distributions matching your target environment, observe the phi distribution under normal and degraded conditions, and pick a threshold that separates them with a comfortable margin. Treat the chosen threshold as configuration, not a hardcoded constant.

</details>

<details>
<summary>2. Rate-based vs count-based breaker thresholds</summary>

Count-based thresholds ("trip after 50 failures") don't generalize across traffic levels. A high-traffic dependency hits 50 failures quickly during normal operation; a low-traffic one might never accumulate 50 failures even during sustained outages. Use rate-based thresholds ("50% failure rate over a 10-second window with minimum 20 calls") and the minimum-volume floor prevents flapping at low traffic.

</details>

<details>
<summary>3. Bulkhead acquire-timeout vs queue-depth</summary>

A bulkhead can either queue requests (calls wait for a permit) or reject (calls fail when no permit is available). Queueing introduces unpredictable latency; rejection produces clearer error signals. Most production bulkheads queue with an acquire timeout: calls wait up to T for a permit, then fail. This is the shape the project's `Bulkhead::execute` takes.

</details>

<details>
<summary>4. Retry budget mechanics</summary>

The retry budget is typically implemented as a sliding-window counter. Track (base_calls, retry_calls) over a fixed window (e.g., 10 seconds). When considering a retry: if retry_calls / base_calls would exceed the budget (e.g., 0.1), reject the retry. This caps retry volume regardless of the failure rate. The counters can be approximate (atomic counters) for performance; exact accounting under contention is more expensive than the value it provides.

</details>

<details>
<summary>5. Wiring the chaos module to tests</summary>

The chaos module's API should be programmatic: `chaos.set_latency(Duration::from_millis(200), 0.05)` to inject 200ms latency on 5% of calls. The chaos handle is held by the test and dropped at end, disabling the injection. This makes tests self-contained — no global state, no order-dependent interactions between tests. Each test configures its own chaos and tears it down.

</details>

<details>
<summary>6. Integration vs unit tests</summary>

Each module should have unit tests for its individual behavior. The integration tests verify composition: detector + breaker + bulkhead + retry + failover working together. The composition tests are where the subtle bugs hide — for example, a breaker that trips on bulkhead rejection (incorrect: bulkhead rejection isn't a downstream failure, it's a local resource decision) or a retry that doesn't decrement the budget on success (incorrect: the budget should count attempts, not just failures).

</details>

## Source Anchors

- Michael Nygard, *Release It!* (2nd ed, Pragmatic Bookshelf, 2018) — the canonical reference for circuit breaker, bulkhead, timeout, and retry patterns
- Hayashibara et al., "The φ Accrual Failure Detector" (SRDS 2004) — the phi accrual paper
- Casey Rosenthal & Nora Jones, *Chaos Engineering: System Resiliency in Practice* (O'Reilly, 2020) — for the chaos injection module's design
- DDIA 2nd Edition, Chapter 9 — failure detection and "Degraded performance and partial functionality"
