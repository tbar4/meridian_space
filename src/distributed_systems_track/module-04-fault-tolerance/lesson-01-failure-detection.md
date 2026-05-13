# Lesson 1: Failure Detection — Heartbeats, Timeouts, and Phi Accrual

## Context

Every mechanism we have built so far — Raft elections, replication failovers, conflict resolution — depends on a single primitive that we have so far treated as a given: **the ability to suspect that a node has failed**. When a heartbeat is overdue, when an AppendEntries does not return, when a TCP connection hangs in a half-open state, some component of the system must declare "this node is suspected dead" and trigger the appropriate response (a new election, a failover, a circuit breaker trip). This component is the **failure detector**, and its design choices determine the cluster's behavior under partial network degradation.

The failure detector is also the most direct manifestation of the truth from Module 1 Lesson 1: **a timeout does not tell you the remote is dead; it tells you that you stopped waiting**. Choosing the timeout is choosing the false-positive vs false-negative tradeoff. Choosing the detector algorithm is choosing how aggressively the system adapts to changing network conditions. Production systems spend significant engineering effort on this layer because the failure detector is what determines whether the cluster behaves correctly during incidents — and incidents are exactly the time when correctness matters most.

This lesson covers the spectrum of failure detection approaches, from simple fixed-timeout heartbeats to the **phi accrual failure detector** used by Cassandra and Akka. By the end, you should be able to choose a failure detection approach for a given workload, calibrate its timeouts and thresholds against the operational regime, and recognize the failure modes (flapping, false positives, slow detection) that each approach is and is not robust to.

## Core Concepts

### The Spectrum of Failure Detectors

DDIA's discussion of failure detection introduces a useful spectrum (originally from Chandra & Toueg 1996):

**Perfect failure detector.** Suspects exactly the failed processes. Available only with synchronous assumptions that real systems don't have.

**Eventually perfect failure detector.** Eventually suspects exactly the failed processes, but may produce false positives or false negatives temporarily. Achievable under partial synchrony; this is what real systems aim for.

**Eventually strong failure detector.** Eventually suspects all failed processes, and eventually at least one correct process is never suspected. Sufficient for solving consensus.

The takeaway is operational: no real failure detector is perfect. The choice is between approaches that fail in different ways under different conditions. The goal is to pick an approach whose failure modes are tolerable for the workload, not to find one that never fails.

### Fixed-Timeout Heartbeats: The Baseline

The simplest detector: each node sends a heartbeat at interval `H`, and a monitor declares a node dead if no heartbeat arrives within timeout `T` (typically `T = 3 * H`). This is what Raft, etcd, ZooKeeper, and most production systems use as a baseline.

The two parameters interact:

- **H (heartbeat interval).** Shorter means faster detection but more network traffic. For a 5-node cluster with 50ms heartbeats, that's 100 heartbeats per second of cluster-internal traffic just for liveness. For wide-area deployments with 12 ground stations, this becomes meaningful.
- **T (timeout).** Longer means fewer false positives during transient network blips but slower detection of real failures. The 3× rule (`T = 3 * H`) is a reasonable default: it tolerates one or two missed heartbeats from jitter but declares a failure within 3H of the actual event.

The failure modes:

- **False positives under network congestion.** When the network is slow but not failed, heartbeats are delayed past `T`, triggering spurious failure declarations. In Raft, this means spurious elections; in a circuit breaker, this means breaking on healthy services.
- **Slow detection under bursty failure.** A node that fails immediately after sending a heartbeat will not be detected for up to `T` seconds. For services with strict latency budgets, this is too slow.
- **Flapping.** A node that intermittently fails (e.g., a process that periodically GCs for 200ms) repeatedly crosses the threshold, causing alternating "alive" and "suspected" declarations. Each flap may trigger an expensive recovery action; the system spends more time recovering than running.

The constellation network's standard is `H=50ms, T=150ms` for ground-station-to-leader heartbeats within a region, and `H=500ms, T=2s` for cross-region paths. The two settings reflect the different latency regimes; using the same settings everywhere either produces false positives on the cross-region path or slow detection on the intra-region path.

### Adaptive Detectors: Phi Accrual

The fixed-timeout approach assumes a stable distribution of inter-heartbeat times. Production networks aren't stable. A detector that uses the same threshold during a midday traffic spike as during an idle period will be wrong in one of those regimes.

**Phi accrual** is the technique pioneered by Hayashibara et al. (2004) and adopted by Cassandra, Akka, and Apache HBase. Instead of "declare dead after T seconds without heartbeat," phi accrual outputs a continuous suspicion level: the probability that the node is dead, given the inter-heartbeat history.

The algorithm maintains a sliding window of recent inter-heartbeat times, fits a distribution (typically lognormal), and on each tick computes the probability that the current gap since the last heartbeat could arise from the observed distribution. The output is `phi = -log10(P)`, where P is the probability of the current gap. Higher phi = more suspicious. The application chooses a phi threshold above which to declare the node dead.

The advantages:

- **Adapts to network conditions.** If inter-heartbeat times are normally clustered around 50ms with occasional 100ms outliers, the detector tolerates the outliers without flagging. If conditions degrade and 100ms becomes the new normal, the detector recalibrates.
- **Suspicion is continuous.** The application can take graduated action: at phi=3, log a warning; at phi=5, redirect new traffic; at phi=8, declare dead and trigger failover. Different consumers of the suspicion signal can use different thresholds without re-running the detector.
- **Operational tunability.** The phi threshold is a single dimensionless parameter, easier to reason about than a timeout (which interacts with the heartbeat interval).

The disadvantages:

- **Implementation complexity.** Maintaining the sliding window, fitting the distribution, and computing phi efficiently is non-trivial. Several open-source implementations exist (Akka's, Cassandra's, the akka-failure-detector crate); reimplementing for a new system is rarely worth it.
- **Sensitivity to bootstrap.** The detector needs some history before it can produce meaningful phi values. During bootstrap, it falls back to fixed timeouts.
- **Not necessarily appropriate for all workloads.** For workloads where the cost of false positives is high and the cost of slow detection is low (financial trading, for example), a more conservative fixed timeout may be the right call.

The catalog uses fixed-timeout heartbeats inside the Raft consensus layer (because the Raft paper's analysis assumes fixed timeouts, and changing it requires re-deriving the safety properties) and phi accrual for service-to-service liveness (where graduated suspicion drives load balancing and circuit breaker decisions).

### Gossip-Based Failure Detection

For large clusters (hundreds or thousands of nodes), point-to-point heartbeating doesn't scale. With N nodes, every-to-every heartbeating is O(N²) messages per round, which dominates network traffic at large N.

**Gossip protocols** (covered in detail in Module 5) provide a scalable alternative: each node tracks failure information for a small subset of peers, exchanges information with random peers periodically, and the cluster-wide view emerges via epidemic propagation. The SWIM protocol (Das, Gupta, Motivala 2002) is the canonical reference, and Hashicorp's Serf is a widely-deployed implementation.

The tradeoff is delayed propagation: information about a failure takes O(log N) gossip rounds to reach the entire cluster, where N is the cluster size. For latency-critical detection (a Raft cluster), this is too slow. For broad cluster-membership awareness (Cassandra-style "which nodes are up"), it's the right shape.

The constellation's 48-satellite + 12-ground-station deployment is small enough that point-to-point heartbeating is feasible, but the architecture team chose gossip for the data plane (telemetry distribution) on the basis that it scales to future expansion without rearchitecting.

### Failure Suspicion vs Failure Declaration

A subtle distinction worth naming: **suspicion** (a probability the node may be dead) is different from **declaration** (a decision to act as if the node is dead). Production systems separate these.

Suspicion is continuous and cheap. Every component that consumes liveness information (the load balancer, the circuit breaker, the Raft cluster) can subscribe to suspicion updates and react in proportion. A 30% suspicion might mean "don't send new requests"; an 80% suspicion might mean "drop existing connections."

Declaration is discrete and expensive. Acting as if a node is dead — promoting a follower, evicting from the cluster, triggering reconciliation — has costs that should not be paid casually. Declarations should require sustained high suspicion, not transient spikes.

The phi accrual model makes this separation natural: phi is the suspicion signal, and each consumer chooses its own declaration threshold. The fixed-timeout model conflates the two, which is one of the reasons phi accrual is preferred for systems with many consumers of liveness information.

### Failure Detector Calibration

Whatever detector you choose, calibration is operational work that pays off:

1. **Measure the actual inter-heartbeat distribution.** Histograms of p50, p99, p99.9 inter-heartbeat times tell you what "normal" looks like. The timeout (or phi threshold) should be set relative to this baseline.
2. **Estimate the false-positive rate.** In a healthy cluster, how often does the detector spuriously flag a node? If the answer is "weekly," the cluster will see a steady drumbeat of unnecessary failover attempts.
3. **Measure detection latency.** When a real failure occurs, how long until the detector flags it? This bounds the recovery latency the cluster can offer.
4. **Track the calibration over time.** Network conditions drift. A detector tuned for last year's traffic patterns may be poorly tuned for this year's. Periodic recalibration is part of normal operations.

The catalog publishes failure-detector metrics — false positive count per day, p99 detection latency, current phi values — to the operations dashboard. The metrics are reviewed quarterly and the thresholds adjusted. This sounds tedious. It is. It is also the difference between a system that ages gracefully and one that produces an increasing rate of incidents over time.

## Code Examples

### Fixed-Timeout Heartbeat Detector

```rust,edition2021
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub struct FixedHeartbeatDetector {
    last_seen: HashMap<String, Instant>,
    timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus { Alive, Suspected, Dead }

impl FixedHeartbeatDetector {
    pub fn new(timeout: Duration) -> Self {
        Self { last_seen: HashMap::new(), timeout }
    }

    pub fn note_heartbeat(&mut self, node: &str) {
        self.last_seen.insert(node.to_string(), Instant::now());
    }

    /// Returns the status of a node based on time since last heartbeat.
    /// The 'suspected' band gives consumers a chance to react before
    /// the node is declared dead.
    pub fn status(&self, node: &str) -> NodeStatus {
        match self.last_seen.get(node) {
            None => NodeStatus::Dead,
            Some(&t) => {
                let elapsed = t.elapsed();
                if elapsed < self.timeout / 2 { NodeStatus::Alive }
                else if elapsed < self.timeout { NodeStatus::Suspected }
                else { NodeStatus::Dead }
            }
        }
    }
}

fn main() {
    let mut det = FixedHeartbeatDetector::new(Duration::from_millis(150));
    det.note_heartbeat("node-a");
    // Status is Alive immediately after heartbeat
    println!("{:?}", det.status("node-a"));
    // Status of a never-seen node is Dead
    println!("{:?}", det.status("node-z"));
}
```

The Alive / Suspected / Dead three-state model is the minimal version of the suspicion-vs-declaration separation. Production systems extend this with finer-grained suspicion levels.

### Phi Accrual (Simplified)

A correct phi accrual implementation requires careful numerical work; this sketch shows the structural shape:

```rust,edition2021
use std::collections::VecDeque;
use std::time::{Duration, Instant};

pub struct PhiAccrualDetector {
    intervals: VecDeque<f64>,  // recent inter-heartbeat times in milliseconds
    max_samples: usize,
    last_heartbeat: Option<Instant>,
}

impl PhiAccrualDetector {
    pub fn new(max_samples: usize) -> Self {
        Self { intervals: VecDeque::new(), max_samples, last_heartbeat: None }
    }

    pub fn note_heartbeat(&mut self) {
        let now = Instant::now();
        if let Some(prev) = self.last_heartbeat {
            let interval_ms = now.duration_since(prev).as_secs_f64() * 1000.0;
            self.intervals.push_back(interval_ms);
            if self.intervals.len() > self.max_samples {
                self.intervals.pop_front();
            }
        }
        self.last_heartbeat = Some(now);
    }

    /// Compute phi based on current time-since-last-heartbeat and the
    /// distribution of past intervals. Uses normal approximation; real
    /// implementations use lognormal or other empirically-validated fits.
    pub fn phi(&self) -> f64 {
        let last = match self.last_heartbeat {
            None => return f64::INFINITY,  // never seen - maximum suspicion
            Some(t) => t,
        };
        if self.intervals.is_empty() {
            return 0.0;  // not enough data
        }

        let n = self.intervals.len() as f64;
        let mean: f64 = self.intervals.iter().sum::<f64>() / n;
        let variance: f64 = self
            .intervals
            .iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>()
            / n;
        let stddev = variance.sqrt().max(1.0);  // floor to avoid divide-by-zero

        let elapsed_ms = last.elapsed().as_secs_f64() * 1000.0;

        // Normal-approximation P(time-since-last > elapsed). Real
        // implementations use a more accurate model and handle the tail
        // probability with proper numerical techniques.
        let z = (elapsed_ms - mean) / stddev;
        let p = 0.5 * libm::erfc(z / std::f64::consts::SQRT_2);

        if p <= 0.0 { f64::INFINITY } else { -p.log10() }
    }

    pub fn is_alive(&self, phi_threshold: f64) -> bool {
        self.phi() < phi_threshold
    }
}

# mod libm {
#     pub fn erfc(_x: f64) -> f64 { 0.5 }
# }
fn main() {
    let mut det = PhiAccrualDetector::new(100);
    det.note_heartbeat();
    // After only one heartbeat, no interval data - phi is 0
    println!("phi = {}", det.phi());
}
```

The production-grade version is significantly more careful: it uses a proper tail probability (not the normal approximation), handles the warm-up period with conservative defaults, and exposes a separate "interval mean" and "interval stddev" for the operations dashboard. The akka-failure-detector crate is a good reference implementation.

### Composing Suspicion with Declaration

The pattern for separating suspicion from declaration:

```rust,edition2021
# struct PhiAccrualDetector;
# impl PhiAccrualDetector {
#     fn new(_n: usize) -> Self { Self }
#     fn phi(&self) -> f64 { 0.0 }
# }
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct LivenessSupervisor {
    detector: Arc<PhiAccrualDetector>,
    // Declaration is sticky: once a node is declared dead, it stays declared
    // until explicitly cleared. This prevents rapid flapping.
    declared_dead: bool,
    declared_at: Option<Instant>,
}

impl LivenessSupervisor {
    pub fn evaluate(&mut self, declare_threshold: f64, clear_threshold: f64) {
        let phi = self.detector.phi();

        if !self.declared_dead && phi > declare_threshold {
            self.declared_dead = true;
            self.declared_at = Some(Instant::now());
            // trigger failover, alert, etc.
        } else if self.declared_dead && phi < clear_threshold {
            // Hysteresis: clear only when phi drops well below the threshold.
            // This prevents flapping if phi hovers around the declare value.
            self.declared_dead = false;
            self.declared_at = None;
            // trigger recovery, re-add to load balancer, etc.
        }
    }
}
```

The hysteresis between `declare_threshold` and `clear_threshold` is operational defense against flapping. Without it, a node whose phi oscillates around the declare value will be declared and undeclared repeatedly, producing a stream of expensive recovery actions.

## Key Takeaways

- A failure detector is the primitive every higher-level recovery mechanism (elections, failovers, circuit breakers) depends on. Its accuracy determines whether the cluster's reactions are appropriate or pathological.
- Fixed-timeout heartbeats are the practical baseline. The 3× rule (`T = 3 * H`) is a sensible default; calibrating `H` and `T` to the actual network's latency distribution is the work that turns "good enough" into "operationally robust."
- Phi accrual is the adaptive alternative used by Cassandra, Akka, and similar systems. It outputs a continuous suspicion level rather than a binary declaration, which lets multiple consumers react with different thresholds.
- Suspicion (continuous, cheap, frequently updated) and declaration (discrete, expensive, sticky with hysteresis) should be separated. Conflating them produces flapping and unnecessary recovery actions.
- Calibration is ongoing work. Network conditions drift; detector thresholds tuned six months ago may be poorly tuned now. Treat false-positive rate and detection latency as monitored metrics, not set-and-forget config.

> **Source note:** This lesson synthesizes from multiple sources. The Chandra & Toueg failure detector taxonomy is from "Unreliable Failure Detectors for Reliable Distributed Systems" (Journal of the ACM, 1996). Phi accrual is from Hayashibara, Defago, Yared, Katayama, "The φ Accrual Failure Detector" (SRDS 2004). SWIM is from Das, Gupta, Motivala (2002). DDIA Chapter 9 covers fixed-timeout heartbeats but does not deeply treat adaptive detectors; that content is synthesized from training knowledge and the cited papers. Specific operational parameters (50ms heartbeats, phi thresholds) are illustrative and should be calibrated for any production deployment. *Foundations of Scalable Systems* was unavailable as a source; the synthesis here should be cross-checked against that text or another systems-scaling reference before publication.

{{#quiz quiz-1.toml}}
