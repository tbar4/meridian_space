# Lesson 3: Chaos Engineering and Fault Injection

## Context

The catalog's resilience layer — failure detection, circuit breakers, bulkheads, retries — was deployed two years ago. It has prevented 14 documented incidents, according to the after-action records. It has also failed to prevent three incidents that occurred because of failure modes the resilience layer had not been designed for: a downstream returning correct responses with a 30-second latency that fell *within* the timeout but *above* the bulkhead-acquire wait; a coordinator service that returned HTTP 200 with an error body the caller's framework did not parse as an error; and a deadlock in the resilience layer itself that occurred under a specific race between circuit-breaker state transitions and pool acquisition.

The pattern is consistent. Resilience patterns protect against known failure modes. The next outage is, by definition, an unknown failure mode — something the system has not been tested against. The discipline that closes the gap is **chaos engineering**: the deliberate, controlled injection of failures into production-like environments to discover failure modes before they discover you.

Chaos engineering was popularized at Netflix in the early 2010s with the Chaos Monkey tool, which randomly terminated production instances during business hours to verify that the architecture survived single-node failures. The practice has matured into a broader discipline covering network partitions, latency injection, dependency failures, configuration errors, and the human-factors elements of incident response. By the end of this lesson, you should understand the principles of chaos engineering, the spectrum of fault-injection tools and approaches, and the operational discipline (game days, deterministic simulation testing, runbook validation) that converts chaos engineering from a "nice idea" into reliable knowledge about how the system actually fails.

## Core Concepts

### The Principles of Chaos Engineering

The Principles of Chaos Engineering, formalized by the Netflix team and codified at principlesofchaos.org, lay out four guiding ideas:

**Build a hypothesis around steady-state behavior.** Before injecting a failure, define what "normal" looks like. The hypothesis is "in the steady state of N requests per second and P% error rate, when we inject failure F, the system will still serve at least M% of those requests with no observable degradation beyond X." Without a measurable steady-state hypothesis, the experiment cannot have a clear result.

**Vary real-world events.** The failures injected should be ones the system will plausibly encounter in production: instance termination, network latency, packet loss, dependency unavailability, region outages. Inventing artificial failures that production won't produce is less valuable than reproducing realistic ones.

**Run experiments in production.** This is the principle that distinguishes chaos engineering from traditional testing. Production has properties — actual traffic, actual data distributions, actual operator behavior — that no test environment can replicate. The discipline is to run controlled chaos experiments in production while bounding the blast radius so that user-visible impact is minimized.

**Automate experiments to run continuously.** A failure mode discovered once and then never re-checked will silently regress as the system evolves. Automation makes chaos a continuous discipline rather than a quarterly event.

These principles are aspirational; production chaos engineering also requires substantial guardrails. The catalog's chaos program runs experiments only during business hours, only when on-call is staffed, only with explicit dashboards and kill-switches, and only after the experiment has passed in lower environments. The discipline is "controlled chaos," not "chaos."

### The Chaos Tool Lineage

Chaos engineering as practice traces through several generations of tooling:

**Chaos Monkey (Netflix, 2011)** — randomly terminates EC2 instances during business hours. The original; still the standard introduction. Forces the architecture to tolerate single-instance failures, which surfaces dependencies on specific machine instances rather than on roles.

**Simian Army (Netflix, 2012)** — extends Chaos Monkey with siblings: Chaos Gorilla (region failure), Latency Monkey (delay injection), Janitor Monkey (resource cleanup), Conformity Monkey (configuration drift). The pattern: each monkey injects one specific failure mode, automated to run regularly.

**Gremlin, ChaosMesh, LitmusChaos (mid-2010s onward)** — commercial and open-source platforms that productize fault injection. Network partitions, CPU/memory stress, disk failure, time skew, and dependency-specific failures (Kafka, Redis, gRPC) are all injectable as targeted experiments.

**Deterministic simulation testing (FoundationDB, 2014; TigerBeetle, 2020s)** — instead of injecting failures in production, simulate the entire system at the level of individual messages and CPU operations, then run millions of random schedules to find concurrency bugs. FoundationDB's simulation is what allowed them to claim "we found the bugs Jepsen would find before Jepsen would have found them" — and the claim held up.

The catalog uses a layered approach: deterministic simulation for the consensus and replication layers (where bugs are subtle and the state space is tractable), Gremlin-style production fault injection for service-level failures, and game days (covered below) for the human-factors layer.

### What to Inject: A Taxonomy of Failure Modes

A useful checklist of failure modes to consider for any new system:

**Infrastructure failures.** Instance termination, machine restart, disk full, network partition between zones, DNS unavailability, certificate expiry. The Chaos Monkey lineage covers these well.

**Network failures.** Packet loss, latency injection, asymmetric routes, MTU changes, intermittent dropouts. Latency injection is particularly valuable because it surfaces timeout misconfiguration without the system actually failing.

**Dependency failures.** A downstream returning errors, returning correct responses with high latency, returning malformed data, returning correct data but very slowly. Each is a different failure mode with different defenses needed.

**Configuration errors.** Wrong feature flags, expired credentials, mismatched versions across the cluster, schema migrations that haven't propagated. These are statistically the most common cause of real outages, and they are testable: deliberately inject a wrong config and see what happens.

**Time-related failures.** Clock skew between nodes, NTP unavailability, leap-second handling, system clock jumping backward. The Module 1 lesson on clock unreliability is exactly the set of failure modes this category covers.

**Resource exhaustion.** Memory pressure, CPU saturation, file descriptor leaks, thread pool exhaustion. These are the failures that classically take down systems that have only been tested under normal load.

**Adversarial input.** Malformed requests, very large requests, requests with unusual character encodings, replay attacks. Less about chaos engineering and more about robustness testing, but the line is blurry.

A mature chaos program covers all of these on a rotating cadence. The catalog's chaos engineering team runs one experiment per category per quarter, with the specific scenario varying so the system is not just tested against the same fault repeatedly.

### Game Days

The human-factors element of incident response is the part most easily neglected. A system can be technically resilient and still produce protracted outages because the on-call engineer doesn't know which runbook to follow, can't find the relevant dashboard, or doesn't have the access credentials for the failing component.

**Game days** are scheduled exercises where a controlled failure is injected and the operations team responds as if it were a real incident. The team uses the actual runbook, the actual dashboards, the actual escalation paths. The result is a list of gaps: the runbook step that references a deleted dashboard, the alert that doesn't actually page anyone, the credential that needs a manager's approval to use during off-hours.

The discipline:

1. **Schedule the game day in advance.** Not a surprise; the team knows it will happen, but not the specific scenario. This mimics the real on-call experience without the genuine stress of a 3 AM page.
2. **Inject the failure with realistic constraints.** The injector is a separate team or person; the responding team doesn't know what was injected and must diagnose from first principles using only production tooling.
3. **Time-box and observe.** A facilitator tracks the response, noting decisions and bottlenecks. The blameless retro afterward is where the gaps surface.
4. **Convert findings to backlog items.** Every "we didn't have a runbook for this" becomes a runbook item. Every "we couldn't find the right dashboard" becomes a dashboard improvement.

Game days are slow and expensive — a full one takes a half-day from a dozen people — but they surface the soft-skill and tooling gaps that automated chaos cannot. The catalog runs four game days per year, two scheduled in advance and two with the only-the-leadership-knows-the-date variety.

### Deterministic Simulation Testing

For the lower layers of the stack (consensus, storage, replication), production chaos engineering is too coarse. A bug that requires a specific interleaving of three message receives between Raft followers may not manifest in years of production traffic, but a deterministic simulation that tries thousands of message orderings per minute will find it in an afternoon.

The pattern, pioneered at FoundationDB:

1. **Build the system as a deterministic state machine.** Every component — networking, storage, threading — is implemented behind an interface that allows a simulator to control its behavior. In production the implementation is real network/disk/threads; in tests, the simulator substitutes mock implementations with deterministic random scheduling.
2. **Inject faults at the simulation level.** The simulator can drop, reorder, and delay messages arbitrarily; pause and resume "threads" at any point; corrupt disk writes; introduce arbitrary clock skew.
3. **Run with random seeds.** Each test run produces a deterministic trace from a seed. A failing run can be replayed exactly by re-running the same seed.
4. **Run millions of seeds.** The state space of a non-trivial system is too large to exhaustively explore, but random sampling finds bugs with high probability. FoundationDB ran billions of simulation-seconds before they considered the system production-ready.

The cost is that the system must be architected from the start to support deterministic simulation. Retrofitting an existing codebase is hard. TigerBeetle and FoundationDB are designed for it; most other systems get partial benefit by simulating subset of components (the Raft layer, for instance) while leaving others untested.

The catalog's Raft implementation uses deterministic simulation (the Module 3 project includes a simplified version). Other components rely on more conventional testing. The result is that the Raft layer has very high confidence and the others have moderate confidence — which matches the cost asymmetry of bugs in each layer.

### Measuring the Value of Chaos Engineering

A common pushback against chaos engineering: "we're spending engineering time creating problems instead of solving them." The response requires measurement.

Useful metrics:

- **Findings per experiment.** How many real bugs or operational gaps does each chaos experiment surface? If the answer is consistently zero, either the system is genuinely resilient (great) or the experiments are not exercising the system's weaknesses (more likely).
- **Mean time to detection (MTTD).** When a failure is injected, how long until monitoring detects it? Chaos engineering exposes detection gaps directly.
- **Mean time to recovery (MTTR).** After the failure is detected, how long until the system returns to baseline? Recovery time is a function of the runbook, the tooling, and the automation — each of which chaos surfaces.
- **Production incident rate.** The lagging indicator. A successful chaos program correlates with a decrease in production incident rate, holding architecture stable.

The catalog tracks all of these. After two years of chaos engineering, MTTD has dropped from 12 minutes to 3 minutes, MTTR has dropped from 47 minutes to 18 minutes, and production incident rate (per million requests) has dropped 60%. The chaos engineering investment was sustained, then refined, on the basis of these numbers.

## Code Examples

### A Simple Latency Injector

For service-level fault injection, the cheapest mechanism is a wrapper that introduces controlled delays:

```rust,edition2021
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub struct LatencyInjector {
    delay_ms: AtomicU64,  // 0 means disabled
    probability_pct: AtomicU64,  // 0-100
}

impl LatencyInjector {
    pub fn new() -> Self {
        Self {
            delay_ms: AtomicU64::new(0),
            probability_pct: AtomicU64::new(0),
        }
    }

    pub fn configure(&self, delay_ms: u64, probability_pct: u64) {
        self.delay_ms.store(delay_ms, Ordering::SeqCst);
        self.probability_pct.store(probability_pct.min(100), Ordering::SeqCst);
    }

    pub fn disable(&self) {
        self.delay_ms.store(0, Ordering::SeqCst);
        self.probability_pct.store(0, Ordering::SeqCst);
    }

    /// Called at the start of each request. Injects delay with the configured
    /// probability. The delay is uniform; production injectors use distributions
    /// that match observed real-world latency tails.
    pub async fn maybe_inject(&self) {
        let prob = self.probability_pct.load(Ordering::SeqCst);
        let delay = self.delay_ms.load(Ordering::SeqCst);
        if prob == 0 || delay == 0 { return; }

        // Roll the dice. Probability is per-request.
        let roll = fastrand::u64(0..100);
        if roll < prob {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
    }
}

# mod fastrand {
#     pub fn u64(_r: std::ops::Range<u64>) -> u64 { 50 }
# }
fn main() {
    let inj = LatencyInjector::new();
    inj.configure(200, 5);  // 5% of requests get 200ms delay
    println!("latency injector configured");
    // Production: the injector is wired into every cross-service call.
}
```

The configuration is dynamically adjustable, so the chaos team can ramp up the injection rate gradually and back off if the system shows distress beyond the experimental budget. Production injectors integrate with feature flag systems so the kill switch is one toggle away.

### A Deterministic Simulation Harness (Sketch)

```rust,ignore
// SKETCH: deterministic simulation framework for a Raft-style protocol.

use std::collections::VecDeque;

pub struct SimEnvironment {
    nodes: Vec<SimNode>,
    network: SimNetwork,
    rng: SeededRng,
    clock: SimulatedClock,
}

pub struct SimNetwork {
    // Pending messages, controllable by the simulator.
    pending: VecDeque<(String, String, Vec<u8>)>,
    drop_probability: f64,
    reorder_probability: f64,
}

impl SimEnvironment {
    pub fn new(seed: u64, node_count: usize) -> Self { /* ... */ unimplemented!() }

    /// Single simulation step: deliver one message, advance one node's clock,
    /// or inject one fault. The choice is made by the seeded RNG, so the
    /// entire run is determined by the seed.
    pub fn step(&mut self) -> StepResult {
        unimplemented!()
    }

    pub fn run_until(&mut self, predicate: impl Fn(&SimEnvironment) -> bool) -> usize {
        let mut steps = 0;
        while !predicate(self) {
            self.step();
            steps += 1;
            if steps > 1_000_000 { panic!("simulation diverged"); }
        }
        steps
    }
}

// In a test:
// for seed in 0..1_000_000 {
//     let mut env = SimEnvironment::new(seed, 5);
//     env.run_until(|e| e.has_committed_index(10));
//     assert!(env.no_safety_violations());
// }

# struct SimNode;
# struct SeededRng;
# struct SimulatedClock;
# struct StepResult;
```

The structure is the point. A bug found by seed 4837291 can be reproduced exactly by re-running with that seed — no "it's flaky" excuses, no "we couldn't reproduce." Bugs are deterministically reproducible, which makes them deterministically fixable.

### Game Day Tracker (Sketch of Structure)

```rust,edition2021
use std::time::Instant;

pub struct GameDayEvent {
    pub timestamp: Instant,
    pub actor: String,
    pub action: String,
    pub notes: String,
}

pub struct GameDay {
    pub scenario_name: String,
    pub scheduled_at: Instant,
    pub injection_at: Option<Instant>,
    pub detected_at: Option<Instant>,
    pub mitigated_at: Option<Instant>,
    pub resolved_at: Option<Instant>,
    pub timeline: Vec<GameDayEvent>,
    pub findings: Vec<String>,
}

impl GameDay {
    pub fn mttd(&self) -> Option<std::time::Duration> {
        match (self.injection_at, self.detected_at) {
            (Some(i), Some(d)) => Some(d.duration_since(i)),
            _ => None,
        }
    }

    pub fn mttr(&self) -> Option<std::time::Duration> {
        match (self.detected_at, self.resolved_at) {
            (Some(d), Some(r)) => Some(r.duration_since(d)),
            _ => None,
        }
    }

    pub fn record_event(&mut self, actor: &str, action: &str, notes: &str) {
        self.timeline.push(GameDayEvent {
            timestamp: Instant::now(),
            actor: actor.to_string(),
            action: action.to_string(),
            notes: notes.to_string(),
        });
    }
}

fn main() {
    let gd = GameDay {
        scenario_name: "primary region failure".to_string(),
        scheduled_at: Instant::now(),
        injection_at: None,
        detected_at: None,
        mitigated_at: None,
        resolved_at: None,
        timeline: Vec::new(),
        findings: Vec::new(),
    };
    println!("game day: {}", gd.scenario_name);
}
```

The point is not the code — game day tracking is process work, not engineering. The point is that the data produced by a game day is structured enough to feed back into MTTD and MTTR metrics, and findings should be queryable and trackable like any other engineering work.

## Key Takeaways

- Resilience patterns protect against known failure modes; chaos engineering discovers the unknown ones. Both are necessary. Defenses without testing are theoretical; testing without defenses produces incidents.
- The principles of chaos engineering — steady-state hypothesis, real-world events, production experiments, continuous automation — are aspirational. Production chaos requires guardrails: business hours, kill switches, bounded blast radius, on-call coverage.
- Chaos targets span infrastructure, network, dependencies, configuration, time, resources, and adversarial input. A mature program rotates through these categories rather than testing the same fault repeatedly.
- Game days surface human-factors gaps that automated chaos cannot: missing runbooks, broken dashboards, escalation paths that don't work at 3 AM. The slow, expensive ones produce findings that other testing misses.
- Deterministic simulation testing is the right tool for low-level concurrent systems (consensus, storage). It requires architectural support and substantial up-front cost, and it produces dramatically higher confidence than conventional testing for the systems where it applies.

> **Source note:** This lesson synthesizes from Netflix's published material on Chaos Monkey and the Simian Army (techblog.netflix.com), the Principles of Chaos Engineering document (principlesofchaos.org), Casey Rosenthal & Nora Jones, *Chaos Engineering: System Resiliency in Practice* (O'Reilly, 2020), and the FoundationDB simulation testing approach as described in Will Wilson's "Testing Distributed Systems w/ Deterministic Simulation" talk (Strange Loop 2014). DDIA Chapter 9 has a brief discussion under "Fault injection" and "Formal Methods and Randomized Testing." Specific operational numbers (MTTD reduction from 12 to 3 minutes, etc.) are illustrative and not real Meridian metrics. *Foundations of Scalable Systems* would have been the standard reference here and was unavailable; the content should be cross-checked against that text before publication.

{{#quiz quiz-3.toml}}
