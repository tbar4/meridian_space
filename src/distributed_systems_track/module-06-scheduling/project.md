# Module 06 Project — Pass Window Scheduler

## Mission Brief

> **Incident ticket CN-2708-002**
> **Severity:** P1 (track-defining)
> **Reporter:** Constellation Operations, Pass Coordination
> **Status:** Open

The constellation's pass windows are getting denser. Today the system handles roughly 800 satellite passes per day across 48 satellites and 12 ground stations; the next-quarter plan adds 30 more satellites and three more ground stations, projecting 1,400 passes per day. The existing scheduler — a Python script that runs first-fit placement on a single thread with no priority awareness — already drops roughly 4% of pass-window jobs during peak hours. Scaling 75% beyond current load will break the scheduler outright.

You are building the **Pass Window Scheduler**, a Rust crate that integrates the patterns from this entire module: priority+EDF scheduling discipline (Lesson 1), best-fit multi-dimensional bin-packing with headroom and constraints (Lesson 2), and work stealing across worker threads within the scheduler itself plus task migration when the cluster load becomes imbalanced (Lesson 3).

The deliverable is a scheduler that demonstrably handles 1,400 passes per simulated day with a 99.9% deadline-meeting rate, fair-share allocation across regions, and no scheduler thread becoming a bottleneck.

## Repository Layout

```
pass-window-scheduler/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── job.rs              # PassWindowJob, deadline, priority_class, resource req
│   ├── machine.rs          # MachineFleet, capacity, used, residual
│   ├── placer.rs           # Best-fit multi-dim with headroom and constraints
│   ├── queue.rs            # Per-region priority+EDF queue with aging
│   ├── scheduler.rs        # Scheduler: submit, tick, dispatch
│   ├── stealing.rs         # Work-stealing across scheduler workers
│   └── migration.rs        # Cluster-level migration decisions
├── tests/
│   ├── deadline_compliance.rs
│   ├── fair_share.rs
│   ├── bin_packing.rs
│   ├── work_stealing.rs
│   ├── migration_triggers.rs
│   └── simulation_scale.rs
└── README.md
```

## Required API

```rust,ignore
// job.rs
#[derive(Clone, Debug)]
pub struct PassWindowJob {
    pub id: String,
    pub region: String,         // "pacific", "atlantic", "indian", etc.
    pub priority_class: u8,     // 0 = P0 (alerts), 1 = P1 (pass-window), 2 = P2 (batch)
    pub deadline: Instant,      // absolute deadline by which the job must complete
    pub estimated_runtime: Duration,
    pub req: ResourceRequest,
    pub required_tags: HashSet<String>,
    pub submitted_at: Instant,
}

#[derive(Clone, Debug)]
pub struct ResourceRequest {
    pub cpu_millicores: u32,
    pub memory_mb: u32,
    pub gpu_count: u32,
}

// machine.rs
#[derive(Clone, Debug)]
pub struct Machine {
    pub id: String,
    pub region: String,
    pub tags: HashSet<String>,
    pub capacity: ResourceRequest,
    pub used: ResourceRequest,
    pub running: Vec<String>,  // job IDs currently running here
}

pub struct MachineFleet { /* ... */ }
impl MachineFleet {
    pub fn add_machine(&mut self, m: Machine);
    pub fn machines(&self) -> &[Machine];
    pub fn machine(&self, id: &str) -> Option<&Machine>;
    pub fn utilization(&self) -> HashMap<String, f64>;
}

// scheduler.rs
pub struct Scheduler { /* ... */ }
impl Scheduler {
    pub fn new(fleet: Arc<Mutex<MachineFleet>>, num_workers: usize, headroom_pct: u32) -> Self;
    pub fn submit(&self, job: PassWindowJob) -> Result<()>;
    pub fn tick(&self);  // run one scheduling cycle
    pub fn metrics(&self) -> SchedulerMetrics;
}

pub struct SchedulerMetrics {
    pub total_submitted: u64,
    pub total_completed: u64,
    pub deadline_misses: u64,
    pub queue_depth_by_priority: HashMap<u8, usize>,
    pub migrations_executed: u64,
    pub steal_attempts: u64,
    pub steal_successes: u64,
}
```

## Acceptance Criteria

- [x] `cargo build --release` completes without warnings under `#![deny(warnings)]`.
- [x] `cargo test --release` passes all integration tests with zero flakes across 20 consecutive runs.
- [x] `cargo clippy -- -D warnings` produces no lints.
- [x] **Deadline compliance test:** simulate 1,400 jobs per day distributed across the 12-region fleet with realistic resource shapes. Measure deadline-meeting rate; assert ≥99.9%.
- [x] **Priority test:** with the queue saturated by P2 jobs, a newly-submitted P0 job is dispatched within one scheduler tick. Assert P0 latency p99 is bounded regardless of P2 backlog.
- [x] **Fair-share test:** simulate two regions submitting jobs at different rates — pacific submits 2× indian's volume — with equal region weights. Assert that pacific jobs do not consume more than ~60% of capacity (some imbalance is tolerable, but pacific shouldn't fully starve indian).
- [x] **Weighted fair-share test:** same setup but with pacific weight=3 and indian weight=1. Assert pacific gets approximately 75% of capacity.
- [x] **Best-fit placement test:** submit jobs of varying sizes to a heterogeneous fleet. Assert that jobs are placed on machines with the tightest residual fit (verified by reading per-machine utilization after placement).
- [x] **Headroom respect test:** with 15% headroom configured, normal-priority jobs do not push any machine past 85% utilization. P0 jobs CAN override headroom; assert this works correctly.
- [x] **Constraint filter test:** GPU-tagged jobs are only placed on GPU machines; ground-station-tagged jobs (for a specific pass) are only placed at the requested ground station. Assert that constraint violations never occur.
- [x] **Work stealing test:** with N scheduler workers, simulate skewed job submission (all jobs initially go to worker 0). Assert that worker 0's queue depth is eventually balanced with the others via stealing.
- [x] **Migration trigger test:** create a fleet imbalance by manipulating per-machine utilization directly. Assert that the migration loop detects the imbalance (variance > 30 percentage points) and initiates a migration. Assert that the migration is not initiated for variance within bounds.
- [x] **Scale simulation:** run the full simulated day with 1,400 jobs in under 60 seconds of wall-clock time (simulating an order of magnitude faster than real time). Assert all metrics are within their respective bounds.
- [ ] *(self-assessed)* The README explains how the four mechanisms (priority+EDF, best-fit placement, work stealing, task migration) interact, and what failure modes the integrated system is and is not designed to handle.
- [ ] *(self-assessed)* The scheduler exposes per-queue and per-machine metrics so operators can diagnose bottlenecks. The README documents the metric set and recommended alerting thresholds.
- [ ] *(self-assessed)* The work-stealing implementation uses a Chase-Lev-style asymmetric deque (or equivalent lock-free structure) so the steal path doesn't contend with the owner's push/pop in the common case.

## Expected Output

`cargo test --release simulation_scale -- --nocapture`:

```text
[setup] fleet: 90 machines across 12 regions
[setup] scheduler: 8 workers, 15% headroom, region-weighted fair-share
[setup] simulating 1,400 jobs over a day, accelerated 1440x (1 minute wall-time per simulated day)

[t=0.000s] simulation start
[t=15.000s] simulated 6 hours; 350 jobs completed; 0 deadline misses; avg utilization 62%
[t=30.000s] simulated 12 hours; 720 jobs completed; 0 deadline misses; avg utilization 68%
[t=45.000s] simulated 18 hours; 1,070 jobs completed; 1 deadline miss; avg utilization 71%
[t=60.000s] simulation complete
  total submitted: 1,400
  total completed: 1,400
  deadline misses: 1 (0.07%)
  migrations executed: 4
  steal successes / steal attempts: 12,847 / 18,103 (70.9%)
  per-region throughput (jobs/day): pacific=312, atlantic=298, indian=187, ...
PASS: 99.9% deadline compliance achieved
```

## Hints

<details>
<summary>1. Order the four mechanisms in the request path</summary>

The order matters. On submit: (a) place the job into the per-region priority+EDF queue. The scheduling tick: (b) for each worker, pull the next job from the local queue (with aging applied); (c) attempt placement via best-fit on the fleet, filtered by constraints; (d) on placement failure, the job stays queued (with potentially an aging boost); (e) on placement success, dispatch to the machine. Separately: (f) periodically (every N ticks), check for fleet imbalance and trigger migration. The work stealing happens between the per-worker queues for the scheduling work itself, not for the jobs being scheduled.

</details>

<details>
<summary>2. Separate scheduling work from scheduled work</summary>

There are two distinct workloads in this project: the scheduler's own work (deciding where to place jobs) and the jobs themselves (running on the fleet). The work-stealing pool is for the *scheduler's* work — multiple worker threads each making placement decisions. The placement decisions themselves use the bin-packing algorithm against the *fleet*. Don't confuse them: tokio's work-stealing is for the scheduler workers; the fleet machines are the bin-packing targets, not work-stealing peers.

</details>

<details>
<summary>3. Deadlines and EDF in a priority+EDF queue</summary>

The EDF property within a priority class means the queue must be ordered by deadline (smallest first). A BTreeMap keyed by deadline gives O(log n) insertion and O(1) front-peek. The priority dimension means you have N such BTreeMaps, one per class. Dispatching is: find the highest non-empty class, peek the smallest deadline, that's the next job. Aging interacts: when computing the effective priority class, factor in time waited. A job that should be P2 but has waited an hour effectively becomes P1.

</details>

<details>
<summary>4. Per-machine utilization tracking</summary>

The fleet's `utilization` is updated on every placement (add the job's resource cost to the machine's `used`) and on every completion (subtract). Use atomic counters or fine-grained locks; the placement path is hot. A coarse-grained `Mutex<HashMap<MachineId, Machine>>` will become a bottleneck at scale; in the project's required scope it's acceptable, but the scaling assumption is that the read path uses per-machine RwLock.

</details>

<details>
<summary>5. Migration cost estimation</summary>

The migration trigger needs to estimate: (a) is the imbalance significant enough to migrate? (b) is the migration cost lower than the benefit? Use a simple heuristic: imbalance > 30 percentage points triggers consideration; pick a job whose migration_cost is < some threshold (say, 30 seconds); verify the destination has room *with headroom*; verify the move doesn't reverse the imbalance. The required tests are deterministic — the metrics expose whether migrations happen and roughly how often.

</details>

<details>
<summary>6. Simulating accelerated time</summary>

For the 1440x simulation, don't use real `tokio::time::sleep` — it would take a full day. Use a virtual clock (a wrapper around an AtomicU64 that the simulation advances) and have the scheduler's deadline checks consult the virtual clock. This makes the simulation deterministic, fast, and reproducible. tokio::time::pause + advance is one way; a custom Clock trait that the scheduler uses is another. Either is fine.

</details>

## Source Anchors

- All sources cited in the three lessons of this module
- Verma et al., "Large-scale cluster management at Google with Borg" (EuroSys 2015) — the canonical paper on integrated cluster scheduling
- Kubernetes' scheduler documentation (kubernetes.io/docs/concepts/scheduling-eviction/) — the practical production reference
- The tokio runtime's work-stealing implementation (tokio.rs/blog) — for the scheduler-worker layer
