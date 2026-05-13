# Lesson 1: Scheduling Algorithms

## Context

The Constellation Network's compute grid runs in fragments: each ground station has a fleet of GPUs for on-site image processing; each satellite has a small embedded compute budget for in-orbit triage; the central catalog tier has a large CPU pool for batch reconciliation. Jobs arriving at the system — process this raster, predict that conjunction, reconcile this catalog — must be assigned to specific machines. The decision of which job runs where, and when, is the **scheduling problem**, and the answer determines whether the system meets its latency budgets, whether it utilizes hardware efficiently, and whether high-priority work (conjunction alerts) gets through during periods of high load.

The naïve scheduler is a single global queue with a single worker per machine: pull the next job off the queue, hand it to the next idle machine. This is FIFO, and it is sufficient for systems with one workload, one machine class, and no priority differentiation. The constellation has none of these properties. Conjunction alerts must preempt routine telemetry processing. Pass-window-scoped jobs have deadlines (they must complete before the next pass) that FIFO cannot honor. Some workloads (large image processing) belong on GPUs; others (text-shaped log analysis) belong on CPUs. The scheduler must reason about all of this.

This lesson covers the family of scheduling algorithms — FIFO, priority, fair-share, deadline-aware, EDF, multi-level feedback — and the tradeoffs each makes. By the end, you should be able to choose a scheduling discipline for a given workload, articulate the failure modes (starvation, priority inversion, convoy effects) each is and is not vulnerable to, and recognize when the right answer is to partition the workload across multiple schedulers rather than to find a single perfect algorithm.

## Core Concepts

### FIFO and Its Limits

**First-in-first-out** scheduling pulls jobs from a queue in arrival order. Each machine takes the next job when it becomes idle. The implementation is one shared queue per scheduling domain.

The properties:

- **Fair in the sense of arrival order.** Every job waits its turn; no job is favored over another by anything other than arrival time.
- **Simple.** The implementation is a single concurrent queue. No priority comparison, no preemption, no deadline tracking.
- **Convoy effect prone.** A long-running job at the head of the queue blocks all subsequent shorter jobs. If a 10-second job is followed by ten 1-second jobs, the 1-second jobs wait 10 seconds in queue and the system spends 10 seconds with one machine busy and others idle (or, if the machines pull independently, the queue empties faster but the convoy effect manifests when machines re-poll a long-busy queue).
- **No priority awareness.** Critical jobs (conjunction alerts) wait behind routine jobs; the scheduler cannot distinguish.

FIFO is the right choice for workloads that are genuinely homogeneous and unconcerned with priority. For the catalog's background reconciliation pipeline — every job is the same shape and the system has hours to complete them — FIFO is the right tool. For anything that needs differential treatment, FIFO is the baseline that more sophisticated schedulers improve on.

### Priority Scheduling

**Priority scheduling** assigns each job a priority class; the scheduler always picks the highest-priority pending job. Within a priority class, jobs are typically FIFO.

The structure: instead of one queue, the scheduler has N queues, one per priority class. To pick the next job, scan from highest priority down and take the first non-empty queue's head.

The properties:

- **High-priority work is unblocked by low-priority work.** Conjunction alerts (P0) run before routine telemetry (P3) regardless of arrival order.
- **Starvation risk for low-priority work.** If high-priority jobs arrive faster than the system can process them, low-priority jobs may never run. The classic fix is **priority aging**: a job's effective priority increases the longer it waits. After waiting for some bound, even a P3 job is effectively P0 and runs.
- **Priority inversion.** A low-priority job holds a lock that a high-priority job needs; the high-priority job waits behind the low-priority job, effectively inverted. The fix is **priority inheritance**: the lock-holder's priority is temporarily raised to match the highest-priority waiter. Linux's real-time scheduler supports this; most application-level schedulers do not.

The constellation uses three priority levels: P0 for safety-critical (conjunction alerts, anomaly responses), P1 for time-bounded operational (pass-window jobs), and P2 for background (reconciliation, archival). Priority aging triggers after 5 minutes in queue; below P2 there is no priority floor — jobs that don't fit the three levels don't get scheduled.

### Fair-Share / Weighted Fair Scheduling

In multi-tenant systems, FIFO and priority schedulers can be hijacked by a single high-volume tenant. **Fair-share scheduling** allocates resources by tenant, with each tenant guaranteed a share regardless of how many jobs it submits.

The structure: each tenant has a quota (a share of system capacity). The scheduler tracks per-tenant consumption and prefers the tenant whose recent usage is furthest below quota. Hadoop's Fair Scheduler, Kubernetes' fair-share queue, and AWS's Auto Scaling all use variants of this.

The key insight: **fairness is a property of allocation over time**, not of any single scheduling decision. A tenant that has used 80% of capacity in the last minute is "ahead of fair"; a tenant that has used 5% is "behind fair." The scheduler advances the behind-fair tenants until balance is restored.

**Weighted fair scheduling** generalizes: each tenant has a weight, and the fair share is proportional to the weight. Tenant A with weight 3 and tenant B with weight 1 get a 3:1 split of capacity under contention; if A submits fewer jobs than its share, the unused capacity goes to B.

The constellation's Pass Window Scheduler treats each region as a tenant: each region has a weight proportional to the number of satellites it tracks, and the scheduler ensures every region's pass-window jobs run on time even if one region (the densely-tracked North Atlantic) submits more jobs than another.

### Deadline-Aware Scheduling: Earliest Deadline First (EDF)

When jobs have hard deadlines — "this conjunction prediction must complete before T+30 seconds, after which the result is useless" — the scheduler should prefer jobs with the earliest deadlines.

**Earliest Deadline First (EDF)** picks the pending job with the earliest deadline. Provably optimal in the sense that if any schedule meets all deadlines, EDF finds one.

The properties:

- **Optimal for hard real-time.** EDF is the theoretical baseline for real-time scheduling on a single processor (Liu & Layland 1973).
- **Degrades badly under overload.** When too many jobs are submitted to meet all deadlines, EDF doesn't degrade gracefully — it tries to meet every deadline and misses them in a cascading pattern. Real-time systems often add **admission control**: reject new jobs at submission time if the system cannot meet their deadlines.
- **Mixed-criticality complications.** EDF doesn't know that a P0 job's deadline is more important to meet than a P2 job's. In practice, deadline-aware schedulers combine deadlines with priority classes: within a priority class, EDF; across classes, priority dominates.

The constellation's pass-window scheduler is essentially EDF with priority classes. Conjunction alerts have priority over routine pass-window jobs; within either class, the job with the earliest deadline goes first.

### Multi-Level Feedback Queue (MLFQ)

For workloads where job durations are unknown in advance — "is this job going to take 1 second or 1 hour?" — fixed-priority and deadline-aware schedulers cannot make optimal decisions. The **multi-level feedback queue** schedules without prior knowledge by adjusting a job's priority based on its observed behavior.

The structure:

1. New jobs enter the highest-priority queue.
2. The scheduler always runs from the highest non-empty queue.
3. If a job uses its full time slice (i.e., is CPU-bound), it's demoted to the next lower queue with a longer time slice.
4. Short jobs that complete within their time slice stay at high priority; long jobs gradually migrate down.

The effect: short jobs finish quickly (they don't get demoted), long jobs eventually get fair share at lower priority. This is the algorithm Linux's CFS and Windows' priority scheduler are descended from.

The constellation doesn't use MLFQ at the application level — the workloads have known characteristics, so the scheduler can use explicit priority and deadline information. But the operating system underneath every machine uses MLFQ-like scheduling, and the application scheduler interacts with the OS scheduler in subtle ways (a job that the application scheduler considers low-priority may still be high-priority at the OS level if it's interactive).

### When to Partition Workloads Rather Than Build One Scheduler

A common scheduler antipattern: trying to build one global scheduler that handles every workload, every priority class, every deadline, every machine type. The result is a scheduler that is complex, slow to tune, and rarely optimal for any individual workload.

The alternative is **workload partitioning**: separate schedulers (with separate queues, separate machines) for genuinely different workloads. The constellation runs three schedulers:

1. **Pass Window Scheduler** — deadline-driven, priority-aware, region-fair. Handles jobs scoped to satellite passes (typically 5-15 minute windows).
2. **Conjunction Alert Scheduler** — strict priority, no resource pre-allocation. Handles safety-critical jobs that must preempt everything else.
3. **Reconciliation Scheduler** — FIFO across a dedicated pool of low-priority machines. Handles background work that runs whenever capacity is available.

Each scheduler is simpler than a single unified scheduler would be. The cost is some inefficiency at the boundaries — a machine assigned to reconciliation cannot be used for pass-window jobs even if the pass-window scheduler is overloaded. The benefit is operational comprehensibility: when something goes wrong, the on-call knows which scheduler is involved and what its rules are.

### Scheduler Failure Modes

A few specific operational failure modes worth recognizing:

**Starvation.** Some class of jobs never runs because the scheduler always prefers another. Fix: priority aging or fair-share guarantees.

**Priority inversion.** A high-priority job is blocked by a low-priority job holding a shared resource. Fix: priority inheritance or lock-free designs.

**Convoy effects.** A small number of long jobs cause many short jobs to queue behind them, producing poor throughput. Fix: separate queues for short vs long jobs, or preemption.

**Thundering herd.** Many jobs become eligible to run at the same instant (e.g., a scheduled batch fires). Fix: jitter in the scheduling tick or rate-limited release.

**Stale-deadline accumulation.** Jobs whose deadlines have already passed continue to occupy queue slots, displacing fresh work. Fix: admission control plus aggressive aging-out of expired jobs.

**Resource fragmentation.** Available capacity is spread across machines such that no single machine has enough for the next job. Fix: bin-packing-aware scheduling or workload consolidation, covered in Lesson 2.

The catalog has hit four of these six in production over the past two years. The operational playbook for each is a specific scheduling discipline; the discipline is configured at design time, not discovered during the incident.

## Code Examples

### A Priority-Aging Scheduler

```rust,edition2021
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::{Duration, Instant};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Job {
    pub id: String,
    pub priority: u8,  // higher = more important
    pub submitted_at: Instant,
}

#[derive(Eq, PartialEq)]
struct ScheduleKey {
    effective_priority: u8,
    submitted_at: Instant,
}

impl Ord for ScheduleKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority first; within priority, earlier submission first.
        self.effective_priority
            .cmp(&other.effective_priority)
            .then_with(|| other.submitted_at.cmp(&self.submitted_at))
    }
}

impl PartialOrd for ScheduleKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct AgingScheduler {
    queue: BinaryHeap<(ScheduleKey, Job)>,
    aging_step: Duration,  // priority increases by 1 per step waited
    max_priority: u8,
}

impl AgingScheduler {
    pub fn new(aging_step: Duration, max_priority: u8) -> Self {
        Self { queue: BinaryHeap::new(), aging_step, max_priority }
    }

    pub fn submit(&mut self, job: Job) {
        let key = ScheduleKey {
            effective_priority: job.priority,
            submitted_at: job.submitted_at,
        };
        self.queue.push((key, job));
    }

    /// Pick the next job, applying aging to all queued jobs first. This is
    /// O(n) per pick; production implementations use a more efficient
    /// data structure (a tree indexed by 'next aging tick').
    pub fn pick(&mut self) -> Option<Job> {
        // Rebuild the heap with aged priorities.
        let now = Instant::now();
        let mut updated: Vec<(ScheduleKey, Job)> = self.queue.drain().collect();
        for (key, job) in updated.iter_mut() {
            let waited = now.duration_since(job.submitted_at);
            let aging_steps = (waited.as_secs_f64() / self.aging_step.as_secs_f64()) as u8;
            key.effective_priority = job.priority
                .saturating_add(aging_steps)
                .min(self.max_priority);
        }
        for entry in updated {
            self.queue.push(entry);
        }
        self.queue.pop().map(|(_, job)| job)
    }
}

fn main() {
    let mut sched = AgingScheduler::new(Duration::from_secs(60), 10);
    sched.submit(Job {
        id: "P3-old".into(),
        priority: 3,
        submitted_at: Instant::now() - Duration::from_secs(600),
    });
    sched.submit(Job {
        id: "P5-fresh".into(),
        priority: 5,
        submitted_at: Instant::now(),
    });
    let next = sched.pick();
    println!("scheduler picked: {:?}", next.map(|j| j.id));
    // P3-old has aged 10 priority steps (10 minutes / 1 minute each), reaching
    // priority 13 (capped at 10); P5-fresh is at 5. P3-old wins.
}
```

The aging mechanism prevents starvation: even very-low-priority jobs eventually rise to compete with high-priority ones. The cost is implementation complexity and the need to re-prioritize the queue periodically.

### Earliest-Deadline-First With Priority Classes

```rust,edition2021
use std::collections::BTreeMap;
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct DeadlineJob {
    pub id: String,
    pub priority_class: u8,  // higher = more important
    pub deadline: Instant,
}

pub struct PriorityEDFScheduler {
    // Map of priority class -> BTreeMap of deadline -> job.
    // Within a class, BTreeMap ordering gives EDF: smallest deadline first.
    queues: BTreeMap<u8, BTreeMap<Instant, DeadlineJob>>,
}

impl PriorityEDFScheduler {
    pub fn new() -> Self {
        Self { queues: BTreeMap::new() }
    }

    pub fn submit(&mut self, job: DeadlineJob) {
        let queue = self.queues.entry(job.priority_class).or_default();
        queue.insert(job.deadline, job);
    }

    /// Pick the highest-priority class's earliest-deadline job.
    pub fn pick(&mut self) -> Option<DeadlineJob> {
        // Scan classes from highest priority to lowest.
        let highest_class = self.queues.iter().rev().find(|(_, q)| !q.is_empty()).map(|(c, _)| *c)?;
        let queue = self.queues.get_mut(&highest_class)?;
        let earliest_deadline = *queue.keys().next()?;
        queue.remove(&earliest_deadline)
    }
}

fn main() {
    let mut sched = PriorityEDFScheduler::new();
    let now = Instant::now();
    sched.submit(DeadlineJob {
        id: "routine-1".into(),
        priority_class: 1,
        deadline: now + std::time::Duration::from_secs(60),
    });
    sched.submit(DeadlineJob {
        id: "alert-1".into(),
        priority_class: 5,
        deadline: now + std::time::Duration::from_secs(120),
    });
    sched.submit(DeadlineJob {
        id: "routine-2".into(),
        priority_class: 1,
        deadline: now + std::time::Duration::from_secs(30),
    });
    // alert-1 (class 5) outranks both routine jobs (class 1) regardless of deadline.
    let next = sched.pick();
    println!("first pick: {:?}", next.map(|j| j.id));  // alert-1
    let next = sched.pick();
    println!("second pick: {:?}", next.map(|j| j.id));  // routine-2 (earlier deadline)
}
```

The two-level structure encodes the policy: priority dominates across classes; within a class, EDF orders by deadline. This is the standard pattern for mixed-criticality real-time scheduling.

### Weighted Fair-Share Scheduling

```rust,edition2021
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct TenantJob {
    pub tenant: String,
    pub job_id: String,
}

pub struct FairShareScheduler {
    weights: HashMap<String, f64>,
    consumed: HashMap<String, f64>,  // virtual time consumed per tenant
    queues: HashMap<String, Vec<TenantJob>>,
}

impl FairShareScheduler {
    pub fn new() -> Self {
        Self {
            weights: HashMap::new(),
            consumed: HashMap::new(),
            queues: HashMap::new(),
        }
    }

    pub fn add_tenant(&mut self, tenant: &str, weight: f64) {
        self.weights.insert(tenant.to_string(), weight);
        self.consumed.entry(tenant.to_string()).or_insert(0.0);
        self.queues.entry(tenant.to_string()).or_default();
    }

    pub fn submit(&mut self, job: TenantJob) {
        let queue = self.queues.entry(job.tenant.clone()).or_default();
        queue.push(job);
    }

    /// Pick the tenant whose virtual-time consumption is lowest relative to
    /// their weight. This is the deficit-round-robin-style fair-share algorithm.
    pub fn pick(&mut self) -> Option<TenantJob> {
        // Find the tenant with the smallest consumed/weight ratio that has a
        // pending job. Lower ratio = further behind fair = picked next.
        let next_tenant = self
            .consumed
            .iter()
            .filter(|(t, _)| self.queues.get(*t).map(|q| !q.is_empty()).unwrap_or(false))
            .min_by(|(t1, c1), (t2, c2)| {
                let w1 = self.weights.get(*t1).copied().unwrap_or(1.0);
                let w2 = self.weights.get(*t2).copied().unwrap_or(1.0);
                let r1 = **c1 / w1;
                let r2 = **c2 / w2;
                r1.partial_cmp(&r2).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(t, _)| t.clone())?;

        let queue = self.queues.get_mut(&next_tenant)?;
        let job = queue.remove(0);
        // Advance the picked tenant's virtual time. The increment is the
        // "size" of the work; for simplicity here we treat each job as 1 unit.
        *self.consumed.entry(next_tenant).or_insert(0.0) += 1.0;
        Some(job)
    }
}

fn main() {
    let mut sched = FairShareScheduler::new();
    sched.add_tenant("pacific", 3.0);  // weight 3
    sched.add_tenant("indian", 1.0);   // weight 1

    for i in 0..6 {
        sched.submit(TenantJob {
            tenant: "pacific".into(),
            job_id: format!("p-{}", i),
        });
    }
    for i in 0..6 {
        sched.submit(TenantJob {
            tenant: "indian".into(),
            job_id: format!("i-{}", i),
        });
    }

    // Pacific has 3x the weight; over the next 12 picks, expect roughly 9 pacific, 3 indian.
    let mut counts = (0, 0);
    for _ in 0..12 {
        let job = sched.pick().unwrap();
        if job.tenant == "pacific" { counts.0 += 1; } else { counts.1 += 1; }
    }
    println!("pacific: {}, indian: {}", counts.0, counts.1);  // ~9,3
}
```

The weighted fair-share gives each tenant a guaranteed share under contention. When the queue is unbalanced (tenant A has many jobs, tenant B has none), tenant A gets all the capacity it can use; when both compete, they get their proportional share.

## Key Takeaways

- FIFO is fair-by-arrival but vulnerable to convoy effects and priority blindness. It's the right tool only for genuinely homogeneous workloads with no priority differentiation.
- Priority scheduling lets high-priority work preempt routine work, at the cost of starvation risk for low-priority work. Priority aging is the standard defense against starvation; priority inheritance is the defense against priority inversion.
- Fair-share scheduling allocates resources by tenant proportion rather than per-job FIFO. Weighted variants give each tenant a guaranteed share that scales with the weight; unused capacity flows to other tenants on demand.
- Earliest Deadline First is optimal for hard real-time on a single processor but degrades badly under overload. Combine with priority classes for mixed-criticality workloads; add admission control to prevent the cascade-miss failure mode.
- When workloads are genuinely different, multiple schedulers are simpler and more operable than one universal scheduler. Partition by workload class and accept some inefficiency at the boundaries.

> **Source note:** This lesson synthesizes from training knowledge plus the canonical scheduling references. Liu & Layland, "Scheduling Algorithms for Multiprogramming in a Hard-Real-Time Environment" (Journal of the ACM, 1973) is the EDF original. Multi-level feedback queue traces to Corbató et al.'s CTSS work in the 1960s. Fair-share scheduling at scale traces through the Hadoop Fair Scheduler and the Linux Completely Fair Scheduler. DDIA does not treat scheduling directly; this content is synthesized and should be cross-checked against an operating systems text (Tanenbaum, *Modern Operating Systems*) or a scheduler-specific reference. *Foundations of Scalable Systems* was unavailable; the operational guidance here would normally cite that text.

{{#quiz quiz-1.toml}}
