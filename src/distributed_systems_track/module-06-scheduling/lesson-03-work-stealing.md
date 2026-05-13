# Lesson 3: Work Stealing and Task Migration

## Context

The Pass Window Scheduler from Lessons 1 and 2 makes placement decisions when jobs arrive. Once placed, jobs run on their assigned machine until completion. This works when placement decisions are good and workloads are predictable. It does not work when one machine ends up with several long-running jobs while neighboring machines sit idle, or when the cost of a job turns out to be much larger than estimated at placement time, or when a machine's effective capacity changes mid-flight (a GPU thermal throttle, a noisy-neighbor VM, a brief hardware degradation).

The mechanism for handling these cases is **work redistribution after placement** — either pulling work toward idle resources (work stealing) or pushing work away from saturated ones (task migration). Tokio's runtime, the Go scheduler, Cilk, and most modern parallel-task frameworks use work stealing internally. Cluster schedulers like Borg and Kubernetes use migration for less granular cases. The two mechanisms are different in detail but solve the same problem: load imbalance that emerges after initial placement.

This lesson covers the two patterns, the data structures that make them efficient (Chase-Lev deques, ABA-resistant queues), and the operational tradeoffs (migration cost, locality loss, cache effects). By the end, you should be able to recognize when work stealing is the right pattern, understand why tokio's multi-threaded scheduler is built the way it is, and choose between fine-grained intra-process stealing and coarse-grained cross-machine migration for a given workload.

## Core Concepts

### Why Initial Placement Is Not Enough

Initial placement decisions are necessarily approximate. The scheduler estimates a job's resource requirements before placing it, but estimates can be wrong: a "5-minute" reconciliation that turns out to take 50 minutes, a "1 GB" data load that grows to 10 GB after expansion, a CPU-bound job that turns out to be memory-bound. Misestimation produces load imbalance that placement cannot anticipate.

Environmental changes also matter. A machine's effective capacity changes over time: thermal throttling, sibling VMs becoming noisy, network congestion to a downstream service that turns CPU-bound jobs into I/O-waiting ones. The placement decision was correct at the time; it's wrong now.

Finally, **work arrival patterns vary**. A region experiences a burst of jobs while another sits idle. Placement decisions made during the burst land on different machines than they would have during the quiet period. The cumulative effect is imbalance even when each individual decision was reasonable.

The fix is post-placement redistribution. Two patterns dominate.

### Work Stealing: Pull-Based Redistribution

In **work stealing**, idle workers proactively pull work from busy workers. The pattern was popularized by Cilk in 1995 (Blumofe & Leiserson) and is now standard in modern thread pools.

The structure:

1. Each worker has a local task queue (typically a double-ended queue / deque).
2. The worker pushes and pops tasks from one end (the "back") in LIFO order. LIFO is cache-friendly: recently-created tasks reference recently-touched data.
3. When a worker has no local tasks, it picks a random other worker and steals from the opposite end of that worker's deque (the "front"). FIFO from the front means stealing oldest tasks, which are typically the coarsest-grained.
4. The owner pushes/pops one end without coordination; thieves pop from the other end. This is what makes work stealing efficient — the steal operation only needs synchronization when the owner is near the same end as the thief.

The Chase-Lev deque (Chase & Lev 2005) is the standard lock-free implementation. The owner's push/pop are nearly synchronization-free; the thief's steal uses CAS to detect conflicts. Tokio's multi-threaded runtime, Rayon's parallel iterator, and the Go scheduler all use Chase-Lev-style work stealing.

The properties:

- **Excellent cache locality.** Each worker reuses tasks it created, keeping working set local. Only stealing crosses cache boundaries.
- **No central coordinator.** Each worker steals from random others; the load balances naturally across the pool.
- **Asymptotic optimality.** Theory shows work-stealing schedulers achieve near-optimal speedup on parallel-recursive workloads.
- **Cost is in steal attempts.** When everyone is busy, idle workers may attempt many steals before succeeding (or before they get new local work). Production runtimes add backoff and exponential reattempt to bound this overhead.

The constellation's per-machine scheduler — the userspace runtime that distributes work across CPU cores within a single machine — is built on tokio's work-stealing pool. The cluster-level scheduler operates at a coarser granularity.

### Task Migration: Push-Based Cross-Machine Redistribution

At the cluster level, work stealing's fine granularity isn't right. Moving a job from one machine to another is expensive: the job's working memory must be copied, file descriptors must be migrated or recreated, the job typically must be restarted (or at least paused and resumed). For per-task overhead in the hundreds of milliseconds, you don't want to migrate every time you observe imbalance.

The cluster pattern is **task migration**: a monitoring layer periodically observes load distribution and migrates specific jobs to rebalance. The migration is intentional and infrequent, triggered by significant imbalance rather than continuous balancing.

The decision logic:

1. Identify a hotspot machine (utilization significantly above the fleet average).
2. Identify a target machine (utilization significantly below the fleet average, fits the job's requirements).
3. Pick a migrateable job on the hotspot. Some jobs are not migrateable (long-running stateful tasks with significant in-memory state); others migrate easily (stateless batch jobs).
4. Pause the job on the hotspot, transfer its state to the target, resume on the target.

The migration cost (time during which the job is paused or running degraded) is the main tradeoff. Kubernetes' descheduler does this; Mesos handles it explicitly via its framework API; Borg's reschedule rate is one of the operational metrics that bounds the system's churn.

The constellation's cluster scheduler migrates low-priority batch jobs (which can tolerate the pause-and-resume) when the fleet variance in utilization exceeds 30%. P0 and P1 jobs are not migrated except in extreme cases — the migration cost is higher than the imbalance cost.

### Hybrid Patterns: Local Stealing + Cluster Migration

The two patterns are complementary, not alternatives. Modern systems use both at different scales:

- **Within a process**, work stealing across CPU cores. Tokio, Go, Rayon. Fine-grained; high frequency; low cost.
- **Within a cluster**, occasional task migration. Kubernetes, Borg, the constellation's pass-window scheduler. Coarse-grained; low frequency; significant cost.
- **Between availability zones or regions**, almost never. The cost of moving large jobs across regions is enormous; place correctly the first time.

The pattern: as granularity coarsens, redistribution frequency drops. Cache-line-granularity stealing happens millions of times per second; job-migration happens a few times per minute; cross-region migration happens during planned maintenance, not autonomously.

### The Cost of Migration

Migration is not free. The specific costs:

**Pause and resume.** The job stops on the source, doesn't do work for the migration duration, and resumes on the target. For latency-critical workloads, this pause is the entire SLO budget.

**State transfer.** The job's in-memory state must be moved. For a 4 GB job, this is bandwidth and time — and CPU on both ends to serialize and deserialize.

**Connection re-establishment.** Long-lived TCP connections, file handles, database sessions — all must be recreated. Some can be migrated transparently (Linux's `criu` can checkpoint and restore many connection states); most require the job to re-establish.

**Cache cold-start.** The target machine's local caches (CPU L1/L2/L3, page cache, application-level caches) are cold for the migrated job. The first few seconds run slower than steady-state.

**Locality loss.** If the job was placed near its data (same machine, same rack, same region), the migration may break that locality, increasing cross-network latency for the job's data accesses.

For most workloads, these costs are tolerable as long as migration is rare. For latency-critical workloads, they may exceed the latency budget, in which case migration is the wrong tool.

### Stealing Granularity and Locality Tradeoffs

Work stealing's frequency depends on granularity. Stealing every task — typical in Tokio's task queue — produces excellent load balance and acceptable overhead for in-process work. Stealing larger chunks (a thousand tasks at a time, used in some parallel frameworks) reduces overhead but produces less even balance.

The locality tradeoff: tasks that share data benefit from running on the same worker. A naive work-stealing scheduler might steal a task that needs data the original worker just loaded into cache. The data has to move to the new worker, which is slower than if the task had stayed.

Modern work-stealing schedulers add locality hints: tasks can declare data they need; the scheduler prefers to keep them on the worker that produced the data. Tokio supports this via the `local_set` API (single-threaded tasks that never migrate); some HPC frameworks expose more granular locality affinities.

The constellation's image-processing pipeline runs on tokio's standard work-stealing pool for most tasks but uses `local_set` for stages that have heavy CPU-cache-resident state. The tradeoff is deliberate: lose some load-balancing benefit for the heavy stages in exchange for keeping the working set local.

### When Not to Steal or Migrate

A few cases where the redistribution patterns are wrong:

**Workloads with strict ordering.** If task `B` must execute after task `A` and on the same machine, stealing `B` to another worker breaks the ordering or the locality. Document and enforce the ordering at the scheduler level.

**Workloads with significant per-machine warm state.** A long-running database, a model server that has loaded a multi-GB model — migrating these is much more expensive than tolerating some imbalance.

**Latency-critical real-time workloads.** Migration introduces a pause; stealing introduces queueing variance. If the latency budget is sub-millisecond, neither is appropriate.

**Workloads with affinity constraints not visible to the scheduler.** A job that has cached significant data from a specific upstream service; the scheduler doesn't know about the cache and migrates the job, breaking the implicit affinity.

In each case, the right answer is to not redistribute. The discipline is to know when redistribution is helpful and when it produces more harm than good.

## Code Examples

### A Simplified Work-Stealing Deque

```rust,edition2021
use std::collections::VecDeque;
use std::sync::Mutex;

pub struct WorkStealingDeque<T> {
    // Mutex-protected for simplicity; production implementations (Chase-Lev)
    // are lock-free with atomic operations on indices.
    inner: Mutex<VecDeque<T>>,
}

impl<T> WorkStealingDeque<T> {
    pub fn new() -> Self {
        Self { inner: Mutex::new(VecDeque::new()) }
    }

    /// Owner: push to the back. LIFO with pop_back means recent tasks first.
    pub fn push(&self, task: T) {
        self.inner.lock().unwrap().push_back(task);
    }

    /// Owner: pop from the back. Cache-friendly.
    pub fn pop(&self) -> Option<T> {
        self.inner.lock().unwrap().pop_back()
    }

    /// Thief: steal from the front. FIFO from the front means stealing oldest
    /// tasks, which are typically coarsest-grained (better steal value).
    pub fn steal(&self) -> Option<T> {
        self.inner.lock().unwrap().pop_front()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}

fn main() {
    let deque: WorkStealingDeque<u32> = WorkStealingDeque::new();
    // Owner pushes tasks
    for i in 0..10 { deque.push(i); }
    // Owner pops (LIFO)
    println!("owner pops: {:?}", deque.pop()); // 9 (most recent)
    println!("owner pops: {:?}", deque.pop()); // 8
    // Thief steals (FIFO from other end)
    println!("thief steals: {:?}", deque.steal()); // 0 (oldest)
    println!("thief steals: {:?}", deque.steal()); // 1
    println!("remaining: {}", deque.len()); // 6
}
```

The asymmetry is the point: the owner operates at one end (LIFO for cache benefits), the thief at the other (FIFO for steal-value). In the lock-free Chase-Lev variant, these operations require synchronization only when the deque is nearly empty and owner and thief converge on the same task.

### A Work-Stealing Pool of Workers

```rust,edition2021
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

# struct WorkStealingDeque<T>(std::sync::Mutex<std::collections::VecDeque<T>>);
# impl<T> WorkStealingDeque<T> {
#     fn new() -> Self { Self(std::sync::Mutex::new(std::collections::VecDeque::new())) }
#     fn push(&self, t: T) { self.0.lock().unwrap().push_back(t); }
#     fn pop(&self) -> Option<T> { self.0.lock().unwrap().pop_back() }
#     fn steal(&self) -> Option<T> { self.0.lock().unwrap().pop_front() }
# }

pub struct Pool {
    workers: Vec<Arc<WorkStealingDeque<Box<dyn FnOnce() + Send>>>>,
    shutdown: Arc<AtomicBool>,
}

impl Pool {
    pub fn new(num_workers: usize) -> Self {
        let workers: Vec<_> = (0..num_workers)
            .map(|_| Arc::new(WorkStealingDeque::new()))
            .collect();
        let shutdown = Arc::new(AtomicBool::new(false));

        for (i, worker) in workers.iter().enumerate() {
            let worker = worker.clone();
            let peers = workers.clone();
            let shutdown = shutdown.clone();
            thread::spawn(move || {
                run_worker(i, worker, peers, shutdown);
            });
        }

        Self { workers, shutdown }
    }

    pub fn submit(&self, idx: usize, task: Box<dyn FnOnce() + Send>) {
        self.workers[idx % self.workers.len()].push(task);
    }
}

fn run_worker(
    own_idx: usize,
    own_queue: Arc<WorkStealingDeque<Box<dyn FnOnce() + Send>>>,
    peers: Vec<Arc<WorkStealingDeque<Box<dyn FnOnce() + Send>>>>,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::Relaxed) {
        // Try local queue first - cache-friendly.
        if let Some(task) = own_queue.pop() {
            task();
            continue;
        }

        // Local queue is empty; try stealing.
        let mut stole = false;
        for (i, peer) in peers.iter().enumerate() {
            if i == own_idx { continue; }
            if let Some(task) = peer.steal() {
                task();
                stole = true;
                break;
            }
        }

        if !stole {
            // No work anywhere; brief sleep before retrying.
            // Production runtimes use parking with wakeup on submit;
            // sleep is a simplification.
            thread::sleep(Duration::from_millis(1));
        }
    }
}
```

This is the structural shape. Production implementations (tokio, rayon) are much more sophisticated: lock-free deques, work-stealing with backoff, park/unpark to avoid spinning when there's no work, and explicit task locality hints. The pattern, however, is the same as what's shown here.

### A Task Migration Decision

```rust,edition2021
#[derive(Clone, Debug)]
pub struct MachineLoad {
    pub id: String,
    pub utilization: f64,
}

#[derive(Clone, Debug)]
pub struct MigrateableJob {
    pub id: String,
    pub estimated_size: f64,  // expected utilization contribution
    pub migration_cost: std::time::Duration,
}

pub fn should_migrate(
    machines: &[MachineLoad],
    jobs_by_machine: &std::collections::HashMap<String, Vec<MigrateableJob>>,
    threshold: f64,  // e.g., 0.30 = migrate when variance exceeds 30 percentage points
) -> Option<(String, String, String)> {
    if machines.len() < 2 { return None; }

    let max = machines.iter().max_by(|a, b| a.utilization.partial_cmp(&b.utilization).unwrap())?;
    let min = machines.iter().min_by(|a, b| a.utilization.partial_cmp(&b.utilization).unwrap())?;

    // Only migrate if the imbalance is significant.
    if max.utilization - min.utilization < threshold { return None; }

    // Pick a migrateable job from the hotspot. Prefer jobs whose migration
    // cost is low and whose size won't overload the target.
    let jobs = jobs_by_machine.get(&max.id)?;
    let job = jobs.iter().min_by(|a, b| a.migration_cost.cmp(&b.migration_cost))?;

    // Verify the move doesn't itself produce imbalance the other way.
    if min.utilization + job.estimated_size > max.utilization - job.estimated_size {
        return None;
    }

    Some((job.id.clone(), max.id.clone(), min.id.clone()))
}

fn main() {
    let machines = vec![
        MachineLoad { id: "edge-a".into(), utilization: 0.92 },
        MachineLoad { id: "edge-b".into(), utilization: 0.45 },
        MachineLoad { id: "edge-c".into(), utilization: 0.50 },
    ];
    let mut jobs = std::collections::HashMap::new();
    jobs.insert("edge-a".to_string(), vec![
        MigrateableJob {
            id: "reconcile-batch-17".into(),
            estimated_size: 0.10,
            migration_cost: std::time::Duration::from_secs(15),
        },
    ]);

    let decision = should_migrate(&machines, &jobs, 0.30);
    println!("migration decision: {:?}", decision);
    // Expect Some((job, from, to)) because edge-a (0.92) - edge-b (0.45) > 0.30.
}
```

The decision pattern is deliberately conservative: only migrate when imbalance is large; only migrate jobs with low cost; verify the move improves balance rather than just shifting the problem. Production migration loops add more checks (job age, eviction history, anti-thrash protections) on top of these.

## Key Takeaways

- Initial placement is approximate; redistribution after placement is necessary when workloads misestimate, environments change, or arrival patterns vary. Work stealing and task migration are the two redistribution mechanisms.
- Work stealing is the right pattern for fine-grained in-process work distribution: each worker has a local deque, owners use one end (LIFO, cache-friendly), thieves the other (FIFO, coarse-grained steal-value). Tokio, Rayon, and Go's scheduler all use this pattern.
- Task migration is the right pattern for coarse-grained cluster-level redistribution: less frequent, more expensive per operation, triggered by significant imbalance rather than continuous rebalancing.
- The two patterns layer cleanly: work stealing within a process for nanosecond-granularity, task migration within a cluster for second-granularity. Cross-region migration is rare and intentional.
- Both patterns have costs (steal overhead, migration pause). The discipline is to know when redistribution helps and when its costs exceed the imbalance it would correct. Latency-critical real-time workloads typically can't tolerate either.

> **Source note:** This lesson is synthesized from training knowledge plus the canonical references. Blumofe & Leiserson, "Scheduling Multithreaded Computations by Work Stealing" (FOCS 1994; JACM 1999) is the canonical work-stealing paper. Chase & Lev, "Dynamic Circular Work-Stealing Deque" (SPAA 2005) is the standard lock-free deque. Tokio's runtime documentation (tokio.rs/blog) covers the production Rust implementation. Borg (Verma et al., EuroSys 2015) is the cluster-scale reference for migration. DDIA does not treat work stealing or task migration. *Foundations of Scalable Systems* and a parallel-programming text are the natural references and were not available; cross-check before publication.

{{#quiz quiz-3.toml}}
