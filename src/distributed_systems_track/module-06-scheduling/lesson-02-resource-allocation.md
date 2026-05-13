# Lesson 2: Resource Allocation and Bin Packing

## Context

The Constellation Network's compute fleet runs at 38% average CPU utilization. Half the machines are idle most of the time; the other half are full enough that new pass-window jobs are routinely rejected for lack of capacity. The configuration is correct in the sense that no single machine is overloaded; the configuration is wrong in the sense that the fleet has more aggregate capacity than the workload requires, but the workload is poorly distributed across it. The catalog spends roughly $480k per quarter on machines that are mostly idle, while the pass-window scheduler regularly drops jobs for lack of room.

The problem is not the scheduling discipline from Lesson 1 (priorities and deadlines are working fine). The problem is **placement**: when a new job arrives, which machine should run it? The naive answer — "any machine with enough free capacity" — produces the observed distribution: the first-fit machine accumulates jobs until full; subsequent machines get the residual; one or two machines stay mostly empty because no individual job needs the room they offer. This is a **bin-packing problem**, and the answers differ in important ways from the scheduling problems of Lesson 1.

This lesson covers resource allocation algorithms — first-fit, best-fit, worst-fit, bin-packing approximations — and the operational practices (overcommitment, headroom reservation, eviction) that make them work in real systems. By the end, you should be able to choose a placement algorithm for a given workload, recognize the fragmentation patterns that cause utilization gaps, and understand the tradeoff between high utilization (good for cost) and low queuing delay (good for latency).

## Core Concepts

### The Bin-Packing Problem

Classical bin-packing: given items of various sizes, pack them into the minimum number of bins of fixed capacity. The problem is NP-hard in general. Real schedulers don't solve it optimally; they use approximation algorithms.

**First-Fit.** Place each item in the first bin that has room. Simple, fast, online (handles items as they arrive without seeing the full sequence). Known to use at most 1.7× the optimal number of bins for any input.

**Best-Fit.** Place each item in the bin with the smallest residual that still fits. Tends to fill bins tightly, leaving small remainders. Similar approximation ratio to first-fit, sometimes slightly better.

**Worst-Fit.** Place each item in the bin with the largest residual. Distributes items across bins, leaving large remainders everywhere. Better for handling subsequent large items but produces poorer utilization overall.

**Decreasing variants (First-Fit-Decreasing, Best-Fit-Decreasing).** Sort items by size before placing. Improves the approximation ratio to about 11/9 of optimal — substantially better than the online variants — but requires knowing all items in advance.

For online scheduling (items arriving over time), **best-fit-decreasing-by-priority** is a useful approximation: when placing a new job, prefer the machine with the smallest residual that still fits, breaking ties by the priority of jobs already running there. This concentrates load on a smaller number of machines, leaving others free for large incoming jobs.

### Multi-Dimensional Bin Packing

Real machines have multiple resource dimensions: CPU, memory, disk, network, GPU. A job requires some quantity of each. Bin packing along one dimension is the classical problem; along multiple dimensions, it gets considerably harder.

The standard heuristic: **dominant resource fairness** or **dot-product packing**. Each job is characterized by a vector of resource requirements; each machine by a vector of available capacity. The job fits if every dimension is satisfied. The placement algorithm picks the machine whose remaining capacity most closely matches the job's profile — minimizing the "waste" along dimensions where the job doesn't fully consume the residual.

Kubernetes' default scheduler uses something close to this. It scores each candidate machine on multiple criteria (resource fit, affinity, anti-affinity, node selectors, taints/tolerations) and picks the highest-scoring machine. The resource-fit scoring is a variant of dot-product matching.

The constellation's pass-window scheduler treats CPU, memory, and GPU as separate dimensions. A telemetry-processing job that wants 4 CPU cores and 8GB RAM is placed on the machine whose residual most closely matches `(4 CPU, 8GB, 0 GPU)`. This concentrates GPU-using jobs on GPU machines (because non-GPU jobs find their best match on non-GPU machines) and avoids the failure mode where a CPU-heavy job lands on a GPU machine, displacing future GPU-hungry workloads.

### Overcommitment and Headroom

Statistical multiplexing — the same idea that powers networking — applies to compute too. If you allocate every machine to its full nominal capacity, then any spike pushes the system into degraded performance. If you reserve **headroom** — typically 10-25% of capacity — the spikes are absorbed without spilling.

The flip side: **overcommitment** intentionally allocates more capacity than the machine has, on the theory that not all allocated jobs will use their full reservation simultaneously. Cloud providers do this aggressively; a "4 CPU" instance often shares hardware with other "4 CPU" instances on the assumption that most are not fully utilizing their allocation at any given moment.

The two practices are complementary. Headroom on individual machines prevents per-machine failure; overcommitment at the fleet level extracts more aggregate utilization. The numbers must be operationally calibrated: too much overcommitment and the spikes correlate (everyone needs CPU at the same time, and someone gets throttled); too little and the fleet is underutilized.

The catalog uses 15% per-machine headroom and 130% fleet-level overcommitment. The combination matches the observed workload profile: spikes are uncorrelated enough that 30% overcommitment is safe, and 15% per-machine headroom absorbs the burstiness of individual jobs.

### Eviction and Preemption

When the fleet is genuinely overloaded — the headroom is consumed and there's nowhere to put the next high-priority job — the scheduler has two choices: reject the job or evict an existing one to make room.

**Hard eviction** terminates a running job. The work in progress is lost; the job may be retried later when capacity returns. This is what Kubernetes does to lower-priority pods under node pressure.

**Soft eviction** sends a signal asking the job to pause or terminate gracefully. The job has a chance to checkpoint state, return partial results, or release resources cleanly. Useful when the work is non-trivial to redo.

**Suspension** pauses the job in place (writing its state to disk if necessary), runs the higher-priority job, then resumes. Conceptually clean; operationally complex.

The constellation uses hard eviction for low-priority batch work and soft eviction with a 30-second grace period for medium-priority work. P0 jobs (conjunction alerts) can preempt any lower-priority job; the lesson here is that the eviction policy is part of the priority model and should be explicit, not implicit.

### Affinity, Anti-Affinity, and Constraints

Real placement decisions are not just about resource sizes. Some constraints:

**Affinity.** This job should run on the same machine as that one (data locality), or in the same rack (network locality), or in the same region. Implementations: Kubernetes' `nodeAffinity` and `podAffinity`; HashiCorp Nomad's constraints.

**Anti-affinity.** This job should NOT run on the same machine as that one (redundancy, blast-radius limitation). Two replicas of the same service should be on different machines so a single failure doesn't take both down.

**Tagging / node selectors.** This job needs a GPU, this job needs SSD, this job needs to be in the EU for data-residency reasons. The scheduler filters candidate machines by tag match.

**Taints and tolerations.** Some machines are reserved for specific workloads (e.g., dedicated to streaming pipelines); only jobs that explicitly tolerate the taint can be placed there. Kubernetes uses this; the pattern is a defense against accidental placement on specialized machines.

The constellation's pass-window scheduler treats ground station as a hard constraint (a pass-window job must run on the station that owns the pass) and GPU availability as a tagged requirement. The scheduler's placement algorithm filters candidates by constraint first, then applies bin-packing on the remaining set.

### Utilization vs Latency Tradeoff

There is a fundamental tradeoff in placement that does not have a free-lunch resolution. Pushing utilization toward 100% — packing jobs as tightly as possible — produces high utilization but increases queuing delay (the next job is more likely to wait). Keeping utilization lower (say, 60-70%) reduces queuing delay but leaves more capacity idle.

The math, from queueing theory: a queue's expected waiting time grows as `1 / (1 - ρ)` where `ρ` is the utilization. At ρ=0.5, waiting time is reasonable; at ρ=0.9, waiting time is 10× higher; at ρ=0.95, 20× higher. This is the **queueing latency cliff** that separates "healthy under load" from "in distress under load."

The operational implication: target utilization should match the latency budget. A workload with strict latency requirements (sub-100ms response time) should target 60-70% utilization at most. A batch workload tolerant of queueing can target 85-90%. Trying to push the latency-sensitive workload to 90% utilization is the canonical failure that produces "the system worked fine yesterday and is timing out today."

The catalog runs the conjunction-alert pipeline at 50% utilization (because alerts can't wait) and the reconciliation pipeline at 85% (because reconciliation can wait). The two share underlying capacity but the scheduler's target utilization differs by workload class.

## Code Examples

### A Best-Fit Bin Packing Placement

```rust,edition2021
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct ResourceRequest {
    pub cpu_millicores: u32,
    pub memory_mb: u32,
}

#[derive(Clone, Debug)]
pub struct Machine {
    pub id: String,
    pub capacity: ResourceRequest,
    pub used: ResourceRequest,
}

impl Machine {
    pub fn residual(&self) -> ResourceRequest {
        ResourceRequest {
            cpu_millicores: self.capacity.cpu_millicores.saturating_sub(self.used.cpu_millicores),
            memory_mb: self.capacity.memory_mb.saturating_sub(self.used.memory_mb),
        }
    }

    pub fn fits(&self, req: &ResourceRequest) -> bool {
        let r = self.residual();
        r.cpu_millicores >= req.cpu_millicores && r.memory_mb >= req.memory_mb
    }

    pub fn slack_score(&self, req: &ResourceRequest) -> u64 {
        // Sum of leftover across both dimensions. Lower = tighter fit = better.
        let r = self.residual();
        (r.cpu_millicores - req.cpu_millicores) as u64
            + (r.memory_mb - req.memory_mb) as u64
    }
}

pub struct BestFitPlacer {
    machines: HashMap<String, Machine>,
}

impl BestFitPlacer {
    pub fn new(machines: Vec<Machine>) -> Self {
        Self {
            machines: machines.into_iter().map(|m| (m.id.clone(), m)).collect(),
        }
    }

    /// Pick the machine with the smallest residual that still fits the request.
    /// This concentrates load on a smaller set of machines, leaving others free
    /// for jobs that need more room.
    pub fn place(&mut self, req: ResourceRequest) -> Option<String> {
        let best = self
            .machines
            .values()
            .filter(|m| m.fits(&req))
            .min_by_key(|m| m.slack_score(&req))
            .map(|m| m.id.clone())?;

        let machine = self.machines.get_mut(&best)?;
        machine.used.cpu_millicores += req.cpu_millicores;
        machine.used.memory_mb += req.memory_mb;
        Some(best)
    }

    pub fn release(&mut self, machine_id: &str, req: ResourceRequest) {
        if let Some(m) = self.machines.get_mut(machine_id) {
            m.used.cpu_millicores = m.used.cpu_millicores.saturating_sub(req.cpu_millicores);
            m.used.memory_mb = m.used.memory_mb.saturating_sub(req.memory_mb);
        }
    }
}

fn main() {
    let machines = vec![
        Machine {
            id: "edge-a".into(),
            capacity: ResourceRequest { cpu_millicores: 8000, memory_mb: 16000 },
            used: ResourceRequest { cpu_millicores: 6000, memory_mb: 12000 },  // 2 CPU, 4GB residual
        },
        Machine {
            id: "edge-b".into(),
            capacity: ResourceRequest { cpu_millicores: 8000, memory_mb: 16000 },
            used: ResourceRequest { cpu_millicores: 1000, memory_mb: 2000 },   // 7 CPU, 14GB residual
        },
    ];

    let mut placer = BestFitPlacer::new(machines);
    // A small job (1 CPU, 2GB) should land on edge-a (tighter fit), leaving
    // edge-b free for a future larger job.
    let placed = placer.place(ResourceRequest { cpu_millicores: 1000, memory_mb: 2000 });
    println!("placed on: {:?}", placed);  // edge-a
}
```

The best-fit heuristic concentrates load. Worst-fit would have placed the job on edge-b, distributing the load more evenly but leaving smaller residuals on both machines. Either is defensible; the choice depends on whether future jobs are expected to be larger (favor worst-fit to keep big residuals) or smaller (favor best-fit to consolidate).

### Headroom-Aware Placement

```rust,edition2021
# #[derive(Clone, Debug)] struct ResourceRequest { cpu_millicores: u32, memory_mb: u32 }
# #[derive(Clone, Debug)] struct Machine { id: String, capacity: ResourceRequest, used: ResourceRequest }
# impl Machine { fn fits(&self, _r: &ResourceRequest) -> bool { true } }

pub struct HeadroomPlacer {
    machines: Vec<Machine>,
    headroom_pct: u32,  // e.g., 15 for 15%
}

impl HeadroomPlacer {
    pub fn effective_capacity(&self, m: &Machine) -> ResourceRequest {
        ResourceRequest {
            cpu_millicores: m.capacity.cpu_millicores * (100 - self.headroom_pct) / 100,
            memory_mb: m.capacity.memory_mb * (100 - self.headroom_pct) / 100,
        }
    }

    pub fn fits_with_headroom(&self, m: &Machine, req: &ResourceRequest) -> bool {
        let cap = self.effective_capacity(m);
        m.used.cpu_millicores + req.cpu_millicores <= cap.cpu_millicores
            && m.used.memory_mb + req.memory_mb <= cap.memory_mb
    }

    /// Place a job; if no machine has room within the headroom budget, try
    /// again ignoring headroom (operational override for high-priority work).
    pub fn place(&self, req: &ResourceRequest, allow_headroom_override: bool) -> Option<String> {
        // First pass: respect headroom.
        let with_headroom = self
            .machines
            .iter()
            .find(|m| self.fits_with_headroom(m, req))
            .map(|m| m.id.clone());

        if with_headroom.is_some() { return with_headroom; }

        // Second pass: only for high-priority jobs allowed to override.
        if allow_headroom_override {
            self.machines
                .iter()
                .find(|m| m.fits(req))
                .map(|m| m.id.clone())
        } else {
            None
        }
    }
}
```

The two-pass placement encodes the policy: routine jobs honor the headroom; P0 jobs can override it. The override is a deliberate choice — using up the headroom means losing the buffer against spikes — and is restricted to the workload class that justifies it.

### A Tagged-Constraint Filter

```rust,edition2021
use std::collections::HashSet;

# #[derive(Clone, Debug)] struct ResourceRequest { cpu_millicores: u32, memory_mb: u32 }

#[derive(Clone, Debug)]
pub struct TaggedMachine {
    pub id: String,
    pub tags: HashSet<String>,  // e.g., {"gpu", "ssd", "region=us-west"}
    pub capacity: ResourceRequest,
    pub used: ResourceRequest,
}

#[derive(Clone, Debug)]
pub struct ConstrainedJob {
    pub req: ResourceRequest,
    pub required_tags: HashSet<String>,
    pub anti_affinity_machines: HashSet<String>,  // don't place here
}

pub fn place_with_constraints(
    machines: &[TaggedMachine],
    job: &ConstrainedJob,
) -> Option<String> {
    machines
        .iter()
        .filter(|m| {
            // All required tags must be present.
            job.required_tags.iter().all(|t| m.tags.contains(t))
        })
        .filter(|m| !job.anti_affinity_machines.contains(&m.id))
        .filter(|m| {
            // Capacity check.
            m.capacity.cpu_millicores - m.used.cpu_millicores >= job.req.cpu_millicores
                && m.capacity.memory_mb - m.used.memory_mb >= job.req.memory_mb
        })
        .min_by_key(|m| {
            // Best-fit among the candidates.
            (m.capacity.cpu_millicores - m.used.cpu_millicores) as u64
                + (m.capacity.memory_mb - m.used.memory_mb) as u64
        })
        .map(|m| m.id.clone())
}
```

The filter-then-pack pattern is the standard structure for constrained placement: filter the candidate set by all hard constraints first, then apply the bin-packing heuristic to the remaining candidates. The filter is fast (set operations); the packing is the more interesting algorithm.

## Key Takeaways

- Bin-packing is NP-hard in general; production schedulers use approximation algorithms. First-fit and best-fit are the standard online algorithms; decreasing variants improve the approximation ratio at the cost of requiring full knowledge of items in advance.
- Multi-dimensional resource allocation (CPU + memory + GPU + ...) uses dot-product matching: pick the machine whose residual capacity vector most closely matches the job's requirement vector. Kubernetes and similar schedulers use this pattern.
- Headroom (reserved capacity per machine, typically 10-25%) and overcommitment (total fleet allocation exceeds physical capacity) are complementary practices. Headroom absorbs per-machine spikes; overcommitment extracts aggregate utilization from uncorrelated demand.
- Eviction policy is part of the priority model. Hard, soft, and suspension are the three mechanisms; the choice depends on the cost of redoing the evicted work versus the urgency of the preempting job.
- Utilization trades against latency. Target utilization should match the latency budget: latency-critical workloads cap below 70%; batch workloads can target 85-90%. Pushing latency-critical work toward 95% utilization is the canonical failure mode.

> **Source note:** This lesson is synthesized from training knowledge plus the canonical references. Bin-packing theory traces to Johnson et al. (1974) for the first approximation analyses; "Bin Packing" by Coffman, Garey, Johnson (1996) is the survey reference. Multi-dimensional packing for clusters is treated in the Borg paper (Verma et al., EuroSys 2015) and Kubernetes' scheduler documentation. The queueing-latency formula `1/(1-ρ)` is standard M/M/1 queue theory. DDIA does not treat scheduling or placement directly. *Foundations of Scalable Systems* and an operating-systems text are the natural references and were not available; cross-check before publication.

{{#quiz quiz-2.toml}}
