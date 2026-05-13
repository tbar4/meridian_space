# Module 06 — Scheduling and Resource Management

> *"Today the system handles roughly 800 satellite passes per day. The next-quarter plan adds 30 more satellites and three more ground stations, projecting 1,400 passes per day. The existing scheduler — a Python script that runs first-fit placement on a single thread with no priority awareness — already drops roughly 4% of pass-window jobs during peak hours."*

## Mission Context

The previous five modules built the foundations: failure model, replication, consensus, fault tolerance, coordination. This final module covers the discipline that puts work onto resources: **scheduling** (which job runs next) and **resource allocation** (which machine runs it). Where Modules 1-5 made the system *correct under failure*, Module 6 makes the system *efficient under load*.

The three lessons cover three layers of the problem. **Scheduling algorithms** (Lesson 1) — FIFO, priority, EDF, fair-share, MLFQ — are the policy primitives that decide ordering when jobs compete for the same resource. **Resource allocation** (Lesson 2) — bin-packing, multi-dimensional matching, headroom, overcommitment, eviction — is the placement layer that maps jobs to machines. **Work stealing and task migration** (Lesson 3) — Chase-Lev deques, in-process redistribution, cluster-level migration — handle the imbalance that emerges after placement.

The track converges in the capstone project. The Pass Window Scheduler is the integration of every pattern in this module and several from earlier modules: priority+EDF ordering with fair-share across regions; best-fit multi-dimensional placement with headroom and constraints; work-stealing across scheduler workers; cluster-level task migration when load becomes imbalanced. The scheduler is what would replace the Python prototype that currently drops 4% of pass-window jobs.

The lessons are synthesis-heavy because scheduling is treated only briefly in DDIA and the canonical references (Borg paper, Kubernetes scheduler docs, operating systems texts) are scattered. Source notes call out where claims are synthesized versus cited.

## Lessons

| # | Title | Source |
|---|---|---|
| 1 | [Scheduling Algorithms](./lesson-01-scheduling-algorithms.md) | Synthesis + Liu & Layland 1973 + OS texts |
| 2 | [Resource Allocation and Bin Packing](./lesson-02-resource-allocation.md) | Synthesis + Borg paper + Kubernetes docs |
| 3 | [Work Stealing and Task Migration](./lesson-03-work-stealing.md) | Blumofe & Leiserson 1994 + Chase & Lev 2005 + tokio docs |

## Project

[Pass Window Scheduler](./project.md) — implement a production-quality scheduler that integrates priority+EDF discipline, best-fit multi-dimensional placement, work-stealing across scheduler workers, and cluster-level migration. The required acceptance criteria include a simulated 1,400-jobs-per-day workload meeting 99.9% deadline compliance, region-fair throughput, and no scheduler thread bottleneck. The capstone for the entire Distributed Systems track.

## Position

Module 6 of 6 in the Distributed Systems track — the final module.

## What You Should Be Able to Do After This Module

- Choose a scheduling discipline (FIFO, priority+aging, EDF, fair-share, MLFQ, or a combination) appropriate for a given workload and explain the failure modes each is and is not vulnerable to.
- Identify scheduling pathologies in production systems by name: starvation, priority inversion, convoy effects, thundering herd, deadline-miss cascades.
- Apply bin-packing algorithms (first-fit, best-fit, worst-fit, decreasing variants) and choose the right one based on workload characteristics.
- Reason about the utilization vs latency tradeoff using queueing theory's `1/(1-ρ)` formula, and pick target utilizations matched to each workload's latency budget.
- Compose headroom, overcommitment, and eviction policies as complementary mechanisms rather than as alternatives.
- Distinguish in-process work stealing (fine-grained, continuous) from cluster-level task migration (coarse-grained, infrequent), and recognize the workloads where neither is appropriate.

## Source Materials

- **Liu & Layland, "Scheduling Algorithms for Multiprogramming in a Hard-Real-Time Environment"** (Journal of the ACM, 1973) — the EDF and rate-monotonic original.
- **Blumofe & Leiserson, "Scheduling Multithreaded Computations by Work Stealing"** (FOCS 1994; JACM 1999) — the canonical work-stealing paper.
- **Chase & Lev, "Dynamic Circular Work-Stealing Deque"** (SPAA 2005) — the standard lock-free deque used by Tokio, Go, and Rayon.
- **Verma et al., "Large-scale cluster management at Google with Borg"** (EuroSys 2015) — the canonical reference for integrated cluster scheduling.
- **Coffman, Garey, Johnson, "Approximation Algorithms for Bin Packing: A Survey"** (in Hochbaum, *Approximation Algorithms for NP-Hard Problems*, 1996) — bin-packing theory reference.
- **Kubernetes' scheduler documentation** (kubernetes.io/docs/concepts/scheduling-eviction/) — the practical production reference for filtering, scoring, taints/tolerations, affinity.
- **Tokio runtime documentation** (tokio.rs/blog) — the production Rust work-stealing implementation.
- A general operating-systems text (Tanenbaum, *Modern Operating Systems*; Silberschatz, *Operating System Concepts*) for the MLFQ and OS-scheduler-level material.

> **Track-level synthesis note:** *Foundations of Scalable Systems* was unavailable as a source during authoring. The scheduling-and-resource-management chapters of that book are the natural companion text to this module; the synthesized material here should be cross-checked against it before publication. Specific operational parameters (15% headroom, 30% migration threshold, work-stealing fanout) are illustrative defaults — production deployments should calibrate against actual workload characteristics.
