# Module 05 — Distributed Coordination

> *"Two registry instances each believed they held the daily-merge lock. The bug was not in the lock service. The bug was in treating a distributed lock like a single-machine mutex."*

## Mission Context

Modules 1 through 4 covered the foundations: the failure model, replication, consensus, fault tolerance. This module covers the **coordination primitives** built on those foundations: distributed locks and leases (Lesson 1), service discovery and load balancing (Lesson 2), and gossip protocols (Lesson 3). These primitives are what real services use to call each other, share state, and avoid stepping on each other's resources. They are also where the previous modules' theoretical hazards (clock unreliability, partial failure, replication lag) become operational realities.

The three lessons connect: **locks** use consensus from Module 3 plus fencing tokens (themselves a Module 4 idea); **discovery** uses failure detection from Module 4 combined with the load-balancing algorithms introduced here; **gossip** is the scale-up alternative to centralized registries, with its own consistency model that requires the vector-clock and CRDT thinking from Modules 1 and 2.

The opening incidents — the April lock-without-fencing corruption (Lesson 1), the static-config-file decay (Lesson 2), the central-registry CPU saturation (Lesson 3) — are the standard operational pathologies of growing distributed systems. Knowing the patterns is what lets you choose the right primitive at design time rather than discover the failure at incident time.

## Lessons

| # | Title | Source |
|---|---|---|
| 1 | [Distributed Locks and Leases](./lesson-01-distributed-locks-leases.md) | DDIA Ch. 10 + Kleppmann's fencing tokens post |
| 2 | [Service Discovery and Load Balancing](./lesson-02-service-discovery.md) | Synthesis + Mitzenmacher 2001 + Karger et al. 1997 |
| 3 | [Gossip Protocols](./lesson-03-gossip-protocols.md) | Demers 1987 + Das/Gupta/Motivala 2002 (SWIM) |

## Project

[Telemetry Gossip](./project.md) — implement a SWIM-based gossip layer that replaces a central health registry for a 250-node edge fleet. The project covers the membership-state structures, the SWIM indirect-probe failure detection, the push-pull gossip exchange, and a deterministic test harness that verifies convergence properties and partition behavior at scale.

## Position

Module 5 of 6 in the Distributed Systems track.

## What You Should Be Able to Do After This Module

- Use a distributed lock service correctly: leases with explicit renewal, fencing tokens validated at the protected resource, monotonic-time renewal loops, halt-on-lease-loss semantics.
- Recognize the holder-pauses failure mode and other ways that distributed locks differ from single-machine mutexes.
- Choose between client-side and server-side service discovery for a given workload and articulate the operational tradeoffs.
- Implement and tune load balancing algorithms: round-robin, P2C, consistent hashing with virtual nodes, weighted variants.
- Choose between centralized registry and gossip-based membership based on cluster size and consistency requirements.
- Reason about gossip propagation time, fanout, and bandwidth as a function of cluster size, and tune the protocol's parameters against the operational regime.

## Source Materials

- **DDIA 2nd Edition** (Kleppmann & Riccomini, 2026), Chapter 10 — "Membership and Coordination Services." The primary direct source for Lesson 1.
- **Martin Kleppmann, "How to do distributed locking"** (kleppmann.com, 2016) — the canonical public reference for fencing tokens.
- **Hunt et al., "ZooKeeper: Wait-free coordination for Internet-scale systems"** (USENIX ATC 2010) — ZooKeeper's foundational paper.
- **Karger et al., "Consistent Hashing and Random Trees"** (STOC 1997) — the consistent-hashing original.
- **Mitzenmacher, "The Power of Two Choices in Randomized Load Balancing"** (IEEE TPDS 2001) — the P2C analysis.
- **Demers et al., "Epidemic Algorithms for Replicated Database Maintenance"** (PODC 1987) — the gossip-protocol foundational paper.
- **Das, Gupta, Motivala, "SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol"** (DSN 2002) — the SWIM paper.
- The Envoy, NGINX, and Hashicorp Consul/Serf documentation for production-grade reference implementations of the patterns covered.

> **Track-level synthesis note:** *Foundations of Scalable Systems* — the source book originally planned for parts of Lessons 2 and 3 — was not available during authoring. Synthesized content is flagged in each lesson's source note. Specific operational parameters (lease durations, gossip periods, P2C fanout) are illustrative; production deployments should calibrate against the actual workload.
