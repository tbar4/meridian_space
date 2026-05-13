# Module 03 — Consensus & Raft

> *"The Antarctic relay path has a 14-minute coverage gap. During that gap, the catalog's leader-election runbook calls for a human operator to declare promotion. The November storm proved this doesn't scale."*

## Mission Context

Modules 1 and 2 established the failure model (the network and time are unreliable) and the basic replication strategies that build reliability on top. This module covers the consensus mechanisms that allow a cluster to make safe, automatic decisions despite that unreliability — most importantly, *which node is the leader*, but also which writes are committed, which membership changes have taken effect, and which transactions have been atomically committed.

Consensus is theoretically difficult (FLP impossibility) and practically achievable (Raft, Paxos under partial synchrony). The three lessons in this module trace that arc: why consensus is hard and why it is the right tool; how Raft works at the level of detail needed to implement and operate it; and what the protocol's operational regime — membership changes, log compaction, partition recovery — looks like in production. The capstone project asks you to implement a working Raft library and demonstrate it survives adversarial scheduling.

Once a team operates a Raft cluster, they have moved from a "single leader with manual failover" architecture to a "cluster that elects its own leader" architecture. This is the architectural inflection point that the rest of the Constellation Network depends on — coordination services (Module 5), distributed locks, fencing tokens, and most automated cluster management all build on consensus.

## Lessons

| # | Title | Source |
|---|---|---|
| 1 | [Why Consensus Is Hard](./lesson-01-why-consensus-is-hard.md) | DDIA Ch. 10 + FLP 1985 |
| 2 | [The Raft Algorithm — Leader Election & Log Replication](./lesson-02-raft-algorithm.md) | Raft paper (Ongaro & Ousterhout 2014) |
| 3 | [Raft in Practice — Membership, Snapshots, Recovery](./lesson-03-raft-in-practice.md) | Raft paper + Ongaro dissertation |

## Project

[Orbital Raft](./project.md) — implement a Raft consensus library in Rust. The project covers the full protocol: leader election, log replication, persistent state, single-server membership changes, snapshots and InstallSnapshot, and a test harness that injects partitions and verifies both safety (no two leaders in a term, no committed entry lost) and liveness (the cluster elects a leader and commits commands within bounded time) under adversarial conditions.

## Position

Module 3 of 6 in the Distributed Systems track.

## What You Should Be Able to Do After This Module

- Explain why consensus is required for linearizable operations and automatic leader election, and articulate the FLP impossibility and how Raft sidesteps it.
- Trace a write through Raft from client submission to commit to state-machine application, identifying every persistence and quorum-acknowledgment step along the way.
- Identify the election restriction in a Raft implementation and explain why it is safety-critical.
- Diagnose a Raft cluster's operational state from its metrics: leader stability, commit latency, election frequency, snapshot rate.
- Choose between read-index and lease-read for linearizable reads, and articulate the clock-skew dependency of the lease-read approach.
- Reason about cluster behavior under partition scenarios — which side makes progress, what happens to uncommitted entries when the partition heals, and why odd-numbered clusters are preferred.

## Source Materials

- **DDIA 2nd Edition** (Kleppmann & Riccomini, 2026), Chapter 10 — "Consensus" and "Consensus in Practice." The chapter framing for why consensus is needed and how it is structurally equivalent to atomic commit, linearizable CAS, and total-order broadcast.
- **Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm"** (USENIX ATC 2014) — the canonical Raft paper. Required reading; readable; rigorous. This module's pedagogy follows the paper's decomposition into election, replication, and safety.
- **Ongaro, "Consensus: Bridging Theory and Practice"** (Stanford PhD dissertation, 2014) — the deep reference for the operational concerns covered in Lesson 3: membership changes, snapshots, linearizable reads, performance optimizations.
- **Fischer, Lynch, Paterson, "Impossibility of Distributed Consensus with One Faulty Process"** (Journal of the ACM, April 1985) — the original FLP impossibility paper. Foundational but dense; the dissertation discussion in Lesson 1 is sufficient unless you want the formal proof.
- **etcd raft library source** (github.com/etcd-io/raft) — a high-quality production Raft reference. Useful for comparing design choices to your project implementation.

Source notes on individual lessons flag specific claims for verification.
