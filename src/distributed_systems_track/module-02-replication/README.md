# Module 02 — Replication Strategies

> *"The Constellation Catalog serves twelve ground stations across five continents. Forcing every TLE write through a single Virginia primary is no longer a viable design."*

## Mission Context

Module 1 established that the network is unreliable and time is unreliable. Module 2 introduces the first family of mechanisms for *building reliable systems on top of those unreliable substrates*: replication. The catalog you operate cannot survive a datacenter failure with a single instance; it cannot serve global reads with acceptable latency from a single region; and it cannot accept writes during inter-region partitions without giving up some property the team must choose deliberately.

The three lessons in this module walk through the three replication shapes that real systems use. **Single-leader** is the default: one writer, many readers, well-understood failure modes, the basis for almost every production database. **Multi-leader and leaderless** trade that simplicity for write availability across regions and partitions, at the cost of conflict resolution. **Read consistency under lag** is the operational discipline that makes either model usable at scale — the menu of session guarantees that bound what a user can observe.

The opening incidents — the catalog's earlier failovers (Lesson 1), the MSS-17 attitude overwrite extended into the multi-leader regime (Lesson 2), and the three categories of read anomaly that appear after enabling follower-served reads (Lesson 3) — are not edge cases. They are the standard operational landscape of any system that has decided to replicate.

## Lessons

| # | Title | Source |
|---|---|---|
| 1 | [Single-Leader Replication](./lesson-01-single-leader.md) | DDIA Ch. 6 |
| 2 | [Multi-Leader and Leaderless Replication](./lesson-02-multi-leader-leaderless.md) | DDIA Ch. 6 + Dynamo paper |
| 3 | [Read Consistency and Replication Lag](./lesson-03-read-consistency.md) | DDIA Ch. 6 + Bayou 1994 |

## Project

[Replicated Telemetry Store](./project.md) — a Rust crate implementing single-leader replication with synchronous and asynchronous followers, a pluggable read router supporting three modes (Leader / AnyFollower / SessionConsistent), and a test suite that demonstrates each replication-lag anomaly and shows the session guarantees that eliminate them. Includes a partition-durability test that promotes the synchronous follower after a leader crash with no lost acknowledged writes.

## Position

Module 2 of 6 in the Distributed Systems track.

## What You Should Be Able to Do After This Module

- Read a system's replication configuration (sync vs async, leader topology, conflict policy) and predict its behavior under follower crash, leader crash, and partition.
- Choose between WAL-shipping, statement-based, and logical replication for a specific operational scenario, articulating the upgrade-path tradeoffs of each.
- Diagnose a multi-leader or leaderless system's conflict-resolution policy by inspecting the data structures it uses to detect concurrency (vector clocks, last-write-wins timestamps, CRDTs).
- Map a workload to the right read consistency mode: linearizable (leader), eventually consistent (any follower), session-consistent (LSN tokens), or bounded staleness (lag-aware routing).
- Implement read-after-write and monotonic-reads session guarantees on top of a leader/follower replication system without modifying the underlying storage engine.

## Source Materials

- **DDIA 2nd Edition** (Kleppmann & Riccomini, 2026) — Chapter 6 ("Replication") is the primary source for all three lessons. The chapter's treatment of single-leader, multi-leader, leaderless, and replication-lag anomalies is the most rigorous public reference.
- **DeCandia et al., "Dynamo: Amazon's Highly Available Key-value Store"** (SOSP 2007) — the foundational paper for the leaderless model and the source of the N/W/R quorum framework. Strongly recommended supplemental reading for Lesson 2.
- **Shapiro et al., "Conflict-Free Replicated Data Types"** (SSS 2011) — the formal treatment of CRDTs. Lesson 2 introduces the G-Set as an example; this paper is the reference for the full taxonomy.
- **Terry et al., "Session Guarantees for Weakly Consistent Replicated Data"** (PDIS 1994) — the Bayou paper that formalized the four session guarantees (read-your-writes, monotonic reads, monotonic writes, writes-follow-reads). The canonical reference for Lesson 3.

Source notes on individual lessons flag where content has been synthesized beyond the available source material.
