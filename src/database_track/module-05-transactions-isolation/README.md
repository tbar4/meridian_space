# Module 05 — Transactions & Isolation

**Track:** Database Internals — Orbital Object Registry  
**Position:** Module 5 of 6  
**Source material:** *Database Internals* — Alex Petrov, Chapters 12–13; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 7; [Mini-LSM Week 3](https://skyzh.github.io/mini-lsm/week3-overview.html)  
**Quiz pass threshold:** 70% on all three lessons to unlock the project

---

## Mission Context

> **SDA INCIDENT REPORT — OOR-2026-0046**  
> **Classification:** DATA ANOMALY  
> **Subject:** Conjunction query returned stale TLE data during concurrent catalog update  
>  
> At 14:22 UTC, a conjunction assessment for NORAD 43013 used TLE epoch 2026-084.2 while a concurrent bulk update was writing epoch 2026-084.7 for the same object. The assessment computed a miss distance of 2.3km using the stale epoch. The updated epoch would have yielded 0.8km — below the avoidance maneuver threshold. The conjunction alert was delayed by 4 minutes until the next assessment cycle picked up the updated TLE.
>  
> **Root cause:** The LSM engine provides no isolation between concurrent readers and writers. A long-running conjunction query can read a mix of old and new TLE versions, producing inconsistent results.
>  
> **Directive:** Implement multi-version concurrency control (MVCC) with snapshot isolation. Every conjunction query must see a consistent snapshot of the catalog — either entirely before or entirely after any concurrent update.

---

## Learning Outcomes

After completing this module, you will be able to:

1. Explain the ACID properties and which guarantees are provided by each isolation level
2. Implement two-phase locking (2PL) and explain why it prevents all anomalies but limits concurrency
3. Implement MVCC snapshot isolation in an LSM engine using timestamped keys
4. Explain write skew and why snapshot isolation does not prevent it
5. Design a garbage collection strategy for old MVCC versions
6. Reason about the tradeoff between isolation level and concurrent throughput

---

## Lesson Summary

### Lesson 1 — ACID Properties and Isolation Levels
What Atomicity, Consistency, Isolation, and Durability mean concretely. The isolation levels (Read Uncommitted, Read Committed, Repeatable Read, Serializable) and which anomalies each prevents.

### Lesson 2 — Two-Phase Locking (2PL)
Lock-based concurrency control. Shared and exclusive locks, the growing and shrinking phases, strict 2PL, and deadlock detection. Why 2PL is correct but limits throughput.

### Lesson 3 — MVCC and Snapshot Isolation
Multi-version concurrency control: storing multiple versions of each key with timestamps. Snapshot reads, write conflicts, and garbage collection. Adapted for the LSM architecture using timestamped keys (following Mini-LSM Week 3).

---

## Capstone Project — Conjunction Query Engine with MVCC Snapshot Reads

Add MVCC snapshot isolation to the LSM engine. Concurrent conjunction queries see consistent catalog snapshots. Concurrent writers do not block readers. Full brief in `project-conjunction-engine.md`.

---

## File Index

```
module-05-transactions-isolation/
├── README.md
├── lesson-01-acid-isolation.md
├── lesson-01-quiz.toml
├── lesson-02-two-phase-locking.md
├── lesson-02-quiz.toml
├── lesson-03-mvcc-snapshots.md
├── lesson-03-quiz.toml
└── project-conjunction-engine.md
```

---

## Prerequisites

- Module 4 (WAL & Recovery) completed

## What Comes Next

Module 6 (Query Processing) adds structured query execution on top of the transactional storage engine — the volcano iterator model, vectorized execution, and join algorithms.
