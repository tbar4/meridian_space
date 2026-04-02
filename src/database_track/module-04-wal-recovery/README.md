# Module 04 — Write-Ahead Logging & Recovery

**Track:** Database Internals — Orbital Object Registry  
**Position:** Module 4 of 6  
**Source material:** *Database Internals* — Alex Petrov, Chapters 9–10; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 7  
**Quiz pass threshold:** 70% on all three lessons to unlock the project

---

## Mission Context

> **SDA INCIDENT REPORT — OOR-2026-0045**  
> **Classification:** DATA LOSS INCIDENT  
> **Subject:** 2,400 TLE records lost after unplanned power failure  
>  
> At 03:17 UTC, a PDU failure at Ground Station Bravo caused an unclean shutdown of the OOR storage engine. The active memtable contained approximately 2,400 TLE updates from the preceding 12-minute pass window. Because the memtable is a volatile in-memory structure, all 2,400 records were lost. The LSM engine restarted with only the previously flushed SSTables, leaving the catalog 12 minutes stale. Two conjunction alerts were delayed because the missing TLEs contained the most recent orbital elements for objects in a close-approach trajectory.
>  
> **Directive:** Implement a write-ahead log. Every mutation must be logged to durable storage *before* it is applied to the memtable. On crash recovery, replay the WAL to reconstruct the memtable to its pre-crash state.

---

## Learning Outcomes

After completing this module, you will be able to:

1. Explain the write-ahead rule and why it is the foundation of crash recovery
2. Implement a WAL that logs key-value operations to an append-only file with checksummed records
3. Describe the ARIES recovery protocol — analysis, redo, and undo phases
4. Implement crash recovery by replaying the WAL to reconstruct the memtable
5. Design a checkpointing strategy that limits WAL replay time after a crash
6. Reason about the tradeoff between `fsync` frequency and durability guarantees

---

## Lesson Summary

### Lesson 1 — WAL Fundamentals
The write-ahead rule, log record format, LSN ordering, and the WAL's role in the LSM write path. Why `fsync` is the only guarantee of durability, and the latency cost of calling it.

*Key question:* What is the maximum data loss window under group commit with 50ms batch intervals?

### Lesson 2 — Crash Recovery
The ARIES recovery protocol adapted for the LSM engine. Analysis phase (determine which WAL records need replay), redo phase (replay committed operations into the memtable), and how the manifest tracks SSTable state for consistent restart.

*Key question:* If the engine crashes during a compaction, does it use the old or new SSTables on recovery?

### Lesson 3 — Checkpointing
Fuzzy checkpoints that snapshot the LSM state without blocking writes. How checkpoints bound WAL replay time and enable WAL truncation. The tradeoff between checkpoint frequency and recovery time.

*Key question:* What is the maximum WAL replay time for the OOR with 60-second checkpoint intervals?

---

## Capstone Project — Durable TLE Update Pipeline

Add WAL-based durability to the Module 3 LSM engine. Every write is logged before entering the memtable. On simulated crash, the engine recovers to a consistent state by replaying the WAL. Full brief in `project-durable-pipeline.md`.

---

## File Index

```
module-04-wal-recovery/
├── README.md
├── lesson-01-wal-fundamentals.md
├── lesson-01-quiz.toml
├── lesson-02-crash-recovery.md
├── lesson-02-quiz.toml
├── lesson-03-checkpointing.md
├── lesson-03-quiz.toml
└── project-durable-pipeline.md
```

---

## Prerequisites

- Module 3 (LSM Trees & Compaction) completed

## What Comes Next

Module 5 (Transactions & Isolation) adds concurrent read/write support with MVCC snapshot isolation, building on the durable foundation established here.
