# Module 03 — LSM Trees & Compaction

**Track:** Database Internals — Orbital Object Registry  
**Position:** Module 3 of 6  
**Source material:** *Database Internals* — Alex Petrov, Chapter 7; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 3; *Mini-LSM Course* — Alex Chi Z  
**Quiz pass threshold:** 70% on all three lessons to unlock the project

---

## Mission Context

> **SDA INCIDENT REPORT — OOR-2026-0044**  
> **Classification:** PERFORMANCE DEFICIENCY  
> **Subject:** B+ tree write throughput insufficient for fragmentation event ingestion  
>  
> During the Cosmos-2251 debris cascade simulation, the OOR must ingest 12,000 new TLE records in under 60 seconds. The B+ tree index from Module 2 achieves ~200 inserts/second — each insert requires a root-to-leaf traversal and potential node split, generating 2-4 random page writes per insert. At this rate, ingesting 12,000 records takes 60 seconds, consuming the entire conjunction window.
>  
> **Directive:** Evaluate and implement an LSM-tree-based storage architecture. LSM trees batch writes in memory and flush them as immutable sorted files, converting random writes to sequential writes. The tradeoff: reads become more expensive (must check multiple files), but write throughput increases by 10–100x.

The LSM tree (Log-Structured Merge Tree) is the dominant architecture for write-heavy storage engines. RocksDB, LevelDB, Cassandra, HBase, CockroachDB, and TiKV all use LSM-tree variants. Where the B+ tree is optimized for read-heavy workloads with moderate writes, the LSM tree is optimized for write-heavy workloads where reads can tolerate checking multiple sorted structures.

This module covers the full LSM architecture: memtables, sorted string tables (SSTs), the write and read paths, compaction strategies, and read optimizations (bloom filters, block cache). It draws on the mini-lsm course structure and the LSM coverage in *Database Internals* Chapter 7.

---

## Learning Outcomes

After completing this module, you will be able to:

1. Describe the LSM write path (memtable → immutable memtable → SST flush) and explain why it converts random writes to sequential I/O
2. Implement a memtable backed by a sorted data structure (e.g., `BTreeMap`) and flush it to an immutable SST file
3. Design an SST file format with data blocks, index blocks, and metadata blocks
4. Explain the three amplification factors (read, write, space) and how compaction strategies trade between them
5. Compare leveled, tiered, and FIFO compaction strategies and choose the appropriate strategy for a given workload
6. Implement bloom filters and a block cache to reduce read amplification in an LSM engine

---

## Lesson Summary

### Lesson 1 — Memtables and Sorted String Tables

The LSM write path: how writes are batched in an in-memory memtable, frozen into immutable memtables, and flushed to sorted string table (SST) files on disk. The SST file format: data blocks, index blocks, bloom filter blocks, and footer. How the read path probes the memtable first, then SSTs from newest to oldest.

*Key question:* Why does the LSM tree maintain both a mutable and one or more immutable memtables instead of writing directly from the mutable memtable to disk?

### Lesson 2 — Compaction Strategies

The core problem: as SSTs accumulate, reads slow down (more files to check) and space grows (deleted keys aren't reclaimed until compacted). Compaction merges SSTs to reduce read amplification and reclaim space, at the cost of write amplification. Leveled compaction, tiered (universal) compaction, and FIFO compaction — each trades differently between read, write, and space amplification.

*Key question:* Can you design a compaction strategy that minimizes all three amplification factors simultaneously?

### Lesson 3 — Bloom Filters, Block Cache, and Read Optimization

LSM reads are expensive — they must check the memtable plus potentially every SST level. Bloom filters let the engine skip SSTs that definitely don't contain the target key. The block cache keeps hot SST data blocks in memory. Together, they reduce the effective read amplification from O(levels) to near O(1) for point lookups.

*Key question:* A bloom filter with a 1% false positive rate eliminates 99% of unnecessary SST reads. What is the cost of increasing it to 0.1%?

---

## Capstone Project — LSM-Backed TLE Storage Engine

Build an LSM storage engine for the Orbital Object Registry that supports `put`, `get`, `delete`, and `scan` operations. The engine must implement memtable→SST flush, a basic leveled compaction strategy, and bloom filters for point lookup optimization. Full project brief in `project-lsm-engine.md`.

---

## File Index

```
module-03-lsm-trees-compaction/
├── README.md                          ← this file
├── lesson-01-memtables-ssts.md        ← Memtables and sorted string tables
├── lesson-01-quiz.toml                ← Quiz (5 questions)
├── lesson-02-compaction.md            ← Compaction strategies
├── lesson-02-quiz.toml                ← Quiz (5 questions)
├── lesson-03-read-optimization.md     ← Bloom filters, block cache, read path
├── lesson-03-quiz.toml                ← Quiz (5 questions)
└── project-lsm-engine.md             ← Capstone project brief
```

---

## Prerequisites

- Module 1 (Storage Engine Fundamentals) — page I/O concepts
- Module 2 (B-Tree Index Structures) — understanding of B+ tree tradeoffs (to compare against)

## What Comes Next

Module 4 (Write-Ahead Logging & Recovery) adds durability to the LSM engine. Currently, a crash loses all data in the memtable (which is in-memory only). The WAL ensures that every write is persisted before being acknowledged, and the recovery process replays the WAL to rebuild the memtable after a crash.
