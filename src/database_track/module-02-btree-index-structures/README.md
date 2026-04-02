# Module 02 — B-Tree Index Structures

**Track:** Database Internals — Orbital Object Registry  
**Position:** Module 2 of 6  
**Source material:** *Database Internals* — Alex Petrov, Chapters 2, 4–6; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 3  
**Quiz pass threshold:** 70% on all three lessons to unlock the project

---

## Mission Context

> **SDA INCIDENT REPORT — OOR-2026-0043**  
> **Classification:** PERFORMANCE DEFICIENCY  
> **Subject:** NORAD catalog ID lookups require full page scans  
>  
> The page manager from Module 1 stores TLE records but provides no way to locate a specific record without scanning every page. A conjunction query for NORAD ID 25544 (ISS) currently reads all data pages sequentially — O(N) in the number of pages. With 100,000 tracked objects across ~2,000 data pages, a single point lookup takes 2–5ms. During a pass window with 500 conjunction checks per second, this saturates the I/O subsystem.
>  
> **Directive:** Build a B+ tree index over NORAD catalog IDs. Point lookups must be O(log N) in the number of records. Range scans over contiguous NORAD ID ranges must traverse only the relevant leaf pages.

The B-tree is the most widely deployed index structure in database systems. PostgreSQL, MySQL/InnoDB, SQLite, and most file systems use B-tree variants for ordered key lookups. This module covers the structure, invariants, and maintenance operations (splits and merges) that keep the tree balanced under insert and delete workloads.

---

## Learning Outcomes

After completing this module, you will be able to:

1. Describe the B-tree invariants (minimum fill factor, sorted keys, balanced height) and explain why they guarantee O(log N) lookups
2. Implement node split and merge operations that maintain B-tree balance on insert and delete
3. Distinguish between B-trees and B+ trees, and explain why B+ trees are preferred for range scans and disk-based storage
4. Implement a B+ tree leaf-level linked list for efficient range scans over NORAD ID ranges
5. Analyze the I/O cost of B-tree operations in terms of tree height and page size
6. Integrate a B+ tree index with the page manager from Module 1

---

## Lesson Summary

### Lesson 1 — B-Tree Structure: Keys, Pointers, and Invariants

The B-tree data structure: internal nodes, leaf nodes, the fill factor invariant, and the guarantee of O(log N) height. How keys and child pointers are arranged within a node, and how the tree is traversed for point lookups.

*Key question:* What is the maximum height of a B-tree indexing 100,000 NORAD IDs with a branching factor of 200?

### Lesson 2 — Node Splits and Merges

Maintaining B-tree balance under writes. How inserts cause node splits (bottom-up), how deletes cause node merges or redistributions, and how these operations propagate up the tree. The difference between eager and lazy merge strategies.

*Key question:* Can a single insert into a B-tree with height H cause more than H page writes?

### Lesson 3 — B+ Trees and Range Scans

The B+ tree variant: all data in leaf nodes, internal nodes hold only separator keys, and leaf nodes are linked for sequential access. Why this layout is optimal for disk-based storage engines that need both point lookups and range scans.

*Key question:* Why do B+ trees outperform B-trees for range scans even when both have the same height?

---

## Capstone Project — B+ Tree TLE Index Engine

Build a B+ tree index over NORAD catalog IDs that supports point lookups, range scans, inserts, and deletes. The index is backed by the page manager from Module 1 — each B+ tree node is a slotted page in the buffer pool. Full project brief in `project-btree-index.md`.

---

## File Index

```
module-02-btree-index-structures/
├── README.md                          ← this file
├── lesson-01-btree-structure.md       ← B-tree structure and invariants
├── lesson-01-quiz.toml                ← Quiz (5 questions)
├── lesson-02-splits-merges.md         ← Node splits and merges
├── lesson-02-quiz.toml                ← Quiz (5 questions)
├── lesson-03-bplus-trees.md           ← B+ trees and range scans
├── lesson-03-quiz.toml                ← Quiz (5 questions)
└── project-btree-index.md             ← Capstone project brief
```

---

## Prerequisites

- Module 1 (Storage Engine Fundamentals) completed
- Understanding of slotted pages and the buffer pool

## What Comes Next

Module 3 (LSM Trees & Compaction) introduces a fundamentally different index structure optimized for write-heavy workloads. The B+ tree you build here is read-optimized — every insert modifies pages in-place, which is expensive for high write throughput. The LSM tree takes the opposite approach: batch writes in memory and flush them to immutable files. Understanding both structures and their tradeoffs is essential for choosing the right approach for the OOR's workload.
