# Module 01 — Storage Engine Fundamentals

**Track:** Database Internals — Orbital Object Registry  
**Position:** Module 1 of 6  
**Source material:** *Database Internals* — Alex Petrov, Chapters 1–4; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 3  
**Quiz pass threshold:** 70% on all three lessons to unlock the project

---

## Mission Context

> **SDA INCIDENT REPORT — OOR-2026-0041**  
> **Classification:** OPERATIONAL DEFICIENCY  
> **Subject:** TLE index query latency exceeding conjunction avoidance SLA  
>  
> ESA's Space Surveillance and Tracking (SST) division has notified Meridian Space Systems that our Two-Line Element (TLE) index cannot scale past 100,000 tracked orbital objects. Current architecture stores TLE records as serialized JSON blobs in PostgreSQL — every conjunction query triggers a full table scan. With the post-fragmentation debris field projected to add 12,000 new objects this quarter, the system will exceed the 500ms conjunction query SLA within 60 days.
>  
> **Directive:** Build a purpose-built storage engine for the Orbital Object Registry. Start with the lowest layer — how bytes hit disk and come back.

This module establishes the foundational layer of the Orbital Object Registry storage engine. Before you can index, query, or recover data, you need a reliable on-disk format and an efficient way to move pages between disk and memory. Every decision made here — page size, record layout, eviction policy — propagates upward through the entire engine.

---

## Learning Outcomes

After completing this module, you will be able to:

1. Design a fixed-size page format with headers, magic bytes, and checksums for integrity verification
2. Implement a buffer pool that caches hot pages in memory and evicts cold pages using LRU or CLOCK policies
3. Explain why random I/O is the dominant cost in storage engines and how page-aligned access patterns reduce it
4. Implement a slotted page layout that supports variable-length records with in-page compaction
5. Reason about the tradeoffs between page size, I/O amplification, and internal fragmentation
6. Map TLE records to a binary page format suitable for the Orbital Object Registry

---

## Lesson Summary

### Lesson 1 — File Formats and Page Layout

How storage engines organize bytes on disk. Fixed-size pages, headers, magic bytes, and the page abstraction that separates logical records from physical storage. Why 4KB or 8KB pages align with OS and hardware boundaries.

*Key question:* Why do storage engines use fixed-size pages instead of variable-length records written sequentially?

### Lesson 2 — Buffer Pool Management

The page cache that sits between the storage engine and the OS. LRU and CLOCK eviction policies, page pinning, dirty page tracking, and the flush protocol. Why the buffer pool exists even though the OS has its own page cache.

*Key question:* When should a storage engine bypass the OS page cache and manage its own buffer pool?

### Lesson 3 — Slotted Pages

How to store variable-length records within a fixed-size page. The slot array, free space pointer, and in-page compaction. How deletions create fragmentation and how the engine reclaims space without rewriting the entire page.

*Key question:* How does a slotted page maintain stable record identifiers when records are moved during compaction?

---

## Capstone Project — TLE Record Page Manager

Build a page manager that reads and writes orbital TLE records to a custom binary page format backed by a simple buffer pool. The page manager must support insert, lookup by slot, delete, and page-level compaction. Acceptance criteria and the full project brief are in `project-tle-page-manager.md`.

---

## File Index

```
module-01-storage-engine-fundamentals/
├── README.md                          ← this file
├── lesson-01-page-layout.md           ← File formats and page layout
├── lesson-01-quiz.toml                ← Quiz (5 questions)
├── lesson-02-buffer-pool.md           ← Buffer pool management
├── lesson-02-quiz.toml                ← Quiz (5 questions)
├── lesson-03-slotted-pages.md         ← Slotted pages
├── lesson-03-quiz.toml                ← Quiz (5 questions)
└── project-tle-page-manager.md        ← Capstone project brief
```

---

## Prerequisites

- Foundation Track completed (all 6 modules)
- Familiarity with `std::fs::File`, `Read`, `Write`, `Seek` traits
- Basic understanding of how operating systems manage file I/O

## What Comes Next

Module 2 (B-Tree Index Structures) builds on the page abstraction from this module. The B-tree nodes you implement in Module 2 are stored *in* the pages you design here. The buffer pool you build here is the same buffer pool that serves page requests for the B-tree and, later, the LSM engine.
