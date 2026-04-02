# Module 06 — Query Processing

**Track:** Database Internals — Orbital Object Registry  
**Position:** Module 6 of 6  
**Source material:** *Database Internals* — Alex Petrov, Chapters 14–15; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 6  
**Quiz pass threshold:** 70% on all three lessons to unlock the project

---

## Mission Context

> **SDA INCIDENT REPORT — OOR-2026-0047**  
> **Classification:** PERFORMANCE DEFICIENCY  
> **Subject:** Multi-source TLE merge exceeds conjunction window deadline  
>  
> The OOR ingests TLE data from 5 independent sources (18th SDS, ESA SST, LeoLabs, ExoAnalytic, Numerica). When multiple sources provide TLEs for the same object, the engine must merge them — selecting the most recent epoch, resolving conflicts, and joining against the master catalog. Currently this is done in application code with ad-hoc nested loops. A full catalog merge of 100,000 objects from 5 sources takes 45 seconds. The conjunction pipeline requires merge results within 10 seconds.
>  
> **Directive:** Implement a structured query processing layer: scan operators, join algorithms, and a composable execution model that can be optimized for the catalog merge workload.

---

## Learning Outcomes

After completing this module, you will be able to:

1. Implement the volcano (iterator) model for pull-based query execution with composable operators
2. Explain vectorized execution and why processing column batches outperforms row-at-a-time for analytical queries
3. Implement nested-loop, hash, and sort-merge join algorithms and determine which is optimal for a given workload
4. Compose scan, filter, projection, and join operators into a query execution plan
5. Analyze the I/O and memory costs of different join strategies for the OOR catalog merge workload

---

## Lesson Summary

### Lesson 1 — The Volcano (Iterator) Model
Pull-based query execution. Each operator (scan, filter, join) implements `next() → Option<Row>`. Operators compose like iterator chains. Pipelining and its limitations.

### Lesson 2 — Vectorized Execution
Processing batches of rows (column vectors) instead of one row at a time. Cache efficiency, SIMD potential, and why OLAP engines (DuckDB, Velox, DataFusion) use vectorized execution.

### Lesson 3 — Join Algorithms
Nested-loop join, hash join, and sort-merge join. Cost models, memory requirements, and when each algorithm is optimal. Application to the OOR multi-source TLE merge.

---

## Capstone Project — Orbital Catalog Merge System

Build a query execution engine that merges TLE data from 5 sources using composable operators. The merge pipeline uses scan, filter, sort, and join operators composed in the volcano model. Full brief in `project-catalog-merge.md`.

---

## File Index

```
module-06-query-processing/
├── README.md
├── lesson-01-volcano-model.md
├── lesson-01-quiz.toml
├── lesson-02-vectorized-execution.md
├── lesson-02-quiz.toml
├── lesson-03-join-algorithms.md
├── lesson-03-quiz.toml
└── project-catalog-merge.md
```

---

## Prerequisites

- Module 5 (Transactions & Isolation) completed
- All previous modules in the Database Internals track

## Track Complete

This is the final module of the Database Internals track. After completing it, you will have built a storage engine from the ground up: page layout (M1) → B-tree indexing (M2) → LSM write-optimized storage (M3) → crash recovery (M4) → MVCC concurrency (M5) → query processing (M6).
