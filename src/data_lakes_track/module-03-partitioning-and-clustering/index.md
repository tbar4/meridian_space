# Module 3 — Partitioning and Clustering

**Track:** Data Lakes — Artemis Base Cold Archive
**Mission framing:** Module 2 produced a transactional table over the Parquet files. This module designs the file-and-directory layout that makes queries against the table fast.

---

## Context

A table format with no partition strategy is a table format that has to read every file for every query. The metadata-level pruning from Module 2 prunes by what is *recorded* in the per-file statistics — but if the data within each file is randomly distributed across the predicate domain, every file has min/max statistics that span the entire domain and pruning prunes nothing. Partitioning and clustering are the disciplines that arrange the data so that statistics are tight, files are small along the query-relevant dimensions, and pruning actually works.

The Artemis cold archive has a clear partition-driving workload. Analyst queries always specify a mission, almost always specify a time window, and frequently specify a sensor or payload. The partition strategy must make these dimensions cheap to filter on. The clustering strategy must handle the multidimensional cases where queries pick out a corner of the (mission, time, sensor, region) space — Z-order clustering buys order-of-magnitude reductions in files scanned for these cases.

The lessons proceed from least to most sophisticated. Lesson 1 develops single-dimensional partition strategies — date, mission, hidden partitioning via transform functions — and the small-file problem that naive partitioning produces. Lesson 2 develops multidimensional clustering: Z-order and Hilbert curves, why they work, when to use which. Lesson 3 develops query-time pruning end-to-end, showing how partition layout and per-file statistics compose to make queries cheap.

---

## Lessons

1. **Partition Strategies and the Small-File Problem** — date partitioning, identity vs transformed partitioning, hidden partitioning, and the failure mode of over-partitioning.
2. **Multidimensional Clustering** — Z-order and Hilbert space-filling curves, locality preservation, the linearization trick that makes them work with columnar statistics.
3. **Partition Pruning at Query Time** — the predicate-to-pruning translation, the three pruning passes (manifest list → manifest → file), and how clustering enables pruning beyond the partition dimension.

Each lesson has a quiz. All three quizzes must be passed (≥ 70%) to unlock the project.

---

## Capstone Project

**SDA Observation Partition Layout.** Design and implement the partition + clustering strategy for the Artemis SDA observation table, measure its effectiveness against a realistic analyst-query benchmark, and document the design decisions. The implementation extends the Module 2 table format with partition-spec and transform support.

---

## Learning Objectives

By the end of this module, the engineer will be able to:

- Choose partition columns for a workload based on the query patterns and the cardinality of candidate dimensions, defending the choice against the small-file failure mode.
- Apply Iceberg-style transformed partitioning (bucket, truncate, year/month/day) to handle high-cardinality columns without proliferating tiny partitions.
- Decide whether and when to add Z-order or Hilbert clustering, and explain what the clustering buys that single-dimensional partitioning does not.
- Trace a query predicate through the three-level pruning hierarchy and predict which files will be scanned.

---

## Source Material

Primary source: *Designing Data-Intensive Applications* (Kleppmann & Riccomini, 2025), Chapter 6 ("Partitioning"). Iceberg specification's "Partitioning" and "Partition Transforms" sections. The Z-order discussion synthesizes from the original Morton (1966) paper and the literature applying Z-order to data warehousing.

> **Source note:** The Z-order and Hilbert-curve material is synthesis-mode from training knowledge plus the public literature. The Iceberg-specific transform shape comes from the spec.
