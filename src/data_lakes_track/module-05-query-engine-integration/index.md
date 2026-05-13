# Module 5 — Query Engine Integration

**Track:** Data Lakes — Artemis Base Cold Archive
**Mission framing:** Modules 2-4 produced a transactional, partitioned, time-traveling table format. This module plugs a query engine on top: predicate pushdown, vectorized reads, and the catalog architecture that lets a query engine discover the tables it can read.

---

## Context

A table format is only as useful as the query engine that consumes it. The Artemis archive's analyst workload is SQL — `SELECT mission_id, AVG(panel_voltage) FROM sda_observations WHERE day BETWEEN '2024-03-01' AND '2024-03-08' GROUP BY mission_id` — and an analyst does not want to write the three-pass pruning, file-fetch, batch-decode, and aggregation logic by hand. The query engine does that work. The lakehouse's job is to expose the table format's capabilities (pruning, schema, statistics) through interfaces the engine can use.

This module develops the integration end to end. Lesson 1 is **predicate pushdown** — the contract by which the query engine hands the table format a predicate to evaluate at planning time, so the file-level pruning happens before any data is read. Lesson 2 is **vectorized reads via Arrow IPC** — how the query engine consumes the table format's output as Arrow record batches without an intermediate row-by-row materialization, and the IPC protocol that lets the engine and the table format live in different processes. Lesson 3 is **catalog architecture** — the discovery layer that lets a query engine find the tables it has access to, with the four catalog backends (REST, Hive Metastore, Nessie, Postgres-JDBC) compared on the dimensions that matter.

The capstone — the Cross-Mission Analytics Engine — is a small DataFusion-based query engine that exposes the Module 2/3/4 substrate as a SQL surface. It does not implement a full SQL parser or planner; DataFusion does. The capstone's job is the integration shim: a `TableProvider` that wires the table format's planning and reading into DataFusion's query execution.

---

## Lessons

1. **Predicate Pushdown and the Pruning Pyramid** — the engine-to-table-format predicate contract, the conservatively-translatable subset of SQL predicates, the predicate-vs-projection separation.
2. **Vectorized Reads via Arrow IPC** — the Arrow IPC record-batch wire format, why the lakehouse uses it for engine-to-storage transport, the streaming protocol that bounds memory.
3. **Catalog Architecture** — REST catalog vs Hive Metastore vs Nessie vs Postgres-JDBC, their tradeoffs on consistency, branching, ergonomics, and operational complexity.

Each lesson has a quiz. All three quizzes must be passed (≥ 70%) to unlock the project.

---

## Capstone Project

**Cross-Mission Analytics Engine.** A DataFusion-based SQL engine over the Module 2-4 substrate. The engine implements a custom `TableProvider` that translates DataFusion's predicate pushdown calls into the Module 3 pruning protocol, returns Arrow record batches as a `Stream`, and supports time-travel queries via SQL syntax (`SELECT ... FROM table AS OF TIMESTAMP '2024-03-15'`).

---

## Learning Objectives

By the end of this module, the engineer will be able to:

- Explain predicate pushdown as a query-engine-to-storage contract: which predicates the engine pushes, which the engine retains, and the correctness asymmetry that bounds what the storage can push back.
- Implement the Arrow IPC streaming protocol for transporting record batches between processes, including the schema-message-then-batches discipline.
- Compare the four common catalog backends and defend a catalog choice for a specific operational context.
- Build a custom `TableProvider` that wires a non-trivial table format into DataFusion's execution path.

---

## Source Material

Primary source: *Designing Data-Intensive Applications* (Kleppmann & Riccomini, 2025), Chapter 4 ("Storage and Retrieval") for the column-store predicate-pushdown framing, Chapter 9 ("Replication and Consistency") for the catalog-consistency tradeoffs. *In-Memory Analytics with Apache Arrow* (Topol) for the Arrow IPC format. Apache Iceberg specification for the catalog API.

> **Source note:** DataFusion-specific details are synthesis-mode against current public docs. The DataFusion API surface is stable but evolves; capstone implementations should validate against the deployed version.
