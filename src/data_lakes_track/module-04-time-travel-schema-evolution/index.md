# Module 4 — Time Travel and Schema Evolution

**Track:** Data Lakes — Artemis Base Cold Archive
**Mission framing:** The table format is transactional (Module 2) and well-partitioned (Module 3). This module exploits the snapshot model to answer questions about past versions of the table, and develops the discipline for evolving schemas and partition specs over time.

---

## Context

The snapshot model from Module 2 gave us an immutable history for free: every commit produces a new snapshot file; old snapshots remain on disk; the catalog pointer is the only mutable state. This is the substrate for two capabilities that traditional row-store databases either don't have or charge a fortune for: **time travel** (querying the table as it existed at any past moment) and **safe schema evolution** (adding, dropping, or renaming columns without downtime or data rewrites). Both are central to the Artemis archive's accident-investigation workload: when something goes wrong on orbit, the analyst needs to reconstruct the operational state as of the minute before the anomaly, against the schema that was current then.

This module develops both. Lesson 1 is the snapshot isolation model — what it guarantees, what it doesn't, and how readers pin a snapshot for the duration of a query. Lesson 2 develops the time-travel query path and the change-data-feed pattern: reading the table at past timestamps and computing the diff between any two snapshots. Lesson 3 develops schema and partition-spec evolution: the rules that make column changes safe, how column IDs decouple physical storage from logical names, and the partition-evolution discipline that lets a long-lived table change its partitioning without rewriting historical data.

The capstone — the Mission Replay Engine — exercises all three: reconstruct the orbital object registry's state at any specified past timestamp, traversing through schema changes that occurred in the table's history.

---

## Lessons

1. **Snapshot Isolation on Object Storage** — what snapshot isolation guarantees in a lakehouse, the read-side protocol that pins a snapshot, and the lifecycle interactions with snapshot expiration.
2. **Time Travel and Change Data Feed** — querying by snapshot ID and by timestamp, the snapshot-difference primitive, and the change-data-feed pattern for downstream consumers.
3. **Schema and Partition Evolution** — column-ID-based schema evolution, the safe vs unsafe column changes, partition-spec evolution and the per-snapshot spec ID, and the migration disciplines that make long-lived tables stable.

Each lesson has a quiz. All three quizzes must be passed (≥ 70%) to unlock the project.

---

## Capstone Project

**Mission Replay Engine.** A read-only query service that reconstructs the state of the Artemis Orbital Object Registry at any specified past timestamp, returning Arrow record batches as the table existed at that moment. The implementation handles schema changes that occurred between the query time and the present (the result projects against the schema that was current at the query time, not the table's current schema), and produces a complete replay in under ten minutes for a full mission's worth of data.

---

## Learning Objectives

By the end of this module, the engineer will be able to:

- Explain snapshot isolation in the lakehouse context: what guarantees readers get, what concurrent writes are admissible, and how the guarantee differs from serializable isolation.
- Implement time-travel reads against a snapshot ID and against a timestamp, including the snapshot-lookup mechanic and the schema-of-the-time projection.
- Compute the difference (added rows, removed rows) between two snapshots, and emit it as a change-data-feed.
- Apply the schema-evolution rules: which column changes are safe additions, which require explicit migration, which are structurally impossible.
- Evolve a partition spec over time, handling the per-snapshot spec ID that lets historical data continue to be readable under the spec it was written with.

---

## Source Material

Primary source: *Designing Data-Intensive Applications* (Kleppmann & Riccomini, 2025), Chapter 7 ("Transactions — Snapshot Isolation and Repeatable Read") for the isolation model, and Chapter 5 ("Encoding and Evolution") for schema-evolution mechanics. Iceberg specification's "Schema Evolution" and "Partition Evolution" sections for the format-specific shape.

> **Source note:** The isolation-model framing is grounded in DDIA. The format-specific assertions about column-ID semantics and per-snapshot spec IDs are synthesized from the Iceberg spec; verification recommended.
