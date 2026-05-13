# Module 6 — Compaction, Lineage, and Lifecycle

**Track:** Data Lakes — Artemis Base Cold Archive
**Mission framing:** Modules 1-5 produced a complete query-capable lakehouse. This module develops the maintenance disciplines that keep it operational over years of writes: compaction (consolidating the small-file accumulation), snapshot expiration (reclaiming storage from old versions), orphan cleanup (deleting metadata-unreferenced files), and lineage tracking (the audit trail that ties together what produced what).

---

## Context

A lakehouse with no maintenance is a lakehouse that slowly stops working. Each commit adds files; failed commits leave orphans; small ingest batches produce small files that accumulate into the file-count problem from Module 3; old snapshots stay on disk indefinitely; the metadata grows linearly with commit history. The Module 2-5 substrate handles writes and reads correctly, but it does not on its own keep the storage layout healthy. The maintenance jobs are what keep it healthy.

The four jobs the Artemis platform team runs continuously. **Compaction** rewrites small files into fewer large files and applies the cluster ordering (Module 3 Lesson 2) to the new files. **Snapshot expiration** removes snapshot metadata and unreferenced data files after the retention window. **Orphan cleanup** finds and deletes object-store files that no metadata references — typically left behind by failed commits or interrupted compactions. **Lineage tracking** records what produced each commit (which job, which input snapshots, which transformation logic) so the audit trail is queryable.

This module is the operational discipline that makes the lakehouse durable. The lessons proceed from the most visible failure mode (small files), through the storage-reclamation jobs (expiration, orphan cleanup), to the audit-and-discovery layer (lineage). The capstone — the Artemis Archive Lifecycle Worker — is the background service that runs all four jobs against the production archive on a schedule.

---

## Lessons

1. **Compaction Strategies** — the small-file problem at scale, bin-packing vs sort-based vs Z-order rewrite strategies, the safety discipline that keeps compaction non-disruptive.
2. **Snapshot Expiration and Storage Tiering** — the retention-window contract, the snapshot-graph traversal that decides what's reachable, the storage-tier handoff for snapshots that age out of the live archive.
3. **Lineage, Audit Trails, and Orphan Cleanup** — per-commit metadata for audit, snapshot-history compaction, the orphan-file detection scan that reconciles object-store contents against metadata.

Each lesson has a quiz. All three quizzes must be passed (≥ 70%) to unlock the project.

---

## Capstone Project

**Artemis Archive Lifecycle Worker.** A background service that runs compaction, snapshot expiration, and orphan cleanup against the Artemis cold archive on a continuous schedule. The implementation runs as a long-lived Rust process, reports its operational metrics to the observability stack, and respects the live workload's bandwidth budget (its jobs do not impact analyst query latency).

---

## Learning Objectives

By the end of this module, the engineer will be able to:

- Choose a compaction strategy (bin-packing, sort-based, Z-order rewrite) for a specific workload and defend the choice against the small-file problem and the maintenance-overhead tradeoff.
- Implement snapshot expiration: identify reachable snapshots, classify unreferenced files, schedule physical deletion with the appropriate retention guards.
- Detect and clean up orphan files left by failed writes, distinguishing genuine orphans from in-flight writes that have not yet been committed.
- Design a lineage-tracking schema that records what produced each commit, with enough metadata to answer the audit questions (when, by whom, with what input) but compact enough that the schema-history doesn't grow unboundedly.

---

## Source Material

Primary source: Apache Iceberg specification, "Maintenance" and "Snapshot Expiration" sections; *Designing Data-Intensive Applications* (Kleppmann & Riccomini, 2025), Chapter 11 ("Stream Processing") for the lineage framing. *Database Internals* (Petrov) Chapter 7 ("Log-Structured Storage") for the analogous compaction discipline in LSM trees.

> **Source note:** This module is largely synthesis-mode from training knowledge plus the public Iceberg specification. Iceberg/Delta-specific maintenance details should be verified against the current versions.
