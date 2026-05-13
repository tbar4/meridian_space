# Module 2 — Open Table Formats

**Track:** Data Lakes — Artemis Base Cold Archive
**Mission framing:** Module 1 produced well-formed Parquet files in object storage. This module wraps them in the metadata layer that turns a directory of Parquet files into a queryable, transactional table.

---

## Context

A pile of Parquet files in a directory is not a table. It is a pile of Parquet files. The question "what rows exist in this dataset right now?" has no canonical answer — three concurrent writers can have just dropped three files, two of which might be incomplete, one of which might overlap with the data in the others. The question "what did the dataset look like an hour ago?" cannot be answered at all, because the directory is its own history.

Open table formats — Iceberg, Delta Lake, Hudi — exist to make a directory of Parquet files into a real table. They do this by adding a metadata layer above the data files: a structure that records which files belong to the table, at what version, with which schema, and the operational machinery (atomic commits, snapshot isolation, schema evolution) that databases have always provided but that raw Parquet does not. The Artemis cold archive uses Iceberg as its table format. Module 2 develops the metadata layer end-to-end so the engineer can reason about the storage system Module 3 will partition, Module 4 will time-travel through, and Module 5 will query against.

The lessons proceed bottom-up. Lesson 1 develops the lakehouse problem — what concretely breaks when Parquet sits on object storage without a table format above it. Lesson 2 develops the metadata hierarchy common to Iceberg, Delta, and Hudi (snapshot → manifest list → manifest → data file). Lesson 3 develops the atomic-commit mechanic that makes concurrent writes safe, drawing on the optimistic-concurrency-control machinery from *Designing Data-Intensive Applications* Chapter 8.

> **Source note:** Iceberg/Delta-specific books are not in the curriculum project. This module is synthesis-mode for format-specific details, grounded in DDIA Chapter 8 for the transaction-theory side and the public Iceberg specification (`iceberg.apache.org/spec`) and Delta Lake protocol (`github.com/delta-io/delta/blob/master/PROTOCOL.md`) for the format-specific shape. Source-note callouts in individual lessons flag the claims that should be verified against the spec.

---

## Lessons

1. **The Lakehouse Problem** — what fails when Parquet sits on object storage without a table format above it: atomic visibility, schema enforcement, snapshot consistency, transactional writes.
2. **The Manifest Hierarchy and Snapshot Model** — the four-level metadata structure (catalog pointer → snapshot → manifest list → manifest → data file) and what each level buys.
3. **Atomic Commits via Optimistic Concurrency** — how an open table format makes concurrent writes safe using compare-and-swap on the catalog pointer.

Each lesson has a quiz. All three quizzes must be passed (≥ 70%) to unlock the project.

---

## Capstone Project

**Mission Archive Table.** Build a minimal Iceberg-shaped table format in Rust over the Parquet files Module 1 produces. The implementation has the four-level metadata hierarchy, atomic commit via CAS on the catalog pointer, snapshot-isolated reads, and a small command-line tool for inspecting the table.

---

## Learning Objectives

By the end of this module, the engineer will be able to:

- Explain why a directory of Parquet files on object storage cannot serve as a transactional table, in terms of the specific failure modes (torn appends, partial visibility, schema drift, no atomic deletes).
- Describe the four-level metadata hierarchy (snapshot, manifest list, manifest, data file) and the role of each level in answering "what files are in the table at version N?"
- Implement an atomic commit using compare-and-swap on a catalog pointer, and explain what each step prevents.
- Reason about commit failures under optimistic concurrency control — what conflicts occur, when retries are safe, and when they are not.

---

## Source Material

Primary source for transaction theory: *Designing Data-Intensive Applications* (Kleppmann & Riccomini, 2025), Chapter 8 ("The Trouble with Distributed Systems") and Chapter 7 ("Transactions"). Primary source for format-specific shape: the Apache Iceberg specification (`iceberg.apache.org/spec`), with cross-references to the Delta Lake protocol where the contrast illuminates a design choice. The Iceberg whitepaper from Netflix (Ryan Blue, 2018) is the recommended public supplement.
