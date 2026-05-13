# Module 1 — Columnar Storage Foundations

**Track:** Data Lakes — Artemis Base Cold Archive
**Mission framing:** Replace the legacy JSONL archive that has been bottlenecking analyst queries against downlinked sensor data. The new archive is built on Apache Parquet and Apache Arrow.

---

## Context

The Artemis Base downlink pipeline lands roughly 40 TB per lunar day of high-resolution sensor data into the cold archive. The legacy storage tier was compressed JSONL — fast to write from the ground station handoff, catastrophically slow to query. Reading the `panel_voltage` channel for a single payload across one mission required scanning every byte of every file in that mission's directory. A typical analyst query took twenty minutes.

The replacement archive is built on **Apache Parquet** for on-disk storage and **Apache Arrow** for in-memory representation. Both are columnar. Both are designed to make "read one column from a hundred-column table" cheap. Module 1 develops the engineer's mental model of these formats well enough to make competent decisions about row group sizing, column chunk layout, encoding choice, and reader implementation. The remaining modules in the track build the table format, partitioning, time travel, query integration, and lifecycle operations on top of this foundation.

This module is three lessons followed by a capstone project. The capstone produces the Artemis archive Parquet writer that Module 2's table format will wrap, Module 3's partition strategy will organize, and Module 5's query engine will read.

---

## Lessons

1. **Parquet File Layout** — the physical structure of a Parquet file (footer, row groups, column chunks, pages) and the design constraints it imposes on writers and readers.
2. **Columnar Encodings** — the encoding schemes Parquet uses to make columnar data small (dictionary, RLE, bit-packing, delta, byte-stream-split) and how to pick the right one per column.
3. **Apache Arrow and the In-Memory Side** — Arrow's columnar in-memory layout, the IPC and file formats, zero-copy reads via memory mapping, and the Arrow ↔ Parquet boundary.

Each lesson has a quiz. All three quizzes must be passed (≥ 70%) to unlock the project.

---

## Capstone Project

**Artemis Archive Parquet Writer.** Build a streaming Parquet writer in Rust that ingests downlinked telemetry from the Artemis ground-segment handoff, writes well-sized row groups with per-column encoding choices, and produces files that subsequent modules can wrap, partition, and query.

---

## Learning Objectives

By the end of this module, the engineer will be able to:

- Read a Parquet file's footer programmatically and locate the byte ranges for any column chunk without scanning the file.
- Choose a row group size for a given workload and defend the choice against query pattern, memory budget, and parallelism constraints.
- Pick the appropriate per-column encoding (dictionary, RLE, delta, plain) given the column's value distribution and access pattern.
- Explain why Arrow's in-memory format and Parquet's on-disk format are different formats, and what the Arrow-Parquet boundary costs at read time.
- Use memory-mapped Arrow IPC files for zero-copy reads when the access pattern justifies it.

---

## Source Material

Primary sources for this module are *In-Memory Analytics with Apache Arrow* (Topol, 2024 — Chapters 1–3) and *Designing Data-Intensive Applications* (Kleppmann & Riccomini, 2025 — Chapter 4, "Storage and Retrieval"). The Apache Parquet specification (`github.com/apache/parquet-format`) is the authoritative reference for the on-disk format; lessons cite the spec where Topol's coverage is light.
