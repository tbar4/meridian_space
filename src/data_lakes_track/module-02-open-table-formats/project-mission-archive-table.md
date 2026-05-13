# Capstone — Mission Archive Table

**Module:** Data Lakes — M02: Open Table Formats
**Estimated effort:** 1–2 weeks of focused work
**Prerequisite:** All three lessons in this module completed; all three quizzes passed (≥ 70%). The Module 1 capstone (Artemis archive Parquet writer) is the producer this table format wraps.

---

## Mission Briefing

### From: Cold Archive Platform Lead

```
ARCHIVE BRIEFING — RC-2026-04-DL-002
SUBJECT: Mission archive table format, Iceberg-shaped metadata layer
         over the Parquet writer from M01.
PRIORITY: P1 — required to unblock concurrent ingest from the daily
         downlink pipeline and the weekly correction job.
```

The Module 1 Parquet writer is in production for the new cold archive, but the storage layer is still a directory of files. We are seeing the failure modes Lesson 1 enumerated: concurrent runs of the daily ingest and the weekly correction job race; analyst queries against the archive return different result sets across consecutive runs; a corrupted ingest from last Tuesday left the directory in a state that took manual cleanup. The table format layer is the fix.

Your work is the minimal Iceberg-shaped table format that solves these problems. The goal is operational correctness over feature completeness — we are not building an Iceberg-spec-compliant implementation; we are building the metadata layer that makes the archive transactional. The capstone produces a Rust crate, `artemis-table-format`, that the ingestion pipeline and the correction job can both use without coordination.

Module 3 will add partition layout on top of this; Module 4 will add time-travel reads; Module 5 will plug a query engine in; Module 6 will add the maintenance jobs (compaction, snapshot expiration). This module's project is the foundation those modules build on. Get the metadata model and the commit protocol right.

---

## What You're Building

A Rust crate, `artemis-table-format`, exposing:

- A `Table` struct holding a reference to the catalog and the current snapshot, with methods `current_snapshot()`, `read_plan(predicate)`, `commit_append(data_files)`, `commit_overwrite(removed, added)`.
- A `PostgresCatalog` (or equivalent — SQLite acceptable for the capstone) providing linearizable CAS on the per-table snapshot pointer.
- Concrete types for `Snapshot`, `ManifestList`, `Manifest`, `DataFile` matching the Iceberg-spec-aligned shape from Lesson 2.
- Serialization to and from a metadata directory in the same object store as the data files (local filesystem is acceptable for the capstone; the production version uses S3-compatible `object_store`).
- A CLI binary, `artemis-table`, with subcommands `init`, `info`, `commit` (consumes a list of data files from stdin), `history` (lists snapshots), `inspect-manifest` (dumps a manifest's contents).

The crate must support concurrent commits from at least two writers against the same table without data loss or visibility anomalies, demonstrated in the integration test suite.

---

## Functional Requirements

1. **Snapshot immutability.** Once written, a snapshot file is never modified. Commits write new snapshot files. The metadata directory's contents grow over time.
2. **Catalog CAS.** The commit's only transactionally-significant operation is a single linearizable CAS on the per-table snapshot pointer. The CAS implementation can be any transactional backend (Postgres, SQLite with `BEGIN IMMEDIATE`, in-memory `Mutex<HashMap>` for unit tests).
3. **Optimistic retry loop.** Failed CAS attempts trigger a rebase against the new current snapshot and a retry. The retry bound is configurable (default 16) with exponential backoff and jitter.
4. **Read planning.** A read against a snapshot produces a list of data files using the three-pass pruning (manifest list summaries → manifest entries → done; the in-file row-group pruning is the Parquet reader's job, not the table format's).
5. **Schema enforcement.** Data files committed to the table must match the table's current schema, or the commit is rejected. The capstone supports schema-compatible additions (new columns, all-nullable) without requiring a schema-evolution commit.
6. **Append and overwrite commit types.** Append commits add files without removing any. Overwrite commits remove a specified file set and add a new file set in one snapshot. Both must work under concurrent contention.
7. **History query.** `table.history()` returns the sequence of snapshots from the current snapshot back to the table's creation, via the `parent_snapshot_id` chain.

---

## Acceptance Criteria

### Verifiable (automated tests must demonstrate these)

- [x] `table.commit_append(files)` against a fresh table produces a new snapshot referencing the files; `table.current_snapshot().manifest_list` lists one manifest containing the files.
- [x] After 10 sequential `commit_append` operations, `table.history()` returns 11 snapshots (1 initial + 10 commits) with the correct parent chain.
- [x] Concurrent `commit_append` from two writers (orchestrated with a `Barrier` to line them up at the CAS) results in exactly one commit succeeding on first attempt and one observing a conflict; after the conflict-handler's retry, both commits are applied in sequence, and the final snapshot references all files from both writers.
- [x] `commit_overwrite` removes the specified files from the snapshot's effective file set (the manifest entries' status changes from `Existing` to `Deleted`) and adds the new files, in one snapshot.
- [x] A `commit_append` rejected for schema mismatch (data file's schema is incompatible with the table schema) does not modify the catalog pointer; subsequent reads return the previous snapshot's file set.
- [x] `read_plan(predicate)` for a partition-equality predicate returns only data files whose partition statistics overlap the predicate value; manifests whose partition summaries do not overlap are not opened.
- [x] After 100 random concurrent commits across two writers (operations interleaved arbitrarily, retries enabled), the final snapshot's effective file set equals the set of files that succeeded in either writer's view. No commits are silently lost; no commits are silently double-applied.
- [x] An old snapshot remains readable after a sequence of subsequent commits. `read_plan(snapshot_id = N)` for an N from 50 commits ago returns the file set that was current at snapshot N.

### Self-assessed (you write a short justification; reviewer checks it)

- [ ] *(self-assessed)* The catalog backend choice (Postgres / SQLite / in-memory) is documented in `docs/catalog-choice.md` with the linearizable-CAS argument: why the chosen primitive provides the required atomicity, and what fails if it does not.
- [ ] *(self-assessed)* The retry policy (bound, backoff, jitter) is documented in `docs/retry-policy.md` against the livelock failure mode: why the chosen bounds are sufficient for expected contention and what the failure surface looks like when they are not.
- [ ] *(self-assessed)* The metadata file layout (path conventions, file naming, directory structure) is documented in `docs/metadata-layout.md` against the future-modules concern: Module 6's snapshot expiration job needs to be able to enumerate snapshots and decide which to delete; your layout makes that operation efficient.
- [ ] *(self-assessed)* The `read_plan` pruning correctness is justified in `docs/read-plan-correctness.md` against the false-negative concern: pruning must never skip a file that *could* match the predicate. Your implementation's correctness argument is one paragraph plus a test that exercises the boundary cases.

---

## Architecture Notes

A reasonable module layout:

```text
artemis-table-format/
├── src/
│   ├── lib.rs              # Table, Snapshot, Manifest, etc.
│   ├── metadata.rs         # serialization of snapshot/manifest-list/manifest
│   ├── catalog.rs          # CatalogTrait, PostgresCatalog, InMemoryCatalog
│   ├── commit.rs           # commit_append, commit_overwrite, retry loop
│   ├── read.rs             # read_plan and pruning
│   └── bin/artemis_table.rs
├── tests/
│   ├── single_writer.rs    # baseline correctness
│   ├── concurrent_writers.rs  # the Barrier-orchestrated concurrent tests
│   ├── history.rs          # snapshot chain queries
│   └── time_travel.rs      # reading old snapshots
└── docs/
    ├── catalog-choice.md
    ├── retry-policy.md
    ├── metadata-layout.md
    └── read-plan-correctness.md
```

The metadata directory layout the docs/metadata-layout.md should justify:

```text
<table_location>/
├── data/                       # written by the M01 Parquet writer
│   └── <data files>.parquet
└── metadata/
    ├── v0/                     # snapshot 0 (initial empty table)
    │   ├── snap.json
    │   └── ml.avro
    ├── v1/
    │   ├── snap.json
    │   ├── ml.avro
    │   └── m0001.avro
    └── v2/
        ├── snap.json
        ├── ml.avro
        └── m0002.avro
```

The version-prefixed directory is one option; a flat directory with snapshot-ID-prefixed filenames is another. The Iceberg spec uses a flat layout with monotonically-incrementing version files; either is defensible if the doc explains the choice.

---

## Hints

<details>

<summary>Hint 1 — A simple in-memory CAS for unit tests</summary>

The CAS abstraction is small enough that a `Mutex<HashMap<String, CurrentSnapshot>>` is enough for fast unit tests. Concurrent commit tests run against the in-memory implementation; integration tests run against SQLite or Postgres to verify the same protocol works against a real transactional backend. The trait sketch:

```rust,ignore
#[async_trait]
pub trait Catalog: Send + Sync {
    async fn get_current(&self, table: &str) -> Result<CatalogEntry>;
    async fn compare_and_swap(
        &self,
        table: &str,
        expected_old: SnapshotId,
        new: SnapshotId,
        new_metadata_path: &str,
    ) -> Result<(), CommitError>;
}
```

</details>

<details>

<summary>Hint 2 — Avoiding the wrong kind of retry test</summary>

Concurrent-writer tests must orchestrate the writers so they all reach the CAS at the same time; otherwise the test is testing timing, not the CAS protocol. Use `tokio::sync::Barrier` to line up both writers after they've read the same base snapshot. Without the barrier, one writer almost always finishes its work before the other starts, and the CAS never sees concurrent contention.

</details>

<details>

<summary>Hint 3 — Manifest pruning correctness boundary cases</summary>

The pruning correctness argument has to handle two edge cases: (1) partition statistics with `contains_null = true` against a non-null predicate (the manifest *might* contain matching rows even though its bounds don't overlap), and (2) `min_value == max_value` for a single-value partition (the comparison must be `<=` and `>=`, not `<` and `>`, or the matching manifest gets pruned). The pruning test suite should include both cases. A pruning bug that produces false negatives — incorrectly skipping a matching file — is silent data loss from the analyst's perspective.

</details>

<details>

<summary>Hint 4 — Reading the parent snapshot chain</summary>

`table.history()` walks the `parent_snapshot_id` chain backward from the current snapshot. Each step reads one metadata file. For tables with hundreds of snapshots, this is hundreds of file reads — acceptable for an `info` CLI but too slow for any per-query use. If you find yourself wanting to use history in the read path, that is the signal to add a "snapshot log" file (the Iceberg `metadata_log` field) that summarizes the chain in a single file. The capstone does not require this optimization; production deployments do.

</details>

<details>

<summary>Hint 5 — The orphan-file question</summary>

A failed CAS attempt leaves orphan metadata files in the metadata directory (the snapshot, manifest list, and manifest the writer produced but that no catalog pointer references). These are not a correctness concern — they cost only the storage to keep them. Module 6 will introduce the orphan-file cleanup job that finds and deletes them. Your capstone does not need to implement cleanup, but the documentation should note that orphans accumulate and reference Module 6 as the eventual fix.

</details>

---

## References

- *Designing Data-Intensive Applications* (Kleppmann & Riccomini), Chapter 7 — "Pessimistic vs Optimistic Concurrency Control"
- Apache Iceberg specification (`iceberg.apache.org/spec`), particularly "Table Spec V2" and "Commit Process"
- Iceberg whitepaper (Ryan Blue, Netflix, 2018) — `github.com/apache/iceberg/blob/main/docs/img/iceberg-paper.pdf`
- `sqlx` documentation (`docs.rs/sqlx`) — Postgres CAS pattern

---

## When You're Done

The crate is "done" when all eight verifiable acceptance criteria pass in CI and the four self-assessed docs are written. The integration tests run against both the in-memory and the SQLite catalog backends to verify the protocol works regardless of the CAS implementation. The next module's project will build the partition strategy that this format's `read_plan` uses; your `read_plan` must support partition-equality predicates by the time Module 3 begins.
