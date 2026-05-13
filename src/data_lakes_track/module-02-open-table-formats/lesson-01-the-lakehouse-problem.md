# Lesson 1 — The Lakehouse Problem

**Module:** Data Lakes — M02: Open Table Formats
**Position:** Lesson 1 of 3
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann and Chris Riccomini, Chapter 7 ("Transactions — Single-Object Writes") and Chapter 8 ("The Trouble with Distributed Systems"). Apache Iceberg specification, "Goals" section.

> **Source note:** Iceberg/Delta books are not in the project corpus. The transaction-theory framing in this lesson is grounded in DDIA; the format-specific assertions about table formats are synthesized from the Iceberg spec and would benefit from verification.

---

## Context

Module 1 ended with a writer that produces well-formed Parquet files into the Artemis cold-archive object bucket. Each file is correct in isolation: footer at the end, encodings chosen, statistics computed, atomic rename on success. The cold archive now holds millions of these files across thousands of object paths. The question this lesson asks is: **what is the table?**

The naive answer is "the directory." A query against the cold archive lists the bucket, reads every Parquet file, and unions the results. This answer is wrong in five distinct ways that Module 2 will spend three lessons fixing. The list-and-read pattern is slow (every query pays the cost of listing every file). It has no transactional semantics (a writer dropping new files mid-query produces split-brain query results). It has no schema enforcement (a file with a stale schema is silently included). It has no version (the table cannot be queried as it was an hour ago). It has no atomic delete (removing a file requires hoping no reader is currently using it).

A table format solves these problems by adding a metadata layer between the catalog and the data files. The metadata is the source of truth for "what files are in the table at version N." Queries read the metadata first, learn which files to read, and read only those. Writes produce new metadata that atomically swaps in. The data files themselves do not change — they are the same Parquet files Module 1 produced. The table format is purely a metadata problem; the data layer is unchanged.

This lesson develops the lakehouse problem in five failure-mode-shaped pieces, each one a real outage type that has happened in production data warehouses without a table format. The remaining two lessons in the module build the metadata layer that fixes them.

---

## Core Concepts

### Listing Is Not Membership

The first failure mode is the cheapest to demonstrate. A query against the cold archive without a table format must answer "what files belong to this table?" by listing the object store. Object store listings are slow (paginated, eventually consistent on some backends, billed per request) and incomplete (an in-flight write may or may not be visible). A query against a 100k-file table issues a list operation that returns 100k object keys, filters the ones it wants, and reads the data. The listing itself can take ten seconds.

Worse, the listing answer is not stable. A writer dropping a new file during the listing produces undefined behavior: the listing may or may not include the new file depending on where the listing's pagination cursor is when the file appears. The query that reads the listing now races the writer. Two consecutive queries can return different results not because the table changed but because the listing returned different subsets.

The fix the table format provides is to make membership an explicit, atomic data structure — a list of files written at a known version of the metadata. The list of files is the table's membership; the directory contents are storage-layer details the table format manages. Listing the object store is a maintenance operation, not a query operation.

### Atomic Visibility Across Multiple Files

A single Parquet file's atomic visibility is solved by Module 1's `.inprogress` rename pattern: a reader sees a file only after it is complete. But most table mutations produce *multiple* files at once. A daily Artemis batch ingest drops twelve new Parquet files for the day's downlinks. A correction job rewrites three files to fix a quality-flag bug. An optimization job consolidates fifty small files into five large ones.

Without a table format, "drop twelve files atomically" is not a primitive object storage exposes. The writer drops file 1 through file 11, crashes, and now the table contains eleven files of a twelve-file commit — partial data the reader sees as if it were complete. DDIA (Ch. 7) makes the same point in single-machine terms: atomicity at the multi-object level is what transactions provide and what storage engines alone do not. The same logic applies in the lakehouse case, scaled up: a multi-file commit is a multi-object transaction, and storage object stores do not provide them.

The table format makes multi-file commits atomic by indirecting through metadata. The writer produces the twelve new data files (each individually atomic via the rename pattern), then writes a *new metadata snapshot* listing the table's now-complete file set, then atomically swaps the catalog pointer to point at the new snapshot. Until the catalog pointer swaps, readers see the old table. After the swap, readers see the new. There is no in-between state any reader can observe.

### Schema Enforcement and Schema Drift

A directory of Parquet files has no canonical schema. Every file has its own schema (in its footer), and the schemas may differ. A writer that adds a new column to its output produces files with one schema; old files in the same directory have the previous schema. A reader that unions these files must choose how to handle the inconsistency. The naive choices are bad: pick one file's schema and reject the rest; union all schemas and produce nulls for missing columns silently; fail. Each choice produces a different "the table" depending on which files were listed and which order.

This is **schema drift**, and it is the failure mode that makes ad-hoc data lakes unusable past the first year of operation. The Artemis legacy archive accumulated nine variants of the telemetry schema over six years, each one a writer that updated its output without coordinating with readers. Analysts learned to special-case ranges of dates against schema versions; the analyst tooling carried a schema-versioning table that no one was sure was complete.

The table format fixes schema drift by storing the table's schema in the metadata, not in the data files. New data files conform to the metadata's schema; schema changes are atomic operations on the metadata (Module 4 develops schema evolution mechanics). Readers consult the metadata schema, project data files against it, and reject files whose schema is incompatible. The metadata schema is what "the table" means; individual file schemas are storage details.

### No Snapshot, No Time Travel

Without a table format, the table is whatever the directory contains right now. There is no version. There is no query that asks "what did this table look like an hour ago." The accident-investigation use case that motivates the Artemis cold archive — reconstructing the operational state of the satellite constellation as of a specific past minute — is fundamentally incompatible with a raw-directory lakehouse, because there is no representation of "the table as of past minute N."

DDIA (Ch. 7) makes the same observation about snapshots in transactional databases: snapshot isolation is a discipline that requires the storage engine to preserve old versions of data while new versions are being written. The lakehouse case has the same requirement and the same shape of solution: every mutation produces a new immutable version of the metadata; old versions remain on disk; queries against past timestamps read the version that was current at that timestamp. The data files themselves are content-addressed in effect — a data file is never modified once written, so all past snapshots can reference it directly.

Time travel is not the goal; immutable versioning is the goal. Time travel falls out of immutable versioning for free. Module 4 develops the time-travel query path explicitly.

### Atomic Deletes and the Concurrent-Reader Problem

The last failure mode is the one that bites operational engineers running a lakehouse for the first time. Files in a data lake are not append-only forever — they need to be deleted. Old data ages out per retention policy. Bad files from a corrupted ingest need to be removed. Compaction (Module 6) rewrites many small files into fewer large ones and then deletes the originals. The naive delete is `object_store.delete(path)`.

The naive delete races concurrent readers. A query that is reading a file when the delete arrives sees its in-flight read fail with "object not found." The query crashes, the analyst retries, the next listing doesn't include the file, the retry succeeds. The right outcome — the query against the old version of the table sees the old files — requires the file to remain readable until every reader of the old snapshot has finished. The table format solves this by making file deletion a two-phase operation: the new metadata snapshot stops referencing the deleted files, and a separate maintenance job (snapshot expiration; Module 6) physically deletes the files only after a configurable retention window during which the old snapshot remains queryable.

The discipline is the same as the rename pattern at the file level: visibility is decoupled from the underlying storage operation. The metadata controls visibility; the storage operations are kept apart and run on schedules that respect reader requirements.

---

## Code Examples

### Demonstrating the Listing-Race Failure Mode

The minimal demonstration of why listing-is-not-membership matters: two threads concurrently writing and reading, both treating the directory as the table.

```rust,ignore
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::task::JoinHandle;

/// Simulate the "list the directory and read everything" pattern that a
/// lakehouse without a table format produces. Two writers and one reader
/// run concurrently; the reader sees inconsistent counts depending on
/// where the listing's pagination falls relative to the writers.
async fn race_demo(dir: PathBuf) -> Result<()> {
    let dir = Arc::new(dir);

    // Two writers, each dropping new files at a steady cadence.
    let w1 = spawn_writer(dir.clone(), "ingest-A", 100);
    let w2 = spawn_writer(dir.clone(), "ingest-B", 80);

    // Reader runs a query every 250ms by listing the directory and
    // reading the files. Each query is independent; no coordination
    // with the writers exists, so the result depends on what files
    // the listing happens to return.
    for _ in 0..10 {
        let listed = tokio::fs::read_dir(&*dir).await?;
        let count = count_complete_files(listed).await?;
        // Two successive queries may return different counts not because
        // the table semantics have changed, but because the writers
        // dropped files between the calls. The reader cannot distinguish
        // "the table grew" from "I read at a different moment in time."
        println!("query saw {count} files");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    w1.abort();
    w2.abort();
    Ok(())
}

fn spawn_writer(
    dir: Arc<PathBuf>,
    name: &'static str,
    rate_ms: u64,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut seq = 0u64;
        loop {
            let path = dir.join(format!("{name}-{seq:06}.parquet"));
            // Write file with the .inprogress / rename pattern from M1.
            // Each file is individually atomically visible, but the
            // *set* of files visible to a reader is not stable.
            write_one_parquet(&path).await?;
            seq += 1;
            tokio::time::sleep(Duration::from_millis(rate_ms)).await;
        }
    })
}

// Helpers `count_complete_files` and `write_one_parquet` elided — see the
// repository for the full demo. The point is structural: every reader
// gets a different table because the directory listing is the membership
// and the membership is racing with the writers.
```

What to notice. Each individual file is correctly atomic at the file-level (Module 1's rename discipline holds). The bug is *above* the file level — at the table level — and no amount of per-file discipline fixes it. The race is structural: the table format is the only place where the fix can live, because the directory listing has no notion of a "table version."

The fix the next two lessons will develop: the table is not the directory. The table is a metadata pointer to a known-complete file set. Listing the directory becomes a maintenance operation (find orphaned files that the metadata no longer references; Module 6), not a query operation.

### What an Atomic Multi-File Commit Looks Like

The contrast: a writer that produces three new data files and makes them visible atomically through a metadata snapshot. The data files are written first; the metadata pointer swap happens last and is the only operation that affects what readers see.

```rust,ignore
use std::sync::Arc;
use anyhow::{Context, Result};

/// Sketch of an atomic multi-file commit. Steps 1–3 happen in any order
/// and can fail without affecting the table; step 4 is the atomic
/// operation that makes the commit visible. Step 5 is best-effort
/// cleanup if step 4 fails.
async fn atomic_commit(
    table: &Table,
    new_data_files: Vec<DataFile>,
) -> Result<SnapshotId> {
    // Step 1: Write each new data file to a content-addressed object
    // store path. Each write is individually atomic via the rename
    // pattern from M1. If any of these fails, none of the files is
    // referenced by table metadata yet, so the partial state is
    // invisible to readers.
    for file in &new_data_files {
        write_parquet(&file.path, &file.batches)
            .await
            .with_context(|| format!("writing {}", file.path))?;
    }

    // Step 2: Read the table's current snapshot to learn what files
    // are already in the table. This snapshot will be the *base* for
    // the new snapshot — the new snapshot's file set is the base file
    // set plus the new files.
    let base = table.current_snapshot().await?;

    // Step 3: Construct the new snapshot's metadata: a manifest listing
    // the new data files, the existing manifests from the base
    // snapshot, and the snapshot record itself.
    let new_snapshot = build_snapshot(&base, &new_data_files).await?;

    // Step 4: Atomic pointer swap. This is the only operation in the
    // entire commit that has transactional semantics. Until it returns
    // Ok, readers see the base snapshot. After it returns Ok, readers
    // see the new snapshot. There is no in-between state.
    table.compare_and_swap_snapshot(base.id, new_snapshot.id).await?;

    Ok(new_snapshot.id)
}
```

The two structural properties that make this work. **All the work is decoupled from visibility**: writing the data files, building the manifests, constructing the snapshot — all of these can fail or be retried without affecting readers, because none of them changes the table's catalog pointer. **The visibility change is one CAS**: the only operation that has transactional semantics is the compare-and-swap on the catalog pointer, and the catalog pointer is small (typically a single object store key holding the current snapshot ID). The lakehouse problem reduces to "how does the catalog provide CAS?" — which is the subject of Lesson 3.

### A Schema Enforcement Check at Read Time

The schema-drift fix in code: the reader consults the table's metadata schema, not the data file's schema, and projects/casts data files against the metadata schema. Files whose schema is incompatible are rejected at read planning, not at row-decode time.

```rust,ignore
use arrow::datatypes::SchemaRef;
use anyhow::{anyhow, Result};

/// Decide whether a data file's schema is compatible with the table's
/// current schema, and produce a projection plan if so. Compatibility
/// is asymmetric: the data file's schema must be a subset of the table
/// schema, with matching types for the columns present. Columns missing
/// from the data file are filled with nulls at read time; columns
/// present in the data file but absent from the table schema are
/// dropped.
fn plan_file_read(
    table_schema: &SchemaRef,
    file_schema: &SchemaRef,
) -> Result<FileReadPlan> {
    let mut projection = Vec::with_capacity(table_schema.fields().len());
    for table_field in table_schema.fields() {
        match file_schema.field_with_name(table_field.name()) {
            Ok(file_field) => {
                if file_field.data_type() != table_field.data_type() {
                    // Incompatible type. The right action is to flag the
                    // file as quarantined (Module 6's lineage system) and
                    // skip it; never silently coerce a mismatched type.
                    return Err(anyhow!(
                        "type mismatch on {}: table={:?} file={:?}",
                        table_field.name(),
                        table_field.data_type(),
                        file_field.data_type(),
                    ));
                }
                projection.push(FileColumn::Existing(file_field.clone()));
            }
            Err(_) => {
                // Column added to the table after this file was written.
                // Synthesize a null column of the right type at read time.
                projection.push(FileColumn::Null(table_field.clone()));
            }
        }
    }
    Ok(FileReadPlan { projection })
}
```

The discipline this enforces. **The table schema is the source of truth.** Files that conform are read; files that conflict are flagged. The reader never silently produces inconsistent results because of schema drift. This is what schema enforcement means in the lakehouse context: the table format records the schema in the metadata, and reads project against the metadata schema rather than against whatever the individual data files happen to contain.

---

## Key Takeaways

- **A directory of Parquet files is not a table.** Five distinct failure modes emerge: slow/inconsistent listing-as-membership, no atomic multi-file visibility, no schema enforcement, no snapshot/time-travel, no safe deletes.
- The table format's job is to make membership an explicit, versioned data structure stored as metadata. The data files are unchanged; the metadata is the source of truth for "what files are in this table at version N."
- **Atomicity at the multi-file level requires indirection through metadata.** A multi-file commit produces all its data files, builds a new metadata snapshot, and atomically swaps the catalog pointer. The CAS on the catalog pointer is the only transactional operation; everything else is decoupled work.
- **Schema enforcement** lives in the table metadata, not in individual data files. Readers project file contents against the metadata schema, accept conforming files, flag non-conforming ones.
- **File deletion is decoupled from visibility.** New snapshots stop referencing old files; physical deletion runs later, on a retention window long enough that no reader still needs the old snapshot.

{{#quiz quiz-01.toml}}
