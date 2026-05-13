# Lesson 2 — Time Travel Queries and Change Data Feed

**Module:** Data Lakes — M04: Time Travel and Schema Evolution
**Position:** Lesson 2 of 3
**Source:** Apache Iceberg specification, "Scan Planning — Time Travel" section. Delta Lake protocol, "Change Data Feed" section, for the diff-based pattern. *Designing Data-Intensive Applications* — Martin Kleppmann and Chris Riccomini, Chapter 11 ("Stream Processing — Change Data Capture") for the CDC semantics.

> **Source note:** The time-travel query mechanics are well-supported by the Iceberg spec. The change-data-feed pattern in this lesson is synthesized — the lakehouse formats implement CDF differently (Delta has a first-class feature; Iceberg supports it via snapshot diff inference); the lesson focuses on the structural pattern, with references to the format-specific implementations.

---

## Context

Lesson 1 made snapshot isolation explicit: a query pins a snapshot, reads against it, and is unaffected by subsequent commits. The natural generalization is **time travel** — pinning a snapshot other than the current one. The mechanism is unchanged; what changes is which snapshot the query pins, and how the query's results are interpreted by the consumer.

Two query patterns dominate the time-travel workload. **Point-in-time queries** ask "what did the table look like at moment T?" — a historical reconstruction for accident investigation, audit reporting, or reproducing a prior analysis. **Incremental queries** ask "what changed in the table between moments T1 and T2?" — the change-data-feed (CDF) pattern that drives downstream pipelines, dashboards, and ML feature stores. Both are built on the same primitive: read the table at a specific snapshot.

The capstone — the Mission Replay Engine — implements point-in-time queries against the Artemis Orbital Object Registry. Replay is what enables the accident-investigation workflow that motivates the cold archive: when something fails on orbit, the analyst pins the snapshot from immediately before the anomaly and reconstructs the registry's state as the operators saw it at the time. The lesson develops both the point-in-time and the change-data-feed patterns; the capstone exercises the point-in-time path.

---

## Core Concepts

### Two Time-Travel Addressing Modes

A time-travel query specifies the snapshot to read in one of two ways: by **snapshot ID** or by **timestamp**. Both addressings ultimately resolve to a single snapshot ID; the difference is the lookup mechanism.

**By snapshot ID.** The query specifies the snapshot ID directly. The reader consults the snapshot history to find the snapshot's metadata path, then proceeds with the normal read protocol. This is the operationally simplest path and the one the audit-trail use case typically uses — an event recorded as "snapshot 4729 contained the configuration" can be replayed later by pinning snapshot 4729.

**By timestamp.** The query specifies a UTC timestamp. The reader walks the snapshot history backward to find the most recent snapshot whose commit timestamp is at or before the target. The pinned snapshot is the one the table was at *at* the target time. This is the analyst-friendly path — humans think in timestamps, not snapshot IDs — and it is the path the Mission Replay Engine exposes.

Both addressings depend on the **snapshot history metadata**: a small append-only log of `(snapshot_id, commit_timestamp_ms, metadata_path)` triples maintained by every commit. Iceberg calls this the `metadata_log`. The log is bounded by the retention window — only snapshots still on disk are reachable for time travel. Snapshots older than the retention horizon are absent from the log (Module 6's snapshot expiration removes them), and queries against them return a "snapshot not found" error.

A subtle case the timestamp-based lookup must handle: **the target timestamp falls between two commits.** The convention is "snapshot N was current from `commit_timestamp_ms[N]` to `commit_timestamp_ms[N+1]`." A query at a timestamp `T` finds the snapshot whose `commit_timestamp_ms <= T` and either there is no next snapshot (it's still current) or the next snapshot's timestamp is `> T`. The found snapshot is the one to pin. The convention puts every instant into exactly one snapshot's window — including the rare instant exactly at a commit timestamp, which is conventionally assigned to the *new* snapshot.

### Schema-of-the-Time Projection

A time-travel query against snapshot S sees S's data, but the *schema* projection is the operator's choice. Two reasonable answers, both supported:

**Current-schema projection.** The query reads S's data files and projects them against the table's *current* schema. Columns added since S was committed appear as nulls (the data files don't have them); columns dropped since S are filtered out. This is the right choice when the consumer is current-tooling that expects the current schema — for instance, the dashboard that pulls the table over a stable schema and replays historical data.

**Snapshot-time schema projection.** The query reads S's data files and projects them against the schema that was *current at S's commit time*. Columns added later are not in the result; columns dropped later are present. This is the right choice for accident investigation and audit replay — the analyst wants to see the table as it looked then, not as it would look now.

The Mission Replay Engine exposes both via a query parameter (`schema_mode = current | snapshot_time`); the default is `snapshot_time` because the dominant use case is reconstructing operational state. The mechanics differ only in which `schema_id` the reader uses for projection; the data file reads are identical.

The schema-of-the-time projection requires the table's schema history. Iceberg records every schema the table has had via the `schemas` field in the snapshot metadata, keyed by `schema_id`. Each snapshot records the `schema_id` that was current at its commit. The reader looks up the snapshot's `schema_id`, finds the schema in the schemas table, and uses it to project the data file reads.

### Change Data Feed: The Snapshot Diff Pattern

The change-data-feed (CDF) pattern reads the *difference* between two snapshots. Given snapshots `S_old` and `S_new`, CDF returns the rows that were *added* (in `S_new` but not `S_old`) and the rows that were *removed* (in `S_old` but not `S_new`). The pattern is the foundation of downstream pipelines that want to keep their derived data in sync with the table without recomputing from scratch.

The diff is computed at the **manifest entry** level, not by comparing row content. Each manifest entry has an `Added`, `Existing`, or `Deleted` status; the entry's status is what records the change relative to the prior snapshot. Diffing two snapshots produces:

- **Added files**: files whose manifest entries have status `Added` in any snapshot strictly between `S_old` and `S_new` (inclusive of `S_new`).
- **Removed files**: files whose manifest entries have status `Deleted` in any snapshot strictly between `S_old` and `S_new`.

A consumer that wants the added rows reads the added files; a consumer that wants the removed rows reads the removed files (the data is still on disk because the files have not yet been physically deleted by snapshot expiration). The output is two streams of Arrow record batches: the inserts and the deletes.

The Delta Lake CDF feature provides this directly. Iceberg's `snapshot.added_files()` and `snapshot.removed_files()` provide the primitives; the Artemis archive's CDF tooling wraps these into a stream-of-batches API. Both formats produce the same result for the same diff; the format-specific details are around how the row-level data is materialized (Delta supports per-row CDC with `_change_type` columns; Iceberg infers the change types from the file-level diff).

The cost of CDF computation is proportional to the number of changes between the snapshots, not the table size. A consumer that polls for changes every 30 seconds sees only the files added/removed in the last 30 seconds — typically a handful of files, regardless of the table's total size. This is the property that makes CDF cheap enough to drive downstream pipelines without batch-job overhead.

### Long-Running Replays and the Expiration Boundary

The replay workload has a subtlety the operational team must handle: replay queries are long-running by nature. A full-mission replay reads months of data; the query can run for hours. The pin protocol holds the snapshot for the query's duration, which means the snapshot's data files must remain on disk for at least that long.

The retention window is the operational lever. The Artemis archive's 30-day window allows replays of up to 30 days of history to complete in real time; replays against older history use the longer-retention cold-archive backup tier. Replay queries that exceed the retention window fail with a clear error code; the application either accepts the failure or escalates to the cold-archive tier.

A separate concern: **replay queries are read-heavy and may starve concurrent ingest writers' bandwidth**. The Artemis read path runs replays through a separate worker pool with rate-limiting on object-store GET requests, so the ingest writers' Parquet uploads are not delayed. This is operational shaping, not a property of the table format itself; the same pattern applies to any read-heavy workload sharing infrastructure with writers.

### The Replay Workflow End to End

A canonical Mission Replay flow:

1. The analyst specifies a target timestamp (typically the moment before an anomaly observed in orbit).
2. The replay engine calls `pin_at_time(catalog, "orbital_object_registry", target_timestamp_ms)` to resolve the snapshot.
3. The replay engine reads the snapshot's `schema_id` and looks up the corresponding schema from the snapshot's `schemas` field.
4. The replay engine plans the scan against the pinned snapshot (Module 3's three-pass pruning, using the snapshot's partition spec).
5. The replay engine reads the scan plan, producing Arrow batches projected against the snapshot-time schema.
6. The engine emits the batches to the consumer (an analyst's notebook, a dashboard, or a downstream replay validator).

Each step is bounded by the snapshot's contents. The replay produces deterministic output: running the same query at a different time produces the same result, because the snapshot is unchanged. This determinism is what makes replay-based analyses scientifically valid; the analyst can reproduce another team's investigation by replaying against the same snapshot and seeing the same data.

---

## Core Mechanics in Code

### Reading the Snapshot History

The snapshot history is the source of truth for time-travel resolution. The function below is the minimal reader.

```rust,ignore
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotHistoryEntry {
    pub snapshot_id: SnapshotId,
    pub timestamp_ms: i64,
    pub metadata_path: String,
    pub schema_id: u32,
    pub partition_spec_id: u32,
}

/// Read the table's snapshot history. The history is stored as a small
/// metadata file (Iceberg's metadata_log) that is updated on every commit.
/// Entries older than the retention window are removed by snapshot
/// expiration (Module 6).
pub async fn read_snapshot_history(
    catalog: &PostgresCatalog,
    table: &str,
) -> Result<Vec<SnapshotHistoryEntry>> {
    let entry = catalog.get_current(table).await?;
    let table_metadata: TableMetadata = read_metadata_file(&entry.metadata_path).await?;
    Ok(table_metadata.snapshot_log)
}

/// Resolve a target timestamp to the snapshot ID that was current at
/// that moment. The convention: snapshot N is current from
/// `timestamp_ms[N]` to `timestamp_ms[N+1]`. A target equal to a
/// commit timestamp resolves to the new snapshot (not the prior one).
pub fn resolve_timestamp(
    history: &[SnapshotHistoryEntry],
    target_ms: i64,
) -> Result<&SnapshotHistoryEntry> {
    // The history is in commit order (ascending timestamp). Find the
    // last entry with timestamp <= target.
    let mut found: Option<&SnapshotHistoryEntry> = None;
    for entry in history {
        if entry.timestamp_ms <= target_ms {
            found = Some(entry);
        } else {
            break;
        }
    }
    found.ok_or_else(|| anyhow::anyhow!(
        "no snapshot at or before {target_ms} (oldest is {})",
        history.first().map(|e| e.timestamp_ms).unwrap_or(0),
    ))
}
```

The pattern. The snapshot history is small (one entry per commit, dozens of bytes each) and bounded by the retention window. A typical table with hourly commits and a 30-day retention has 720 entries; reading the history is one small object-store GET. The linear search is fast enough at this scale; a sorted index would be unnecessary engineering.

### Reading at a Snapshot

The actual time-travel read is the Module 2/3 read path applied to a pinned snapshot. The function below brings together the pieces.

```rust,ignore
use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

pub enum SchemaMode {
    /// Project against the table's current schema. New columns appear
    /// as nulls; dropped columns are filtered out.
    Current,
    /// Project against the schema that was current at the pinned
    /// snapshot's commit time.
    SnapshotTime,
}

/// Plan and execute a time-travel read at the given snapshot ID with
/// the specified schema-projection mode. Returns a stream of record
/// batches with the appropriate schema.
pub async fn read_at_snapshot(
    catalog: &PostgresCatalog,
    table: &str,
    snapshot_id: SnapshotId,
    predicate: &Predicate,
    schema_mode: SchemaMode,
) -> Result<RecordBatchStream> {
    let pinned = pin_by_id(catalog, table, snapshot_id).await?;
    let snapshot: Snapshot = read_metadata_file(&pinned.snapshot_metadata_path).await?;

    // Look up the schema to project against.
    let table_metadata = read_table_metadata(catalog, table).await?;
    let schema: SchemaRef = match schema_mode {
        SchemaMode::Current => table_metadata.current_schema(),
        SchemaMode::SnapshotTime => table_metadata.schema_by_id(snapshot.schema_id)?,
    };

    // Plan the scan with the snapshot's partition spec (which may
    // differ from the current spec — Lesson 3 develops this).
    let plan = plan_scan_against_snapshot(&snapshot, predicate).await?;

    // Execute the plan, projecting each Parquet read against the
    // chosen schema. The Parquet reader handles column-ID-based
    // mapping (Lesson 3) to project files written with different
    // schemas against the chosen schema.
    let stream = execute_plan_with_schema(plan, schema).await?;
    Ok(stream)
}
```

The structure. The read protocol is the same as for the current snapshot; the only changes are which snapshot is pinned and which schema is used for projection. The data file reads are unchanged. This is what makes time travel cheap to implement once the snapshot-isolation foundation is in place — it is the same read path with a different pin.

### Computing a Snapshot Diff for CDF

The change-data-feed primitive: given two snapshot IDs, return the added and removed files.

```rust,ignore
use anyhow::Result;
use std::collections::HashSet;

pub struct SnapshotDiff {
    pub added_files: Vec<DataFile>,
    pub removed_files: Vec<DataFile>,
}

/// Compute the file-level diff between two snapshots. The diff is the
/// set of files added in any snapshot strictly between old and new
/// (exclusive of old, inclusive of new), and the set of files removed
/// in the same range. The implementation walks the snapshot chain from
/// old+1 to new, accumulating Added and Deleted manifest entries.
pub async fn snapshot_diff(
    catalog: &PostgresCatalog,
    table: &str,
    old_snapshot_id: SnapshotId,
    new_snapshot_id: SnapshotId,
) -> Result<SnapshotDiff> {
    let history = read_snapshot_history(catalog, table).await?;

    // Find the range of snapshots strictly after old, up to and
    // including new.
    let old_idx = history.iter().position(|h| h.snapshot_id == old_snapshot_id)
        .ok_or_else(|| anyhow::anyhow!("old snapshot not in history"))?;
    let new_idx = history.iter().position(|h| h.snapshot_id == new_snapshot_id)
        .ok_or_else(|| anyhow::anyhow!("new snapshot not in history"))?;
    if new_idx <= old_idx {
        return Err(anyhow::anyhow!("new must be strictly after old"));
    }
    let range = &history[(old_idx + 1)..=new_idx];

    // Accumulate file-level changes across the range. Each snapshot's
    // manifests list one entry per file with status Added/Existing/Deleted;
    // we collect the Added and Deleted ones across the range.
    let mut added: Vec<DataFile> = Vec::new();
    let mut removed: Vec<DataFile> = Vec::new();
    for entry in range {
        let snap: Snapshot = read_metadata_file(&entry.metadata_path).await?;
        let manifest_list = read_manifest_list(&snap.manifest_list_path).await?;
        for ml_entry in manifest_list.manifests {
            // Only read manifests that have any added or deleted entries —
            // existing-only manifests have nothing to contribute.
            if ml_entry.added_data_files == 0 && ml_entry.deleted_data_files == 0 {
                continue;
            }
            let manifest = read_manifest(&ml_entry.manifest_path).await?;
            for me in manifest.entries {
                match me.status {
                    EntryStatus::Added => added.push(me.data_file),
                    EntryStatus::Deleted => removed.push(me.data_file),
                    EntryStatus::Existing => {}
                }
            }
        }
    }

    Ok(SnapshotDiff { added_files: added, removed_files: removed })
}
```

The cost. The function reads `O(range)` snapshot metadata files and `O(changed_manifests)` manifest files; the data files themselves are not read at all for the diff computation. A typical CDF poll comparing two consecutive snapshots reads one snapshot file and the one new manifest — single-digit GETs, well under a second. The CDF consumer reads the data files themselves only after the diff has identified the relevant ones; the data-file reads are bounded by the actual change volume.

The `manifest_list.manifests` filter that skips manifests with no Added/Deleted entries is an important optimization. Most commits in a long-running table touch only a small fraction of the manifests; skipping the rest is what keeps CDF cheap relative to the table's total size. The skipping is safe because a manifest that has no Added or Deleted entries contributes nothing to the diff.

---

## Key Takeaways

- **Time travel is the pin protocol applied to a non-current snapshot.** The mechanism is unchanged from Lesson 1; the only new piece is resolving snapshot IDs (by-ID or by-timestamp) via the snapshot history metadata.
- **Two schema-projection modes** for time-travel reads: current-schema (back-fill nulls for columns added since; drop columns added since) and snapshot-time-schema (the table as it actually looked then). The Mission Replay Engine defaults to snapshot-time for the accident-investigation workload.
- **Change Data Feed reads the difference between two snapshots** by accumulating Added/Deleted manifest entries across the snapshot chain. The cost is proportional to the changes between the snapshots, not the table size — this is what makes CDF cheap enough to drive downstream pipelines without batch overhead.
- **The retention window bounds time travel.** Snapshots older than the window have been physically removed and cannot be replayed; the operational team sets the window based on the longest-supported replay duration plus a margin.
- **Replay queries are deterministic and reproducible** because the snapshot is immutable. Two analysts running the same time-travel query against the same snapshot ID get identical results; this is what makes replay-based investigations scientifically valid.

{{#quiz quiz-02.toml}}
