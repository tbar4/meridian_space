# Lesson 2 — The Manifest Hierarchy and Snapshot Model

**Module:** Data Lakes — M02: Open Table Formats
**Position:** Lesson 2 of 3
**Source:** Apache Iceberg specification, "Table Spec" and "Manifests" sections (`iceberg.apache.org/spec`). Delta Lake protocol, "Actions" section, for the contrast where useful. Iceberg whitepaper (Ryan Blue, Netflix, 2018).

> **Source note:** This lesson is synthesis-mode against the Iceberg specification. The four-level hierarchy and the snapshot-update mechanic are accurate to the spec; the per-format detail differences (Iceberg's V1 vs V2 spec, Delta's transaction log vs Iceberg's snapshot pointer) are noted where they illuminate a design decision. Verification against the current spec recommended.

---

## Context

Lesson 1 established the table format's job: provide an atomic, versioned, schema-enforced view over a directory of immutable data files. This lesson develops the *shape* of the metadata that does the work. The shape is a four-level hierarchy: a catalog pointer to a snapshot, a snapshot pointer to a manifest list, a manifest list pointer to manifests, a manifest pointer to data files. Each level has a job that the level below cannot do. The hierarchy is what makes commits cheap, queries fast, and pruning effective.

The hierarchy is not Iceberg-specific. Delta Lake uses a transaction log instead of a snapshot pointer, but the log is functionally a sequence of snapshot deltas; reading it produces the same logical structure. Hudi uses a slightly different shape but the same essential layering. Once the engineer understands the four-level hierarchy, switching between formats is a question of vocabulary, not architecture. The Artemis archive uses Iceberg, so this lesson uses Iceberg's vocabulary throughout; the design considerations transfer.

The lesson develops each level in isolation, then traces a worked example through all four levels: a single commit's effect on the metadata, end to end. The capstone project implements this hierarchy in code; this lesson is the model the capstone is implementing.

---

## Core Concepts

### The Four-Level Hierarchy

The metadata hierarchy answers two questions: "what files are in the table at the current version?" and "what files were in the table at version N?". The hierarchy makes both questions cheap by trading a small amount of indirection cost for substantial reductions in the amount of metadata a reader must scan.

```text
catalog pointer
       │
       ▼
   snapshot          ← table version: schema, partitioning, statistics
       │
       ▼
 manifest list       ← per-snapshot listing of manifests + per-manifest stats
       │
       ├─▶ manifest  ← group of data files, typically per-partition
       ├─▶ manifest
       └─▶ manifest
              │
              ├─▶ data file (Parquet)
              ├─▶ data file (Parquet)
              └─▶ data file (Parquet)
```

The naming follows Iceberg. **Catalog pointer**: a small piece of state (a row in a database, an S3 object key, a ZooKeeper node) that holds "the current version of the table is snapshot S." **Snapshot**: a metadata file describing the table at one point in time: its schema, partition spec, and a pointer to the manifest list that enumerates the table's data files. **Manifest list**: a file listing the manifests that, together, enumerate every data file in the snapshot, plus per-manifest summary statistics. **Manifest**: a file listing some subset of the table's data files (typically grouped by partition), plus per-file statistics. **Data file**: a Parquet file as produced by Module 1's writer.

Each level fans out to the next. A catalog has one current snapshot per table. A snapshot's manifest list contains tens to hundreds of manifests. A manifest contains tens to thousands of data files. A 100k-data-file table has perhaps 1k manifests, one manifest list, one snapshot. Read planning starts at the snapshot and prunes aggressively at every level — most queries touch one snapshot, a small fraction of its manifests, and an even smaller fraction of those manifests' data files. The pruning power compounds.

### Snapshots Are Immutable

The single most important property of the snapshot model: **once written, a snapshot is never modified.** A new commit produces a new snapshot file, leaving the old snapshot file untouched. The catalog pointer changes; the snapshots themselves are content-addressed (or near enough — Iceberg uses unique snapshot IDs and timestamp-prefixed paths).

Immutability is what makes time travel and concurrent reads cheap. A reader that started a query against snapshot S can finish the query against snapshot S even if a hundred concurrent writers committed new snapshots in the meantime — the snapshot S metadata is still on disk, unmodified, fully readable. The catalog pointer's current value is irrelevant to the in-flight reader; the reader holds the snapshot ID it started with and reads against that.

Immutability also makes commits cheap: a commit is "write some new metadata files, then swap the catalog pointer." None of the old metadata is touched. The old metadata's storage lives forever, in principle; in practice, the snapshot-expiration job (Module 6) deletes old snapshots after a configurable retention window, but the deletion is decoupled from any commit.

### Manifests: The Pruning Unit

A manifest is the smallest unit of "many data files described together." A manifest's payload is one record per data file: the file path, the partition tuple the file belongs to, the per-column statistics (min, max, null count, distinct count), the row count, the byte size. The manifest is structured as a columnar file itself — typically Avro in Iceberg — for the same reason data files are columnar: queries against the manifest typically read a few statistics columns out of many.

The manifest list, one level up, holds one record per manifest with summary statistics across the manifest's files: the partition ranges spanned by the manifest, the count of files, the count of rows, the count of deleted records. The manifest list is the **first pruning step** at query time: a query against `mission_id = 'apollo-7'` reads the manifest list, finds the three manifests whose partition range includes `apollo-7`, and ignores the other ninety-seven manifests entirely.

The second pruning step is inside the chosen manifests: read each chosen manifest's data-file records, find the data files whose `mission_id` statistic contains `apollo-7`, and read only those. The third pruning step is inside each chosen data file: use the Parquet footer's per-column-chunk statistics (Module 1) to prune row groups. Pruning compounds across all three levels; the read fraction at each level is multiplied at the next.

The grouping of data files into manifests is a design lever. Iceberg's default is one manifest per partition per commit, which produces good co-locality (files for the same partition are in the same manifest) but a many-files-per-manifest tail over time. The optimization process (Module 6's compaction) periodically rewrites manifests to consolidate small ones; this is a metadata-level compaction, complementary to the data-level compaction that consolidates small Parquet files.

### The Catalog: External Atomicity

The catalog pointer is the part of the table format that lives *outside* the data lake's object store. Object stores have a weak primitive for atomicity: most support per-object atomic write (the rename pattern), but few support cross-object atomicity or transactional CAS in the general case. Iceberg's design pushes the CAS requirement to an external catalog — Hive Metastore, Project Nessie, AWS Glue, JDBC databases, a small DynamoDB row, ZooKeeper, or in the Artemis case a small Postgres row keyed on table name.

The catalog's job is exactly two operations: `get_current_snapshot(table)` and `compare_and_swap_snapshot(table, expected_old, new)`. The CAS must be linearizable — concurrent writers must not both succeed in moving the pointer if they both based their new snapshot on the same old snapshot. Everything else the table format does (schema, manifests, data files) lives in the object store and inherits the catalog's CAS for atomicity.

The choice of catalog is operational, not architectural. Postgres is the obvious choice for systems that already run a transactional database; Nessie adds Git-like branching semantics on top; DynamoDB is the AWS-native option; Hive Metastore is legacy compatibility for systems migrating off Hadoop. The Artemis archive uses Postgres on the ground-segment ingestion node, which is already deployed for unrelated control-plane state. The single-row-per-table commit pattern is so cheap that even at very high commit rates the catalog is not the bottleneck.

The Delta Lake protocol takes a different approach: instead of an external catalog, Delta uses an append-only transaction log stored in the object store (`_delta_log/00000000000000000000.json`, `_delta_log/00000000000000000001.json`, …). The commit is "atomically create a new log file with the next sequential number." On filesystems that support atomic create-if-not-exists (HDFS, POSIX), this works directly. On S3, Delta uses a coordination service (DynamoDB, a small extra catalog) to provide the missing CAS. The two designs converge: both need external coordination for the commit primitive; the names and shapes differ.

### A Worked Example: One Commit, Four Levels

Trace the change to the metadata when one batch ingest adds twelve new data files to the Artemis telemetry table.

**Before the commit.** Table is at snapshot `S101`. Snapshot `S101` references manifest list `ML101`. Manifest list `ML101` references manifests `M1, M2, …, M100`. Each manifest references some data files. The catalog row says `current_snapshot_id = S101`.

**During the commit.** The writer:

1. Writes the twelve new data files to the object store, each one via the rename pattern. Files are in the object store but no metadata references them yet.
2. Writes one new manifest, `M101`, containing exactly the twelve new data files (one manifest per commit is Iceberg's default).
3. Writes a new manifest list, `ML102`, containing every manifest from `ML101` plus the new `M101`. Note `ML101` is not modified — `ML102` is a new file with `101 + 1 = 102` entries.
4. Writes a new snapshot, `S102`, with `parent_snapshot_id = S101`, the same schema as `S101`, and a pointer to `ML102`.
5. Issues `compare_and_swap_snapshot(table, expected_old=S101, new=S102)` against the catalog.

**After the commit.** The catalog row says `current_snapshot_id = S102`. Readers that started before step 5 see `S101`. Readers that start after step 5 see `S102`. There is no observable in-between state. The old metadata files (`S101`, `ML101`) remain in the object store, available for time-travel reads against the old version of the table.

The cost of the commit: one new manifest of twelve entries, one new manifest list of 101 entries, one new snapshot record, one catalog CAS. The cost is proportional to the size of the commit, not the size of the table. The hundred existing manifests are referenced by the new manifest list but not rewritten. This is what makes the format scale to millions of files — every commit is local to its own changes.

---

## Code Examples

### Modeling the Hierarchy in Rust

The Artemis capstone implements the four levels as Rust types. The structure below is a sketch — the production version has more fields per the Iceberg spec, but the shape is correct.

```rust,ignore
use std::collections::HashMap;
use anyhow::Result;
use serde::{Deserialize, Serialize};

pub type SnapshotId = u64;
pub type ManifestPath = String;
pub type DataFilePath = String;

/// Catalog row — the only mutable state in the table format. Held in
/// a transactional catalog (Postgres in the Artemis case) that
/// provides linearizable CAS.
#[derive(Debug, Clone)]
pub struct CatalogEntry {
    pub table_name: String,
    pub current_snapshot_id: SnapshotId,
    // The metadata path the catalog points at. Reading the table starts here.
    pub metadata_path: String,
}

/// Snapshot metadata file. Immutable once written. References a manifest
/// list that, together with the schema and partition spec, fully
/// determines the table at this version.
#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub snapshot_id: SnapshotId,
    pub parent_snapshot_id: Option<SnapshotId>,
    pub timestamp_ms: i64,
    pub schema_id: u32,
    pub partition_spec_id: u32,
    pub manifest_list_path: String,
    /// Summary of what this snapshot did relative to its parent.
    /// Used by maintenance and audit tooling.
    pub summary: SnapshotSummary,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotSummary {
    pub operation: SnapshotOp,
    pub added_files: u32,
    pub removed_files: u32,
    pub added_rows: u64,
    pub removed_rows: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SnapshotOp {
    Append,
    Overwrite,
    Replace,
    Delete,
}

/// Manifest list: one entry per manifest, with per-manifest summary
/// statistics for the first pruning pass at query planning time.
#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestList {
    pub manifests: Vec<ManifestListEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestListEntry {
    pub manifest_path: ManifestPath,
    pub added_data_files: u32,
    pub existing_data_files: u32,
    pub deleted_data_files: u32,
    pub partition_summaries: Vec<PartitionSummary>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionSummary {
    pub partition_field: String,
    pub lower_bound: Vec<u8>, // serialized partition value
    pub upper_bound: Vec<u8>,
    pub contains_null: bool,
}

/// Manifest: one entry per data file, with per-file statistics for the
/// second pruning pass. Data files are not modified; the manifest is
/// the level where add/remove decisions are recorded.
#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub entries: Vec<ManifestEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub status: EntryStatus,
    pub data_file: DataFile,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum EntryStatus {
    Existing,
    Added,
    Deleted,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataFile {
    pub path: DataFilePath,
    pub file_format: FileFormat,
    pub partition: HashMap<String, Vec<u8>>,
    pub record_count: u64,
    pub file_size_bytes: u64,
    pub column_sizes: HashMap<u32, u64>,
    pub value_counts: HashMap<u32, u64>,
    pub null_counts: HashMap<u32, u64>,
    pub lower_bounds: HashMap<u32, Vec<u8>>,
    pub upper_bounds: HashMap<u32, Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum FileFormat {
    Parquet,
    Avro,
    Orc,
}
```

The shape to notice. Every level holds enough information to make pruning decisions without consulting the level below. The manifest list's per-manifest summaries answer "does this manifest contain partition value X" without reading the manifest. The manifest's per-data-file statistics answer "does this data file contain column value X" without reading the file. Statistics propagate upward at write time so they are available for downward pruning at read time.

### Read Planning: Three Pruning Passes

The reader's job is to turn a query — say, `WHERE mission_id = 'apollo-7' AND panel_voltage > 28.5` — into a minimal set of data files to read. The metadata hierarchy makes this three sequential filters.

```rust,ignore
use anyhow::Result;

/// Plan the data files to read for a query against the current snapshot.
/// Returns the data files that *might* contain matching rows; the reader
/// still applies the predicate row-wise after reading, because the
/// statistics are bounds, not exact matches.
async fn plan_query(
    catalog: &Catalog,
    table: &str,
    predicate: &Predicate,
) -> Result<Vec<DataFile>> {
    // Step 0: Resolve the current snapshot. One catalog read, one
    // metadata file fetch.
    let entry = catalog.get_current(table).await?;
    let snapshot: Snapshot = read_metadata_file(&entry.metadata_path).await?;
    let manifest_list: ManifestList =
        read_metadata_file(&snapshot.manifest_list_path).await?;

    // Step 1: Prune at the manifest-list level. A manifest whose
    // partition summary doesn't overlap the predicate's partition
    // constraints is skipped entirely. For mission_id='apollo-7' this
    // typically drops ~95% of manifests.
    let candidate_manifests: Vec<_> = manifest_list
        .manifests
        .iter()
        .filter(|m| partition_summary_overlaps(&m.partition_summaries, predicate))
        .collect();

    // Step 2: Prune at the manifest level. Open each candidate manifest
    // and apply the predicate to per-data-file statistics. For
    // panel_voltage > 28.5, drop files whose upper_bound for that column
    // is <= 28.5.
    let mut candidate_files: Vec<DataFile> = Vec::new();
    for manifest_entry in &candidate_manifests {
        let manifest: Manifest = read_metadata_file(&manifest_entry.manifest_path).await?;
        for entry in manifest.entries {
            if matches!(entry.status, EntryStatus::Existing | EntryStatus::Added) {
                if data_file_might_match(&entry.data_file, predicate) {
                    candidate_files.push(entry.data_file);
                }
            }
        }
    }

    // Step 3 (not shown): The reader will further prune row groups
    // within each data file using the Parquet footer statistics from
    // M1. That step happens at Parquet open time, not metadata
    // planning time, so it's outside this function.

    Ok(candidate_files)
}
```

The discipline this enforces. **Each pruning step reads only what the previous step did not prune.** The reader never lists the object store, never reads a data file it has already proven cannot contain matching rows, never opens a manifest it has already proven contains no matching files. The metadata cost is proportional to what survived pruning; the data cost is proportional to what survived further pruning. The compounding is what makes the format usable at the 100k-file scale.

### Writing a Snapshot

The write-side counterpart: producing a new snapshot from a base snapshot plus a set of file changes. The function is metadata-only — the data files are assumed already written.

```rust,ignore
use anyhow::Result;
use std::time::SystemTime;

/// Produce a new snapshot from the base snapshot plus a set of newly
/// added data files. The snapshot, manifest list, and new manifest are
/// written to the object store; the catalog CAS happens in the caller
/// (Lesson 3).
pub async fn build_append_snapshot(
    base: &Snapshot,
    base_manifest_list: &ManifestList,
    new_data_files: Vec<DataFile>,
    metadata_dir: &str,
) -> Result<(Snapshot, String)> {
    let now_ms = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as i64;

    let new_snapshot_id = next_snapshot_id();

    // 1. Build the new manifest from the new data files. One manifest
    // per commit is Iceberg's default; manifest compaction (Module 6)
    // periodically consolidates small manifests.
    let new_manifest = Manifest {
        entries: new_data_files
            .iter()
            .cloned()
            .map(|df| ManifestEntry {
                status: EntryStatus::Added,
                data_file: df,
            })
            .collect(),
    };
    let new_manifest_path = format!(
        "{metadata_dir}/m{new_snapshot_id}.avro"
    );
    write_metadata_file(&new_manifest_path, &new_manifest).await?;

    // 2. Build the new manifest list: the base list's entries plus an
    // entry for the new manifest. The base list is not modified.
    let new_manifest_list = ManifestList {
        manifests: {
            let mut all = base_manifest_list.manifests.clone();
            all.push(ManifestListEntry {
                manifest_path: new_manifest_path.clone(),
                added_data_files: new_data_files.len() as u32,
                existing_data_files: 0,
                deleted_data_files: 0,
                partition_summaries: summarize_partitions(&new_data_files),
            });
            all
        },
    };
    let new_list_path = format!(
        "{metadata_dir}/ml{new_snapshot_id}.avro"
    );
    write_metadata_file(&new_list_path, &new_manifest_list).await?;

    // 3. Build the snapshot record.
    let snapshot = Snapshot {
        snapshot_id: new_snapshot_id,
        parent_snapshot_id: Some(base.snapshot_id),
        timestamp_ms: now_ms,
        schema_id: base.schema_id,
        partition_spec_id: base.partition_spec_id,
        manifest_list_path: new_list_path,
        summary: SnapshotSummary {
            operation: SnapshotOp::Append,
            added_files: new_data_files.len() as u32,
            removed_files: 0,
            added_rows: new_data_files.iter().map(|f| f.record_count).sum(),
            removed_rows: 0,
        },
    };
    let snapshot_path = format!(
        "{metadata_dir}/s{new_snapshot_id}.json"
    );
    write_metadata_file(&snapshot_path, &snapshot).await?;

    Ok((snapshot, snapshot_path))
}
```

What to notice. Three new files are written; nothing existing is modified. The new snapshot's `parent_snapshot_id` records the version this commit was based on — Lesson 3's CAS uses this to detect conflicts when two writers concurrently base their commits on the same parent. The function returns the snapshot and its path; the caller's job is to perform the catalog CAS that makes the snapshot visible.

---

## Key Takeaways

- The table format's metadata is a **four-level hierarchy**: catalog pointer → snapshot → manifest list → manifests → data files. Each level fans out to the next; pruning at each level compounds.
- **Snapshots are immutable.** A commit produces new metadata files; old metadata files remain. This is what makes time travel and concurrent reads cheap, and what makes commits cost proportional to the change size, not the table size.
- **Manifests are the pruning unit.** Manifest-list summaries enable partition-level pruning without reading manifests; manifest entries enable file-level pruning without reading data files; Parquet footers enable row-group pruning without reading data pages. The three layers of statistics propagate up at write time so they can be used for pruning down at read time.
- **The catalog provides external atomicity.** Object stores don't offer cross-object CAS; the table format pushes the CAS requirement to a transactional catalog (Postgres, Hive Metastore, Nessie). The catalog's job is exactly `get_current` and `compare_and_swap`.
- **A commit's cost is proportional to the change.** Write the new data files, write one new manifest, write one new manifest list (referencing the base's manifests plus the new one), write the snapshot, CAS the catalog. The base manifests and old data files are not rewritten.

{{#quiz quiz-02.toml}}
