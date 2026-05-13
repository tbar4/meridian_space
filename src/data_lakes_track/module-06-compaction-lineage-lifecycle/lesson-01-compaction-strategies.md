# Lesson 1 — Compaction Strategies

**Module:** Data Lakes — M06: Compaction, Lineage, and Lifecycle
**Position:** Lesson 1 of 3
**Source:** *Database Internals* — Alex Petrov, Chapter 7 ("Log-Structured Storage — Compaction") for the analogous LSM compaction discipline. Apache Iceberg specification, "Rewriting Data Files" section. Delta Lake protocol's "OPTIMIZE" command documentation for contrast.

> **Source note:** The compaction strategies and tradeoffs are drawn from the lakehouse community's accumulated practice; the LSM analogy is grounded in *Database Internals*. Specific implementation details should be verified against the current Iceberg maintenance APIs.

---

## Context

The Artemis cold archive has been ingesting data for two years. The ingestion pipeline commits a new snapshot every 30 seconds with the previous 30 seconds' downlinks; a typical commit produces 8-15 data files. After two years that's roughly 2 million data files, distributed across 40,000 partitions, with an average file size of around 18 MB — substantially smaller than the 128 MB target Module 1 set. The metadata is bloated; the planner reads thousands of manifests per query; per-file overhead dominates the actual data read; the analyst queries that used to complete in 30 seconds now take 5 minutes.

The fix is **compaction**: rewriting many small files into fewer large files. The discipline is mechanically straightforward (read the small files, write a large file with the same rows, atomically swap the file set in a single commit) but operationally subtle. Compaction competes with the ingest workload for object-store bandwidth, with the query workload for compute, and with the storage-tier infrastructure for IOPS. A naive compaction that runs full-speed continuously degrades the live system; a too-conservative compaction never catches up with the small-file accumulation.

This lesson develops the compaction discipline. The strategies — bin-packing, sort-based, Z-order rewriting — that handle different small-file patterns. The safety properties that make compaction non-disruptive (atomic file-set swap via overwrite commit; concurrent readers unaffected; rollback on failure). The operational tuning that keeps compaction running at the right rate against the right partitions. The capstone's lifecycle worker runs compaction continuously against the production archive; this lesson is the design.

---

## Core Concepts

### The Small-File Problem at Scale

The small-file problem comes from a structural mismatch between ingest cadence and target file size. Ingest commits arrive at the cadence the workload requires — for the Artemis archive, every 30 seconds, because the downlink path produces data at that rate and the analyst workload wants the data to be queryable within a minute. The 30-second window contains a finite amount of data; for a typical mission with 8 active payloads producing telemetry at 10 Hz, that's 2,400 samples per payload, or about 19 KB of payload per row times 8 payloads, or roughly 4 MB of uncompressed data per partition per commit. After ZSTD-3 compression, that's around 1-2 MB per data file per commit.

The result: small files. Each commit produces one file per active partition, each one about 1 MB, well below the 128 MB target the Module 1 writer aims for. Over time, partitions accumulate hundreds or thousands of small files. The Module 3 read planning consults all of them. The per-file overhead (footer parse, statistics evaluation, range request setup) is a fixed cost; for 1-MB files, the overhead dominates the data read. The read latency grows with the file count, not the data size.

DDIA's framing is the same one *Database Internals* (Petrov, Ch. 7) develops for LSM trees: small files are the inevitable consequence of a system that commits more often than its target file size justifies. The LSM tree's compaction is exactly the analogous discipline — merge small SSTables into larger SSTables, keep the file count bounded by an upper limit per level, keep the per-query overhead constant. The lakehouse's compaction does the same thing at the table-format layer.

The thresholds that define "small": the lakehouse community's accumulated practice is that files below 32 MB are "small" (they impose unacceptable per-file overhead at query time) and files above 1 GB are "too large" (they don't fit in a row-group at the target row count and they impose long re-decode times when a row group's worth of data is read for projection). The Module 1 target of 128 MB sits in the middle; compaction's job is to consolidate small files toward this size without overshooting into too-large.

### Compaction as an Overwrite Commit

The mechanics: compaction is an overwrite commit (Module 2 Lesson 3). The compactor reads the source files, writes the consolidated output file, then commits a snapshot that adds the new file and removes the source files — both changes atomically applied via the CAS protocol. The compaction is invisible to readers because the catalog pointer advances atomically; readers pinned to the pre-compaction snapshot continue to see the source files (the snapshot's metadata still references them); readers starting after the compaction see the consolidated file.

```text
Before compaction (snapshot S):
  /data/abc.parquet  (1 MB, 1k rows)
  /data/def.parquet  (1 MB, 1k rows)
  /data/ghi.parquet  (2 MB, 2k rows)
  /data/jkl.parquet  (1 MB, 1k rows)
  /data/mno.parquet  (1 MB, 1k rows)
  /data/pqr.parquet  (2 MB, 2k rows)
  /data/stu.parquet  (1 MB, 1k rows)
  /data/vwx.parquet  (1 MB, 1k rows)
  Total: 8 files, 10 MB, 10k rows

After compaction (snapshot S+1):
  /data/{compacted}.parquet  (10 MB, 10k rows)
  Total: 1 file, 10 MB, 10k rows

Catalog pointer swap S → S+1 is atomic.
Readers pinned to S still see 8 files (their snapshot is unchanged).
Readers starting at S+1 see 1 file.
The 8 source files remain on disk until snapshot expiration (Lesson 2) removes them.
```

Two properties this gives. **Compaction is safe under concurrent reads.** A query that started before compaction proceeds against its pinned snapshot; the data files are unchanged. A query that started after compaction sees the consolidated layout. **Compaction is safe under concurrent writes.** The compaction commit uses the same CAS protocol as any other commit; concurrent ingest commits and compaction commits race at the CAS, with the loser rebasing and retrying. For append-only ingest, the rebase is cheap (Module 2 Lesson 3); for compaction it's more expensive because the compactor must re-evaluate which source files still exist, but the cost is bounded by the compaction's scope, not the table size.

### Bin-Packing: The Simplest Strategy

The simplest compaction strategy is **bin-packing**: find small files, group them into bins targeting the 128 MB output size, write one consolidated file per bin. The strategy is content-agnostic — it doesn't sort, it doesn't cluster, it just concatenates.

The procedure:
1. List the data files in a target partition. (The partition scope matters; compacting across partitions would violate the file-per-partition-tuple discipline from Module 3 Lesson 1.)
2. Filter to files below the small-file threshold (e.g., 32 MB).
3. Group the small files into bins of roughly 128 MB total — typically by sorting by size descending and greedily packing into bins.
4. For each bin, read all source files, concatenate the row batches, write one output file with the consolidated rows.
5. Commit a single overwrite snapshot that removes all the source files and adds all the output files for the partition.

The strategy's strength is its simplicity and its low per-row cost. Reading and rewriting rows without sorting is O(rows) with a small constant; the output file's compression is similar to the inputs' (the row distribution doesn't change). The strategy's weakness is that it preserves whatever row ordering the inputs had — typically arrival order, which is poorly clustered for analytical queries. If the source files were unclustered, the output is also unclustered; bin-packing doesn't improve clustering, it just reduces file count.

For workloads where clustering is not a concern (typical when the partition spec already captures the dominant query dimensions), bin-packing is the right strategy. It's the cheapest compaction; it solves the file-count problem; it doesn't try to do more. The Artemis archive uses bin-packing for partitions where queries always specify both partition columns (`mission_id` and `day`), because at that granularity additional clustering buys little.

### Sort-Based Compaction

When clustering matters — Module 3 Lesson 2's `(payload_id, sensor_kind)` clustering for the Artemis archive — bin-packing alone isn't enough. The compaction must produce files with tight per-column statistics for the cluster columns, which requires sorting the rows before writing.

**Sort-based compaction** runs bin-packing's bin-grouping step but, instead of concatenating the source files' batches in arrival order, sorts the rows in each bin by the sort order before writing. For a linear sort order (`ORDER BY payload_id, sensor_kind`), this is a multi-way merge over the source files; each source file is already sorted within itself (if produced by a sort-aware writer), so the merge is O(rows × log(files)) — sub-quadratic and tractable for typical compaction bin sizes.

The output files have tight per-column statistics on the sort columns. A bin containing 64 source files, each averaging 8 distinct `payload_id` values mixed randomly, produces compacted files where each file has 1-2 distinct `payload_id` values. The per-file `payload_id` statistic is tight; the query-time pruning from Module 3 Lesson 3 prunes hundreds of compacted files to single-digit files for any specific `payload_id` predicate.

The cost of sort-based compaction is the sort: an extra O(log n) factor per row plus the memory to hold the merge state. For typical Artemis bin sizes (128 MB compacted output, around 1M rows), the sort fits in memory comfortably and adds maybe 10-15% to the bin-packing cost. The tradeoff is favorable for any workload where clustering pays off.

### Z-Order Compaction

The most aggressive strategy is **Z-order compaction** (Module 3 Lesson 2's space-filling curve applied at compaction time). The compactor reads source files, computes Z-order keys for every row across the sort columns, sorts globally by Z-order key, and writes files in Z-order. The output files cluster on multiple dimensions simultaneously — both `payload_id` and `sensor_kind`, in the Artemis case.

The cost is the Z-order key computation plus the sort. Z-order computation is cheap (a few cycles per row; the key is a u64 or u128). The sort is the same as sort-based compaction's. The total overhead over bin-packing is around 20%; the clustering improvement is the 5-10× pruning win Module 3 Lesson 2 measured.

The Artemis archive uses Z-order compaction on the SDA observation table. The capstone's lifecycle worker computes Z-order keys at compaction time using the column-rank normalization from Module 3 (the per-column ranks are computed once per partition per compaction; ranks are cheap given Arrow's `compute::rank` function).

### Concurrent Compaction: The Conflict Pattern

Compaction is an overwrite commit; concurrent ingest commits are append-only commits. The two commit types can run concurrently; their conflict behavior at the CAS is asymmetric.

**Compaction loses conflicts more often than it wins.** An ingest commit that arrives after the compaction started its work has typically advanced the table's snapshot pointer by the time the compaction commits its CAS. The compaction's CAS fails; the compactor must rebase. The rebase requires re-evaluating which source files still exist (the ingest commit didn't touch them, so all source files are still present), then producing a new commit on top of the new base. The metadata work is cheap; the data write work (the consolidated output file) is *not* repeated — it's already written to the object store.

**Long-running compactions amplify the conflict rate.** A compaction that takes 5 minutes against a table with ingest every 30 seconds sees roughly 10 ingest commits arrive during its work; each one is a conflict at the CAS. The retry-loop handles this; the rebase is cheap; eventually the compaction wins. But the win is delayed by the conflict count, and the CAS retries consume catalog throughput unnecessarily.

The operational fix the Artemis archive uses: **batch compactions per partition with a soft serialization at the partition level**. The lifecycle worker runs one compaction job per partition at a time; many partitions can compact in parallel, but a single partition is never the target of two concurrent compactions. The ingest writer is single-writer per table anyway, so the only concurrency for compaction's CAS is the ingest writer for the same partition, and the conflict rate stays low.

A second pattern, more sophisticated: **window-based compaction that compacts only sealed time windows.** A partition like `day('2024-03-15')` is compacted only after the day has ended and ingest for that day has stopped. The compaction has no concurrent ingest to race; CAS conflicts are zero. For time-partitioned tables this works cleanly; for tables partitioned on dimensions where the window is harder to define (mission_id, payload_id), the per-partition serialization above is the fallback.

### Resource Budget and Pacing

A naive compaction runs full-speed. Full-speed compaction reads gigabytes from the object store, writes gigabytes back, and uses available CPU for sorting and Z-order computation. On a shared system this competes with the analyst query workload for the same resources. The result is observable query slowdowns, which the operations team will rightly view as a regression.

The discipline is **rate-limited compaction with explicit bandwidth budgets**. The Artemis lifecycle worker:

- Consumes at most 30% of available object-store read bandwidth (configured as IOPS and MB/s limits per worker).
- Runs at most 4 parallel compaction tasks across the cluster, each with bounded local memory.
- Pauses compaction during the daily analyst peak window (09:00-17:00 UTC for the SDA team's analytical traffic).
- Backs off automatically when query-latency metrics exceed thresholds (Module 5's observability picks this up).

The result is that compaction makes steady progress without ever degrading the analyst-visible system. The compaction rate is approximately matched to the file-creation rate; the table's small-file count stays bounded.

### Compacting Manifests

A separate compaction subject worth noting: **manifest compaction**. The Module 2 metadata model produces one manifest per commit. Over time the manifest list grows linearly with commit count; for a table with 2 million commits, the manifest list is 2 million entries. Reading the manifest list for query planning becomes a non-trivial cost — even though pruning Pass 1 (Module 3 Lesson 3) eliminates most manifests, the manifest list itself is large.

Manifest compaction rewrites many small manifests into fewer large manifests. It's structurally similar to data file compaction but operates on metadata: read the per-commit manifests, group them, write consolidated manifests with the same entries. The new snapshot references the consolidated manifests; the old per-commit manifests are deleted by orphan cleanup. The cost is proportional to manifest count, not data size — typically a few tens of MB of I/O for a billion-row table.

The Artemis archive runs manifest compaction weekly. The manifest list goes from ~100k entries at week-end to ~5k entries at week-start, and query-planning latency on cold cache drops from ~200ms to ~20ms. The compaction commit is a metadata-only overwrite — the data files are unchanged; only the manifest references move.

---

## Core Mechanics in Code

### A Bin-Packing Compactor

The minimal compaction implementation: identify small files in a partition, group them into bins, rewrite them.

```rust,ignore
use anyhow::Result;
use std::collections::HashMap;

const SMALL_FILE_THRESHOLD_BYTES: u64 = 32 * 1024 * 1024; // 32 MB
const TARGET_OUTPUT_BYTES: u64 = 128 * 1024 * 1024;       // 128 MB

pub struct CompactionPlan {
    pub partition: PartitionTuple,
    pub bins: Vec<Vec<DataFile>>,
}

/// Plan a bin-packing compaction for a single partition. The plan
/// is a vec of bins; each bin is a vec of source files whose total
/// size is near the target output size. Files larger than the small-
/// file threshold are not compacted.
pub fn plan_compaction(
    partition_files: Vec<DataFile>,
    partition: PartitionTuple,
) -> CompactionPlan {
    // 1. Filter to files below the small-file threshold.
    let mut small_files: Vec<DataFile> = partition_files
        .into_iter()
        .filter(|f| f.file_size_bytes < SMALL_FILE_THRESHOLD_BYTES)
        .collect();

    // 2. Sort by size descending. The greedy bin-packing then takes
    // the largest remaining file and adds smaller files until the
    // bin is near the target. This produces fewer "overstuffed" bins
    // than sort-ascending.
    small_files.sort_by(|a, b| b.file_size_bytes.cmp(&a.file_size_bytes));

    // 3. Greedy bin packing.
    let mut bins: Vec<Vec<DataFile>> = Vec::new();
    let mut current_bin: Vec<DataFile> = Vec::new();
    let mut current_size: u64 = 0;

    for file in small_files {
        if current_size + file.file_size_bytes > TARGET_OUTPUT_BYTES && !current_bin.is_empty() {
            bins.push(std::mem::take(&mut current_bin));
            current_size = 0;
        }
        current_size += file.file_size_bytes;
        current_bin.push(file);
    }
    if !current_bin.is_empty() {
        bins.push(current_bin);
    }

    // 4. Drop bins of size 1 — a single small file doesn't benefit
    // from compaction (the output would be one small file, same as
    // the input). Compaction only fires when there's something to
    // consolidate.
    bins.retain(|b| b.len() > 1);

    CompactionPlan { partition, bins }
}

/// Execute a single bin: read the source files, write a consolidated
/// output file, return the new DataFile metadata. The output file
/// inherits the partition tuple of the source files.
pub async fn compact_bin(
    bin: Vec<DataFile>,
    partition: &PartitionTuple,
    object_store: &dyn ObjectStore,
) -> Result<DataFile> {
    let output_path = generate_compacted_path(partition);
    let writer = ParquetWriter::new(&output_path).await?;

    for source in &bin {
        let reader = ParquetReader::open(&source.path, object_store).await?;
        for batch_result in reader.read_batches() {
            let batch = batch_result?;
            writer.write_batch(&batch).await?;
        }
    }

    writer.finalize().await?;
    let new_meta = read_file_metadata(&output_path, object_store).await?;
    Ok(new_meta)
}
```

The discipline. The bin-packing plan is the cheap, deterministic part. The execution reads the sources and writes the consolidated output — this is the expensive part, dominated by I/O. The output's `DataFile` metadata (the per-file statistics for Pass 2 pruning) is computed at write time by the Parquet writer, the same way it was in Module 1's initial writer; nothing about compaction-produced files is different from ingest-produced files at the read layer.

### Committing the Compaction

The CAS-protected swap that makes the compaction visible:

```rust,ignore
use anyhow::Result;

/// Commit a compaction: remove the source files from the table, add
/// the new compacted file, in one snapshot. Uses the optimistic-CAS
/// retry pattern from Module 2 Lesson 3.
pub async fn commit_compaction(
    table: &Table,
    bin: Vec<DataFile>,            // the source files (to remove)
    new_file: DataFile,            // the compacted output (to add)
) -> Result<SnapshotId> {
    let source_paths: Vec<String> = bin.iter().map(|f| f.path.clone()).collect();

    for attempt in 0..16 {
        let base = table.current_snapshot().await?;

        // Sanity check: every source file must still be referenced by
        // the current snapshot. If a concurrent commit removed any of
        // them, this compaction is no longer applicable and must abort.
        let current_paths = base.list_data_file_paths().await?;
        for src in &source_paths {
            if !current_paths.contains(src) {
                return Err(anyhow::anyhow!(
                    "source file {} no longer in snapshot; compaction obsolete",
                    src,
                ));
            }
        }

        // Build the overwrite commit: removed = source paths, added = new file.
        let new_snapshot = build_overwrite_snapshot(
            &base,
            &source_paths,
            vec![new_file.clone()],
            "compaction",
        ).await?;

        match table.catalog
            .compare_and_swap(
                &table.id,
                base.snapshot_id,
                new_snapshot.snapshot_id,
                &new_snapshot.metadata_path,
            )
            .await
        {
            Ok(()) => return Ok(new_snapshot.snapshot_id),
            Err(CommitError::Conflict(_)) => {
                // Rebase and retry. The new compacted file is unchanged
                // — it's already in the object store — so the next
                // attempt reuses it.
                tracing::warn!("compaction commit conflict; retrying attempt {}", attempt);
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }

    Err(anyhow::anyhow!("compaction commit failed after retries"))
}
```

What to notice. The retry loop is the same shape as Module 2 Lesson 3's ingest commit. The difference is the source-file sanity check before each attempt: if any source file has been removed by a concurrent commit (perhaps a previous compaction touched the same files), the current compaction is no longer applicable and must abort. The new file isn't deleted — it's left in the object store as an orphan, to be cleaned up by Lesson 3's orphan-cleanup job. This is the expected cost of optimistic concurrency in the overwrite case; the rebase cost is bounded, the abort case is rare.

### Sort-Based Compaction (Sketch)

The sort-based variant extends `compact_bin` by sorting the rows before writing:

```rust,ignore
use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::compute::{concat_batches, lexsort_to_indices, SortColumn};

/// Compact a bin with rows sorted by the given sort order. The merge
/// uses a streaming approach: read source files into memory, concat,
/// sort, write. For Artemis bin sizes (~128 MB) the in-memory approach
/// is sufficient; larger bins require an external merge sort.
pub async fn compact_bin_sorted(
    bin: Vec<DataFile>,
    partition: &PartitionTuple,
    sort_columns: &[String],
    object_store: &dyn ObjectStore,
) -> Result<DataFile> {
    // 1. Read all source files' batches into a single vec.
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let schema = read_table_schema(partition).await?;
    for source in &bin {
        let reader = ParquetReader::open(&source.path, object_store).await?;
        for batch_result in reader.read_batches() {
            all_batches.push(batch_result?);
        }
    }

    // 2. Concatenate into one batch for sorting.
    let combined = concat_batches(&schema, &all_batches)?;

    // 3. Compute the sort indices.
    let sort_keys: Vec<SortColumn> = sort_columns
        .iter()
        .map(|name| {
            let idx = schema.index_of(name).unwrap();
            SortColumn {
                values: combined.column(idx).clone(),
                options: None,
            }
        })
        .collect();
    let indices = lexsort_to_indices(&sort_keys, None)?;

    // 4. Gather rows in sort order.
    let sorted_columns: Vec<_> = combined
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c, &indices, None))
        .collect::<Result<_, _>>()?;
    let sorted_batch = RecordBatch::try_new(schema.clone(), sorted_columns)?;

    // 5. Write the sorted batch as the consolidated output.
    let output_path = generate_compacted_path(partition);
    let mut writer = ParquetWriter::new(&output_path).await?;
    writer.write_batch(&sorted_batch).await?;
    writer.finalize().await?;

    Ok(read_file_metadata(&output_path, object_store).await?)
}
```

The trade against bin-packing: this version requires holding the entire bin in memory at once (~128 MB plus overhead). For Artemis-typical bin sizes that's fine; for larger bins, replace step 2 with a streaming k-way merge over already-sorted source files. The Z-order variant differs only in how the sort keys are computed — replace step 3's `lexsort_to_indices` with a Z-order-key computation followed by an integer sort.

---

## Key Takeaways

- **The small-file problem is structural**: ingest commits at a cadence faster than the target file size justifies produce small files; over time the file count grows linearly with commit count and per-file overhead dominates query latency.
- **Compaction is an overwrite commit** that uses the Module 2 CAS protocol. It is safe under concurrent reads (snapshots are immutable; readers pinned to the pre-compaction snapshot see the old files) and safe under concurrent writes (the CAS protocol orders them).
- **Three compaction strategies** with different tradeoffs: bin-packing (cheap, content-agnostic; doesn't improve clustering), sort-based (adds a sort; improves clustering on a single linear order), Z-order (adds Z-order key computation plus sort; improves multidimensional clustering at modest extra cost).
- **Resource pacing matters.** Naive compaction degrades the live workload. Production compactors run with explicit bandwidth budgets, parallelism caps, and quiet-window scheduling.
- **Manifest compaction** is the structural analog at the metadata layer. Both data file and manifest compaction follow the same overwrite-commit pattern; manifest compaction is cheaper because the metadata is much smaller than the data.

{{#quiz quiz-01.toml}}
