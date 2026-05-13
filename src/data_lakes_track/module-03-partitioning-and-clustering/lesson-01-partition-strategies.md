# Lesson 1 — Partition Strategies and the Small-File Problem

**Module:** Data Lakes — M03: Partitioning and Clustering
**Position:** Lesson 1 of 3
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann and Chris Riccomini, Chapter 6 ("Partitioning"). Apache Iceberg specification, "Partitioning" and "Partition Transforms" sections.

---

## Context

The Module 2 table format gave us a metadata-tracked file set with three pruning passes available. Pruning depends entirely on per-file statistics being tight along the query-relevant dimensions. If a file contains rows from every mission ever flown, the per-file `min(mission_id)` and `max(mission_id)` statistics span every mission, and a query for one specific mission cannot prune the file. Partitioning is the discipline that arranges rows into files so the statistics are useful.

The Artemis analyst workload tells us what the partition columns should be. Every query mentions a mission. Most queries mention a time window. Many queries mention a sensor or payload. A partition strategy that aligns the file layout with these dimensions makes the queries cheap. A strategy that doesn't is worse than no strategy at all — because partitioning has a real cost (more files, more metadata, more directory operations), and a wrongly-chosen partition strategy pays the cost without delivering the benefit.

This lesson develops partitioning bottom-up. The mechanics of what "partitioning" means at the table-format level. The two question types every partition decision answers: which columns to partition on, and what transform to apply to handle high-cardinality columns. The small-file failure mode that over-partitioning produces and how Iceberg-style hidden partitioning avoids it. By the end of the lesson the engineer can defend a partition-spec choice against a real workload; the capstone designs the spec for the SDA observation table.

---

## Core Concepts

### What Partitioning Is, Mechanically

In an Iceberg-shaped table format, partitioning is metadata, not directory structure. The table's **partition spec** is a list of partition fields, each one a `(source_column, transform, partition_name)` triple. When a new data file is committed, the writer computes the partition tuple for the file — typically by computing the transform on the min and max values of the source column and requiring them to be equal (the file is "in" exactly one partition value). The partition tuple is recorded in the data file's manifest entry. Read planning prunes by computing the partition tuple's overlap with the query predicate.

Two important consequences. First, **partitioning does not require physical directory structure.** The table format records the partition tuple in metadata; the data files can live anywhere in the object store. Hive's directory-based partitioning (`/year=2024/month=03/file.parquet`) is a convention, not a requirement; Iceberg supports it but doesn't require it. The Artemis archive uses content-addressed paths (`<table>/data/<hash>.parquet`) and recovers the partition value entirely from metadata.

Second, **partitioning affects write-side file boundaries.** The writer cannot put rows belonging to different partition tuples in the same file, because a file has exactly one partition tuple. A writer ingesting a record batch containing rows from twelve missions must produce at least twelve files — one per mission partition. The Module 1 writer's row-group-size discipline still applies *within* a partition; multiple files in the same partition are produced when the partition's row count exceeds the row group size target. The partition spec sets a *minimum* file count per ingest; the row group size sets the *maximum* row count per file.

DDIA (Ch. 6) draws the same distinction between partitioning's logical purpose (limiting how much data each query touches) and its physical realization (which can vary across storage layers). In the lakehouse, the realization is the file-level partition tuple recorded in the manifest.

### Identity vs Transformed Partitioning

The simplest partition transform is **identity**: partition by the column's value directly. `partition by mission_id` produces one partition per distinct mission ID. For a low-cardinality column like `mission_id` (~40 distinct values across the archive's history), this is fine. For high-cardinality columns it is not.

Consider partitioning by `sample_timestamp_ns`. The column's cardinality is enormous — every sample at every timestamp is a distinct value. Identity partitioning on `sample_timestamp_ns` would produce one partition per row, which produces one tiny file per row, which is catastrophic. The small-file problem at scale: a year of data at 100 Hz becomes three billion partitions, and the metadata cost is more than the data cost.

The fix is **transformed partitioning**: partition by some function of the column rather than the column itself. The standard Iceberg transforms:

- `year(ts)`, `month(ts)`, `day(ts)`, `hour(ts)` — extract a time grain from a timestamp. `partition by day(sample_timestamp_ns)` produces one partition per day, regardless of how many samples that day contains.
- `bucket(N, col)` — hash the column value into one of N buckets. `partition by bucket(16, payload_id)` produces 16 partitions regardless of how many distinct payload IDs exist. Used for high-cardinality columns where the workload's queries are exact-match against the column.
- `truncate(width, col)` — for strings, take the first `width` characters; for numbers, round to a multiple of `width`. `partition by truncate(8, mission_id)` partitions by the first eight characters of the mission ID, collapsing similar IDs into the same partition.

The transform is what makes the partition tractable. The right transform produces a partition count in the tens-to-hundreds-of-thousands range — enough granularity for pruning, few enough partitions that the metadata overhead is acceptable.

### Hidden Partitioning: The Iceberg Innovation

The Hive-era partition pattern required the query to explicitly mention the partition column. A table partitioned by `day(ts)` needed queries that filtered on the partition column directly:

```sql
-- Hive-style: works
WHERE ts >= '2024-03-01' AND ts < '2024-03-02' AND day = '2024-03-01'
-- Hive-style: does NOT prune; full scan
WHERE ts >= '2024-03-01' AND ts < '2024-03-02'
```

The second query is logically equivalent to the first and produces the same rows, but Hive's planner could not derive the partition predicate from the timestamp predicate. Analysts had to know about the partition layout and write queries that explicitly referenced it. Schema changes (changing the partition transform) required all queries to be rewritten.

Iceberg's **hidden partitioning** moves the transform from "user-visible partition column" to "partition-spec metadata." The query writes the natural predicate on the source column; the planner derives the partition predicate from the partition spec. Both queries above prune identically against an Iceberg table partitioned by `day(ts)`. The analyst doesn't need to know the partition strategy; the planner derives it.

Hidden partitioning is what makes partition-strategy changes safe over time. An Iceberg table partitioned by `day(ts)` can switch to `month(ts)` without rewriting queries (the Module 4 partition-evolution mechanic). The query layer is decoupled from the storage layout. This is impossible in Hive's directory-based scheme.

### The Small-File Problem

The naive partition strategy is "partition by everything that ever appears in a query predicate." This produces the **small-file problem**: a partition spec with `(mission_id, day(ts), payload_id, sensor_kind)` against an archive with 40 missions × 1000 days × 8 payloads × 12 sensors = 3.84M partitions. Each partition contains, on average, a vanishing fraction of the table's rows. The vast majority of partitions hold one file each, and most of those files are tiny — kilobytes, not megabytes. The metadata overhead overwhelms the data.

The downstream costs of small files compound. **Read planning** must enumerate every relevant partition; the manifest list grows with partition count. **Object store listings** during maintenance are slow and expensive when there are millions of small objects. **Query planning** sees many candidate files even after pruning; the per-file overhead (footer parse, statistics check) dominates the actual data read. **Compaction** (Module 6) becomes a continuous background workload to consolidate tiny files into larger ones.

DDIA (Ch. 6) calls out the same tradeoff in terms of partition count: too few partitions limits parallelism and pruning; too many partitions wastes coordination overhead and produces hot spots. The right partition count depends on the workload — the rule of thumb that has held up in practice is **target file size 64-512 MB**, **target partition size 1-100 GB**, **target partition count in the tens of thousands or fewer**. Outside these ranges, the operator should expect to fight maintenance overhead constantly.

The Artemis SDA observation table sits well inside the ranges with `partition by (mission_id, day(sample_timestamp_ns))` — 40 × 1000 ≈ 40,000 partitions, average partition size of a few hundred MB, average file size of around 128 MB.

### Choosing Partition Columns: The Workload Determines the Spec

The partition spec is determined by the workload. The decision procedure for the Artemis SDA observation table goes like this.

First, **enumerate the typical query predicates**. Sample the actual query log (or the query patterns described in the requirements). For the Artemis archive, the workload distribution is approximately:

- 100% of queries filter on `mission_id`
- 95% of queries filter on a time window
- 30% of queries filter on `payload_id`
- 15% of queries filter on `sensor_kind`

Second, **estimate the cardinality and selectivity of each candidate**. A predicate that prunes 99% of partitions is highly selective; one that prunes 50% is not. `mission_id` is highly selective (filter to 1 of 40 missions → 97.5% pruning). `day(ts)` for typical queries (one week to one month windows) is highly selective (7-30 days of 1000 → 97-99% pruning). `payload_id` is less selective (1 of 8) but only 30% of queries use it. `sensor_kind` is less selective still (1 of 12) and used by 15% of queries.

Third, **pick the columns with high coverage × high selectivity**. `mission_id` is in every query and is highly selective → include. `day(ts)` is in 95% of queries and is highly selective → include. `payload_id` and `sensor_kind` are less universal; including them would increase partition count by ×100 for relatively little additional pruning. Module 3 Lesson 2 develops clustering as the answer for these second-tier dimensions: arrange the data within each partition so that pruning on `payload_id` and `sensor_kind` still works at the file level, without partitioning by them.

The final spec: `partition by (identity(mission_id), day(sample_timestamp_ns))`. 40 missions × ~1000 days ≈ 40,000 partitions. Average partition size of ~500 MB. Average file size of ~128 MB. Manifest count per snapshot in the low thousands. Inside every operational threshold.

---

## Code Examples

### Defining and Applying a Partition Spec

The partition spec is part of the table's snapshot metadata. Extending the Module 2 types:

```rust,ignore
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSpec {
    pub spec_id: u32,
    pub fields: Vec<PartitionField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionField {
    /// Name of the source column in the table schema.
    pub source_column: String,
    /// Name the partition value gets in metadata (and any directory
    /// layout if used).
    pub partition_name: String,
    /// Transform applied to the source column to derive the partition.
    pub transform: Transform,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Transform {
    /// Use the source value directly. Suitable for low-cardinality
    /// columns: mission_id, sensor_kind, payload_id.
    Identity,
    /// Extract the year as YYYY from a timestamp. Coarsest time grain.
    Year,
    /// Extract YYYY-MM from a timestamp.
    Month,
    /// Extract YYYY-MM-DD from a timestamp. Most common for daily data.
    Day,
    /// Extract YYYY-MM-DD-HH from a timestamp. For very high-volume tables.
    Hour,
    /// Hash the source value into one of N buckets. For high-cardinality
    /// columns with exact-match query patterns.
    Bucket(u32),
    /// Truncate strings to the first N characters / round numbers down to
    /// a multiple of N. For string columns with hierarchical structure.
    Truncate(u32),
}
```

The application — compute a partition tuple for a value — has one variant per transform:

```rust,ignore
use std::hash::{Hash, Hasher};

/// Compute the partition value for a single source value under a given
/// transform. The result is the value that gets recorded in the data
/// file's manifest entry and that pruning compares predicates against.
pub fn apply_transform(transform: &Transform, value: &Value) -> PartitionValue {
    match transform {
        Transform::Identity => PartitionValue::from_value(value),
        Transform::Day => match value {
            Value::TimestampNs(ns) => {
                let secs = ns / 1_000_000_000;
                let days = secs / 86_400;
                PartitionValue::Date(days as i32)
            }
            _ => panic!("Day transform requires Timestamp source"),
        },
        Transform::Hour => match value {
            Value::TimestampNs(ns) => {
                let secs = ns / 1_000_000_000;
                let hours = secs / 3_600;
                PartitionValue::Hour(hours as i64)
            }
            _ => panic!("Hour transform requires Timestamp source"),
        },
        Transform::Bucket(n) => {
            // Iceberg specifies a particular hash function (Murmur3)
            // to keep partition values stable across implementations.
            // The production version uses fastmurmur3; this sketch uses
            // the default hasher for illustration.
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            value.hash(&mut hasher);
            let bucket = (hasher.finish() % (*n as u64)) as u32;
            PartitionValue::Bucket(bucket)
        }
        Transform::Truncate(width) => match value {
            Value::String(s) => {
                PartitionValue::String(s.chars().take(*width as usize).collect())
            }
            Value::Int64(n) => {
                let w = *width as i64;
                PartitionValue::Int64((n / w) * w)
            }
            _ => panic!("Truncate requires String or Int64 source"),
        },
        // Year and Month omitted for brevity; same shape as Day.
        _ => todo!(),
    }
}
```

The discipline: the transform is a *pure function* of the source value. Two writers computing the partition value for the same row produce the same result. The partition values are stable across implementations as long as the hash function and the time-grain boundaries match the spec.

### The Hidden-Partitioning Derivation

Read planning's job for a hidden partition: turn a predicate on the *source column* into a predicate on the *partition value*. For each partition transform, this is a small piece of arithmetic.

```rust,ignore
use std::ops::RangeInclusive;

#[derive(Debug, Clone)]
pub enum SourcePredicate {
    Equals(Value),
    Range(RangeInclusive<Value>),
    In(Vec<Value>),
}

/// Translate a predicate on a source column into a predicate on the
/// partition value for a given transform. The returned predicate is a
/// safe over-approximation: it may match more partitions than strictly
/// necessary, but never fewer. False negatives would be silent data
/// loss; false positives just mean reading extra files.
pub fn lift_predicate(
    transform: &Transform,
    source_pred: &SourcePredicate,
) -> PartitionPredicate {
    match (transform, source_pred) {
        (Transform::Identity, SourcePredicate::Equals(v)) => {
            PartitionPredicate::Equals(PartitionValue::from_value(v))
        }
        (Transform::Day, SourcePredicate::Range(r)) => {
            // sample_timestamp_ns range -> day range
            let lo_day = ts_to_day(r.start());
            let hi_day = ts_to_day(r.end());
            PartitionPredicate::Range(lo_day..=hi_day)
        }
        (Transform::Bucket(_), SourcePredicate::Equals(v)) => {
            // Equality on the source becomes equality on the bucket.
            let bucket = apply_transform(transform, v);
            PartitionPredicate::Equals(bucket)
        }
        (Transform::Bucket(_), SourcePredicate::Range(_)) => {
            // Range queries cannot be lifted through bucket hashing;
            // the predicate could match any bucket. The planner falls
            // back to scanning all partitions; bucket-only partitioning
            // is wrong for range-queried columns.
            PartitionPredicate::AllPartitions
        }
        _ => PartitionPredicate::AllPartitions,
    }
}

fn ts_to_day(ts: &Value) -> PartitionValue {
    match ts {
        Value::TimestampNs(ns) => PartitionValue::Date((ns / 1_000_000_000 / 86_400) as i32),
        _ => panic!("ts_to_day requires Timestamp"),
    }
}
```

What to notice. The lifting is **conservative** — when the transform's algebra doesn't admit a tight derivation, the planner produces `AllPartitions` (no pruning). False negatives would silently drop data from query results; false positives just read extra files. The `Bucket` + range case is the textbook example: hashing destroys the order property that range queries need; bucket partitioning is only valid for equality predicates. A capstone test must exercise this case and verify the planner doesn't try to prune.

### The Per-Partition File-Boundary Discipline

The writer's contract: each data file has rows from exactly one partition tuple. Implementing this requires partitioning the incoming `RecordBatch` by the partition spec before handing slices to the Module 1 Parquet writer.

```rust,ignore
use std::collections::HashMap;
use arrow::array::RecordBatch;

/// Partition a record batch by the table's partition spec, returning
/// one sub-batch per partition tuple. Each sub-batch can then be
/// written to a separate Parquet file (or appended to the partition's
/// in-progress file).
pub fn split_by_partition(
    batch: &RecordBatch,
    spec: &PartitionSpec,
) -> Vec<(PartitionTuple, RecordBatch)> {
    // 1. For each row, compute its partition tuple by applying every
    // transform in the spec to the row's source column values.
    let row_partitions: Vec<PartitionTuple> = (0..batch.num_rows())
        .map(|row| compute_partition_tuple(batch, row, spec))
        .collect();

    // 2. Group row indices by partition tuple.
    let mut groups: HashMap<PartitionTuple, Vec<usize>> = HashMap::new();
    for (row, pt) in row_partitions.iter().enumerate() {
        groups.entry(pt.clone()).or_default().push(row);
    }

    // 3. Produce a sub-batch per partition by gathering the rows.
    // Arrow's `compute::take` does the per-column index gather in a
    // single allocation per column.
    groups
        .into_iter()
        .map(|(pt, rows)| {
            let indices = arrow::array::UInt32Array::from(
                rows.iter().map(|&r| r as u32).collect::<Vec<u32>>()
            );
            let sub_columns: Vec<_> = batch
                .columns()
                .iter()
                .map(|c| arrow::compute::take(c, &indices, None).unwrap())
                .collect();
            let sub_batch = RecordBatch::try_new(batch.schema(), sub_columns).unwrap();
            (pt, sub_batch)
        })
        .collect()
}
```

The cost of partition splitting is one `take` per column per partition — O(rows × columns × partitions). For a record batch of 8192 rows × 40 columns × an average of 3-5 distinct partition tuples per batch, this is fast (microseconds). The constant-factor cost is real but tractable. Production code that produces many partition tuples per batch — for instance, a backfill that spans many days — benefits from batching the incoming data into per-partition buffers and flushing partition-by-partition; the M01 writer already supports this via per-partition `ArrayBuilder` state.

---

## Key Takeaways

- **Partitioning is metadata**, not necessarily directory structure. The partition spec is part of the table's snapshot; the partition value is recorded per data file in the manifest entry. Read planning compares predicates against the per-file partition value to prune.
- **Identity partitioning works for low-cardinality columns** (`mission_id`, `sensor_kind`). **Transformed partitioning** (`day(ts)`, `bucket(16, payload_id)`, `truncate(8, name)`) makes high-cardinality columns tractable by collapsing many values into fewer partitions.
- **Hidden partitioning** is the Iceberg-vs-Hive distinction worth knowing: the query writes the natural predicate on the source column; the planner derives the partition predicate from the spec. Queries are decoupled from the storage layout; partition strategy changes are safe.
- **The small-file problem** is the dominant failure mode of over-partitioning. Operational thresholds: target file size 64–512 MB, target partition size 1–100 GB, target partition count in the tens of thousands. Outside these, expect to fight maintenance overhead.
- **The partition spec is determined by the workload.** Pick columns with high query coverage × high selectivity. The Artemis SDA spec is `partition by (mission_id, day(sample_timestamp_ns))` — two columns, ~40,000 partitions, ~500 MB per partition. Secondary dimensions (`payload_id`, `sensor_kind`) get handled by clustering, not partitioning.

{{#quiz quiz-01.toml}}
