# Lesson 3 — Partition Pruning at Query Time

**Module:** Data Lakes — M03: Partitioning and Clustering
**Position:** Lesson 3 of 3
**Source:** Apache Iceberg specification, "Scan Planning" section. *Database Internals* — Alex Petrov, Chapter 9 ("Query Processing"), for the predicate-as-tree machinery. Earlier modules (M01 row group statistics, M02 manifest hierarchy) for the substrate.

> **Source note:** The pruning protocol's structural pieces are spec-driven (Iceberg scan planning, Parquet metadata) and well-established. The Artemis-specific tuning details are synthesis-mode against the workload patterns described in Lesson 1.

---

## Context

Lessons 1 and 2 designed the partition spec and the cluster layout. This lesson is the read-side counterpart: given a query, how does the planner turn the query's predicate into a minimal set of files to scan? The answer is **three sequential pruning passes**, each one consuming the output of the previous one and applying tighter statistics. Pruning power compounds across the passes; getting any one of them wrong loses an order of magnitude of work.

The three passes are: at the **manifest list** level, prune manifests whose partition summaries don't overlap the predicate; at the **manifest entry** level, prune data files whose per-file statistics don't overlap the predicate; at the **row group** level (inside the data files), prune row groups whose per-column-chunk statistics don't overlap the predicate. The first two passes are the table format's responsibility (Module 2's metadata); the third is the Parquet reader's responsibility (Module 1's footer). All three use the same logical operation — check whether the predicate's value range intersects the chunk's value range — at different granularities.

This lesson develops the protocol end to end. The predicate-tree shape that the planner uses internally and how it composes with the statistics. The lifting from source-column predicates to partition-value predicates (Lesson 1's hidden partitioning, applied at read time). The conservative-overshoot discipline that makes pruning correctness tractable. By the end the engineer can predict how many files a query will scan and explain why; the capstone's pruning-effectiveness measurements depend on exactly this skill.

---

## Core Concepts

### The Predicate Tree and Statistics Algebra

A query predicate is a tree of conjunctions, disjunctions, and leaves. Each leaf is a single-column comparison (`mission_id = 'apollo-7'`, `panel_voltage > 28.5`, `sample_timestamp_ns BETWEEN A AND B`). The planner walks the tree and produces, for each node, a function `(statistics) → maybe_matches` that takes a statistics record and returns `true` if rows matching the predicate *could* exist in the data the statistics describe.

The crucial property of `maybe_matches` is that it is **conservative**: it must return `true` whenever rows could match. False positives are acceptable — the planner reads a file that happens not to contain matching rows. False negatives are silent data loss — the planner skips a file that does contain matching rows, and the query returns wrong results. Every pruning function preserves this asymmetry.

The leaf rules are direct. For a predicate `column op value` against statistics `(min, max, null_count)`:

- `column = value`: `min <= value <= max`
- `column < value`: `min < value`
- `column > value`: `max > value`
- `column IN (vlist)`: `(min, max)` overlaps `[min(vlist), max(vlist)]`
- `column IS NULL`: `null_count > 0`
- `column IS NOT NULL`: `null_count < row_count`

The composition rules follow basic predicate logic. `A AND B` matches if both `A` matches and `B` matches; `A OR B` matches if either does. The planner produces one boolean per file by walking the tree against the file's statistics record; files where the root produces `false` are pruned.

This is the same statistics algebra DDIA (Ch. 4) describes for column-store predicate pushdown, generalized to the table-format hierarchy. The mechanics don't change between Parquet row-group pruning and Iceberg manifest pruning — only the granularity of the statistics does.

### Pass 1: Manifest List Pruning

The first pruning pass operates on the manifest list. The manifest list holds one entry per manifest, summarizing the manifest's contents: which partitions it spans, the count of data files, the per-partition-field summary statistics (lower bound, upper bound, contains-null flag).

For a query against `mission_id = 'apollo-7' AND sample_timestamp_ns >= '2024-03-01' AND sample_timestamp_ns < '2024-03-08'`, the manifest list pruning derives partition predicates for each partition field in the spec:

- `mission_id` partition (identity transform): `partition.mission_id = 'apollo-7'`
- `day(sample_timestamp_ns)` partition (day transform): `partition.day BETWEEN '2024-03-01' AND '2024-03-07'`

The planner then checks each manifest's partition summary against these predicates. A manifest with `mission_id` summary `(min='apollo-3', max='apollo-3')` and `day` summary `(min='2024-01-01', max='2024-01-31')` is pruned by both predicates — neither overlaps. A manifest with `mission_id` summary `(min='apollo-7', max='apollo-7')` and `day` summary `(min='2024-03-01', max='2024-03-31')` matches the first predicate exactly and overlaps the second; it survives the pass.

For the Artemis archive with 40,000 partitions distributed across roughly 4,000 manifests (one manifest per commit, with hundreds of commits per year per mission), the typical week-long-window query prunes to ~10 manifests out of ~4,000. The pass takes one manifest-list read (a few hundred KB, one S3 GET) and produces a 99.75% pruning ratio.

### Pass 2: Manifest Entry Pruning

The second pruning pass opens each surviving manifest and applies file-level statistics. The manifest entries hold one record per data file with the column-level lower bounds, upper bounds, and null counts. The planner applies the *full* predicate tree (not just partition predicates) against each file's column statistics.

For the example query, the planner now checks each file against:

- The partition predicates (same as Pass 1, redundant at this level but cheap).
- Any non-partition predicates — for instance, if the query also has `WHERE panel_voltage > 28.5`, the planner uses the file's `panel_voltage` upper-bound statistic. A file with `panel_voltage` `(min=24.1, max=27.3)` is pruned because `max < 28.5`.
- Any IN-list and BETWEEN predicates, applied with the intersection algebra above.

This is where **clustering pays off**. A clustered table has files whose `payload_id` and `sensor_kind` statistics are tight; a query for one payload reads 1-2 files out of 30 inside the matching partition. An unclustered table has files where every file's `payload_id` statistic spans every payload; the same query reads all 30 files. The 10×-15× pruning improvement from Lesson 2's Z-order clustering shows up entirely at this pass.

The cost of Pass 2 is one manifest read per surviving manifest. For 10 surviving manifests of 1 MB each, this is 10 MB of metadata I/O — order tens of milliseconds against object storage. The output is typically tens to hundreds of data files to scan.

### Pass 3: Row Group Pruning

The third pruning pass happens inside the data files, at the Parquet row group level. Module 1 introduced the Parquet footer's per-column-chunk statistics; Pass 3 applies the same statistics algebra at row-group granularity.

For a Parquet file with 8 row groups of 128 MB each, the planner reads the footer, evaluates the predicate against each row group's column statistics, and emits the byte ranges of the column chunks that need to be read. A file where 2 out of 8 row groups survive Pass 3 has 75% of its bytes pruned without being read. The Parquet reader's `ProjectionMask` (Lesson 1, Module 1) combines with the row group pruning to produce a minimal-bytes read plan.

Pass 3 is the Parquet reader's responsibility, not the table format's. The table format hands the Parquet reader the list of files; the reader opens each file's footer, applies row-group pruning, and emits Arrow batches for the surviving row groups. The pruning is invisible at the table-format API boundary — the planner sees a "scan the file" operation; the file's actual I/O depends on the row-group pruning the reader does internally.

The three passes together produce the pruning pyramid: 4,000 manifests → 10 manifests → ~50 files → ~80 row groups → reads of column chunks for selected columns. The product is what makes the query fast.

### Lifting Source Predicates Through Partition Transforms

Lesson 1 introduced hidden partitioning: the query writes the predicate on the source column, the planner derives the partition predicate from the partition spec. Pass 1 needs to do this derivation; the lifting must be **conservative** — return a partition predicate that matches at least every partition where the source predicate could match.

The leaf cases:

- **Identity transform.** Source `column = v` lifts to `partition = v` exactly. Source `column IN [a, b, c]` lifts to `partition IN [a, b, c]`. Source `column > v` lifts to `partition > v`. The identity transform preserves order and equality.
- **Day, month, year transforms.** Source `ts >= '2024-03-01' AND ts < '2024-03-08'` lifts to `day_partition BETWEEN '2024-03-01' AND '2024-03-07'`. Range predicates on the source translate to range predicates on the partition because day-extraction preserves order.
- **Bucket transform.** Source `column = v` lifts to `partition = bucket(v)` exactly. Source `column IN [a, b, c]` lifts to `partition IN [bucket(a), bucket(b), bucket(c)]`. **Source `column > v` cannot be lifted** — bucket hashing destroys order. The planner falls back to "all partitions might match" for range predicates on bucket-transformed columns. This is the structural reason bucket partitioning is correct only when the column is queried with equality predicates.
- **Truncate transform.** Source `column = v` lifts to `partition = truncate(v)`. Source `column > v` lifts to `partition >= truncate(v)`, because truncation rounds down. Range predicates lift conservatively (some extra partitions match) but soundly.

The Iceberg spec records the transform per partition field. The planner uses the transform to pick the right lifting rule; the rule lives in the planner code, not in the table format itself. A new transform requires a new lifting rule; the table format's flexibility is bounded by the lifting rules the planner implements.

### When Pruning Fails: Sparse and Non-Selective Predicates

Pruning works when the predicate is **selective** relative to the partition layout — when the file/manifest's value range is small relative to the predicate's value range, the pruning function returns `true` (no prune); when the value range is large relative to the predicate, the pruning function more often returns `false` (prune happens). The failure modes are predicates where the algebra doesn't help:

- **Predicates on unsupported transforms.** A range predicate on a bucket-partitioned column is the canonical example: bucket hashing destroys order, no lifting is possible, every partition is "potentially matching." The fix is changing the partition strategy, not the query.
- **Predicates on columns without statistics.** Module 1's writer enables statistics on numeric columns by default but disables them on very wide string columns to save metadata space. A predicate on a non-statistics-bearing column gets no pruning at the file or row-group level. The fix is enabling statistics for the column (paying the metadata cost) or accepting the full scan.
- **Predicates whose value range exceeds the partition value range.** A query `WHERE mission_id != 'apollo-3'` (where `apollo-3` is one of 40 missions) prunes 1/40th — almost nothing. Inequality with a single excluded value is structurally hard to prune. The fix is rephrasing the query in terms of an IN-list (`WHERE mission_id IN [apollo-1, apollo-2, ..., apollo-40] EXCEPT apollo-3`), which is awkward, or accepting the broader scan.
- **OR predicates that span many partition values.** A query `WHERE mission_id = 'apollo-3' OR panel_voltage > 30` cannot prune any file unless *both* sides prune that file. The pruning power is bounded by the less selective branch. The fix is restructuring the query as a UNION of two more selective queries, if the application allows.

Operationally, the planner emits a **pruning summary** for each query: how many manifests, files, and row groups survived each pass. The Artemis observability stack ingests these summaries; queries with low pruning ratios (high file counts relative to predicate selectivity) are surfaced for analyst review. A "this query reads 1.4 TB" alert is usually a sign that the query missed an opportunity to use a partition column — the analyst rephrases and the next run reads 14 GB.

---

## Code Examples

### A Statistics-Check Function for a Numeric Predicate

The atomic check at every pruning level: does a `(min, max, null_count)` triple admit a predicate of the form `column op value`?

```rust,ignore
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Statistics<T: PartialOrd + Clone> {
    pub min: Option<T>,
    pub max: Option<T>,
    pub null_count: u64,
    pub row_count: u64,
}

#[derive(Debug, Clone)]
pub enum LeafOp<T> {
    Eq(T),
    Ne(T),
    Lt(T),
    Le(T),
    Gt(T),
    Ge(T),
    In(Vec<T>),
    IsNull,
    IsNotNull,
}

/// Return true if the statistics admit *any* row matching the predicate.
/// Returns false only when the predicate is proven not to match — i.e.,
/// when pruning is safe. Conservative on uncertainty: missing or
/// ambiguous statistics return true (no prune).
pub fn might_match<T: PartialOrd + Clone>(
    stats: &Statistics<T>,
    op: &LeafOp<T>,
) -> bool {
    let (min, max) = match (&stats.min, &stats.max) {
        (Some(a), Some(b)) => (a, b),
        _ => return true, // no stats → can't prune
    };

    let has_non_null = stats.null_count < stats.row_count;

    match op {
        LeafOp::Eq(v) => has_non_null && min <= v && v <= max,
        LeafOp::Ne(v) => {
            // Pruned only if every row equals v; safe to claim when
            // null_count == 0 and min == max == v. Conservative
            // otherwise.
            !(stats.null_count == 0 && min == v && max == v)
        }
        LeafOp::Lt(v) => has_non_null && min < v,
        LeafOp::Le(v) => has_non_null && min <= v,
        LeafOp::Gt(v) => has_non_null && max > v,
        LeafOp::Ge(v) => has_non_null && max >= v,
        LeafOp::In(values) => {
            has_non_null && values.iter().any(|v| min <= v && v <= max)
        }
        LeafOp::IsNull => stats.null_count > 0,
        LeafOp::IsNotNull => has_non_null,
    }
}
```

The pattern. Each `LeafOp` has a conservative answer derived from the bounds. Predicates that the bounds rule out return `false` (prune); everything else returns `true` (read). Note the special handling of `Ne`: an inequality is only prunable when the bounds prove every row equals the excluded value, which essentially never happens — `Ne` predicates are structurally hard to prune, as discussed above.

### Lifting a Source Predicate to a Partition Predicate

The transform-aware lifting that Pass 1 uses. The function turns a predicate on the source column into a predicate on the partition value.

```rust,ignore
use anyhow::Result;

/// Lift a source-column predicate through a partition transform.
/// Returns Some(partition_predicate) if the transform admits a tight
/// lift, or None if the planner must treat all partitions as potentially
/// matching. The "None" return represents the bucket-with-range case
/// and any other unsupported combination.
pub fn lift(
    transform: &Transform,
    source_op: &LeafOp<Value>,
) -> Option<LeafOp<PartitionValue>> {
    match (transform, source_op) {
        // Identity preserves everything.
        (Transform::Identity, op) => Some(op.map(PartitionValue::from_value)),

        // Day transform on a timestamp: order-preserving.
        (Transform::Day, LeafOp::Eq(Value::TimestampNs(ns))) => {
            Some(LeafOp::Eq(PartitionValue::Date(ns_to_day(*ns))))
        }
        (Transform::Day, LeafOp::Ge(Value::TimestampNs(ns))) => {
            Some(LeafOp::Ge(PartitionValue::Date(ns_to_day(*ns))))
        }
        (Transform::Day, LeafOp::Lt(Value::TimestampNs(ns))) => {
            // For Lt, the day of the boundary is included if any
            // nanoseconds of that day are below the boundary. Conservative
            // lift: Lt becomes Le on the day.
            Some(LeafOp::Le(PartitionValue::Date(ns_to_day(*ns))))
        }
        // (Other Day cases: Gt → Ge, Le → Le, etc., omitted for brevity.)

        // Bucket transform: only Eq and In lift; range predicates cannot.
        (Transform::Bucket(_), LeafOp::Eq(v)) => {
            Some(LeafOp::Eq(apply_transform(transform, v)))
        }
        (Transform::Bucket(_), LeafOp::In(values)) => {
            Some(LeafOp::In(values.iter().map(|v| apply_transform(transform, v)).collect()))
        }
        // Bucket + Lt/Gt/Le/Ge/Ne: no lifting possible.
        (Transform::Bucket(_), _) => None,

        // Truncate: range predicates lift conservatively.
        (Transform::Truncate(_), LeafOp::Eq(v)) => {
            Some(LeafOp::Eq(apply_transform(transform, v)))
        }
        (Transform::Truncate(_), LeafOp::Ge(v)) => {
            Some(LeafOp::Ge(apply_transform(transform, v)))
        }
        (Transform::Truncate(_), LeafOp::Lt(v)) => {
            // Truncation rounds down; the truncated value is <= the
            // source. A source < v could come from a partition with
            // truncated value < truncate(v) OR == truncate(v).
            Some(LeafOp::Le(apply_transform(transform, v)))
        }
        _ => None,
    }
}

fn ns_to_day(ns: i64) -> i32 {
    (ns / 1_000_000_000 / 86_400) as i32
}
```

The discipline this implements. Each `(transform, leaf_op)` pair has either a tight lift, a conservative lift, or no lift. The conservative lift is sometimes slightly loose (the example's `Day` + `Lt` becomes `Le` on the day — one extra day's worth of partitions might be read) but never produces false negatives. The "no lift" case (bucket + range) returns `None`, which the planner translates to "all partitions are potentially matching" for this leaf, defeating pruning on this dimension. A real planner combines partial lifts: a query with `mission_id = 'apollo-7'` (lifts) AND `panel_voltage > 28.5` (does not lift if `panel_voltage` is bucket-partitioned, but most likely is not partitioned at all) still gets the partition-level pruning from `mission_id`.

### A Complete Read Plan with All Three Passes

The end-to-end planner: query in, byte-ranges out. The function uses the Module 2 metadata types and the Module 1 Parquet reader.

```rust,ignore
use anyhow::Result;

pub struct ScanPlan {
    /// Files to open, with the row groups inside each that survive
    /// pass-3 pruning.
    pub files: Vec<FileScan>,
}

pub struct FileScan {
    pub path: String,
    pub row_groups: Vec<usize>,
}

pub async fn plan_scan(
    table: &Table,
    predicate: &Predicate,
) -> Result<ScanPlan> {
    let snapshot = table.current_snapshot().await?;
    let manifest_list = read_manifest_list(&snapshot.manifest_list_path).await?;
    let spec = &snapshot.partition_spec;

    // PASS 1: prune manifests by partition summary.
    let candidate_manifests: Vec<_> = manifest_list
        .manifests
        .iter()
        .filter(|m| manifest_might_match(m, predicate, spec))
        .collect();

    // PASS 2: open each surviving manifest, prune data files by per-file
    // statistics. The full predicate (not just partition predicates) is
    // applied here.
    let mut candidate_files: Vec<DataFile> = Vec::new();
    for manifest_entry in &candidate_manifests {
        let manifest = read_manifest(&manifest_entry.manifest_path).await?;
        for entry in manifest.entries {
            if matches!(entry.status, EntryStatus::Existing | EntryStatus::Added) {
                if data_file_might_match(&entry.data_file, predicate) {
                    candidate_files.push(entry.data_file);
                }
            }
        }
    }

    // PASS 3: for each candidate file, open the Parquet footer and
    // prune row groups by per-column-chunk statistics. This is the
    // Parquet reader's responsibility; the table format hands it the
    // file list and the predicate.
    let mut file_scans = Vec::new();
    for data_file in candidate_files {
        let file_meta = open_parquet_metadata(&data_file.path).await?;
        let surviving_row_groups: Vec<usize> = (0..file_meta.num_row_groups())
            .filter(|&rg_idx| {
                row_group_might_match(file_meta.row_group(rg_idx), predicate)
            })
            .collect();

        // If no row groups survive, drop the file entirely; otherwise
        // emit a FileScan with the surviving row group indices.
        if !surviving_row_groups.is_empty() {
            file_scans.push(FileScan {
                path: data_file.path,
                row_groups: surviving_row_groups,
            });
        }
    }

    Ok(ScanPlan { files: file_scans })
}
```

The three passes are visible as three blocks. Each block consumes only the output of the previous; the metadata reads are bounded; the pruning compounds. Production code parallelizes the per-manifest reads in Pass 2 and the per-file footer reads in Pass 3 — the Artemis planner uses a `tokio::stream::FuturesUnordered` to issue concurrent S3 GETs, capped at a configurable concurrency level (typically 64) to avoid overwhelming the connection pool. The structure is the same; the I/O is parallel.

The function also surfaces the **pruning summary** the operations team uses to spot ineffective queries. A production version returns a `ScanPlan` plus a `ScanPlanSummary` containing the counts at each pass (`manifests_total`, `manifests_kept`, `files_total`, `files_kept`, `row_groups_total`, `row_groups_kept`) — the metric the audit job consumes.

---

## Key Takeaways

- **Pruning compounds across three passes**: manifest list → manifest entries → row groups. Each pass uses statistics at a different granularity but the same statistics algebra (min/max/null intersection). Getting any one pass wrong loses an order of magnitude of work.
- **Pruning correctness is asymmetric.** False positives (reading a file that doesn't match) are acceptable; false negatives (skipping a file that does match) are silent data loss. Every pruning function preserves the asymmetry by being conservative on uncertainty.
- **Lifting source predicates through transforms** is the planner's most error-prone job. Identity preserves everything; date transforms preserve order with slight conservatism; bucket destroys order so range predicates can't lift; truncate produces conservative but sound lifts. Unsupported transforms fall back to "all partitions might match" — bucket partitioning is only correct for equality-queried columns.
- **Clustering pays off at Pass 2.** A clustered file has tight per-column statistics on the cluster columns; an unclustered file's statistics span the entire dimension. The 10×-15× clustering improvement from Lesson 2 shows up entirely in Pass 2's manifest-entry filtering.
- **The pruning summary is a first-class metric.** Per-query ratios (manifests-kept / manifests-total, files-kept / files-total) are the lakehouse's QPS-and-latency equivalent. Low ratios on frequent queries are an architectural signal that the partition or clustering strategy is misaligned with the workload.

{{#quiz quiz-03.toml}}
