# Lesson 2 — Multidimensional Clustering

**Module:** Data Lakes — M03: Partitioning and Clustering
**Position:** Lesson 2 of 3
**Source:** Apache Iceberg specification, "Sort Orders" and "Clustering" sections. Z-order from Morton (1966), "A computer oriented geodetic data base and a new technique in file sequencing." Hilbert curve from Hilbert (1891) with the practical engineering treatment in Faloutsos (1986). DuckDB blog post on Z-order indexing for an applied perspective.

> **Source note:** Synthesis-mode from training knowledge and the public literature; the historical references are well-established. Applied details (which cluster ordering Iceberg implements, when Snowflake-style clustering applies) would benefit from verification against the relevant vendor documentation.

---

## Context

Lesson 1's partition spec — `(mission_id, day(ts))` — makes the queries that filter on mission and time fast. The 30% of queries that also filter on `payload_id` and the 15% that filter on `sensor_kind` still get partition-level pruning, but within each partition every payload and every sensor is randomly mixed across the files. The per-file statistics on `payload_id` span every payload; pruning by payload alone produces no benefit at the file level.

The fix is **clustering**: an arrangement of rows *within* a partition such that rows with similar values for the clustering columns end up in the same file. A partition that contains 30 GB of telemetry spanning 8 payloads, clustered on `payload_id`, becomes 30 files where each file contains rows from one or two payloads. A query for one payload reads 1-2 files out of 30. The per-file `payload_id` statistic is now tight; pruning works.

The hard case is multidimensional clustering: clustering on `payload_id` and `sensor_kind` and `region` and `quality_flag` simultaneously. The data sits in a four-dimensional space; the file boundaries need to be arranged so that nearby points in *any* of the dimensions tend to share a file. The naive solution — sort by `(payload_id, sensor_kind, region, quality_flag)` — clusters perfectly on the first column but degrades to random on the others. **Space-filling curves** — Z-order, Hilbert — are the answer. They map the multidimensional space to a single dimension in a way that preserves locality across all dimensions simultaneously.

This lesson develops Z-order and Hilbert curves: what they are, why they work, the limits of the technique, and how to apply them in a lakehouse. The capstone uses Z-order on the SDA observation table to cluster on `(payload_id, sensor_kind)`; this lesson is the design and intuition behind that choice.

---

## Core Concepts

### The Multidimensional Locality Problem

A 2D analog makes the problem visible. Imagine a square of points distributed uniformly in a `(x, y)` plane. The query workload picks rectangular regions of the square. The data is stored in files; the planner picks which files to read based on per-file min/max statistics for `x` and `y`. What arrangement of points into files minimizes the number of files a typical query reads?

The naive options:

- **Sort by x.** Each file has a tight `x` range (one strip of the square) and the full `y` range (the whole height). Queries with tight `x` bounds prune well; queries with tight `y` bounds prune nothing.
- **Sort by y.** Mirror image: queries with tight `y` bounds prune well; queries with tight `x` bounds prune nothing.
- **Sort by `(x, y)` lex.** Equivalent to sort by `x` for clustering purposes; the secondary sort by `y` happens within already-narrow `x` strips, which buys very little.

None of these clusters on *both* dimensions simultaneously. A file's bounding rectangle should be roughly square — narrow in both `x` and `y` — so that queries with bounds in either or both dimensions prune the file. The arrangement that produces square-ish files is a **space-filling curve**: a function from a 1D ordering to the 2D plane that preserves locality, so consecutive elements in the 1D ordering are nearby in the 2D plane.

### Z-Order: Bit Interleaving

The simplest space-filling curve is **Z-order**, also called Morton order after the 1966 paper that introduced it. The construction is direct: take the binary representations of the column values, interleave the bits, and use the interleaved value as the sort key.

For 4-bit values `x = 0110` and `y = 1010`, the Z-order key interleaves them bit by bit starting from the high bit:

```
x: 0  1  1  0
y: 1  0  1  0
z: 0 1 1 0 1 1 0 0   (read top-to-bottom, left-to-right)
```

The key property: in the binary representation of the Z-order key, the high bits determine the high-level position in the plane, the next bits refine within that, and so on. Two values with the same top eight bits of Z-order key live in the same coarse 2D region. Sorting points by Z-order key produces a 1D ordering where consecutive points are clustered in the 2D plane.

Visually, the Z-order curve traces a Z-shaped path through the plane: visit the bottom-left, then top-left, then bottom-right, then top-right (the four quadrants); then recursively within each quadrant trace another Z-shape. The pattern is fractal — same shape at every scale.

For `N` dimensions the construction generalizes: interleave the bits of `N` column values, one bit from each column in round-robin order. The resulting key has the same property: high bits determine coarse N-dimensional position. Iceberg's clustering support for Z-order takes a list of columns and produces this multidimensional ordering.

The cost: bit interleaving is cheap (a few CPU instructions per coordinate). The constraint: the columns must be normalized to the same numeric range and bit width — typically by computing the rank or quantile of each column's distribution and using the rank as the interleaving input. The Iceberg implementation does this normalization internally; the user supplies the column names.

### Z-Order's Limits: The Diagonal Discontinuity

Z-order has a known weakness. The Z-shape's recursive structure produces a "jump" at every level boundary: the curve goes from the top-right corner of one Z to the bottom-left corner of the next Z. Points adjacent along this jump in 1D space can be far apart in 2D space — a query that happens to span the discontinuity sees the file containing both endpoints listed as "potentially matching" even though the file actually contains widely-separated points.

The practical effect: Z-order is good but not perfect. Pruning effectiveness is typically 2-10× over a single-column sort for two-dimensional clustering, less as the dimension count grows. For most workloads this is enough; the diagonal-discontinuity loss is small relative to the win.

The fix is the **Hilbert curve**, a more sophisticated space-filling curve that avoids the discontinuity. The Hilbert curve traces a continuous path through the plane (no jumps), which produces tighter clustering and better pruning. The cost is computation: deriving the Hilbert key for a coordinate is several times slower than bit interleaving, and the implementation is famously fiddly. The benefit is typically a 1.5-2× improvement in clustering effectiveness over Z-order.

For the Artemis archive, Z-order is the right choice. The diagonal-discontinuity loss is dominated by the per-file row count (~1M rows per 128MB file); the clustering granularity is fine relative to the file size. The Hilbert curve would be marginally better and substantially harder to implement correctly. Production deployments that have measured both have generally landed on Z-order; Hilbert is most often seen in specialized geospatial or scientific contexts where the 1.5-2× matters enough to pay the complexity cost.

### Clustering vs Partitioning: When to Use Which

The two disciplines are complementary, not competing. The decision rule:

- **Partition** dimensions that appear in nearly every query and have manageable cardinality after transform. The partition makes pruning the cheapest possible operation (manifest-list summary lookup, no file open). The Artemis archive partitions on `(mission_id, day(ts))` because these are in 95-100% of queries.
- **Cluster** dimensions that appear in many but not all queries, or dimensions where partitioning would produce the small-file problem. Clustering keeps the partition count manageable while preserving file-level pruning. The Artemis archive clusters on `(payload_id, sensor_kind)` inside each `(mission_id, day)` partition.

Cardinality is the other determinant. A dimension with 4 distinct values (`quality_flag`) gives essentially no clustering benefit — the four values are mixed into every file anyway. A dimension with 8-1000 distinct values is the sweet spot for clustering: enough cardinality to produce useful file-level statistics, low enough that the clustering keys don't drown in noise.

Three dimensions is the practical maximum for Z-order clustering. With four or more dimensions, the bit-interleaving structure starts producing keys where each individual dimension gets only 16 / N bits of resolution, and the per-dimension clustering degrades. The Artemis archive uses two-dimensional clustering (`payload_id`, `sensor_kind`); adding a third dimension is being evaluated for the Module 6 compaction redesign but the early measurements suggest the marginal benefit isn't worth the complexity.

### Clustering Is a Write-Time Discipline

Crucially, **clustering does not change the data file format.** A clustered table is just a normal Parquet table where the rows happen to be sorted by the cluster key. The reader sees ordinary Parquet files with per-column statistics; the pruning that works is the same pruning Module 1 introduced (min/max on each column chunk). The cluster key never appears in the data — it is computed at write time, used to order the rows, and discarded.

This has a useful operational consequence: clustering can be *added* to an existing table without breaking readers. The Module 6 compaction job is what physically applies the clustering — it reads existing data files, sorts the rows by the cluster key, and rewrites them as new files. Readers see a slow improvement in pruning effectiveness as the compaction completes; no schema change, no query rewrite, no migration.

The Iceberg "sort order" metadata records the cluster key for the table so the writer can apply it on new ingests. Snapshot N+1 of a table can have a different sort order than snapshot N; existing files retain their old sort, new files use the new sort, compaction migrates old files when it runs. The discipline is the same as the partition-evolution discipline (Module 4): the table's logical contract is unchanged; the physical layout migrates over time.

### Measuring Clustering Effectiveness

The clustering decision must be measured, not assumed. The metric is the **average files-scanned ratio**: for a workload of representative queries, the ratio of files actually opened to files that would be opened with no clustering at all. A clustering scheme that reduces the ratio from 1.0 (no pruning) to 0.1 (10× pruning improvement) is winning; a scheme that reduces it from 1.0 to 0.95 is doing nothing.

The Artemis archive runs a quarterly **clustering-audit job** against the analyst query log. The job samples 1000 recent queries, evaluates each against the current table layout and against the no-clustering baseline, and reports the average ratio. If the ratio drifts above 0.3, the clustering needs reconsideration — typically because the workload has shifted to query patterns the original spec didn't anticipate. The audit job is the feedback loop that keeps the clustering aligned with the actual workload.

---

## Code Examples

### Computing a 2D Z-Order Key

The bit-interleaving function for two `u32` values, producing a `u64` Z-order key:

```rust,ignore
/// Interleave the bits of two u32 values into a u64 Z-order key.
/// Bit 0 of `x` becomes bit 0 of the result; bit 0 of `y` becomes bit 1;
/// bit 1 of `x` becomes bit 2; bit 1 of `y` becomes bit 3; and so on.
/// Higher-order bits of x/y end up at higher-order positions in the
/// result, which means sorting by the result clusters in 2D.
pub fn zorder_2d(x: u32, y: u32) -> u64 {
    fn spread(v: u32) -> u64 {
        // Spread 32 bits across 64, leaving gaps for the other dimension.
        // The classic "magic number" technique; faster than the bit-by-bit
        // loop. Each line moves and masks half the bits.
        let mut v = v as u64;
        v = (v | (v << 16)) & 0x0000_FFFF_0000_FFFF;
        v = (v | (v << 8))  & 0x00FF_00FF_00FF_00FF;
        v = (v | (v << 4))  & 0x0F0F_0F0F_0F0F_0F0F;
        v = (v | (v << 2))  & 0x3333_3333_3333_3333;
        v = (v | (v << 1))  & 0x5555_5555_5555_5555;
        v
    }
    spread(x) | (spread(y) << 1)
}
```

The "magic numbers" are a standard trick for parallel bit spreading. Each step doubles the spacing between bits while masking off the extras. For five steps, 32 bits become 32 bits spread across 64 positions — exactly half the positions, leaving the other half for the second dimension.

### Computing an N-Dimensional Z-Order Key

The general case for an arbitrary number of `u32` dimensions, producing a `u128` key (or an arbitrary-precision integer for higher dimensions):

```rust,ignore
/// Compute an N-dimensional Z-order key by bit-interleaving the inputs.
/// For N=2 this gives a 64-bit key; for N=4 a 128-bit key; for higher
/// N the result type would be a u256 or similar.
pub fn zorder_nd(values: &[u32]) -> u128 {
    let n = values.len();
    debug_assert!(n <= 4, "u128 result supports up to 4 u32 dimensions");

    let mut key: u128 = 0;
    // For each bit position in the source values, scatter one bit from
    // each dimension into N consecutive output positions.
    for bit in 0..32 {
        for (dim_idx, &value) in values.iter().enumerate() {
            let v_bit = ((value >> bit) & 1) as u128;
            let out_position = bit * n + dim_idx;
            key |= v_bit << out_position;
        }
    }
    key
}
```

The function above is the canonical reference implementation — correct, easy to verify, but slow. Production code uses the magic-number technique generalized to N dimensions, which is about 10× faster but harder to read. For batch processing at write time, the slow version is plenty fast; the writer computes O(rows) keys per file, which is small relative to the encoding-and-compression cost.

### Applying Z-Order at Write Time

The clustering step in the write path: compute Z-order keys for the rows, sort the rows by the keys, hand the sorted rows to the Module 1 Parquet writer.

```rust,ignore
use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::{lexsort_to_indices, SortColumn, SortOptions};

/// Re-order the rows of a record batch by the Z-order of the given
/// cluster columns, in preparation for writing to Parquet. The cluster
/// columns must be u32-normalized — typically the rank or quantile of
/// the column's value distribution, since Z-order requires same-scale
/// inputs.
pub fn zorder_batch(
    batch: &RecordBatch,
    cluster_columns: &[&str],
) -> Result<RecordBatch, anyhow::Error> {
    // 1. Extract the cluster columns as u32 arrays.
    let cluster_arrays: Vec<&UInt32Array> = cluster_columns
        .iter()
        .map(|name| {
            let idx = batch.schema().index_of(name)?;
            batch.column(idx)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| anyhow::anyhow!("{name} is not UInt32"))
        })
        .collect::<Result<_, _>>()?;

    // 2. Compute Z-order keys for each row.
    let n_rows = batch.num_rows();
    let mut keys: Vec<u128> = Vec::with_capacity(n_rows);
    for row in 0..n_rows {
        let values: Vec<u32> = cluster_arrays.iter().map(|a| a.value(row)).collect();
        keys.push(zorder_nd(&values));
    }

    // 3. Sort row indices by Z-order key.
    let mut indices: Vec<u32> = (0..n_rows as u32).collect();
    indices.sort_unstable_by_key(|&i| keys[i as usize]);

    // 4. Gather the columns by the sorted indices.
    let idx_array = UInt32Array::from(indices);
    let sorted_columns: Vec<_> = batch.columns().iter()
        .map(|c| arrow::compute::take(c, &idx_array, None))
        .collect::<Result<_, _>>()?;

    Ok(RecordBatch::try_new(batch.schema(), sorted_columns)?)
}
```

The pattern. **Z-order is applied before the file is written**, on a record batch large enough to span the file's worth of rows. For the Artemis writer, this is one row group (1M rows): compute Z-order keys for 1M rows, sort, hand to the writer. The cost is the sort, which is `O(n log n)` and dominated by the key comparison cost for `u128` keys — measurable but not dominant against the encoding and compression cost. For very large row groups (10M+), production code uses partial-radix sort on the high bits of the Z-order key, which is faster but more complex.

The "cluster columns must be u32-normalized" caveat is the production tax. The Iceberg implementation handles this internally by computing the rank of each column's value distribution at write time and using the rank as the interleaving input. The capstone may simplify by assuming the cluster columns are already `u32` (true for `payload_id` and `sensor_kind` in the Artemis schema), with a note that production code generalizes.

### Measuring Pruning Effectiveness

The clustering-audit job, sketched. For each query in the sample, plan against the current table layout and against a hypothetical no-clustering baseline; compute the files-scanned ratio.

```rust,ignore
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct PruningMeasurement {
    pub query_id: u64,
    pub files_with_clustering: usize,
    pub files_without_clustering: usize,
    pub ratio: f64,
}

pub async fn measure_pruning(
    table: &Table,
    queries: &[Query],
) -> Result<Vec<PruningMeasurement>> {
    let mut measurements = Vec::new();

    for query in queries {
        // Plan against the actual (clustered) table.
        let actual_files = table.read_plan(&query.predicate).await?;
        let clustered_count = actual_files.len();

        // Plan against a hypothetical table with the same partition spec
        // but no clustering. The simplest approximation: assume every
        // file in every relevant partition is matched (because the
        // unsorted files would have statistics spanning the full range
        // of the cluster columns). For the Artemis archive's
        // (mission, day) partitioning, this equals every file in
        // partitions matching the partition predicates of the query.
        let partition_files = table
            .read_plan_partition_only(&query.predicate)
            .await?
            .len();

        measurements.push(PruningMeasurement {
            query_id: query.id,
            files_with_clustering: clustered_count,
            files_without_clustering: partition_files,
            ratio: clustered_count as f64 / partition_files.max(1) as f64,
        });
    }

    Ok(measurements)
}
```

The measurement is approximate — the "no-clustering baseline" is a counterfactual that the production code only ever computes synthetically. The Artemis observability stack ingests these measurements weekly and tracks the average ratio over time; sustained drift above 0.3 produces a paging alert that the clustering needs revisiting against the current workload.

---

## Key Takeaways

- **Clustering arranges rows within a partition** so that file-level statistics are tight on multiple dimensions simultaneously. It is the answer for query dimensions that don't fit in the partition spec — appearing in many but not all queries, or having cardinalities that would produce the small-file problem.
- **Z-order via bit interleaving** is the standard multidimensional clustering technique. Sorting rows by their Z-order key clusters the data in N-dimensional space; the bit-interleaving construction is cheap and well-supported in the lakehouse ecosystem.
- **Z-order has a known discontinuity loss** at recursive-step boundaries; the **Hilbert curve** avoids it at substantial implementation cost. For most workloads Z-order is the right tradeoff; Hilbert appears in specialized geospatial and scientific deployments.
- **Clustering is a write-time discipline**, not a format change. Clustered tables are normal Parquet files with rows happening to be sorted by the cluster key. Readers see ordinary per-column statistics; pruning uses the same mechanisms as unclustered tables.
- **Measure the clustering effectiveness.** The files-scanned ratio against a no-clustering baseline is the metric. The Artemis archive's quarterly audit job tracks this against representative queries; drift above 0.3 means the clustering needs reconsideration against the actual workload.

{{#quiz quiz-02.toml}}
