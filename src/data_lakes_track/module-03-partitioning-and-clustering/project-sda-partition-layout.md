# Capstone — SDA Observation Partition Layout

**Module:** Data Lakes — M03: Partitioning and Clustering
**Estimated effort:** 1–2 weeks of focused work
**Prerequisite:** All three lessons in this module completed; all three quizzes passed (≥ 70%). The Module 2 capstone (mission archive table) is the substrate this module extends.

---

## Mission Briefing

### From: SDA Platform Lead, Cold Archive

```
ARCHIVE BRIEFING — RC-2026-04-DL-003
SUBJECT: SDA observation table partition + clustering layout for the
         analyst workload.
PRIORITY: P1 — direct dependency for Q3 SDA dashboard rollout.
```

The SDA observation table now holds 18 months of fused observation data — roughly 8 TB compressed, 40,000 partitions if we use the proposed (mission, day) layout, 250,000 data files at current row-group sizing. The analyst workload against this table has been measured for six weeks; we have a query log of 3,200 representative queries with their predicates and the columns they project.

The job: design the partition spec and clustering layout for the table, implement them on top of the Module 2 table format, demonstrate the pruning effectiveness on the measured workload, and produce a design document that the next operator can read and understand.

The design must hold up under the actual workload — not the workload we wish we had. The query log will tell you which predicates appear with what frequency. Your spec must serve those predicates well. Don't over-fit (the workload will shift), but don't ignore the data either.

Module 4 will add time-travel queries that work against this layout; Module 6's compaction will rewrite files into the chosen cluster order. Get the layout right; the rest of the track builds on it.

---

## What You're Building

A Rust crate, `artemis-partition-layout`, that extends the Module 2 `artemis-table-format` with:

- A `PartitionSpec` type and the transforms (Identity, Day, Month, Year, Bucket, Truncate) as described in Lesson 1, implementing Iceberg-spec-compatible computation.
- A `SortOrder` type recording the cluster columns and ordering (NullsFirst/Last) for the table.
- A `lift_predicate` function (Lesson 3) that turns source-column predicates into partition predicates per the transform's lifting rules. The function must produce safe conservative outputs for all (transform, op) pairs.
- An extended writer that respects both the partition spec (file boundaries) and the sort order (row order within file) on commit.
- An extended `read_plan` (Module 2 carry-forward) implementing all three pruning passes.
- A benchmarking harness that takes a query log and produces a pruning-effectiveness report.

The deliverable includes the implementation, the integration tests, the benchmark against the measured workload, and the design document.

---

## Functional Requirements

1. **Transform implementations.** Identity, Day, Month, Year, Bucket(N), Truncate(width). Match the Iceberg spec for output values (Day = days since Unix epoch as int32; Bucket = `(Murmur3(value) & Long.MAX_VALUE) % N`; etc.).
2. **Predicate lifting.** For each `(transform, leaf_op)` pair, implement the lifting rule. The output must be safe-conservative: never produces a partition predicate that excludes a partition where the source predicate could match. Unsupported pairs return `AllPartitions` (no lifting).
3. **Write-side partition splitting.** A `RecordBatch` is split into per-partition-tuple sub-batches before being handed to the Parquet writer. Each output Parquet file contains rows from exactly one partition tuple.
4. **Write-side sort order.** Within each partition's data files, rows are sorted by the table's sort order. For Z-order clustering, the writer computes Z-order keys and sorts by them. For lexicographic sort orders (defined in `SortOrder`), the writer uses `arrow::compute::lexsort`.
5. **Read planning, all three passes.** Pass 1 prunes manifests by partition summary; Pass 2 prunes files by column statistics; Pass 3 prunes row groups via Parquet footers. Pass 3 happens in the Parquet reader; the table format hands the reader the file list and the predicate.
6. **Pruning-summary instrumentation.** The planner returns a `ScanPlanSummary` with counts at each pass (`manifests_total`, `manifests_kept`, `files_total`, `files_kept`, `row_groups_total`, `row_groups_kept`).
7. **Benchmark harness.** A binary `partition-bench` that takes a query log (TSV: query_id, predicate JSON, projected columns) and produces a report with the pruning ratios for each query plus aggregate statistics.

---

## Acceptance Criteria

### Verifiable (automated tests must demonstrate these)

- [x] Each transform's output matches the Iceberg spec's reference test vectors (provided in `tests/iceberg_transform_vectors.toml`).
- [x] `lift_predicate(Day, Range(t1..t2))` produces a `Day` range predicate covering exactly the days containing `[t1, t2]`. Boundary-day tests (`t1` is the first nanosecond of a day, `t2` is the last) pass without dropping or duplicating data.
- [x] `lift_predicate(Bucket(N), Range)` returns `AllPartitions`. `lift_predicate(Bucket(N), Eq)` returns the singleton bucket-equality predicate.
- [x] `lift_predicate(Truncate(W), Ge(v))` returns `Ge(truncate(v))` — strictly conservative for the boundary case where `v` is not on a truncation boundary.
- [x] After committing a 5-million-row batch with `partition by (mission_id, day(ts))` and 3 distinct missions × 7 distinct days, the resulting snapshot has exactly 21 partition tuples in the manifest entries (or fewer if some tuples have no rows).
- [x] After committing the same batch with Z-order clustering on `(payload_id, sensor_kind)`, each output Parquet file's per-column-chunk statistics for both `payload_id` and `sensor_kind` are tighter than the corresponding unclustered baseline (measured in the test as `max - min` for each column, averaged across files).
- [x] `read_plan(WHERE mission_id = 'apollo-7' AND ts >= D1 AND ts < D7)` against a table with 40 missions × 1000 days returns at most 6 manifests in `ScanPlanSummary.manifests_kept`, demonstrating Pass 1 pruning.
- [x] `read_plan(WHERE mission_id = 'apollo-7' AND ts BETWEEN D1 AND D7 AND payload_id = 5)` against a Z-order-clustered table returns at most 30% of the files that the same plan against an unclustered baseline returns, demonstrating Pass 2 pruning improvement from clustering.

### Self-assessed (you write a short justification; reviewer checks it)

- [ ] *(self-assessed)* The partition spec choice for the SDA observation table is justified in `docs/partition-spec-rationale.md` against the measured workload. The doc enumerates the candidate columns, their query coverage and selectivity, and explains why the chosen spec (likely `(mission_id, day(ts))`) wins over alternatives.
- [ ] *(self-assessed)* The clustering choice is justified in `docs/clustering-rationale.md` against the measured workload. The doc shows the files-scanned-ratio improvement on representative queries and explains why Z-order (over lex sort, no clustering, or Hilbert) is the right tradeoff.
- [ ] *(self-assessed)* The lifting rules table is documented in `docs/lifting-rules.md` with a row per `(transform, op)` pair. For each row, the lift is either tight, conservative-but-sound (with the conservative loss quantified), or no-lift (with the reason). The doc is the artifact a future engineer adding a new transform will consult.
- [ ] *(self-assessed)* The benchmark harness's correctness is justified in `docs/benchmark-correctness.md`. The doc describes what counterfactual the harness compares against (no-clustering baseline) and why the comparison is valid.

---

## Architecture Notes

A reasonable module layout (extending the Module 2 crate or as a new crate that depends on it):

```text
artemis-partition-layout/
├── src/
│   ├── lib.rs
│   ├── spec.rs            # PartitionSpec, PartitionField, SortOrder
│   ├── transform.rs       # Transform enum, apply_transform
│   ├── lift.rs            # lift_predicate for each (transform, op)
│   ├── write.rs           # partition splitting + sort + parquet write
│   ├── read.rs            # extended read_plan with three-pass pruning
│   ├── zorder.rs          # zorder_2d, zorder_nd, batch sort helpers
│   └── bin/partition_bench.rs
├── tests/
│   ├── iceberg_transform_vectors.toml
│   ├── lift_correctness.rs
│   ├── partition_split.rs
│   ├── zorder_clustering.rs
│   └── pruning_pyramid.rs
├── benches/
│   └── workload_bench.rs
└── docs/
    ├── partition-spec-rationale.md
    ├── clustering-rationale.md
    ├── lifting-rules.md
    └── benchmark-correctness.md
```

The Iceberg transform reference vectors should be sourced from the actual Iceberg test suite (`apache/iceberg/api/src/test/java/.../TransformTestUtils.java`) and ported to TOML for use here. The bucket transform must use the Iceberg-specified Murmur3 variant; the `fastmurmur3` crate is the recommended implementation.

The Z-order normalization is the production tax flagged in Lesson 2. For the capstone, the cluster columns can be treated as already-`u32` (true for the Artemis schema's `payload_id` and `sensor_kind` after dictionary indexing); a production implementation computes per-column ranks via approximate-quantile sketches at write time. The doc should note the simplification.

---

## Hints

<details>

<summary>Hint 1 — Iceberg's bucket transform's exact specification</summary>

The Iceberg bucket transform is `(Murmur3_32(value) & Integer.MAX_VALUE) % N` for integer N. The Murmur3 variant is specifically the 32-bit one with seed=0. The `fastmurmur3::murmur3_32(&value_bytes, 0)` call produces the right hash; AND-ing with `i32::MAX` (which is `0x7FFF_FFFF` as u32) before the modulo is what produces non-negative bucket indices. Mismatches with the Iceberg reference are almost always sign-handling: the test vector expects bucket 7, your code produces 7 for positive seeds and `N - 7 - 1` for negative seeds.

</details>

<details>

<summary>Hint 2 — The Day transform's UTC convention</summary>

The Day transform produces "days since Unix epoch" as an int32. The conversion is `ns_since_epoch / 1_000_000_000 / 86_400`, treating the timestamp as UTC. Some test inputs are negative (pre-1970 timestamps); the division semantics for negative integers (round-toward-zero vs round-toward-negative-infinity) matter. The Iceberg spec uses Java's integer division, which is round-toward-zero. Rust's `i64::div_euclid` is round-toward-negative-infinity; plain `/` is round-toward-zero — match Java's behavior with plain `/`.

</details>

<details>

<summary>Hint 3 — The lifting-rule table as the test driver</summary>

Implement the lifting rules as a data-driven table: a TOML file with rows of `(transform, source_op, partition_op)` triples, and a test that exercises each row by constructing the source predicate, lifting it, and asserting the partition predicate matches the expected output. The TOML-driven approach makes the rules easy to extend (new transform → new TOML rows) and easy to audit (the table itself is the doc the lift-correctness doc points at).

</details>

<details>

<summary>Hint 4 — Measuring against a no-clustering baseline</summary>

The benchmark harness needs a "no-clustering" counterfactual to measure clustering effectiveness against. The simplest approximation: for each query, compute the set of *partition tuples* that the partition predicates select, then assume every file in those partitions would be read. This is what an unclustered table would do at Pass 2 (no per-file pruning beyond the partition's). The clustered table's files-scanned count divided by this counterfactual is the clustering ratio. The approximation is conservative (it slightly overestimates the unclustered baseline, because real unclustered files have *some* statistics noise), but it gives a clean, reproducible number.

</details>

<details>

<summary>Hint 5 — The query-log format</summary>

The supplied query log (in `tests/queries.tsv`) is TSV with columns `query_id`, `predicate_json`, `projected_columns`. The `predicate_json` is a JSON-encoded AST with `op`, `column`, `value` (or `lo`, `hi`) fields. A small `serde`-driven parser turns each line into a `Predicate` value the planner can consume. The harness does not need to actually execute queries — it only needs to plan them and measure the planning output. Plan-and-measure is far cheaper than plan-and-execute; the benchmark runs the entire 3,200-query log in seconds, not hours.

</details>

---

## References

- *Designing Data-Intensive Applications* (Kleppmann & Riccomini), Chapter 6 — "Partitioning"
- Apache Iceberg specification, "Partition Transforms" and "Partition Evolution" sections
- Morton (1966), "A computer oriented geodetic data base and a new technique in file sequencing" — the Z-order paper
- DuckDB blog, "Z-Order Indexing for Multi-Dimensional Range Queries" — the applied perspective

---

## When You're Done

The crate is "done" when all eight verifiable acceptance criteria pass in CI, the four self-assessed docs are written, and the benchmark report shows the pruning improvement on the 3,200-query workload. Module 4 begins with the assumption that this layout is in place; the time-travel mechanics will exercise it heavily.
