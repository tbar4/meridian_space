# Project — Orbital Catalog Merge System

**Module:** Database Internals — M06: Query Processing  
**Track:** Orbital Object Registry  
**Estimated effort:** 6–8 hours

---
<!-- toc -->
---
## SDA Incident Report — OOR-2026-0047

> **Classification:** ENGINEERING DIRECTIVE  
> **Subject:** Build a structured query engine for multi-source TLE catalog merging  
>  
> Replace the ad-hoc nested-loop catalog merge with a composable query execution engine. The engine must support scan, filter, sort, and join operators, and merge TLE data from 5 sources within the conjunction pipeline's 10-second deadline.

---

## Acceptance Criteria

1. **Volcano operators.** Implement `SeqScan`, `Filter`, `Projection`, and `Sort` operators using the `Operator` trait. Compose them into a plan that scans 100,000 TLE records, filters by inclination > 80°, and projects to (norad_id, epoch).

2. **Hash join.** Implement a hash join operator. Join two 100k-row sources on NORAD ID. Verify the output contains exactly the matching pairs.

3. **Sort-merge join.** Implement a sort-merge join for pre-sorted inputs. Join two 100k-row sources (pre-sorted by NORAD ID). Verify output matches the hash join result.

4. **Multi-way merge.** Implement a 5-way merge join using a min-heap (reuse the merge iterator pattern from Module 3). Merge 5 sources of 100k records each, all sorted by NORAD ID. Verify the merged output is sorted and contains all matching records.

5. **Performance target.** The 5-way merge of 500,000 total records must complete in under 2 seconds. Print elapsed time. Compare against a naive nested-loop join on a subset (1,000 records per source) and report the speedup.

6. **Conflict resolution.** When multiple sources provide TLEs for the same NORAD ID, select the TLE with the most recent epoch. Print the number of conflicts resolved and the winning source for 10 sample objects.

7. **Vectorized filter (bonus).** Implement a vectorized filter that processes batches of 1024 rows in columnar format. Compare its throughput to the row-at-a-time volcano filter on 100,000 rows.

---

## Starter Structure

```
catalog-merge/
├── Cargo.toml
├── src/
│   ├── main.rs            # Entry point
│   ├── operator.rs         # Operator trait, SeqScan, Filter, Projection, Sort
│   ├── hash_join.rs        # HashJoinOperator
│   ├── sort_merge.rs       # SortMergeJoinOperator
│   ├── merge_iter.rs       # MultiWayMerge (reuse from Module 3)
│   ├── vectorized.rs       # VecFilter (bonus)
│   └── tle.rs              # TleRow, test data generation
```

---

## Test Data Generation

Generate synthetic TLE data for 5 sources:

```rust,ignore
fn generate_source(source_name: &str, num_objects: usize) -> Vec<TleRow> {
    let mut rng = /* deterministic seed per source */;
    (0..num_objects).map(|i| TleRow {
        norad_id: i as u32 + 1, // NORAD IDs 1..100000
        epoch: 84.0 + rng.gen::<f64>() * 0.5, // Slight epoch variation per source
        inclination: rng.gen::<f64>() * 180.0,
        mean_motion: 14.0 + rng.gen::<f64>() * 2.0,
        source: source_name.to_string(),
    }).collect()
}
```

Each source provides TLEs for the same 100k objects but with slightly different epochs and measurements. The merge resolves conflicts by picking the most recent epoch per NORAD ID.

---

## Hints

<details>
<summary>Hint 1 — Hash join operator as a volcano operator</summary>

The hash join operator's `open()` consumes the entire build side (calling `build_child.next()` until None, building the hash table). Then `next()` probes one row at a time from the probe side. The build phase is a pipeline breaker; the probe phase pipelines.

</details>

<details>
<summary>Hint 2 — Conflict resolution as a post-merge step</summary>

After the 5-way merge produces groups of TLEs with the same NORAD ID, apply a "group-by" operator that collects all rows with the same key and emits the winner (most recent epoch). This is a simple reduce over each group.

</details>

<details>
<summary>Hint 3 — Performance measurement</summary>

Use `std::time::Instant` for timing. Measure the merge end-to-end (including any sort phases). For the nested-loop comparison, use a small subset (1,000 rows per source) to avoid waiting minutes.

</details>

---

## Track Complete

Congratulations. You have built a storage engine from the ground up:

| Module | What You Built |
|--------|---------------|
| M1: Storage Engine Fundamentals | Page layout, buffer pool, slotted pages |
| M2: B-Tree Index Structures | B+ tree with splits, merges, range scans |
| M3: LSM Trees & Compaction | Memtable, SSTables, leveled compaction, bloom filters |
| M4: WAL & Recovery | Write-ahead log, crash recovery, checkpointing |
| M5: Transactions & Isolation | MVCC snapshot isolation, write conflict detection, GC |
| M6: Query Processing | Volcano model, vectorized execution, hash/sort-merge joins |

The Orbital Object Registry is now a fully functional, crash-recoverable, transactional storage engine with indexed access and structured query execution. The ESA deadline has been met.
