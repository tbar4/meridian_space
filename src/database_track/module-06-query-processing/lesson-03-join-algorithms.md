# Lesson 3 — Join Algorithms

**Module:** Database Internals — M06: Query Processing  
**Position:** Lesson 3 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapter 15; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 6

> **Source note:** This lesson was synthesized from training knowledge. Verify Petrov's join algorithm cost models and Kleppmann's distributed join discussion against the source chapters.

---

## Context

The OOR's catalog merge problem is fundamentally a **join**: match TLE records from 5 sources on NORAD catalog ID, then select the best TLE for each object (most recent epoch, highest source priority). In SQL terms: `SELECT * FROM source_a JOIN source_b ON a.norad_id = b.norad_id`.

The choice of join algorithm determines whether this merge takes 45 seconds (nested-loop) or under 1 second (hash join). This lesson covers the three fundamental join algorithms, their cost models, and when each is optimal.

---

## Core Concepts

### Nested-Loop Join

The simplest join: for each row in the outer table, scan the entire inner table for matches.

```text
for each row_a in source_a:          # |A| iterations
    for each row_b in source_b:      # |B| iterations per outer row
        if row_a.norad_id == row_b.norad_id:
            emit (row_a, row_b)
```

**Cost:** O(|A| × |B|) comparisons. For two 100k-row sources: 10 billion comparisons. Completely impractical for the OOR catalog merge.

**When to use:** Only when one side is very small (< 100 rows) or when no better algorithm is available (no index, insufficient memory for a hash table). Also useful for non-equi joins (e.g., `a.epoch > b.epoch`) where hash join doesn't apply.

**Block nested-loop** improves this by loading a block of the outer table into memory and scanning the inner table once per block. This reduces I/O from |A| × (inner scans) to |A|/B × (inner scans).

### Hash Join

Build a hash table on the smaller input (the **build side**), then probe it with the larger input (the **probe side**).

**Build phase:** Scan the build side and insert each row into a hash table keyed by the join column.

**Probe phase:** Scan the probe side. For each row, hash the join column and look up matching rows in the hash table.

```text
Build: hash_table = {}
for each row_b in source_b:
    hash_table[row_b.norad_id].append(row_b)

Probe:
for each row_a in source_a:
    for each row_b in hash_table[row_a.norad_id]:
        emit (row_a, row_b)
```

**Cost:** O(|A| + |B|) — one scan of each input. Hash table operations are O(1) amortized.

**Memory:** The hash table must fit in memory. Size ≈ |build_side| × (key_size + row_size + overhead). For 100k TLE records at ~100 bytes each: ~10MB. Trivially fits in memory.

**When to use:** Equi-joins (join on equality) where the build side fits in memory. This is the default join algorithm in most query engines for good reason — it's optimal for the vast majority of join workloads.

For the OOR: hash join merges two 100k-row sources in ~200k operations. Five sources require 4 sequential hash joins (or a multi-way hash join), all completing in under 100ms.

### Sort-Merge Join

Sort both inputs on the join column, then merge them in a single pass (like the LSM merge iterator from Module 3).

**Sort phase:** Sort both inputs by the join key. O(|A| log |A| + |B| log |B|).

**Merge phase:** Advance two cursors through the sorted inputs, matching on the join key. O(|A| + |B|).

```text
Sort source_a by norad_id
Sort source_b by norad_id

cursor_a = 0, cursor_b = 0
while cursor_a < |A| and cursor_b < |B|:
    if a[cursor_a].norad_id == b[cursor_b].norad_id:
        emit (a[cursor_a], b[cursor_b])
        advance both cursors (handling duplicates)
    elif a[cursor_a].norad_id < b[cursor_b].norad_id:
        cursor_a += 1
    else:
        cursor_b += 1
```

**Cost:** O(|A| log |A| + |B| log |B|) for the sort phases, O(|A| + |B|) for the merge. Dominated by the sort.

**When to use:** When inputs are already sorted (e.g., from an index scan or a preceding sort operator), the sort phase is free and the total cost is O(|A| + |B|) — optimal. Also useful when the join result must be sorted (the output is already in join-key order). Handles non-memory-fitting inputs gracefully via external sort.

For the OOR: if TLE sources are pre-sorted by NORAD ID (which they often are, since NORAD IDs are sequential), sort-merge join is optimal — the sort phase costs nothing, and the merge is a single linear pass.

### Cost Comparison

| Algorithm | Time | Memory | Pre-sorted Input |
|-----------|------|--------|-----------------|
| Nested-loop | O(A × B) | O(1) | No benefit |
| Hash join | O(A + B) | O(min(A,B)) | No benefit |
| Sort-merge | O(A log A + B log B) | O(A + B) for sort | O(A + B) if pre-sorted |

### Multi-Way Join for 5 Sources

The OOR catalog merge joins 5 sources. Strategies:

**Sequential pairwise:** Join source 1 with 2, then result with 3, then with 4, then with 5. Four hash joins. Total cost: O(5 × N) where N is the source size. Simple and effective.

**Multi-way sort-merge:** Sort all 5 sources by NORAD ID, then merge all 5 simultaneously using a priority queue (exactly the merge iterator from Module 3). One pass through all data. Optimal if sources are pre-sorted.

For the OOR, the multi-way sort-merge is the better choice: TLE sources arrive pre-sorted by NORAD ID, and the merge iterator is already implemented.

---

## Code Examples

### Hash Join Implementation

```rust,ignore
use std::collections::HashMap;

/// Hash join: match TLE records from two sources on NORAD ID.
fn hash_join(
    build_side: &[TleRow],   // Smaller source
    probe_side: &[TleRow],   // Larger source
) -> Vec<(TleRow, TleRow)> {
    // Build phase: index the build side by NORAD ID
    let mut hash_table: HashMap<u32, Vec<&TleRow>> = HashMap::new();
    for row in build_side {
        hash_table.entry(row.norad_id).or_default().push(row);
    }

    // Probe phase: look up each probe-side row in the hash table
    let mut results = Vec::new();
    for probe_row in probe_side {
        if let Some(matches) = hash_table.get(&probe_row.norad_id) {
            for &build_row in matches {
                results.push((build_row.clone(), probe_row.clone()));
            }
        }
    }
    results
}
```

### Sort-Merge Join for Pre-Sorted Sources

```rust,ignore
/// Sort-merge join on pre-sorted inputs. Both inputs must be sorted by norad_id.
fn sort_merge_join(
    left: &[TleRow],
    right: &[TleRow],
) -> Vec<(TleRow, TleRow)> {
    let mut results = Vec::new();
    let mut li = 0;
    let mut ri = 0;

    while li < left.len() && ri < right.len() {
        match left[li].norad_id.cmp(&right[ri].norad_id) {
            std::cmp::Ordering::Equal => {
                // Collect all rows with this key from both sides
                let key = left[li].norad_id;
                let l_start = li;
                while li < left.len() && left[li].norad_id == key { li += 1; }
                let r_start = ri;
                while ri < right.len() && right[ri].norad_id == key { ri += 1; }

                // Cross product of matching rows (for equi-join)
                for l in &left[l_start..li] {
                    for r in &right[r_start..ri] {
                        results.push((l.clone(), r.clone()));
                    }
                }
            }
            std::cmp::Ordering::Less => li += 1,
            std::cmp::Ordering::Greater => ri += 1,
        }
    }
    results
}
```

The sort-merge join's merge phase is identical to the LSM merge iterator logic. For the OOR's unique NORAD IDs (no duplicates within a source), the cross-product in the equal case always produces exactly one match — the merge is linear.

---

## Key Takeaways

- Nested-loop join is O(A × B) — only viable for very small inputs. Hash join is O(A + B) with O(min(A,B)) memory. Sort-merge join is O(A + B) if inputs are pre-sorted.
- Hash join is the default for equi-joins in most query engines. It requires the build side to fit in memory, which is almost always true for the OOR's workload sizes.
- Sort-merge join is optimal when inputs are pre-sorted (the sort phase is free). The LSM merge iterator from Module 3 is already a sort-merge join — the same algorithm applies here.
- The OOR catalog merge (5 pre-sorted sources × 100k objects) is best served by a multi-way sort-merge: one linear pass through all sources using a merge iterator with a priority queue.
- Join algorithm selection is a query optimization decision. The execution engine should support all three algorithms and choose based on input sizes, sort order, and available memory.

---

{{#quiz lesson-03-quiz.toml}}
