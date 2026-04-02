# Lesson 2 — Vectorized Execution

**Module:** Database Internals — M06: Query Processing  
**Position:** Lesson 2 of 3  
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 6

> **Source note:** This lesson was synthesized from training knowledge. Verify Kleppmann's columnar processing analysis against Chapter 6. Additional references: MonetDB/X100 paper (Boncz et al., 2005), DuckDB architecture documentation.

---
<!-- toc -->
---
## Context

The volcano model processes one row at a time. For the OOR's catalog merge — 500,000 rows from 5 sources, with comparison logic on 4 floating-point fields — the per-row function call overhead and cache inefficiency dominate CPU time. Vectorized execution addresses this by changing the unit of work from a single row to a **batch** (or vector) of rows.

Instead of `next() → Option<Row>`, vectorized operators return `next_batch() → Option<ColumnBatch>`, where a `ColumnBatch` contains 256–2048 rows stored in columnar format (one array per column). Operators process entire columns at once — a filter evaluates a predicate on an `f64` array of 1024 inclination values in a tight loop, producing a selection bitmap. This tight loop is cache-friendly (one contiguous array), branch-predictor-friendly (same instruction repeated), and SIMD-exploitable (process 4 or 8 values per instruction).

---

## Core Concepts

### Row-at-a-Time vs. Batch-at-a-Time

**Row-at-a-time (volcano):** Each operator call processes 1 row. For N rows through K operators: N × K function calls, each touching a different memory region.

**Batch-at-a-time (vectorized):** Each operator call processes B rows. For N rows through K operators: (N/B) × K function calls. Each call processes a contiguous array, keeping the CPU cache warm and enabling auto-vectorization (SIMD).

The batch size B is typically 1024 or 2048 — large enough to amortize per-call overhead, small enough to fit in L1/L2 cache.

### Columnar Batch Format

A vectorized batch stores data in **columnar layout** — one array per column:

```text
Row-oriented (volcano):
  Row 0: { norad_id: 25544, epoch: 84.7, inclination: 51.6, ... }
  Row 1: { norad_id: 43013, epoch: 84.2, inclination: 97.4, ... }

Columnar (vectorized):
  norad_id:    [25544, 43013, ...]    ← contiguous u32 array
  epoch:       [84.7,  84.2,  ...]    ← contiguous f64 array
  inclination: [51.6,  97.4,  ...]    ← contiguous f64 array
```

A filter on `inclination > 80.0` processes the `inclination` array without touching `norad_id` or `epoch` — only the relevant column is loaded into cache. The Rust compiler can auto-vectorize the tight comparison loop into SIMD instructions (e.g., `_mm256_cmp_pd` comparing 4 `f64` values per instruction).

### Selection Vectors

Instead of copying matching rows to a new batch (expensive), vectorized engines use a **selection vector** — an array of indices into the batch that identifies which rows passed the filter:

```rust,ignore
// Filter: inclination > 80.0
let inclinations: &[f64] = &batch.inclination;
let mut selection: Vec<u32> = Vec::new();
for (i, &inc) in inclinations.iter().enumerate() {
    if inc > 80.0 {
        selection.push(i as u32);
    }
}
// selection = [1, 3, 7, ...]  ← indices of matching rows
```

Downstream operators use the selection vector to skip non-matching rows without copying data. This avoids the memory allocation and copy cost of materializing filtered batches.

### Apache Arrow as the Columnar Format

Apache Arrow defines a standardized in-memory columnar format used by DataFusion, DuckDB (internally similar), Polars, and many data processing engines. Key features:

- **Zero-copy sharing** between operators — no serialization/deserialization between pipeline stages.
- **Validity bitmaps** for null handling — one bit per value indicating null/non-null.
- **Dictionary encoding** for low-cardinality string columns — stores unique values once and references them by index.

For the OOR, using Arrow arrays for TLE column batches enables integration with the broader Rust data ecosystem (the `arrow` crate).

---

## Code Examples

### Vectorized Filter Operator

```rust,ignore
const BATCH_SIZE: usize = 1024;

/// A batch of TLE records in columnar format.
struct TleBatch {
    norad_ids: Vec<u32>,
    epochs: Vec<f64>,
    inclinations: Vec<f64>,
    mean_motions: Vec<f64>,
    len: usize,
}

/// Vectorized operator interface.
trait VecOperator {
    fn open(&mut self) -> io::Result<()>;
    fn next_batch(&mut self) -> io::Result<Option<TleBatch>>;
    fn close(&mut self) -> io::Result<()>;
}

/// Vectorized filter: evaluates predicate on entire columns at once.
struct VecFilter {
    child: Box<dyn VecOperator>,
    /// Returns a boolean mask: true for rows that pass the filter.
    predicate: Box<dyn Fn(&TleBatch) -> Vec<bool>>,
}

impl VecOperator for VecFilter {
    fn open(&mut self) -> io::Result<()> { self.child.open() }

    fn next_batch(&mut self) -> io::Result<Option<TleBatch>> {
        loop {
            match self.child.next_batch()? {
                None => return Ok(None),
                Some(batch) => {
                    let mask = (self.predicate)(&batch);
                    let filtered = apply_mask(&batch, &mask);
                    if filtered.len > 0 {
                        return Ok(Some(filtered));
                    }
                    // Entire batch filtered out — pull next
                }
            }
        }
    }

    fn close(&mut self) -> io::Result<()> { self.child.close() }
}

/// Apply a boolean mask to a batch, keeping only rows where mask[i] is true.
fn apply_mask(batch: &TleBatch, mask: &[bool]) -> TleBatch {
    let mut out = TleBatch {
        norad_ids: Vec::new(), epochs: Vec::new(),
        inclinations: Vec::new(), mean_motions: Vec::new(), len: 0,
    };
    for (i, &keep) in mask.iter().enumerate() {
        if keep {
            out.norad_ids.push(batch.norad_ids[i]);
            out.epochs.push(batch.epochs[i]);
            out.inclinations.push(batch.inclinations[i]);
            out.mean_motions.push(batch.mean_motions[i]);
            out.len += 1;
        }
    }
    out
}
```

The predicate function operates on entire columns: `|batch| batch.inclinations.iter().map(|&inc| inc > 80.0).collect()`. This tight loop over a contiguous `f64` array is exactly the pattern the compiler auto-vectorizes into SIMD instructions. A production implementation would use selection vectors instead of copying rows.

---

## Key Takeaways

- Vectorized execution processes batches of rows (typically 1024) instead of single rows, reducing per-row overhead by 100–1000x for CPU-bound operations.
- Columnar layout stores each column as a contiguous array, enabling cache-efficient processing and SIMD auto-vectorization. A filter on one column never touches other columns.
- Selection vectors track which rows pass a filter without copying data. This avoids materialization cost and keeps downstream operators working on the original arrays.
- Apache Arrow provides a standardized columnar format for zero-copy interop between operators and libraries. The `arrow` Rust crate is the foundation for DataFusion.
- Vectorized execution is most impactful for CPU-bound analytical queries (aggregations, joins, comparisons). For I/O-bound point lookups, the volcano model is sufficient.

---

{{#quiz lesson-02-quiz.toml}}
