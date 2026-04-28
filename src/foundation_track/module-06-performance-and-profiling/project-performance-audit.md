# Project — Meridian Control Plane Performance Audit

**Module:** Foundation — M06: Performance & Profiling
**Prerequisite:** All three module quizzes passed (≥70%)

---
<!-- toc -->
---
## Mission Brief

**TO:** Platform Engineering
**FROM:** Mission Control Systems Lead
**CLASSIFICATION:** UNCLASSIFIED // INTERNAL
**SUBJECT:** RFC-0058 — Control Plane Performance Audit and Remediation

---

The telemetry processor built in Module 5 achieves 100,000 frames per second in isolation. When integrated with the full control plane pipeline — ground station TCP ingress, deduplication, sort, downstream forwarding — the integrated system runs at 71,000 frames per second, 29% below target.

Your task is to conduct a structured performance audit of the integrated pipeline, identify the bottleneck using the tools from this module, implement a targeted fix with measurable improvement, and document the result.

---

## Pipeline Under Audit

The pipeline processes frames through four stages:

```
[TCP Ingress] → [Validator] → [Deduplicator] → [Forwarder]
```

Each stage has a measurable input and output rate. Profiling tools tell you which stage is the bottleneck and which specific function within that stage consumes the most CPU.

---

## Audit Procedure

### Phase 1: Establish a Baseline with `criterion`

Write a `criterion` benchmark for the full pipeline (not just the processor). Measure:
- Frames per second through the complete pipeline
- Per-stage latency breakdown (validator, deduplicator, forwarder separately)
- Memory allocation count per batch (using a counting allocator)

The baseline establishes the starting point. Every fix must demonstrate measurable improvement against this baseline — not just "it felt faster".

### Phase 2: CPU Profile with `flamegraph`

Run `cargo flamegraph` on the pipeline binary for 30 seconds under sustained load. Identify:
- Which stage occupies the most flamegraph width
- Which function within that stage is the hot leaf
- Whether the flamegraph shows `malloc`/`free` as significant contributors

### Phase 3: Memory Profile with a Counting Allocator

Integrate the counting allocator from Lesson 3. For each batch of 1,000 frames:
- Count total allocations per batch
- Count allocations per stage (reset/snapshot around each stage)
- Identify which stage is responsible for the most allocations

### Phase 4: Implement and Measure a Fix

Based on the profiling findings, implement the highest-impact fix. Typical candidates:
- Replace `Vec::new()` in the deduplicator with a reused buffer (`clear()` pattern)
- Replace `HashMap::new()` with `HashMap::with_capacity(batch_size)`
- Replace `format!()` in the validator with a pre-allocated error buffer
- Apply arena allocation for payloads that were missed in Module 5

Re-run the `criterion` benchmark. Document the before/after comparison.

---

## Expected Output

A workspace with:
1. A `meridian-pipeline` binary crate implementing the four-stage pipeline
2. A `benches/pipeline.rs` criterion benchmark measuring the full pipeline and each stage
3. An `audit.md` document recording:
   - Baseline criterion output (copy from terminal)
   - Flamegraph findings (which function was the hot path)
   - Allocation counts per stage per batch (from counting allocator)
   - The fix implemented
   - Post-fix criterion output showing improvement
   - `criterion`'s statistical significance output (p-value)

---

## Acceptance Criteria

| # | Criterion | Verifiable |
|---|---|---|
| 1 | `criterion` benchmark runs and produces confidence intervals for the full pipeline | Yes — `cargo bench` output |
| 2 | `black_box` applied correctly — input and output both wrapped | Yes — code review |
| 3 | Test data built outside the criterion closure, not inside | Yes — code review |
| 4 | Flamegraph generated for a ≥ 30-second profiling run | Yes — `flamegraph.svg` present |
| 5 | Allocation counts per stage documented in `audit.md` | Yes — numbers in the document |
| 6 | At least one measurable fix implemented and documented with before/after timing | Yes — `audit.md` |
| 7 | `criterion` reports p < 0.05 for the improvement (statistically significant) | Yes — criterion output in `audit.md` |

---

## Hints

<details>
<summary>Hint 1 — Criterion benchmark structure</summary>

```rust,edition2021
// benches/pipeline.rs
// use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
//
// fn bench_pipeline(c: &mut Criterion) {
//     let mut group = c.benchmark_group("pipeline");
//
//     for batch_size in [100, 500, 1000, 5000].iter() {
//         let headers = build_test_headers(*batch_size);
//
//         group.bench_with_input(
//             BenchmarkId::new("full", batch_size),
//             batch_size,
//             |b, _| {
//                 b.iter(|| {
//                     black_box(run_pipeline(black_box(&headers)))
//                 })
//             },
//         );
//     }
//     group.finish();
// }
//
// criterion_group!(benches, bench_pipeline);
// criterion_main!(benches);
```

</details>

<details>
<summary>Hint 2 — Per-stage allocation counting</summary>

```rust,edition2021
// Reset counter, run stage, snapshot:
ALLOC_COUNT.store(0, Ordering::Relaxed);
let result = run_validator(black_box(&frames));
let validator_allocs = ALLOC_COUNT.load(Ordering::Relaxed);

ALLOC_COUNT.store(0, Ordering::Relaxed);
let deduped = run_deduplicator(black_box(&result));
let dedup_allocs = ALLOC_COUNT.load(Ordering::Relaxed);

println!("validator: {validator_allocs} allocs/batch");
println!("deduplicator: {dedup_allocs} allocs/batch");
```

</details>

<details>
<summary>Hint 3 — Reusing buffers between batches</summary>

If the deduplicator creates a new `HashSet` each batch, convert it to a persistent struct:

```rust,edition2021
pub struct Deduplicator {
    seen: std::collections::HashSet<(u32, u64)>,
    unique_indices: Vec<usize>,
}

impl Deduplicator {
    pub fn new(expected_batch: usize) -> Self {
        Self {
            seen: std::collections::HashSet::with_capacity(expected_batch),
            unique_indices: Vec::with_capacity(expected_batch),
        }
    }

    pub fn process(&mut self, headers: &[(u32, u64)]) -> &[usize] {
        self.seen.clear();           // Retains allocation.
        self.unique_indices.clear(); // Retains allocation.
        for (i, &key) in headers.iter().enumerate() {
            if self.seen.insert(key) {
                self.unique_indices.push(i);
            }
        }
        &self.unique_indices
    }
}
```

</details>

<details>
<summary>Hint 4 — Flamegraph build configuration</summary>

Add to `Cargo.toml`:
```toml
[profile.release]
debug = true

[profile.profiling]
inherits = "release"
debug = true
```

Build and profile:
```bash
cargo build --profile profiling
cargo flamegraph --profile profiling --bin meridian-pipeline -- \
    --duration 30 --batch-size 1000
```

If `cargo flamegraph` is not installed: `cargo install flamegraph`. Requires `perf` on Linux or Xcode instruments on macOS.

</details>

---

## Reference Implementation

<details>
<summary>Reveal reference implementation</summary>

```rust,edition2021
// src/main.rs — pipeline implementation for profiling
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::hint::black_box;
use std::time::Instant;

// --- Counting allocator ---

struct CountingAllocator;
static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

// --- Pipeline stages ---

#[inline(never)]
fn validate(headers: &[(u32, u64, u8)]) -> Vec<(u32, u64)> {
    headers.iter()
        .filter(|&&(_, _, flags)| flags & 0x80 == 0)
        .map(|&(sat, seq, _)| (sat, seq))
        .collect()
}

pub struct Deduplicator {
    seen:    std::collections::HashSet<(u32, u64)>,
    indices: Vec<usize>,
}

impl Deduplicator {
    pub fn new(cap: usize) -> Self {
        Self {
            seen:    std::collections::HashSet::with_capacity(cap),
            indices: Vec::with_capacity(cap),
        }
    }

    #[inline(never)]
    pub fn process(&mut self, valid: &[(u32, u64)]) -> &[usize] {
        self.seen.clear();
        self.indices.clear();
        for (i, &key) in valid.iter().enumerate() {
            if self.seen.insert(key) { self.indices.push(i); }
        }
        &self.indices
    }
}

#[inline(never)]
fn forward(valid: &[(u32, u64)], unique: &[usize]) -> usize {
    unique.iter().map(|&i| valid[i].0 as usize).sum()
}

fn run_pipeline(
    headers: &[(u32, u64, u8)],
    dedup: &mut Deduplicator,
) -> usize {
    let valid = validate(headers);
    let unique = dedup.process(&valid).to_vec();
    forward(&valid, &unique)
}

fn main() {
    let batch_size = 1_000usize;
    let headers: Vec<(u32, u64, u8)> = (0..batch_size)
        .map(|i| ((i % 48) as u32, (i / 3) as u64, 0u8))
        .collect();

    let mut dedup = Deduplicator::new(batch_size);

    // Warm up.
    for _ in 0..10 { run_pipeline(&headers, &mut dedup); }

    // Measure allocations per batch.
    ALLOC_COUNT.store(0, Relaxed);
    for _ in 0..1000 {
        black_box(run_pipeline(black_box(&headers), &mut dedup));
    }
    let allocs = ALLOC_COUNT.load(Relaxed);
    println!("allocs across 1000 batches: {allocs}");
    println!("allocs per batch: {:.1}", allocs as f64 / 1000.0);

    // Throughput measurement.
    let batches = 100_000u32;
    let start = Instant::now();
    for _ in 0..batches {
        black_box(run_pipeline(black_box(&headers), &mut dedup));
    }
    let elapsed = start.elapsed();
    let fps = (batches as usize * batch_size) as f64 / elapsed.as_secs_f64();
    println!("throughput: {:.0} frames/sec", fps);
    println!("elapsed: {:.2?}", elapsed);
}
```

</details>

---

## Reflection

The audit methodology in this project — baseline, profile, identify, fix, verify — is the standard performance engineering workflow. The workflow is the skill, not the specific tools. `perf` and `flamegraph` will be replaced by better tools; the habit of measuring before and after, asserting statistical significance, and documenting findings will not.

The counting allocator CI assertion from Lesson 3 is the instrument that keeps the improvements from this module from being silently regressed six months from now. Every performance optimisation needs a regression test. For throughput, that test is a `criterion` baseline stored in `target/criterion`. For allocation-freedom, it is a `assert_eq!(allocs, 0)` assertion in the CI pipeline.

With Module 6 complete, the full Foundation track is done. Every capability the control plane relies on — async scheduling, concurrency primitives, message passing, networking, data layout, and performance measurement — is now in your toolkit. The track-specific modules (Database Internals, Data Pipelines, Data Lakes, Distributed Systems) build directly on this foundation.
