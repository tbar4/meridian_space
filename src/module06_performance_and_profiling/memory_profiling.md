# Lesson 3 — Memory Profiling: Heap Allocation Tracking and Reducing Allocator Pressure

**Module:** Foundation — M06: Performance & Profiling
**Position:** Lesson 3 of 3
**Source:** Synthesized from training knowledge (`dhat`, `heaptrack`, `jemalloc` statistics, custom allocator wrappers)

> **Source note:** This lesson synthesizes from `dhat` (Valgrind/DHAT profiler), `heaptrack` documentation, and allocator counting patterns. Verify `dhat-rs` API against the current crate version.

---
<!-- toc -->
---
## Context

The CPU flamegraph from Lesson 2 shows the telemetry processor spending 18% of time in `malloc` and `free`. The criterion benchmark from Lesson 1 confirms: 12.5µs per 1000-frame batch, 2.3µs of which is allocator overhead. The fix from Module 5 — arena allocation — eliminates this. But before implementing it, you need to know: exactly how many allocations happen per batch? Which call sites are responsible? Are there unexpected allocations from library code that you assumed was allocation-free?

Memory profiling answers these questions. Unlike CPU profiling (which samples stochastically), allocation profiling intercepts every `alloc` and `dealloc` call — giving you exact counts, sizes, and call sites. The tools: `dhat` for lightweight in-process counting, `heaptrack` for comprehensive heap timeline recording, and a custom counting allocator for targeted measurements in CI.

---

## Core Concepts

### The Allocation Cost Model

Every `Vec::new()`, `Box::new()`, `String::from()`, and collection growth hits the global allocator. The actual cost depends on the allocator (jemalloc is faster than the system allocator for concurrent workloads), the allocation size (small allocations have higher per-byte overhead), and contention (the global allocator serialises concurrent allocations internally).

Profiling allocation patterns reveals three categories of allocatable objects:

**Long-lived allocations** — startup config, connection state, per-session data structures. These are unavoidable and not a throughput problem.

**Per-batch allocations** — temporary buffers, work vectors, accumulators that are created and freed within one processing epoch. These are the target of arena allocation — eliminate them with pre-allocation.

**Unexpected allocations** — library calls that allocate internally even though the API looks allocation-free. `format!()`, `HashMap::new()`, `Vec::collect()` when the iterator doesn't know its size. These show up in memory profiles and are often surprising.

### Counting Allocations: The Simplest Approach

Before reaching for a full memory profiler, a counting allocator wrapper tells you exactly how many allocations occur in a specific code path. This works in any environment and imposes very low overhead:

```rust,edition2021
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

/// Wraps the system allocator and counts every alloc/dealloc.
struct CountingAllocator;

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static DEALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn snapshot() -> (u64, u64, u64) {
    (
        ALLOC_COUNT.load(Ordering::Relaxed),
        DEALLOC_COUNT.load(Ordering::Relaxed),
        ALLOC_BYTES.load(Ordering::Relaxed),
    )
}

fn reset_counters() {
    ALLOC_COUNT.store(0, Ordering::Relaxed);
    DEALLOC_COUNT.store(0, Ordering::Relaxed);
    ALLOC_BYTES.store(0, Ordering::Relaxed);
}

// --- Application code under test ---

fn process_frames(frames: &[Vec<u8>]) -> usize {
    // Allocates a HashSet internally.
    let mut seen = std::collections::HashSet::new();
    frames.iter().filter(|f| seen.insert(f.as_ptr())).count()
}

fn main() {
    let frames: Vec<Vec<u8>> = (0..100).map(|i| vec![i as u8; 256]).collect();

    // Reset — we only want to count allocations from process_frames.
    reset_counters();

    let result = process_frames(&frames);

    let (allocs, deallocs, bytes) = snapshot();
    println!("process_frames({}) result: {}", frames.len(), result);
    println!("  allocations:   {allocs}");
    println!("  deallocations: {deallocs}");
    println!("  bytes:         {bytes}");
}
```

The output reveals exactly how many times the global allocator was called inside `process_frames`. If the count is non-zero when it should be zero (the function is supposed to be allocation-free), you have a hidden allocation to hunt down.

### Common Hidden Allocation Sources

**`HashSet::new()` and `HashMap::new()`** — these start empty (no allocation) but trigger an allocation on the first insert. `HashSet::with_capacity(n)` pre-allocates for `n` elements, avoiding the first realloc. Using `with_capacity` eliminates the grow-and-rehash allocation that occurs when the initial capacity is exceeded.

**`Vec::collect()` without size hint** — if the iterator does not implement `ExactSizeIterator`, the `Vec` starts with a small capacity and grows (allocating) as elements arrive. Call `.collect::<Vec<_>>()` only when you know the iterator is small or provide a size hint via `.size_hint()`.

**`format!()` and string operations** — every `format!` call allocates a `String`. In hot paths, prefer writing to a pre-allocated `String` with `write!` or `push_str`, or avoid `String` entirely in favour of a stack buffer.

**`Arc::clone()` is not free** — cloning an `Arc` does not allocate, but `Arc::new()` does. In a hot path, pre-create the `Arc` at batch setup time rather than per-frame.

**Iterator adapters that buffer** — `.sorted()` from `itertools` allocates a `Vec`. `.flat_map()` with iterators that have non-trivial state may allocate. Check whether the adapter is allocation-free before using it in a hot path.

### `dhat`: In-Process Heap Profiling

`dhat` (from Valgrind, with a Rust port via the `dhat` crate) instruments every allocation with a call-site stack trace. It produces a profile that shows, for each allocation site, the total bytes allocated, the peak live bytes, and the number of calls:

```toml
# Cargo.toml
[dependencies]
dhat = { version = "0.3", optional = true }

[features]
dhat-heap = ["dhat"]
```

```rust,edition2021
// In main.rs — only active when the dhat-heap feature is enabled.
// cfg gate prevents any overhead in production builds.
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    // ... run workload ...
    println!("dhat profile written on drop of _profiler");
}
```

Run with: `cargo run --features dhat-heap`. At program exit, dhat writes `dhat-heap.json`. View it at https://nnethercote.github.io/dh_view/dh_view.html.

The profile shows total bytes allocated per call site — letting you immediately identify which function is responsible for most allocations, even if that function is inside a library you did not write.

### Reducing Allocator Pressure: Patterns

**Pre-allocate with `with_capacity`:**

```rust,edition2021
fn process_batch_optimised(n: usize) -> Vec<usize> {
    // Pre-allocate with known capacity — no reallocation on push.
    let mut result = Vec::with_capacity(n);
    let mut seen = std::collections::HashSet::with_capacity(n);

    for i in 0..n {
        if seen.insert(i % (n / 2)) {  // ~50% are unique
            result.push(i);
        }
    }
    result
}

fn main() {
    let batch = process_batch_optimised(10_000);
    println!("{} unique items", batch.len());
}
```

**Reuse allocations across calls with `clear()` instead of dropping and reallocating:**

```rust,edition2021
struct FrameProcessor {
    // Persistent buffers — allocated once, reused every batch.
    seen:    std::collections::HashSet<(u32, u64)>,
    indices: Vec<usize>,
}

impl FrameProcessor {
    fn new(expected_batch_size: usize) -> Self {
        Self {
            seen:    std::collections::HashSet::with_capacity(expected_batch_size),
            indices: Vec::with_capacity(expected_batch_size),
        }
    }

    fn process(&mut self, headers: &[(u32, u64)]) -> &[usize] {
        // clear() retains the allocation — no new malloc per batch.
        self.seen.clear();
        self.indices.clear();

        for (i, &(sat, seq)) in headers.iter().enumerate() {
            if self.seen.insert((sat, seq)) {
                self.indices.push(i);
            }
        }
        &self.indices
    }
}

fn main() {
    let mut processor = FrameProcessor::new(1000);
    let headers: Vec<(u32, u64)> = (0..1000)
        .map(|i| ((i % 48) as u32, (i / 3) as u64))
        .collect();

    for batch_num in 0..5 {
        let unique = processor.process(&headers);
        println!("batch {batch_num}: {} unique frames", unique.len());
    }
}
```

The `FrameProcessor` struct holds the `HashSet` and `Vec` across batch calls. Each batch calls `clear()` — which sets the length to zero but retains the allocated capacity. After the first batch warms up the allocation, subsequent batches make zero allocator calls for these data structures.

---

## Code Examples

### Measuring Allocations Per Batch in CI

Embedding an allocation count assertion in CI ensures that future refactors do not accidentally reintroduce per-frame allocations:

```rust,edition2021
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

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

// --- Frame processor under test ---

struct Processor {
    seen:    std::collections::HashSet<(u32, u64)>,
    indices: Vec<usize>,
}

impl Processor {
    fn new(cap: usize) -> Self {
        Self {
            seen:    std::collections::HashSet::with_capacity(cap),
            indices: Vec::with_capacity(cap),
        }
    }

    fn process_batch(&mut self, headers: &[(u32, u64)]) -> usize {
        self.seen.clear();
        self.indices.clear();
        for (i, &key) in headers.iter().enumerate() {
            if self.seen.insert(key) {
                self.indices.push(i);
            }
        }
        self.indices.len()
    }
}

fn main() {
    let headers: Vec<(u32, u64)> = (0..1000)
        .map(|i| ((i % 48) as u32, (i / 3) as u64))
        .collect();

    let mut processor = Processor::new(1000);

    // Warm up — first batch may allocate as HashSet grows.
    processor.process_batch(&headers);

    // Reset counter — subsequent batches should be allocation-free.
    ALLOC_COUNT.store(0, Relaxed);

    // Run 100 batches.
    for _ in 0..100 {
        std::hint::black_box(processor.process_batch(std::hint::black_box(&headers)));
    }

    let allocs = ALLOC_COUNT.load(Relaxed);
    println!("allocations across 100 batches: {allocs}");

    // In CI: assert!(allocs == 0, "unexpected allocations in hot path: {allocs}");
    if allocs == 0 {
        println!("PASS: hot path is allocation-free after warm-up");
    } else {
        println!("WARN: {allocs} unexpected allocations detected");
    }
}
```

The pattern: warm up once (let pre-allocated capacity fill), reset the counter, then assert zero allocations across subsequent batches. This assertion in CI will fail the build if any refactor introduces a hidden allocation.

---

## Key Takeaways

- Memory profiling reveals the call sites responsible for allocations, total bytes allocated per site, and peak live bytes. `dhat` (via the `dhat` crate) provides this with minimal production overhead when gated behind a feature flag.

- A counting allocator wrapper (`#[global_allocator]` with atomic counters) is the fastest way to count allocations in a specific code path. Use it to establish a baseline, then assert zero allocations in CI for hot paths.

- `HashSet::with_capacity(n)` and `Vec::with_capacity(n)` pre-allocate to avoid grow-and-rehash allocations. If you know the expected size, always use `with_capacity`.

- `clear()` retains the underlying allocation. Use it to reuse `Vec` and `HashMap` buffers across batches rather than dropping and reallocating each time.

- Common hidden allocation sources: `format!()`, `HashMap::new()` without capacity, `Vec::collect()` on unsized iterators, iterator adapters that buffer internally (`.sorted()`, `.chunks()` on non-slice iterators), and `Arc::new()` in a per-frame code path.

- Profile allocations before optimising. The counting allocator tells you *how many* allocations happen. The flamegraph from Lesson 2 tells you *where* time is spent. Together they give a complete picture: is the bottleneck the allocation count, the allocator latency, or the subsequent memory access pattern?

---

{{#quiz lesson-03-quiz.toml}}
