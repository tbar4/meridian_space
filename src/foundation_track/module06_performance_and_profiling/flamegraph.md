# Lesson 2 — CPU Profiling with `flamegraph` and `perf`: Finding Hot Paths

**Module:** Foundation — M06: Performance & Profiling
**Position:** Lesson 2 of 3
**Source:** Synthesized from training knowledge (`cargo flamegraph`, `perf`, `pprof` documentation)

> **Source note:** This lesson synthesizes from `cargo-flamegraph`, Linux `perf`, and `pprof` documentation. Verify specific CLI flags against your installed version of `perf` — options vary between kernel versions.

---
<!-- toc -->
## Context

`criterion` tells you that the frame deduplication function takes 12.5µs. It does not tell you *why*. Is it the `HashSet` insertions? The iterator chain? A memory allocation path? To answer that question, you need a CPU profiler — a tool that samples the program's call stack at regular intervals and shows you where time is being spent.

The flamegraph is the standard visualisation for this: a call tree where width encodes time and the call stack grows upward. The widest frames at the top are where your program actually spends its time. A deep narrow tower is a deep but fast call chain. A wide flat bar is a hot leaf function. Reading flamegraphs is a skill that takes a few profiling sessions to develop, but the insight-to-effort ratio is very high.

This lesson covers the two-tool profiling workflow for Rust on Linux: `perf` to collect samples and `cargo flamegraph` to generate the visualisation.

---

## Core Concepts

### The Profiling Workflow

CPU profiling works by sampling: the OS timer fires at regular intervals (typically 99 Hz or 999 Hz), captures the current call stack, and records the sample. After the program finishes, the accumulated stack samples are folded into a call tree and rendered as a flamegraph. Functions that appear in more samples are proportionally wider in the graph.

The standard workflow:

```bash
# 1. Build with debug info (but optimisations enabled — profile release code).
#    debug = true in [profile.release] preserves symbols without losing optimisations.
#    Add to Cargo.toml:
#    [profile.release]
#    debug = true

# 2. Install cargo-flamegraph (wraps perf or dtrace depending on platform).
cargo install flamegraph

# 3. Profile the binary.
cargo flamegraph --bin meridian-processor -- --frames 100000

# 4. Open the generated flamegraph.svg in a browser.
#    Click any frame to zoom in. Search by function name.
```

On Linux, `cargo flamegraph` uses `perf record` under the hood. On macOS, it uses `dtrace`. The output is always a `flamegraph.svg`.

### Building for Profiling: Debug Symbols in Release Mode

Profiling a debug build measures the wrong thing — debug code contains bounds checks, non-inlined functions, and other overhead that does not exist in production. Profile release builds.

But release builds strip debug symbols by default — the flamegraph shows mangled symbol addresses instead of function names. The fix: add debug info to the release profile without disabling optimisations:

```toml
# Cargo.toml
[profile.release]
debug = true      # Include debug symbols (DWARF info).
opt-level = 3     # Keep full optimisation.
# Note: debug = true increases binary size (~3-10×) but has negligible
# runtime overhead. Strip the binary before deploying to production.
```

Alternatively, use the `profiling` profile convention:

```toml
[profile.profiling]
inherits = "release"
debug = true
```

Then `cargo build --profile profiling && cargo flamegraph --profile profiling`.

### Reading a Flamegraph

A flamegraph stacks call frames vertically — the root (`main`) at the bottom, callees above. Width is proportional to the percentage of samples that included that frame in the call stack. The top-most wide frames are the actual hot spots.

Patterns to recognise:

**Wide flat bar at the top** — a leaf function consuming significant CPU. Investigate whether it can be optimised directly (algorithm, data structure choice) or eliminated (caching, avoiding the call).

**Wide bar with many narrow children** — a function that spends time distributed across many callees. No single child is dominant; the function itself may be doing overhead work.

**Deep narrow tower** — a long call chain that is individually fast. Usually indicates overhead from indirection (dynamic dispatch, many small function calls). `#[inline]` or refactoring may help.

**`[unknown]` frames** — samples from code without debug symbols (runtime, system libraries). Usually not actionable. Can be reduced by profiling with kernel symbols (`--call-graph dwarf`).

### `perf stat`: Hardware Counter Snapshot

Before generating a flamegraph, `perf stat` gives a quick diagnostic of what kind of bottleneck you have:

```bash
perf stat -e cycles,instructions,cache-misses,cache-references,branch-misses \
    ./target/release/meridian-processor --frames 100000
```

Output:
```
 Performance counter stats for './target/release/meridian-processor':

      4,521,847,032      cycles
      6,234,891,045      instructions          #    1.38  insn per cycle
         12,847,334      cache-misses          #    8.23% of all cache refs
        156,234,123      cache-references
          2,341,234      branch-misses         #    0.21% of all branches
```

**Instructions per cycle (IPC):** 1.38 is moderate. Modern CPUs can sustain 3–4 IPC. Low IPC (< 1.5) suggests the processor is stalling — often on memory latency (cache misses) or branch mispredictions.

**Cache miss rate:** 8.23% is high. Typically < 1% is good. High cache miss rates point to the data layout problems covered in Module 5 — large structs, poor locality, random access patterns.

**Branch miss rate:** 0.21% is normal. > 5% suggests unpredictable branches — sorting or using branchless comparisons may help.

### `cargo-flamegraph` in Practice

```rust,edition2021
// Example: a function with a deliberately inefficient hot path
// to demonstrate profiling workflow.

fn find_conjunctions_naive(
    altitudes: &[f64],
    norad_ids: &[u32],
    threshold_km: f64,
) -> Vec<(u32, u32)> {
    let mut alerts = Vec::new();
    let n = altitudes.len();
    for i in 0..n {
        for j in (i + 1)..n {
            // This inner loop is O(n²) — will show as wide in a flamegraph.
            // The call to f64::abs() will likely appear as a hot child.
            if (altitudes[i] - altitudes[j]).abs() < threshold_km {
                alerts.push((norad_ids[i], norad_ids[j]));
            }
        }
    }
    alerts
}

fn main() {
    // Simulate workload for profiling.
    let n = 5_000;
    let altitudes: Vec<f64> = (0..n).map(|i| 400.0 + (i as f64) * 0.1).collect();
    let norad_ids: Vec<u32> = (0..n as u32).collect();

    let alerts = find_conjunctions_naive(&altitudes, &norad_ids, 2.0);
    println!("{} conjunction alerts", alerts.len());
}
```

In a flamegraph of this code, `find_conjunctions_naive` will be wide at the top (O(n²) iterations), with the subtraction and `abs()` call visible as the actual hot operations. The outer loop iteration overhead and the `Vec::push` for matches will also appear.

The flamegraph makes it immediately obvious: the inner loop is the hot path. The fix — using a sort + linear scan instead of O(n²) comparison — is visible from the profile before reading a single line of source.

### Annotating Hot Functions with `#[inline(never)]`

By default, the compiler inlines small functions, which is good for performance but bad for profiling — inlined calls disappear into their callers in the flamegraph. For functions you specifically want to measure in isolation:

```rust,edition2021
// Prevents inlining — this function will appear as a distinct frame in the flamegraph.
// Remove before production use if inlining is desired for performance.
#[inline(never)]
fn compute_altitude_delta(a: f64, b: f64) -> f64 {
    (a - b).abs()
}

fn main() {
    // In a flamegraph, compute_altitude_delta will appear as its own frame,
    // making it easy to see exactly how much time the subtraction + abs costs.
    let result = compute_altitude_delta(410.0, 408.5);
    println!("{}", result);
}
```

Use `#[inline(never)]` temporarily during profiling investigations. Remove it afterward — the compiler's inlining decisions are generally correct for production code.

---

## Code Examples

### A Profiling-Instrumented Processor Binary

The entry point for profiling runs a realistic workload of sufficient duration for the sampler to collect meaningful data. Too short (< 1 second) and there are too few samples for a reliable flamegraph.

```rust,edition2021
use std::collections::HashSet;
use std::hint::black_box;
use std::time::Instant;

fn build_test_data(n: usize) -> (Vec<u64>, Vec<(u32, u64)>) {
    let timestamps: Vec<u64> = (0..n as u64).rev().collect();
    let headers: Vec<(u32, u64)> = (0..n)
        .map(|i| ((i % 48) as u32, (i / 3) as u64))
        .collect();
    (timestamps, headers)
}

#[inline(never)] // Visible as its own frame in flamegraph
fn dedup_pass(headers: &[(u32, u64)]) -> Vec<usize> {
    let mut seen = HashSet::with_capacity(headers.len());
    headers.iter().enumerate()
        .filter_map(|(i, &(sat, seq))| {
            if seen.insert((sat, seq)) { Some(i) } else { None }
        })
        .collect()
}

#[inline(never)] // Visible as its own frame in flamegraph
fn sort_pass(indices: &mut Vec<usize>, timestamps: &[u64]) {
    indices.sort_unstable_by_key(|&i| timestamps[i]);
}

fn process_batch(timestamps: &[u64], headers: &[(u32, u64)]) -> usize {
    let mut indices = dedup_pass(headers);
    sort_pass(&mut indices, timestamps);
    indices.len()
}

fn main() {
    // Run enough iterations for perf to collect ~1000+ samples.
    // At 99 Hz sampling, we need ~10 seconds of CPU time.
    let (timestamps, headers) = build_test_data(10_000);
    let batches = 50_000;

    let start = Instant::now();
    let mut total = 0usize;
    for _ in 0..batches {
        total += black_box(process_batch(
            black_box(&timestamps),
            black_box(&headers),
        ));
    }
    let elapsed = start.elapsed();

    println!("processed {} batches, {} unique frames", batches, total);
    println!("throughput: {:.0} batches/sec", batches as f64 / elapsed.as_secs_f64());
    println!("wall time:  {:.2?}", elapsed);
}
```

The `#[inline(never)]` attributes on `dedup_pass` and `sort_pass` ensure they appear as distinct frames in the flamegraph. The `black_box` calls prevent dead-code elimination from interfering with the profiling workload. The loop runs long enough to collect statistically meaningful samples.

---

## Key Takeaways

- Profile release builds with debug symbols (`debug = true` in `[profile.release]`). Profiling debug builds measures overhead that does not exist in production.

- `perf stat` provides a hardware counter snapshot before you generate a flamegraph. High cache miss rate (> 5%) points to data layout issues; low IPC (< 1.5) suggests the processor is stalling on memory; high branch miss rate suggests unpredictable conditionals.

- In a flamegraph, width encodes time. Wide frames at the top are hot leaf functions — the actual bottleneck. Wide frames with narrow children indicate distributed overhead. Deep narrow towers indicate fast call chains, not hot spots.

- `#[inline(never)]` temporarily prevents a function from being inlined so it appears as a distinct frame in the profiler. Remove it after the investigation — inlining is correct for production code.

- A profiling session should last at least 5–10 seconds of CPU time for reliable sample counts at 99 Hz. Use a workload that resembles production access patterns at production input sizes.

---

{{#quiz lesson-02-quiz.toml}}
