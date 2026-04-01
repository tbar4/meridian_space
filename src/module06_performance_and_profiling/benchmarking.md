# Lesson 1 — Benchmarking with `criterion`: Writing Reliable Microbenchmarks

**Module:** Foundation — M06: Performance & Profiling
**Position:** Lesson 1 of 3
**Source:** *Rust for Rustaceans* — Jon Gjengset, Chapter 6

---
<!-- toc -->
---
## Context

The Module 5 project processor targets 100,000 frames per second. You have a number — but how confident are you in it? The benchmark loop used `Instant::now()` / `elapsed()` around a single iteration. That measurement is subject to three failure modes documented in *Rust for Rustaceans* Ch. 6: performance variance between runs (caused by CPU temperature, OS scheduler interrupts, memory layout), compiler optimisation eliminating the code under test entirely, and I/O overhead masking the actual function cost. A timing loop that contains a `println!` is usually measuring the speed of terminal output, not your function.

The `criterion` crate addresses all three. It runs each benchmark hundreds of times, applies statistical analysis to separate signal from noise, detects and reports outliers, and generates comparison reports that tell you whether a change is a real regression or measurement noise. When Meridian's CI pipeline regresses the frame processor throughput by 15%, `criterion` is how you prove the regression is real, quantify its size, and track it to the specific commit.

*Source: Rust for Rustaceans, Chapter 6 (Gjengset)*

---

## Core Concepts

### Why `Instant::now()` Loops Are Not Enough

Consider this naive benchmark:

```rust,edition2021
fn my_function(data: &[u32]) -> u64 {
    data.iter().map(|&x| x as u64).sum()
}

fn main() {
    let data: Vec<u32> = (0..1_000).collect();
    let start = std::time::Instant::now();
    for _ in 0..10_000 {
        let _ = my_function(&data);
    }
    println!("took {:?}", start.elapsed());
}
```

Two problems. First, the compiler may eliminate `my_function` entirely — the result `_` is discarded, so nothing in the code requires the computation to happen (*Rust for Rustaceans*, Ch. 6). In release mode, the loop body may compile to nothing. Second, a single run on a loaded machine is noise, not signal. CPU clock scaling, branch predictor warmup, and OS scheduler preemption all add variance. A function that takes 50µs may measure anywhere from 40µs to 200µs depending on external conditions.

### `criterion` Basics

Add criterion to `Cargo.toml`:

```toml
[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }

[[bench]]
name = "frame_processor"
harness = false
```

A criterion benchmark in `benches/frame_processor.rs`:

```rust,edition2021
// Note: criterion is a dev-dependency — not available in the Playground.
// This demonstrates the API. In production add criterion = "0.5" to Cargo.toml.

// use criterion::{black_box, criterion_group, criterion_main, Criterion};

// fn bench_deduplication(c: &mut Criterion) {
//     let headers: Vec<u64> = (0..1000).collect();
//     c.bench_function("dedup_1000", |b| {
//         b.iter(|| {
//             // black_box prevents the compiler from optimising away the input
//             // or treating the result as dead code.
//             black_box(deduplicate(black_box(&headers)))
//         })
//     });
// }
//
// criterion_group!(benches, bench_deduplication);
// criterion_main!(benches);

// Illustrating the structure with std::hint::black_box instead:
fn deduplicate(headers: &[u64]) -> usize {
    let mut seen = std::collections::HashSet::new();
    headers.iter().filter(|&&h| seen.insert(h)).count()
}

fn main() {
    let headers: Vec<u64> = (0..1000).collect();

    // Warm up
    for _ in 0..100 {
        std::hint::black_box(deduplicate(std::hint::black_box(&headers)));
    }

    // Measure
    let iterations = 10_000;
    let start = std::time::Instant::now();
    for _ in 0..iterations {
        std::hint::black_box(deduplicate(std::hint::black_box(&headers)));
    }
    let elapsed = start.elapsed();
    println!("deduplicate(1000): {:.2?} per iteration",
        elapsed / iterations);
}
```

### `black_box`: Preventing Dead-Code Elimination

`std::hint::black_box` (or `criterion::black_box`) is the key primitive for correct benchmarks. It is an identity function that tells the compiler: assume this value is used in some arbitrary way (*Rust for Rustaceans*, Ch. 6). This prevents two failure modes:

**Eliminating dead computation:** if the benchmark discards the result with `let _ = expensive()`, the compiler may eliminate the call. `black_box(expensive())` forces the computation to occur because the compiler must assume `black_box` uses its argument.

**Constant-folding inputs:** if the input to a benchmark is a compile-time constant, the compiler may pre-compute the result at compile time. `black_box(input)` forces the compiler to treat the input as runtime-unknown.

```rust,edition2021
fn sum_slice(data: &[u32]) -> u64 {
    data.iter().map(|&x| x as u64).sum()
}

fn main() {
    let data: Vec<u32> = (0..1_000).collect();

    // WRONG: compiler may eliminate the call — result discarded, input known.
    {
        let start = std::time::Instant::now();
        for _ in 0..10_000 {
            let _ = sum_slice(&data);
        }
        println!("(likely wrong) took {:?}", start.elapsed());
    }

    // CORRECT: black_box prevents dead-code elimination and constant folding.
    {
        let start = std::time::Instant::now();
        for _ in 0..10_000 {
            std::hint::black_box(sum_slice(std::hint::black_box(&data)));
        }
        println!("(correct) took {:?}", start.elapsed());
    }
}
```

Note the placement: `black_box` on the **input** prevents constant folding (the compiler must treat the slice as runtime-unknown). `black_box` on the **output** prevents dead-code elimination (the compiler must assume the result is used).

### Benchmark Structure Best Practices

**Separate setup from measurement.** Criterion's `b.iter(|| { ... })` closure is the measured unit. Anything outside it is setup and runs once. Constructing test data inside the measured closure inflates the result with allocation cost.

```rust,edition2021
// Illustrating the pattern with manual timing:
fn build_test_headers(n: usize) -> Vec<u64> {
    // This is setup — not what we want to measure.
    (0..n as u64).collect()
}

fn deduplicate_headers(headers: &[u64]) -> usize {
    let mut seen = std::collections::HashSet::new();
    headers.iter().filter(|&&h| seen.insert(h)).count()
}

fn bench_with_setup() {
    // Build test data ONCE — not inside the measured loop.
    let headers = build_test_headers(1000);

    let iterations = 100_000u32;
    let start = std::time::Instant::now();
    for _ in 0..iterations {
        // Only the function under test is measured.
        std::hint::black_box(deduplicate_headers(std::hint::black_box(&headers)));
    }
    let elapsed = start.elapsed();
    println!("deduplicate(1000): {:.2?}/iter", elapsed / iterations);
}

fn main() {
    bench_with_setup();
}
```

**Benchmark at realistic input sizes.** A function that is O(n log n) may be cache-bound at n=100 and compute-bound at n=100,000. Benchmark at the sizes you actually use in production. For Meridian's conjunction screen, that is 50,000 objects — not 100.

**Use `criterion`'s input size parameter for scaling analysis.** `BenchmarkGroup` lets you benchmark the same function at multiple input sizes and plot throughput vs. size. The slope of that plot tells you whether your function is cache-bound (throughput drops sharply above L2 size) or compute-bound (throughput scales smoothly).

### Interpreting Criterion Output

`cargo bench` produces output like:

```
dedup_1000              time:   [12.453 µs 12.501 µs 12.554 µs]
                        change: [-2.1431% -1.6789% -1.1920%] (p=0.00 < 0.05)
                        Performance has improved.
```

The three numbers are the lower bound, estimate, and upper bound of the 95% confidence interval. If you see a wide interval (e.g., `[5 µs, 50 µs, 200 µs]`), measurement variance is high — run on a quieter machine, increase iteration count, or use `--profile-time` for more samples.

The `change` line compares against the previous run (stored in `target/criterion`). A p-value below 0.05 means the change is statistically significant with 95% confidence. Changes with p > 0.05 are likely noise.

---

## Code Examples

### Benchmarking the Telemetry Processor with Parameterised Input Sizes

This example uses `black_box` correctly and varies input size to understand the performance profile across the range of realistic batch sizes.

```rust,edition2021
use std::collections::HashSet;
use std::hint::black_box;
use std::time::{Duration, Instant};

fn deduplicate(headers: &[(u32, u64)]) -> Vec<usize> {
    let mut seen = HashSet::with_capacity(headers.len());
    headers.iter().enumerate()
        .filter_map(|(i, &(sat, seq))| {
            if seen.insert((sat, seq)) { Some(i) } else { None }
        })
        .collect()
}

fn sort_by_timestamp(indices: &mut Vec<usize>, timestamps: &[u64]) {
    indices.sort_unstable_by_key(|&i| timestamps[i]);
}

/// Run `iterations` iterations, return median per-iteration duration.
fn time_fn<F: Fn()>(f: F, iterations: u32) -> Duration {
    // Warm up — let branch predictor and instruction cache settle.
    for _ in 0..10 { f(); }

    let start = Instant::now();
    for _ in 0..iterations { f(); }
    start.elapsed() / iterations
}

fn main() {
    println!("{:<10} {:>15} {:>15} {:>15}",
        "n_frames", "dedup (µs)", "sort (µs)", "total (µs)");
    println!("{}", "-".repeat(55));

    for &n in &[100usize, 500, 1_000, 5_000, 10_000] {
        // Build test data once — not in the measured loop.
        let headers: Vec<(u32, u64)> = (0..n)
            .map(|i| ((i % 48) as u32, (i / 3) as u64)) // ~33% duplicates
            .collect();
        let timestamps: Vec<u64> = (0..n).map(|i| (n - i) as u64).collect();

        let dedup_time = time_fn(|| {
            black_box(deduplicate(black_box(&headers)));
        }, 10_000);

        let sort_time = time_fn(|| {
            let mut indices = (0..n).collect::<Vec<_>>();
            sort_by_timestamp(black_box(&mut indices), black_box(&timestamps));
            black_box(indices);
        }, 10_000);

        println!("{:<10} {:>15.2} {:>15.2} {:>15.2}",
            n,
            dedup_time.as_secs_f64() * 1e6,
            sort_time.as_secs_f64() * 1e6,
            (dedup_time + sort_time).as_secs_f64() * 1e6,
        );
    }
}
```

The slope of the dedup time as n grows reveals whether the function is O(n) (linear slope on a linear plot) or showing cache effects (steeper slope beyond a threshold). If dedup time grows faster than linearly above n=1000, the `HashSet` working set has exceeded L1 cache and you are paying for L2/L3 misses.

---

## Key Takeaways

- `Instant::now()` around a single-pass loop is not a reliable benchmark. Performance variance between runs, compiler dead-code elimination, and I/O in the loop can all produce completely wrong numbers (*Rust for Rustaceans*, Ch. 6).

- `std::hint::black_box` (or `criterion::black_box`) prevents the compiler from eliminating benchmark code as dead. Apply it to both the **input** (prevent constant folding) and the **output** (prevent dead-code elimination).

- `criterion` runs each benchmark the statistically appropriate number of times, computes confidence intervals, detects outliers, and reports whether changes between runs are statistically significant. Use `p < 0.05` as the threshold for treating a change as real.

- Separate setup from measurement. Build test data outside the measured closure. Benchmark at realistic production input sizes, not toy sizes. Use parameterised input to understand performance scaling behaviour.

- A 95% confidence interval that is narrow (< 5% spread) indicates a reliable measurement. A wide interval indicates high variance — run on a quieter machine or use `cargo bench -- --sample-size 200` for more samples.

---

{{#quiz lesson-01-quiz.toml}}
