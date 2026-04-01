# Module 06 — Performance & Profiling

**Track:** Foundation — Mission Control Platform
**Position:** Module 6 of 6 (Foundation track complete)
**Source material:** *Rust for Rustaceans* — Jon Gjengset, Chapter 6; `criterion`, `cargo-flamegraph`, `perf`, `dhat` documentation
**Quiz pass threshold:** 70% on all three lessons to unlock the project

---
<!-- toc -->
---
## Mission Context

The Module 5 telemetry processor achieves 100,000 frames per second in isolation. The integrated control plane pipeline runs at 71,000. The 29% gap is not in the algorithm — it is in measurement blind spots: unmeasured allocations, unverified assumptions about what the compiler optimises away, and code paths that look fast but are not.

Performance engineering without measurement is optimism. This module provides the measurement toolkit: `criterion` for reliable benchmarks, `flamegraph` and `perf` for identifying hot paths, and allocation counting for detecting hidden heap overhead. The project combines all three into a structured audit that turns a performance gap into a documented, measured, verified improvement.

---

## What You Will Learn

By the end of this module you will be able to:

- Identify the three failure modes of naive `Instant::now()` benchmarks: dead-code elimination, constant folding, and I/O overhead masking the function under test
- Apply `std::hint::black_box` correctly to both inputs and outputs to prevent compiler optimisations from invalidating benchmark results
- Write `criterion` benchmarks with proper setup/measurement separation, interpret confidence intervals and p-values, and run parameterised benchmarks across input sizes
- Configure the release profile with debug symbols for profiling, generate flamegraphs with `cargo flamegraph`, and identify hot paths from flamegraph visual patterns
- Read `perf stat` output to diagnose whether a workload is compute-bound, memory-bound, or branch-prediction-bound before generating a flamegraph
- Use a `#[global_allocator]` counting wrapper to count allocations in a specific code path, embed zero-allocation assertions in CI, and eliminate common hidden allocation sources (`HashMap::new()`, `Vec::collect()`, `format!()`)

---

## Lessons

### Lesson 1 — Benchmarking with `criterion`: Writing Reliable Microbenchmarks

Covers the three failure modes of naive timing loops, `std::hint::black_box` placement for both input and output, `criterion` API and confidence interval interpretation, setup/measurement separation, benchmarking at realistic input sizes, and reading the statistical significance output.

**Key question this lesson answers:** How do you know your benchmark is measuring what you think it is, and how do you distinguish a real performance change from measurement noise?

→ `lesson-01-benchmarking.md` / `lesson-01-quiz.toml`

---

### Lesson 2 — CPU Profiling with `flamegraph` and `perf`: Finding Hot Paths

Covers the sampling profiler model, configuring release builds with debug symbols for profiling, `perf stat` hardware counter diagnosis (IPC, cache miss rate, branch miss rate), `cargo flamegraph` workflow, reading flamegraph visual patterns (wide flat bars, deep towers, distributed overhead), and `#[inline(never)]` for profiling visibility.

**Key question this lesson answers:** Which function is consuming the most CPU time, and how do you distinguish a compute-bound bottleneck from a memory-bound one?

→ `lesson-02-flamegraph.md` / `lesson-02-quiz.toml`

---

### Lesson 3 — Memory Profiling: Heap Allocation Tracking and Reducing Allocator Pressure

Covers the allocation cost model, `#[global_allocator]` counting wrappers for exact per-path allocation counts, `HashMap::with_capacity` and `Vec::with_capacity` pre-allocation, `clear()` for buffer reuse across batches, `dhat` for call-site-attributed heap profiling, and CI-embedded zero-allocation assertions.

**Key question this lesson answers:** How many allocations happen in the hot path, which call sites are responsible, and how do you make that a CI assertion rather than a one-time finding?

→ `lesson-03-memory-profiling.md` / `lesson-03-quiz.toml`

---

## Capstone Project — Meridian Control Plane Performance Audit

Apply the full three-phase audit workflow to the integrated telemetry pipeline: establish a `criterion` baseline, generate a `flamegraph` to identify the hot path, use a counting allocator to quantify per-stage allocation overhead, implement the highest-impact fix, and verify the improvement is statistically significant (p < 0.05). Document findings in `audit.md`.

Acceptance is against 7 verifiable criteria including correct `criterion` usage, flamegraph generation, per-stage allocation counts, a documented fix, and a p < 0.05 improvement.

→ `project-performance-audit.md`

---

## Prerequisites

Modules 1–5 must be complete. Module 5 (Data-Oriented Design) established the optimisations being measured here — this module gives you the tools to verify that those optimisations actually work and to prevent future regressions. Module 2 (Concurrency Primitives) introduced atomic operations, which are used by the counting allocator in Lesson 3.

## Foundation Track Complete

With Module 6 complete, the Foundation track is done. The six modules cover the complete toolset for building Meridian's control plane in Rust: async task scheduling, concurrency primitives, message-passing architectures, network I/O, data-oriented design, and performance measurement. The four specialisation tracks — Database Internals, Data Pipelines, Data Lakes, and Distributed Systems — are now unlocked and can be taken in any order.
