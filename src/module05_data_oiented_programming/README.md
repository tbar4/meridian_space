# Module 05 — Data-Oriented Design in Rust

**Track:** Foundation — Mission Control Platform  
**Position:** Module 5 of 6  
**Source material:** *Rust for Rustaceans* — Jon Gjengset, Chapters 2, 9  
**Quiz pass threshold:** 70% on all three lessons to unlock the project

---
<!-- toc -->
## Mission Context

The Meridian telemetry processor runs at 62,000 frames per second. The conjunction avoidance pipeline requires 100,000. The gap is not a missing algorithm or a suboptimal data structure — it is allocator pressure and cache waste, both caused by data layout decisions made when defining types. Each frame allocates a `Vec<u8>` on the global heap. Each deduplication pass loads 2.4× more data than the deduplication logic uses.

Data-oriented design is a discipline for making data layout decisions that align with CPU hardware realities: cache lines are 64 bytes, cache misses cost 100–300 cycles, and SIMD instructions operate on contiguous uniform-type data. The three techniques in this module — cache-optimal struct layout, SoA field separation, and arena allocation — directly address the two profiling findings above.

---

## What You Will Learn

By the end of this module you will be able to:

- Explain how field alignment and padding inflate struct sizes, use `repr` attributes to control layout, and write `const` assertions to lock in size expectations at compile time
- Identify false sharing between concurrent tasks, apply `repr(align(64))` with padding to isolate per-thread data to separate cache lines, and separate hot fields from cold fields in structs used in high-volume collections
- Explain when SoA layout outperforms AoS (field-subset sequential operations) and when AoS outperforms SoA (per-entity random access), implement an `OrbitalCatalog` using field grouping, and transition from AoS to SoA incrementally without a full rewrite
- Implement a bump/arena allocator for same-lifetime batch allocations, contrast its allocation cost with the global allocator, use thread-local arenas for zero-contention concurrent allocation, and identify when arena allocation is inappropriate (mixed lifetimes, individual deallocation)

---

## Lessons

### Lesson 1 — Cache-Friendly Data Layouts: Struct Layout, Padding, and Cache Line Alignment

Covers alignment and padding mechanics, `repr(C)` vs `repr(Rust)` vs `repr(packed)` vs `repr(align(n))`, false sharing between concurrent tasks, `repr(align(64))` for per-thread counter isolation, and hot/cold field separation. Grounded in *Rust for Rustaceans*, Chapter 2.

**Key question this lesson answers:** How does field order affect struct size, what causes false sharing between concurrent tasks, and how do you isolate hot data from cold data?

→ `lesson-01-cache-friendly-layouts.md` / `lesson-01-quiz.toml`

---

### Lesson 2 — Struct-of-Arrays vs Array-of-Structs: When Each Wins

Covers the AoS and SoA layout patterns, the cache utilisation argument for each, the conditions that favour SoA (field-subset sequential scans, SIMD, large N), the conditions that favour AoS (per-entity random access, all-field operations), the hybrid field-grouping pattern, and incremental AoS-to-SoA transition via a companion index.

**Key question this lesson answers:** When does splitting fields into separate vectors improve performance, and when does it hurt?

→ `lesson-02-soa-vs-aos.md` / `lesson-02-quiz.toml`

---

### Lesson 3 — Arena Allocation: Bump Allocators for High-Throughput Telemetry Processing

Covers the global allocator's cost for high-frequency short-lived allocations, the bump allocator pattern (O(1) alloc, O(1) epoch free), the lifetime constraint, thread-local arenas for zero-contention concurrent allocation, the `bumpalo` crate interface, and the workloads where arena allocation is inappropriate.

**Key question this lesson answers:** When is the global allocator the bottleneck, and how does a bump allocator eliminate that overhead for same-lifetime batch objects?

→ `lesson-03-arena-allocation.md` / `lesson-03-quiz.toml`

---

## Capstone Project — High-Throughput Telemetry Packet Processor

Rebuild the Meridian telemetry processor core to achieve ≥100,000 frames/sec using all three techniques: a 24-byte `FrameHeader` with const size assertion, SoA separation of headers from arena-allocated payloads, bump arena for batch payload allocation with O(1) epoch reset, and SoA-based deduplication operating only on the hot header array.

Acceptance is against 7 verifiable criteria including compile-time size assertions, no per-frame heap allocations, correct arena reset, and measured throughput.

→ `project-telemetry-processor.md`

---

## Prerequisites

Modules 1–4 must be complete. Module 2 (Concurrency Primitives) introduced atomic operations and the false sharing problem — Lesson 1 of this module extends that with the `repr(align(64))` solution. Module 5's content stands alone otherwise; it does not build on the networking or message-passing material from Modules 3–4.

## What Comes Next

**Module 6 — Performance and Profiling** gives you the measurement tools to validate the optimisations introduced here: `criterion` for reliable microbenchmarks, `flamegraph` and `perf` for identifying hot paths, and heap profiling for measuring allocator pressure. You will profile the processor built in this module's project and verify the improvement against the baseline.
