# Lesson 1 — Cache-Friendly Data Layouts: Struct Layout, Padding, and Cache Line Alignment

**Module:** Foundation — M05: Data-Oriented Design in Rust  
**Position:** Lesson 1 of 3  
**Source:** *Rust for Rustaceans* — Jon Gjengset, Chapter 2

---
<!-- toc -->
---
## Context

The Meridian telemetry processor receives 100,000 frames per second at peak load across 48 uplinks. Each frame passes through validation, deduplication, and routing — operations that read specific fields from a `TelemetryFrame` struct on every iteration. At that throughput, the cost of a CPU cache miss — roughly 100–300 clock cycles to fetch from RAM, compared to 4 cycles for an L1 cache hit — is the difference between keeping up and falling behind.

CPU cache performance is not something you can bolt on after profiling shows a bottleneck. It is determined by the decisions you make when you define your data types. How fields are ordered. How large structs are. Whether hot fields and cold fields share a cache line. These decisions are locked in by the `struct` definition, and changing them later requires touching every callsite that constructs or accesses the type.

This lesson covers the mechanics that determine how Rust lays out your types in memory, the `repr` attributes that control those mechanics, and how to make decisions that keep hot data in cache.

*Source: Rust for Rustaceans, Chapter 2 (Gjengset)*

---

## Core Concepts

### Alignment and Padding

Every type has an alignment requirement — the CPU needs its address to be a multiple of some power of two. A `u8` needs 1-byte alignment. A `u32` needs 4-byte alignment. A `u64` needs 8-byte alignment.

When you put fields of different alignments in a struct, the compiler inserts **padding** bytes between fields to satisfy alignment requirements (*Rust for Rustaceans*, Ch. 2). Consider this struct with `#[repr(C)]` (which preserves field order):

```rust,edition2021
#[repr(C)]
struct BadLayout {
    tiny: bool,    // 1 byte
    // 3 bytes padding — to align `normal` to 4 bytes
    normal: u32,   // 4 bytes
    small: u8,     // 1 byte
    // 7 bytes padding — to align `long` to 8 bytes
    long: u64,     // 8 bytes
    short: u16,    // 2 bytes
    // 6 bytes padding — to make total size a multiple of alignment (8)
}
// Total: 32 bytes. Actual data: 16 bytes. Wasted: 16 bytes — half the struct is padding.
```

With `#[repr(Rust)]` (the default), the compiler is free to reorder fields by size, descending — eliminating most padding:

```rust,edition2021
// Default Rust layout — compiler reorders fields for minimal padding.
// Effective order: long (8), normal (4), short (2), tiny (1), small (1)
struct GoodLayout {
    tiny: bool,
    normal: u32,
    small: u8,
    long: u64,
    short: u16,
}
// Total: 16 bytes. Same fields, no wasted padding.
```

The difference at scale: a `Vec<BadLayout>` of 1 million elements occupies 32 MB. A `Vec<GoodLayout>` with the same data occupies 16 MB — fitting twice as many elements in the same cache footprint, doubling cache hit rate for sequential access.

You can verify sizes at compile time with `std::mem::size_of`:

```rust,edition2021
#[repr(C)]
struct BadLayout { tiny: bool, normal: u32, small: u8, long: u64, short: u16 }
struct GoodLayout { tiny: bool, normal: u32, small: u8, long: u64, short: u16 }

fn main() {
    // Confirm the size difference at compile time.
    const _: () = assert!(std::mem::size_of::<BadLayout>() == 32);
    const _: () = assert!(std::mem::size_of::<GoodLayout>() == 16);
    println!("BadLayout: {} bytes", std::mem::size_of::<BadLayout>());
    println!("GoodLayout: {} bytes", std::mem::size_of::<GoodLayout>());
}
```

Use `const` assertions as compile-time guards on struct sizes for types that appear in high-volume collections. When a future change accidentally adds padding, the assertion fails at compile time rather than silently degrading cache performance.

### `repr` Attributes

**`repr(Rust)`** — the default. The compiler may reorder fields for minimal padding and does not guarantee a specific layout. This is optimal for Rust-only code but incompatible with C interop.

**`repr(C)`** — fields laid out in declaration order, C-compatible. Required when passing structs across FFI boundaries. At the cost of potentially more padding if fields are not ordered by descending alignment.

**`repr(packed)`** — removes all padding. Fields may be misaligned, which can be much slower on x86 (unaligned loads trigger microcode assists) and cause bus errors on architectures that require alignment. Use only when minimizing memory footprint is more important than access speed — for example, serialized wire formats, or extremely memory-constrained environments.

**`repr(align(n))`** — forces the struct to have at least `n` byte alignment. The most common use in systems programming is cache line alignment for concurrent data structures:

```rust,edition2021
use std::sync::atomic::AtomicU64;

// Each counter occupies a full 64-byte cache line.
// Without this: two counters from different threads share a cache line,
// causing false sharing — each write by one thread invalidates the
// other thread's cache entry even though they touch different data.
#[repr(align(64))]
struct CacheAlignedCounter {
    value: AtomicU64,
    _pad: [u8; 56], // Explicit padding to fill the 64-byte cache line.
}
```

### Cache Lines and False Sharing

A CPU cache line is 64 bytes on x86-64. The CPU fetches and evicts cache lines as atomic units — not individual bytes or words. When two logical pieces of data share a cache line, any write to either one invalidates the entire line in every other core's cache.

**False sharing** occurs when two threads write to different variables that happen to occupy the same cache line (*Rust for Rustaceans*, Ch. 2). Each write by either thread causes the cache line to bounce between cores — effectively serializing what should be independent writes:

```rust,edition2021
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

// BAD: both counters fit in one 16-byte struct, sharing a cache line.
// Thread A's writes to `a` invalidate thread B's cached copy of the line,
// which also contains `b`. Both threads contend on the same cache line.
struct SharedCounters {
    a: AtomicU64,
    b: AtomicU64,
}

// GOOD: each counter on its own cache line.
#[repr(align(64))]
struct IsolatedCounter {
    value: AtomicU64,
}

fn demonstrate_false_sharing() {
    // With SharedCounters: threads A and B writing independently
    // still cause cache coherence traffic because they share a line.

    // With two IsolatedCounter instances: threads write truly independently.
    let counter_a = IsolatedCounter { value: AtomicU64::new(0) };
    let counter_b = IsolatedCounter { value: AtomicU64::new(0) };

    // counter_a and counter_b now occupy separate cache lines.
    // Writes by one thread do not invalidate the other's cache entry.
    counter_a.value.fetch_add(1, Ordering::Relaxed);
    counter_b.value.fetch_add(1, Ordering::Relaxed);
}
```

### Hot Field / Cold Field Separation

Not all fields in a struct are accessed with equal frequency. For a `TelemetryFrame`, the routing fields (`satellite_id`, `sequence`) are read on every frame. The full payload is only read when forwarding downstream. Putting hot and cold data in the same struct means every cache miss for a hot field also loads the cold payload into cache — evicting other useful data.

The pattern: split the struct. Keep a hot "header" struct with frequently accessed fields, and access the cold data via an `Arc<Vec<u8>>` or a separate index:

```rust,edition2021
use std::sync::Arc;

// Hot: accessed on every frame for routing decisions.
// 24 bytes — fits comfortably in cache alongside many sibling headers.
struct FrameHeader {
    satellite_id: u32,   // 4 bytes
    sequence: u64,       // 8 bytes
    timestamp_ms: u64,   // 8 bytes
    flags: u8,           // 1 byte
    _pad: [u8; 3],       // 3 bytes padding (explicit, documented)
}

// Cold: accessed only when forwarding to downstream consumers.
// Heap-allocated; not loaded until needed.
struct FrameBody {
    header: FrameHeader,
    payload: Arc<Vec<u8>>,  // Heap allocation keeps cold data out of hot path.
}
```

A `Vec<FrameHeader>` for routing decisions keeps 24-byte hot entries packed. 64 bytes (one cache line) holds 2 full headers plus change — much better than loading 24 + `payload.len()` bytes per frame just to check a sequence number.

---

## Code Examples

### Verifying Layout Decisions at Compile Time

Use constant assertions to lock in size expectations for hot types. This catches accidental regressions — adding a field that introduces padding shows up as a compile error immediately.

```rust,edition2021
use std::mem::{size_of, align_of};

/// A telemetry frame header optimized for sequential scanning.
/// Fields ordered by alignment (descending) to minimize padding.
#[derive(Debug, Clone, Copy)]
pub struct TelemetryHeader {
    pub timestamp_ms: u64,      // 8 bytes — largest alignment first
    pub sequence: u64,          // 8 bytes
    pub satellite_id: u32,      // 4 bytes
    pub byte_count: u32,        // 4 bytes
    pub flags: u8,              // 1 byte
    pub station_id: u8,         // 1 byte
    pub _reserved: [u8; 2],     // 2 bytes explicit pad — documented intent
}

// Lock in the expected size at compile time.
// If a future change causes unexpected padding, this fails to compile.
const _SIZE_CHECK: () = assert!(size_of::<TelemetryHeader>() == 32);
const _ALIGN_CHECK: () = assert!(align_of::<TelemetryHeader>() == 8);

/// A per-uplink session counter, cache-line aligned to prevent false sharing.
/// 48 sessions each updating their own counter never contend on a shared line.
#[repr(align(64))]
pub struct SessionCounter {
    pub frames_received: u64,
    pub bytes_received: u64,
    pub frames_dropped: u64,
    _pad: [u8; 40],  // Pad to fill 64-byte cache line: 3×8 + 40 = 64.
}

const _COUNTER_ALIGN: () = assert!(align_of::<SessionCounter>() == 64);
const _COUNTER_SIZE: () = assert!(size_of::<SessionCounter>() == 64);

fn main() {
    println!("TelemetryHeader: {} bytes", size_of::<TelemetryHeader>());
    println!("SessionCounter:  {} bytes (cache-line aligned)", size_of::<SessionCounter>());

    // Verify that an array of counters places each on its own cache line.
    let counters: Vec<SessionCounter> = (0..4)
        .map(|_| SessionCounter {
            frames_received: 0,
            bytes_received: 0,
            frames_dropped: 0,
            _pad: [0; 40],
        })
        .collect();

    // Each counter is 64 bytes and 64-byte aligned — no false sharing.
    for (i, c) in counters.iter().enumerate() {
        let addr = c as *const _ as usize;
        println!("counter[{i}] at 0x{addr:x} (aligned: {})", addr % 64 == 0);
    }
}
```

---

## Key Takeaways

- The compiler inserts padding between fields to satisfy alignment requirements. Field order determines how much padding is inserted. Ordering fields by decreasing size (largest alignment first) minimizes padding with default `repr(Rust)`.

- `repr(Rust)` (default) allows the compiler to reorder fields — usually optimal. `repr(C)` preserves field order for FFI compatibility at the potential cost of more padding. `repr(packed)` removes padding but risks misaligned access penalties.

- `repr(align(n))` forces a minimum alignment. Use it to ensure hot atomic counters occupy separate cache lines when accessed from multiple threads concurrently, preventing false sharing.

- False sharing occurs when two threads write to different variables that share a 64-byte cache line. The fix is `repr(align(64))` with explicit padding to fill the cache line.

- Separate hot fields (read on every iteration) from cold fields (read rarely). A struct that bundles both forces the CPU to load cold data into cache on every hot access, evicting more useful data. Use a header struct for hot fields and heap-allocated or indexed access for cold data.

- Use `const` assertions on `size_of` and `align_of` for types in high-volume collections. They turn accidental layout regressions into compile errors rather than silent performance degradation.

---

{{#quiz lesson-01-quiz.toml}}
