# Lesson 3 — Arena Allocation: Bump Allocators for High-Throughput Telemetry Processing

**Module:** Foundation — M05: Data-Oriented Design in Rust
**Position:** Lesson 3 of 3
**Source:** *Rust for Rustaceans* — Jon Gjengset, Chapter 9 (`GlobalAlloc`). SoA and arena patterns synthesized from training knowledge.

> **Source note:** The `GlobalAlloc` trait and its safety requirements are covered in *Rust for Rustaceans*, Ch. 9. The bump allocator pattern and its application to telemetry pipelines are synthesized from training knowledge. Recommended further reading: the `bumpalo` crate documentation and Alexis Beingessner's "The Allocator API, Bump Allocation, and You" (Gankra.github.io).

---
<!-- toc -->
---
## Context

The Meridian telemetry processor allocates a `Vec<u8>` for every telemetry frame payload. At 100,000 frames per second, that is 100,000 `malloc`/`free` calls per second hitting the global allocator. The global allocator (jemalloc or the system allocator) is designed for general-purpose allocation: it handles arbitrary sizes, arbitrary lifetimes, and thread-safe concurrent access. This generality has a cost: each allocation acquires an internal lock or performs atomic operations, searches for a free block of appropriate size, and updates allocator metadata.

For short-lived objects that all die together — a batch of frames processed in one scheduling quantum, all freed at the end — a **bump allocator** eliminates all of that overhead. A bump allocator maintains a pointer into a pre-allocated slab of memory. Each allocation is one pointer addition. Deallocation is a no-op — the entire slab is reclaimed at once when the allocation epoch ends. For the right workload, this is 10–100× faster than the global allocator.

This lesson covers how bump allocators work, when they are appropriate, and how to implement and use them in Rust for high-throughput frame processing.

---

## Core Concepts

### The Global Allocator and Its Overhead

Every `Box::new(x)`, `Vec::new()`, and `String::new()` in Rust calls the global allocator via the `GlobalAlloc` trait (*Rust for Rustaceans*, Ch. 9):

```rust,edition2021
// The GlobalAlloc trait (simplified from std):
pub unsafe trait GlobalAlloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8;
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout);
}
```

The default allocator (jemalloc in most production Rust, or the system allocator) handles:
- Thread safety (internal locks or lock-free data structures)
- Size classes and free lists for different allocation sizes
- Fragmentation management
- Returning memory to the OS when freed

For long-lived, variously-sized allocations with arbitrary lifetimes, this is correct and often fast. For thousands of small, short-lived allocations that all have the same lifetime, it is expensive overkill.

### Bump Allocation: The Pattern

A bump allocator owns a contiguous slab of memory. Each allocation is a pointer increment:

```
[slab start]  →  [offset]  →  [slab end]
                    ↑
               pointer bumps forward on each allocation
```

Freeing individual allocations is not supported. The entire slab is reset in one operation when all allocations are no longer needed — the "epoch" ends and the offset pointer resets to zero.

Properties:
- **Allocation:** O(1), typically one integer addition and a bounds check.
- **Deallocation:** O(1) total for all allocations from one epoch — reset the offset.
- **Thread safety:** A single-threaded bump allocator has no synchronisation overhead. A thread-local bump allocator gives each thread its own slab with no contention.
- **Fragmentation:** None within the epoch. Memory is never reused for a different allocation during the epoch — no fragmentation.
- **Limitation:** Cannot free individual allocations. All allocations from one bump allocator share the same lifetime.

### Using `bumpalo` for Safe Bump Allocation

The `bumpalo` crate provides a production-quality bump allocator with a safe Rust interface:

```rust,edition2021
// bumpalo is not available in the Playground — this shows the API.
// Add to Cargo.toml: bumpalo = { version = "3", features = ["collections"] }
// use bumpalo::Bump;
// use bumpalo::collections::Vec as BumpVec;

// Illustrating the pattern with a manual approach instead:
struct BumpArena {
    slab: Vec<u8>,
    offset: usize,
}

impl BumpArena {
    fn new(capacity: usize) -> Self {
        Self {
            slab: vec![0u8; capacity],
            offset: 0,
        }
    }

    /// Allocate `size` bytes aligned to `align`.
    /// Returns None if the slab is exhausted.
    fn alloc(&mut self, size: usize, align: usize) -> Option<&mut [u8]> {
        // Align the current offset up.
        let aligned = (self.offset + align - 1) & !(align - 1);
        let end = aligned + size;
        if end > self.slab.len() {
            return None; // Out of space.
        }
        self.offset = end;
        Some(&mut self.slab[aligned..end])
    }

    /// Reset the arena — all previous allocations are invalidated.
    fn reset(&mut self) {
        self.offset = 0;
    }

    fn used(&self) -> usize { self.offset }
    fn capacity(&self) -> usize { self.slab.len() }
}

fn main() {
    let mut arena = BumpArena::new(4096);

    // Allocate space for 10 u64 values.
    let buf = arena.alloc(10 * 8, 8).expect("arena exhausted");
    println!("allocated {} bytes, used {}/{}", buf.len(), arena.used(), arena.capacity());

    // Reset — all allocations invalidated, slab reused.
    arena.reset();
    println!("after reset: used {}", arena.used());
}
```

### Thread-Local Arenas for Concurrent Processing

For a multi-threaded pipeline where each worker thread processes its own batch of frames, a thread-local arena eliminates all lock contention:

```rust,edition2021
use std::cell::RefCell;

const ARENA_CAPACITY: usize = 16 * 1024 * 1024; // 16MB per thread

thread_local! {
    // Each worker thread has its own private arena.
    // No synchronisation — no atomic operations, no locks.
    static FRAME_ARENA: RefCell<Vec<u8>> = RefCell::new(vec![0u8; ARENA_CAPACITY]);
    static ARENA_OFFSET: RefCell<usize> = const { RefCell::new(0) };
}

fn alloc_frame_buffer(size: usize) -> *mut u8 {
    FRAME_ARENA.with(|arena| {
        ARENA_OFFSET.with(|offset| {
            let mut off = offset.borrow_mut();
            let aligned = (*off + 7) & !7; // 8-byte alignment
            let end = aligned + size;
            let arena = arena.borrow();
            if end > arena.len() {
                panic!("thread-local arena exhausted — increase ARENA_CAPACITY or reduce batch size");
            }
            *off = end;
            arena.as_ptr() as *mut u8
        })
    })
}

fn reset_thread_arena() {
    ARENA_OFFSET.with(|offset| *offset.borrow_mut() = 0);
}
```

In practice, use `bumpalo::Bump` in a `thread_local!` instead of building the unsafe version above. `bumpalo` handles alignment, growth, and lifetime correctly with a safe interface.

### Epoch-Based Processing: The Right Workload

The bump allocator pattern maps naturally onto batch processing where all objects in a batch share the same lifetime:

```rust,edition2021
use std::time::Instant;

/// Simulates a frame batch processor using a bump-style pre-allocated pool.
/// Each frame's payload is drawn from the batch buffer.
/// When the batch is complete, the buffer is reset — no individual frees.
struct FrameBatchProcessor {
    /// Pre-allocated buffer for all frame payloads in one batch.
    payload_pool: Vec<u8>,
    pool_offset: usize,
    batch_size: usize,
    frames_in_batch: usize,
}

impl FrameBatchProcessor {
    fn new(batch_size: usize, max_payload_per_frame: usize) -> Self {
        Self {
            payload_pool: vec![0u8; batch_size * max_payload_per_frame],
            pool_offset: 0,
            batch_size,
            frames_in_batch: 0,
        }
    }

    /// Claim space for a frame payload from the pool.
    /// Returns a mutable slice for the caller to fill.
    fn claim_payload_slot(&mut self, size: usize) -> Option<&mut [u8]> {
        if self.frames_in_batch >= self.batch_size {
            return None; // Batch full.
        }
        let end = self.pool_offset + size;
        if end > self.payload_pool.len() {
            return None; // Pool exhausted.
        }
        let slot = &mut self.payload_pool[self.pool_offset..end];
        self.pool_offset = end;
        self.frames_in_batch += 1;
        Some(slot)
    }

    /// Process the current batch and reset for the next one.
    /// All payload slots are implicitly freed — no individual deallocation.
    fn flush_and_reset(&mut self) -> usize {
        let processed = self.frames_in_batch;
        self.pool_offset = 0;
        self.frames_in_batch = 0;
        processed
    }
}

fn main() {
    let mut processor = FrameBatchProcessor::new(1000, 1024);
    let start = Instant::now();

    // Simulate processing 100,000 frames in batches of 1,000.
    let mut total = 0;
    for _batch in 0..100 {
        for _frame in 0..1000 {
            // Claim a 256-byte payload slot — no malloc.
            if let Some(slot) = processor.claim_payload_slot(256) {
                slot[0] = 0xAA; // Simulate writing frame data.
            }
        }
        total += processor.flush_and_reset();
    }

    let elapsed = start.elapsed();
    println!("processed {total} frames in {:?}", elapsed);
    println!("~{:.0} frames/sec", total as f64 / elapsed.as_secs_f64());
}
```

### When Not to Use Bump Allocation

Bump allocators are not appropriate when:

- **Lifetimes are mixed.** If some objects from a batch need to outlive the batch (e.g., forwarding a specific frame to a slow downstream consumer while releasing the rest), a bump allocator cannot express this. The solution is to copy out the long-lived objects to global-allocator memory before resetting.

- **Individual deallocation is required.** A bump allocator cannot free one allocation while keeping others alive. Use a pool allocator (fixed-size slots with a free list) if individual deallocation of same-sized objects is needed.

- **Batches are unpredictably sized.** If you cannot bound the total allocation size of a batch, the arena may exhaust. Size the arena conservatively — or use `bumpalo`, which supports growth by chaining multiple slabs.

---

## Code Examples

### Comparing Global vs Arena Allocation for Frame Batches

This benchmark illustrates the overhead difference. Without running it on actual hardware, the expected speedup for small short-lived allocations is 5–20× in favour of the arena.

```rust,edition2021
use std::time::Instant;

const FRAMES: usize = 100_000;
const PAYLOAD_SIZE: usize = 256;

fn bench_global_alloc() -> std::time::Duration {
    let start = Instant::now();
    for _ in 0..FRAMES {
        // Each Vec::new() + push triggers malloc + memcpy.
        let mut v = Vec::with_capacity(PAYLOAD_SIZE);
        for i in 0..PAYLOAD_SIZE {
            v.push(i as u8);
        }
        // Drop at end of loop iteration — free() called 100,000 times.
        let _ = v;
    }
    start.elapsed()
}

fn bench_arena_alloc() -> std::time::Duration {
    // Pre-allocate a slab for the entire batch.
    let mut slab = vec![0u8; FRAMES * PAYLOAD_SIZE];
    let start = Instant::now();
    let mut offset = 0;
    for frame_idx in 0..FRAMES {
        let start_byte = offset;
        let end_byte = offset + PAYLOAD_SIZE;
        for (i, byte) in slab[start_byte..end_byte].iter_mut().enumerate() {
            *byte = (frame_idx ^ i) as u8;
        }
        offset = end_byte;
    }
    // All frames "freed" by resetting offset to 0 — one operation.
    offset = 0;
    let _ = offset;
    start.elapsed()
}

fn main() {
    // Warm up to avoid measurement noise from cold caches.
    let _ = bench_global_alloc();
    let _ = bench_arena_alloc();

    let global_time = bench_global_alloc();
    let arena_time  = bench_arena_alloc();

    println!("global alloc: {:?}", global_time);
    println!("arena alloc:  {:?}", arena_time);
    let speedup = global_time.as_nanos() as f64 / arena_time.as_nanos() as f64;
    println!("arena speedup: {speedup:.1}×");
}
```

The arena's advantage grows with allocation count. At 100,000 256-byte frames, the arena avoids 100,000 malloc/free round-trips. The global allocator also has to find and merge free blocks over time as the heap fragments; the arena has zero fragmentation overhead.

---

## Key Takeaways

- The global allocator (`malloc`/`free`) is general-purpose: thread-safe, handles arbitrary sizes and lifetimes, manages fragmentation. Its generality has overhead — internal synchronisation, free list management, metadata updates.

- A bump allocator eliminates this overhead for objects with a shared lifetime. Allocation is one integer addition. Deallocation is resetting one offset — all objects from one epoch freed simultaneously.

- The lifetime constraint is the critical requirement. If any object from a bump-allocated batch must outlive the batch, copy it out to the global allocator before resetting. Do not try to mix lifetimes within one arena.

- Thread-local arenas eliminate all cross-thread contention. Each worker thread gets its own slab; no lock, no atomic operation, no cache line bounce for allocation.

- Use `bumpalo` in production. It handles alignment, growth via chained slabs, and safe lifetimes. Implement your own bump allocator only for educational purposes or in `no_std` environments where crate dependencies are restricted.

- Profile before optimising. The global allocator is fast for typical workloads. Bump allocation is a targeted optimisation for high-frequency, same-lifetime allocation patterns — not a universal replacement for `Vec` or `Box`.

---

{{#quiz lesson-03-quiz.toml}}
