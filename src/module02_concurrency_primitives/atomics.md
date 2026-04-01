# Lesson 3 — Atomics and Memory Ordering: Acquire/Release/SeqCst in Practice

**Module:** Foundation — M02: Concurrency Primitives  
**Position:** Lesson 3 of 3  
**Source:** *Rust Atomics and Locks* — Mara Bos, Chapters 2–3

---
<!-- toc -->
---
## Context

The Meridian control plane increments a frame counter every time a telemetry frame is received — 4,800 times per second at full uplink load across 48 satellites. The per-session heartbeat timer fires every 100ms. The frame drop rate is sampled by the monitoring dashboard every second. None of these operations need the overhead of a mutex lock. They need a single integer that multiple threads can read and write without data races.

This is the domain of atomics. `std::sync::atomic` provides integer and boolean types that support safe concurrent mutation without locking. The operations are indivisible — they either complete entirely or have not happened yet — which prevents the torn reads and non-atomic increments that would corrupt counters under concurrent access.

But atomics are not free. The memory ordering argument on every atomic operation — `Relaxed`, `Acquire`, `Release`, `AcqRel`, `SeqCst` — controls what guarantees the processor and compiler make about the ordering of operations across threads. Getting this wrong produces bugs that are invisible in development and intermittent in production.

*Source: Rust Atomics and Locks, Chapters 2–3 (Bos)*

---

## Core Concepts

### What Atomic Operations Guarantee

An atomic operation is indivisible: it either completes entirely before any other operation on the same variable, or it has not happened yet (*Rust Atomics and Locks*, Ch. 2). Two threads simultaneously performing `counter += 1` on a plain integer is undefined behavior — the read-modify-write is three separate operations, and the interleaving is unpredictable. Two threads simultaneously calling `counter.fetch_add(1, Relaxed)` is defined and correct: each `fetch_add` is a single atomic step.

The available types live in `std::sync::atomic`: `AtomicBool`, `AtomicI8`/`U8` through `AtomicI64`/`U64`, `AtomicIsize`/`Usize`, and `AtomicPtr<T>`. All support mutation through a shared reference (`&AtomicUsize`) — they use interior mutability without `UnsafeCell` runtime checks.

Every atomic operation takes an `Ordering` argument. The ordering is not about the value — it is about the visibility of *other* memory operations to other threads.

### Load, Store, and Fetch-and-Modify

The three basic operation families:

**Load and store** — read or write the atomic value:
```rust,edition2021
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
static FRAME_COUNT: AtomicU64 = AtomicU64::new(0);

fn record_frame() {
    FRAME_COUNT.fetch_add(1, Relaxed);
}

fn read_frame_count() -> u64 {
    FRAME_COUNT.load(Relaxed)
}
```

**Fetch-and-modify** — atomically modify the value and return the *previous* value (*Rust Atomics and Locks*, Ch. 2):
```rust,edition2021
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

fn main() {
    let counter = AtomicU64::new(100);
    let old = counter.fetch_add(23, Relaxed);
    assert_eq!(old, 100);          // returned the value before the add
    assert_eq!(counter.load(Relaxed), 123); // value after the add
}
```

The full set: `fetch_add`, `fetch_sub`, `fetch_and`, `fetch_or`, `fetch_xor`, `fetch_max`, `fetch_min`, and `swap`. Use these in preference to compare-and-exchange when the operation fits — they are simpler and the compiler can map them to a single hardware instruction.

### `compare_exchange` — The General Atomic Primitive

`compare_exchange` atomically checks whether the current value equals an expected value, and if so, replaces it with a new value. It returns the previous value on success, and the actual current value on failure (*Rust Atomics and Locks*, Ch. 2):

```rust,edition2021
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

fn increment_if_below(a: &AtomicU32, limit: u32) -> bool {
    let mut current = a.load(Relaxed);
    loop {
        if current >= limit { return false; }
        match a.compare_exchange(current, current + 1, Relaxed, Relaxed) {
            Ok(_) => return true,   // successfully incremented
            Err(v) => current = v,  // another thread changed it; retry
        }
    }
}

fn main() {
    let seq = AtomicU32::new(0);
    println!("{}", increment_if_below(&seq, 5)); // true
}
```

The loop-and-retry pattern is fundamental: load the current value, compute the desired new value without holding any lock, then swap atomically only if the value has not changed since the load. If it has changed, retry. This is a lock-free algorithm — no thread blocks, and progress is guaranteed as long as at least one thread makes progress.

`compare_exchange_weak` may spuriously fail (return `Err` even when the value matches) on some architectures. Use it in loops where spurious failure just triggers another iteration. Use the strong version when you need a guarantee that success or failure is definitive.

The ABA problem: if a value changes from A to B and back to A between the load and the CAS, `compare_exchange` will succeed even though the value was modified. For simple counters and flags this is harmless; for pointer-based data structures it can be a correctness issue.

### Memory Ordering — The Model

Processors and compilers reorder operations when it does not change single-threaded program behavior. In concurrent code, these reorderings can change observed behavior across threads. Memory ordering tells the compiler and processor what reorderings are permissible around a given atomic operation (*Rust Atomics and Locks*, Ch. 3).

**`Relaxed`** — no ordering guarantees beyond consistency on the single atomic variable. All threads see modifications of a given atomic in the same total order, but operations on *different* variables may be reordered arbitrarily. Use for statistics counters and progress indicators where you only care about the eventual value, not the timing relationship with other operations.

**`Release` (stores) / `Acquire` (loads)** — the most important pair. A release-store establishes a *happens-before* relationship with any subsequent acquire-load that reads the stored value:

```rust,edition2021
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::{Acquire, Release, Relaxed}};
use std::thread;

static DATA: AtomicU64 = AtomicU64::new(0);
static READY: AtomicBool = AtomicBool::new(false);

fn main() {
    thread::spawn(|| {
        DATA.store(12345, Relaxed);     // (1) write data
        READY.store(true, Release);     // (2) publish: everything before this is visible...
    });

    while !READY.load(Acquire) {        // (3) ...once this returns true.
        std::hint::spin_loop();
    }
    println!("{}", DATA.load(Relaxed)); // guaranteed to print 12345
}
```

Once the acquire-load at (3) sees `true`, the happens-before relationship guarantees that (1) is visible. Without the Acquire/Release pair — using Relaxed on both — the processor could see READY as true while DATA still holds 0.

The names come from the mutex pattern: a mutex unlock is a release-store; a mutex lock-acquire is an acquire-load. Everything the thread did before releasing the mutex is visible to the thread that acquires it next.

**`AcqRel`** — both Acquire and Release in a single operation. Used for read-modify-write operations (like `fetch_add` or `compare_exchange`) that must both see all prior releases and publish all prior stores.

**`SeqCst`** — sequentially consistent: the strongest ordering. All `SeqCst` operations across all threads form a single total order that every thread agrees on. This is stronger than Acquire/Release and is rarely needed. Use it when you have two threads each setting a flag and then reading the other's flag, and you need to guarantee that at least one thread sees the other's write (*Rust Atomics and Locks*, Ch. 3). In nearly all other cases, Acquire/Release is sufficient.

### When to Reach for Atomics vs Mutex

Atomics are not a general replacement for mutexes. They are appropriate for:

- Single-value counters and flags (frame counts, connection counts, shutdown flags)
- Lock-free reference counting (the internal mechanism of `Arc`)
- Progress indicators shared between threads
- Single-producer/single-consumer patterns where acquire/release establishes the necessary ordering

Mutexes are appropriate for:
- Protecting multi-field structs where all fields must be updated atomically
- Any operation that requires a multi-step transaction
- Data structures that cannot be represented as a single atomic value

Reaching for `SeqCst` everywhere is not safe by default — it has higher cost on some architectures (notably ARM) and the extra strength is rarely needed. Start with Acquire/Release. If your correctness argument requires a global total order across multiple atomics, then `SeqCst` is warranted.

---

## Code Examples

### Multi-Thread Frame Counter with Atomic Statistics

The telemetry pipeline tracks three counters: total frames received, total frames dropped (due to backpressure), and bytes processed. These are written by 48 uplink tasks and read by the monitoring dashboard. A mutex would serialize all 48 writes; atomics let them proceed in parallel.

```rust,edition2021
use std::sync::atomic::{AtomicU64, Ordering::{Relaxed, Release, Acquire}};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

struct PipelineMetrics {
    frames_received: AtomicU64,
    frames_dropped: AtomicU64,
    bytes_processed: AtomicU64,
    // Shutdown flag: Release on write, Acquire on read.
    shutdown: AtomicU64,
}

impl PipelineMetrics {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            frames_received: AtomicU64::new(0),
            frames_dropped: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            shutdown: AtomicU64::new(0),
        })
    }

    fn record_frame(&self, bytes: u64) {
        // Relaxed: these counters are for monitoring only.
        // The exact ordering relative to other threads' stores doesn't matter;
        // we only care about the eventual totals.
        self.frames_received.fetch_add(1, Relaxed);
        self.bytes_processed.fetch_add(bytes, Relaxed);
    }

    fn record_drop(&self) {
        self.frames_dropped.fetch_add(1, Relaxed);
    }

    fn signal_shutdown(&self) {
        // Release: ensures all frame counts written before this are visible
        // to any thread that reads shutdown with Acquire.
        self.shutdown.store(1, Release);
    }

    fn should_stop(&self) -> bool {
        // Acquire: establishes happens-before with the Release store above.
        // Any Relaxed loads on frames_received etc. after this call
        // will see all stores from before signal_shutdown().
        self.shutdown.load(Acquire) == 1
    }

    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.frames_received.load(Relaxed),
            self.frames_dropped.load(Relaxed),
            self.bytes_processed.load(Relaxed),
        )
    }
}

fn main() {
    let metrics = PipelineMetrics::new();

    // Simulate 4 uplink tasks.
    let workers: Vec<_> = (0..4).map(|i| {
        let m = Arc::clone(&metrics);
        thread::spawn(move || {
            for _ in 0..100 {
                if m.should_stop() { break; }
                m.record_frame(1024);
                if i == 0 { m.record_drop(); } // simulate occasional drops on uplink 0
            }
        })
    }).collect();

    // Monitoring thread samples every 5ms.
    let m = Arc::clone(&metrics);
    let monitor = thread::spawn(move || {
        for _ in 0..3 {
            thread::sleep(Duration::from_millis(5));
            let (recv, drop, bytes) = m.snapshot();
            println!("recv={recv} drop={drop} bytes={bytes}");
        }
        m.signal_shutdown();
    });

    for w in workers { w.join().unwrap(); }
    monitor.join().unwrap();
    let (recv, drop, bytes) = metrics.snapshot();
    println!("final: recv={recv} drop={drop} bytes={bytes}");
}
```

The Acquire/Release pair on the `shutdown` flag ensures that after any thread reads `should_stop()` as true, all `Relaxed` frame counts written before `signal_shutdown()` are visible. Without this pair, the monitoring thread could read `shutdown=1` but still see stale frame counts from before the shutdown writes.

---

## Key Takeaways

- Atomic operations are indivisible: a `fetch_add` on an `AtomicU64` is a single step with no observable intermediate state. Plain integer `+=` is not atomic — concurrent modification is undefined behavior.

- `fetch_add` and friends return the value *before* the operation. This is intentional: it lets you use the old value to implement compare-and-swap patterns or sequence counters.

- `compare_exchange` is the general-purpose lock-free primitive. The loop-and-retry pattern — load, compute, CAS, retry on failure — enables lock-free algorithms where no thread ever blocks.

- `Relaxed` ordering gives only modification order on a single variable. It is correct for statistics counters and progress indicators where cross-variable ordering does not matter.

- Acquire/Release establishes happens-before across threads. A release-store publishes all preceding memory operations; an acquire-load that reads that value sees all of them. This is what makes mutex unlock/lock, `Arc` drop/clone, and cross-thread data handoffs safe.

- `SeqCst` provides a global total order across all `SeqCst` operations on all threads. Use it only when you need to coordinate two or more flags where the relative order matters globally. In practice, Acquire/Release covers the vast majority of use cases.

---

{{#quiz lesson-03-quiz.toml}}