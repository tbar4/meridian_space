# Lesson 1 — Threads vs Async Tasks: When to Use Each and Why

**Module:** Foundation — M02: Concurrency Primitives  
**Position:** Lesson 1 of 3  
**Source:** *Rust Atomics and Locks* — Mara Bos, Chapter 1

---

## Context

The Meridian control plane is not a purely async system. The async runtime handles the high-frequency path — accepting ground station connections, reading telemetry frames, routing them to downstream consumers. But the control plane also runs work that has no business on an async worker thread: a vendor-supplied TLE validation library with a synchronous C FFI, a CPU-intensive conjunction check that processes several hundred orbital elements per pass, and a legacy configuration parser that performs synchronous file I/O.

The Python system handled this by running everything on threads, leaning on the GIL to serialize concurrent access. The Rust replacement needs a deliberate model. The first decision you make when writing any new piece of the control plane is: does this go on an async task or an OS thread? Getting this wrong produces either a system that starves its async executor with blocking work, or one that spawns OS threads unnecessarily, paying per-thread stack overhead at scale.

This lesson establishes the model. Every rule here has a corresponding failure mode that has been observed in Meridian's staging environment.

*Source: Rust Atomics and Locks, Chapter 1 (Bos)*

---

## Core Concepts

### The Fundamental Difference

An OS thread is scheduled by the kernel. The kernel decides when it runs, when it is preempted, and which CPU core it runs on. The thread has its own stack (typically 2–8 MB by default), and blocking — whether on I/O, a mutex, or `std::thread::sleep` — is perfectly safe: the kernel parks the thread and runs something else.

An async task is scheduled by the executor. It runs until it voluntarily yields at an `await` point. It shares executor worker threads with other tasks. Blocking on the worker thread — calling a synchronous library, running a long computation, sleeping with `std::thread::sleep` — starves every other task scheduled on that thread. There is no kernel to preempt you and run something else.

This is the core rule: **any call that can block a thread for non-trivial time belongs on an OS thread, not on an async worker thread.** In Tokio, the mechanism is `spawn_blocking`, which routes the closure to a dedicated blocking thread pool. From the async side, it looks like an awaitable future. On the execution side, it gets a real OS thread.

### `std::thread::spawn` — Ownership and Lifetimes

`std::thread::spawn` takes a closure that is `Send + 'static`. The `'static` requirement means the thread cannot borrow from the spawning scope — it must own everything it uses, or access data through shared references that are themselves `'static` (like `Arc`).

```rust,edition2021
use std::thread;
use std::sync::Arc;

fn main() {
    let catalog = Arc::new(vec!["ISS", "CSS", "STARLINK-1"]);

    let handle = thread::spawn({
        let catalog = Arc::clone(&catalog);
        move || {
            // catalog is owned by this thread — no borrow, no lifetime issue.
            println!("Thread sees {} objects", catalog.len());
        }
    });

    handle.join().unwrap();
    println!("Main sees {} objects", catalog.len());
}
```

The `Arc::clone` before the move is idiomatic: clone the handle, not the data. The thread gets its own `Arc` pointer (cheap — one atomic increment), and both threads share the underlying `Vec`. When both `Arc`s drop, the `Vec` deallocates.

### `thread::scope` — Scoped Threads with Borrowed Data

The `'static` requirement on `spawn` prevents borrowing stack data. `thread::scope` lifts this restriction: threads spawned within a scope are guaranteed to finish before the scope exits, which allows them to borrow data from the enclosing frame.

```rust,edition2021
use std::thread;

fn validate_tle_batch(records: &[String]) -> usize {
    let mid = records.len() / 2;
    let (left, right) = records.split_at(mid);

    // Scoped threads can borrow `left` and `right` — no Arc, no clone.
    thread::scope(|s| {
        let left_handle = s.spawn(|| left.iter().filter(|r| r.starts_with("1 ")).count());
        let right_handle = s.spawn(|| right.iter().filter(|r| r.starts_with("1 ")).count());

        // scope blocks here until both threads finish.
        left_handle.join().unwrap() + right_handle.join().unwrap()
    })
}

fn main() {
    let records: Vec<String> = (0..100)
        .map(|i| format!("{} {:05}U record", if i % 2 == 0 { "1" } else { "2" }, i))
        .collect();
    println!("{} valid TLE lines", validate_tle_batch(&records));
}
```

`thread::scope` is the right tool for data-parallel CPU work over a borrowed slice — exactly the conjunction check pattern in the Meridian pipeline. No heap allocation, no `Arc`, no `'static` constraint. The compiler enforces that the borrowed data outlives the scope.

### `Send` and `Sync` — The Type System's Enforcement

Rust enforces thread safety through two marker traits (*Rust Atomics and Locks*, Ch. 1):

**`Send`:** a type is `Send` if ownership of a value of that type can be transferred to another thread. `Arc<T>` is `Send` (if `T: Send + Sync`), but `Rc<T>` is not — `Rc`'s reference count is non-atomic and would race if shared across threads.

**`Sync`:** a type is `Sync` if it can be shared between threads by shared reference. `i32` is `Sync`. `Cell<i32>` is not — mutating through a shared reference is not safe across threads.

The compiler enforces these automatically. You cannot accidentally send a `Rc<T>` to another thread — `thread::spawn` requires `Send`, and `Rc` does not implement it. You cannot share a `RefCell<T>` across threads — `Mutex<T>` requires `T: Send`, and `RefCell` does not implement `Sync`.

Both traits are auto-derived: a struct whose fields are all `Send` is itself `Send`. The common exceptions are raw pointers (`*const T`, `*mut T`), `Rc`, `Cell`, `RefCell`, and types that wrap OS handles that are not thread-safe. When you implement a type that wraps these, you must opt in to `Send`/`Sync` manually with `unsafe impl`, accepting responsibility for the invariant.

### Choosing the Right Model

The decision tree for any piece of work in the control plane:

| Work type | Right model | Mechanism |
|---|---|---|
| Concurrent TCP connections, channel receive/send | Async task | `tokio::spawn` |
| CPU-bound computation (conjunction check, CRC) | Blocking thread | `spawn_blocking` |
| Synchronous vendor library (C FFI) | Blocking thread | `spawn_blocking` |
| Synchronous file I/O (`std::fs`) | Blocking thread | `spawn_blocking` |
| Data-parallel work over borrowed data | Scoped threads | `thread::scope` |
| Independent long-running background service | OS thread | `thread::spawn` |

The cost difference matters at scale. An OS thread on Linux has a default 8 MB stack reservation (even if physical pages are not committed until used), a kernel thread structure, and scheduling overhead. Tokio tasks use a few hundred bytes of heap. The control plane at 48 uplinks can sustain thousands of concurrent tasks trivially; it cannot sustain thousands of OS threads without careful stack-size tuning.

---

## Code Examples

### Mixing Async and Blocking: The Vendor TLE Validator

The TLE validation library provided by Meridian's orbit data vendor is a synchronous C library wrapped in a Rust FFI crate. It performs checksum validation and orbital element range checking — purely CPU work, no I/O, but it takes 2–15ms per record depending on complexity. Calling it from an async task would stall the executor for the duration.

```rust,edition2021
use std::time::Duration;
use tokio::task;

// Simulates a synchronous vendor library call.
// In production: calls into the C FFI wrapper.
fn validate_tle_sync(line1: &str, line2: &str) -> Result<(), String> {
    // Vendor library does checksum + orbital element bounds checking.
    // Blocks for 2–15ms depending on record complexity.
    std::thread::sleep(Duration::from_millis(5)); // placeholder
    if line1.starts_with("1 ") && line2.starts_with("2 ") {
        Ok(())
    } else {
        Err(format!("malformed TLE: {line1}"))
    }
}

async fn validate_tle_async(line1: String, line2: String) -> Result<(), String> {
    // Move strings into the blocking closure.
    // spawn_blocking runs on the dedicated blocking thread pool —
    // async worker threads are not touched.
    task::spawn_blocking(move || validate_tle_sync(&line1, &line2))
        .await
        // JoinError means the blocking thread panicked.
        .map_err(|e| format!("validator panicked: {e}"))?
}

#[tokio::main]
async fn main() {
    // All 48 sessions can submit validation concurrently.
    // Each runs on the blocking pool; none stall the async workers.
    let tasks: Vec<_> = (0..6).map(|i| {
        tokio::spawn(validate_tle_async(
            format!("1 {:05}U 98067A   21275.52  .00001234  00000-0  12345-4 0  999{i}", i),
            format!("2 {:05}  51.6400 337.6640 0007417  62.6000 297.5200 15.4888958300000{i}", i),
        ))
    }).collect();

    for (i, t) in tasks.into_iter().enumerate() {
        match t.await.unwrap() {
            Ok(()) => println!("record {i}: valid"),
            Err(e) => println!("record {i}: {e}"),
        }
    }
}
```

### Scoped Threads for Parallel Conjunction Screening

The conjunction screening pass runs every 10 minutes against the full 50k-object catalog. It splits the catalog across CPU cores using scoped threads. The catalog is a large `Vec<OrbitalRecord>` — no clone, no `Arc`, just borrowed slices distributed across workers.

```rust,edition2021
use std::thread;

#[derive(Clone)]
struct OrbitalRecord {
    norad_id: u32,
    altitude_km: f64,
}

struct ConjunctionAlert {
    object_a: u32,
    object_b: u32,
    closest_approach_km: f64,
}

fn screen_shard(shard: &[OrbitalRecord], threshold_km: f64) -> Vec<ConjunctionAlert> {
    // Simplified: real implementation computes relative positions via SGP4.
    shard.windows(2)
        .filter(|pair| (pair[0].altitude_km - pair[1].altitude_km).abs() < threshold_km)
        .map(|pair| ConjunctionAlert {
            object_a: pair[0].norad_id,
            object_b: pair[1].norad_id,
            closest_approach_km: (pair[0].altitude_km - pair[1].altitude_km).abs(),
        })
        .collect()
}

fn run_conjunction_screen(catalog: &[OrbitalRecord], threshold_km: f64) -> Vec<ConjunctionAlert> {
    let num_cores = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let shard_size = (catalog.len() + num_cores - 1) / num_cores;

    thread::scope(|s| {
        let handles: Vec<_> = catalog
            .chunks(shard_size)
            .map(|shard| s.spawn(move || screen_shard(shard, threshold_km)))
            .collect();

        handles.into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect()
    })
}

fn main() {
    let catalog: Vec<OrbitalRecord> = (0..1000)
        .map(|i| OrbitalRecord { norad_id: i, altitude_km: 400.0 + (i as f64 * 0.3) })
        .collect();

    let alerts = run_conjunction_screen(&catalog, 5.0);
    println!("{} conjunction alerts generated", alerts.len());
}
```

Each shard runs on its own OS thread via `thread::scope`, borrowing its slice without any heap allocation for sharing. The scope blocks until all workers finish, then results are collected. This is the correct pattern for data-parallel CPU work where all input data is available upfront and results need to be aggregated.

---

## Key Takeaways

- OS threads are preemptively scheduled by the kernel. Async tasks are cooperatively scheduled by the executor. Blocking on an async worker thread — any call that does not yield at `await` — starves other tasks on that thread.

- Use `spawn_blocking` for any synchronous, blocking, or CPU-intensive work that originates in an async context. It routes work to a dedicated thread pool separate from the async workers.

- `thread::scope` allows scoped threads to borrow data from the enclosing frame without `Arc` or `'static` constraints. It is the right tool for data-parallel work over borrowed slices. The scope blocks until all spawned threads finish.

- `Send` and `Sync` are marker traits enforced at compile time. `Send` permits transferring ownership across threads; `Sync` permits sharing by reference. Violating these constraints — sending `Rc`, sharing `Cell` — is a compile error, not a runtime race.

- The thread vs async decision is about scheduling model, not concurrency. Both models run work concurrently. The difference is what happens when work blocks: OS threads can block safely; async tasks cannot.

---

{{#quiz lesson-01-quiz.toml}}