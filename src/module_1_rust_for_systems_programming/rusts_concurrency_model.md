# Rust's Concurrency Model
## Context

Meridian's legacy Python control plane manages uplink sessions for a growing constellation of satellites. As the fleet scales beyond 48 satellites, that control plane faces a fundamental structural problem: Python's GIL serializes concurrent work, and the thread-per-connection model it uses can't sustain the volume of simultaneous ground station sessions without saturating memory. The replacement, written in Rust, will manage shared mutable state — active session maps, per-satellite connection counters, ground station health registries — across multiple OS threads and Tokio worker threads simultaneously.

Getting this right requires a precise understanding of Rust's concurrency model. Unlike runtime-checked languages, Rust enforces the rules of concurrent access at compile time through its ownership and type systems. The two marker traits `Send` and `Sync` encode thread safety directly into the type system, making entire classes of data race impossible to express in safe code. This isn't a convention or a lint — it's a compiler invariant.

This lesson covers the primitives you will use throughout the Foundation track and beyond: how Rust prevents data races at the type level, how to share ownership across threads with `Arc`, how to coordinate shared mutable access with `Mutex<T>` and `RwLock<T>`, and how to reason about the distinct failure modes — deadlock, lock poisoning, and writer starvation — that remain your responsibility even after the compiler has done its job.

---

## Core Concepts

### Threads, Tasks, and When to Use Each

Rust supports two models of concurrent execution. OS threads (`std::thread::spawn`) are kernel-scheduled, carry their own stack, and are appropriate for CPU-bound work or blocking I/O that would otherwise stall a Tokio worker. Async tasks (`tokio::spawn`) are cooperatively scheduled on a thread pool and are appropriate for I/O-bound workloads where many units of concurrent work spend most of their time waiting. The Tokio runtime covered in later modules sits on top of OS threads — understanding threads is prerequisite to understanding what the async runtime is actually doing underneath.

This lesson focuses on OS-thread concurrency and the shared-state primitives that serve both threads and async tasks. `Arc<Mutex<T>>` works identically whether you're sharing state between two `std::thread::spawn` threads or two Tokio tasks.

### The Ownership–Concurrency Connection

Rust's borrow checker guarantees that at any point in a program, a value has either any number of shared references (`&T`) or exactly one exclusive reference (`&mut T`), never both. This rule, enforced statically, is precisely what prevents data races: a data race requires one thread mutating a value while another concurrently accesses it. The borrow checker makes that state unrepresentable in safe code.

As Mara Bos notes in *Rust Atomics and Locks*, "these two concepts together fully prevent data races... the compiler is free to assume they do not happen." This is not just a safety property — it's a performance property. The compiler can make aggressive optimizations based on the guarantee that aliased mutation cannot occur.

When you need mutation through a shared reference — which is unavoidable in concurrent systems — you use types that implement *interior mutability*: `Mutex<T>`, `RwLock<T>`, and the atomics. These types move the borrow-checking enforcement from compile time to runtime, but they do so in controlled ways that preserve the absence of undefined behavior.

### `Send` and `Sync`: Thread Safety in the Type System

Two marker traits govern whether a type can participate in concurrent code:

**`Send`**: A type is `Send` if ownership of a value can be transferred to another thread. `Arc<i32>` is `Send`. `Rc<i32>` is not — its reference counter uses non-atomic operations, so transferring it across threads would produce a data race on the counter itself.

**`Sync`**: A type is `Sync` if a shared reference to it can be sent to another thread — formally, `T: Sync` if and only if `&T: Send`. `Mutex<T>` is `Sync` (multiple threads can hold a reference to it and contend for the lock). `Cell<T>` is not `Sync` (it allows mutation through a shared reference without any synchronization).

Both are *auto traits*: the compiler derives them automatically based on a type's fields. A struct whose fields are all `Send + Sync` is itself `Send + Sync`. The way to opt out is to include a field that is not `Send` or not `Sync`. The way to opt in — required when wrapping raw pointers — is `unsafe impl Send for T {}`, which is a promise to the compiler that you have manually verified safety.

The practical consequence: if you try to send a non-`Send` type across a thread boundary, the compiler rejects it at the call site with a diagnostic that names the trait and the type that violated it. There is no runtime check, no race condition, no debugging session. The violation is a compile error.

### `Arc<T>`: Shared Ownership Across Threads

When data must outlive any single thread — because neither thread is guaranteed to exit before the other — neither can own it exclusively. `Arc<T>` (atomically reference-counted) solves this by tracking ownership with an atomic counter. Cloning an `Arc` increments the counter; dropping one decrements it. When the counter reaches zero, the allocation is freed.

`Rc<T>` serves the same purpose within a single thread, but uses non-atomic counter operations. The type system encodes this: `Rc<T>` does not implement `Send`, so the compiler prevents it from crossing a thread boundary. `Arc<T>` does implement `Send` (when `T: Send + Sync`).

`Arc<T>` gives shared *read* access. It behaves like `&T` — you cannot mutate through it directly. For shared mutable access, wrap the interior in a `Mutex` or `RwLock`: `Arc<Mutex<T>>`.

### `Mutex<T>`: Exclusive Access with Runtime Enforcement

`std::sync::Mutex<T>` wraps a `T` and enforces exclusive access at runtime. Calling `.lock()` either acquires the lock immediately or blocks the calling thread until the current holder releases it. The return value is a `MutexGuard<T>`, which implements `DerefMut`, giving you an `&mut T` to the protected data. Dropping the guard releases the lock.

Rust's `Mutex` differs from mutex types in C or C++ in one critical way: the data is *inside* the mutex. It is impossible to access the protected value without going through the lock. There is no separate "remember to lock before you touch this" convention; the type enforces it structurally.

**Lock poisoning.** If a thread panics while holding a `MutexGuard`, the mutex is marked as *poisoned*. Subsequent calls to `.lock()` return an `Err` containing the guard. This is a signal that the protected data may be in an inconsistent state. In production Meridian code, the correct response depends on whether partial writes to that data can leave it in a state that violates invariants other threads depend on. Most implementations either propagate the panic (`.unwrap()`) or inspect and correct the inconsistent state before clearing the poison via `PoisonError::into_inner()`.

**Deadlock.** `Mutex` provides no deadlock detection. If two threads each hold a lock and each attempt to acquire the other's lock, both block indefinitely. The standard mitigations: establish a consistent lock acquisition order across all code paths, minimize lock scope, and prefer data structures that require only one lock at a time.

### `RwLock<T>`: Optimizing Read-Heavy Workloads

`std::sync::RwLock<T>` extends `Mutex<T>` with the distinction between shared and exclusive access. Multiple readers can hold `RwLockReadGuard` simultaneously; a writer requires exclusive access and blocks until all readers have released their guards.

This is the right choice when the protected data is read frequently and written rarely — configuration tables, routing maps, ground station capability registries. For data written at high frequency, an `RwLock` can perform worse than a `Mutex` due to the overhead of tracking the reader count.

**Writer starvation.** If readers continuously acquire the lock, a waiting writer may never get access. Most platform `RwLock` implementations — including Rust's standard library implementation, which delegates to the OS — block new readers when a writer is waiting, preventing this. The behavior is platform-dependent, which is relevant if your code must run on multiple OS targets.

---

## Code Examples

### Diagnosing a `Send` Violation in the Session Registry

The Python control plane passes mutable session state dictionaries between threads using shared references — a pattern that Rust's type system will reject outright. This example shows what that rejection looks like and the correct fix.

```rust
use std::rc::Rc;
use std::sync::Arc;
use std::thread;

/// Represents the state of an active uplink session.
struct SessionState {
    satellite_id: u32,
    bytes_received: u64,
}

fn start_session_logger_broken() {
    // Rc uses non-atomic reference counting — not safe to share across threads.
    let state = Rc::new(SessionState {
        satellite_id: 7,
        bytes_received: 0,
    });

    // COMPILE ERROR: `Rc<SessionState>` cannot be sent between threads safely.
    // The trait `Send` is not implemented for `Rc<SessionState>`.
    // thread::spawn(move || {
    //     println!("logging session for satellite {}", state.satellite_id);
    // });
}

fn start_session_logger_correct() {
    // Arc uses atomic reference counting — safe to clone and send across threads.
    let state = Arc::new(SessionState {
        satellite_id: 7,
        bytes_received: 0,
    });

    // Clone before moving into the thread so the original handle remains valid.
    let state_for_thread = Arc::clone(&state);

    let handle = thread::spawn(move || {
        println!("logging session for satellite {}", state_for_thread.satellite_id);
    });

    // The main thread retains its own Arc and can continue using `state`.
    println!("spawned logger for satellite {}", state.satellite_id);
    handle.join().unwrap(); // propagate panics from the spawned thread
}
```

The compiler error from the commented-out code names `Rc<SessionState>` and `Send` precisely. This is not a runtime failure that shows up in testing — it's a build failure. The fix is mechanical: replace `Rc` with `Arc` when data crosses thread boundaries. The `Arc` imposes an atomic increment/decrement cost on clone and drop; that cost is appropriate for cross-thread ownership, and irrelevant when `Rc` suffices within a single thread.

---

### Shared Session Counter with `Arc<Mutex<T>>`

Multiple Tokio worker threads and OS threads may concurrently update the active session count for each satellite as connections open and close. This requires shared mutable access.

```rust
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;

type SatelliteId = u32;

/// Tracks the number of active uplink sessions per satellite.
/// Wrapped in Arc so multiple threads can share ownership without
/// either thread being "the owner" that outlives the other.
#[derive(Clone)]
struct SessionRegistry {
    counts: Arc<Mutex<HashMap<SatelliteId, u32>>>,
}

impl SessionRegistry {
    fn new() -> Self {
        SessionRegistry {
            counts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn open_session(&self, satellite_id: SatelliteId) {
        // lock() blocks until the mutex is available.
        // unwrap() here propagates mutex poisoning — if another thread panicked
        // while holding this lock, the map may be inconsistent.
        let mut counts = self.counts.lock().unwrap();
        *counts.entry(satellite_id).or_insert(0) += 1;
        // `counts` (the MutexGuard) is dropped here, releasing the lock.
        // Do not hold the guard across expensive operations or I/O.
    }

    fn close_session(&self, satellite_id: SatelliteId) {
        let mut counts = self.counts.lock().unwrap();
        if let Some(count) = counts.get_mut(&satellite_id) {
            // Saturating sub prevents underflow if close is called without a prior open —
            // possible during a control plane restart that inherits partial state.
            *count = count.saturating_sub(1);
        }
    }

    fn active_sessions(&self, satellite_id: SatelliteId) -> u32 {
        // Read and release immediately — don't hold the lock while formatting
        // log output or performing any other work.
        *self.counts.lock().unwrap().get(&satellite_id).unwrap_or(&0)
    }
}

fn simulate_concurrent_connections() {
    let registry = SessionRegistry::new();

    let handles: Vec<_> = (0..4)
        .map(|thread_id| {
            let registry = registry.clone(); // clone the Arc, not the HashMap
            thread::spawn(move || {
                let satellite_id = (thread_id % 3) as u32; // distribute across 3 satellites
                registry.open_session(satellite_id);
                // simulate work
                thread::sleep(std::time::Duration::from_millis(10));
                registry.close_session(satellite_id);
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // All sessions should be closed; counts should be zero.
    for sat_id in 0..3_u32 {
        assert_eq!(registry.active_sessions(sat_id), 0);
    }
}
```

The `Arc::clone` in the closure copies the reference-counted pointer, not the underlying `HashMap`. All four threads share the same allocation. The `Mutex` serializes their access to it. Notice that `SessionRegistry::clone()` is cheap — it clones an `Arc`, not the data it points to.

A common mistake in this pattern is holding the `MutexGuard` across `.await` points in async code or across blocking I/O calls in threaded code. That holds the lock for the entire duration of the blocking operation, serializing all other threads that need the map. The `close_session` implementation above is structured to release the guard at the end of the block. In async code, you must explicitly drop the guard before the first `.await`, or use `tokio::sync::Mutex` which is designed to be held across await points.

---

### Read-Heavy Configuration with `Arc<RwLock<T>>`

Ground station capability configurations are set at startup and updated only during maintenance windows, but are read on every inbound packet to determine routing. A `Mutex` would serialize all reads unnecessarily.

```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug)]
struct StationConfig {
    max_uplink_rate_kbps: u32,
    supported_frequencies: Vec<u32>,
    is_active: bool,
}

type StationId = String;

struct StationConfigRegistry {
    configs: Arc<RwLock<HashMap<StationId, StationConfig>>>,
}

impl StationConfigRegistry {
    fn new() -> Self {
        StationConfigRegistry {
            configs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Called infrequently — during maintenance windows or initial load.
    fn update_config(&self, station_id: StationId, config: StationConfig) {
        // write() blocks until all readers have released their guards.
        // On most platforms, new readers are also blocked once a writer is waiting,
        // preventing writer starvation.
        let mut configs = self.configs.write().unwrap();
        configs.insert(station_id, config);
    }

    /// Called on every inbound packet. Multiple threads can execute this
    /// concurrently — the RwLock allows concurrent read access.
    fn get_config(&self, station_id: &str) -> Option<StationConfig> {
        let configs = self.configs.read().unwrap();
        configs.get(station_id).cloned()
        // Read guard drops here, allowing other readers (and waiting writers) to proceed.
    }

    fn deactivate_station(&self, station_id: &str) {
        let mut configs = self.configs.write().unwrap();
        if let Some(config) = configs.get_mut(station_id) {
            config.is_active = false;
        }
    }
}
```

Under a read-heavy workload — thousands of `get_config` calls per second from packet handlers, with `update_config` called perhaps once an hour — this structure allows near-lock-free read throughput while still serializing the rare writes. Under a write-heavy workload, the cost of tracking readers would eat into that advantage; prefer `Mutex` when write frequency is comparable to read frequency.

---

### The `MutexGuard` Lifetime Pitfall

This example demonstrates a non-obvious hazard from *Rust Atomics and Locks* (Chapter 1) that causes guards to live longer than expected, holding locks longer than intended.

```rust
use std::sync::{Arc, Mutex};

struct CommandQueue {
    pending: Arc<Mutex<Vec<String>>>,
}

impl CommandQueue {
    fn new() -> Self {
        CommandQueue {
            pending: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn push(&self, cmd: String) {
        self.pending.lock().unwrap().push(cmd);
    }

    /// INCORRECT: The MutexGuard is kept alive for the entire duration of
    /// the if-let block, including while `process_command` runs.
    /// Any other thread attempting to enqueue a command will block for the
    /// entire processing duration.
    fn process_next_incorrect(&self) {
        if let Some(cmd) = self.pending.lock().unwrap().pop() {
            // Lock is still held here. The MutexGuard is a temporary in the
            // if-let expression and lives until the closing brace.
            process_command(&cmd);
        }
    }

    /// CORRECT: Pop under the lock, release the lock, then process.
    /// The lock is held only for the duration of the pop operation.
    fn process_next_correct(&self) {
        let cmd = self.pending.lock().unwrap().pop();
        // Guard is dropped at the end of the statement above. Lock is released.
        if let Some(cmd) = cmd {
            process_command(&cmd); // lock not held during processing
        }
    }
}

fn process_command(cmd: &str) {
    // Potentially slow: network I/O, serialization, logging.
    println!("processing: {cmd}");
}
```

The `if let Some(cmd) = self.pending.lock().unwrap().pop()` form is idiomatic-looking but semantically wrong for this use case. Temporaries in an `if let` pattern — including the `MutexGuard` — live until the closing brace of the `if let` block, not just the condition expression. The fix is a separate `let` binding, which drops the guard at the end of its statement. This is one of the few cases where a less-compact style is mechanically required for correctness.

---

## Key Takeaways

- `Send` and `Sync` are compile-time contracts, not runtime checks. A type that is not `Send` cannot cross a thread boundary in safe code — the compiler rejects it at the call site. There is no equivalent to Python's unchecked thread-unsafe sharing.
- Use `Rc<T>` for single-threaded shared ownership; use `Arc<T>` when ownership must be shared across threads. The difference is the cost of atomic reference counting operations, which is negligible on modern hardware but nonzero — don't pay it when you don't need to.
- `Arc<T>` provides shared read access only. For shared mutation, the pattern is `Arc<Mutex<T>>` or `Arc<RwLock<T>>`. The choice between them depends on read/write ratio: `RwLock` wins under read-heavy workloads; `Mutex` wins when writes are frequent or write latency is critical.
- Deadlock, lock poisoning, and writer starvation are three distinct failure modes. The compiler eliminates data races; the other three remain your responsibility. Design lock acquisition order deliberately, handle `PoisonError` explicitly in state-critical code, and verify your platform's `RwLock` writer-starvation behavior if your target includes non-Linux systems.
- `MutexGuard` lifetime is determined by the scope rules of the expression containing it, not by when you "finish" with the data. In `if let`, `match`, and `while let` expressions, temporaries live until the closing brace of the arm. Use a separate `let` binding to force early release.
- Minimize lock scope. Holding a `Mutex` or `RwLock` across blocking I/O, `.await` points, or expensive computations serializes all other contending threads for that entire duration, eliminating the parallelism the lock was intended to enable.

---

{{#quiz ../quizzes/rusts_concurrency_model.toml}}