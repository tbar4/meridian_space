# Module 02 — Concurrency Primitives

**Track:** Foundation — Mission Control Platform  
**Position:** Module 2 of 6  
**Source material:** *Rust Atomics and Locks* — Mara Bos, Chapters 1–3  
**Quiz pass threshold:** 70% on all three lessons to unlock the project

---

## Mission Context

The Meridian control plane is not a purely async system. The async runtime handles the high-frequency I/O path. But the control plane also runs CPU-bound conjunction checks, synchronous vendor libraries with C FFI, a shared priority command table written by multiple connections and read by the session dispatcher, and lock-free statistics counters sampled by the monitoring dashboard.

The Python system handled shared state with a global threading lock. In six months of operation, that lock has caused four production incidents. This module establishes the Rust concurrency model that replaces it — not by eliminating shared state, but by giving you the type-level guarantees and primitive toolkit to reason about it precisely.

---

## What You Will Learn

By the end of this module you will be able to:

- Distinguish OS threads from async tasks at the scheduling level, and route work to the correct model for its characteristics — blocking work to `spawn_blocking` or scoped threads, I/O-bound work to async tasks
- Use `Send` and `Sync` to reason about which types can cross thread boundaries, and understand why `Rc`, `Cell`, and raw pointers opt out
- Implement shared mutable state with `Mutex<T>` and `RwLock<T>`, manage `MutexGuard` lifetimes correctly, and handle lock poisoning appropriately
- Identify the three deadlock patterns that cause most production incidents and apply the structural patterns that prevent them
- Use `tokio::sync::Mutex` when locks must be held across `.await` points in async code
- Apply atomic operations (`fetch_add`, `compare_exchange`, `load`/`store`) and select the correct memory ordering (`Relaxed`, `Acquire`/`Release`, `SeqCst`) for the intended guarantee

---

## Lessons

### Lesson 1 — Threads vs Async Tasks: When to Use Each and Why

Covers `std::thread::spawn` vs `tokio::spawn`, the preemptive vs cooperative scheduling distinction, `thread::scope` for scoped threads with borrowed data, `Send` and `Sync` as the compiler's enforcement mechanism, and `spawn_blocking` as the bridge between the two models.

**Key question this lesson answers:** When a piece of work needs to happen concurrently, how do you decide between an OS thread and an async task — and what goes wrong if you choose incorrectly?

→ `lesson-01-threads-vs-async.md` / `lesson-01-quiz.toml`

---

### Lesson 2 — Shared State: Mutex, RwLock, and Avoiding Deadlocks

Covers `Mutex<T>` mechanics (locking, `MutexGuard`, RAII unlock, lock poisoning), `RwLock<T>` and the read-heavy access pattern, `MutexGuard` lifetime pitfalls in async code, `tokio::sync::Mutex`, `Condvar` for blocking on data conditions, and the three deadlock patterns with structural prevention strategies.

**Key question this lesson answers:** How do you share mutable data between threads correctly, and what are the failure modes that Rust's type system does not prevent?

→ `lesson-02-shared-state.md` / `lesson-02-quiz.toml`

---

### Lesson 3 — Atomics and Memory Ordering: Acquire/Release/SeqCst in Practice

Covers atomic types and the operations they support (`load`/`store`, `fetch_add`/`fetch_sub`, `compare_exchange`), memory ordering (`Relaxed`, `Acquire`/`Release`, `AcqRel`, `SeqCst`), the happens-before relationship established by the Acquire/Release pair, and the practical decision of when to use atomics vs a mutex.

**Key question this lesson answers:** When is a mutex overkill, and how do you safely share single values between threads without locking — including ensuring the processor and compiler do not reorder the operations that matter?

→ `lesson-03-atomics.md` / `lesson-03-quiz.toml`

---

## Capstone Project — Ground Station Command Queue

Build a typed, concurrent priority command queue for Meridian's mission operations system. The queue accepts commands from multiple concurrent ground station producer threads, dispatches them to a consumer in priority order with FIFO tie-breaking, blocks producers when at capacity (without dropping commands), exposes lock-free metrics readable without acquiring the queue lock, and shuts down gracefully by draining remaining commands.

Acceptance is against 7 verifiable criteria including correct priority dispatch, non-busy-waiting, lock-free metrics access, and clean shutdown drain.

→ `project-command-queue.md`

---

## Prerequisites

Module 1 (Async Rust Fundamentals) must be complete. This module assumes you understand how async tasks are scheduled and why blocking an async worker thread is harmful — that understanding is the foundation for the threads-vs-async distinction in Lesson 1 and the async Mutex guidance in Lesson 2.

## What Comes Next

**Module 3 — Message Passing Patterns** builds the next layer: rather than sharing state between concurrent actors, you pass ownership of data through channels. The command queue from this module's project is extended in Module 3 with a `tokio::sync::mpsc` front-end, moving backpressure into async channel semantics.