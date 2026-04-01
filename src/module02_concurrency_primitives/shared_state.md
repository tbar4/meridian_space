# Lesson 2 — Shared State: Mutex, RwLock, and Avoiding Deadlocks

**Module:** Foundation — M02: Concurrency Primitives  
**Position:** Lesson 2 of 3  
**Source:** *Rust Atomics and Locks* — Mara Bos, Chapter 1

---

## Context

The Meridian command queue maintains a shared priority table: incoming operator commands are written by the command ingress task, read by the session dispatch task, and occasionally queried by the monitoring dashboard. The Python system used a global dictionary with a threading lock. In production, that lock has been involved in three separate deadlock incidents — two in the same deployment week — all caused by the same root pattern: lock acquired, function called, that function also acquires the same lock.

Rust does not prevent deadlocks at compile time. But it gives you the tools to reason about them precisely: `Mutex<T>` and `RwLock<T>` make the protected data visible in the type signature, `MutexGuard` makes it impossible to access data without holding the lock, and RAII makes it impossible to forget to release it. This lesson covers how these primitives work, the failure modes that remain after Rust's type system has done its job, and the patterns that prevent them.

*Source: Rust Atomics and Locks, Chapter 1 (Bos)*

---

## Core Concepts

### `Mutex<T>` — Exclusive Access with RAII

`std::sync::Mutex<T>` wraps a value of type `T` and enforces that only one thread can access it at a time. The data is inaccessible without locking. There is no way to accidentally read `T` without going through `.lock()`.

`.lock()` returns `LockResult<MutexGuard<'_, T>>`. The `MutexGuard` dereferences to `T` and automatically releases the lock when it drops. There is no `.unlock()` method. The lock is released when the guard goes out of scope — or, critically, when it is explicitly dropped.

```rust,edition2021
use std::sync::{Arc, Mutex};
use std::thread;

fn main() {
    let command_count = Arc::new(Mutex::new(0u64));

    let handles: Vec<_> = (0..4).map(|_| {
        let counter = Arc::clone(&command_count);
        thread::spawn(move || {
            for _ in 0..1000 {
                // Lock is acquired here. Guard is dropped at end of block.
                let mut count = counter.lock().unwrap();
                *count += 1;
                // Guard dropped here — lock released before next iteration.
            }
        })
    }).collect();

    for h in handles { h.join().unwrap(); }
    println!("commands processed: {}", command_count.lock().unwrap());
}
```

The `Arc` provides shared ownership across threads (`Rc` is not `Send` and will not compile here). The `Mutex` provides exclusive access. This is the standard pattern for shared mutable state between threads.

### Lock Poisoning

When a thread panics while holding a `Mutex` lock, the mutex is marked poisoned. Subsequent calls to `.lock()` return `Err(PoisonError)`. The data is still accessible through the error — `err.into_inner()` returns the `MutexGuard` — but the poison signals that the data may be in an inconsistent state.

In practice, most Meridian code uses `.unwrap()` on mutex locks. This is deliberate: if a thread panics while holding the command queue lock, it is not safe to continue operating on potentially corrupted queue state. Propagating the panic is the correct response. The cases where you would recover from a poisoned mutex are rare and require domain-specific knowledge about what "inconsistent" means for that data.

One place where `.unwrap()` is wrong: in a test or in a thread that genuinely needs to clean up a partially-written state. In those cases, match on the `LockResult` explicitly.

### `MutexGuard` Lifetime — A Common Bug

The most common Mutex bug in Rust code is holding a guard longer than intended, or — worse — holding it across an `.await` point in async code. A guard held across an await parks the lock for the duration of the async operation. If another task tries to acquire the same lock, it will block the async worker thread (since `std::sync::Mutex::lock` blocks, not yields).

```rust,edition2021
use std::sync::Mutex;

fn main() {
    let data = Mutex::new(vec![1u32, 2, 3]);

    // BUG: guard lives to end of the if block, holding lock during the push
    {
        let guard = data.lock().unwrap();
        if guard.contains(&2) {
            drop(guard); // Must explicitly drop before re-locking.
            data.lock().unwrap().push(4);
        }
        // Without the explicit drop, this deadlocks: the guard is still
        // alive when we try to lock again at data.lock().unwrap().push(4)
    }

    println!("{:?}", data.lock().unwrap());
}
```

In async code, use `tokio::sync::Mutex` instead of `std::sync::Mutex`. It yields to the executor while waiting for the lock rather than blocking the thread. Conversely, never hold a `tokio::sync::MutexGuard` across a `.await` that might block for a long time — you are holding the lock for the duration of that await, which blocks all other lock waiters.

### `RwLock<T>` — Read Concurrency, Write Exclusivity

`RwLock<T>` distinguishes between reads and writes. Multiple readers can hold the lock simultaneously; a writer requires exclusive access. This is the concurrent version of `RefCell`.

It is appropriate when reads are frequent and writes are rare. For the Meridian session state table: many tasks read current session state, but writes only happen when sessions start or end. An `RwLock` allows those many concurrent reads without serializing them.

```rust,edition2021
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;

type SessionTable = Arc<RwLock<HashMap<u32, String>>>;

fn register_session(table: &SessionTable, id: u32, station: String) {
    // Write lock — exclusive.
    table.write().unwrap().insert(id, station);
}

fn query_session(table: &SessionTable, id: u32) -> Option<String> {
    // Read lock — concurrent with other readers.
    table.read().unwrap().get(&id).cloned()
}

fn main() {
    let table: SessionTable = Arc::new(RwLock::new(HashMap::new()));

    register_session(&table, 25544, "gs-svalbard".into());

    let readers: Vec<_> = (0..4).map(|_| {
        let t = Arc::clone(&table);
        thread::spawn(move || {
            // All four reader threads can hold the read lock simultaneously.
            println!("{:?}", query_session(&t, 25544));
        })
    }).collect();

    for r in readers { r.join().unwrap(); }
}
```

`RwLock` is not always faster than `Mutex`. If writes are frequent, readers pay the overhead of checking for pending writers. On some platforms, `RwLock` can starve writers if readers continuously hold the lock. Profile before committing to `RwLock` as an optimisation. For the common case of a hot write path with rare reads, `Mutex` is simpler and often faster.

### Deadlock Patterns and How to Prevent Them

A deadlock requires at least two resources and two threads acquiring them in opposite order. Rust's type system does not prevent this. Three patterns cause the vast majority of deadlocks in production:

**Lock ordering violation:**
Thread A acquires lock 1 then lock 2. Thread B acquires lock 2 then lock 1. Each holds what the other needs. Prevention: establish a global lock acquisition order and document it. If the command queue lock must always be acquired before the session table lock, enforce that convention in code review.

**Re-entrant locking:**
`std::sync::Mutex` is not reentrant. A thread that calls `.lock()` on a mutex it already holds will deadlock immediately — there is no second locking that succeeds. This is the source of Meridian's production incidents: a function that acquires the lock, calls a helper, and the helper also acquires the same lock.

Prevention: keep lock-holding code flat. Do not call functions while holding a lock unless you can verify they do not acquire the same lock. If a function is callable both with and without a lock held, split it into two versions or restructure the locking scope.

**Holding guards across blocking calls:**
In synchronous code: holding a `MutexGuard` while calling a function that blocks on I/O. In async code: holding a `std::sync::MutexGuard` across an `.await`.

Prevention: minimize the scope of guards. Acquire, mutate, release. Do not hold a lock while doing I/O. In async code, use `tokio::sync::Mutex` or restructure to release the lock before awaiting.

---

## Code Examples

### The Meridian Priority Command Queue

The command queue receives operator commands from the ground network interface. Commands have integer priorities. The session dispatcher reads the highest-priority pending command. Multiple ground network connections can write concurrently.

```rust,edition2021
use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::time::Duration;

#[derive(Eq, PartialEq)]
struct Command {
    priority: u8,
    payload: String,
}

impl Ord for Command {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority = higher value in max-heap.
        self.priority.cmp(&other.priority)
    }
}
impl PartialOrd for Command {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct CommandQueue {
    // Mutex + Condvar is the standard pattern for blocking producers/consumers.
    inner: Mutex<BinaryHeap<Command>>,
    available: Condvar,
}

impl CommandQueue {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(BinaryHeap::new()),
            available: Condvar::new(),
        })
    }

    fn push(&self, cmd: Command) {
        self.inner.lock().unwrap().push(cmd);
        // Notify one waiting consumer that data is available.
        self.available.notify_one();
    }

    fn pop_blocking(&self) -> Command {
        let mut queue = self.inner.lock().unwrap();
        // Condvar::wait releases the mutex and blocks until notified,
        // then reacquires the mutex before returning.
        loop {
            if let Some(cmd) = queue.pop() {
                return cmd;
            }
            queue = self.available.wait(queue).unwrap();
        }
    }
}

fn main() {
    let queue = CommandQueue::new();

    // Producer threads simulate ground network connections.
    let producers: Vec<_> = (0..3).map(|i| {
        let q = Arc::clone(&queue);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(i * 10));
            q.push(Command {
                priority: (i as u8 % 3) + 1,
                payload: format!("CMD-{i:04}"),
            });
            println!("producer {i}: pushed priority {}", (i as u8 % 3) + 1);
        })
    }).collect();

    // Consumer runs on a separate thread — simulates session dispatcher.
    let q = Arc::clone(&queue);
    let consumer = thread::spawn(move || {
        for _ in 0..3 {
            let cmd = q.pop_blocking();
            println!("dispatcher: executing '{}' (priority {})", cmd.payload, cmd.priority);
        }
    });

    for p in producers { p.join().unwrap(); }
    consumer.join().unwrap();
}
```

The `Condvar` solves the busy-wait problem: without it, the consumer would spin-lock on `queue.is_empty()`, wasting CPU. `Condvar::wait` atomically releases the mutex and parks the thread, then reacquires the mutex before returning. The `.unwrap()` on `lock()` is intentional: if a producer panics while holding the lock, corrupting the queue, the consumer should not continue silently.

---

## Key Takeaways

- `Mutex<T>` makes protected data inaccessible without locking. `MutexGuard` is the only way to reach the data, and it releases the lock on drop. There is no way to forget to unlock — but there are ways to hold the lock longer than intended.

- Lock poisoning marks a mutex as potentially inconsistent when a thread panics while holding it. Most production code uses `.unwrap()` on locks, propagating the panic. Recover from a poisoned mutex only when you can correct the inconsistent state.

- `RwLock<T>` allows concurrent reads and exclusive writes. It is appropriate when reads are dominant. It is not always faster than `Mutex` on write-heavy paths — profile before optimizing.

- Three deadlock patterns cover most production incidents: lock ordering violations (acquire in inconsistent order across threads), re-entrant locking (acquiring a lock you already hold), and holding guards across blocking calls. Document lock acquisition order and minimize guard scope.

- In async code, `std::sync::Mutex::lock` blocks the OS thread, which parks the async worker. Use `tokio::sync::Mutex` when the lock may be contended and the wait must yield to the executor. Never hold any `MutexGuard` across a slow `.await`.

- `Condvar` is the correct primitive for blocking on a data condition (waiting for a non-empty queue, waiting for a flag). It atomically releases the mutex and parks the thread, avoiding busy-waiting.

---

{{#quiz lesson-02-quiz.toml}}