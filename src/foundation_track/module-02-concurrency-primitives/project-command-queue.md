# Project — Ground Station Command Queue

**Module:** Foundation — M02: Concurrency Primitives  
**Prerequisite:** All three module quizzes passed (≥70%)

---
<!-- toc -->
---
## Mission Brief

**TO:** Platform Engineering  
**FROM:** Mission Operations Lead  
**CLASSIFICATION:** UNCLASSIFIED // INTERNAL  
**SUBJECT:** RFC-0044 — Priority Command Queue Implementation

---

The legacy Python command queue used a global dictionary with a threading lock. Over the past six months it has been involved in four production incidents: two deadlocks from re-entrant locking, one priority inversion where a low-priority housekeeping command blocked an emergency SAFE_MODE injection, and one data race when a monitoring process read the queue mid-write.

The replacement must be a typed, concurrent priority queue in Rust. It accepts mission-critical commands from multiple concurrent ground network connections, dispatches them in priority order to the session controller, and exposes lock-free metrics to the monitoring system — without the failure modes of the Python implementation.

---

## System Specification

### Command Model

Commands have a u8 priority (0 = lowest, 255 = highest). Predefined priorities:

| Command type | Priority |
|---|---|
| `SAFE_MODE` | 255 |
| `ABORT_PASS` | 200 |
| `REPOINT` | 100 |
| `STATUS_REQUEST` | 50 |
| `HOUSEKEEPING` | 10 |

A `Command` struct:

```rust,edition2021
#[derive(Debug, Eq, PartialEq)]
pub struct Command {
    pub priority: u8,
    pub kind: CommandKind,
    pub issued_at: std::time::Instant,
}

#[derive(Debug, Eq, PartialEq)]
pub enum CommandKind {
    SafeMode,
    AbortPass,
    Repoint { azimuth: f32, elevation: f32 },
    StatusRequest,
    Housekeeping,
}
```

### Queue Behaviour

- Multiple producer threads (one per ground station connection) push commands concurrently.
- One consumer thread (the session dispatcher) pops the highest-priority command. If multiple commands share the same priority, the oldest (by `issued_at`) is dispatched first.
- When the queue is empty, the consumer blocks without busy-waiting.
- The queue has a configurable capacity. If full, a push from a producer blocks until space is available. Blocking producers must not block the consumer.

### Metrics

The following counters are maintained lock-free and available to the monitoring system without acquiring any lock:

- `commands_pushed` — total commands ever pushed (all priorities)
- `commands_dispatched` — total commands ever dispatched
- `safe_mode_count` — number of SAFE_MODE commands dispatched (priority 255)

### Shutdown

The queue accepts a shutdown signal. On shutdown:

1. No new pushes are accepted — producers get an `Err(QueueShutdown)`.
2. The consumer drains any remaining commands in priority order.
3. Once the queue is empty and shutdown is signalled, the consumer returns.

---

## Expected Output

A library crate (`meridian-cmdqueue`) with:

- A `CommandQueue` type with `push`, `pop`, and `shutdown` methods
- An `Arc<Metrics>` accessible from the `CommandQueue` with the three lock-free counters
- A binary that demonstrates: 3 producer threads pushing 5 commands each, 1 consumer thread dispatching them in priority order, a monitoring thread sampling metrics every 20ms, and shutdown after all producers finish

The output should clearly show commands being dispatched in priority order (not FIFO).

---

## Acceptance Criteria

| # | Criterion | Verifiable |
|---|---|---|
| 1 | Commands dispatched in priority order (highest first, then oldest-first within priority) | Yes — log output order |
| 2 | Consumer blocks without busy-waiting when queue is empty | Yes — no >5% CPU when idle (measure with `top`) |
| 3 | Multiple concurrent producers do not cause data races | Yes — runs under `cargo test` with `--test-threads=1` via `loom` or ThreadSanitizer |
| 4 | Metrics counters are readable without acquiring any queue lock | Yes — code review: metrics accessed via atomics only |
| 5 | Shutdown drains remaining commands before consumer exits | Yes — log shows all pushed commands dispatched before exit |
| 6 | Producer push blocks (does not drop commands) when queue is at capacity | Yes — test with capacity=2 and 10 concurrent pushes |
| 7 | No `.unwrap()` on `Mutex::lock()` without a comment on the invariant | Yes — code review |

---

## Hints

<details>
<summary>Hint 1 — Implementing priority + FIFO ordering on BinaryHeap</summary>

`BinaryHeap` is a max-heap. To get "highest priority first, then oldest first within the same priority," implement `Ord` on `Command` to compare first by priority (descending), then by `issued_at` (ascending — older is higher priority):

```rust,edition2021
use std::cmp::Ordering;
use std::time::Instant;

struct Command { priority: u8, issued_at: Instant }

impl Ord for Command {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority)
            .then_with(|| other.issued_at.cmp(&self.issued_at)) // older = higher
    }
}
impl PartialOrd for Command {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl PartialEq for Command {
    fn eq(&self, other: &Self) -> bool { self.priority == other.priority && self.issued_at == other.issued_at }
}
impl Eq for Command {}
```

</details>

<details>
<summary>Hint 2 — Blocking push with capacity using Mutex + two Condvars</summary>

Two `Condvar`s: one signals "not full" (wake a blocked producer), one signals "not empty" (wake the consumer):

```rust,edition2021
use std::sync::{Mutex, Condvar};
use std::collections::BinaryHeap;

struct QueueInner<T> {
    heap: BinaryHeap<T>,
    capacity: usize,
    shutdown: bool,
}

struct CommandQueue<T> {
    inner: Mutex<QueueInner<T>>,
    not_empty: Condvar,
    not_full: Condvar,
}
```

Push blocks on `not_full` when the heap is at capacity; pop blocks on `not_empty` when the heap is empty. Each operation notifies the other condvar after completing.

</details>

<details>
<summary>Hint 3 — Lock-free metrics with atomics</summary>

Counters increment in push and pop, which both hold the mutex. But the monitoring thread must read without the mutex. Use atomics for all three counters — write from inside the locked section (ordering is Relaxed since the mutex provides the actual happens-before relationship), read from the monitoring thread with Relaxed:

```rust,edition2021
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

pub struct Metrics {
    pub commands_pushed: AtomicU64,
    pub commands_dispatched: AtomicU64,
    pub safe_mode_count: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            commands_pushed: AtomicU64::new(0),
            commands_dispatched: AtomicU64::new(0),
            safe_mode_count: AtomicU64::new(0),
        })
    }
}
```

</details>

<details>
<summary>Hint 4 — Shutdown drain sequence</summary>

Set `shutdown = true` in the inner state while holding the mutex, then `notify_all()` on both condvars. In `push`, check `shutdown` after acquiring the lock and return `Err` if set. In `pop`, check `shutdown && heap.is_empty()` — if both are true, return `None` to signal the consumer to exit:

```rust,edition2021
// In pop:
let mut inner = self.inner.lock().unwrap();
loop {
    if let Some(cmd) = inner.heap.pop() {
        self.not_full.notify_one();
        return Some(cmd);
    }
    if inner.shutdown {
        return None; // Queue is empty and shutdown — consumer exits
    }
    inner = self.not_empty.wait(inner).unwrap();
}
```

</details>

---

## Reference Implementation

<details>
<summary>Reveal reference implementation</summary>

```rust,edition2021
// src/lib.rs
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

#[derive(Debug)]
pub struct QueueShutdown;

impl std::fmt::Display for QueueShutdown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "command queue is shut down")
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum CommandKind {
    SafeMode,
    AbortPass,
    Repoint { azimuth: u32, elevation: u32 },
    StatusRequest,
    Housekeeping,
}

#[derive(Debug, Eq, PartialEq)]
pub struct Command {
    pub priority: u8,
    pub kind: CommandKind,
    pub issued_at: Instant,
}

impl Ord for Command {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            // Within the same priority, older commands go first.
            .then_with(|| other.issued_at.cmp(&self.issued_at))
    }
}
impl PartialOrd for Command {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct Metrics {
    pub commands_pushed: AtomicU64,
    pub commands_dispatched: AtomicU64,
    pub safe_mode_count: AtomicU64,
}

impl Metrics {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            commands_pushed: AtomicU64::new(0),
            commands_dispatched: AtomicU64::new(0),
            safe_mode_count: AtomicU64::new(0),
        })
    }
}

struct Inner {
    heap: BinaryHeap<Command>,
    capacity: usize,
    shutdown: bool,
}

pub struct CommandQueue {
    inner: Mutex<Inner>,
    not_empty: Condvar,
    not_full: Condvar,
    pub metrics: Arc<Metrics>,
}

impl CommandQueue {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                heap: BinaryHeap::with_capacity(capacity),
                capacity,
                shutdown: false,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
            metrics: Metrics::new(),
        })
    }

    pub fn push(&self, cmd: Command) -> Result<(), QueueShutdown> {
        let mut inner = self.inner.lock().unwrap();
        loop {
            if inner.shutdown {
                return Err(QueueShutdown);
            }
            if inner.heap.len() < inner.capacity {
                let is_safe_mode = matches!(cmd.kind, CommandKind::SafeMode);
                inner.heap.push(cmd);
                // Relaxed: the mutex provides the happens-before. These are
                // statistics only — no cross-variable ordering needed.
                self.metrics.commands_pushed.fetch_add(1, Relaxed);
                if is_safe_mode {
                    self.metrics.safe_mode_count.fetch_add(1, Relaxed);
                }
                self.not_empty.notify_one();
                return Ok(());
            }
            // Queue full — block until space opens or shutdown.
            inner = self.not_full.wait(inner).unwrap();
        }
    }

    pub fn pop(&self) -> Option<Command> {
        let mut inner = self.inner.lock().unwrap();
        loop {
            if let Some(cmd) = inner.heap.pop() {
                self.metrics.commands_dispatched.fetch_add(1, Relaxed);
                self.not_full.notify_one();
                return Some(cmd);
            }
            if inner.shutdown {
                return None;
            }
            inner = self.not_empty.wait(inner).unwrap();
        }
    }

    pub fn shutdown(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.shutdown = true;
        // Wake all blocked producers and the consumer.
        self.not_empty.notify_all();
        self.not_full.notify_all();
    }
}
```

```rust,edition2021
// src/main.rs (demonstration binary)
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn main() {
    // Inline the relevant types for the playground demo
    // (in the real crate, use `use meridian_cmdqueue::*`)

    tracing_subscriber::fmt::init();

    let queue = CommandQueue::new(20);
    let metrics = Arc::clone(&queue.metrics);

    // Three producer threads — simulate ground network connections.
    let producers: Vec<_> = (0..3u8).map(|gs| {
        let q = Arc::clone(&queue);
        thread::spawn(move || {
            let priorities = [255u8, 200, 100, 50, 10];
            for &priority in &priorities {
                thread::sleep(Duration::from_millis(gs as u64 * 5));
                let kind = match priority {
                    255 => CommandKind::SafeMode,
                    200 => CommandKind::AbortPass,
                    100 => CommandKind::Repoint { azimuth: 180, elevation: 45 },
                    50  => CommandKind::StatusRequest,
                    _   => CommandKind::Housekeeping,
                };
                match q.push(Command { priority, kind, issued_at: Instant::now() }) {
                    Ok(()) => println!("gs-{gs}: pushed priority {priority}"),
                    Err(e) => println!("gs-{gs}: push rejected — {e}"),
                }
            }
        })
    }).collect();

    // Consumer thread — session dispatcher.
    let q = Arc::clone(&queue);
    let consumer = thread::spawn(move || {
        while let Some(cmd) = q.pop() {
            println!("dispatcher: {:?} (priority {})", cmd.kind, cmd.priority);
            thread::sleep(Duration::from_millis(10));
        }
        println!("dispatcher: queue drained, exiting");
    });

    // Monitoring thread.
    let monitor = thread::spawn(move || {
        for _ in 0..4 {
            thread::sleep(Duration::from_millis(20));
            println!(
                "metrics: pushed={} dispatched={} safe_mode={}",
                metrics.commands_pushed.load(std::sync::atomic::Ordering::Relaxed),
                metrics.commands_dispatched.load(std::sync::atomic::Ordering::Relaxed),
                metrics.safe_mode_count.load(std::sync::atomic::Ordering::Relaxed),
            );
        }
    });

    for p in producers { p.join().unwrap(); }
    queue.shutdown();

    consumer.join().unwrap();
    monitor.join().unwrap();
}
```

</details>

---

## Reflection

The command queue built here uses all three concurrency layers from this module: OS threads for the producer and consumer, `Mutex` + `Condvar` for blocking coordination, and atomics for the metrics that must be readable without acquiring any lock. The relationship between these layers — the mutex providing the happens-before for the atomic writes, the condvar providing the non-busy-waiting block, the atomics avoiding any lock on the read path — is the pattern used throughout the Meridian control plane.

The natural next question: the blocking push is correct but puts an upper bound on producer throughput. In Module 3, this queue is extended with a `tokio::sync::mpsc` front-end that moves the backpressure into async channel semantics rather than blocking OS threads.