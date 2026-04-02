# Lesson 1 — The async/await Model: Futures, Polling, and the Executor Loop

**Module:** Foundation — M01: Async Rust Fundamentals  
**Position:** Lesson 1 of 3  
**Source:** *Async Rust* — Maxwell Flitton & Caroline Morton, Chapters 1–2

---
<!-- toc -->
---
## Context

Meridian's legacy Python control plane was designed for a 6-satellite constellation. It handles ground station connections sequentially: accept a connection, process its telemetry frame, move to the next connection. At 6 satellites, this was acceptable. At 48 satellites across 12 ground station sites, it is a bottleneck. A single slow uplink from a station in the Atacama Desert holds up frames from every other active connection. The Python GIL makes true parallelism on this I/O-bound workload impossible without forking processes, which multiplies memory overhead and complicates shared state.

The replacement control plane is being written in Rust with `tokio`. Before writing any of that system, you need an accurate mental model of how async Rust actually executes code — not at the level of the `tokio` macro, but at the level of futures, the polling protocol, and the executor's task queue. Misunderstanding this model is the root cause of most async Rust bugs in production: dropped wakers, blocking the executor thread, and state machine explosions that are impossible to reason about.

This lesson covers the mechanics that every `await` desugars into. By the end, you should be able to read a future's `poll` implementation and trace exactly when it will make progress, when it will yield, and what will wake it back up. That skill is indispensable when debugging a hung ground station connection at 0300.

*Source: Async Rust, Chapters 1–2 (Flitton & Morton)*

---

## Core Concepts

### What Async Actually Is

Async programming does not add CPU cores. It reorganizes work so that dead time — waiting for a network response, waiting for a disk write — is used to make progress on other tasks. The classic analogy: you do not stand still while the kettle boils. You put the bread in the toaster. The key insight is that both tasks share one pair of hands but interleave their execution during wait periods.

In Rust, this interleaving is explicit and zero-cost. There is no runtime scheduler running on a background OS thread intercepting your code. Instead, you write state machines, and the Rust compiler compiles `async fn` into those state machines for you. `await` is a yield point — a place where the current task volunteers to give up the thread so another task can run.

This is the critical difference from threads: with threads, preemption is involuntary. With async tasks, yield is voluntary, at every `await`. A task that never hits an `await` — one that runs a tight CPU loop — will starve every other task on that executor thread. This is not hypothetical. In Meridian's uplink pipeline, a single malformed frame that triggers O(n²) validation holds the entire thread if there's no await in the hot path.

### The `Future` Trait

Every value produced by an `async fn` or an `async` block implements `Future`. The trait is:

```rust,edition2021
pub trait Future {
    type Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output>;
}
```

`Poll` has two variants: `Poll::Ready(value)` when the computation is complete, and `Poll::Pending` when the future cannot yet make progress and should be woken up later.

The `poll` function is not async. This matters: futures are polled synchronously. The executor calls `poll`, it runs synchronously until it either completes or hits a point where it cannot proceed, and then it returns. If it returns `Pending`, it is the future's responsibility to arrange for a wake-up. If it returns `Pending` without registering a waker, the task will never run again — a silent deadlock.

### The Waker Contract

`Context` carries a `Waker`. The `Waker` is a handle that, when called, schedules the associated task back onto the executor's run queue. The contract is: if `poll` returns `Pending`, it must have called `cx.waker().wake_by_ref()` or stored the waker to be called later when the awaited resource becomes available.

Violating this contract — returning `Pending` without registering the waker — produces a future that stalls forever with no error. The executor sees a pending task, never reschedules it, and the task silently vanishes from the run queue. At the Meridian scale, this manifests as a ground station connection that goes quiet mid-session: no error, no disconnect, just silence until the session timeout fires.

The executor side of this contract: when a waker is called, the executor re-queues the task and eventually calls `poll` again. The future may be polled many times before it completes. The state it needs to resume must be owned by the future struct itself — this is why async Rust desugars `async fn` into a struct that holds all local variables as fields.

### Pinning

The `poll` signature takes `Pin<&mut Self>` rather than `&mut Self`. `Pin` prevents the future from being moved in memory after it has been pinned. This matters because async state machines frequently contain self-referential structures: a future that awaits another future may hold a reference into its own fields. If the outer future were moved, that reference would dangle.

`Pin` enforces at compile time that once you call `poll`, the future cannot be moved. For futures composed entirely of `Unpin` types (most standard types), this is a no-op. For futures holding references into themselves — which the compiler generates automatically from `async fn` — it is essential.

Practical implication: you cannot call `poll` directly on a future obtained from an `async fn` without first pinning it via `Box::pin(future)` or `tokio::pin!(future)`. `tokio::spawn` handles this for you; you only encounter it directly when building custom executors or when polling a future by reference inside `select!`.

### `tokio::pin!` — Polling a Future by Reference

`tokio::pin!` pins a value to the current stack frame in place, making it safe to poll by mutable reference. The common situation where this matters: you need to start an async operation *once* and track its progress across multiple iterations of a `select!` loop, rather than restarting it fresh on every iteration.

Consider fetching a TLE catalog update while simultaneously processing incoming session commands. The fetch should run to completion in the background; the command loop should not restart it on each iteration:

```rust,edition2021
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

async fn fetch_tle_update() -> Vec<u8> {
    // Simulate a slow catalog fetch — ~200ms in production.
    sleep(Duration::from_millis(200)).await;
    vec![0u8; 64] // placeholder TLE payload
}

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel::<String>(8);

    // Spawn a sender to simulate incoming commands.
    tokio::spawn(async move {
        for cmd in ["REPOINT", "STATUS", "RESET"] {
            sleep(Duration::from_millis(60)).await;
            let _ = tx.send(cmd.to_string()).await;
        }
    });

    // Create the future ONCE, outside the loop.
    let tle_fetch = fetch_tle_update();
    // Pin it to the stack so we can poll it by reference (&mut tle_fetch).
    tokio::pin!(tle_fetch);

    let mut tle_done = false;

    loop {
        tokio::select! {
            // Poll the same future instance each iteration.
            // Without tokio::pin!, each iteration would call fetch_tle_update()
            // again, creating a brand-new future and discarding all progress.
            tle = &mut tle_fetch, if !tle_done => {
                println!("TLE update received: {} bytes", tle.len());
                tle_done = true;
            }
            Some(cmd) = rx.recv() => {
                println!("command received: {cmd}");
                if cmd == "RESET" { break; }
            }
            else => break,
        }
    }
}
```

Two things to notice. First, `tle_fetch` is created before the loop and pinned with `tokio::pin!`. Inside `select!`, `&mut tle_fetch` polls the *same* future on every iteration — it accumulates progress across polls. If you wrote `fetch_tle_update()` directly inside `select!`, you would get a new future each time and the fetch would restart from zero on every loop iteration.

Second, the `, if !tle_done` precondition disables the branch once the fetch has completed. This is essential: if the branch stays enabled after the future resolves, `select!` would attempt to poll an already-completed future on the next iteration, causing a `"async fn resumed after completion"` panic. The precondition guards against this. The next section covers preconditions in full.

### The Executor Loop

The executor maintains a run queue of tasks ready to be polled. Its loop is approximately:

1. Pop a task from the ready queue.
2. Call `poll` on it.
3. If `Poll::Ready`, the task is done — drop it.
4. If `Poll::Pending`, the task is parked. It will be re-queued only when its waker is called.

Tasks are not re-polled speculatively. They are polled exactly when woken. This means a task can sit in `Pending` state indefinitely if nothing triggers its waker — which is the correct behavior for a task waiting on a network connection that has gone silent.

`tokio::spawn` places a task on the executor's ready queue. `tokio::join!` polls multiple futures concurrently on the same task — no new OS threads, no new tasks, just interleaved polling within the same scheduler slot. `tokio::spawn` creates a new independent task that can be scheduled on any worker thread.

---

## Code Examples

### Implementing `Future` Directly — A Telemetry Frame Validator

This example implements `Future` manually to illustrate what `async/await` desugars into. In production this would be an `async fn`, but seeing the state machine explicitly clarifies exactly when control yields and what triggers resumption.

The scenario: validating an incoming telemetry frame header requires checking a CRC that is computed in a background thread pool. The future polls a oneshot channel for the result.

```rust,edition2021
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// Represents a frame whose header CRC is being validated asynchronously.
/// The validation runs on a blocking thread; this future waits for its result.
pub struct FrameValidationFuture {
    // oneshot::Receiver implements Future directly, but we wrap it here
    // to show the polling mechanics explicitly.
    receiver: oneshot::Receiver<bool>,
}

impl FrameValidationFuture {
    pub fn new(receiver: oneshot::Receiver<bool>) -> Self {
        Self { receiver }
    }
}

impl Future for FrameValidationFuture {
    type Output = Result<(), String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Pin::new is safe here because oneshot::Receiver is Unpin.
        // For a self-referential type we'd need unsafe or box-pinning.
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(Ok(true)) => Poll::Ready(Ok(())),
            Poll::Ready(Ok(false)) => {
                Poll::Ready(Err("CRC validation failed".to_string()))
            }
            Poll::Ready(Err(_)) => {
                // Sender dropped without sending — the validator thread panicked
                // or was cancelled. Treat as a validation failure, not a panic.
                Poll::Ready(Err("Validator thread terminated unexpectedly".to_string()))
            }
            // The result is not ready yet. The oneshot::Receiver has already
            // registered cx's waker — it will call it when a value is sent.
            // We return Pending; the executor parks this task.
            Poll::Pending => Poll::Pending,
        }
    }
}

#[tokio::main]
async fn main() {
    let (tx, rx) = oneshot::channel::<bool>();

    // Simulate the CRC validator running on a blocking thread pool.
    tokio::spawn(async move {
        // In production: tokio::task::spawn_blocking(|| compute_crc(...)).await
        // Here we just send a valid result immediately.
        let _ = tx.send(true);
    });

    let validation = FrameValidationFuture::new(rx);
    match validation.await {
        Ok(()) => println!("Frame header valid — forwarding to telemetry pipeline"),
        Err(e) => eprintln!("Frame rejected: {e}"),
    }
}
```

The `poll` implementation delegates to the inner `oneshot::Receiver`'s own `poll`. When `Receiver::poll` returns `Pending`, it has already stored the waker from `cx` internally. When `tx.send(true)` fires, `Receiver` calls that waker, which re-queues this task. No manual waker management is needed here because we compose with a type that already handles it correctly.

This is the pattern to follow when building custom futures: compose with existing futures and channel primitives wherever possible. Write `unsafe` waker code only when you are bridging to a non-async notification source (an epoll fd, a hardware interrupt, a C library callback).

### Concurrent Polling with `tokio::join!` vs. Sequential `await`

Sequential `await` for two telemetry frame fetches from different ground stations means the second fetch does not start until the first completes:

```rust,edition2021
// SEQUENTIAL — total latency = latency(station_a) + latency(station_b)
let frame_a = fetch_frame("gs-atacama").await?;
let frame_b = fetch_frame("gs-svalbard").await?;
```

`tokio::join!` polls both concurrently on the same task. While one is pending, the executor can drive the other forward:

```rust,edition2021
use anyhow::Result;
use tokio::net::TcpStream;

async fn fetch_frame(station_id: &str) -> Result<Vec<u8>> {
    // Simplified: in production this reads from a persistent connection pool.
    let mut _stream = TcpStream::connect(format!("{station_id}:7777")).await?;
    // ... read frame bytes ...
    Ok(vec![]) // placeholder
}

#[tokio::main]
async fn main() -> Result<()> {
    // CONCURRENT — total latency ≈ max(latency_a, latency_b)
    // Both futures are polled in the same task; no new OS threads are created.
    let (frame_a, frame_b) = tokio::join!(
        fetch_frame("gs-atacama"),
        fetch_frame("gs-svalbard")
    );

    // Both results are available here; handle errors independently.
    match (frame_a, frame_b) {
        (Ok(a), Ok(b)) => {
            println!("Received {} + {} bytes from ground stations", a.len(), b.len());
        }
        (Err(e), _) | (_, Err(e)) => {
            eprintln!("Ground station fetch failed: {e}");
        }
    }
    Ok(())
}
```

`tokio::join!` is appropriate when the futures are independent and you need both results. If you only need the first result and want to cancel the loser, use `tokio::select!`. If the futures have no data dependency and need to run across multiple threads simultaneously, `tokio::spawn` each and join the handles.

---

## Key Takeaways

- The `Future` trait's `poll` method is synchronous. An async runtime is a loop that calls `poll` on ready tasks; it does not preempt running tasks. A future that does significant CPU work without an `await` will monopolize its executor thread.

- If `poll` returns `Poll::Pending` without registering the context's waker, the task is silently parked forever. Always verify that the resource you're awaiting will call the waker when it becomes available.

- `Pin<&mut Self>` exists to prevent futures from being moved after polling begins. For futures containing self-referential state (which the compiler generates automatically), this is load-bearing. Most composed futures are `Unpin`; the constraint only bites when bridging to raw async primitives.

- `tokio::join!` achieves concurrency within a single task by interleaved polling. It does not create threads or new tasks. Use it for independent I/O operations that should proceed simultaneously but whose results you need together.

- `tokio::pin!` pins a future to the current stack frame so it can be polled by mutable reference across multiple `select!` iterations. Use it when you need to start an operation once and track its progress, not restart it on each loop. Always pair it with a precondition (`, if !done`) to prevent polling the future after it has already resolved.

- Every `async fn` is compiled into a state machine struct. Variables live across `await` points become fields of that struct. Understanding this explains why async Rust futures can be large, why they must be pinned, and why capturing large values across await points inflates memory use.

---

{{#quiz module1-lesson-01-quiz.toml}}