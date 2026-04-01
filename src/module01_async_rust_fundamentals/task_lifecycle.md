# Lesson 3 — Task Lifecycle: Cancellation, Timeouts, and JoinHandle Management

**Module:** Foundation — M01: Async Rust Fundamentals  
**Position:** Lesson 3 of 3  
**Source:** *Async Rust* — Maxwell Flitton & Caroline Morton, Chapters 2, 7

---

## Context

The Meridian control plane manages connections that span orbital passes. A ground station connection is live while the target satellite is above the horizon — typically 8 to 12 minutes. When the pass ends, the connection should be torn down cleanly: in-flight frames flushed, session state persisted, downstream consumers notified. If the control plane is restarted mid-pass — rolling deploy, crash recovery, OOM kill — active tasks must be cancelled in a way that does not corrupt shared state or leave downstream systems with partial data.

Understanding task lifecycle is not optional for this system. Tasks that outlive their useful scope waste resources. Tasks cancelled without cleanup leave corrupted state. Tasks that silently swallow their errors make incident response a guessing game. The Tokio `JoinHandle`, the `.abort()` call, and `tokio::time::timeout` are the instruments for managing these concerns; this lesson covers each one in depth.

*Source: Async Rust, Chapters 2 & 7 (Flitton & Morton)*

---

## Core Concepts

### The `JoinHandle` and Task Output

`tokio::spawn` returns `JoinHandle<T>`. The handle has two primary uses: waiting for the task's output with `.await`, and cancelling the task with `.abort()`.

`.await` on a `JoinHandle<T>` produces `Result<T, JoinError>`. `JoinError` indicates one of two things: the task panicked, or it was aborted. Distinguishing them matters:

```rust,edition2021
match handle.await {
    Ok(value) => { /* normal completion */ }
    Err(e) if e.is_panic() => { /* task panicked — log and recover */ }
    Err(e) if e.is_cancelled() => { /* task was aborted */ }
    Err(_) => unreachable!(),
}
```

If you drop a `JoinHandle` without awaiting it, the task continues running — it is not cancelled. This is the correct behavior for fire-and-forget tasks. If you need the task to stop when the handle is dropped, use `tokio_util::task::AbortOnDropHandle` (a wrapper that calls `.abort()` on drop) or implement the same pattern manually.

### Task Cancellation with `.abort()`

`.abort()` sends a cancellation signal to the task. The task does not stop immediately — it is cancelled at the next `.await` point. This is cooperative cancellation: the task's state machine is dropped when it next yields to the executor, which runs the `Drop` implementation of any held values.

The implication: resources guarded by RAII are dropped correctly on cancellation. A `tokio::net::TcpStream` held by the task will be closed. A `MutexGuard` will be released. A `tokio::fs::File` will be flushed and closed. What is not guaranteed: code after the `.await` where cancellation occurred will not run. If you have cleanup logic that must run regardless of cancellation, it must be in a `Drop` impl, not in code that follows an `.await`.

```rust,edition2021
// This cleanup logic may NOT run if the task is cancelled at the await:
async fn session_handler(id: u64) {
    process_frames().await; // <-- task may be cancelled here
    // The following line may never execute if aborted above.
    persist_session_state(id).await; // NOT guaranteed on cancellation
}

// This cleanup logic WILL run on cancellation because it is in Drop:
struct Session {
    id: u64,
    state: SessionState,
}

impl Drop for Session {
    fn drop(&mut self) {
        // Synchronous cleanup only — no async here.
        // Flush to a synchronous in-memory buffer; a separate housekeeping
        // task drains the buffer to persistent storage.
        tracing::info!(session_id = self.id, "session dropped, state buffered");
    }
}
```

### `CancellationToken` and `TaskTracker`

`broadcast` and `watch` channels work for shutdown signalling, but `tokio-util` provides two purpose-built primitives that are cleaner for the specific problem of cooperative shutdown.

**`CancellationToken`** is a cloneable, shareable cancellation handle. Any clone of a token represents the same cancellation event: when `.cancel()` is called on any one of them, all clones see it. Tasks wait on `.cancelled()`, which returns a future that resolves when the token is cancelled:

```rust,edition2021
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

async fn uplink_session(station_id: u32, token: CancellationToken) {
    loop {
        tokio::select! {
            // cancelled() is just a future — it composes naturally with select!.
            _ = token.cancelled() => {
                // Run async cleanup here before returning.
                // This is the key advantage over .abort(): we choose when to stop
                // and can flush state, send final messages, close connections.
                tracing::info!(station_id, "session received cancellation — draining");
                flush_pending_frames().await;
                break;
            }
            _ = process_next_frame(station_id) => {
                // Normal frame processing continues until token is cancelled.
            }
        }
    }
}

async fn flush_pending_frames() {
    sleep(Duration::from_millis(10)).await; // placeholder
}

async fn process_next_frame(_id: u32) {
    sleep(Duration::from_millis(50)).await; // placeholder
}

#[tokio::main]
async fn main() {
    let token = CancellationToken::new();

    // Clone the token for each task — all clones share the same cancellation.
    let handles: Vec<_> = (0..4)
        .map(|id| {
            let t = token.clone();
            tokio::spawn(uplink_session(id, t))
        })
        .collect();

    // Simulate running for a short time then shutting down.
    sleep(Duration::from_millis(120)).await;

    // Cancel all sessions simultaneously with one call.
    token.cancel();

    for handle in handles {
        let _ = handle.await;
    }
    tracing::info!("all sessions shut down");
}
```

The critical difference from `.abort()`: when the token fires, the task's `select!` arm runs, giving the task the opportunity to execute async cleanup before it exits. `.abort()` drops the future at the next `.await` with no opportunity for the task to run any further code.

`CancellationToken::child_token()` creates a child that is cancelled when the parent is cancelled, but can also be cancelled independently. Use this for hierarchical shutdown: cancel the top-level token to shut down everything, or cancel a child token to shut down one subsystem while leaving others running.

**`TaskTracker`** solves the drain-waiting problem more cleanly than collecting `JoinHandle`s into a `Vec`. Spawn tasks through the tracker; call `.close()` when no more tasks will be added; then `.wait()` to block until all tracked tasks finish:

```rust,edition2021
use tokio::time::{sleep, Duration};
use tokio_util::task::TaskTracker;

#[tokio::main]
async fn main() {
    let tracker = TaskTracker::new();
    let token = tokio_util::sync::CancellationToken::new();

    for station_id in 0..12u32 {
        let t = token.clone();
        tracker.spawn(async move {
            tokio::select! {
                _ = t.cancelled() => {
                    tracing::info!(station_id, "session shutting down");
                }
                _ = sleep(Duration::from_secs(300)) => {
                    tracing::info!(station_id, "session pass complete");
                }
            }
        });
    }

    // Signal that no more tasks will be spawned.
    // wait() will not resolve until close() has been called.
    tracker.close();

    // Trigger shutdown.
    sleep(Duration::from_millis(50)).await;
    token.cancel();

    // Block until all 12 sessions finish their cleanup.
    tracker.wait().await;
    tracing::info!("all sessions drained");
}
```

`tracker.wait()` only resolves after both conditions are true: all spawned tasks have completed, *and* `tracker.close()` has been called. The `close()` requirement prevents a race where `wait()` resolves between the last task finishing and the next one being spawned. Always call `close()` before `wait()`.

### `tokio::time::timeout`

`tokio::time::timeout(duration, future)` wraps any future and adds a deadline. If the future does not complete within the duration, it is cancelled and the wrapper returns `Err(tokio::time::error::Elapsed)`.

```rust,edition2021
use tokio::time::{timeout, Duration};

async fn fetch_frame_with_deadline(station: &str) -> anyhow::Result<Vec<u8>> {
    timeout(Duration::from_secs(5), fetch_frame(station))
        .await
        // Elapsed is returned as Err — map it to an application error.
        .map_err(|_| anyhow::anyhow!("ground station {station} timed out after 5s"))?
}
```

A critical detail: `timeout` cancels the inner future when the deadline fires — with the same cooperative semantics as `.abort()`. The future is dropped at its next `.await` point after the deadline. If the future holds a database transaction or has submitted writes that should be rolled back on timeout, the transaction handle's `Drop` must handle the rollback.

For scenarios where you want to retry on timeout, wrap the timeout in a loop. For scenarios where you want to give a task one deadline with no retry, `timeout` is the right primitive. For scenarios where you want to cancel based on an external signal (graceful shutdown, satellite pass end), use `CancellationToken` or `tokio::select!` with a shutdown receiver.

### `tokio::select!` for Racing Futures

`tokio::select!` polls multiple futures concurrently and completes with the first one that becomes ready, cancelling the others. It is the right tool for:

- Racing a task against a timeout
- Racing a task against a shutdown signal
- Implementing priority receive patterns on multiple channels

```rust,edition2021
use tokio::sync::oneshot;

async fn session_with_shutdown(
    session: impl std::future::Future<Output = ()>,
    mut shutdown: oneshot::Receiver<()>,
) {
    tokio::select! {
        _ = session => {
            tracing::info!("session completed normally");
        }
        _ = &mut shutdown => {
            // Shutdown signal received — session future is cancelled here.
            // RAII cleanup in the session's Drop runs.
            tracing::info!("session cancelled: shutdown signal received");
        }
    }
}
```

The branch that wins is executed; the branches that lose are cancelled (futures dropped at their next await point). If you need to do async cleanup when the losing branch is cancelled, you cannot do it inside `select!` — you need `CancellationToken` combined with a cleanup task.

**Important:** all branches of a `select!` run concurrently on the *same task*. They are never truly simultaneous — only one executes at a time — but they are polled in interleaved fashion within a single scheduler slot. This is distinct from `tokio::spawn`, which creates a new task that can run on a different worker thread. `select!` is lightweight concurrent multiplexing; `spawn` is independent parallel scheduling.

### `select!` Loop Patterns and Branch Preconditions

`select!` is most often used inside a loop. Two patterns come up constantly in production systems.

**Multi-channel drain with `else`:** when a session task needs to drain from multiple upstream channels until all are closed:

```rust,edition2021
use tokio::sync::mpsc;

async fn drain_uplinks(
    mut primary: mpsc::Receiver<Vec<u8>>,
    mut redundant: mpsc::Receiver<Vec<u8>>,
) {
    loop {
        tokio::select! {
            // select! randomly picks which ready branch to check first —
            // this prevents the redundant channel from always being starved
            // if the primary is consistently busy.
            Some(frame) = primary.recv() => {
                process_frame(frame, "primary");
            }
            Some(frame) = redundant.recv() => {
                process_frame(frame, "redundant");
            }
            // else fires when ALL patterns fail — both channels returned None,
            // meaning both are closed. This is the clean exit condition.
            else => {
                tracing::info!("all uplink channels closed — drain complete");
                break;
            }
        }
    }
}

fn process_frame(frame: Vec<u8>, source: &str) {
    tracing::debug!(bytes = frame.len(), source, "frame processed");
}
```

The `else` branch is not optional when you pattern-match on `Some(...)`. If both channels close and there is no `else`, `select!` will panic because no branch can make progress. Always include `else` when all branches use fallible patterns.

**Branch preconditions:** the `, if condition` syntax disables a branch before `select!` evaluates it. This is essential when polling a pinned future by reference inside a loop — once the future completes, the branch must be disabled or the next iteration will attempt to poll an already-resolved future, causing a panic:

```rust,edition2021
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

async fn catalog_refresh() -> Vec<u8> {
    sleep(Duration::from_millis(100)).await;
    vec![0u8; 128]
}

#[tokio::main]
async fn main() {
    let (_tx, mut cmd_rx) = mpsc::channel::<String>(8);

    let refresh = catalog_refresh();
    tokio::pin!(refresh);
    let mut refresh_done = false;

    for _ in 0..5 {
        tokio::select! {
            // Branch is disabled once refresh_done = true.
            // Without this precondition: panic on second iteration.
            result = &mut refresh, if !refresh_done => {
                println!("catalog refreshed: {} bytes", result.len());
                refresh_done = true;
            }
            Some(cmd) = cmd_rx.recv() => {
                println!("command: {cmd}");
            }
            else => break,
        }
    }
}
```

When the precondition is `false`, `select!` simply skips that branch. If *all* branches are disabled by preconditions, `select!` panics — so structure your logic to ensure at least one branch is always eligible or an `else` handles the case.

### Graceful Shutdown Pattern

A production service needs a defined shutdown sequence. For the Meridian control plane:

1. Stop accepting new connections.
2. Signal active session tasks to finish or cancel.
3. Wait for tasks to drain (with a deadline — do not wait forever).
4. Flush pending telemetry to downstream consumers.
5. Exit cleanly.

```rust,edition2021
use std::time::Duration;
use tokio::sync::broadcast;

struct ShutdownCoordinator {
    sender: broadcast::Sender<()>,
}

impl ShutdownCoordinator {
    fn new() -> Self {
        let (sender, _) = broadcast::channel(1);
        Self { sender }
    }

    fn subscribe(&self) -> broadcast::Receiver<()> {
        self.sender.subscribe()
    }

    async fn shutdown(&self, tasks: Vec<tokio::task::JoinHandle<()>>) {
        // Signal all subscribers.
        let _ = self.sender.send(());

        // Give tasks 10 seconds to drain. After that, abort stragglers.
        let deadline = Duration::from_secs(10);
        let _ = tokio::time::timeout(deadline, async {
            for handle in tasks {
                // Ignore individual task errors during shutdown.
                let _ = handle.await;
            }
        })
        .await;
    }
}
```

The coordinator sends a shutdown signal over a `broadcast` channel. Each session task holds a `Receiver` and uses `tokio::select!` to race its work against the shutdown signal. After broadcasting, `shutdown` awaits all handles with a 10-second deadline. Any task that has not completed by then is left to the OS — in a containerized environment, the container will be killed by the orchestrator anyway.

---

## Code Examples

### Managing a Satellite Pass Session with Full Lifecycle Control

A pass session has a well-defined lifetime: it starts when the satellite rises above the ground station horizon and ends when it sets. The session task must complete cleanly if the pass ends normally, abort gracefully on shutdown, and timeout if the satellite goes silent mid-pass (antenna tracking failure, power anomaly).

```rust,edition2021
use anyhow::Result;
use tokio::{
    sync::oneshot,
    time::{timeout, Duration},
};
use tracing::{info, warn};

#[derive(Debug)]
struct PassSession {
    satellite_id: u32,
    ground_station: String,
}

impl Drop for PassSession {
    fn drop(&mut self) {
        // Synchronous state flush — no async.
        // In production, push final state to a lock-free ring buffer
        // that a background writer drains to persistent storage.
        info!(
            satellite_id = self.satellite_id,
            ground_station = %self.ground_station,
            "PassSession dropped — flushing state synchronously"
        );
    }
}

impl PassSession {
    async fn run(&mut self) -> Result<()> {
        info!(
            satellite_id = self.satellite_id,
            "pass session started"
        );
        // Simulate frame processing loop.
        // In production: read frames from TcpStream, validate, forward.
        for frame_num in 0u32..100 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            info!(frame = frame_num, "frame processed");
        }
        Ok(())
    }
}

async fn manage_pass(
    satellite_id: u32,
    ground_station: String,
    pass_duration: Duration,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    let mut session = PassSession {
        satellite_id,
        ground_station,
    };

    // Race: session completion, pass duration timeout, or shutdown signal.
    tokio::select! {
        result = timeout(pass_duration, session.run()) => {
            match result {
                Ok(Ok(())) => info!(satellite_id, "pass completed normally"),
                Ok(Err(e)) => warn!(satellite_id, "session error: {e}"),
                Err(_) => warn!(satellite_id, "pass duration exceeded — session timed out"),
            }
        }
        _ = &mut shutdown_rx => {
            // PassSession::drop runs here, flushing state before the task exits.
            warn!(satellite_id, "pass cancelled: shutdown received");
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let handle = tokio::spawn(manage_pass(
        25544,
        "gs-svalbard".to_string(),
        Duration::from_secs(30),
        shutdown_rx,
    ));

    // Simulate shutdown signal after 1 second.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let _ = shutdown_tx.send(());

    match handle.await {
        Ok(Ok(())) => info!("task completed"),
        Ok(Err(e)) => warn!("task error: {e}"),
        Err(e) if e.is_cancelled() => warn!("task was aborted externally"),
        Err(e) => warn!("task panicked: {e}"),
    }

    Ok(())
}
```

Key decisions in this code: the `Drop` impl handles synchronous cleanup, which is guaranteed to run whether the session completes normally, times out, or is cancelled. The `select!` gives the session three possible exit paths with distinct log entries — observable, diagnosable behavior rather than silent state corruption. The outer `.await` on the handle distinguishes between clean task exit, application errors, external abort, and panics.

---

## Key Takeaways

- `JoinHandle<T>` awaits as `Result<T, JoinError>`. Distinguish between panics and cancellation using `e.is_panic()` / `e.is_cancelled()`. Never `.unwrap()` a `JoinHandle` in production code without a comment explaining the invariant.

- Dropping a `JoinHandle` does not cancel the task. Call `.abort()` explicitly if you need cancellation on drop. `.abort()` is cooperative — the task stops at its next `.await` point, not immediately.

- Async cleanup after an `.await` is not guaranteed on cancellation. Put mandatory cleanup in `Drop` (synchronous) or use `CancellationToken` to intercept the shutdown signal and run async teardown before the task exits.

- `tokio::time::timeout` wraps any future with a deadline. On expiry, it cancels the inner future at its next `.await`. Resources held by the cancelled future are dropped via RAII — no manual cleanup needed if your types implement `Drop` correctly.

- `tokio::select!` runs all branches on the same task — they multiplex, they do not parallelize. Branches randomly compete for selection when multiple are ready, which prevents starvation. Use `tokio::spawn` when you need true independent scheduling; use `select!` when you need lightweight concurrency within a single task.

- `select!` branch preconditions (`, if condition`) disable a branch before evaluation. Always use them with pinned futures in loops to prevent the "async fn resumed after completion" panic.

- In `select!` loops, always include an `else` branch when all active branches use fallible patterns like `Some(...)`. The `else` branch fires when all patterns fail to match — typically when all channels are closed — and provides the clean exit condition.

- `CancellationToken` (from `tokio-util`) is the preferred cancellation primitive for cooperative shutdown. Cloning shares the same cancellation event. `.cancelled().await` composes naturally with `select!` and, unlike `.abort()`, allows the task to run async cleanup before exiting.

- `TaskTracker` (from `tokio-util`) is the preferred drain primitive for shutdown. Spawn tasks through the tracker, call `.close()` when done spawning, then `.wait().await` to block until all tasks finish. This avoids the `JoinHandle` Vec pattern and correctly handles the close/wait ordering requirement.

---

{{#quiz module1-lesson-03-quiz.toml}}