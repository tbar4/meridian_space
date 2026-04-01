# Lesson 2 — The Tokio Runtime: Spawning Tasks, the Scheduler, and Thread Pools

**Module:** Foundation — M01: Async Rust Fundamentals  
**Position:** Lesson 2 of 3  
**Source:** *Async Rust* — Maxwell Flitton & Caroline Morton, Chapter 7

---
<!-- toc -->
---
## Context

The Meridian control plane receives telemetry from 48 satellite uplinks simultaneously. Each uplink connection is long-lived: a ground station holds a persistent TCP session with the control plane and streams frames at irregular intervals driven by orbital geometry and antenna alignment. Alongside these connections, the control plane runs housekeeping tasks — session health checks, TLE refresh from the catalog, and periodic flush of buffered frames to the downstream aggregator.

The `#[tokio::main]` macro stands up a default multi-threaded runtime and runs your async main function in it. For prototyping and simple services, this is sufficient. For a system with the throughput profile and operational requirements of Meridian's control plane, you need to understand what that runtime is actually doing — how many threads it allocates, how it distributes work across them, what happens when a blocking operation enters the mix, and how to configure it for your actual workload rather than the defaults.

This lesson covers Tokio's scheduler architecture, the distinction between async tasks and blocking tasks, how to size thread pools for I/O-bound vs. compute-bound work, and how to configure the runtime explicitly via `Builder`. The goal is not to tune prematurely — it is to understand the model well enough to make deliberate choices rather than accepting defaults that may be wrong for your system.

*Source: Async Rust, Chapter 7 (Flitton & Morton)*

---

## Core Concepts

### The Multi-Thread Scheduler

Tokio's default `multi_thread` scheduler maintains a pool of worker threads — by default, one per logical CPU core. Each worker thread has a local run queue. Tasks spawned with `tokio::spawn` go onto a global run queue and are stolen by whichever worker thread is idle. This work-stealing design keeps all workers busy when there is backlog, at the cost of some cross-thread synchronization.

Each worker runs the same loop from Lesson 1: pop a ready task, call `poll`, re-queue it if it returns `Pending`, drop it if `Ready`. When a worker's local queue is empty, it attempts to steal tasks from other workers' queues before checking the global queue. The `global_queue_interval` configuration controls how many local-queue tasks a worker processes before checking the global queue — the default is 61. Lowering this value gives newly spawned tasks lower latency at the cost of more global-queue contention.

The `current_thread` runtime (used by `#[tokio::main]` in tests and the single-threaded mode) runs all tasks on the calling thread. It is appropriate for services that are purely I/O-bound with no CPU-intensive tasks and where single-threaded throughput is sufficient. The Meridian control plane uses the multi-thread runtime.

### Worker Threads and Blocking Threads

Tokio distinguishes between two kinds of threads:

**Worker threads** run the async executor loop. They poll futures and run async task code. There should be enough of them to saturate your I/O capacity without exceeding your core count. A typical production setting is `num_cpus::get()`, which `Builder::new_multi_thread()` uses by default.

**Blocking threads** are spawned on demand by `tokio::task::spawn_blocking`. They run synchronous, blocking code — file I/O, CPU-intensive computation, synchronous library calls — in a separate thread pool that does not interfere with the async worker threads. The key rule: **never perform blocking I/O or long CPU work directly on an async worker thread.** Doing so parks that thread for the duration, reducing effective parallelism and starving other tasks.

`max_blocking_threads` caps the number of blocking threads that can exist simultaneously. The default is 512. For the Meridian control plane, which may process TLE bulk imports concurrently with live uplinks, sizing this correctly prevents runaway thread creation under load spikes.

### `tokio::spawn` and Task Identity

`tokio::spawn` places a new task onto the runtime's global queue. It returns a `JoinHandle<T>` — a handle to the spawned task's eventual output. The task is independent of the spawner: if the spawner drops the handle, the task continues running (though its output is lost). If you need the task's output, keep the handle and `.await` it. If you need to cancel the task, call `.abort()` on the handle.

Spawned tasks must be `'static` — they cannot borrow from the spawning scope. If the task needs data from the spawner, move it in with `async move { ... }`, clone cheaply clonable data (like `Arc`-wrapped state), or use channels to communicate.

A common mistake is spawning a task per connection without any admission control. At 48 uplinks with 100 frames per second each, that is 4,800 task-spawns per second for frame processing alone. Task creation has overhead. For Meridian's frame processing workload, a bounded task pool or a pipeline of fixed workers is more appropriate than an unbounded spawn-per-frame pattern.

### Configuring the Runtime Explicitly

The `#[tokio::main]` macro is shorthand for building a default runtime and blocking on the async main function. Replacing it with an explicit `Builder` gives fine-grained control:

```rust,edition2021
use tokio::runtime::Builder;

fn main() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(8)
        .max_blocking_threads(16)
        .thread_name("meridian-worker")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("failed to build Tokio runtime");

    runtime.block_on(async_main());
}
```

The runtime is a value. You can have multiple runtimes in the same process — useful when you need strict resource isolation between subsystems (e.g., keeping the telemetry ingress runtime separate from the housekeeping runtime so a housekeeping spike does not starve active uplinks).

---

## Code Examples

### Explicit Runtime Configuration for the Meridian Control Plane

Meridian's control plane has two distinct workload profiles that benefit from isolated runtimes: the high-frequency telemetry ingress path (many short-lived I/O tasks) and the housekeeping path (fewer, slower tasks including blocking TLE catalog refreshes). Sharing a single runtime risks head-of-line blocking when a TLE import saturates the blocking thread pool.

```rust,edition2021
use std::sync::LazyLock;
use tokio::runtime::{Builder, Runtime};

// Ingress runtime: tuned for concurrent I/O — one worker per core,
// minimal blocking threads since real blocking work routes to the
// housekeeping runtime.
static INGRESS_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .max_blocking_threads(4)
        .thread_name("meridian-ingress")
        .thread_stack_size(2 * 1024 * 1024)
        .on_thread_start(|| tracing::debug!("ingress worker started"))
        .enable_all()
        .build()
        .expect("failed to build ingress runtime")
});

// Housekeeping runtime: fewer workers, more blocking threads for
// catalog refreshes and frame archival.
static HOUSEKEEPING_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(32)
        .thread_name("meridian-housekeeping")
        .enable_all()
        .build()
        .expect("failed to build housekeeping runtime")
});

async fn handle_uplink_session() {
    // This runs on an ingress worker thread.
    // Long-running I/O awaits are fine here.
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    tracing::info!("uplink session processed");
}

async fn refresh_tle_catalog() {
    // CPU + blocking I/O — route to spawn_blocking so we do not
    // park an ingress worker for the duration of the refresh.
    tokio::task::spawn_blocking(|| {
        // Synchronous HTTP fetch + file write; blocks for ~200ms.
        tracing::info!("TLE catalog refreshed");
    })
    .await
    .expect("TLE refresh panicked");
}

fn main() {
    // Ingress and housekeeping run in separate thread pools.
    // A TLE refresh spike cannot starve active uplink sessions.
    std::thread::spawn(|| {
        HOUSEKEEPING_RUNTIME.block_on(async {
            loop {
                refresh_tle_catalog().await;
                tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;
            }
        });
    });

    INGRESS_RUNTIME.block_on(async {
        // In production: bind TCP listener, accept connections,
        // spawn handle_uplink_session per connection.
        for _ in 0..48 {
            INGRESS_RUNTIME.spawn(handle_uplink_session());
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    });
}
```

The `on_thread_start` hook enables per-thread tracing setup. In a production deployment, this is where you would initialize thread-local metrics recorders. The `thread_name` setting surfaces in `top`, `htop`, and `perf` output — essential when profiling which runtime is responsible for CPU usage.

### Dispatching Blocking Work Correctly

The most common async-correctness mistake in production Rust services is calling blocking code on an async worker thread. The rule is simple but frequently violated: if a function does not have `async` in its signature and it does any I/O or takes more than a few hundred microseconds, it belongs in `spawn_blocking`.

```rust,edition2021
use anyhow::Result;
use tokio::task;

/// Parse and validate a TLE record from a raw string.
/// TLE parsing is synchronous and O(n) with input length.
/// On a 100KB batch, this can take several milliseconds.
fn parse_tle_batch_blocking(raw: String) -> Result<Vec<String>> {
    // Synchronous parsing — no I/O, but CPU-bound for large inputs.
    raw.lines()
        .filter(|l| l.starts_with("1 ") || l.starts_with("2 "))
        .map(|l| Ok(l.to_string()))
        .collect()
}

async fn ingest_tle_update(raw_batch: String) -> Result<Vec<String>> {
    // Moving raw_batch into spawn_blocking satisfies the 'static bound.
    // The closure executes on a blocking thread; we await the JoinHandle.
    let records = task::spawn_blocking(move || parse_tle_batch_blocking(raw_batch))
        .await
        // The outer error is a JoinError (task panicked or was aborted).
        // Propagate it as an application error.
        .map_err(|e| anyhow::anyhow!("TLE parser panicked: {e}"))??;

    Ok(records)
}

#[tokio::main]
async fn main() -> Result<()> {
    let raw = "1 25544U 98067A   21275.52500000  .00001234  00000-0  12345-4 0  9999\n\
               2 25544  51.6400 337.6640 0007417  62.6000 297.5200 15.48889583300000\n"
        .to_string();

    let records = ingest_tle_update(raw).await?;
    println!("Parsed {} TLE records", records.len());
    Ok(())
}
```

The double `?` on `.await.map_err(...)??` deserves explanation: `spawn_blocking` returns `Result<T, JoinError>`, and `parse_tle_batch_blocking` itself returns `Result<Vec<String>, anyhow::Error>`. The first `?` propagates `JoinError` (after mapping it), the second propagates the inner application error. Collapsing these correctly is a common stumbling point — do not use `.unwrap()` on `JoinHandle` in production code; a parser panic should not take down the ingress runtime.

---

## Key Takeaways

- Tokio's multi-thread scheduler uses work-stealing across a pool of worker threads (defaulting to one per logical CPU). Tasks spawned via `tokio::spawn` enter the global queue and are picked up by idle workers.

- Worker threads and blocking threads serve different purposes. Never run synchronous blocking I/O or long CPU computation on a worker thread. Use `tokio::task::spawn_blocking` to route blocking work to the dedicated blocking thread pool.

- Explicit `Builder` configuration lets you control thread counts, stack sizes, thread names, and lifecycle hooks. Use it in production to separate high-frequency I/O workloads from lower-frequency blocking workloads, preventing one from starving the other.

- `tokio::spawn` creates a task with `'static` lifetime. If you need to share data from the spawning scope, move it into the closure with `async move`, wrap it in `Arc`, or communicate via channels.

- Multiple runtimes in the same process are a valid pattern for resource isolation. Ingress and housekeeping workloads with fundamentally different resource profiles benefit from separate thread pools rather than competing on a shared executor.

---

{{#quiz module1-lesson-02-quiz.toml}}