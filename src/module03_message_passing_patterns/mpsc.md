# Lesson 1 — `tokio::mpsc`: Bounded Channels, Backpressure, and Sender Cloning

**Module:** Foundation — M03: Message Passing Patterns  
**Position:** Lesson 1 of 3  
**Source:** *Async Rust* — Maxwell Flitton & Caroline Morton, Chapter 8

---

## Context

Module 2's command queue used a `Mutex<BinaryHeap>` plus a `Condvar` to share state between threads. That approach works, but it couples the producers and consumer through a shared data structure — every access requires acquiring the same lock, and the consumer must hold the lock while inspecting queue contents. Under contention at 48-uplink load, that lock becomes a bottleneck.

The alternative model is message passing: producers send values into a channel; the consumer receives from it. There is no shared data structure, no explicit locking, and no `Arc` to pass around. The channel itself manages all synchronization. The backpressure mechanism is built in: when the channel is full, `send` yields rather than blocking a thread, and the async runtime can schedule other work while the producer waits.

This lesson covers `tokio::sync::mpsc` — the multi-producer, single-consumer channel that is the workhorse of most async Rust systems. It also covers `oneshot` for request-response patterns and introduces the actor model as the structural pattern that emerges naturally from combining channels with task ownership.

*Source: Async Rust, Chapter 8 (Flitton & Morton)*

---

## Core Concepts

### MPSC Channels: The Model

`tokio::sync::mpsc::channel(capacity)` creates a bounded channel and returns a `(Sender<T>, Receiver<T>)` pair. The capacity is the maximum number of messages that can sit in the channel before senders must wait:

```rust,edition2021
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    // Capacity of 32: up to 32 messages can be buffered.
    // If the receiver falls behind, the 33rd send will yield.
    let (tx, mut rx) = mpsc::channel::<String>(32);

    tokio::spawn(async move {
        tx.send("frame-001".to_string()).await.unwrap();
    });

    while let Some(msg) = rx.recv().await {
        println!("received: {msg}");
    }
}
```

`recv()` returns `None` when all `Sender` handles have been dropped — this is the clean shutdown signal for a consumer loop. No explicit close call is needed; the channel closes naturally when the last sender drops.

### `Sender` is `Clone`: Multiple Producers

`Sender<T>` implements `Clone`. Each clone is an independent handle to the same channel. This is the "multi-producer" part of MPSC — any number of tasks can hold a `Sender` and push messages concurrently. The receiver sees messages from all senders interleaved, in the order they are delivered to the channel.

```rust,edition2021
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel::<(u32, Vec<u8>)>(64);

    // Each uplink session gets its own cloned sender.
    for satellite_id in 0..4u32 {
        let tx = tx.clone();
        tokio::spawn(async move {
            for seq in 0u8..3 {
                let frame = vec![satellite_id as u8, seq];
                // Yields if channel is full — backpressure in action.
                tx.send((satellite_id, frame)).await
                    .expect("aggregator task dropped");
            }
        });
    }

    // Drop the original sender so the channel closes when all
    // spawned tasks finish. Without this drop, rx.recv() never
    // returns None — the original sender keeps the channel alive.
    drop(tx);

    while let Some((sat, frame)) = rx.recv().await {
        println!("sat {sat}: {:?}", frame);
    }
}
```

The drop of the original `tx` after spawning is important and easy to forget. If any `Sender` clone outlives its usefulness, the channel stays open and the consumer loop blocks forever. The idiomatic pattern is to clone before spawning and drop the original.

### Backpressure and Capacity Sizing

A bounded channel applies backpressure: when the channel reaches capacity, `send().await` yields and does not return until the consumer has drained a slot. This is the async equivalent of a blocking queue — it prevents fast producers from overwhelming a slow consumer.

`try_send` is the non-blocking variant. It returns `Err(TrySendError::Full(_))` immediately if the channel is full rather than yielding. Use it when the producer should take an alternative action (log, drop, route to overflow) rather than applying backpressure:

```rust,edition2021
use tokio::sync::mpsc;

async fn forward_or_drop(tx: &mpsc::Sender<Vec<u8>>, frame: Vec<u8>) {
    match tx.try_send(frame) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(frame)) => {
            // Aggregator is falling behind — record the drop and continue.
            // In production: increment a metrics counter here.
            tracing::warn!(bytes = frame.len(), "frame dropped: aggregator full");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            tracing::error!("aggregator task has exited");
        }
    }
}

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(8);
    // Demonstrate try_send behaviour
    for i in 0u8..12 {
        forward_or_drop(&tx, vec![i]).await;
    }
    drop(tx);
    let mut count = 0;
    while rx.recv().await.is_some() { count += 1; }
    println!("received {count} frames (8 max due to capacity)");
}
```

Capacity sizing: too small causes unnecessary producer backpressure; too large hides a slow consumer until the buffer is exhausted. For the Meridian aggregator, a capacity of 2–4× the expected burst size is a reasonable starting point. Profile under realistic load.

`unbounded_channel()` provides no capacity limit — senders never yield. Use it only when backpressure is handled at an outer layer and unbounded buffering is acceptable (e.g., a metrics sink that can absorb any burst). Unbounded channels can cause OOM if the consumer is slower than the producers.

### `oneshot`: Request-Response

`tokio::sync::oneshot` is a single-message channel: exactly one send, exactly one receive. It is the correct primitive for request-response patterns, where a task sends a request and needs to await the result:

```rust,edition2021
use tokio::sync::{mpsc, oneshot};

enum ControlMsg {
    GetQueueDepth { reply: oneshot::Sender<usize> },
    Flush,
}

async fn aggregator(mut rx: mpsc::Receiver<ControlMsg>) {
    let mut queue: Vec<Vec<u8>> = Vec::new();
    while let Some(msg) = rx.recv().await {
        match msg {
            ControlMsg::GetQueueDepth { reply } => {
                // reply.send consumes the sender — can only respond once.
                let _ = reply.send(queue.len());
            }
            ControlMsg::Flush => {
                println!("flushing {} frames", queue.len());
                queue.clear();
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel::<ControlMsg>(8);
    tokio::spawn(aggregator(rx));

    // Ask the aggregator for its current queue depth.
    let (reply_tx, reply_rx) = oneshot::channel::<usize>();
    tx.send(ControlMsg::GetQueueDepth { reply: reply_tx }).await.unwrap();
    let depth = reply_rx.await.unwrap();
    println!("aggregator queue depth: {depth}");
}
```

The `oneshot::Sender` is embedded in the message itself. When the aggregator handles the message, it sends back through the oneshot and the caller's `reply_rx.await` resolves. This pattern — sometimes called the "mailbox" or "actor" pattern — eliminates the need for any shared state between the caller and the aggregator.

### The Actor Pattern

An actor is an async task that owns its state exclusively and exposes its functionality entirely through message passing (*Async Rust*, Ch. 8). No locks, no shared `Arc`, no exposed fields. Every operation on the actor's state happens sequentially within the actor's message loop — concurrent safety is structural, not from locking.

The advantages: the actor's state is never accessed concurrently. There are no data races by construction. Testing is straightforward — send messages, check responses. Adding operations means adding enum variants, not adding lock guards.

The tradeoffs: all operations are async (each call involves a channel send and an await). If many callers need responses simultaneously, the actor is a serialization point. If the actor's work is CPU-intensive, it blocks its own message loop. Both are solvable — the first with multiple actors, the second with `spawn_blocking` inside the loop — but they require deliberate design.

---

## Code Examples

### Telemetry Frame Aggregator Actor

The aggregator is an actor: it owns the frame buffer exclusively, receives frames and control messages through a single channel, and responds to queries via embedded oneshot channels. No locks anywhere.

```rust,edition2021
use tokio::sync::{mpsc, oneshot};
use std::collections::VecDeque;

const MAX_BUFFER: usize = 1000;

#[derive(Debug)]
struct TelemetryFrame {
    satellite_id: u32,
    sequence: u64,
    payload: Vec<u8>,
}

enum AggregatorMsg {
    /// A new frame from an uplink session.
    Frame(TelemetryFrame),
    /// Request: how many frames are buffered?
    Depth { reply: oneshot::Sender<usize> },
    /// Drain the buffer and return all frames.
    Drain { reply: oneshot::Sender<Vec<TelemetryFrame>> },
}

async fn run_aggregator(mut rx: mpsc::Receiver<AggregatorMsg>) {
    let mut buffer: VecDeque<TelemetryFrame> = VecDeque::with_capacity(MAX_BUFFER);

    while let Some(msg) = rx.recv().await {
        match msg {
            AggregatorMsg::Frame(frame) => {
                if buffer.len() >= MAX_BUFFER {
                    tracing::warn!(
                        satellite_id = frame.satellite_id,
                        "buffer full — dropping oldest frame"
                    );
                    buffer.pop_front();
                }
                buffer.push_back(frame);
            }
            AggregatorMsg::Depth { reply } => {
                let _ = reply.send(buffer.len());
            }
            AggregatorMsg::Drain { reply } => {
                let frames: Vec<_> = buffer.drain(..).collect();
                let _ = reply.send(frames);
            }
        }
    }
    tracing::info!("aggregator: all senders dropped, shutting down");
}

/// A typed handle to the aggregator actor.
/// Hides the channel internals from callers.
#[derive(Clone)]
struct AggregatorHandle {
    tx: mpsc::Sender<AggregatorMsg>,
}

impl AggregatorHandle {
    fn spawn(capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        tokio::spawn(run_aggregator(rx));
        Self { tx }
    }

    async fn send_frame(&self, frame: TelemetryFrame) -> anyhow::Result<()> {
        self.tx.send(AggregatorMsg::Frame(frame)).await
            .map_err(|_| anyhow::anyhow!("aggregator has shut down"))
    }

    async fn depth(&self) -> anyhow::Result<usize> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(AggregatorMsg::Depth { reply: reply_tx }).await
            .map_err(|_| anyhow::anyhow!("aggregator has shut down"))?;
        reply_rx.await.map_err(|_| anyhow::anyhow!("aggregator dropped reply"))
    }

    async fn drain(&self) -> anyhow::Result<Vec<TelemetryFrame>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(AggregatorMsg::Drain { reply: reply_tx }).await
            .map_err(|_| anyhow::anyhow!("aggregator has shut down"))?;
        reply_rx.await.map_err(|_| anyhow::anyhow!("aggregator dropped reply"))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let agg = AggregatorHandle::spawn(128);

    // Simulate 4 concurrent uplink sessions each sending 3 frames.
    let tasks: Vec<_> = (0..4u32).map(|sat_id| {
        let agg = agg.clone();
        tokio::spawn(async move {
            for seq in 0u64..3 {
                agg.send_frame(TelemetryFrame {
                    satellite_id: sat_id,
                    sequence: seq,
                    payload: vec![sat_id as u8; 64],
                }).await.unwrap();
            }
        })
    }).collect();

    for t in tasks { t.await.unwrap(); }

    println!("buffered: {}", agg.depth().await?);
    let frames = agg.drain().await?;
    println!("drained {} frames", frames.len());
    Ok(())
}
```

The `AggregatorHandle` is the public API. Callers see `send_frame`, `depth`, and `drain` — they never interact with the channel directly. The handle is `Clone`, so it can be shared freely across tasks by cloning, with no `Arc<Mutex<...>>` needed.

---

## Key Takeaways

- `tokio::sync::mpsc::channel(capacity)` creates a bounded channel. The capacity is the backpressure valve: `send().await` yields when the channel is full, preventing fast producers from overwhelming slow consumers.

- `Sender<T>` is `Clone`. Every clone is an independent producer on the same channel. The channel closes when all senders drop. Always drop the original sender after spawning cloned senders, or the consumer loop will block forever.

- `try_send` is the non-blocking variant. Use it when the producer should take an alternative action — drop, log, route to overflow — rather than yielding. Prefer `send().await` when backpressure is the correct response.

- `oneshot` is the single-message channel for request-response patterns. Embed the `oneshot::Sender` in the message to allow the receiver to reply exactly once. The `Sender` is consumed on send — using it more than once is a compile error.

- The actor pattern — an async task that owns its state exclusively and receives all operations as messages — eliminates shared state and all associated locking. It is the structural pattern that emerges naturally from MPSC channels in async systems.

---

{{#quiz lesson-01-quiz.toml}}