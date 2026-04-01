# Lesson 3 — Fan-In Aggregation: Merging Streams from Multiple Satellite Feeds

**Module:** Foundation — M03: Message Passing Patterns  
**Position:** Lesson 3 of 3  
**Source:** *Async Rust* — Maxwell Flitton & Caroline Morton, Chapters 3, 8

---

## Context

Lesson 1 covered moving data from many producers to one consumer via MPSC. That is fan-in at its simplest: all producers push to the same channel. But the Meridian aggregator's real requirements are more demanding. The 48 uplink sessions produce at different rates. Archived replay feeds produce at a different priority level than live feeds. A session that goes silent should not block the aggregator from processing the other 47. A priority command frame from a SAFE_MODE event should not wait behind a queue of housekeeping frames.

These requirements call for structured fan-in: merging multiple independent async sources into one stream, with control over priority, fairness, and behaviour when sources are slow or silent. This lesson covers three fan-in patterns — shared-sender MPSC, `select!`-based merge with priority, and the router actor pattern — and when to use each.

*Source: Async Rust, Chapters 3 & 8 (Flitton & Morton)*

---

## Core Concepts

### Shared-Sender Fan-In: The Simple Case

The simplest fan-in is the one already established in Lesson 1: clone the `Sender`, give each producer a clone, and let the single `Receiver` consume them all. Every message enters the same queue; the consumer sees them in arrival order.

```rust,edition2021
use tokio::sync::mpsc;

async fn uplink_producer(satellite_id: u32, tx: mpsc::Sender<(u32, Vec<u8>)>) {
    for seq in 0u8..5 {
        let frame = vec![satellite_id as u8, seq];
        if tx.send((satellite_id, frame)).await.is_err() {
            break; // Aggregator shut down.
        }
    }
}

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel::<(u32, Vec<u8>)>(256);

    for sat_id in 0..4u32 {
        tokio::spawn(uplink_producer(sat_id, tx.clone()));
    }
    drop(tx);

    while let Some((sat, frame)) = rx.recv().await {
        println!("sat {sat}: {:?}", frame);
    }
}
```

This is correct and efficient for uniform, same-priority inputs. It has one limitation: arrival order provides no priority control. A SAFE_MODE frame from satellite 7 waits behind whatever housekeeping frames arrived first.

### `select!`-Based Priority Fan-In

When sources have different priorities, `select!` can implement a priority receive by always checking a high-priority channel before a lower-priority one. Tokio's `select!` macro randomly selects among ready branches for fairness, but the `biased` modifier overrides this and evaluates branches in source order:

```rust,edition2021
use tokio::sync::mpsc;

async fn priority_aggregator(
    mut high: mpsc::Receiver<Vec<u8>>,
    mut low: mpsc::Receiver<Vec<u8>>,
) {
    loop {
        // biased: always check high-priority first.
        // Without biased, both channels are polled in random order —
        // low-priority frames could be dispatched before high-priority ones
        // if both are ready simultaneously.
        tokio::select! {
            biased;
            Some(frame) = high.recv() => {
                println!("HIGH: {} bytes", frame.len());
            }
            Some(frame) = low.recv() => {
                println!("LOW: {} bytes", frame.len());
            }
            else => break,
        }
    }
}

#[tokio::main]
async fn main() {
    let (high_tx, high_rx) = mpsc::channel::<Vec<u8>>(64);
    let (low_tx, low_rx) = mpsc::channel::<Vec<u8>>(256);

    // High-priority: SAFE_MODE and emergency commands.
    tokio::spawn(async move {
        high_tx.send(vec![0xFF; 8]).await.unwrap(); // emergency frame
    });

    // Low-priority: housekeeping telemetry.
    tokio::spawn(async move {
        for _ in 0..3 {
            low_tx.send(vec![0x00; 64]).await.unwrap();
        }
    });

    priority_aggregator(high_rx, low_rx).await;
}
```

`biased` is important here. Without it, if both channels have messages ready, `select!` randomly picks which to process — a high-priority frame could wait behind three low-priority frames. With `biased`, the high-priority channel is always drained first. The tradeoff: if the high-priority channel receives messages faster than they are processed, the low-priority channel is starved. For mission-critical applications like SAFE_MODE injection, this is the intended behaviour.

This pattern directly implements what *Async Rust* Chapter 3 builds when constructing a priority async queue with HIGH_CHANNEL and LOW_CHANNEL — the concept is the same, applied to async channels rather than thread queues.

### Tagging Messages with Source Identity

When fan-in merges undifferentiated `Vec<u8>` frames from multiple sources, the consumer cannot determine which satellite the frame came from. Tag messages at the producer side with an enum or a source identifier:

```rust,edition2021
use tokio::sync::mpsc;

#[derive(Debug)]
enum FeedKind {
    LiveUplink { satellite_id: u32 },
    ArchivedReplay { mission_id: String },
}

#[derive(Debug)]
struct TaggedFrame {
    source: FeedKind,
    sequence: u64,
    payload: Vec<u8>,
}

async fn live_uplink(sat_id: u32, tx: mpsc::Sender<TaggedFrame>) {
    for seq in 0u64..3 {
        let _ = tx.send(TaggedFrame {
            source: FeedKind::LiveUplink { satellite_id: sat_id },
            sequence: seq,
            payload: vec![sat_id as u8; 32],
        }).await;
    }
}

async fn replay_feed(mission: String, tx: mpsc::Sender<TaggedFrame>) {
    for seq in 0u64..2 {
        let _ = tx.send(TaggedFrame {
            source: FeedKind::ArchivedReplay { mission_id: mission.clone() },
            sequence: seq,
            payload: vec![0xAA; 128],
        }).await;
    }
}

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel::<TaggedFrame>(128);

    for sat_id in 0..3u32 {
        tokio::spawn(live_uplink(sat_id, tx.clone()));
    }
    tokio::spawn(replay_feed("ARTEMIS-IV".to_string(), tx.clone()));
    drop(tx);

    while let Some(frame) = rx.recv().await {
        match &frame.source {
            FeedKind::LiveUplink { satellite_id } => {
                println!("live sat {satellite_id} seq {}: {} bytes",
                    frame.sequence, frame.payload.len());
            }
            FeedKind::ArchivedReplay { mission_id } => {
                println!("replay {mission_id} seq {}: {} bytes",
                    frame.sequence, frame.payload.len());
            }
        }
    }
}
```

Using an enum for source identity is more robust than a raw integer: the compiler enforces that all source types are handled. When a new source type is added, `match` exhaustiveness forces updates at all handling sites.

### The Router Actor Pattern

For more than two or three sources, or when sources are created dynamically (e.g., a new ground station connection comes online mid-session), a router actor is the correct abstraction. The router owns a set of active input channels, polls them all, and forwards to a single output channel. This is the pattern *Async Rust* Chapter 8 builds as the foundation of its actor system.

```rust,edition2021
use tokio::sync::mpsc;
use std::collections::HashMap;

#[derive(Debug)]
struct TaggedFrame {
    source_id: u32,
    payload: Vec<u8>,
}

enum RouterMsg {
    /// Register a new uplink feed.
    AddFeed { source_id: u32, feed: mpsc::Receiver<Vec<u8>> },
    /// Remove an uplink feed (session ended).
    RemoveFeed { source_id: u32 },
}

async fn router_actor(
    mut ctrl: mpsc::Receiver<RouterMsg>,
    out: mpsc::Sender<TaggedFrame>,
) {
    // Tokio's mpsc doesn't provide a built-in multi-receiver select,
    // so we use a secondary MPSC where all feeds forward their frames.
    let (internal_tx, mut internal_rx) = mpsc::channel::<TaggedFrame>(512);
    let mut feed_handles: HashMap<u32, tokio::task::JoinHandle<()>> = HashMap::new();

    loop {
        tokio::select! {
            // Control messages: add or remove feeds.
            Some(msg) = ctrl.recv() => {
                match msg {
                    RouterMsg::AddFeed { source_id, mut feed } => {
                        let fwd_tx = internal_tx.clone();
                        let handle = tokio::spawn(async move {
                            while let Some(payload) = feed.recv().await {
                                if fwd_tx.send(TaggedFrame { source_id, payload }).await.is_err() {
                                    break; // Router shut down.
                                }
                            }
                            tracing::debug!(source_id, "feed task exiting");
                        });
                        feed_handles.insert(source_id, handle);
                    }
                    RouterMsg::RemoveFeed { source_id } => {
                        if let Some(handle) = feed_handles.remove(&source_id) {
                            handle.abort(); // Feed task no longer needed.
                        }
                    }
                }
            }
            // Frames from all registered feeds, already fan-in'ed via internal channel.
            Some(frame) = internal_rx.recv() => {
                if out.send(frame).await.is_err() {
                    break; // Downstream consumer has shut down.
                }
            }
            else => break,
        }
    }
}

#[tokio::main]
async fn main() {
    let (ctrl_tx, ctrl_rx) = mpsc::channel::<RouterMsg>(8);
    let (out_tx, mut out_rx) = mpsc::channel::<TaggedFrame>(256);

    tokio::spawn(router_actor(ctrl_rx, out_tx));

    // Register two satellite feeds dynamically.
    for sat_id in [25544u32, 48274] {
        let (feed_tx, feed_rx) = mpsc::channel::<Vec<u8>>(32);
        ctrl_tx.send(RouterMsg::AddFeed {
            source_id: sat_id,
            feed: feed_rx,
        }).await.unwrap();

        tokio::spawn(async move {
            for i in 0u8..3 {
                feed_tx.send(vec![i; 16]).await.unwrap();
            }
        });
    }

    drop(ctrl_tx);

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    let mut count = 0;
    while let Ok(frame) = tokio::time::timeout(
        tokio::time::Duration::from_millis(20),
        out_rx.recv()
    ).await {
        if let Some(f) = frame {
            println!("sat {}: {} bytes", f.source_id, f.payload.len());
            count += 1;
        } else {
            break;
        }
    }
    println!("total frames: {count}");
}
```

Each registered feed gets a dedicated forwarding task that moves frames to the router's internal channel. The router selects between control messages (add/remove feeds) and forwarded frames. Adding a new satellite source at runtime is a single `ctrl_tx.send(RouterMsg::AddFeed {...})` call — no restructuring of the select loop.

---

## Key Takeaways

- Shared-sender MPSC is the simplest fan-in: all producers clone the `Sender`, and the consumer reads from the single `Receiver`. Use it when sources have equal priority and arrival order is acceptable.

- `select!` with `biased` implements priority fan-in: the first branch is always evaluated before the second. Use it for two or three sources with different priority levels. Without `biased`, `select!` randomizes branch selection — a high-priority source is not guaranteed to be drained first when both are ready.

- Tag messages at the source with a typed identifier (`enum` or struct field) rather than relying on arrival order to infer provenance. An enum exhaustiveness check at the consumer forces all source types to be handled explicitly.

- The router actor pattern handles dynamic fan-in: sources can be registered and deregistered at runtime via control messages. Each source gets a dedicated forwarding task that converts its `Receiver` into tagged frames on the internal channel. The router selects between control and data messages.

- Fan-in and fan-out compose: an aggregator can receive from a router (fan-in) and forward to a broadcast channel (fan-out), building a full hub-and-spoke telemetry pipeline from these primitives.

---

{{#quiz lesson-03-quiz.toml}}