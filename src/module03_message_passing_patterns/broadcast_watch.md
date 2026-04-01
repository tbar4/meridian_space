# Lesson 2 — Broadcast and Watch Channels: Fan-Out Patterns for Telemetry Distribution

**Module:** Foundation — M03: Message Passing Patterns  
**Position:** Lesson 2 of 3  
**Source:** *Async Rust* — Maxwell Flitton & Caroline Morton, Chapters 6, 7

---

## Context

MPSC channels move work from many producers to one consumer. The inverse problem is fan-out: distributing one event to many consumers. The Meridian control plane has two distinct fan-out requirements that call for different solutions.

The first: when a TLE catalog update arrives, every active uplink session needs to process it. Each session must see the update — no session should receive it twice, and no session should miss it. This is an event-distribution problem.

The second: the shutdown flag. Every task in the control plane needs to know when the system is shutting down, but they do not need to receive a separate "shutdown event" — they just need to be able to check the current value at any time. This is a state-distribution problem.

Tokio provides a dedicated primitive for each. `broadcast` solves event distribution: every subscriber receives every message. `watch` solves state distribution: subscribers observe the latest value and are notified when it changes.

*Source: Async Rust, Chapters 6–7 (Flitton & Morton)*

---

## Core Concepts

### `tokio::sync::broadcast` — Every Subscriber Gets Every Message

`broadcast::channel(capacity)` returns a `(Sender<T>, Receiver<T>)` pair. Additional receivers are created by calling `sender.subscribe()` — each receiver gets its own position in the channel and receives every message sent after the subscription point.

```rust,edition2021
use tokio::sync::broadcast;

#[tokio::main]
async fn main() {
    let (tx, _rx) = broadcast::channel::<String>(16);

    // Each session gets its own receiver.
    let mut session_a = tx.subscribe();
    let mut session_b = tx.subscribe();

    tx.send("TLE-UPDATE-2024-001".to_string()).unwrap();

    // Both sessions receive the same message independently.
    println!("A: {}", session_a.recv().await.unwrap());
    println!("B: {}", session_b.recv().await.unwrap());
}
```

`Sender::send` does not require `await` — it is synchronous. Messages are placed in a ring buffer; receivers read from their own position in that buffer.

### The Lagged Error and What to Do With It

The broadcast channel has a fixed capacity ring buffer. If a slow receiver falls behind by more than `capacity` messages, it loses its position in the buffer. The next `recv()` call returns `Err(RecvError::Lagged(n))`, where `n` is the number of messages missed.

This is not a fatal error. The receiver continues to work — it simply missed `n` messages and will receive all subsequent ones. Whether missing messages is acceptable depends on the use case. For TLE catalog updates, a session that missed 3 updates can request a fresh fetch. For an audit log, missing messages is a compliance issue.

```rust,edition2021
use tokio::sync::broadcast;

async fn session_loop(mut rx: broadcast::Receiver<Vec<u8>>) {
    loop {
        match rx.recv().await {
            Ok(frame) => {
                // Normal path.
                process_update(frame).await;
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                // Receiver fell behind — n messages were lost from this receiver's view.
                // Log the gap and continue; the next recv will succeed.
                tracing::warn!(missed = n, "session fell behind broadcast — requesting resync");
                request_catalog_resync().await;
            }
            Err(broadcast::error::RecvError::Closed) => {
                // All senders dropped — broadcast channel is done.
                tracing::info!("broadcast channel closed, session exiting");
                break;
            }
        }
    }
}

async fn process_update(_frame: Vec<u8>) {}
async fn request_catalog_resync() {}
```

Capacity sizing for broadcast is more sensitive than for MPSC. The slowest subscriber determines whether lagging occurs. If subscribers have variable processing speeds, size the capacity to accommodate the slowest realistic consumer under load, plus a safety margin.

### `tokio::sync::watch` — Latest Value, Change Notification

`watch::channel(initial_value)` creates a single-value channel: the sender can update the value at any time, and receivers are notified when it changes. Receivers always see the latest value; intermediate values may be missed if the sender updates faster than the receiver reads.

```rust,edition2021
use tokio::sync::watch;

#[tokio::main]
async fn main() {
    let (tx, rx) = watch::channel::<bool>(false);

    // Clone the receiver for multiple tasks.
    let mut rx2 = rx.clone();

    tokio::spawn(async move {
        // Wait for the value to change.
        rx2.changed().await.unwrap();
        println!("shutdown signal received");
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    tx.send(true).unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
}
```

`watch::Receiver::borrow()` returns the current value without waiting. `changed().await` waits for the next change and then lets you `borrow()` the new value. This is the pattern for config reloading: tasks watch for a config change, then read the new config with `borrow()`.

`watch` is the correct primitive for the Meridian shutdown flag — much better than a `broadcast` channel. The shutdown event needs to be observed once by each task, and latecomers (tasks that check the flag after shutdown is signalled) need to see `true` immediately. A `broadcast` receiver created after the shutdown send would miss the message. A `watch` receiver always sees the current state.

### Choosing Between `mpsc`, `broadcast`, and `watch`

| Pattern | Channel | Use when |
|---|---|---|
| Work queue: one item consumed once | `mpsc` | 48 sessions each send frames to one aggregator |
| Event broadcast: every subscriber gets every event | `broadcast` | TLE update delivered to all active sessions |
| State sync: subscribers need the latest value | `watch` | Shutdown flag, config updates, current orbital state |
| One-shot reply | `oneshot` | Request-response within an actor message |

The key question: does each message need to be consumed exactly once (`mpsc`), by every subscriber (`broadcast`), or is only the latest value relevant (`watch`)?

### `watch` for Configuration Distribution

A common pattern in the Meridian control plane: runtime configuration loaded at startup and potentially reloaded via a management API. All tasks need to read the current config, and they need to be notified when it changes:

```rust,edition2021
use tokio::sync::watch;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct ControlPlaneConfig {
    max_frame_size: usize,
    session_timeout_secs: u64,
}

async fn uplink_session(
    satellite_id: u32,
    mut config_rx: watch::Receiver<Arc<ControlPlaneConfig>>,
) {
    loop {
        // Read current config — no lock, no await.
        let config = config_rx.borrow().clone();

        tokio::select! {
            // Process frames using current config.
            _ = tokio::time::sleep(
                tokio::time::Duration::from_secs(config.session_timeout_secs)
            ) => {
                tracing::warn!(satellite_id, "session timeout");
                break;
            }
            // React to config changes mid-session.
            Ok(()) = config_rx.changed() => {
                let new_config = config_rx.borrow().clone();
                tracing::info!(
                    satellite_id,
                    max_frame = new_config.max_frame_size,
                    "config reloaded"
                );
                // Loop continues with new config.
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let initial = Arc::new(ControlPlaneConfig {
        max_frame_size: 65536,
        session_timeout_secs: 600,
    });

    let (config_tx, config_rx) = watch::channel(Arc::clone(&initial));

    // Spawn a few sessions.
    for sat_id in 0..3u32 {
        let rx = config_rx.clone();
        tokio::spawn(uplink_session(sat_id, rx));
    }

    // Simulate a config reload.
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    config_tx.send(Arc::new(ControlPlaneConfig {
        max_frame_size: 32768,
        session_timeout_secs: 300,
    })).unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
}
```

`Arc<Config>` avoids cloning the full config struct on every `borrow()`. The `Arc::clone` is cheap (one atomic increment); the config data is shared read-only across tasks.

---

## Code Examples

### TLE Catalog Update Broadcaster

When the orbit data pipeline ingests a new TLE batch, it publishes the update over a `broadcast` channel. Every active session task receives the update and can refresh its orbital prediction model.

```rust,edition2021
use tokio::sync::broadcast;
use std::sync::Arc;

#[derive(Clone, Debug)]
struct TleUpdate {
    batch_id: u32,
    records: Arc<Vec<String>>,
}

async fn session_task(
    satellite_id: u32,
    mut tle_rx: broadcast::Receiver<TleUpdate>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let mut shutdown = shutdown_rx.clone();
    loop {
        tokio::select! {
            result = tle_rx.recv() => {
                match result {
                    Ok(update) => {
                        tracing::info!(
                            satellite_id,
                            batch = update.batch_id,
                            records = update.records.len(),
                            "TLE update applied"
                        );
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(satellite_id, missed = n, "TLE lag — resyncing");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            Ok(()) = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
        }
    }
    tracing::info!(satellite_id, "session task exiting");
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (tle_tx, _) = broadcast::channel::<TleUpdate>(32);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn 4 sessions, each with its own broadcast receiver.
    for sat_id in 0..4u32 {
        let tle_rx = tle_tx.subscribe();
        let sd = shutdown_rx.clone();
        tokio::spawn(session_task(sat_id, tle_rx, sd));
    }

    // Publish a TLE update to all sessions.
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    tle_tx.send(TleUpdate {
        batch_id: 42,
        records: Arc::new(vec!["1 25544U...".to_string(); 100]),
    }).unwrap();

    // Trigger shutdown.
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    shutdown_tx.send(true).unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
}
```

The combination of `broadcast` for events and `watch` for state is idiomatic Tokio. The `broadcast` channel delivers the catalog update to every session independently; the `watch` channel distributes the shutdown signal to all tasks simultaneously. The `select!` in the session loop races the two — whichever fires first wins.

---

## Key Takeaways

- `broadcast::channel(capacity)` distributes every message to every subscriber. Subscribers receive from their own position in a ring buffer. Creating a receiver via `sender.subscribe()` is the only way to subscribe — receivers created after a message is sent do not receive that message retroactively.

- `RecvError::Lagged(n)` is recoverable. A lagged receiver missed `n` messages but can continue receiving future ones. Whether missing messages is acceptable is application-specific; always handle it explicitly rather than treating it as a fatal error.

- `watch::channel(initial)` is for state distribution: the latest value, not every intermediate value. `borrow()` reads without waiting. `changed().await` waits for the next update. Receivers created after a send see the current value immediately.

- Use `broadcast` when every subscriber must receive every event. Use `watch` when subscribers need the current state and can tolerate missing intermediate updates. Use `mpsc` when each message should be consumed by exactly one task.

- `Arc<Config>` wrapped in a `watch` channel is the idiomatic pattern for distributing read-heavy configuration to many tasks. The watch notify is cheap; the config read is a lock-free `borrow()`.

---

{{#quiz lesson-02-quiz.toml}}