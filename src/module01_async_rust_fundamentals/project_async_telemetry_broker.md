# Project — Async Telemetry Ingestion Broker

**Module:** Foundation — M01: Async Rust Fundamentals  
**Prerequisite:** All three module quizzes passed (≥70%)

---

## Mission Brief

**TO:** Platform Engineering  
**FROM:** Mission Control Systems Lead  
**CLASSIFICATION:** UNCLASSIFIED // INTERNAL  
**SUBJECT:** RFC-0041 — Telemetry Ingestion Broker Replacement

---

The legacy Python telemetry broker is being decommissioned. It accepted connections sequentially on a single thread and could not keep up beyond 12 concurrent ground station feeds. With constellation expansion to 48 LEO satellites and 12 active ground station sites, the broker routinely falls behind during peak pass windows, buffering up to 40 seconds of lag before flushing — unacceptable for conjunction avoidance workflows that require sub-10-second delivery.

Your task is to implement the replacement broker in Rust using Tokio. The broker must accept concurrent TCP connections from ground stations, parse incoming telemetry frames, and fan each frame out to multiple registered downstream handlers — without blocking on any single slow handler.

The broker does not perform conjunction computation. It is a pure ingress and distribution layer. Correctness, throughput, and clean lifecycle management are the acceptance criteria.

---

## System Specification

### Connection Model

- Ground stations connect over TCP to a configurable bind address.
- Each connection streams telemetry frames encoded as length-prefixed byte sequences: a 4-byte big-endian `u32` length header followed by `length` bytes of payload.
- Connections are persistent for the duration of a satellite pass (8–12 minutes). They may drop and reconnect within a pass without notice.
- The broker must handle up to 48 concurrent connections without degradation.

### Frame Routing

- Registered downstream handlers receive every frame via a bounded `tokio::sync::broadcast` channel.
- If a slow handler's receiver falls behind and the broadcast channel fills, it is the handler's problem — the broker must not block or slow its ingress path to accommodate a slow consumer.
- The broker logs a warning when a receiver falls behind (broadcast returns `RecvError::Lagged`).

### Lifecycle

- The broker accepts a shutdown signal (a `tokio::sync::watch` or `oneshot` channel) and performs graceful shutdown:
  1. Stop accepting new connections.
  2. Signal all active session tasks to drain and exit.
  3. Wait up to 10 seconds for tasks to finish.
  4. Force-abort any remaining tasks and exit.
- Session tasks must flush their in-progress frame before shutting down (complete the current frame read, then exit — do not abort mid-frame).

---

## Expected Output

A binary crate (`meridian-broker`) that:

1. Binds a TCP listener on a configurable address (default `0.0.0.0:7777`).
2. Spawns a new async task per incoming connection.
3. Each task reads frames using the length-prefix protocol.
4. Each parsed frame is sent over a `broadcast::Sender<Frame>`.
5. A configurable number of simulated downstream handler tasks subscribe to the broadcast channel and print/log received frames.
6. Ctrl-C triggers graceful shutdown with the sequence described above.

The binary should run, accept at least one connection from `telnet` or `netcat` with hand-crafted bytes, and log frame receipt and shutdown cleanly.

---

## Acceptance Criteria

| # | Criterion | Verifiable |
|---|---|---|
| 1 | Broker accepts ≥2 simultaneous TCP connections without either blocking the other | Yes — connect two `nc` sessions concurrently |
| 2 | Frames are delivered to all registered downstream handlers | Yes — log output shows frame receipt on each handler |
| 3 | A slow downstream handler does not stall frame ingestion on other connections | Yes — add a `tokio::time::sleep` in one handler; other connections continue at full rate |
| 4 | Ctrl-C triggers graceful shutdown; in-progress frame reads complete before the task exits | Yes — observable in log output |
| 5 | If shutdown drain exceeds 10 seconds, remaining tasks are aborted | Yes — simulate a stuck task and verify the process exits within 11 seconds |
| 6 | No `.unwrap()` on `JoinHandle::await` or channel send/receive in production paths | Yes — code review |
| 7 | `spawn_blocking` is used for any synchronous I/O or CPU-intensive frame processing | Yes — code review |

---

## Frame Format Reference

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      Payload Length (u32 BE)                  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                   Payload (variable length)                   |
|                          ...                                  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

A `Frame` struct for the purpose of this project:

```rust,edition2021
#[derive(Clone, Debug)]
pub struct Frame {
    pub station_id: String,
    pub payload: Vec<u8>,
}
```

---

## Hints

<details>
<summary>Hint 1 — Reading length-prefixed frames</summary>

`tokio::io::AsyncReadExt` provides `.read_exact(&mut buf)` which reads exactly `buf.len()` bytes or returns an error. Use it to read the 4-byte header, parse the length, allocate the payload buffer, and read the payload:

```rust,edition2021
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

async fn read_frame(stream: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}
```

</details>

<details>
<summary>Hint 2 — Broadcast channel for fan-out</summary>

`tokio::sync::broadcast::channel(capacity)` returns `(Sender<T>, Receiver<T>)`. Additional receivers are created with `sender.subscribe()`. Receivers that fall behind by more than `capacity` messages receive `Err(RecvError::Lagged(n))` — not an error in the fatal sense, just a signal that they missed `n` messages. Log it and continue receiving.

```rust,edition2021
use tokio::sync::broadcast;

let (tx, _rx) = broadcast::channel::<Frame>(256);

// Downstream handler
let mut rx = tx.subscribe();
tokio::spawn(async move {
    loop {
        match rx.recv().await {
            Ok(frame) => { /* process */ }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(missed = n, "handler fell behind");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
});
```

</details>

<details>
<summary>Hint 3 — Graceful shutdown with watch channel</summary>

`tokio::sync::watch` is well-suited for broadcasting a shutdown signal to an arbitrary number of tasks: one sender, many receivers, each receiver can check the current value or wait for a change.

```rust,edition2021
use tokio::sync::watch;

let (shutdown_tx, shutdown_rx) = watch::channel(false);

// In each session task:
let mut shutdown = shutdown_rx.clone();
tokio::select! {
    result = read_frames(&mut stream) => { /* ... */ }
    _ = shutdown.changed() => {
        tracing::info!("shutdown received, finishing current frame");
        // complete current frame read if mid-frame, then return
    }
}

// In shutdown handler:
let _ = shutdown_tx.send(true);
```

</details>

<details>
<summary>Hint 4 — Collecting JoinHandles for drain</summary>

Keep a `Vec<JoinHandle<()>>` of spawned session tasks. During shutdown, wrap the drain loop in `tokio::time::timeout`:

```rust,edition2021
let drain_deadline = Duration::from_secs(10);
let drain_result = tokio::time::timeout(drain_deadline, async {
    for handle in session_handles {
        let _ = handle.await; // ignore individual task errors
    }
}).await;

if drain_result.is_err() {
    tracing::warn!("drain deadline exceeded — some tasks may not have flushed");
}
```

After the timeout, the remaining `JoinHandle`s are dropped (tasks continue) or you can collect and abort them explicitly.

</details>

---

## Reference Implementation

<details>
<summary>Reveal reference implementation (attempt the project first)</summary>

```rust,edition2021
// src/main.rs
use anyhow::Result;
use std::sync::Arc;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, watch},
    time::{timeout, Duration},
};
use tokio::io::AsyncReadExt;
use tracing::{info, warn};

#[derive(Clone, Debug)]
pub struct Frame {
    pub station_id: String,
    pub payload: Vec<u8>,
}

async fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 65_536 {
        // Reject oversized frames — likely a protocol error or malicious client.
        anyhow::bail!("frame length {len} exceeds maximum 65536 bytes");
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

async fn handle_connection(
    mut stream: TcpStream,
    station_id: String,
    frame_tx: broadcast::Sender<Frame>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    info!(station = %station_id, "connection established");
    loop {
        tokio::select! {
            // Bias toward reading frames to minimize partial-frame cancellation.
            biased;

            result = read_frame(&mut stream) => {
                match result {
                    Ok(payload) => {
                        let frame = Frame {
                            station_id: station_id.clone(),
                            payload,
                        };
                        // Broadcast errors mean all receivers dropped — broker is shutting down.
                        if frame_tx.send(frame).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        // EOF or read error — connection dropped.
                        info!(station = %station_id, "connection closed: {e}");
                        break;
                    }
                }
            }

            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!(station = %station_id, "shutdown — completing current frame then exiting");
                    // The biased select ensures we finish the in-progress read if one was started.
                    // On the next iteration, the shutdown branch will win again and we break.
                    break;
                }
            }
        }
    }

    info!(station = %station_id, "connection handler exiting");
}

fn spawn_handler(
    id: usize,
    mut rx: broadcast::Receiver<Frame>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(frame) => {
                    info!(
                        handler = id,
                        station = %frame.station_id,
                        bytes = frame.payload.len(),
                        "frame received"
                    );
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(handler = id, missed = n, "handler fell behind — lagged");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    info!(handler = id, "broadcast channel closed, handler exiting");
                    break;
                }
            }
        }
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let bind_addr = "0.0.0.0:7777";
    let listener = TcpListener::bind(bind_addr).await?;
    info!("meridian broker listening on {bind_addr}");

    let (frame_tx, _) = broadcast::channel::<Frame>(256);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Spawn 3 downstream handlers.
    let mut handler_handles: Vec<tokio::task::JoinHandle<()>> = (0..3)
        .map(|i| spawn_handler(i, frame_tx.subscribe()))
        .collect();

    // Ctrl-C handler.
    let shutdown_tx = Arc::new(shutdown_tx);
    let shutdown_tx_ctrlc = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("failed to listen for ctrl-c");
        info!("ctrl-c received — initiating graceful shutdown");
        let _ = shutdown_tx_ctrlc.send(true);
    });

    let mut session_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let mut conn_id = 0usize;

    loop {
        // Stop accepting new connections once shutdown is signalled.
        if *shutdown_rx.borrow() {
            break;
        }

        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((stream, addr)) => {
                        conn_id += 1;
                        let station_id = format!("gs-{conn_id}@{addr}");
                        let handle = tokio::spawn(handle_connection(
                            stream,
                            station_id,
                            frame_tx.clone(),
                            shutdown_rx.clone(),
                        ));
                        session_handles.push(handle);
                    }
                    Err(e) => warn!("accept error: {e}"),
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
        }
    }

    info!("draining {} active sessions (10s deadline)", session_handles.len());

    // Drop the broadcast sender so downstream handlers see Closed after drain.
    drop(frame_tx);

    let drain_result = timeout(Duration::from_secs(10), async {
        for handle in session_handles {
            let _ = handle.await;
        }
        for handle in handler_handles.drain(..) {
            let _ = handle.await;
        }
    })
    .await;

    if drain_result.is_err() {
        warn!("drain deadline exceeded — forcing exit");
    } else {
        info!("all tasks drained cleanly");
    }

    Ok(())
}
```

**`Cargo.toml` dependencies:**

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
anyhow = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

**Testing the broker manually:**

```bash
# Terminal 1: run the broker
RUST_LOG=info cargo run

# Terminal 2: send a frame (4-byte length prefix = 5, then "hello")
printf '\x00\x00\x00\x05hello' | nc localhost 7777

# Terminal 3: send concurrently
printf '\x00\x00\x00\x07meridian' | nc localhost 7777

# Ctrl-C in Terminal 1 to trigger graceful shutdown
```

</details>

---

## Reflection

After completing this project, you have built the entry point for Meridian's control plane ingress. The patterns used here — broadcast fan-out, `select!`-driven shutdown, bounded drain with timeout, `JoinHandle` collection — recur throughout the rest of the Foundation modules and into the Data Pipelines track.

Consider for further exploration: what happens if the broker receives 10,000 connections? At what point does the spawn-per-connection model become a problem, and what is the alternative? How would you add backpressure from downstream handlers back to the ingress path without stalling the broker? These questions are the starting point for Module 3 (Message Passing Patterns).