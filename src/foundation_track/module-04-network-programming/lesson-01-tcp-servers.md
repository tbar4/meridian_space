# Lesson 1 — TCP Servers with `tokio::net`: Listeners, Connection Handling, and Graceful Shutdown

**Module:** Foundation — M04: Network Programming  
**Position:** Lesson 1 of 3  
**Source:** Tokio tutorial — I/O and Framing chapters (tokio.rs/tokio/tutorial)

> **Source note:** *Network Programming with Rust* (Chanda) uses pre-async/await Tokio 0.1 APIs that are incompatible with current Tokio 1.x. This lesson is grounded in the current Tokio tutorial and Tokio 1.x API documentation.

---
<!-- toc -->
---
## Context

Every uplink session in the Meridian control plane begins with a TCP connection from a ground station. The Module 1 broker project sketched this accept loop in broad strokes. This lesson provides the complete model: how `TcpListener` binds and accepts connections, how to split a socket for concurrent read and write, how `AsyncReadExt` and `AsyncWriteExt` handle framed protocols, how a connection handler exits cleanly on EOF or error, and how the accept loop itself shuts down gracefully without leaking tasks.

The patterns here are not specific to Meridian. Every TCP server in Rust's async ecosystem — from a Redis clone to a satellite control plane — uses the same building blocks. Understanding them at the structural level means you can build, debug, and extend any such system.

---

## Core Concepts

### `TcpListener` — Binding and Accepting

`tokio::net::TcpListener::bind(addr)` binds the socket and returns a `TcpListener`. `listener.accept().await` waits for the next incoming connection and returns a `(TcpStream, SocketAddr)` pair. The accept call is async — while waiting, the executor can run other tasks.

```rust,edition2021
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:7777").await?;

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("connection from {addr}");
        // Each connection gets its own task.
        tokio::spawn(async move {
            handle_connection(socket).await;
        });
    }
}

async fn handle_connection(_socket: tokio::net::TcpStream) {
    // ... read frames, process, respond
}
```

The accept loop spawns a new task per connection and immediately loops back to accept the next one. The connection handler runs concurrently with all other handlers and with the accept loop itself. This is the fundamental async TCP server structure.

One critical detail: if `listener.accept()` returns an error, it does not always mean the listener is broken. `EAGAIN`, `ECONNABORTED`, and similar transient errors should be logged and retried. An unrecoverable error (e.g., the listener fd was closed) should terminate the loop. A simple approach: log the error and `continue` — the OS will sort out transient errors. For a production-grade implementation, add an exponential backoff on repeated errors.

### `AsyncRead`, `AsyncWrite`, and Their Extension Traits

`tokio::net::TcpStream` implements both `AsyncRead` and `AsyncWrite`, but you almost never call their methods directly. Instead you use the extension traits `AsyncReadExt` and `AsyncWriteExt` (from `tokio::io`), which provide ergonomic higher-level methods:

| Method | Description |
|---|---|
| `read(&mut buf)` | Read up to `buf.len()` bytes; returns `0` on EOF |
| `read_exact(&mut buf)` | Read exactly `buf.len()` bytes; errors on EOF |
| `read_u32()`, `read_u64()`, etc. | Read a big-endian integer |
| `write_all(&buf)` | Write all bytes in `buf` |
| `write_u32(n)`, etc. | Write a big-endian integer |

`read_exact` is the right primitive for fixed-size framing (like Meridian's 4-byte length prefix). It guarantees the buffer is fully populated before returning, handling the case where the underlying read returns fewer bytes than requested.

**EOF handling:** `read()` returning `Ok(0)` means the remote has closed the write half of the connection. Any subsequent `read()` will also return `Ok(0)`. When you see this, exit the read loop — continuing to call `read()` on a closed stream creates a 100% CPU spin loop.

```rust,edition2021
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

async fn read_frame(stream: &mut TcpStream) -> anyhow::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];

    // read_exact returns Err(UnexpectedEof) if the connection closes mid-header.
    match stream.read_exact(&mut len_buf).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            // Clean EOF at frame boundary — connection closed normally.
            return Ok(None);
        }
        Err(e) => return Err(e.into()),
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 65_536 {
        anyhow::bail!("frame too large: {len} bytes");
    }

    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok(Some(payload))
}
```

### Splitting a Socket: `io::split` and `TcpStream::split`

A `TcpStream` implements both `AsyncRead` and `AsyncWrite`, but Rust's borrow rules prevent passing `&mut stream` to two concurrent operations at the same time. To read and write concurrently — for example, to handle a bidirectional protocol or to send heartbeat responses while reading frames — the socket must be split.

**`TcpStream::split()`** splits by reference. Both halves must remain on the same task, but the read and write can be used independently within a single `select!` or sequential pair. Zero cost — no `Arc`, no `Mutex`.

**`io::split(stream)`** splits by value. Each half can be sent to a different task. Internally uses an `Arc<Mutex>` — slightly more overhead than the reference split, but needed when the read and write tasks must be truly independent.

```rust,edition2021
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn bidirectional_handler(stream: TcpStream) -> anyhow::Result<()> {
    // into_split: value split — each half can move to separate tasks.
    let (mut reader, mut writer) = stream.into_split();

    // Write task: sends periodic heartbeats.
    let write_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
            if writer.write_all(b"HEARTBEAT\n").await.is_err() {
                break;
            }
        }
    });

    // Read task: processes incoming frames.
    let mut buf = vec![0u8; 4096];
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 { break; } // EOF
        tracing::debug!(bytes = n, "frame received");
    }

    write_task.abort();
    Ok(())
}
```

Use `TcpStream::split()` (reference) when both read and write stay in one task. Use `TcpStream::into_split()` (value) when they need to move to separate tasks.

### `BufWriter` — Reducing Syscalls on the Write Path

Each `write_all` call is a syscall. For a protocol that sends many small writes (header bytes, then payload bytes), the overhead accumulates. Wrapping the write half in `tokio::io::BufWriter` buffers writes and flushes them in larger batches:

```rust,edition2021
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

async fn write_framed(stream: TcpStream, payload: &[u8]) -> anyhow::Result<()> {
    // BufWriter with 8KB internal buffer — flushes when full or on explicit flush().
    let mut writer = BufWriter::new(stream);

    // These two writes go to the internal buffer, not to the socket.
    let len = payload.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(payload).await?;

    // flush() pushes the buffered bytes to the socket in one syscall.
    writer.flush().await?;
    Ok(())
}
```

Always call `flush()` after writing a complete logical unit (a frame, a response). If you return from the handler without flushing, buffered data is silently dropped when the `BufWriter` drops.

### Graceful Shutdown of the Accept Loop

A simple `loop { listener.accept().await? }` has no shutdown path. The pattern from Lesson 3 of Module 1 applies here: race the accept against a shutdown signal with `select!`:

```rust,edition2021
use tokio::net::TcpListener;
use tokio::sync::watch;

async fn accept_loop(
    listener: TcpListener,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((socket, addr)) => {
                        tracing::info!(%addr, "connection accepted");
                        let sd = shutdown.clone();
                        tokio::spawn(async move {
                            connection_handler(socket, sd).await;
                        });
                    }
                    Err(e) => {
                        tracing::warn!("accept error: {e}");
                        // Continue — transient errors are normal.
                    }
                }
            }
            Ok(()) = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!("accept loop shutting down");
                    break;
                }
            }
        }
    }
}

async fn connection_handler(
    _socket: tokio::net::TcpStream,
    _shutdown: watch::Receiver<bool>,
) {
    // Read frames; check shutdown between reads.
}
```

Pass the `watch::Receiver` into each connection handler so that individual connections can also respond to the shutdown signal — stopping mid-read cleanly rather than being forcibly dropped.

---

## Code Examples

### Production Ground Station TCP Server

A complete TCP server for a Meridian ground station connection. Reads length-prefixed frames, forwards them to the telemetry aggregator from Module 3, and shuts down cleanly.

```rust,edition2021
use anyhow::Result;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, watch},
    time::{timeout, Duration},
};
use tracing::{info, warn};

#[derive(Debug)]
struct TelemetryFrame {
    station_id: String,
    payload: Vec<u8>,
}

async fn read_frame(stream: &mut TcpStream) -> Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 65_536 {
        anyhow::bail!("frame too large: {len}");
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

async fn handle_connection(
    mut stream: TcpStream,
    station_id: String,
    frame_tx: mpsc::Sender<TelemetryFrame>,
    mut shutdown: watch::Receiver<bool>,
) {
    info!(station = %station_id, "session started");
    loop {
        tokio::select! {
            // Bias toward reading to complete in-progress frames.
            biased;
            frame = timeout(Duration::from_secs(60), read_frame(&mut stream)) => {
                match frame {
                    // Session timeout — ground station went silent.
                    Err(_elapsed) => {
                        warn!(station = %station_id, "session timeout");
                        break;
                    }
                    Ok(Ok(Some(payload))) => {
                        if frame_tx.send(TelemetryFrame {
                            station_id: station_id.clone(),
                            payload,
                        }).await.is_err() {
                            break; // Aggregator shut down.
                        }
                    }
                    Ok(Ok(None)) => {
                        info!(station = %station_id, "connection closed by peer");
                        break;
                    }
                    Ok(Err(e)) => {
                        warn!(station = %station_id, "read error: {e}");
                        break;
                    }
                }
            }
            Ok(()) = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!(station = %station_id, "shutdown signal — closing session");
                    break;
                }
            }
        }
    }
    // Send a clean close to the peer.
    let _ = stream.shutdown().await;
    info!(station = %station_id, "session ended");
}

pub async fn run_tcp_server(
    bind_addr: &str,
    frame_tx: mpsc::Sender<TelemetryFrame>,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!("ground station server listening on {bind_addr}");
    let mut conn_id = 0usize;
    let mut sd = shutdown.clone();

    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (socket, addr) = accept?;
                conn_id += 1;
                let station_id = format!("gs-{conn_id}@{addr}");
                tokio::spawn(handle_connection(
                    socket,
                    station_id,
                    frame_tx.clone(),
                    shutdown.clone(),
                ));
            }
            Ok(()) = sd.changed() => {
                if *sd.borrow() { break; }
            }
        }
    }
    info!("accept loop exited");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let (frame_tx, mut frame_rx) = mpsc::channel::<TelemetryFrame>(256);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Frame consumer.
    tokio::spawn(async move {
        while let Some(frame) = frame_rx.recv().await {
            info!(station = %frame.station_id, bytes = frame.payload.len(), "frame received");
        }
    });

    // Shutdown after 2 seconds for demo purposes.
    let sd = shutdown_tx;
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let _ = sd.send(true);
    });

    run_tcp_server("0.0.0.0:7777", frame_tx, shutdown_rx).await
}
```

Several production decisions embedded here: the `timeout` around `read_frame` handles silent connections (antenna loss, network blackout) without leaving ghost sessions open. `stream.shutdown()` sends a TCP FIN to the peer on clean exit. The `biased` `select!` ensures an in-progress frame read is completed before the shutdown branch wins.

---

## Key Takeaways

- `TcpListener::bind().await` binds the socket; `listener.accept().await` yields a `(TcpStream, SocketAddr)`. Spawn a task per connection and loop back immediately — the accept loop should never be blocked by connection handling.

- `read()` returning `Ok(0)` is EOF — the remote closed its write half. Continuing to call `read()` after EOF creates a spin loop. Always exit the read loop on `Ok(0)`.

- `read_exact` is the correct primitive for fixed-size framing. It handles short reads internally and returns `UnexpectedEof` if the connection closes before the buffer is filled.

- Use `TcpStream::split()` for same-task read/write splitting (zero cost). Use `TcpStream::into_split()` when the read and write halves must move to separate tasks.

- `BufWriter` batches small writes. Always call `flush()` after writing a complete logical unit — unflushed data is silently dropped when the writer drops.

- Add a `timeout` to reads in long-lived connections. Ground stations go silent without warning. A 60-second read timeout detects ghost sessions that would otherwise hold open resources indefinitely.

---

{{#quiz lesson-01-quiz.toml}}