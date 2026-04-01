# Project — Ground Station Network Client

**Module:** Foundation — M04: Network Programming  
**Prerequisite:** All three module quizzes passed (≥70%)

---
<!-- toc-->
---
## Mission Brief

**TO:** Platform Engineering  
**FROM:** Mission Control Systems Lead  
**CLASSIFICATION:** UNCLASSIFIED // INTERNAL  
**SUBJECT:** RFC-0051 — Ground Station Network Client Implementation

---

The Meridian control plane currently uses a Python subprocess to manage ground station TCP connections. It provides no reconnection logic, no session health monitoring, and no integration with the TLE catalog API for per-session orbital data refresh. Under antenna tracking interruptions, sessions drop and are never re-established. Under Space-Track API rate limiting, TLE data becomes stale without any backoff or retry.

Your task is to build the ground station network client — the component that owns the full lifecycle of a ground station TCP session: connect, read frames, reconnect on failure, refresh TLE data via HTTP, and shut down cleanly.

---

## System Specification

### Connection Management

The client connects to a ground station TCP endpoint (`host:port`). The length-prefix framing protocol from Lesson 1 applies: 4-byte big-endian `u32` length header followed by `length` bytes of payload.

On connection loss (EOF, read error, timeout), the client reconnects automatically with exponential backoff: 1s, 2s, 4s, 8s, up to 30s maximum. If reconnection fails for more than 5 minutes total, the client marks the station as `Failed` and stops retrying.

### Session Lifecycle

```
Connecting → Connected → Receiving frames → [disconnect] → Reconnecting → Connected → ...
                                          → [shutdown signal] → Disconnecting → Stopped
                                          → [5 min failure] → Failed
```

The current session state is tracked as an enum and exposed via a `watch` channel so monitoring systems can observe it.

### TLE Refresh

Each active session periodically fetches the TLE record for the session's assigned satellite from the mission API (`GET /tle/{norad_id}`). The refresh interval is configurable (default: 10 minutes). The HTTP client uses a `connect_timeout` of 3s and overall `timeout` of 15s. On 5xx or network errors, the refresh is retried with exponential backoff (up to 3 attempts). On 429, the backoff respects a `Retry-After` header if present.

### Frame Forwarding

Successfully received frames are forwarded to a `tokio::sync::mpsc::Sender<Frame>`. The frame includes the station ID, the session's current TLE record (if available), and the raw payload. If the downstream channel is full, the frame is dropped and a warning is logged.

### Shutdown

A `watch::Receiver<bool>` shutdown signal is accepted. On signal: complete the current frame read (do not abort mid-frame), flush any buffered writes (send a final `GOODBYE` frame to the peer), close the TCP connection cleanly, and exit.

---

## Expected Output

A library crate (`meridian-gs-client`) with:

- A `GroundStationClient` struct with `run()` method
- A `SessionState` enum and `watch` channel for state observation
- A `Frame` struct forwarded to the downstream channel
- A test binary that: connects to a local echo server (you implement a minimal echo server in the test), receives 5 frames, triggers reconnect by having the echo server drop the connection, verifies reconnection, then triggers shutdown

The test binary output should clearly show: initial connection, frame receipt, connection drop, reconnection, and clean shutdown.

---

## Acceptance Criteria

| # | Criterion | Verifiable |
|---|---|---|
| 1 | Client reconnects automatically on connection loss with exponential backoff | Yes — drop the server connection and verify reconnection in logs |
| 2 | Reconnection backoff is bounded at 30 seconds | Yes — check timing between reconnect attempts under sustained failure |
| 3 | Client marks station as `Failed` after 5 minutes of failed reconnections | Yes — simulate sustained connection refusal and verify state transition |
| 4 | TLE refresh runs on the configured interval and retries on 5xx/network errors | Yes — mock server returning 503 then 200 |
| 5 | Frame forwarding uses `try_send` — channel-full does not block the receive loop | Yes — code review and test with a slow downstream consumer |
| 6 | Shutdown completes the current frame before exiting | Yes — send a large frame and trigger shutdown mid-send; frame arrives complete |
| 7 | Session state transitions are correctly published to the watch channel | Yes — observer task sees all transitions in order |

---

## Hints

<details>
<summary>Hint 1 — Session state machine</summary>

```rust,edition2021
#[derive(Debug, Clone, PartialEq)]
pub enum SessionState {
    Connecting { attempt: u32 },
    Connected { since: std::time::Instant },
    Reconnecting { attempt: u32, next_retry: std::time::Instant },
    Disconnecting,
    Failed { reason: String },
    Stopped,
}
```

Publish state changes via `watch::Sender<SessionState>`. Observers call `borrow()` to read the current state or `changed().await` to wait for the next transition.

</details>

<details>
<summary>Hint 2 — Reconnect loop structure</summary>

```rust,edition2021
async fn run_with_reconnect(
    config: &ClientConfig,
    tx: mpsc::Sender<Frame>,
    mut shutdown: watch::Receiver<bool>,
    state_tx: watch::Sender<SessionState>,
) {
    let mut attempt = 0u32;
    let start = std::time::Instant::now();

    loop {
        if *shutdown.borrow() { break; }
        if start.elapsed() > std::time::Duration::from_secs(300) {
            let _ = state_tx.send(SessionState::Failed {
                reason: "reconnection window exceeded".into(),
            });
            break;
        }

        let _ = state_tx.send(SessionState::Connecting { attempt });
        match tokio::net::TcpStream::connect(&config.addr).await {
            Ok(stream) => {
                attempt = 0; // Reset on successful connection.
                let _ = state_tx.send(SessionState::Connected {
                    since: std::time::Instant::now(),
                });
                // Run the session until it disconnects or shutdown.
                run_session(stream, config, &tx, &mut shutdown, &state_tx).await;
                if *shutdown.borrow() { break; }
            }
            Err(e) => {
                tracing::warn!("connection failed (attempt {attempt}): {e}");
            }
        }

        attempt += 1;
        let delay = std::time::Duration::from_secs((1u64 << attempt.min(5)).min(30));
        let _ = state_tx.send(SessionState::Reconnecting {
            attempt,
            next_retry: std::time::Instant::now() + delay,
        });
        tokio::time::sleep(delay).await;
    }
}
```

</details>

<details>
<summary>Hint 3 — TLE refresh as a background task per session</summary>

Spawn a TLE refresh task when the session connects. Abort it when the session disconnects. Use a `watch::Sender<Option<TleRecord>>` to share the current TLE with the frame handler:

```rust,edition2021
async fn run_tle_refresh(
    http: reqwest::Client,
    norad_id: u32,
    interval: std::time::Duration,
    tle_tx: tokio::sync::watch::Sender<Option<TleRecord>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                match fetch_tle_with_retry(&http, norad_id, 3).await {
                    Ok(tle) => { let _ = tle_tx.send(Some(tle)); }
                    Err(e) => tracing::warn!(norad_id, "TLE refresh failed: {e}"),
                }
            }
            Ok(()) = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
        }
    }
}
```

</details>

<details>
<summary>Hint 4 — Sending a GOODBYE frame on clean shutdown</summary>

```rust,edition2021
use tokio::io::AsyncWriteExt;

async fn send_goodbye(stream: &mut tokio::net::TcpStream) {
    const GOODBYE: &[u8] = b"GOODBYE";
    let len = (GOODBYE.len() as u32).to_be_bytes();
    // Best-effort — ignore errors (we're shutting down anyway).
    let _ = stream.write_all(&len).await;
    let _ = stream.write_all(GOODBYE).await;
    let _ = stream.flush().await;
    let _ = stream.shutdown().await;
}
```

</details>

---

## Reference Implementation

<details>
<summary>Reveal reference implementation</summary>

```rust,edition2021
use anyhow::Result;
use reqwest::Client as HttpClient;
use serde::Deserialize;
use std::time::{Duration, Instant};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, watch},
    time::sleep,
};
use tracing::{info, warn, error};

#[derive(Debug, Clone, Deserialize)]
pub struct TleRecord {
    pub norad_id: u32,
    pub name: String,
    pub line1: String,
    pub line2: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SessionState {
    Connecting { attempt: u32 },
    Connected,
    Reconnecting { attempt: u32 },
    Failed { reason: String },
    Stopped,
}

#[derive(Debug)]
pub struct Frame {
    pub station_id: String,
    pub tle: Option<TleRecord>,
    pub payload: Vec<u8>,
}

pub struct ClientConfig {
    pub station_id: String,
    pub addr: String,
    pub norad_id: u32,
    pub api_base_url: String,
    pub tle_refresh_interval: Duration,
}

async fn read_frame(stream: &mut TcpStream) -> Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 65_536 { anyhow::bail!("frame too large: {len}"); }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

async fn fetch_tle(http: &HttpClient, base_url: &str, norad_id: u32) -> Result<TleRecord> {
    let url = format!("{base_url}/tle/{norad_id}");
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match http.get(&url).send().await {
            Ok(r) if r.status().is_success() => {
                return Ok(r.json::<TleRecord>().await?);
            }
            Ok(r) if r.status().is_server_error() && attempt < 3 => {
                warn!(norad_id, attempt, "TLE fetch server error, retrying");
                sleep(Duration::from_secs(1 << attempt)).await;
            }
            Ok(r) => anyhow::bail!("TLE fetch: HTTP {}", r.status()),
            Err(e) if (e.is_connect() || e.is_timeout()) && attempt < 3 => {
                warn!(norad_id, attempt, "TLE fetch network error, retrying");
                sleep(Duration::from_secs(1 << attempt)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

async fn run_session(
    mut stream: TcpStream,
    config: &ClientConfig,
    http: &HttpClient,
    frame_tx: &mpsc::Sender<Frame>,
    tle_tx: &watch::Sender<Option<TleRecord>>,
    mut shutdown: watch::Receiver<bool>,
) {
    // Kick off TLE refresh task for this session.
    let (session_shutdown_tx, session_shutdown_rx) = watch::channel(false);
    let tle_refresh = {
        let http = http.clone();
        let base = config.api_base_url.clone();
        let norad_id = config.norad_id;
        let interval = config.tle_refresh_interval;
        let tle_tx = tle_tx.clone();
        tokio::spawn(async move {
            let mut sd = session_shutdown_rx;
            loop {
                tokio::select! {
                    _ = sleep(interval) => {
                        match fetch_tle(&http, &base, norad_id).await {
                            Ok(tle) => { let _ = tle_tx.send(Some(tle)); }
                            Err(e) => warn!("TLE refresh failed: {e}"),
                        }
                    }
                    Ok(()) = sd.changed() => { if *sd.borrow() { break; } }
                }
            }
        })
    };

    loop {
        tokio::select! {
            biased;
            frame = tokio::time::timeout(Duration::from_secs(60), read_frame(&mut stream)) => {
                match frame {
                    Err(_) => { warn!(station = %config.station_id, "session timeout"); break; }
                    Ok(Ok(Some(payload))) => {
                        let tle = tle_tx.subscribe().borrow().clone();
                        let f = Frame {
                            station_id: config.station_id.clone(),
                            tle,
                            payload,
                        };
                        if frame_tx.try_send(f).is_err() {
                            warn!(station = %config.station_id, "frame dropped: pipeline full");
                        }
                    }
                    Ok(Ok(None)) => { info!(station = %config.station_id, "peer closed connection"); break; }
                    Ok(Err(e)) => { warn!(station = %config.station_id, "read error: {e}"); break; }
                }
            }
            Ok(()) = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!(station = %config.station_id, "shutdown — sending GOODBYE");
                    let _ = session_shutdown_tx.send(true);
                    let payload = b"GOODBYE";
                    let len = (payload.len() as u32).to_be_bytes();
                    let _ = stream.write_all(&len).await;
                    let _ = stream.write_all(payload).await;
                    let _ = stream.flush().await;
                    let _ = stream.shutdown().await;
                    break;
                }
            }
        }
    }

    let _ = session_shutdown_tx.send(true);
    let _ = tle_refresh.await;
}

pub async fn run_client(
    config: ClientConfig,
    frame_tx: mpsc::Sender<Frame>,
    mut shutdown: watch::Receiver<bool>,
    state_tx: watch::Sender<SessionState>,
) {
    let http = HttpClient::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(15))
        .build()
        .expect("failed to build HTTP client");

    let (tle_tx, _) = watch::channel::<Option<TleRecord>>(None);
    let mut attempt = 0u32;
    let start = Instant::now();

    loop {
        if *shutdown.borrow() { break; }

        if start.elapsed() > Duration::from_secs(300) {
            let _ = state_tx.send(SessionState::Failed {
                reason: "5-minute reconnect window exceeded".into(),
            });
            return;
        }

        let _ = state_tx.send(SessionState::Connecting { attempt });
        match TcpStream::connect(&config.addr).await {
            Ok(stream) => {
                attempt = 0;
                let _ = state_tx.send(SessionState::Connected);
                info!(station = %config.station_id, "connected to {}", config.addr);
                run_session(stream, &config, &http, &frame_tx, &tle_tx, shutdown.clone()).await;
                if *shutdown.borrow() { break; }
                info!(station = %config.station_id, "session ended, will reconnect");
            }
            Err(e) => {
                warn!(station = %config.station_id, attempt, "connection failed: {e}");
            }
        }

        attempt += 1;
        let delay = Duration::from_secs((1u64 << attempt.min(5)).min(30));
        let _ = state_tx.send(SessionState::Reconnecting { attempt });
        info!(station = %config.station_id, "reconnecting in {delay:?}");
        sleep(delay).await;
    }

    let _ = state_tx.send(SessionState::Stopped);
    info!(station = %config.station_id, "client stopped");
}
```

</details>

---

## Reflection

The ground station client built here is the connection layer that sits between the raw TCP socket and the telemetry aggregator from Module 3. The three lessons of this module are directly combined: `TcpListener`/`TcpStream` from Lesson 1 for the framed session protocol, UDP from Lesson 2 could be added for out-of-band sensor feeds from the same station, and `reqwest` from Lesson 3 for the TLE refresh background task within the session.

The reconnection loop pattern — state machine published to a `watch` channel, exponential backoff, failure timeout — is universal. It applies equally to database connections, message broker connections, and any other persistent network resource that needs supervisory recovery behaviour.