# Lesson 2 — UDP and Datagram Protocols: Low-Latency Sensor Data

**Module:** Foundation — M04: Network Programming  
**Position:** Lesson 2 of 3  
**Source:** Synthesized from training knowledge and `tokio::net::UdpSocket` documentation

> **Source note:** This lesson synthesizes from current `tokio::net::UdpSocket` API documentation and training knowledge. The following concepts would benefit from verification against the source book if the API has changed: `split()` on `UdpSocket`, `recv_from`/`send_to` semantics, and `connect()`-vs-unconnected modes.

---
<!-- toc -->
---
## Context

The Meridian Space Domain Awareness network includes optical sensors and radar installations that report raw detection events at high frequency with strict latency requirements. A radar return needs to reach the conjunction analysis pipeline in under 50ms. At that latency budget, TCP's per-packet acknowledgment and retransmission overhead is a liability, not a feature. When the occasional dropped packet is acceptable — or when the application layer manages its own loss detection — UDP is the right transport.

UDP is a datagram protocol: each `send` and `recv` corresponds to exactly one discrete packet. There are no streams, no connection establishment, no ordering guarantees, and no retransmission. What you get is low overhead, minimal kernel buffering, and latency that is bounded only by the network, not by protocol machinery.

This lesson covers `tokio::net::UdpSocket`: binding, sending, receiving, splitting for concurrent send/receive, and the design decisions around UDP in a high-frequency sensor pipeline.

---

## Core Concepts

### UDP Socket Basics

`UdpSocket::bind(addr)` creates a UDP socket bound to a local address. Unlike TCP, there is no accept loop and no connection concept. A single bound socket can send to any address and receive from any address:

```rust,edition2021
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Bind to receive on all interfaces, port 9090.
    let socket = UdpSocket::bind("0.0.0.0:9090").await?;

    let mut buf = [0u8; 1024];
    loop {
        // recv_from returns the number of bytes and the sender's address.
        let (n, addr) = socket.recv_from(&mut buf).await?;
        println!("received {n} bytes from {addr}: {:?}", &buf[..n]);

        // Echo back.
        socket.send_to(&buf[..n], addr).await?;
    }
}
```

`recv_from` waits for the next datagram. If the incoming datagram is larger than `buf`, the excess bytes are silently discarded — there is no partial read concept in UDP. Size your buffer to the maximum expected datagram, not the average.

### Connected Mode vs. Unconnected Mode

An unconnected UDP socket can communicate with any remote address. A connected UDP socket is associated with one specific remote address via `socket.connect(addr)` — this is not a TCP handshake, just a filter on the local OS socket:

```rust,edition2021
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?; // OS assigns port

    // "Connect" to the sensor — enables send/recv instead of send_to/recv_from.
    // Datagrams from other addresses are filtered out.
    socket.connect("192.168.1.100:5500").await?;

    socket.send(b"POLL").await?;

    let mut buf = [0u8; 256];
    let n = socket.recv(&mut buf).await?;
    println!("sensor response: {:?}", &buf[..n]);
    Ok(())
}
```

After `connect()`, use `send`/`recv` instead of `send_to`/`recv_from`. The OS filters datagrams to only those from the connected address, which is useful for point-to-point sensor polling. For a server receiving from many sensors, use the unconnected mode with `recv_from`.

### Splitting for Concurrent Send/Receive

A single `UdpSocket` cannot be both `recv_from`'d and `send_to`'d simultaneously from different tasks — you need a split. `UdpSocket::into_split()` returns `(OwnedRecvHalf, OwnedSendHalf)`, each of which can be moved to a separate task:

```rust,edition2021
use std::sync::Arc;
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:9090").await?);

    // For UdpSocket, Arc-sharing is the idiomatic split pattern
    // because both send_to and recv_from take &self.
    let recv_socket = Arc::clone(&socket);
    let send_socket = Arc::clone(&socket);

    let recv_task = tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        loop {
            let (n, addr) = recv_socket.recv_from(&mut buf).await.unwrap();
            println!("recv {n} bytes from {addr}");
        }
    });

    let send_task = tokio::spawn(async move {
        // Periodic heartbeat to a known sensor address.
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            send_socket.send_to(b"HEARTBEAT", "192.168.1.100:5500")
                .await.unwrap();
        }
    });

    let _ = tokio::join!(recv_task, send_task);
    Ok(())
}
```

`UdpSocket`'s `send_to` and `recv_from` take `&self` (shared reference), so wrapping in `Arc` lets multiple tasks share the same socket without splitting. This differs from `TcpStream` where `read` and `write` require `&mut self`.

### Buffer Sizing and Packet Loss

UDP datagrams have a maximum size of 65,507 bytes (for IPv4 over Ethernet), but practical limits are lower. A datagram that exceeds the network MTU (typically 1500 bytes on Ethernet) is fragmented at the IP layer. If any fragment is lost, the entire datagram is discarded. For high-frequency sensor data, keep individual datagrams under 1472 bytes (1500 MTU - 20 IP header - 8 UDP header) to avoid fragmentation.

Buffer the receive socket at the OS level with `SO_RCVBUF` if sensor bursts arrive faster than the application can drain them. This requires `socket2` or `nix` crate access to set socket options before wrapping in `tokio::net::UdpSocket`:

```rust,edition2021
use socket2::{Socket, Domain, Type};
use std::net::SocketAddr;
use tokio::net::UdpSocket;

async fn bind_with_large_buffer(addr: &str) -> anyhow::Result<UdpSocket> {
    let addr: SocketAddr = addr.parse()?;
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
    socket.set_reuse_address(true)?;
    // 4MB receive buffer to absorb radar bursts.
    socket.set_recv_buffer_size(4 * 1024 * 1024)?;
    socket.bind(&addr.into())?;
    socket.set_nonblocking(true)?;
    Ok(UdpSocket::from_std(socket.into())?)
}
```

### When to Choose UDP over TCP

| Situation | Preferred |
|---|---|
| Radar/optical detection events, < 50ms latency budget | UDP |
| Telemetry frames requiring ordered delivery and reliability | TCP |
| Configuration commands — must not be lost | TCP |
| Periodic status heartbeats where loss is acceptable | UDP |
| Bulk TLE catalog transfer | TCP |
| High-frequency position updates where only latest matters | UDP |

The core tradeoff: TCP adds ordering, reliability, and flow control at the cost of latency and per-connection overhead. UDP provides a raw datagram channel — if reliability matters, implement it yourself (sequence numbers, ACKs, retransmission) at the application layer.

---

## Code Examples

### SDA Radar Sensor Receiver

The Meridian SDA network has radar stations that broadcast detection events as UDP datagrams. The receiver processes them and forwards to the conjunction analysis pipeline. Packet loss is tolerable — a missed radar return is worse than a delayed one, but the next sweep arrives in 250ms anyway.

```rust,edition2021
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};

#[derive(Debug)]
struct RadarDetection {
    sensor_id: u32,
    azimuth_deg: f32,
    elevation_deg: f32,
    range_km: f32,
    timestamp_ms: u64,
}

fn parse_detection(buf: &[u8], addr: SocketAddr) -> Option<RadarDetection> {
    // Wire format: 4-byte sensor_id | 4-byte azimuth (f32 BE) |
    //              4-byte elevation (f32 BE) | 4-byte range (f32 BE) |
    //              8-byte timestamp (u64 BE)
    if buf.len() < 24 {
        return None; // Malformed datagram — discard silently.
    }
    let sensor_id = u32::from_be_bytes(buf[0..4].try_into().ok()?);
    let azimuth = f32::from_be_bytes(buf[4..8].try_into().ok()?);
    let elevation = f32::from_be_bytes(buf[8..12].try_into().ok()?);
    let range = f32::from_be_bytes(buf[12..16].try_into().ok()?);
    let timestamp = u64::from_be_bytes(buf[16..24].try_into().ok()?);

    tracing::debug!(%addr, sensor_id, "detection received");
    Some(RadarDetection {
        sensor_id,
        azimuth_deg: azimuth,
        elevation_deg: elevation,
        range_km: range,
        timestamp_ms: timestamp,
    })
}

async fn radar_receiver(
    bind_addr: &str,
    tx: mpsc::Sender<RadarDetection>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let socket = Arc::new(UdpSocket::bind(bind_addr).await?);
    tracing::info!("radar receiver listening on {bind_addr}");

    let mut buf = [0u8; 1472]; // Stay under MTU to avoid fragmentation.

    loop {
        tokio::select! {
            biased;
            recv = socket.recv_from(&mut buf) => {
                match recv {
                    Ok((n, addr)) => {
                        if let Some(detection) = parse_detection(&buf[..n], addr) {
                            // Non-blocking — drop if pipeline is full rather than
                            // blocking the receive loop. A queued radar sweep is
                            // useless by the time it clears the backlog.
                            if tx.try_send(detection).is_err() {
                                tracing::warn!("detection pipeline full — datagram dropped");
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("recv error: {e}");
                        // UDP recv errors are typically transient — continue.
                    }
                }
            }
            Ok(()) = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!("radar receiver shutting down");
                    break;
                }
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let (tx, mut rx) = mpsc::channel::<RadarDetection>(512);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    tokio::spawn(radar_receiver("0.0.0.0:9090", tx, shutdown_rx));

    // Consumer: conjunction analysis pipeline.
    tokio::spawn(async move {
        while let Some(det) = rx.recv().await {
            tracing::info!(
                sensor = det.sensor_id,
                az = det.azimuth_deg,
                el = det.elevation_deg,
                range = det.range_km,
                "detection processed"
            );
        }
    });

    // Demo: shut down after 5 seconds.
    tokio::time::sleep(Duration::from_secs(5)).await;
    shutdown_tx.send(true)?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(())
}
```

`try_send` instead of `send().await` is deliberate here. If the conjunction pipeline is saturated, blocking the radar receive loop means subsequent datagrams pile up in the OS socket buffer and eventually overflow it too. Dropping one detection and keeping the receive loop running is the correct behaviour for high-frequency sensor data where recency matters more than completeness.

---

## Key Takeaways

- UDP is a datagram protocol — each `send`/`recv` is one discrete packet with no ordering, reliability, or congestion control. Use it when latency matters more than reliability, or when the application layer manages loss detection.

- `recv_from` returns the number of bytes received and the sender's address. If the datagram is larger than the buffer, excess bytes are silently discarded. Size receive buffers to the maximum expected datagram, not the average.

- `connect()` on a UDP socket is not a handshake — it sets a default remote address and filters incoming datagrams from other addresses. Use connected mode for point-to-point polling; use unconnected mode for servers receiving from many sources.

- `UdpSocket`'s `send_to` and `recv_from` take `&self`. Wrapping in `Arc` lets multiple tasks share one socket without a formal split — unlike `TcpStream` which requires `into_split()` or `split()` for concurrent access.

- Keep datagrams under 1472 bytes on Ethernet networks to avoid IP fragmentation. A single lost IP fragment drops the entire datagram.

- In high-frequency sensor pipelines, use `try_send` rather than `send().await` when forwarding to a downstream channel. Blocking the receive loop on a full channel is worse than dropping one datagram.

---

{{#quiz lesson-02-quiz.toml}}