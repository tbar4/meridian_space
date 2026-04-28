# Module 04 — Network Programming

**Track:** Foundation — Mission Control Platform  
**Position:** Module 4 of 6  
**Source material:** Tokio tutorial I/O and Framing chapters; `reqwest` documentation; `tokio::net` API docs  
**Quiz pass threshold:** 70% on all three lessons to unlock the project

> **Note on source book:** *Network Programming with Rust* (Chanda, 2018) uses pre-async/await Tokio 0.1 APIs that are incompatible with current Tokio 1.x. Lesson content is grounded in the current Tokio tutorial and API documentation rather than that book.

---
<!-- toc -->
---
## Mission Context

The Meridian control plane's telemetry pipeline now has a complete message-passing architecture (Module 3). What it still lacks is the network layer: the actual TCP connections from ground stations that feed the pipeline. This module builds that layer — connecting the abstract pipeline to the physical network.

The control plane operates three distinct network protocols simultaneously: persistent TCP sessions with ground stations (framed, long-lived, must reconnect on failure), UDP datagrams from SDA radar and optical sensors (high-frequency, latency-sensitive, loss-tolerant), and outbound HTTP calls to the TLE catalog API and mission operations endpoints (request-response, with retry logic).

---

## What You Will Learn

By the end of this module you will be able to:

- Build async TCP servers with `tokio::net::TcpListener`, spawn per-connection tasks, handle EOF correctly, and shut down the accept loop cleanly via a `watch` channel shutdown signal
- Use `AsyncReadExt::read_exact` for length-prefix framing, split sockets with `TcpStream::split()` and `into_split()` for concurrent read/write, and wrap writers in `BufWriter` to reduce syscall overhead
- Add per-session timeouts to detect silent connections (antenna tracking failures, network blackouts) without leaving ghost sessions open
- Bind and use `tokio::net::UdpSocket` in both connected and unconnected modes, understand why UDP receive buffers must be sized to the maximum datagram, and apply `try_send` rather than blocking in high-frequency sensor pipelines
- Build a production `reqwest::Client` with appropriate timeout configuration, share it via `Clone` across async tasks, use `error_for_status()` correctly, and implement exponential backoff retry logic that distinguishes retryable server errors from non-retryable client errors

---

## Lessons

### Lesson 1 — TCP Servers with `tokio::net`: Listeners, Connection Handling, and Graceful Shutdown

Covers `TcpListener::bind` and the accept loop, `AsyncReadExt`/`AsyncWriteExt` extension traits, `read_exact` for framing, EOF handling, `TcpStream::split()` vs `into_split()`, `BufWriter` for write batching, read timeouts, and graceful shutdown of both the accept loop and individual connections.

**Key question this lesson answers:** How do you build a TCP server that handles many concurrent connections correctly — reading frames, handling EOF, splitting for bidirectional I/O, and shutting down cleanly?

→ `lesson-01-tcp-servers.md` / `lesson-01-quiz.toml`

---

### Lesson 2 — UDP and Datagram Protocols: Low-Latency Sensor Data

Covers `UdpSocket::bind`, `recv_from`/`send_to` semantics, connected vs unconnected mode, concurrent send/receive via `Arc<UdpSocket>`, buffer sizing and IP fragmentation, OS socket buffer tuning with `socket2`, and the decision between UDP and TCP for high-frequency sensor pipelines.

**Key question this lesson answers:** When does UDP's lack of ordering and reliability become an advantage, and how do you structure a receiver that does not block on a slow downstream consumer?

→ `lesson-02-udp.md` / `lesson-02-quiz.toml`

---

### Lesson 3 — HTTP Clients with `reqwest`: Async REST Calls

Covers `reqwest::Client` construction and sharing, `ClientBuilder` timeout configuration, `error_for_status()`, `.json()` for serialization/deserialization, retry logic with exponential backoff and jitter, status-code-based retry decisions, and multiple clients for services with different SLOs.

**Key question this lesson answers:** How do you build a robust HTTP client that handles transient failures without hammering a rate-limited API, and correctly distinguishes retryable errors from permanent ones?

→ `lesson-03-http-clients.md` / `lesson-03-quiz.toml`

---

## Capstone Project — Ground Station Network Client

Build the full ground station client: connects to a TCP endpoint using the length-prefix framing protocol, automatically reconnects on failure with exponential backoff, runs a background TLE refresh via HTTP with retry logic, forwards received frames to the downstream aggregator pipeline via `try_send`, publishes session state via a `watch` channel, and shuts down cleanly including a GOODBYE frame to the peer.

Acceptance is against 7 verifiable criteria including automatic reconnection, bounded backoff, 5-minute failure timeout, TLE retry, non-blocking frame forwarding, mid-frame shutdown safety, and state machine correctness.

→ `project-gs-client.md`

---

## Prerequisites

Modules 1–3 must be complete. Module 1 established the async task model and `tokio::select!` — both used extensively in connection handlers. Module 3 established the message-passing pipeline that network frames feed into. Understanding `mpsc::Sender` and `try_send` from Module 3 is prerequisite to the UDP and TCP lessons' discussion of non-blocking frame forwarding.

## What Comes Next

**Module 5 — Data-Oriented Design in Rust** shifts from I/O to computation: how to lay out structs for CPU cache efficiency, when to use struct-of-arrays vs array-of-structs, and arena allocation for high-throughput frame processing. The telemetry frames arriving via the TCP and UDP clients from this module are processed in bulk in Module 5.