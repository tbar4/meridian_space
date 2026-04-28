# Capstone Project — SDA Sensor Ingestion Service

**Module:** Data Pipelines — M01: Stream Processing Foundations
**Estimated effort:** 1–2 weeks of focused work
**Prerequisites:** All three lessons in this module passed at ≥70%

---

## Mission Brief

> **OPS DIRECTIVE — SDA-2026-0118 / Phase 1 Implementation**
> **Classification:** PIPELINE STAND-UP — INGESTION TIER
>
> Stand up the front door of the Space Domain Awareness Fusion Service.
> Three sensor source types (X-band radar arrays over UDP, optical telescope
> archive over HTTP, ISL beacon network over TCP) must be unified into a
> single stream of normalized observation envelopes ready for fusion in
> downstream stages. The legacy Python ingestion script will be retired
> when this service reaches feature parity.
>
> Success criteria for Phase 1: the service ingests from all three source
> types simultaneously, normalizes to the canonical `Observation` envelope,
> and writes to a structured event log that downstream stages can consume.
> Sustained throughput of 10,000 observations per second with end-to-end
> ingest-to-log latency under 250 ms at the 99th percentile.

---

## What You're Building

A standalone Rust binary, `sda-ingest`, that:

1. Connects to three configured source types — UDP radar, HTTP optical archive, TCP ISL beacon listener
2. Wraps each source in an `ObservationSource` implementation that normalizes wire formats into the canonical `Observation` envelope
3. Composes the three sources into a single fan-in topology that feeds a downstream sink
4. Writes the normalized stream to a structured JSONL event log on local disk (one observation per line, atomically rotated by size)
5. Exposes a small HTTP control plane for health checks and basic metrics (per-source ingest rate, channel occupancy, error counters)

The service must run as a long-lived process and gracefully shut down on `SIGTERM` — flushing the event log, closing source connections, and exiting cleanly. No data should be lost on a clean shutdown; some data may be in flight on a hard kill, and that is acceptable for this module (Module 5 covers durability).

The deliverable is the binary, the test suite, and a 1-page operational README documenting how to run it, what configuration it expects, and what its observable behavior looks like under load.

---

## Suggested Architecture

```
┌────────────────────┐
│ UDP Radar Source   │──┐
│ (1-3 arrays)       │  │
└────────────────────┘  │
                        │
┌────────────────────┐  │   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ HTTP Optical       │──┼──→│  Normalize   │──→│   Validate   │──→│  JSONL Sink  │
│ Archive Source     │  │   │  (map)       │   │   (filter)   │   │              │
└────────────────────┘  │   └──────────────┘   └──────────────┘   └──────────────┘
                        │
┌────────────────────┐  │
│ TCP ISL Beacon     │──┘
│ Listener           │
└────────────────────┘
```

Each source runs in its own tokio task. All three feed a shared `mpsc::Sender<Observation>` (cloned three ways) that drains into the normalize operator. Normalize feeds validate; validate feeds the JSONL sink. The HTTP control-plane runs on a separate task; it shares an `Arc<Metrics>` with the data-plane tasks for read access.

You may diverge from this architecture if you have a defensible reason. Document it in the operational README.

---

## Acceptance Criteria

### Functional Requirements

- [x] The `Observation` envelope is defined as in Lesson 1 and used unchanged across all three sources
- [x] `UdpRadarSource` consumes the documented wire format (length, layout per Lesson 1) and produces valid `Observation` records
- [x] `OpticalArchiveSource` polls the HTTP endpoint with a `since` watermark, buffers multi-record responses, and produces valid `Observation` records (mock the HTTP endpoint for testing — a small `mockito` or `wiremock` setup is fine)
- [x] `IslBeaconSource` accepts incoming TCP connections, deserializes the wire format, and produces valid `Observation` records (mock the TCP producer for testing — a `tokio::net::TcpListener` paired with a producer task is fine)
- [x] All three sources implement the same `ObservationSource` trait
- [x] The topology fan-in correctly merges all three streams into the normalize operator
- [x] The JSONL sink writes one observation per line, with atomic rotation when the current file exceeds 64 MB
- [x] On `SIGTERM`, the service stops accepting new observations from sources, drains the in-flight pipeline, flushes the JSONL sink, and exits within 5 seconds

### Quality Requirements

- [x] Every source handles a deserialization error by logging it and continuing — one bad frame must not stop the source
- [x] Every channel in the topology is bounded; buffer sizes are chosen and documented (a comment per channel is sufficient)
- [x] All `await` points in the data plane are cancel-safe; a dropped task does not corrupt source state
- [x] No `.unwrap()` or `.expect()` in non-startup code paths (initialization may panic on misconfiguration)
- [x] At least one integration test exercises the full pipeline end-to-end with all three (mocked) sources running concurrently

### Operational Requirements

- [x] HTTP control plane exposes `GET /health` returning HTTP 200 when all source tasks are alive, 503 if any source has terminated unexpectedly
- [x] HTTP control plane exposes `GET /metrics` returning a JSON object with at minimum:
    - per-source ingest rate (observations per second, EWMA over 30s)
    - per-channel occupancy (current count and capacity)
    - per-source deserialization error count (lifetime of the process)
    - JSONL sink bytes written (lifetime of the process)
- [x] The service logs structured events (`tracing` + `tracing-subscriber` JSON formatter) — one log line per significant lifecycle event, not one per observation
- [x] A 1-page `README.md` in the project root documents: build, run, configure, expected metrics under steady-state load, known failure modes

### Self-Assessed Stretch Goals

- [ ] *(self-assessed)* Throughput sustained at 10,000 obs/sec with P99 ingest-to-log latency under 250 ms on a developer laptop. Provide a `criterion` benchmark and a `flamegraph` profile showing where time is spent.
- [ ] *(self-assessed)* The optical source supports both fixed-interval polling and long-polling modes, configurable. Document the latency tradeoff in the README.
- [ ] *(self-assessed)* The pipeline gracefully handles a "fragmentation event" simulation: drive the radar source at 10x normal rate for 60 seconds and observe that no observations are silently dropped at the application layer (UDP-level kernel drops are acceptable; document them).

---

## Hints

<details>
<summary>How should I model the configuration?</summary>

A small TOML file is the path of least resistance:

```toml
[radar]
bind_addrs = ["0.0.0.0:7001", "0.0.0.0:7002"]

[optical]
endpoint = "https://optical-archive.example/observations"
poll_interval_ms = 5000

[isl]
listen_addr = "0.0.0.0:7100"

[sink]
output_dir = "/var/log/sda/ingest"
rotation_bytes = 67108864  # 64 MiB

[control_plane]
bind_addr = "127.0.0.1:9100"
```

`serde` + `toml` makes this trivial. `figment` if you want layered config
(file + env vars). Don't over-engineer; you can add a config crate later.

</details>

<details>
<summary>What buffer size should the channels use?</summary>

The general rule from Module 4: buffers are sized to absorb expected
short-term burstiness, not to be a primary backpressure mechanism.

For ingest-to-normalize, a buffer of 1024–4096 is reasonable for SDA's
volumes. The dominant cost of an oversized buffer is increased latency
under load — every observation in the buffer is one in front of yours.
The dominant risk of an undersized buffer is unnecessary backpressure
oscillation if downstream is bursty.

Pick a number, document why, and revisit once you have load-test data.
You will revisit this in Module 4 with much more rigor.

</details>

<details>
<summary>How should I test the UDP radar source?</summary>

Spin up a `tokio::net::UdpSocket` in the test that *sends* the wire format
to the source's bind address. The source thinks it's reading from a real
radar; the test constructs the bytes and emits them. This pattern works
for any push-over-UDP source.

```rust,ignore
#[tokio::test]
async fn radar_source_decodes_valid_frame() {
    let bind = "127.0.0.1:0";  // OS picks port
    let mut source = UdpRadarSource::bind(bind, "test-radar").await.unwrap();
    let source_addr = source.local_addr().unwrap();

    // The producer side: encode a known frame and send it.
    let producer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let frame = encode_test_radar_frame(/* ... */);
    producer.send_to(&frame, source_addr).await.unwrap();

    let obs = source.next().await.unwrap().unwrap();
    assert_eq!(obs.source_kind, SourceKind::Radar);
    // ... assert other fields
}
```

You'll need to expose `local_addr()` on the source, or have the test know
the bind address ahead of time (less robust because of port races).

</details>

<details>
<summary>What's a clean way to handle SIGTERM?</summary>

`tokio::signal::ctrl_c()` for SIGINT, `tokio::signal::unix::signal(SignalKind::terminate())`
for SIGTERM. Combine them with a `tokio::select!` against the main service
loop; on signal, drop the source senders (which closes the channels), and
let the normalize → validate → sink chain drain naturally.

```rust,ignore
let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())?;
tokio::select! {
    _ = sigterm.recv() => {
        tracing::info!("SIGTERM received; initiating graceful shutdown");
    }
    _ = tokio::signal::ctrl_c() => {
        tracing::info!("SIGINT received; initiating graceful shutdown");
    }
    res = run_service(&mut topology) => {
        // service exited on its own — usually means a source error propagated
        return res;
    }
}
// Drop sources to close their channel senders; downstream drains.
topology.shutdown().await;
```

The `topology.shutdown()` method is yours to design — typically it joins
all tasks with a deadline and force-aborts any that don't finish in time.

</details>

<details>
<summary>How verbose should the metrics be?</summary>

Resist the urge to add a metric per operation. The four metric families
required by the acceptance criteria are sufficient for Module 1.

You will revisit metrics with rigor in Module 6, where Reis and Housley's
DataOps framing and Kafka's monitoring chapter (Shapira et al. Ch. 13)
provide the proper foundation. For now, four metrics that you understand
and that work correctly are far better than twenty that you cargo-culted
from a Kafka dashboard.

</details>

---

## Getting Started

Recommended order:

1. **Define the envelope.** Get `Observation` and the trait in place; write a unit test that round-trips it through serde JSON.
2. **Implement the simplest source.** The UDP radar source is the most self-contained — no HTTP, no TCP listener, no state. Start there. Get it tested end-to-end with a UDP producer in the test harness.
3. **Implement the JSONL sink.** Get observations flowing source → channel → sink to disk before adding the other sources.
4. **Add the optical source.** This is the most complex one because of the HTTP polling and watermark management. Mock the HTTP server in tests.
5. **Add the ISL TCP source.** Apply what you learned from radar plus what you learned from the optical-source error handling.
6. **Wire the topology together.** Compose the three sources into a fan-in; spawn each as a task; verify the integration test.
7. **Add the control plane.** Health and metrics last; they are the cherry on top, not the foundation.

Aim for a working end-to-end pipeline by day 4 even if everything in it is minimal. Optimize and harden after that. Premature optimization (specifically, premature buffer-size tuning) is a common time-sink in this project.

---

## What This Module Sets Up

In Module 2 you will replace this hand-spawned topology with a real orchestrator: a Rust task DAG executor that schedules operators, manages retries, and propagates idempotency keys across stage boundaries. The `Observation` envelope and the source/sink traits you define here will not change. The topology composition will become declarative.

In Module 3 you will replace the JSONL sink with an event-time correlation operator that windows observations by sensor timestamp and computes conjunction risk. The watermark logic in your optical source becomes the foundation for event-time watermark propagation across the pipeline.

You are not building a throwaway. You are building the first stage of a system that grows for five more modules.
