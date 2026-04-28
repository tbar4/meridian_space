# Lesson 3 — Push, Pull, and Poll Semantics

**Module:** Data Pipelines — M01: Stream Processing Foundations
**Position:** Lesson 3 of 3
**Source:** *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 7 ("Push Versus Pull Versus Poll Patterns" and "Consumer Pull and Push"); *Streaming Data* — Andrew Psaltis, Chapter 2 (Common Interaction Patterns); *Kafka: The Definitive Guide* — Shapira et al., Chapter 4 (Kafka Consumers — The Poll Loop)

---

## Context

Three patterns govern how data crosses boundaries in a streaming pipeline. *Push:* the producer initiates contact and forwards data to the consumer. *Pull:* the consumer initiates contact and requests data from the producer. *Poll:* the consumer initiates contact on a schedule, regardless of whether new data exists. The choice among them is one of the most consequential and underrated decisions in streaming architecture. The wrong choice produces systems that look fine in development and fall over in production — push pipelines that overwhelm slow consumers, pull pipelines that introduce avoidable latency, poll pipelines that burn capacity asking sources that have nothing to say.

Reis and Housley make a strong case in *Fundamentals of Data Engineering* Chapter 7: every interaction between a source and the pipeline (and between every pair of stages within the pipeline) is one of these three patterns, and the choice should be deliberate. Most production confusion comes from systems that have grown organically into a mix of all three without anyone designing the mix on purpose. The Kafka consumer's `poll` loop, documented in detail in Shapira et al. Chapter 4, is the canonical example of a deliberately chosen pattern — Kafka could have been push-based, the designers chose poll-based, and the choice shapes the system's operational properties end-to-end.

For the SDA Fusion Service, the three sensor sources naturally land on different patterns. Radar arrays push UDP frames whether anyone is listening; optical archive servers expose a REST endpoint that must be pulled; ISL beacons emit on a fixed cadence that requires polling because there is no notification mechanism on the wire protocol. The pipeline cannot impose a single pattern on all three — it must adapt, and the adaptation logic lives at the boundary. Understanding what each pattern costs and what it guarantees is how you build that boundary correctly.

---

## Core Concepts

### Push Semantics

In push, the producer initiates the transfer. When new data exists, the producer sends it to the consumer immediately, without waiting for a request. The consumer is reactive: it receives data when the producer decides.

Push has two compelling properties. First, **latency is minimal** — data flows from producer to consumer with one network round trip and no waiting. Second, **the producer needs no model of consumer demand** — it produces at its natural rate and the consumer either keeps up or doesn't. For high-rate, latency-sensitive sources (radar at 200 Hz; market data; sensor telemetry), push is the natural fit.

The cost of push is that **flow control is the consumer's responsibility, and getting it wrong is catastrophic**. A push consumer that cannot keep up has only three options:

1. **Buffer.** Store incoming data until processing catches up. If the producer's rate exceeds the consumer's processing rate persistently, the buffer grows without bound and the consumer OOMs.
2. **Drop.** Discard data the consumer cannot process. This is the UDP model — packets in excess of the receive buffer are dropped at the kernel level. Acceptable for high-rate sensor data where individual events are low-value; unacceptable for events that must not be lost.
3. **Push back.** Tell the producer to slow down. This requires a feedback channel that push semantics do not provide by default — implementing it is essentially layering pull semantics on top of push.

Push is the right choice when (a) latency matters more than reliability, (b) the producer's rate is bounded by something the consumer can rely on (sensor specifications, network bandwidth), and (c) the cost of a dropped event is low enough to absorb. Outside of those conditions, push is hazardous — it produces systems that work until they don't and fail badly when they do.

### Pull Semantics

In pull, the consumer initiates the transfer. When the consumer is ready for more data, it sends a request; the producer responds with whatever is available. The consumer is in control: it consumes at its own rate.

Pull's cardinal property is **demand-driven flow**. The consumer asks only when it can process; the producer never sends faster than the consumer can absorb. This eliminates the consumer-side OOM mode of push and makes the producer-consumer relationship symmetric: both sides have a model of the other's pace.

Pull's cost is **latency** — every event waits at the producer until the next pull request arrives. For low-rate, low-latency-tolerant sources, this is negligible. For high-rate sources, it adds round-trip latency to every event and can become a bottleneck if the pull rate cannot keep up with production. Kafka mitigates this by allowing a single pull (`poll`) to return many events at once — `max.poll.records` is configurable up to 2,000 — so the round-trip cost is amortized across a batch. Network Programming with Rust covers similar batching strategies for non-Kafka pull-based protocols.

Pull is the right choice when (a) the consumer needs control over pace (because it is itself rate-limited downstream, or it processes batches), (b) the producer can hold data until requested (which means it has buffered or persistent storage), and (c) the additional round-trip latency is acceptable. Most production message queue clients are pull-based for exactly these reasons.

### Poll Semantics

Polling is pull on a fixed schedule. The consumer issues pull requests at regular intervals — every 30 seconds, every minute, every hour — regardless of whether the producer has new data. It is the simplest possible pull strategy and the easiest to reason about.

Polling has one major virtue: **statelessness**. The consumer needs no notification mechanism, no long-lived connection, no producer-side state about pending data. Every poll cycle is independent. This is why polling dominates in environments where stateful protocols are expensive: HTTP-based archive APIs, cron-driven batch ingest, IoT devices on intermittent connections, and the Meridian ISL beacon protocol — which has no notification support and exposes only a "give me the latest state" endpoint.

Polling has two costs. First, **inherent latency**: an event produced just after a poll cycle must wait until the next cycle. With a 30-second poll interval, average per-event latency is 15 seconds and worst-case is 30 seconds. Second, **wasted requests**: most polls return nothing new, especially when the producer is quiet. The poll-rate-vs-latency tradeoff is direct — halve the interval, double the request rate, halve the average latency. There is no free lunch.

The Kafka `poll` loop is a hybrid: it is poll-based from the consumer's perspective (the consumer calls `poll(timeout)` in a loop), but the broker uses long polling to avoid the wasted-request cost. The broker holds the request open until either new data arrives or the timeout expires; if new data arrives during that window, it is returned immediately. Long polling collapses poll's wasted-request cost while preserving the consumer-driven flow control. *Kafka: The Definitive Guide* Ch. 4 documents the relevant configurations: `fetch.min.bytes` (don't return until at least this much data is available, up to the timeout) and `fetch.max.wait.ms` (don't hold the request longer than this).

### Pattern Mismatch and Adaptation

The most common architecture problem in pipelines is **pattern mismatch**: the source uses one pattern and the pipeline expects another. The radar UDP source is push; the optical archive is pull; the ISL beacon must be polled. The pipeline's first stage cannot impose a single pattern on all three sources — it must adapt at the boundary, and the adaptation logic lives in the source implementation.

Three adaptation patterns:

* **Push to pull.** The source maintains an internal buffer; an external `next()` call pulls from the buffer. The radar source uses this pattern: `recv_from()` is push-based at the kernel level, but the source's `next()` method exposes a pull interface to the rest of the pipeline. The buffer (the kernel UDP receive buffer) is bounded; overflow drops at the kernel.

* **Pull to poll.** The source wraps a pull-based remote API in a polling loop. The optical archive source uses this pattern: it sleeps for the poll interval, makes a pull request to the REST endpoint, and yields the results. The poll interval is the dominant latency cost.

* **Poll to pull.** The source polls on its own schedule and exposes the most recently fetched data on `next()`. The ISL beacon source uses this when the beacon's protocol doesn't support pulls — a background task polls, the foreground `next()` reads from a shared cell. The latency includes the polling interval plus the time to detect a change.

The pipeline interior, after the source layer, runs entirely on push semantics — `tokio::sync::mpsc` is push from the sender's perspective, with backpressure providing the rate-control loop that pure push lacks. This is a deliberate architecture choice: by adapting at the boundary, the rest of the pipeline gets uniform semantics and you don't have to reason about three different patterns at every operator.

### Choosing a Pattern

The decision matrix is small:

| Source property | Push | Pull | Poll |
|---|---|---|---|
| Latency-sensitive (<100 ms) | ✓ | only with batching | ✗ |
| Producer rate exceeds consumer rate | risky | ✓ | ✓ |
| Producer cannot buffer | ✓ | ✗ | ✗ |
| Consumer wants to control pace | ✗ | ✓ | ✓ |
| No notification mechanism on wire | adapt to push at source | adapt to pull at source | ✓ |
| Network is intermittent | ✗ | risky | ✓ |
| Source is bursty | only with consumer-side buffer | ✓ | poor — wastes polls during bursts |

The Reis and Housley framing in FDE Ch. 7 is that push is appropriate for systems where latency dominates, pull for systems where consumer control dominates, and poll for systems where simplicity dominates. None is universally correct; the right answer is whichever matches the source's wire-protocol capabilities and the pipeline's downstream needs. The wrong answer is whichever the developer was most familiar with from a prior project.

---

## Code Examples

### Adapting an HTTP Pull Source: The Optical Archive

The optical telescope archive exposes a REST endpoint at `https://optical-archive.meridian.internal/observations?since={timestamp}`. The endpoint returns a JSON array of observations. The source's job is to wrap this into a pull-based `ObservationSource`.

```rust,ignore
use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use std::collections::VecDeque;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;

#[derive(Deserialize)]
struct OpticalRecord {
    obs_id: String,
    timestamp_ns: u64,
    ra_rad: f64,
    dec_rad: f64,
    sigma_arcsec: f64,
    site_id: String,
}

pub struct OpticalArchiveSource {
    client: Client,
    endpoint: String,
    poll_interval: Duration,
    /// High-water mark: timestamp of the most recent observation we have
    /// already consumed. Sent as the `since` parameter to avoid reprocessing.
    /// Persisting this across restarts is a Module 5 concern; for now we
    /// start fresh and may briefly reprocess on restart.
    high_water_mark_ns: u64,
    /// Local buffer of fetched-but-not-yet-yielded observations. Smooths
    /// the burstiness of a single fetch returning many records.
    buffer: VecDeque<Observation>,
    name: String,
}

impl OpticalArchiveSource {
    pub fn new(endpoint: impl Into<String>, poll_interval: Duration) -> Self {
        Self {
            client: Client::new(),
            endpoint: endpoint.into(),
            poll_interval,
            high_water_mark_ns: 0,
            buffer: VecDeque::new(),
            name: "optical-archive".into(),
        }
    }

    /// Fetch the next batch of observations from the archive. Returns the
    /// number of new observations buffered.
    async fn fetch_batch(&mut self) -> Result<usize> {
        let url = format!("{}?since={}", self.endpoint, self.high_water_mark_ns);
        let records: Vec<OpticalRecord> = self.client
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .context("optical archive HTTP GET")?
            .error_for_status()?
            .json()
            .await
            .context("optical archive JSON parse")?;

        let count = records.len();
        for r in records {
            // Advance the high-water mark as we consume the batch.
            // The archive returns records in timestamp order, so the last
            // record sets the new mark.
            self.high_water_mark_ns = self.high_water_mark_ns.max(r.timestamp_ns);

            self.buffer.push_back(Observation {
                observation_id: uuid::Uuid::new_v4(),
                source_id: SourceId(format!("optical-{}", r.site_id)),
                source_kind: SourceKind::Optical,
                sensor_timestamp: SystemTime::UNIX_EPOCH
                    + Duration::from_nanos(r.timestamp_ns),
                ingest_timestamp: SystemTime::now(),
                target: ObservationTarget::Angular {
                    ra_rad: r.ra_rad,
                    dec_rad: r.dec_rad,
                },
                uncertainty: Uncertainty {
                    // Convert arcseconds to radians for downstream uniformity.
                    sigma: r.sigma_arcsec * std::f64::consts::PI / (180.0 * 3600.0),
                },
            });
        }
        Ok(count)
    }
}

#[async_trait]
impl ObservationSource for OpticalArchiveSource {
    async fn next(&mut self) -> Result<Option<Observation>> {
        loop {
            // Buffered observations from a prior fetch take priority —
            // we drain them before issuing a new pull.
            if let Some(obs) = self.buffer.pop_front() {
                return Ok(Some(obs));
            }
            // Buffer empty: fetch a new batch. If the archive returns
            // nothing, sleep for the poll interval and try again.
            // This is the poll part of the pattern.
            match self.fetch_batch().await {
                Ok(0) => {
                    sleep(self.poll_interval).await;
                    // Loop back to fetch again.
                }
                Ok(_) => {
                    // Buffer now has records; loop back to pop_front.
                }
                Err(e) => {
                    // Transient error: log and retry after the poll interval.
                    // Module 5 covers proper retry/backoff strategies; this
                    // is the minimum viable behavior.
                    tracing::warn!("optical archive fetch failed: {e:#}");
                    sleep(self.poll_interval).await;
                }
            }
        }
    }

    fn name(&self) -> &str { &self.name }
}
```

This source is *poll-to-pull adaptation* in code. The wire protocol is HTTP (a pull primitive), but the consumer's `next()` is also pull (the pipeline asks when it wants more). The polling layer is internal: when the buffer empties, the source decides whether to issue another HTTP request immediately or sleep for the poll interval. The `since` query parameter is the watermark mechanism — without it, every poll would return all observations and the source would either drown in duplicates or have to deduplicate downstream. The watermark approach is the standard way to convert a non-incremental REST API into an incremental stream. Note the operational consequence of the poll interval: at 5 seconds, the average optical observation waits 2.5 seconds at the archive before reaching the pipeline. For SDA's conjunction-detection SLA (sub-30-second end-to-end), this is comfortable. If the SLA tightened to 5 seconds, we would either need to push the optical archive team to add a notification mechanism (push) or shorten the poll interval significantly (which costs them server capacity). This is the conversation the pattern choice forces.

### Long-Polling: The Kafka Pattern Applied

For sources where latency matters but the producer can hold open requests, long polling combines the simplicity of polling with the latency of push. The pipeline's poll request stays open at the producer until either new data arrives or the timeout expires.

```rust,ignore
/// A long-polling source. The remote endpoint supports a `wait_ms` parameter:
/// the request blocks at the server until either a new observation is
/// available or `wait_ms` elapses, whichever comes first. This pattern is
/// borrowed directly from how Kafka's poll() works at the broker.
pub struct LongPollSource {
    client: Client,
    endpoint: String,
    /// How long the server holds the request before returning empty.
    /// Trading off: longer = lower request rate but slower shutdown response.
    wait_ms: u32,
    high_water_mark_ns: u64,
    name: String,
}

#[async_trait]
impl ObservationSource for LongPollSource {
    async fn next(&mut self) -> Result<Option<Observation>> {
        loop {
            let url = format!(
                "{}?since={}&wait_ms={}",
                self.endpoint, self.high_water_mark_ns, self.wait_ms,
            );
            // Note the request timeout exceeds wait_ms — the server will
            // reliably return within wait_ms; we add a small grace period
            // to tolerate network jitter without aborting valid requests.
            let resp = self.client
                .get(&url)
                .timeout(Duration::from_millis(self.wait_ms as u64 + 2_000))
                .send()
                .await?
                .error_for_status()?
                .json::<Vec<OpticalRecord>>()
                .await?;

            if resp.is_empty() {
                // Server returned no new data within wait_ms.
                // Loop immediately to issue another long poll — no sleep.
                continue;
            }
            // Server returned records: convert and yield the first one.
            // (Production code would buffer the rest like OpticalArchiveSource.)
            let r = &resp[0];
            self.high_water_mark_ns = self.high_water_mark_ns.max(r.timestamp_ns);
            return Ok(Some(/* convert r to Observation, see prior example */
                # Observation {
                #     observation_id: uuid::Uuid::new_v4(),
                #     source_id: SourceId(format!("optical-{}", r.site_id)),
                #     source_kind: SourceKind::Optical,
                #     sensor_timestamp: SystemTime::UNIX_EPOCH
                #         + Duration::from_nanos(r.timestamp_ns),
                #     ingest_timestamp: SystemTime::now(),
                #     target: ObservationTarget::Angular {
                #         ra_rad: r.ra_rad,
                #         dec_rad: r.dec_rad,
                #     },
                #     uncertainty: Uncertainty {
                #         sigma: r.sigma_arcsec * std::f64::consts::PI / (180.0 * 3600.0),
                #     },
                # }
            ));
        }
    }

    fn name(&self) -> &str { &self.name }
}
```

The latency profile of long polling is the producer's data-arrival latency plus one network round trip — essentially the same as push, with no producer-side notification mechanism required. The cost is slightly more server capacity (each consumer holds an open connection) and a deliberate choice of `wait_ms` that balances request volume against shutdown responsiveness. Kafka brokers default to a 500-ms maximum wait, which is the right order of magnitude for most systems. Note that long polling shifts complexity to the server: the server must support holding requests open, which not all REST APIs do. When it does, long polling is almost always preferable to fixed-interval polling.

### A Push-Source-with-Backpressure-Adaptation

Sometimes a push source needs to be slowed down. The radar UDP source can't be slowed down (the radar emits whether anyone is listening), but a TCP-based push source can be — by simply not reading from the socket. Modern OS TCP stacks signal back-pressure all the way to the producer when the consumer's receive buffer fills.

```rust,ignore
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

/// A push-based source over TCP. The producer streams length-prefixed binary
/// frames. The transport-level backpressure (TCP windowing) automatically
/// slows the producer when we stop reading — but only if our application-level
/// reads are themselves controlled by backpressure from the downstream sink.
pub struct TcpPushSource {
    stream: TcpStream,
    buf: Vec<u8>,
    name: String,
}

impl TcpPushSource {
    pub async fn connect(addr: &str, name: impl Into<String>) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream,
            buf: vec![0u8; 65_536],
            name: name.into(),
        })
    }
}

#[async_trait]
impl ObservationSource for TcpPushSource {
    async fn next(&mut self) -> Result<Option<Observation>> {
        // Read a 4-byte length prefix.
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let frame_len = u32::from_le_bytes(len_buf) as usize;
        if frame_len > self.buf.len() {
            anyhow::bail!("push source: frame size {frame_len} exceeds buffer {}", self.buf.len());
        }
        // Read exactly frame_len bytes.
        self.stream.read_exact(&mut self.buf[..frame_len]).await?;
        // Parse and return as Observation. Implementation omitted for brevity;
        // see the radar source for the deserialization pattern.
        Ok(Some(parse_isl_frame(&self.buf[..frame_len])?))
    }

    fn name(&self) -> &str { &self.name }
}
```

The interesting thing about this source is what happens when the pipeline downstream is slow. The orchestrator's call to `next()` will not happen as quickly. The TCP stream's read buffer (kernel-level) fills up. The kernel's TCP window shrinks. The remote producer's send window shrinks. The producer's writes block at the syscall level. The producer is slowed down — automatically, by the operating system, with no application-level coordination required. This is the hidden virtue of TCP-based push: backpressure traverses the network for free, *as long as* the application never reads faster than it can process. A push source that internally buffered into an unbounded queue would defeat this. Using `read_exact` synchronously inside `next()` preserves it. The Network Programming with Rust text covers TCP windowing and its interaction with application-level I/O in detail.

> **Source note:** This lesson synthesizes pattern-choice guidance from FDE Ch. 7 ("Push Versus Pull Versus Poll Patterns"), Streaming Data Ch. 2 ("Common Interaction Patterns"), and Kafka Ch. 4 ("The Poll Loop"). The long-polling pattern as described matches Kafka's broker-side `fetch.max.wait.ms` mechanism; the SDA pipeline applies the same pattern to a custom REST API. The TCP-windowing claim about backpressure-for-free is well-established in network-programming texts (Stevens, *TCP/IP Illustrated*) but worth verifying against the production behavior of the specific TCP stack and kernel you deploy on.

---

## Key Takeaways

* **Push, pull, and poll are not implementation details** — they are architectural choices that determine latency, flow control, and failure modes for every interaction in the pipeline. Choose deliberately, document the choice, and review it when requirements change.
* **Push minimizes latency but offloads flow control to the consumer.** When push is appropriate (high-rate, low-event-cost sources where drop-on-overload is acceptable), it is the best choice. When it isn't, push systems fail badly under load.
* **Pull gives the consumer rate control.** Most production message queue clients are pull-based; the round-trip cost is amortized by batching multiple events per pull request. Kafka's `max.poll.records` and `fetch.min.bytes` are the canonical knobs.
* **Long polling is the practical compromise.** It collapses the wasted-request cost of fixed-interval polling while preserving consumer-driven flow control. When the producer supports it, long polling is almost always preferable to fixed-interval polling.
* **TCP windowing provides free backpressure** for push-over-TCP sources, *as long as* the application never reads faster than it can process. Internal unbounded application buffering breaks the chain. Stick to bounded buffers and synchronous read-then-process loops to preserve transport-level backpressure end-to-end.

---

{{#quiz lesson-03-quiz.toml}}
