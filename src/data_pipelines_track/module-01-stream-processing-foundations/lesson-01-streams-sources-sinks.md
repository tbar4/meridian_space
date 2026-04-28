# Lesson 1 — Streams, Sources, and Sinks

**Module:** Data Pipelines — M01: Stream Processing Foundations
**Position:** Lesson 1 of 3
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (Stream Processing); *Streaming Data* — Andrew Psaltis, Chapter 1 (Introducing Streaming Data); *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 7 (Ingestion: Bounded vs Unbounded Data)

---

## Context

Every streaming system answers one question before any other: where does data enter, and where does it leave? The pipeline between those two points can grow arbitrarily complex — windowing, joins, stateful aggregation, exactly-once delivery — but the entry and exit points are non-negotiable. They are the contract between the pipeline and the rest of the world. Get them wrong and no amount of clever processing recovers the system.

The mental shift from batch to streaming is harder than it sounds. A batch job is a function: you give it a finite input, it returns a finite output, it terminates. A streaming pipeline is a *process*: it runs forever, it consumes a potentially infinite input, it produces a potentially infinite output, and "completion" is a category error. Most production incidents in streaming systems trace back to engineers who built a batch job and called it a stream — fixed-size buffers that fill up, retries that assume idempotency the source doesn't provide, "is the job done yet?" health checks against a process that has no notion of done.

For the SDA Fusion Service, the source side is three heterogeneous feeds. Ground-based X-band radar arrays push detection records over UDP at 100–500 Hz per array. Optical telescopes expose a REST API that returns observation batches when polled. The constellation's inter-satellite link beacons emit position reports on a 1 Hz cadence over a custom binary protocol. The sink side is a single fusion stage that expects one normalized envelope format. This module's job is to define what that envelope is, what a source and sink mean in this system, and how to think about the data crossing the boundary.

---

## Core Concepts

### What Makes a Stream a Stream

The defining property of a stream is **unboundedness**: there is no expectation that the input will end. This is the property that forces every other architectural decision in a streaming system. Memory cannot grow without bound, so state must be either fixed-size or explicitly bounded by time or count. Completeness checks (`COUNT(*)` over a stream, `is the result correct?`) cannot terminate, so they must be redefined as point-in-time approximations. Failure recovery cannot rely on "rerun the job from the start" because there is no start.

The DDIA framing is precise: a stream is a sequence of *events*, each event a small, immutable record describing something that happened at a point in time. Events are produced by *producers* (also called *sources*, *publishers*, *emitters*) and consumed by *consumers* (also called *sinks*, *subscribers*, *recipients*). The stream itself is not the events — it is the abstraction over the path they take from producer to consumer.

A finite, replayable input is *not* a stream just because it is processed event-by-event. A 10 GB log file processed line-by-line in a Spark job is a batch job with a streaming-style execution model. The distinction matters because finite inputs admit completion: you can compute exact aggregates, you can verify correctness end-to-end, you can retry the whole computation. Unbounded inputs admit none of these. Bounded data running through streaming infrastructure (sometimes called "stream-with-known-end") is real but rare in practice; treating an unbounded source as if it were bounded is a common and expensive mistake.

### Bounded vs Unbounded Data

The bounded/unbounded distinction is the most important typology in data engineering. Reis and Housley make the case in *Fundamentals of Data Engineering* Ch. 7: the boundary you draw at ingestion shapes every downstream system. If you treat data as bounded, you can use ETL, you can do exact joins, you can cleanly partition by date. If you treat it as unbounded, you must use windowing, you must accept approximation, you must deal with late arrivals.

Sensor data from the SDA Fusion Service is unambiguously unbounded — the radar arrays will emit observations as long as there are objects in the sky and the radar has power. The fusion service has no notion of "the last observation"; the input is a continuous flow that the pipeline drinks from for as long as it operates.

Some sources are bounded but feel unbounded. The optical telescope archive exposes the last 24 hours of observations on demand. From the pipeline's perspective, the source produces observations *now*, and the archive happens to also expose recent ones. Treating it as a bounded "give me the last 24 hours" source produces a different system than treating it as an unbounded "show me observations as they appear" source — even though the underlying data is the same. The latter requires the pipeline to track what it has already consumed (a watermark or offset) and to poll only for new observations since that point.

The architectural rule: bounded sources can be treated as unbounded (just process them and stop when they end), but unbounded sources cannot be treated as bounded without introducing artificial cutoffs. When in doubt, treat it as unbounded.

### Sources and Sinks as a Boundary

A *source* is the component that produces events into the pipeline. It is the boundary between the pipeline and the upstream world — the radar firmware, the satellite telemetry stream, the upstream Kafka topic, the message queue. The source's responsibility is to deliver events into the pipeline's first stage in a format the pipeline understands.

A *sink* is the symmetric component at the output: the boundary between the pipeline and the downstream world. The sink writes events to wherever they need to go — another Kafka topic, an object store, a database, a subscriber callback, a downstream service.

The pivotal insight is that source and sink are *positions*, not types. A Kafka topic is a sink for the pipeline that produces into it and a source for the pipeline that consumes from it. This is why production streaming architectures look like graphs of pipelines connected by durable queues — each queue is the sink of its writer and the source of its readers, and the queue's durability decouples them in time and in failure modes.

Three properties of a source determine how the pipeline must interact with it:

1. **Replayability.** Can the pipeline ask the source for events it has already consumed? Kafka can (configurable retention; consumers track offsets); a UDP radar feed cannot (packets arrive once and are gone if missed).
2. **Ordering guarantees.** Does the source guarantee a total order, a per-partition order, or no order at all? UDP gives no order. Kafka guarantees per-partition order. ISL beacons typically guarantee per-satellite order but not cross-satellite order.
3. **Delivery guarantees.** Does the source guarantee at-least-once delivery, at-most-once, or exactly-once? UDP is at-most-once. TCP-based sources are at-least-once if the application acks correctly. Kafka producers can be configured for exactly-once via the idempotent producer (covered in Module 5).

These three properties propagate. A pipeline cannot offer a stronger delivery guarantee than its weakest source unless it is willing to drop, deduplicate, or buffer to make up the difference.

### The Observation Envelope

When the pipeline accepts events from heterogeneous sources, the first stage's job is to normalize them into a single internal format. This is the *envelope* — a wrapper that preserves source-specific provenance while presenting a uniform interface to downstream stages.

For SDA fusion, every observation, regardless of source, has the same logical content: *something* was detected at *some position* at *some time*. The wire formats differ wildly — radar produces complex IQ samples reduced to range-rate pairs; optical produces angular measurements with timestamps; ISL produces full state vectors — but the downstream correlator does not need to know any of that. It needs position, time, source identifier, and uncertainty.

The envelope pattern:

```rust,ignore
struct Observation {
    // Identity and provenance
    source_id: SourceId,           // which sensor produced this
    source_kind: SourceKind,       // radar | optical | isl
    sensor_timestamp: SystemTime,  // when the sensor recorded it
    ingest_timestamp: SystemTime,  // when we received it

    // The actual observation payload
    target: ObservationTarget,     // what was observed (position, range-rate, etc.)
    uncertainty: Uncertainty,      // standard deviation or covariance

    // Routing and dedup
    observation_id: Uuid,          // unique per observation
}
```

The envelope is *thin*. It carries enough provenance to trace any observation back to its source and enough payload for the next stage to act on, but no more. Production envelopes drift toward fat over time as engineers add convenience fields; resist this. Every field in the envelope is paid for in CPU (deserialization), memory (buffering), and network (when stages run on different hosts).

A common mistake is to make the envelope a sum type that holds the original wire format plus normalized fields. This produces an envelope twice the necessary size and tempts downstream stages to peek at the original wire format, breaking the abstraction. If the original wire format must be preserved (for replay, audit, or forensic analysis), write it to a parallel "raw observations" sink at ingestion time. Don't carry it through the pipeline.

---

## Code Examples

### Defining the Observation Envelope

The envelope is the type that flows through every stage of the SDA Fusion Service. The choice of representation here propagates everywhere — a poor envelope makes every downstream lesson harder.

```rust,ignore
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

/// The kind of sensor that produced an observation. This drives source-specific
/// handling at later stages (e.g., uncertainty models differ by sensor type).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceKind {
    /// Ground-based X-band radar — high-rate, range-rate measurements
    Radar,
    /// Ground-based optical telescope — angular measurements
    Optical,
    /// Inter-satellite link beacon — full state vector reports
    InterSatelliteLink,
}

/// Stable identifier for the specific sensor that produced this observation.
/// Distinct from SourceKind: there are 14 X-band radars in the network, and we
/// want to know *which* one detected this object.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceId(pub String);

/// What the sensor observed. The variants reflect what each sensor type
/// actually measures — we do not pretend they all measure position vectors.
/// The correlator stage (Module 2) is responsible for fusing these into
/// position estimates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObservationTarget {
    /// Range, range-rate, azimuth, elevation from a radar
    RangeRate { range_m: f64, range_rate_m_s: f64, az_rad: f64, el_rad: f64 },
    /// Right ascension and declination from an optical telescope
    Angular { ra_rad: f64, dec_rad: f64 },
    /// ECI-frame state vector from an ISL beacon
    StateVector { position_m: [f64; 3], velocity_m_s: [f64; 3] },
}

/// Per-measurement uncertainty. Production code uses full covariance matrices;
/// this is a starting representation that we will refine in Module 3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Uncertainty {
    pub sigma: f64,
}

/// The canonical envelope. Every observation in the pipeline has this shape.
/// Downstream stages do not look at wire formats — only at this struct.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observation {
    pub observation_id: Uuid,
    pub source_id: SourceId,
    pub source_kind: SourceKind,
    pub sensor_timestamp: SystemTime,
    pub ingest_timestamp: SystemTime,
    pub target: ObservationTarget,
    pub uncertainty: Uncertainty,
}
```

Three things to notice. First, `observation_id` is a UUID, not a sequence number. UUIDs are generated at the source without coordination — sequence numbers would require a central allocator and would become a bottleneck under load. The tradeoff is that UUIDs are 16 bytes versus 8 for a u64; for SDA's volumes (hundreds of thousands of observations per minute), this is a non-issue. Second, the envelope holds *both* `sensor_timestamp` and `ingest_timestamp`. This distinction (event time vs processing time) becomes load-bearing in Module 3, but the data must be captured at ingestion or it is lost forever. Third, `ObservationTarget` is a sum type rather than a normalized "always position vector" representation. Forcing premature unification at ingestion discards information that the correlation stage needs. Normalize the *envelope*; preserve the *measurement*.

### A Source Trait

A source is a thing that produces a stream of observations. In Rust, the natural shape is an async trait that returns successive items:

```rust,ignore
use anyhow::Result;
use async_trait::async_trait;

/// A source produces observations from some upstream system. Implementations
/// hide the wire-format details behind this trait.
///
/// Cancellation: implementations should be cancel-safe at await points.
/// Dropping the future returned by `next` must not corrupt the source's state.
#[async_trait]
pub trait ObservationSource: Send {
    /// Yield the next observation, or None if the source has terminated.
    /// For genuinely unbounded sources (radar feeds), this never returns None.
    async fn next(&mut self) -> Result<Option<Observation>>;

    /// A stable identifier for logging and metrics. Not the same as
    /// SourceId on the envelope — a single source instance may produce
    /// observations from multiple SourceIds (e.g., one ISL listener
    /// receives beacons from many satellites).
    fn name(&self) -> &str;
}
```

Two design choices to flag. The trait returns `Option<Observation>` rather than just `Observation` because we want to signal *graceful termination* without using an error. Errors are reserved for actual failures (network drop, deserialization error, source-specific protocol violation). A radar source that never returns `None` is correct. An optical archive source that returns `None` when the archive has no new observations and the polling cadence has been satisfied is also correct. Second, the trait is not a `Stream`. We could have used `futures::Stream<Item = Result<Observation>>` and gained combinator support, but that buys us less than it costs: the explicit `next` method makes lifecycle management (logging, retries, source-specific timeouts) easier to compose. Modules 2 and 4 will build the orchestration layer around this trait.

### A Minimal UDP Radar Source

This is the actual code that ingests from one of the X-band radar arrays. The radar firmware emits a fixed-size binary frame over UDP at 100–500 Hz; our job is to deserialize it and emit envelopes.

```rust,ignore
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::net::UdpSocket;
use uuid::Uuid;

/// Wire format emitted by the X-band radar firmware. 64 bytes, packed.
/// Field layout is documented in Meridian-RF-2024-RADAR-WIRE-FORMAT.
#[repr(C, packed)]
#[derive(Clone, Copy)]
struct RadarFrame {
    array_id: u32,
    target_track_id: u32,
    timestamp_ns: u64,        // sensor-local clock since UNIX epoch
    range_m: f64,
    range_rate_m_s: f64,
    azimuth_rad: f64,
    elevation_rad: f64,
    sigma_range_m: f32,
    sigma_rate_m_s: f32,
    _reserved: [u8; 4],
}
const RADAR_FRAME_SIZE: usize = std::mem::size_of::<RadarFrame>();

pub struct UdpRadarSource {
    socket: UdpSocket,
    name: String,
    buf: Box<[u8; 1500]>,  // standard MTU; larger frames would arrive truncated
}

impl UdpRadarSource {
    pub async fn bind(addr: &str, name: impl Into<String>) -> Result<Self> {
        let socket = UdpSocket::bind(addr)
            .await
            .with_context(|| format!("binding radar source on {addr}"))?;
        Ok(Self {
            socket,
            name: name.into(),
            buf: Box::new([0u8; 1500]),
        })
    }
}

#[async_trait]
impl ObservationSource for UdpRadarSource {
    async fn next(&mut self) -> Result<Option<Observation>> {
        // recv_from is cancel-safe in tokio: a dropped future leaves no
        // partially-consumed datagram. This matters for the orchestrator
        // (Module 2), which cancels source tasks during shutdown.
        let (n, _peer) = self.socket.recv_from(&mut self.buf[..]).await
            .context("recv_from on radar UDP socket")?;

        if n != RADAR_FRAME_SIZE {
            // Truncated or oversized frame — log and drop. UDP gives no
            // recovery; the radar will emit the next frame in ~2-10ms.
            anyhow::bail!("radar frame size {n} != expected {RADAR_FRAME_SIZE}");
        }

        // SAFETY: we just verified the byte count matches the struct size,
        // and RadarFrame is #[repr(C, packed)] of POD types. The radar firmware
        // is documented to emit little-endian on the wire and our hosts are
        // little-endian; if we ever deploy on big-endian hosts we add a swap.
        let frame: RadarFrame = unsafe {
            std::ptr::read_unaligned(self.buf.as_ptr() as *const RadarFrame)
        };

        let sensor_ts = UNIX_EPOCH + Duration::from_nanos(frame.timestamp_ns);
        let array_id = frame.array_id; // copy out of packed struct for formatting

        Ok(Some(Observation {
            observation_id: Uuid::new_v4(),
            source_id: SourceId(format!("radar-{}", array_id)),
            source_kind: SourceKind::Radar,
            sensor_timestamp: sensor_ts,
            ingest_timestamp: SystemTime::now(),
            target: ObservationTarget::RangeRate {
                range_m: frame.range_m,
                range_rate_m_s: frame.range_rate_m_s,
                az_rad: frame.azimuth_rad,
                el_rad: frame.elevation_rad,
            },
            uncertainty: Uncertainty {
                sigma: frame.sigma_range_m as f64,
            },
        }))
    }

    fn name(&self) -> &str { &self.name }
}
```

A few points worth dwelling on. UDP gives no delivery guarantee — if a radar frame is lost in transit, it's gone. For a sensor producing 100–500 frames per second per array, this is acceptable; the consequence is slightly higher uncertainty in the fused track, not a missed conjunction. If we needed at-least-once delivery here, we would need a different transport (Kafka with a radar-side producer, for instance) and we would lose the simplicity of UDP. The choice of transport is a delivery-guarantee decision, not just a performance decision. We will return to this in Module 5. The `unsafe` block deserializing the frame is necessary because the wire format is `#[repr(C, packed)]` and UDP buffers have no alignment guarantee. Production systems use crates like `zerocopy` or `bytemuck` to make this safe; we use raw `read_unaligned` here for clarity. Either way, the cost of the deserialization is single-digit nanoseconds per frame, far below the per-frame budget.

### A Minimal Sink

A sink consumes observations and writes them somewhere. The simplest possible sink is one that pushes envelopes onto an MPSC channel for the next stage to consume:

```rust,ignore
use tokio::sync::mpsc;

/// A sink consumes observations from a source and forwards them onward.
/// The simplest sink is a channel send: the next stage owns the receiver
/// and pulls from it.
pub struct ChannelSink {
    tx: mpsc::Sender<Observation>,
    name: String,
}

impl ChannelSink {
    pub fn new(tx: mpsc::Sender<Observation>, name: impl Into<String>) -> Self {
        Self { tx, name: name.into() }
    }

    /// Forward an observation to the downstream stage.
    /// Returns Err if the receiver has been dropped (downstream is gone).
    pub async fn write(&self, obs: Observation) -> Result<()> {
        // .send().await applies backpressure: if the channel is full,
        // this future does not resolve until there is capacity. The
        // upstream source is forced to wait. This is the right behavior —
        // we will analyze it in depth in Module 4.
        self.tx.send(obs).await
            .map_err(|_| anyhow::anyhow!("downstream receiver dropped"))?;
        Ok(())
    }

    pub fn name(&self) -> &str { &self.name }
}
```

The single most important property of this sink is that `write` *awaits*. When the downstream channel is full (because the next stage is slow), the source's `next().await` plus the sink's `write().await` form a chain that propagates backpressure all the way back to the radar UDP socket — at which point we start dropping packets at the kernel level rather than building unbounded memory in the application. This is the foundation of the dataflow model we'll cover in Lesson 2 and the explicit subject of Module 4. A non-awaiting sink (one that internally buffered into an unbounded queue) would silently OOM the process during a fragmentation event surge. The choice between bounded and unbounded internal buffering is a load-bearing architectural decision masquerading as an implementation detail.

---

## Key Takeaways

* A stream is defined by **unboundedness**: the input has no expected end. This single property dictates that state must be bounded, completeness is point-in-time, and recovery cannot rerun from the start. Treating an unbounded source as bounded is a category error that produces predictable failures under load.
* Sources and sinks are **boundary positions**, not data types. The Kafka topic that is the sink of one pipeline is the source of the next. This composition is why production streaming architectures look like graphs connected by durable queues.
* A source is characterized by three properties — **replayability, ordering, delivery guarantees** — and the pipeline cannot offer stronger guarantees than its weakest source without compensating mechanisms.
* The **observation envelope** unifies heterogeneous wire formats behind a single internal type. Capture provenance (source ID, sensor timestamp, ingest timestamp) at the boundary; preserve the original measurement form rather than prematurely normalizing to a single shape.
* `tokio::sync::mpsc::Sender::send().await` is the foundation of backpressure. A source that awaits its sink and a sink that awaits its downstream channel form a chain that propagates pressure to the kernel. Internal unbounded buffering breaks this chain and produces silent OOMs.

---

{{#quiz lesson-01-quiz.toml}}
