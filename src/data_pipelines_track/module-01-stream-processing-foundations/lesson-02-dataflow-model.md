# Lesson 2 — The Dataflow Model

**Module:** Data Pipelines — M01: Stream Processing Foundations
**Position:** Lesson 2 of 3
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (Stream Processing, "Processing Streams" section); *Kafka: The Definitive Guide* — Shapira et al., Chapter 14 (Stream Processing: Topology, State); *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 3 ("The Dataflow Model and Unified Batch and Streaming")

---

## Context

The dataflow model is the conceptual frame that production stream processing systems are organized around. Kafka Streams, Apache Flink, Apache Beam, the internal architecture of Tokio's `Stream` combinators, every Rust pipeline you'll write in this track — they all express their work as a *graph of operators* that consume from upstream, transform, and produce downstream. Understanding the model gives you a vocabulary to reason about pipelines that scales from single-process Rust binaries to multi-cluster Beam jobs.

The shift from imperative to dataflow is the same shift you made when you moved from `for` loops to iterator combinators in Rust. An imperative pipeline says "loop over inputs, do step A, do step B, do step C." A dataflow pipeline says "compose operator A with operator B with operator C; the framework decides how to schedule the work, how to parallelize it, where to introduce buffering, when to checkpoint." The shift is more than aesthetic — the dataflow representation is what enables the framework to make decisions an imperative loop can't (parallel execution across operators, automatic backpressure, exactly-once via barrier markers, topology-aware optimization). Reis and Housley argue in *Fundamentals of Data Engineering* Ch. 3 that the dataflow model is what unifies batch and streaming computation: a batch is a stream that ends, and the same operator graph can run over either.

For the SDA Fusion Service, the operator graph is the architecture document. Sources at the edges (radar, optical, ISL), sinks at the other edges (the catalog store, the alert emitter), and a chain of operators in between (normalize, dedupe, correlate, filter, enrich). When the conjunction-alert latency SLA is at risk, the question is which operator in the graph is the bottleneck. When the pipeline is rebuilt for a new sensor, the question is where in the graph the new branch attaches. Without the graph, every conversation is about implementation; with the graph, the conversation can be about architecture.

---

## Core Concepts

### Operators as Functions Over Streams

In the dataflow model, an *operator* is a function that takes one or more input streams and produces one or more output streams. The function is total — it is defined for every possible input event — but the output is not constrained to be a one-for-one mapping. An operator may emit zero, one, or many output events for each input event, and the relationship between input rate and output rate is part of the operator's specification.

The five canonical operator shapes are:

* **Map.** One input event in, one output event out. Stateless. The dominant operator in normalization stages — converting a wire-format frame to an `Observation` envelope is a map.
* **Filter.** One input event in, zero or one output events out. Stateless. Used to drop observations that fail validation, fall outside the area of interest, or duplicate ones already seen.
* **FlatMap.** One input event in, zero or more output events out. Stateless. A radar frame containing multiple detected targets becomes one event per target.
* **Fold (Aggregate).** One or more input events in, one output event out, with state accumulated across events. Stateful. Computing a running mean of range-rate per object is a fold.
* **Window (Group).** A grouping operator that collects events into bounded buckets — by time, by count, by session — and emits one output per bucket when the window closes. Stateful and time-aware. Conjunction risk computation in Module 3 is a windowed operator.

A sixth shape that doesn't fit neatly into the above is the *join*, which takes two input streams and produces a single output stream containing matched pairs. Joins are the most expensive operator class — they require state proportional to the unmatched-but-still-relevant events from both sides — and we cover them in detail in Module 3.

The DDIA framing is that operators are *streaming versions of relational algebra*. Map is projection, filter is selection, fold is aggregation, window is group-by, join is join. The same algebraic identities hold (you can push filters past maps, you can fuse adjacent maps), and the same costs apply (joins are the expensive operation, aggregations require state). If you have an intuition for how a SQL query gets optimized, you have the foundation to reason about a streaming topology.

### The Pipeline as a Topology

The full pipeline — sources to sinks — is a directed graph. Vertices are operators (including sources and sinks); edges are streams flowing between operators. Kafka Streams calls this a *topology*; Flink calls it a *job graph*; Beam calls it a *pipeline*. They are all the same object.

The topology has structural properties that matter:

* **Linear vs branching.** A linear topology has a single path from source to sink. A branching topology has fan-out (one operator feeds multiple downstreams) or fan-in (multiple operators feed one downstream). The SDA pipeline is both: three sources fan in to a single normalization stage, then fan out to a correlator and a raw archive sink in parallel.
* **Acyclic vs cyclic.** Almost all production topologies are acyclic. Cycles introduce hard problems: when does a fixed point exist? How is termination defined? How does backpressure traverse a cycle? Iterative algorithms in Beam and Flink support cycles with explicit barrier semantics, but the cost is significant. Treat cycles as a smell.
* **Stateless vs stateful operators.** Stateless operators (map, filter, flatmap) can be parallelized trivially — replicate the operator N ways and load-balance events across the replicas. Stateful operators (fold, window, join) require *partitioning* — events that share a key must go to the same operator replica, because the state for that key is held there.

The topology view is what makes streaming pipelines explainable to other engineers and to operators. A diagram showing source-to-sink connectivity, with annotations for which operators are stateful and how the streams are partitioned, is more useful than any amount of source code for understanding why the pipeline behaves the way it does.

### Statelessness, State, and Partitioning

The most consequential distinction among operators is whether they carry state. A stateless operator is a pure function — given the same input, it produces the same output every time. It can be torn down and rebuilt on a new host with no recovery; it can run with arbitrary parallelism. A stateful operator carries information between events: a counter, a window of recent values, a lookup table. State is the source of operational complexity in streaming systems. Every stateful operator is a question about checkpointing, recovery, exactly-once semantics, and partitioning.

Where does the state live? Three choices, in order of increasing operational cost:

* **In-process state.** A `HashMap` inside the operator. Fast, simple, lost on crash, doesn't survive rescaling. Acceptable for low-importance operators or for operators whose state can be reconstructed from the input stream by replaying recent events.
* **Embedded persistent state.** RocksDB or sled inside the operator process. Fast for local access, requires explicit checkpointing for recovery, requires partition-aware redistribution when scaling. This is what Kafka Streams and Flink use for their state backends.
* **External state.** A separate database or cache (Redis, Cassandra, the OOR storage engine you built in the Database Internals track). Slow per access, easy to share across operator replicas, decouples scaling from state. Used when state must be queryable from outside the pipeline.

The choice of state backing is one of the most consequential decisions in pipeline architecture. We will not implement embedded persistent state in this track — it would consume the entire track on its own — but we will use in-process state in Modules 2 and 3 and discuss the implications of moving to embedded state in Module 5.

Partitioning is the bridge between state and parallelism. A stateful operator is partitioned by a *key* — for the SDA correlator, the key is the orbital object identifier. All observations of object `2024-001A` route to the same operator replica, where the state for that object lives. Partitioning is what allows stateful operators to scale: add more replicas, repartition the stream by key, and each replica owns a disjoint subset of keys. Partitioning is also where ordering guarantees come from in streaming systems: within a single partition, events are processed in order; across partitions, no order is guaranteed.

### Why Dataflow Beats Imperative Loops

You could write the SDA pipeline as a single async function that reads from sources, transforms in-line, and writes to sinks. Many systems start that way. Three things go wrong as the pipeline grows:

1. **Mixing concerns.** The function ends up containing transport details (UDP receive logic), serialization (binary frame parsing), validation (drop frames with impossible range rates), business logic (correlation), and observability (metrics and lineage). Every change touches a function that touches everything else.

2. **Fixed parallelism.** A monolithic loop runs at a single rate. If correlation is slow, ingestion is slow. If ingestion is slow, the radar UDP buffer overflows. The dataflow model lets each operator run at its own rate, with bounded buffers between them — slow operators can be replicated, fast operators can stay single-threaded.

3. **No structural visibility.** When a metric goes wrong (P99 latency rises, throughput drops, errors spike), the only handle on the system is the call stack. The dataflow model gives every edge in the graph a name and lets you instrument each one independently. Per-stage lag, per-stage throughput, per-stage error rate become first-class observable properties.

The Kafka Streams architecture documentation makes this explicit: the topology is the artifact you reason about, debug against, and scale. The code that implements the topology is much shorter and changes much less often than the equivalent imperative code would.

---

## Code Examples

### Building Operators on tokio Channels

The simplest way to express a pipeline graph in Rust is one task per operator, connected by `mpsc` channels. Each operator is a long-running async function that owns one or more receivers and one or more senders.

```rust,ignore
use anyhow::Result;
use tokio::sync::mpsc;

/// A stateless map operator. Reads observations, applies a transformation,
/// forwards the result.
///
/// The signature is parameterized by the transform function for reusability;
/// in the SDA pipeline this is used for normalization, enrichment, and
/// schema migration.
pub async fn map_operator<F>(
    mut input: mpsc::Receiver<Observation>,
    output: mpsc::Sender<Observation>,
    mut transform: F,
) -> Result<()>
where
    F: FnMut(Observation) -> Observation + Send,
{
    while let Some(obs) = input.recv().await {
        let transformed = transform(obs);
        // .send().await applies backpressure if downstream is full.
        // If the receiver is dropped, the operator terminates cleanly —
        // a downstream shutdown signal naturally propagates upstream.
        if output.send(transformed).await.is_err() {
            tracing::info!("map operator: downstream closed, shutting down");
            return Ok(());
        }
    }
    Ok(())
}

/// A stateless filter operator. Drops observations for which the predicate
/// returns false. Used in SDA for area-of-interest filtering and validation.
pub async fn filter_operator<F>(
    mut input: mpsc::Receiver<Observation>,
    output: mpsc::Sender<Observation>,
    mut predicate: F,
) -> Result<()>
where
    F: FnMut(&Observation) -> bool + Send,
{
    while let Some(obs) = input.recv().await {
        if predicate(&obs) {
            if output.send(obs).await.is_err() {
                return Ok(());
            }
        }
        // Dropped observations: no send, no backpressure stall.
        // The filter operator runs at the input rate.
    }
    Ok(())
}
```

Two design points. The operators take ownership of the receivers but accept a `Sender` (which is cloneable) — this is intentional. When a topology has fan-out (one operator feeds multiple downstreams), the operator clones its `Sender` for each downstream branch. When it has fan-in (multiple operators feed one downstream), each upstream operator owns its own clone of the same `Sender`. Second, the operators terminate cleanly when their output channel is closed. This produces a clean shutdown propagation: closing the final sink causes the last operator to terminate, which drops its receiver, which causes the operator before it to terminate, and so on back to the source. This is the streaming-system equivalent of unwinding a call stack — but it works across asynchronous task boundaries.

### A Stateful Fold Operator

The first interesting operator in the SDA pipeline is a stateful one: the deduplicator. The same orbital object can be detected by multiple radar arrays during a single pass; the deduplicator collapses these into a single observation per (object, time-window) pair before forwarding to the correlator.

```rust,ignore
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Deduplicates observations within a sliding time window keyed on object ID.
/// State is held in-process; on crash, we lose the dedup state and may
/// briefly emit duplicates as the window refills. This is acceptable for
/// the SDA pipeline; alternative state backings are discussed in Module 5.
///
/// The window is *not* an event-time window — that comes in Module 3.
/// This is a processing-time approximation suitable for ingestion-time dedup.
pub async fn dedup_operator(
    mut input: mpsc::Receiver<Observation>,
    output: mpsc::Sender<Observation>,
    window: Duration,
) -> Result<()> {
    // State: object ID -> last-seen time (in processing-time / wall clock).
    // For SDA volumes (~5e4 active objects), this HashMap stays under 5MB.
    // We periodically prune entries older than `window` to bound memory.
    let mut last_seen: HashMap<String, Instant> = HashMap::new();
    let mut last_pruned = Instant::now();

    while let Some(obs) = input.recv().await {
        // Use object track ID from the source for the dedup key. In a real
        // system this requires a per-source mapping to a global ID — that
        // is one of the things the correlator does. For now, dedup within
        // a single source's track ID space.
        let key = format!("{:?}:{}", obs.source_kind, obs.source_id.0);
        let now = Instant::now();

        let should_emit = match last_seen.get(&key) {
            Some(&t) if now.duration_since(t) < window => false,
            _ => true,
        };

        if should_emit {
            last_seen.insert(key, now);
            if output.send(obs).await.is_err() {
                return Ok(());
            }
        }

        // Periodic pruning to bound memory. Run every `window` interval.
        if now.duration_since(last_pruned) > window {
            last_seen.retain(|_, &mut t| now.duration_since(t) < window * 2);
            last_pruned = now;
        }
    }
    Ok(())
}
```

The state in this operator is a `HashMap<String, Instant>`. It survives across observations but does not survive a restart — if the process crashes, the next batch of observations will all appear novel and may be emitted twice. For the SDA pipeline this is an acceptable failure mode; the conjunction analysis tolerates a brief uptick in duplicate events after a restart, and we save the operational cost of a persistent state backend. This tradeoff — accepting transient correctness violations to avoid persistent state — is one of the most common in streaming systems and one that should always be made explicitly. We will revisit it in Module 5 when discussing exactly-once semantics. Note that the dedup window here is in *processing time* (wall-clock since we last saw this key). This is fine for an ingestion-time guard but is not the same thing as an event-time window — which we cover in Module 3, and which is necessary for any time-correctness guarantee.

### Composing the Topology

With operators defined, the topology is built by spawning each operator as a task and wiring the channels between them. The structure of the spawning code is the structure of the pipeline graph.

```rust,ignore
use tokio::task::JoinSet;

/// Spawns the M1 ingestion topology:
///
///     [radar_src] ─┐
///     [optical_src] ┼─→ [normalize] ─→ [dedup] ─→ [sink]
///     [isl_src] ────┘
///
/// This is the operator graph for the SDA Sensor Ingestion Service.
/// Module 2 extends it with a real orchestrator; Module 3 replaces dedup
/// with a windowed correlator.
pub async fn spawn_ingestion_topology(
    mut radar: UdpRadarSource,
    mut optical: OpticalArchiveSource,
    mut isl: IslBeaconSource,
    final_sink: mpsc::Sender<Observation>,
) -> JoinSet<Result<()>> {
    let mut tasks = JoinSet::new();

    // Three source-to-normalize channels (fan-in).
    // Buffer size 1024 is a starting point; Module 4 covers buffer sizing.
    let (n_tx, n_rx) = mpsc::channel::<Observation>(1024);

    // Source tasks: each pushes to the same n_tx, so we clone it per source.
    let n_tx_radar = n_tx.clone();
    tasks.spawn(async move {
        loop {
            match radar.next().await? {
                Some(obs) => {
                    if n_tx_radar.send(obs).await.is_err() { break; }
                }
                None => break,
            }
        }
        Ok(())
    });
    let n_tx_optical = n_tx.clone();
    tasks.spawn(async move {
        loop {
            match optical.next().await? {
                Some(obs) => {
                    if n_tx_optical.send(obs).await.is_err() { break; }
                }
                None => break,
            }
        }
        Ok(())
    });
    let n_tx_isl = n_tx;  // last clone goes here, drop n_tx
    tasks.spawn(async move {
        loop {
            match isl.next().await? {
                Some(obs) => {
                    if n_tx_isl.send(obs).await.is_err() { break; }
                }
                None => break,
            }
        }
        Ok(())
    });

    // Normalize -> dedup edge.
    let (d_tx, d_rx) = mpsc::channel::<Observation>(1024);
    tasks.spawn(map_operator(n_rx, d_tx, |obs| {
        // Normalize timestamps: ensure ingest_timestamp is set.
        // The actual normalization rules grow as the pipeline matures.
        obs
    }));

    // Dedup -> final sink.
    tasks.spawn(dedup_operator(d_rx, final_sink, Duration::from_millis(500)));

    tasks
}
```

Notice that the function signature is the topology specification. The arguments name the inputs and outputs; the body lays out which operators connect to which channels. A reader unfamiliar with the codebase can understand the pipeline shape from this function alone — they don't need to chase through five files to understand what flows where. This is the dataflow model paying off: the structure of the code mirrors the structure of the data. Production systems with many operators eventually outgrow inline channel wiring and adopt a topology builder DSL (Kafka Streams' `StreamsBuilder` is the canonical example), but the principle is the same: declare the graph, let the framework spawn the tasks. We will build a small topology builder in Module 2.

> **Source note:** This lesson synthesizes the dataflow model from DDIA Ch. 11 (which discusses operators and stream processing without using a single canonical name for the framework), Kafka Ch. 14 (which uses the term "topology" extensively), and FDE Ch. 3 (which uses "Dataflow Model" to specifically reference the Apache Beam paper of that name). Engineers familiar with one framework's vocabulary will recognize the others — the concepts are stable across implementations.

---

## Key Takeaways

* The pipeline is a **graph of operators**, not a sequence of function calls. Sources at the edges, sinks at the other edges, operators connected by streams in between. This representation is the artifact you architect, debug, and scale against.
* Operators come in five canonical shapes — **map, filter, flatmap, fold, window** — plus join. Stateless operators (map, filter, flatmap) parallelize trivially; stateful ones (fold, window, join) require partitioning by key.
* **State is the source of operational cost** in streaming systems. Every stateful operator forces decisions about checkpointing, recovery, and where state lives (in-process, embedded persistent, external). Choose deliberately and document the consequences.
* **Backpressure propagates along the topology** when channels are bounded and operators await on send. This is the inverse of the imperative-loop pitfall: in dataflow code, doing nothing (awaiting) is the correct behavior under load.
* The **topology specification is architecture documentation**. A function whose body wires up channels and spawns operators is the most direct expression of the pipeline's structure — clearer than any prose description. Production systems graduate to a topology DSL but the principle is unchanged.

---

{{#quiz lesson-02-quiz.toml}}
