# Lesson 2 — Data Lineage

**Module:** Data Pipelines — M06: Observability and Lineage
**Position:** Lesson 2 of 3
**Source:** *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 2 (Data Management — Data Lineage as an Undercurrent); *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (provenance and audit trails in stream processing)

---

## Context

Metrics from Lesson 1 answer aggregate questions: how many alerts per minute, what's the P99 latency, where is the saturation gradient. They do not answer per-event questions. *Why was this specific conjunction alert emitted? What observations contributed to it? Which sensor's data was the dominant signal?* These are the questions the on-call engineer asks during an incident — when an alert turned out to be a false positive, when the alert subscriber asks "where did this come from," when ops needs to confirm a partner's data quality regression.

The mission framing for this module's predecessor (M5) was the SDA-2026-0207 incident where two false-positive avoidance maneuvers were executed. The post-incident analysis required tracing each phantom alert backward through the pipeline — the alert came from a `ConjunctionRisk` event, which was emitted by the correlator from two `Observation` envelopes, which came from the radar source and the optical source, which... at each step the analysis required pulling logs from the operator that produced the output, finding the matching input by timestamp, querying the upstream operator, and so on. Two days of investigation produced the answer (a single radar with bad calibration produced one of the inputs). With *lineage* — the per-event traceability the pipeline emits as part of every output — the same diagnosis would have taken minutes.

This lesson develops lineage as a first-class pipeline output. The pattern: every event the pipeline emits carries a `lineage` field listing the events that contributed to it. Map and filter operators copy the lineage forward unchanged. Aggregations and joins (the windowed correlator from M3 is the canonical case) merge their inputs' lineages. The lineage is queryable both backward (given a bad output, find the contributing inputs) and forward (given a bad input, find the affected outputs). The cost is per-event memory and serialization overhead; the benefit is the diagnostic capability that turns post-incident analysis from days into minutes.

---

## Core Concepts

### Event-Level Lineage

The simplest form of lineage is per-event: every output carries a list of *parent event IDs* that contributed to it. For a stateless map operator (the radar source's normalize step), the lineage is just the input's ID — one parent. For an aggregating operator (the windowed correlator that produces a `ConjunctionRisk` from two observations), the lineage is two parents. For a join over many inputs (a future operator that fuses N sensors' observations into a track), the lineage grows.

The data structure is small but load-bearing.

```rust,ignore
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageStep {
    /// The operator that produced this step.
    pub operator: String,
    /// When the step happened (the operator's emit time).
    pub timestamp: SystemTime,
    /// The IDs of the events that contributed.
    pub parent_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageTrace {
    pub steps: Vec<LineageStep>,
}
```

A `LineageTrace` carries the entire history of an event's path through the pipeline. The first step lists the source's parent IDs (typically just the source's own observation_id); each subsequent step adds the operator that processed it. The trace can be walked backward from any output to find every contributing event at every stage.

### The Lineage Tag on the Envelope

Lineage extends the M1 `Observation` envelope as an optional field. The `Option` makes the change backwards-compatible — old envelopes without lineage continue to deserialize correctly; new envelopes carry the trace.

```rust,ignore
pub struct Observation {
    // ... existing fields from M1 ...
    pub observation_id: Uuid,
    pub source_id: SourceId,
    // ...
    /// Optional lineage trace. None means "lineage is not being tracked
    /// for this event," typically because of sampling (see below).
    pub lineage: Option<LineageTrace>,
}
```

The same shape applies to derived events like `ConjunctionRisk`: the envelope grows by one optional `LineageTrace` field. Every operator's emit logic appends a `LineageStep` to the trace if it exists, leaves it as `None` if it does not. The pattern is identical at every operator; the implementation is small and easy to instrument uniformly.

### Storage Cost

Lineage is not free. Every event carries the trace; the trace grows with each step the event passes through; for joins and aggregations the trace's parent_ids list grows with the fan-in. For a 10-stage pipeline with average fan-in of 2, lineage doubles ten times — 2^10 = 1024 IDs per output event, plus the per-step metadata. At SDA's volumes (10K events/sec across the pipeline, 100 bytes per LineageStep, 16 bytes per UUID), that is ~17 GB/sec of lineage data — clearly impractical to carry on every event.

Three strategies for managing the cost.

**Sampling.** Only some events carry lineage; the rest carry `lineage: None`. Sample rate is configurable per pipeline; the SDA default is 1% — every 100th event has its lineage tracked. Investigations rely on the sampled events being representative; with 1% sampling and SDA's volumes, the on-call engineer has hundreds of lineage-bearing events per minute to investigate, which is plenty for diagnostic purposes.

**Truncation.** Lineage is bounded at top-K parents per fan-in operator. The correlator's emit lists only the most-contributing 4 parents (by uncertainty weight or similar) rather than every observation in the window. Truncation loses some diagnostic detail but keeps the trace size bounded regardless of fan-in.

**Externalization.** Lineage is written to a separate store (a Kafka topic, a graph database, an object-storage bucket); the envelope carries only a `lineage_id` reference. Investigation queries the external store. Externalization decouples lineage size from envelope size at the cost of an additional system to operate.

For SDA's pipeline, sampling at 1% is the default; truncation is configured per operator; externalization is the path forward when the pipeline scales 10x or when lineage volume becomes a bottleneck. The lesson develops sampling and truncation; externalization is a forward reference for production scale.

### OpenLineage

Production lineage tooling has converged on the OpenLineage standard — a JSON-Schema-defined event format that describes datasets, jobs, and their relationships. Tools like Marquez, Datakin, and DataHub consume OpenLineage events from various pipelines (Airflow, Spark, dbt, custom) and provide a unified lineage browser.

The SDA pipeline can emit OpenLineage events alongside its in-envelope lineage; the in-envelope version is for per-event traceability (the diagnostic case the lesson focuses on), and the OpenLineage version is for cross-system visibility (the dataset-level case where lineage is part of the broader data engineering graph). The two are complementary; this lesson focuses on the in-envelope shape because it is the operationally most useful for the SDA team, but the OpenLineage emission is documented as an operational extension.

### Lineage as a Debugging Tool

The on-call engineer's investigation pattern is the same in every incident.

**Backward walk** — given a wrong output, trace its lineage backward to find the contributing inputs. The phantom alert from M5's incident: lineage shows the two observations that fed the correlator, the radar's observation_id and the optical's observation_id. The radar's lineage shows the upstream UDP frame. The optical's lineage shows the HTTP poll response. The investigation lands at the source within minutes; the next question (which radar? which time?) is answerable from the lineage's metadata.

**Forward walk** — given a known-bad input, trace forward to find every output that incorporates it. A radar that turned out to have a calibration bug between 14:00-15:00 yesterday: lineage queries find every `ConjunctionRisk` event whose parent_ids include any observation from that radar in that time window. The investigation produces the list of affected alerts; the alert subscriber's owner is notified to invalidate them.

Both directions are valuable. The backward walk is the canonical post-incident analysis; the forward walk is the canonical impact assessment. Both require the lineage to be queryable as a graph rather than a stream, which the externalization or in-pipeline indexing supports. The capstone wires this into a small `sda-lineage` CLI tool that performs both walks against the sampled lineage data.

---

## Code Examples

### Adding Lineage to the Envelope

The minimal envelope change. The `Option<LineageTrace>` is backwards-compatible with existing event handling; the lineage-aware operators populate it, the lineage-unaware operators ignore it.

```rust,ignore
use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageStep {
    pub operator: String,
    pub timestamp_unix_ms: u64,
    pub parent_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LineageTrace {
    pub steps: Vec<LineageStep>,
}

impl LineageTrace {
    /// Append a step for an operator that processed `parents` and
    /// emitted a new event derived from them.
    pub fn append(&mut self, operator: impl Into<String>, parents: Vec<Uuid>) {
        self.steps.push(LineageStep {
            operator: operator.into(),
            timestamp_unix_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            parent_ids: parents,
        });
    }

    /// All distinct ancestor IDs across the whole trace. Used by the
    /// forward-walk query: 'is this event_id an ancestor of this output?'
    pub fn all_ancestors(&self) -> std::collections::HashSet<Uuid> {
        let mut set = std::collections::HashSet::new();
        for step in &self.steps {
            for id in &step.parent_ids {
                set.insert(*id);
            }
        }
        set
    }
}
```

The `append` method is the per-operator instrumentation point. Each operator's emit logic — after producing a derived event from its inputs — calls `lineage.append("operator-name", parents)` to record the step. The `all_ancestors` method enables the forward-walk query: given an output, does its lineage include any of the IDs in a known-bad set? If yes, the output is affected. The `Default` impl makes constructing an empty trace easy for source operators.

### A Sampling Lineage Policy

The sampling decision is a hash on the observation_id, which makes it deterministic and reproducible. Across replicas, the same observation_id always samples to the same decision — useful for cross-replica comparisons during debugging.

```rust,ignore
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

pub struct LineageSampler {
    sample_rate: f64,  // 0.0..=1.0; 0.01 = 1% sampling
}

impl LineageSampler {
    pub fn new(sample_rate: f64) -> Self {
        Self { sample_rate: sample_rate.clamp(0.0, 1.0) }
    }

    /// Should this observation carry lineage? Hash-based deterministic
    /// decision — same observation_id always produces the same answer.
    pub fn should_sample(&self, observation_id: &Uuid) -> bool {
        let mut h = DefaultHasher::new();
        observation_id.hash(&mut h);
        let hashed = h.finish();
        // Map the 64-bit hash to a value in [0, 1.0).
        let bucket = (hashed % 10_000) as f64 / 10_000.0;
        bucket < self.sample_rate
    }
}
```

The deterministic-hash approach beats random sampling in two operationally important ways. First, the same input produces the same lineage decision regardless of which replica processed it; debugging a specific event_id always gets the same lineage answer, not a flip-of-a-coin. Second, the sampling distribution is exactly the configured rate — no clustering effects from poor RNG. The cost is microseconds per event for the hash; well below noise in any pipeline operator's hot path.

### A Lineage Query Tool

The CLI tool that performs backward and forward walks against a corpus of sampled lineage data. Production deployments would query a graph database or the externalized lineage store; for SDA's scale and the lesson's scope, an in-memory query against a JSON-Lines file of sampled events is sufficient.

```rust,ignore
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use uuid::Uuid;

pub struct LineageStore {
    /// Map from event_id to its lineage trace.
    traces: HashMap<Uuid, LineageTrace>,
    /// Reverse index: parent_id → set of event_ids that have it as an ancestor.
    forward_index: HashMap<Uuid, Vec<Uuid>>,
}

impl LineageStore {
    /// Load sampled events from a JSON-Lines file (one event per line,
    /// each line a (event_id, lineage) tuple).
    pub async fn load(path: impl AsRef<Path>) -> Result<Self> {
        let mut traces = HashMap::new();
        let mut forward_index: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
        let content = tokio::fs::read_to_string(path).await?;
        for line in content.lines() {
            let (event_id, trace): (Uuid, LineageTrace) = serde_json::from_str(line)?;
            for ancestor in trace.all_ancestors() {
                forward_index.entry(ancestor).or_default().push(event_id);
            }
            traces.insert(event_id, trace);
        }
        Ok(Self { traces, forward_index })
    }

    /// Backward walk: given an event_id, return its full lineage.
    pub fn backward(&self, event_id: Uuid) -> Option<&LineageTrace> {
        self.traces.get(&event_id)
    }

    /// Forward walk: given an event_id (typically a known-bad input),
    /// return all event_ids whose lineage includes it as an ancestor.
    pub fn forward(&self, ancestor_id: Uuid) -> Vec<Uuid> {
        self.forward_index.get(&ancestor_id).cloned().unwrap_or_default()
    }
}
```

The forward index is the operationally critical data structure for the impact-assessment use case. Querying "every event affected by ancestor X" is O(1) lookup if the index is precomputed at load time; without the index, the query is O(N) over every event in the corpus, which makes incident response slower than it needs to be. The cost is memory (the index roughly doubles the lineage corpus's footprint) but typically negligible compared to the diagnostic value. Production code uses a graph database (Neo4j, JanusGraph) or a specialized lineage store (Marquez); the in-memory version above is the SDA pipeline's starting point.

---

## Key Takeaways

* **Event-level lineage** carries each output's parent IDs back through the pipeline. Map operators copy the trace forward; aggregating operators merge their inputs' traces; the trace can be walked backward from any output to find every contributing event at every stage.
* The **storage cost** of lineage is real: 2^N where N is the pipeline depth times average fan-in. For SDA's volumes, 1% **sampling** is the default; **truncation** at top-K parents bounds per-event cost; **externalization** to a graph database is the path to production scale.
* **Deterministic hash sampling** beats random sampling: same input always produces the same lineage decision, so debugging a specific event_id is reproducible across replicas. Cost is microseconds per event for the hash; well below noise.
* The **two query directions** are operationally distinct. Backward walk: given a bad output, find the contributing inputs (post-incident analysis). Forward walk: given a known-bad input, find the affected outputs (impact assessment). Both require lineage to be queryable as a graph; the **forward index** makes the second query O(1).
* **OpenLineage** is the production cross-system standard; the in-envelope lineage from this lesson is the per-event traceability that powers diagnostic queries. The two are complementary — emit OpenLineage events for cross-pipeline visibility, carry in-envelope lineage for per-event investigation.

---

{{#quiz lesson-02-quiz.toml}}
