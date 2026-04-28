# Lesson 2 — DAG Scheduling

**Module:** Data Pipelines — M02: Pipeline Orchestration Internals
**Position:** Lesson 2 of 4
**Source:** *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 11 (the operator-graph execution model in stream processing); *Async Rust* — Maxwell Flitton & Caroline Morton, sections on `tokio::task::JoinSet` and `tokio::select!`; *Fundamentals of Data Engineering* — Joe Reis & Matt Housley, Chapter 2 (Orchestration as the connective tissue of the lifecycle)

> **Source note:** The DDIA chapter discussion of operator-graph execution is referenced from training knowledge of the printed text; the core model — vertices as operators, edges as channels, scheduling via topological order — is well-established and unchanged across editions. Specific algorithmic choices (Kahn's algorithm vs DFS-postorder) are decisions made for this curriculum, not direct citations.

---

## Context

Lesson 1 established the task as the unit of orchestration and the `Task` wrapper as the orchestrator's handle on each one. We now need a way to describe an entire pipeline's worth of operators — radar source, ISL listener, optical poller, three normalizers, a windowed dedup, a correlator, a conjunction emitter, an audit sink — and turn that description into running tasks with their channels correctly wired. Module 1's `spawn_ingestion_topology` did this by hard-coding the topology in a long function. That technique is fine for three operators and fragile for ten. The orchestrator replaces it with a *declarative graph*: the application says what the pipeline looks like, and the orchestrator figures out what to spawn and in what order.

The data structure that captures "what the pipeline looks like" is a directed acyclic graph. Vertices are operators; directed edges are typed channels carrying observations (or, later, watermarks and barriers) from one operator to the next. The acyclic constraint is operationally critical: a pipeline with a cycle is a pipeline that can deadlock under backpressure, and a pipeline that can deadlock under backpressure is one we want to refuse to start rather than start and hope. We will spend most of this lesson building the graph type and its scheduler. The remaining time is on the operational properties the graph buys us: clean shutdown via `JoinSet`, end-to-end backpressure preservation, and detection of unreachable or orphaned operators at build time rather than at 3 AM.

The forward references matter. Module 3 adds watermark propagation to the graph's edges; the graph type designed here must accommodate that addition without a rewrite. Module 4 deepens the backpressure analysis; the bounded-channel-per-edge invariant established here is what makes it tractable. Module 5 adds checkpoint barriers as graph-level events; same invariant applies. The shape of `OperatorGraph` is load-bearing for the rest of the track.

---

## Core Concepts

### The Pipeline as a DAG

A pipeline's logical structure is a graph. Vertices are *operators* — the named pieces of work the pipeline performs (a source, a normalizer, a windowed dedup, a sink). Edges are *channels* — the typed conduits along which data flows from one operator to the next. The graph is *directed* because data flows in one direction along each edge. The graph is *acyclic* because allowing cycles introduces deadlock conditions that are tedious to reason about and unnecessary for the SDA pipeline's needs.

The vertices carry the operator's identity and the means to spawn it: a name, the async function (or closure) that runs the operator, the channel ends it expects to receive, and its restart policy from Lesson 1. The edges carry the channel itself plus its capacity. Edges between specific operators are typed — a channel from radar to normalize carries `Observation`, a channel from windowed-dedup to correlator might carry `DedupedObservation` — but the graph as a whole is heterogeneously typed and represented internally with type erasure (the operator function is boxed; the channels are stored in a typed map keyed on edge ID). This is a deliberate tradeoff that we will spell out in the code: the static-typing alternative produces a graph type whose generic parameters explode with the number of edges, and the orchestrator becomes harder to use than the manual spawn it replaces.

Forbidding cycles is a real constraint, not just a convention. A cyclic pipeline has at least one operator whose downstream depends on its own upstream; under backpressure, the upstream blocks waiting for the downstream to consume, and the downstream blocks waiting for the upstream to produce. Without a tie-breaking mechanism (an explicit budget on the cycle, an unbounded buffer, a priority order on which side blocks first), the cycle deadlocks. SDA's pipelines do not need cycles — every legitimate use case (a "rerun the failed observations" loop, a "feed audit results back to the source for sampling weight adjustment" path) can be expressed as a separate downstream operator that re-emits into a new pipeline. The orchestrator refuses to build a graph that contains a cycle, and produces an error message naming the cycle.

### Topological Sort and Channel Wiring

Spawning the operators in arbitrary order does not work. The downstream operator's task must be created with the receiver end of the channel that connects it to the upstream operator. That receiver does not exist until the channel itself is created, which (in the natural construction order) happens when the upstream is being prepared. The natural flow is therefore: walk the graph in topological order (every operator visited before any of its descendants), create each operator's outgoing channels first, hand the receiver halves to the descendants when their turn comes.

Two algorithms produce a topological order: **Kahn's algorithm** (repeatedly pick a vertex with no remaining incoming edges and remove it) and **DFS post-order** (depth-first traversal, emit each vertex after all its descendants). Both are O(V + E). Kahn's is operationally preferred for pipelines because the order it produces is *level-by-level* — sources first, then their immediate consumers, then theirs, and so on — which corresponds to how an operator engineer thinks about the pipeline. DFS post-order can produce a less intuitive order in which a deep chain is finished before a shallow sibling is started; the spawned tasks are then more interleaved than the engineer expected.

The wiring problem has a subtle two-pass structure that the code below makes explicit: the first pass over the graph creates every channel (sender + receiver pair) and stores each pair in a map keyed on the edge ID; the second pass spawns each operator with the receiver end of its incoming edge and the sender end of its outgoing edge. This separation is what makes downstream-first iteration impossible: we do not know who the receiver belongs to until the second pass identifies the consumer of that edge. Single-pass approaches that try to allocate channels lazily as operators are walked tend to produce subtle bugs — operators spawned with placeholder channels that have to be patched, or scheduling orders that look topological but skip an edge.

### `JoinSet` for Lifecycle Management

A pipeline with N operators produces N `JoinHandle`s. The orchestrator's supervisor (Lesson 4) needs to react when any one of them completes — usually because of a panic or an error, sometimes because of a clean shutdown signal. Iterating over a `Vec<JoinHandle>` and calling `.await` on each is the wrong pattern: it serializes on the slowest operator's completion and offers no way to react to whichever finishes first. The right primitive is `tokio::task::JoinSet`.

A `JoinSet<Result<()>>` is a bag of in-flight tasks with a `join_next() -> Future<Output = Option<Result<Result<()>, JoinError>>>` method. The future resolves whenever any task in the set finishes, returning that task's result. The supervisor awaits `join_next` in a loop, dispatching on each result as it arrives. When the supervisor decides the pipeline should shut down, it calls `JoinSet::abort_all()`, which sends an abort signal to every task and lets the supervisor drain the resulting `JoinError`s through the same `join_next` loop. The structural property: every operator's lifecycle ends through the supervisor's mouth, never through a detached return.

`JoinSet` is also the right place for the heterogeneous-return-type discipline from Lesson 1. Every operator returns `Result<()>`; that uniformity is what lets `JoinSet` be a single typed structure for the entire pipeline. The standardization paid for itself the moment we needed a single supervisor.

### Backpressure Through the DAG

Module 1 established `mpsc::Sender::send().await` as the foundation of backpressure: a full channel suspends the upstream until the downstream catches up. The DAG inherits that property as long as every edge in the DAG is a *bounded* channel. The orchestrator enforces this with a graph-level invariant: there are no `unbounded_channel` calls anywhere in the graph builder. Edges are sized at construction time; sizes are documented per-edge with a comment about expected burst behavior; Module 4 develops the sizing discipline.

The DAG-level corollary is that backpressure traverses the entire graph. A slow sink causes its upstream's channel to fill, suspends that upstream, which causes *its* upstream's channel to fill, and so on back to the sources. The sources themselves either suspend on their own producing primitive (the UDP socket recv, the HTTP poll) or, for sources that cannot suspend (a radar feed that produces whether we are listening or not), drop at the kernel level. The pipeline never grows unbounded internal buffers under load. This is the property the audit script in Lesson 3 verifies and the property the Module 4 burst test exercises.

The DAG is also the natural place to check cycles. We forbid them because a cyclic pipeline cannot have this backpressure-traversal property — a cycle has no "back" to propagate to. If a future requirement legitimately needs cyclic data flow (a feedback path for late-arriving observations, a "retry the failed conjunction analysis" loop), it must be implemented as a feed-through to a new pipeline, not as a back-edge in the existing one. The constraint is a feature.

### Unreachable Tasks and Orphaned Channels

The first non-trivial bug a pipeline-graph type catches is the disconnection bug. An engineer adds a new operator, registers it with the graph, but forgets to call `connect(upstream, new_operator)` or `connect(new_operator, downstream)`. The graph builds. The operator spawns. It either reads from an empty channel forever (its upstream was never wired) or writes into a channel nobody reads (its downstream was never wired) — and the former scenario has no symptoms until somebody notices the operator is silent, while the latter scenario fills its channel and applies false backpressure to its upstream. Both bugs cost real time when they happen in production.

The graph builder catches both cases at `build()` time. An operator that has no incoming edges is either a registered source (legitimate) or a misconfigured operator (illegal). An operator that has no outgoing edges is either a registered sink (legitimate) or a misconfigured operator (illegal). The builder's role enumeration distinguishes these cases; an operator declared as `add_operator` (interior) is required to have both an incoming and an outgoing edge, while `add_source` and `add_sink` adjust the constraint accordingly. The error message names the offending operator and the missing edge direction. This kind of build-time validation is what gives the declarative orchestrator its primary advantage over hand-spawned topology: bugs that would require runtime instrumentation to detect become compile-time-style errors at startup.

---

## Code Examples

### The `OperatorGraph` Builder

The graph is constructed by a builder that collects vertices and edges and validates them at `build()` time. Operators are stored as type-erased boxed closures so the graph itself is not generic over each operator's type signature.

```rust,ignore
use anyhow::{anyhow, bail, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use tokio::sync::mpsc;

/// What an operator does once spawned. Erased to a boxed future so the
/// graph stores a heterogeneous collection.
pub type OperatorFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

/// An operator's role in the topology. The builder uses this to validate
/// that operators have the right edges connected.
#[derive(Debug, Clone, Copy)]
pub enum Role { Source, Operator, Sink }

struct VertexSpec {
    name: String,
    role: Role,
    /// Constructed once the channels are wired in build(). Closure takes
    /// the operator's incoming and outgoing channel-end handles.
    factory: Box<dyn FnOnce(WiredEnds) -> OperatorFuture + Send>,
    incoming: Option<EdgeId>,
    outgoing: Option<EdgeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeId(u32);

/// The handles passed into the operator's factory closure when build()
/// wires the topology.
pub struct WiredEnds {
    pub rx: Option<mpsc::Receiver<Observation>>,
    pub tx: Option<mpsc::Sender<Observation>>,
}

pub struct OperatorGraph {
    vertices: Vec<VertexSpec>,
    edges: Vec<EdgeSpec>,
}

struct EdgeSpec {
    id: EdgeId,
    from_idx: usize,
    to_idx: usize,
    capacity: usize,
}

impl OperatorGraph {
    pub fn new() -> Self {
        Self { vertices: Vec::new(), edges: Vec::new() }
    }

    pub fn add_source(&mut self, name: impl Into<String>,
                      factory: impl FnOnce(WiredEnds) -> OperatorFuture + Send + 'static)
                      -> usize
    {
        self.push_vertex(name.into(), Role::Source, factory)
    }

    pub fn add_operator(&mut self, name: impl Into<String>,
                        factory: impl FnOnce(WiredEnds) -> OperatorFuture + Send + 'static)
                        -> usize
    {
        self.push_vertex(name.into(), Role::Operator, factory)
    }

    pub fn add_sink(&mut self, name: impl Into<String>,
                    factory: impl FnOnce(WiredEnds) -> OperatorFuture + Send + 'static)
                    -> usize
    {
        self.push_vertex(name.into(), Role::Sink, factory)
    }

    fn push_vertex(&mut self, name: String, role: Role,
                   factory: impl FnOnce(WiredEnds) -> OperatorFuture + Send + 'static)
                   -> usize
    {
        let idx = self.vertices.len();
        self.vertices.push(VertexSpec {
            name,
            role,
            factory: Box::new(factory),
            incoming: None,
            outgoing: None,
        });
        idx
    }

    pub fn connect(&mut self, from: usize, to: usize, capacity: usize) -> Result<EdgeId> {
        if self.vertices[from].outgoing.is_some() {
            bail!("operator {} already has an outgoing edge", self.vertices[from].name);
        }
        if self.vertices[to].incoming.is_some() {
            bail!("operator {} already has an incoming edge", self.vertices[to].name);
        }
        let id = EdgeId(self.edges.len() as u32);
        self.edges.push(EdgeSpec { id, from_idx: from, to_idx: to, capacity });
        self.vertices[from].outgoing = Some(id);
        self.vertices[to].incoming = Some(id);
        Ok(id)
    }
}
```

The shape is more verbose than a typical graph type because it carries the operator-spawn closure inline. The closure captures whatever the operator needs (config, source addresses, etc.) and is only invoked once `build()` has wired the channels. This delays the construction of channel-touching state until we actually have the channels, which is exactly the two-pass structure the lesson described. The single-incoming and single-outgoing restriction (one edge per direction per operator) is a simplification that the SDA pipeline does not need to violate; fan-in and fan-out are handled by intermediate router operators rather than by multi-edge vertices. This keeps the topo-sort and the wiring logic simple at modest cost in expressiveness, and the cost is recoverable when needed by introducing explicit fan-in/fan-out operators with their own typed semantics.

### Topo-Sort, Cycle Detection, and Build

The `build()` step is where validation happens. It runs Kahn's algorithm to produce a topological order and to detect cycles, validates per-role edge requirements, allocates the channels, and spawns each operator with its wired channel ends.

```rust,ignore
use std::collections::{HashMap, VecDeque};

pub struct BuiltGraph {
    /// Spawned tasks, in topological order. The supervisor takes ownership.
    pub tasks: Vec<Task>,
}

impl OperatorGraph {
    pub fn build(self) -> Result<BuiltGraph> {
        // Pass 1: validate role constraints.
        for v in &self.vertices {
            match v.role {
                Role::Source if v.incoming.is_some() =>
                    bail!("source {} has an incoming edge; sources have no upstream", v.name),
                Role::Sink if v.outgoing.is_some() =>
                    bail!("sink {} has an outgoing edge; sinks have no downstream", v.name),
                Role::Operator if v.incoming.is_none() =>
                    bail!("operator {} has no incoming edge; did you forget to connect()?", v.name),
                Role::Operator if v.outgoing.is_none() =>
                    bail!("operator {} has no outgoing edge; did you forget to connect()?", v.name),
                Role::Source if v.outgoing.is_none() =>
                    bail!("source {} has no outgoing edge; did you forget to connect()?", v.name),
                Role::Sink if v.incoming.is_none() =>
                    bail!("sink {} has no incoming edge; did you forget to connect()?", v.name),
                _ => {}
            }
        }

        // Pass 2: Kahn's algorithm for topo sort + cycle detection.
        let n = self.vertices.len();
        let mut in_degree = vec![0usize; n];
        for e in &self.edges { in_degree[e.to_idx] += 1; }

        let mut ready: VecDeque<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
        let mut order: Vec<usize> = Vec::with_capacity(n);

        while let Some(idx) = ready.pop_front() {
            order.push(idx);
            for e in &self.edges {
                if e.from_idx == idx {
                    in_degree[e.to_idx] -= 1;
                    if in_degree[e.to_idx] == 0 { ready.push_back(e.to_idx); }
                }
            }
        }

        if order.len() != n {
            // The remaining vertices form one or more cycles.
            let cycle_members: Vec<&str> = (0..n)
                .filter(|i| !order.contains(i))
                .map(|i| self.vertices[i].name.as_str())
                .collect();
            bail!("pipeline graph has a cycle involving operators: {:?}", cycle_members);
        }

        // Pass 3: allocate channels (sender + receiver pair per edge).
        let mut chan_tx: HashMap<EdgeId, mpsc::Sender<Observation>> = HashMap::new();
        let mut chan_rx: HashMap<EdgeId, mpsc::Receiver<Observation>> = HashMap::new();
        for e in &self.edges {
            let (tx, rx) = mpsc::channel(e.capacity);
            chan_tx.insert(e.id, tx);
            chan_rx.insert(e.id, rx);
        }

        // Pass 4: walk in topo order, spawning each operator with its wired ends.
        let mut tasks: Vec<Task> = Vec::with_capacity(n);
        for idx in order {
            let v = &self.vertices[idx];
            let rx = v.incoming.and_then(|e| chan_rx.remove(&e));
            let tx = v.outgoing.and_then(|e| chan_tx.remove(&e));
            // The factory closure consumes itself — pull it out via a take()
            // pattern. (Real code uses a helper to deduplicate this.)
            // ... factory invocation and Task::spawn elided for brevity.
            tasks.push(spawn_via_factory(&v.name, v.role, v.factory_take(), WiredEnds { rx, tx }));
        }

        Ok(BuiltGraph { tasks })
    }
}
```

Three things to notice. The validation in pass 1 rejects misconfigured graphs before any channels are allocated, which gives the engineer the cleanest possible error message — naming the operator that is missing its edge — rather than a downstream "channel is empty forever" symptom at runtime. The Kahn's-algorithm pass in pass 2 doubles as both topological sort and cycle detection: if any vertex has nonzero in-degree at the end, it is part of a cycle, and the unreached vertices name the cycle's members in the error message. The channel allocation in pass 3 is a single pass that creates every channel before any operator spawns; pass 4 then walks the topological order and hands each operator its channel ends, removing them from the maps as it goes. The `remove` rather than `get` is intentional: each receiver is owned by exactly one operator, and the map's emptiness at the end is itself a sanity check.

### Cycle Detection in Practice

What the cycle error looks like to the engineer who wrote the offending pipeline. The example below intentionally builds a cycle (`dedup → normalize → dedup`) to illustrate the diagnostic.

```rust,ignore
fn build_buggy_pipeline() -> Result<BuiltGraph> {
    let mut g = OperatorGraph::new();
    let radar    = g.add_source("radar",    radar_factory());
    let normalize = g.add_operator("normalize", normalize_factory());
    let dedup    = g.add_operator("dedup",    dedup_factory());
    let sink     = g.add_sink("audit",       audit_factory());

    g.connect(radar,     normalize, 1024)?;
    g.connect(normalize, dedup,     1024)?;
    g.connect(dedup,     normalize, 1024)?; // accidental back-edge
    g.connect(dedup,     sink,         64)?;

    g.build() // returns Err with the cycle named
}

// The error returned by g.build():
// Error: pipeline graph has a cycle involving operators: ["normalize", "dedup"]
```

The diagnostic is not perfect — Kahn's algorithm reports the set of vertices in cycles rather than the specific edges that form them — but it is enough to direct the engineer to the right neighborhood. Production graph types augment this with a second pass that finds the strongly connected components and reports each cycle's edge sequence; the SDA orchestrator's diagnostic is sufficient for the topology sizes we expect (tens of operators per pipeline) and we leave the SCC enhancement for when it is needed. The point of the example is that a misconfigured pipeline fails at build time with an actionable message, not at runtime with a deadlocked operator.

---

## Key Takeaways

* The pipeline is a **directed acyclic graph** of operators connected by typed bounded channels. Vertices are operators with a name, role (source / operator / sink), and spawn factory; edges are channels with a documented capacity. The acyclic constraint is a deadlock-prevention requirement, not a stylistic choice.
* `OperatorGraph::build()` runs **four passes**: per-role edge validation, Kahn's topological sort with cycle detection, channel allocation in a single pass, then operator spawning in topological order. Each pass produces an actionable error message at the earliest possible point.
* `tokio::task::JoinSet<Result<()>>` is the **right primitive for owning N operator handles**. It supports `join_next` for whichever-finishes-first reaction and `abort_all` for clean shutdown. Standardize every operator on `Result<()>` and the `JoinSet` is homogeneously typed.
* **Bounded channels everywhere** preserves the end-to-end backpressure property the rest of the track depends on. The graph builder makes "no `unbounded_channel`" a structural invariant, not a code-review rule.
* Build-time validation catches **disconnection bugs** (unwired operators) and **cycle bugs** (back-edges). Both produce readable startup-time errors rather than runtime symptoms that require instrumentation to diagnose.

---

{{#quiz lesson-02-quiz.toml}}
