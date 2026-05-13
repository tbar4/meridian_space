# Lesson 2: Multi-Leader and Leaderless Replication

## Context

The catalog's single-leader replication works when there is a clear "primary" datacenter that all writes flow through. But the Constellation Catalog needs to accept writes at twelve ground stations spanning every continent, often with hundreds of milliseconds of inter-region latency, and sometimes with hours of disconnection. Forcing every Antarctic ground station's update to round-trip through a primary in Virginia is not a viable design: latency is unacceptable for routine writes, and a transatlantic partition would silence Antarctica entirely.

This is the regime where **multi-leader** and **leaderless** replication become real options. Multi-leader replication lets multiple regions accept writes locally and reconcile asynchronously across regions — the model that Couchbase, CockroachDB's regional tables, and historically MySQL multi-master use. Leaderless replication abolishes the leader concept entirely: clients send writes to multiple replicas in parallel, and reads gather responses from multiple replicas and reconcile. This is the Dynamo model, used by Cassandra, ScyllaDB, and Riak.

Both models gain you availability during partitions and lower write latency in exchange for one significant cost: writes can conflict, and the system has to detect and resolve those conflicts. This lesson covers when each model is the right choice, the conflict detection mechanisms (which build on the vector clocks from Module 1), and the menu of resolution strategies. By the end, you should be able to evaluate whether a given workload belongs in a single-leader system, a multi-leader system, or a leaderless system — and to articulate the cost of each choice precisely.

## Core Concepts

### Multi-Leader Replication: Local Writes, Asynchronous Cross-Region Sync

In a multi-leader configuration, each region (or datacenter, or in some cases each user device) has its own leader. Writes are accepted locally and propagate asynchronously to leaders in other regions. From a single region's perspective, the model looks exactly like single-leader. The complication is the cross-region path.

The standard use cases for multi-leader, per DDIA:

**Multi-datacenter operation.** A multi-region deployment with a leader per region. Writes are local-latency for every region. Cross-region replication is asynchronous and tolerates inter-region partitions. This is what the catalog needs.

**Clients with offline operation.** A calendar app where each device is itself a leader. Writes happen locally without connectivity; they sync when the device reconnects. CouchDB, Pouch, and many mobile-first systems use this shape.

**Collaborative editing.** Google Docs and similar tools are effectively multi-leader: each user's local edit is accepted immediately and propagated to other users. The conflict resolution is the operational transform or CRDT layer.

The benefit is local-write latency and partition tolerance. The cost is **write conflicts**: two leaders can accept conflicting writes for the same key, and the system has no a-priori way to decide which is correct.

### Handling Conflicts: Avoid, Detect, Resolve

Conflict-handling strategies fall on a spectrum from "structurally impossible" to "manually resolved." DDIA's framing maps well:

**Conflict avoidance** — Structure the data so that conflicts cannot occur. The catalog could partition orbital objects by hemisphere: each region owns writes for its hemisphere's objects, and any write for an object outside the local hemisphere is forwarded. This is effectively reverting to single-leader for each piece of data, just with the leader selected by data partition rather than by topology. It is the simplest conflict story (no conflicts to handle) but it requires the data to have a partition that maps cleanly to writers.

**Last-write-wins (LWW).** Each write carries a timestamp; on conflict, the larger timestamp wins. This is the most common default and the most subtly wrong. Cassandra's default behavior is LWW with wall-clock timestamps; the failure mode (the MSS-17 incident from Module 1) is that clock skew silently picks the wrong winner. LWW is fine when concurrent writes to the same key are extremely rare and the system can tolerate dropping the loser — but treat LWW with wall-clock timestamps as **silently lossy** until proven otherwise.

**Manual resolution.** On conflict, surface both versions to the application; the application (or a human) decides. Git's merge conflicts are the canonical example: when two branches modify the same file, the user resolves. The cost is operator burden; the benefit is that no automatic policy can quietly make the wrong choice.

**Automatic semantic merge.** For specific data types — sets, counters, last-write maps with causal context — convergent replicated data types (CRDTs) provide automatic merges that are guaranteed to produce the same result regardless of merge order. The catalog's "set of authorized ground stations" could be a grow-only set CRDT; concurrent additions converge automatically. Operational transforms (used in collaborative editors) are the equivalent for ordered sequences. CRDTs are powerful where they apply but require modeling the data as a specific algebraic structure, which is not always possible.

The catalog uses conflict avoidance for orbital object writes (each region owns its assigned objects) and CRDT-based merge for the global registry of active satellites (a grow-only set). The conflict resolution policy is per-table, not per-database.

### Leaderless Replication: The Dynamo Model

Leaderless replication, introduced by Amazon's Dynamo paper and popularized by Cassandra, Riak, and Voldemort, takes a different approach. There is no leader. Clients write to N replicas in parallel and wait for W of them to acknowledge; reads query R replicas and reconcile.

If W + R > N, the system is **strongly consistent in the quorum sense**: any read overlap with the latest write by at least one replica, so the reader can detect and prefer the newest value. This is the formula Dynamo-style systems use. Common configurations are N=3, W=2, R=2; or N=5, W=3, R=3.

Three mechanisms make this work in practice:

**Read repair.** When a client reads from R replicas and detects that some have stale values, it writes the newest value back to the lagging replicas as part of the read. This is opportunistic anti-entropy on the read path.

**Anti-entropy / Merkle trees.** Background processes compare replicas and reconcile divergences. Riak and Cassandra use Merkle trees to identify diverged ranges efficiently. This is the catch-all that ensures eventual convergence even for keys that are not being read frequently.

**Hinted handoff.** When a target replica is unreachable, the writer leaves a "hint" with another node, which forwards the write when the target comes back. This is what allows the system to accept writes during partial node outages without sacrificing the durability target — at the cost of some staleness during the outage.

The Dynamo model gives you **sloppy quorums**: when nodes fail, the system can broaden the replica set to accept writes from any reachable node, then reconcile when the original replicas return. This trades strong durability guarantees during partitions (writes may land on nodes that are not the "right" replicas) for availability. The catalog's TLE registry uses sloppy quorums with N=5, W=3, R=3, and tolerates the resulting eventual consistency.

### Detecting Concurrent Writes Without a Leader

In single-leader replication, the leader imposes a total order on writes; conflicts cannot occur because there is exactly one writer. In multi-leader and leaderless systems, two writes to the same key from different writers can be **concurrent** in the causal sense — neither writer observed the other's write.

The mechanism for detecting this is the **vector clock** from Module 1. Riak attaches a vector clock to every object; when two writes have vector clocks that neither dominates, the system stores both as **siblings** and surfaces them to the application on the next read. This is the operational shape of "we detected a conflict; the application decides."

DDIA's discussion of "Detecting Concurrent Writes" is the practical version of the theory we developed in Module 1: vector clocks are not just a teaching example, they are the production mechanism that lets leaderless systems give correct semantics. The cost is bookkeeping (every value carries a vector clock; sibling values may accumulate); the benefit is that the system never silently drops a write because of a clock skew.

### Topology in Multi-Leader Systems

When you have N leaders, you have to decide how they replicate to each other. Three topologies appear:

**Star (one hub, several spokes).** Every leader writes to a central coordinator that forwards to the others. Simple but the hub is a bottleneck and a single point of failure.

**Circle / ring.** Each leader forwards to one neighbor, which forwards to the next, around a loop. The replication delay across the full ring is the worst-case latency. A single failed leader breaks the ring.

**All-to-all.** Every leader sends every write to every other leader. Maximum throughput, maximum redundancy, but writes can arrive out of order — and "out of order" in a multi-leader system means causally inverted, which is what vector clocks are for.

The catalog uses all-to-all because the twelve ground stations are well-connected to each other and the per-write fan-out is manageable at the catalog's write rate. The conflict detection layer handles causal inversions.

## Code Examples

### A Multi-Leader Conflict Surfaced via Vector Clock

This sketches the shape of a multi-leader write path that surfaces concurrent writes as siblings, modeled on Riak's behavior.

```rust,edition2021
use std::cmp::max;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct VersionedValue {
    pub value: String,
    pub clock: HashMap<String, u64>,
}

pub enum WriteOutcome {
    Applied(VersionedValue),
    Siblings(Vec<VersionedValue>),
}

#[derive(Default)]
pub struct MultiLeaderStore {
    // Per-key state: either a single dominant value, or a set of siblings
    // that the application has not yet resolved.
    state: HashMap<String, Vec<VersionedValue>>,
}

impl MultiLeaderStore {
    pub fn write(&mut self, node_id: &str, key: &str, value: String) -> WriteOutcome {
        let existing = self.state.entry(key.to_string()).or_default();

        // The incoming clock is built from the union of existing siblings
        // (so that the new write 'dominates' anything the writer had observed)
        // plus a tick of the writer's own slot.
        let mut new_clock: HashMap<String, u64> = HashMap::new();
        for sib in existing.iter() {
            for (k, v) in &sib.clock {
                let entry = new_clock.entry(k.clone()).or_insert(0);
                *entry = max(*entry, *v);
            }
        }
        *new_clock.entry(node_id.to_string()).or_insert(0) += 1;

        let new_version = VersionedValue { value, clock: new_clock };

        // The new write dominates any existing sibling whose clock it has
        // observed. Surviving siblings are those that are NOT dominated by
        // the new write.
        let surviving: Vec<VersionedValue> = existing
            .drain(..)
            .filter(|sib| !dominates(&new_version.clock, &sib.clock))
            .collect();

        let mut new_state = surviving;
        new_state.push(new_version.clone());

        if new_state.len() == 1 {
            self.state.insert(key.to_string(), new_state.clone());
            WriteOutcome::Applied(new_state.into_iter().next().unwrap())
        } else {
            self.state.insert(key.to_string(), new_state.clone());
            WriteOutcome::Siblings(new_state)
        }
    }
}

fn dominates(a: &HashMap<String, u64>, b: &HashMap<String, u64>) -> bool {
    let mut strictly = false;
    let keys: std::collections::HashSet<&String> = a.keys().chain(b.keys()).collect();
    for k in keys {
        let av = a.get(k).copied().unwrap_or(0);
        let bv = b.get(k).copied().unwrap_or(0);
        if av < bv { return false; }
        if av > bv { strictly = true; }
    }
    strictly
}

fn main() {
    let mut store = MultiLeaderStore::default();

    // Pacific writes 'attitude_X', Indian Ocean concurrently writes 'attitude_Y'
    // from the same baseline. Neither observed the other.
    match store.write("pacific", "MSS-17/attitude", "X".into()) {
        WriteOutcome::Applied(_) => println!("pacific: applied"),
        WriteOutcome::Siblings(_) => println!("pacific: siblings exist"),
    }

    // Indian Ocean writes from the same starting state, so the second branch
    // does not observe the Pacific write. We simulate this by directly
    // injecting a sibling rather than reading first.
    let mut store2 = MultiLeaderStore::default();
    store2.write("indian_ocean", "MSS-17/attitude", "Y".into());

    // Now the two stores reconcile by exchanging their versions.
    for v in store2.state.get("MSS-17/attitude").unwrap() {
        match store.write_external(v.clone(), "MSS-17/attitude") {
            WriteOutcome::Siblings(s) => println!("merged: {} siblings", s.len()),
            WriteOutcome::Applied(_) => println!("merged: dominated existing"),
        }
    }
}

impl MultiLeaderStore {
    /// Merge an externally-produced versioned value (e.g., received from
    /// another leader in the all-to-all replication topology).
    fn write_external(&mut self, incoming: VersionedValue, key: &str) -> WriteOutcome {
        let existing = self.state.entry(key.to_string()).or_default();
        let surviving: Vec<VersionedValue> = existing
            .drain(..)
            .filter(|sib| !dominates(&incoming.clock, &sib.clock))
            .collect();
        let dominated_by_existing = surviving.iter().any(|s| dominates(&s.clock, &incoming.clock));
        let mut new_state = surviving;
        if !dominated_by_existing {
            new_state.push(incoming);
        }
        let n = new_state.len();
        self.state.insert(key.to_string(), new_state.clone());
        if n == 1 {
            WriteOutcome::Applied(new_state.into_iter().next().unwrap())
        } else {
            WriteOutcome::Siblings(new_state)
        }
    }
}
```

This is the essential shape. The writer attaches a vector clock derived from what it has observed; existing siblings the new write dominates are discarded; siblings the new write does not dominate are preserved. The application reads and sees either a single value or a set of siblings to resolve. The same mechanism handles both local writes and merge events from other leaders.

### A Leaderless Quorum Write with Read Repair

This shows the Dynamo-style write path: send to N, wait for W, return success.

```rust,ignore
// SKETCH: leaderless write with W=2, N=3 quorum.

use anyhow::Result;
use tokio::time::{Duration, timeout};
use futures::stream::{FuturesUnordered, StreamExt};

const N: usize = 3;
const W: usize = 2;
const R: usize = 2;
const REPLICA_TIMEOUT: Duration = Duration::from_millis(500);

async fn quorum_write(
    replicas: &[Replica],
    key: &str,
    value: VersionedValue,
) -> Result<()> {
    // Dispatch the write to all N replicas in parallel.
    let mut pending: FuturesUnordered<_> = replicas
        .iter()
        .take(N)
        .map(|r| {
            let v = value.clone();
            let k = key.to_string();
            async move { timeout(REPLICA_TIMEOUT, r.write(&k, v)).await }
        })
        .collect();

    let mut acks = 0;
    let mut errors = 0;

    // Drain results as they arrive. Return as soon as W replicas ack;
    // any later acks are ignored (the request returns; the writes still
    // complete on the slow replicas in the background).
    while let Some(result) = pending.next().await {
        match result {
            Ok(Ok(())) => {
                acks += 1;
                if acks >= W {
                    return Ok(());  // Quorum met.
                }
            }
            _ => {
                errors += 1;
                // If too many errors, we cannot meet the quorum.
                if errors > N - W {
                    return Err(anyhow::anyhow!("quorum unreachable"));
                }
            }
        }
    }

    if acks >= W { Ok(()) } else { Err(anyhow::anyhow!("quorum unreachable")) }
}

// Read with read-repair: collect R responses, identify the newest, write
// back to lagging replicas.
async fn quorum_read(
    replicas: &[Replica],
    key: &str,
) -> Result<VersionedValue> {
    let mut pending: FuturesUnordered<_> = replicas
        .iter()
        .take(N)
        .map(|r| {
            let k = key.to_string();
            async move { timeout(REPLICA_TIMEOUT, r.read(&k)).await }
        })
        .collect();

    let mut responses: Vec<(usize, VersionedValue)> = Vec::new();
    let mut replica_idx = 0;
    while let Some(result) = pending.next().await {
        if let Ok(Ok(Some(v))) = result {
            responses.push((replica_idx, v));
            if responses.len() >= R {
                break;
            }
        }
        replica_idx += 1;
    }

    let newest = responses
        .iter()
        .max_by(|(_, a), (_, b)| compare_clocks(&a.clock, &b.clock))
        .map(|(_, v)| v.clone())
        .ok_or_else(|| anyhow::anyhow!("no quorum"))?;

    // Read repair: write back to lagging replicas. Don't block the response
    // on this - fire-and-forget.
    for (idx, val) in &responses {
        if !val.clock.eq(&newest.clock) {
            let target = replicas[*idx].clone();
            let k = key.to_string();
            let v = newest.clone();
            tokio::spawn(async move {
                let _ = target.write(&k, v).await;
            });
        }
    }

    Ok(newest)
}

# struct Replica;
# impl Clone for Replica { fn clone(&self) -> Self { Replica } }
# impl Replica {
#     async fn write(&self, _k: &str, _v: VersionedValue) -> Result<()> { Ok(()) }
#     async fn read(&self, _k: &str) -> Result<Option<VersionedValue>> { Ok(None) }
# }
# #[derive(Clone)] struct VersionedValue { clock: std::collections::HashMap<String, u64> }
# fn compare_clocks(_a: &std::collections::HashMap<String, u64>, _b: &std::collections::HashMap<String, u64>) -> std::cmp::Ordering { std::cmp::Ordering::Equal }
```

Three things to notice. First, the early-return on quorum: as soon as W replicas have acknowledged, the call returns — the remaining writes proceed in the background but do not block the client. Second, read-repair is fire-and-forget: the read returns the newest value immediately and lazily updates lagging replicas. Third, the failure model is explicit: if too many replicas fail to respond within the timeout, the quorum is unreachable and the operation returns an error rather than silently producing a partial write.

### A CRDT for the Authorized-Ground-Stations Set

When the data type permits, a CRDT avoids conflict resolution entirely:

```rust,edition2021
use std::collections::HashSet;

// G-Set (Grow-only Set) CRDT. Two replicas can independently add items;
// merging is union, which is commutative, associative, and idempotent -
// so the merge result is independent of the order replicas reconcile in.
#[derive(Default, Clone)]
pub struct GSet<T: Eq + std::hash::Hash + Clone> {
    items: HashSet<T>,
}

impl<T: Eq + std::hash::Hash + Clone> GSet<T> {
    pub fn add(&mut self, item: T) { self.items.insert(item); }
    pub fn contains(&self, item: &T) -> bool { self.items.contains(item) }
    pub fn merge(&mut self, other: &GSet<T>) {
        for item in &other.items {
            self.items.insert(item.clone());
        }
    }
    pub fn len(&self) -> usize { self.items.len() }
}

fn main() {
    let mut pacific = GSet::default();
    pacific.add("ground-pacific-01");
    pacific.add("ground-pacific-02");

    let mut indian = GSet::default();
    indian.add("ground-indian-01");
    indian.add("ground-pacific-01"); // overlap

    pacific.merge(&indian);
    println!("merged set size: {}", pacific.len()); // 3

    // The merge is commutative: doing it the other way produces the same set.
    let mut indian2 = GSet::default();
    indian2.add("ground-indian-01");
    indian2.add("ground-pacific-01");

    let mut pacific2 = GSet::default();
    pacific2.add("ground-pacific-01");
    pacific2.add("ground-pacific-02");

    indian2.merge(&pacific2);
    println!("symmetric merge size: {}", indian2.len()); // 3
}
```

The constraint that makes this work is that **addition is the only operation**. The moment you need removal, you need a more complex CRDT (an OR-Set or LWW-Set) that tracks causal context for removes, because "is this element present" depends on whether the add or the remove was more recent. For the authorized-ground-stations set, where stations are added and decommissioning is rare and audited, a G-Set is sufficient. For more dynamic sets, more sophisticated CRDTs apply — see Shapiro et al., "Conflict-free Replicated Data Types" (2011).

## Key Takeaways

- Multi-leader replication trades local-write latency and partition tolerance for the burden of conflict resolution. The catalog uses it for geographic regions; calendars and collaborative editors use it for offline-capable clients.
- Leaderless (Dynamo-style) replication uses parallel writes to N replicas and quorum reads from R replicas. When W + R > N the system gives quorum-consistent reads, but writes can still conflict across concurrent operations.
- Conflict resolution strategies fall on a spectrum: avoid (structure the data to prevent conflicts), automatic (LWW or CRDTs), or manual (surface siblings to the application). Default LWW with wall-clock timestamps is silently lossy under clock skew; treat it as a deliberate choice, not a default.
- Vector clocks are the production mechanism for detecting concurrent writes in multi-leader and leaderless systems. When a system says it "stores siblings" or "surfaces conflicts," it almost certainly means a vector-clock comparison underneath.
- Replication topology (star, ring, all-to-all) determines the redundancy and bandwidth profile of cross-replica traffic. All-to-all is the simplest but requires conflict detection because writes can be observed in causally-inverted orders.

> **Source note:** This lesson is grounded in DDIA 2nd Edition (Kleppmann & Riccomini), Chapter 6, "Multi-Leader Replication," "Leaderless Replication," "Handling Concurrent Writes to the Same Key," and "Detecting Concurrent Writes." The Dynamo model traces to DeCandia et al., "Dynamo: Amazon's Highly Available Key-value Store" (SOSP 2007). CRDTs are from Shapiro, Preguiça, Baquero, Zawirski, "Conflict-Free Replicated Data Types" (SSS 2011). Specific quorum parameters (N=3 W=2 R=2 etc.) are documented Cassandra/Riak defaults but should be verified against current vendor docs.

{{#quiz quiz-2.toml}}
