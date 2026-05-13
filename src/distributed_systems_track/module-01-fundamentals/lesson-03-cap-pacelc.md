# Lesson 3: CAP, PACELC, and the Consistency Spectrum

## Context

When the Antarctic relay path went down for nine minutes during a Southern Ocean storm, the Constellation Operations team had to make a decision they had not deliberately planned for: should the catalog accept writes from ground stations that could no longer reach a quorum of replicas? The on-call answer was yes — the alternative was rejecting telemetry for nine minutes — but no one had documented what "accept writes during a partition" actually meant for the system's guarantees. By the end of the incident, three ground stations had accepted updates against two divergent replica sets, and reconciliation took another hour of manual work to complete.

This was not a failure of the catalog. It was a failure of the team to have an explicit position on the **CAP theorem**: the impossibility result that says when a distributed system is partitioned, it must give up either consistency (in a specific technical sense) or availability. CAP is one of the most misunderstood theorems in distributed systems, partly because the standard "CAP triangle" framing — "pick two of three" — is misleading. Partitions are not optional; they are a fact of life on every real network. The actual choice is between consistency and availability *during* a partition. PACELC, the refinement of CAP introduced by Daniel Abadi in 2010, extends the framing to cover normal operation as well: when there is no partition, you still trade latency against consistency.

This lesson teaches you to characterize a system by its consistency model rather than by its product category. By the end, you should be able to look at a sentence like "our catalog is eventually consistent" and ask the right follow-up questions: eventually consistent under what failure model, with what staleness bound, exhibiting which anomalies under which workloads. The vocabulary in this lesson is what allows the next two modules — Replication and Consensus — to talk precisely about which guarantees a given algorithm provides.

## Core Concepts

### What CAP Actually Says (and Doesn't Say)

The CAP theorem, formalized by Seth Gilbert and Nancy Lynch in 2002 from Eric Brewer's earlier conjecture, states that a distributed data system cannot simultaneously provide all three of:

- **Consistency** — in CAP, this specifically means **linearizability**: every read sees the result of the most recent write that completed before the read began, as if the system were a single non-distributed register.
- **Availability** — every non-failing node returns a non-error response to every request in a bounded time.
- **Partition tolerance** — the system continues to operate when the network drops or delays arbitrary messages between nodes.

The "two of three" framing is misleading because partition tolerance is not optional. If you build a real distributed system, the network will partition you sooner or later. The actual theorem says: **in the presence of a partition, you must choose between linearizability and availability**. You cannot have both, because honoring a write on the unreachable side of a partition either requires waiting for that side to come back (giving up availability) or accepting writes that conflict with the other side (giving up linearizability).

Two more clarifications matter. First, "consistency" in CAP is not the C in ACID. ACID consistency means transactions preserve declared invariants; CAP consistency means a specific linearizability property. The overloading of the word causes endless confusion. Second, CAP is binary in the formal statement, but in practice systems offer a spectrum of consistency models — linearizability is the strongest, but there are many weaker models (sequential, causal, eventual) that are still useful and that don't fall off the CAP cliff in the same way.

### PACELC: The Half of the Story CAP Doesn't Tell

CAP only describes behavior during a partition. But in practice, partitions are relatively rare events on a well-engineered network. The system spends most of its time *not* partitioned, and during that time it still makes tradeoffs — between consistency and latency.

PACELC, proposed by Daniel Abadi, captures this:

> **If** there is a **P**artition, the system chooses between **A**vailability and **C**onsistency; **E**lse (in normal operation), the system chooses between **L**atency and **C**onsistency.

The P/A/C side is just CAP. The E/L/C side is new: even with no partition, achieving linearizability requires coordination — a write must be acknowledged by enough replicas to ensure subsequent reads will see it — and coordination costs latency. A system that trades coordination for low latency is fast in the normal case and stale in detail; a system that pays the coordination tax has consistent reads but slower writes.

PACELC produces a four-cell taxonomy, and real systems map to it cleanly:

| System | Partition behavior | Normal-operation tradeoff | PACELC |
|---|---|---|---|
| etcd, ZooKeeper, Spanner | Reject writes to maintain linearizability | Pay latency for strong consistency | **PC/EC** |
| Cassandra (default QUORUM) | Sloppy quorums, hinted handoff | Pay latency for tunable consistency | **PA/EC** (tunable) |
| DynamoDB (default), Cassandra (ONE) | Accept writes anywhere | Fast reads possible from any replica | **PA/EL** |
| MongoDB (default, before 4.x) | Continue accepting from primary | Read from primary by default | **PC/EL** (historical) |

The cleanest way to characterize any production data system is to place it on this matrix and ask: do you know which cell it is in? If the team cannot answer, that is itself a finding — the system is making the tradeoff implicitly, and the choice will assert itself during an incident.

### The Consistency Spectrum

Linearizability is the strongest single-object consistency model in practice. Below it lies a spectrum:

**Linearizability** — There exists a single total order of operations consistent with real-time. Every read sees the latest committed write. This is the model CAP refers to. Implementation requires coordination on every write and typically every read.

**Sequential consistency** — There exists a single total order consistent with each node's program order, but not necessarily consistent with real-time. A read can see a slightly stale value as long as no node observes operations in conflicting orders. Cheaper to implement; rarely the default in production data systems because the staleness bound is undefined.

**Causal consistency** — Operations that are causally related (per the happens-before relation from Lesson 2) are seen in the same order by all nodes. Concurrent operations may be observed in different orders. Strong enough to prevent counterintuitive anomalies like "I see B's reply to my message before I see my own message." Achievable without a single coordinator; Riak and COPS implement variants.

**Read-your-writes consistency** — A particular client always sees its own writes in subsequent reads. Easier to provide than causal because it only constrains one client; typically implemented with session tokens.

**Eventual consistency** — If writes stop, all replicas will eventually converge to the same value. Says nothing about staleness, ordering, or anomalies during convergence. Useful as a baseline; insufficient as a guarantee on its own without additional model specification (e.g., "eventually consistent with bounded staleness of N seconds and monotonic-read session guarantees").

The catalog incident in the opening context was specifically an eventual-consistency outcome dressed up as something stronger. The team thought "eventually consistent" was a property of the system; it was actually a placeholder for a real specification they hadn't written. Concurrent writes during the partition diverged with no detection mechanism (the system had no vector clocks), and the convergence was manual rather than automatic.

### Linearizability vs Serializability

These two words are often confused, including in textbooks, and the confusion has consequences.

**Linearizability** is a single-object property: each register or object in the system behaves as if every operation on it happens atomically at some point between its invocation and its response, in an order consistent with real-time.

**Serializability** is a multi-object property: a set of transactions executes in some order that is equivalent to *some* serial schedule. Serializability says nothing about real-time ordering; two transactions with no observable dependency can be reordered however the scheduler chooses.

You can have one without the other. Snapshot isolation is serializable in the sense that there exists a serial equivalent, but it is not linearizable because reads see a snapshot rather than the latest committed value. Strict serializability is the combination of both, and it is the gold standard offered by systems like Spanner.

For the constellation, the practical implication is: a "serializable" catalog can still return stale reads. If you need "every read sees the most recent write," you need linearizability — and you need to be prepared to pay the coordination cost.

### Reading a System's Consistency Claim Skeptically

When a system claims a consistency model, three follow-up questions tell you whether the claim is operationally meaningful:

1. **Under what failure model does the claim hold?** Many systems advertise strong consistency in the absence of failures and degrade silently when nodes fail. Spanner's strict serializability holds under failures; DynamoDB's strong consistency mode does, too. Some early NoSQL systems claimed "strong consistency" but degraded to last-write-wins under partition.
2. **What is the staleness bound?** "Eventually consistent" is not a bound. "Read-your-writes within 100ms under network conditions X" is. If the documentation does not give you numbers, the bound is "indefinite."
3. **What anomalies are observable to a client?** The CAP and PACELC frameworks classify systems but do not enumerate the specific bugs each model permits. Consult Kyle Kingsbury's Jepsen reports for empirical evidence; their analyses of specific products are the most rigorous public source on what consistency models actually deliver under stress.

The catalog choice in the opening context — "accept writes during the partition" — was a defensible PA/EL position. The failure was not the choice; it was that the team made the choice in the middle of an incident, without knowing it was a choice, and without having pre-defined the conflict resolution that PA/EL requires.

## Code Examples

### A Linearizable Register (Pseudocode for a Single-Leader Implementation)

The simplest way to provide linearizability is a single leader that serializes all operations. Every write goes through the leader; every read either goes through the leader or waits for confirmation that the local replica is caught up.

```rust,ignore
// CONCEPTUAL: a linearizable single-register store.
// Production implementations replicate the log via Raft (Module 3) — this
// pseudocode shows the single-leader version for clarity.

use std::sync::Mutex;
use std::time::Duration;

pub struct LinearizableRegister<T> {
    leader: bool,
    state: Mutex<T>,
}

impl<T: Clone + Send> LinearizableRegister<T> {
    pub async fn write(&self, value: T) -> Result<(), Error> {
        if !self.leader {
            // Forward to leader. Read-only replicas reject writes - this is
            // what gives up Availability during a partition: if we can't reach
            // the leader, we can't write.
            return Err(Error::NotLeader);
        }
        // Replicate to a majority before acknowledging. This is the cost of
        // linearizability: every write pays the round-trip to a quorum of
        // followers. During a partition where we lose the quorum, this call
        // blocks or fails - we sacrifice availability to preserve consistency.
        self.replicate_to_majority(&value).await?;
        *self.state.lock().unwrap() = value;
        Ok(())
    }

    pub async fn read(&self) -> Result<T, Error> {
        if self.leader {
            // Even reads need a coordination step (a 'read index' or 'lease
            // read') to ensure we are still the leader and our state isn't
            // stale relative to a newer leader. Otherwise we could be a
            // deposed leader returning stale data, violating linearizability.
            self.confirm_still_leader().await?;
            Ok(self.state.lock().unwrap().clone())
        } else {
            // Followers must contact the leader to get a consistent read, or
            // wait until they are caught up to a known committed index.
            self.read_from_leader().await
        }
    }

    async fn replicate_to_majority(&self, _v: &T) -> Result<(), Error> { Ok(()) }
    async fn confirm_still_leader(&self) -> Result<(), Error> { Ok(()) }
    async fn read_from_leader(&self) -> Result<T, Error> { unimplemented!() }
}

# enum Error { NotLeader }
```

The fact that **reads also require coordination** is what catches most people off guard. Linearizability is not "writes are durable"; it is "reads observe a real-time-consistent order." A leader that has been silently superseded and continues serving local reads is the classic linearizability violation. Module 3 will cover read-index and lease-read techniques in detail.

### A PA/EL Store with Last-Write-Wins (Conflict Anti-Pattern)

This is the catalog's original implementation, captured for posterity:

```rust,ignore
// ANTI-PATTERN: last-write-wins using wall-clock timestamps under PA/EL.
// This is what produced the MSS-17 incident in Lesson 2.

use std::time::SystemTime;
use std::collections::HashMap;

pub struct PAELStore {
    // Each entry stores the value along with the timestamp claimed by the
    // writer. There's no coordination - writes succeed on whatever replica
    // receives them and propagate asynchronously.
    state: HashMap<String, (SystemTime, Vec<u8>)>,
}

impl PAELStore {
    pub fn put(&mut self, key: String, value: Vec<u8>, claimed_ts: SystemTime) {
        match self.state.get(&key) {
            // 'Newer' is defined as 'larger SystemTime' - a lie if the writer's
            // NTP is off. Concurrent writes from machines with different drift
            // will silently pick a winner that has no real causal precedence.
            Some((existing_ts, _)) if *existing_ts >= claimed_ts => return,
            _ => {
                self.state.insert(key, (claimed_ts, value));
            }
        }
    }

    pub fn get(&self, key: &str) -> Option<&[u8]> {
        // No quorum read, no anti-entropy check - we just return whatever
        // this replica happens to have. Two clients reading the same key on
        // different replicas can see different values for an unbounded time.
        self.state.get(key).map(|(_, v)| v.as_slice())
    }
}
```

The fixes are not subtle. Replace `SystemTime` with a Lamport or hybrid logical clock (Lesson 2). Replace "if newer, win" with vector-clock-based conflict detection that surfaces concurrent writes to the application. Add anti-entropy (Merkle trees, read repair) so divergent replicas converge automatically. Each of these is a real implementation step — the point is that **the PA/EL position is not "we don't care about consistency"; it is "we make consistency a property of the conflict resolution mechanism, not the write path."**

### Detecting Concurrency in the Catalog (Vector Clocks, Revisited)

Tying Lesson 2 to PACELC: the right resolution for a PA/EL catalog is to use vector clocks to detect concurrency and either store siblings (Riak's approach) or apply a deterministic, causal-aware merge.

```rust,edition2021
use std::cmp::max;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Versioned<T> {
    pub value: T,
    pub clock: HashMap<String, u64>,
}

pub enum Resolution<T> {
    Single(Versioned<T>),
    Siblings(Vec<Versioned<T>>),
}

/// Compare two versioned values; return whichever causally dominates, or
/// both as siblings if they are concurrent. The application then decides
/// how to merge - last-write-wins is sometimes acceptable here, but the key
/// is that we *know* we are doing it, and we know on which axis.
pub fn reconcile<T: Clone>(a: Versioned<T>, b: Versioned<T>) -> Resolution<T> {
    let mut a_greater = false;
    let mut b_greater = false;

    let keys: std::collections::HashSet<&String> =
        a.clock.keys().chain(b.clock.keys()).collect();

    for k in keys {
        let av = a.clock.get(k).copied().unwrap_or(0);
        let bv = b.clock.get(k).copied().unwrap_or(0);
        if av > bv { a_greater = true; }
        if bv > av { b_greater = true; }
    }

    match (a_greater, b_greater) {
        (true, false) => Resolution::Single(a),    // a dominates
        (false, true) => Resolution::Single(b),    // b dominates
        (false, false) => Resolution::Single(a),   // equal, pick either
        (true, true) => Resolution::Siblings(vec![a, b]),  // concurrent
    }
}

fn main() {
    let pacific = Versioned {
        value: "attitude_X".to_string(),
        clock: HashMap::from([("catalog".into(), 1), ("pacific".into(), 1)]),
    };
    let indian = Versioned {
        value: "attitude_Y".to_string(),
        clock: HashMap::from([("catalog".into(), 1), ("indian_ocean".into(), 1)]),
    };
    match reconcile(pacific, indian) {
        Resolution::Siblings(_) => println!("conflict surfaced - operator decides"),
        Resolution::Single(v) => println!("merged to {}", v.value),
    }
}
```

The output is `conflict surfaced - operator decides`, which is the correct behavior. The PA/EL system is now honest about what it doesn't know: two concurrent updates exist, the system cannot decide which one is "right," and a human (or an application-level rule) needs to resolve it. This is the operational discipline that makes eventual consistency safe to deploy.

## Key Takeaways

- The CAP theorem says that during a partition, a system must give up either linearizability (CAP-style consistency) or availability. Partition tolerance is not an option you opt into; it is a fact of any real network. The choice is about partition *behavior*, not partition *occurrence*.
- PACELC extends CAP with a second axis: during normal (non-partitioned) operation, the system trades latency for consistency. Real systems map to one of four cells (PA/EL, PA/EC, PC/EL, PC/EC) — and the team should know which cell its stack is in, in advance.
- "Consistency" is overloaded. The C in CAP is linearizability. The C in ACID is invariant preservation. They are different concepts. Be explicit about which you mean.
- Linearizability and serializability are not the same thing. Linearizability is a single-object real-time property; serializability is a multi-transaction equivalence property. A serializable system can still return stale reads.
- "Eventually consistent" by itself is not a specification. The useful version names the staleness bound, the failure model, the conflict resolution mechanism, and the observable anomalies. If the documentation doesn't, the operations team will discover those properties during an incident.

> **Source note:** This lesson is grounded in DDIA 2nd Edition (Kleppmann & Riccomini), Chapter 10, "Linearizability" and the chapter introduction. CAP's formal statement is from Gilbert & Lynch (2002); the original conjecture is Brewer (PODC 2000). PACELC is from Daniel Abadi, "Consistency Tradeoffs in Modern Distributed Database System Design" (IEEE Computer, 2012). The system placements on the PACELC matrix reflect documented defaults as of training cutoff and should be verified against current vendor documentation — particularly for DynamoDB and MongoDB, whose default consistency modes have evolved across releases. The Jepsen reference is to Kyle Kingsbury's analyses at jepsen.io; specific findings should be cited to specific reports.

{{#quiz quiz-3.toml}}
