# Lesson 1: Why Consensus Is Hard

## Context

The Constellation Network's most consequential decision is one that has not yet been automated: when the primary leader of the catalog cluster fails, who promotes the new leader? Today this is a paging-followed-by-runbook procedure. An operator confirms the failure (often by waiting for several other operators to confirm the same observation), uses an out-of-band channel to declare the promotion, and updates the configuration of the cluster's followers to point to the new leader. The procedure works because there is exactly one operations team and they can coordinate among themselves. It does not scale, it does not work at 03:00 local time on a weekend, and it does not work when the operations team's communication channel is the same network that just partitioned.

The mechanism the team needs is **distributed consensus**: a protocol by which a group of nodes can agree on a value (in this case, "who is the leader now") in the presence of failures and network partitions. Consensus is the mechanism behind every modern automated failover system, every distributed lock service, every cluster-membership coordinator. ZooKeeper, etcd, Consul, and the consensus layer of Spanner, CockroachDB, and YugabyteDB all implement variants of consensus.

This lesson sets up the **why** of consensus before Lesson 2 covers the **how** (Raft). By the end, you should understand why consensus is theoretically difficult (the FLP impossibility result), why it is practically achievable (with weak synchrony assumptions), and why the family of problems — atomic commit, leader election, total-order broadcast, linearizable CAS — are all the same problem in disguise. This framing matters because it tells you when reaching for consensus is the right tool, and when something cheaper will do.

## Core Concepts

### What Consensus Is, Precisely

DDIA defines consensus in terms of four formal properties that any consensus protocol must satisfy:

1. **Uniform agreement** — No two nodes decide on different values. (Sometimes called "agreement" or "safety.")
2. **Integrity** — No node decides twice. Once a node decides, the decision stands.
3. **Validity** — If a node decides on value `v`, then `v` was proposed by some node.
4. **Termination** — Every non-failing node eventually decides on a value. (Sometimes called "liveness.")

Agreement and integrity say that nothing bad happens (no inconsistent or repeated decisions). Validity says the system can't decide on a value out of nowhere. Termination says something good eventually happens (the protocol completes).

These are not trivial requirements. Most weaker protocols give you only a subset. Best-effort broadcast satisfies validity but not agreement. Single-leader replication satisfies agreement (the leader decides) but only conditional termination (if the leader is up). A consensus protocol is one that satisfies all four properties in the presence of any minority of failures.

The "minority of failures" qualifier is essential. No protocol can tolerate an arbitrary number of failures: if every node fails, no protocol can produce a decision. The standard assumption is that fewer than half the nodes fail, which is why Raft, Paxos, and their relatives are typically configured with odd numbers of nodes (3, 5, 7) — to maximize the failure tolerance for a given cluster size.

### The FLP Impossibility Result

In 1985, Fischer, Lynch, and Paterson published a result that is one of the foundational impossibility theorems of distributed systems: **in a fully asynchronous system, no deterministic consensus protocol can guarantee termination in the presence of even a single faulty process.**

The intuition: in a fully asynchronous network, you cannot distinguish a node that has crashed from a node that is merely slow. A consensus protocol must wait to hear from enough nodes to know the decision is valid — but if it waits forever for a node that is "just slow," it violates termination; if it gives up too early, it might be wrong about which value is decided, violating agreement.

The result sounds devastating but is more constraining than fatal. It says that you cannot have **guaranteed** termination in a **fully asynchronous** system. Real consensus protocols sidestep it in two ways:

**Randomization.** Probabilistic consensus protocols (Ben-Or's algorithm, Bitcoin's proof-of-work) guarantee termination with probability 1, but not with probability 1.0 in finite time. For practical purposes the convergence is fast; the FLP result still applies, but only to deterministic protocols.

**Partial synchrony.** Real networks are not fully asynchronous — they are "mostly synchronous, sometimes not." The partial-synchrony model (Dwork, Lynch, Stockmeyer 1988) says the network has some unknown but finite period during which messages are delivered in bounded time, even though delays can be arbitrary outside that period. Under partial synchrony, deterministic consensus is possible, and Raft and Paxos are the production protocols for this model. They make progress during synchronous periods and gracefully stall during asynchronous ones, resuming when the network stabilizes.

The practical takeaway: when you read that a consensus protocol "tolerates network delays," what is meant is that it preserves safety (no wrong decisions) under arbitrary delays, and preserves liveness (progress) only when the network is synchronous enough. This is the right model. Permanent asynchrony is indistinguishable from total network failure, against which no protocol can make progress.

### Why Linearizability Requires Consensus

Module 1 introduced linearizability as the strongest single-object consistency model. DDIA's deep dive in Chapter 10 makes a specific claim: **linearizable operations require consensus**. Not just resemble consensus, not just enable consensus — they are equivalent in their fundamental requirements.

The argument: a linearizable register is one where every operation appears to take effect at a single point in time, and all clients observe the same total order of operations. Producing this total order requires that the set of replicas agree on the order. Agreement on an order is consensus — specifically, **total-order broadcast** consensus, where the value being agreed on is "the next operation in the log."

The implications are practical:

- Any database that offers linearizability under failures is running consensus internally. Spanner, CockroachDB, FoundationDB, and etcd all expose linearizable operations and all run Paxos or Raft.
- A database that advertises strong consistency without consensus is making a weaker claim. It may be linearizable when no failures occur; it may degrade silently when they do.
- Performance-wise, every linearizable operation pays the consensus round-trip cost. This is why high-throughput systems offer linearizability as an opt-in (per-operation or per-table) rather than the default — the cost is too high for workloads that can tolerate weaker consistency.

### Atomic Commit and Consensus Are the Same Problem

The other place consensus appears unexpectedly is in **atomic commit** — the problem of ensuring that a transaction either commits on all participating nodes or aborts on all of them. The classical solution is two-phase commit (2PC), and DDIA discusses 2PC's pathologies in detail: it blocks indefinitely if the coordinator fails between the prepare and commit phases.

The deep observation is that atomic commit and consensus are equivalent: any solution to one gives you a solution to the other. 2PC's blocking behavior is not a quirk of 2PC; it is a manifestation of the same fundamental difficulty as FLP. The fix is to replace the single coordinator with a consensus-protected log of commit decisions, which is what production systems do — Spanner's 2PC is built on top of Paxos, CockroachDB's distributed transactions are built on Raft.

The atomic commit problem has its own four formal properties (validity, agreement, integrity, termination — same names, slightly different meanings), and they map cleanly onto consensus. If you understand consensus, you understand atomic commit; the implementation details differ but the impossibility is the same.

### Single-Value Consensus and Compare-and-Set

The simplest expressions of consensus are **single-value consensus** (decide on one value, once) and **compare-and-set** (decide which of two writes to a register wins). Both can be implemented on top of total-order broadcast: each operation is broadcast through the log, and the first to land on the deciding offset is the one that takes effect.

DDIA's table makes this explicit: single-value consensus, atomic commit, total-order broadcast, linearizable CAS, lock acquisition, and uniqueness constraints are **all equivalent in computational power**. Any of these can be reduced to any other. The "consensus number" of a primitive (Herlihy 1991) measures how powerful it is: linearizable CAS has infinite consensus number, meaning it can implement consensus for any number of processes. Lower-power primitives (atomic increment, fetch-and-add) cannot implement consensus past a certain group size.

The practical implication: when you reach for a distributed lock service or a leader election service, you are reaching for consensus. The advertised primitive (lock, election, CAS) is a thin veneer over a consensus log. ZooKeeper's API exposes locks; etcd's API exposes a KV store with linearizable operations; both are consensus engines under the hood. Knowing this lets you pick the right level of abstraction — and lets you recognize when you're solving a consensus problem by accident, with mechanisms that don't actually provide consensus guarantees.

### When Not to Reach for Consensus

Given how powerful and important consensus is, the temptation is to use it for everything. Don't. Consensus pays a per-operation latency cost (at minimum, one round-trip to a majority quorum) and a configuration cost (running and operating a consensus cluster). For many problems, cheaper mechanisms suffice:

- **Eventually consistent updates** (CRDTs, vector clocks, anti-entropy) — when the application can tolerate temporary divergence.
- **Single-leader replication** with manual failover — when leader election doesn't need to be automatic.
- **Sharding by key** — when independent shards don't need cross-shard agreement.
- **Best-effort broadcast** — for non-critical notifications.

Use consensus when you need: automatic leader election that survives failures; a linearizable operation under failures; an atomic decision among nodes that must converge to the same answer; or membership changes that must be totally ordered. The catalog's leader election is one of these. The catalog's bulk telemetry ingest is not — it does not need consensus, and forcing it through a consensus log would limit throughput unacceptably.

## Code Examples

### A Toy Single-Value Consensus Decision (Sketch)

The smallest illustrative shape — not a production protocol — captures the structural difficulty:

```rust,ignore
// TOY: a single-value consensus over N nodes with a fixed proposer.
// This is NOT Raft or Paxos; it elides leader election, log replication,
// and crash recovery. The point is to expose what consensus *requires*.

use anyhow::Result;
use std::time::Duration;

struct Node { id: u64 }

async fn propose(nodes: &[Node], value: &str) -> Result<&str> {
    // Phase 1: ask a majority if they have already accepted a value.
    // If any has, we must adopt that value (validity / safety).
    let mut acks = 0;
    let mut existing_value: Option<&str> = None;
    for node in nodes {
        if let Some(prev) = ask_node(node).await? {
            existing_value = Some(prev);  // Honor what was already decided.
        }
        acks += 1;
        if acks > nodes.len() / 2 { break; }  // Majority reached.
    }

    let chosen = existing_value.unwrap_or(value);

    // Phase 2: tell a majority to accept the chosen value.
    // Once a majority has accepted, the decision is final.
    let mut applies = 0;
    for node in nodes {
        if accept_value(node, chosen).await? {
            applies += 1;
            if applies > nodes.len() / 2 {
                return Ok(chosen);
            }
        }
    }

    anyhow::bail!("could not achieve majority")
}

# async fn ask_node(_n: &Node) -> Result<Option<&'static str>> { Ok(None) }
# async fn accept_value(_n: &Node, _v: &str) -> Result<bool> { Ok(true) }
```

Three things to notice. First, **the two-phase structure**: we cannot just tell nodes the value; we first have to discover whether a previous decision exists, then propagate the chosen value. Without phase 1, two simultaneous proposers could pick different values and both reach majority, violating agreement. Second, **the majority quorums**: every phase contacts more than half the nodes, which is the mechanism that guarantees overlap — any two majority quorums share at least one node, so any decision visible to one quorum is visible to the next. Third, **the failure mode**: if a majority is unreachable, the protocol returns an error rather than producing an unsafe decision. This is the consensus tradeoff: preserve safety, sacrifice availability when the quorum is gone.

Real protocols (Raft, Paxos) add leader election to avoid live-lock between competing proposers, log replication to handle a stream of decisions efficiently, and crash recovery to make the protocol durable. Lesson 2 covers Raft's specific instantiation of these mechanisms.

### A Failure Detector That Underpins Consensus

The consensus protocol relies on a failure detector to suspect that a node has crashed. The most basic implementation is a heartbeat:

```rust,edition2021
use std::time::{Duration, Instant};
use std::collections::HashMap;

pub struct HeartbeatDetector {
    last_seen: HashMap<String, Instant>,
    timeout: Duration,
}

impl HeartbeatDetector {
    pub fn new(timeout: Duration) -> Self {
        Self { last_seen: HashMap::new(), timeout }
    }

    pub fn note_heartbeat(&mut self, node: &str) {
        self.last_seen.insert(node.to_string(), Instant::now());
    }

    pub fn suspected_dead(&self) -> Vec<&String> {
        let now = Instant::now();
        self.last_seen
            .iter()
            .filter(|(_, &t)| now.duration_since(t) > self.timeout)
            .map(|(name, _)| name)
            .collect()
    }
}

fn main() {
    let mut det = HeartbeatDetector::new(Duration::from_secs(3));
    det.note_heartbeat("node-a");
    det.note_heartbeat("node-b");
    // Imagine some time passes...
    println!("suspected: {:?}", det.suspected_dead());
}
```

The choice of timeout is the consensus protocol's liveness knob. Too short and the protocol declares healthy nodes dead, triggering unnecessary leader elections; too long and real failures take too long to recover. Module 4 covers more sophisticated detectors (phi accrual) that adapt to observed network conditions, but the principle is the same: consensus requires **some** failure detector, and the detector's accuracy directly determines the protocol's availability profile.

## Key Takeaways

- Consensus is the protocol by which a group of nodes agree on a value despite failures and asynchrony. The four formal properties (agreement, integrity, validity, termination) are what distinguish consensus from cheaper protocols.
- The FLP impossibility result says deterministic consensus cannot guarantee termination in a fully asynchronous system. Practical protocols sidestep this with partial synchrony: they preserve safety always, and make progress whenever the network is synchronous enough.
- Linearizability and consensus are equivalent in their requirements. Any database that offers linearizable operations under failures is running consensus internally. The performance cost is the consensus round-trip per operation.
- Atomic commit, total-order broadcast, linearizable CAS, leader election, and distributed locks are all the same problem. If you understand consensus, you understand all of them; the implementation surface differs but the impossibility profile does not.
- Consensus is expensive. Use it when you need linearizability under failures, automatic leader election, or totally-ordered membership changes. Use cheaper mechanisms (eventually consistent updates, manual failover, sharding) when the application can tolerate them.

> **Source note:** This lesson is grounded in DDIA 2nd Edition (Kleppmann & Riccomini), Chapter 10, "Linearizability," "Ordering Guarantees," "Distributed Transactions and Consensus" — particularly the subsection "Consensus." The FLP result is from Fischer, Lynch, Paterson, "Impossibility of Distributed Consensus with One Faulty Process" (Journal of the ACM, April 1985). Partial synchrony is from Dwork, Lynch, Stockmeyer, "Consensus in the Presence of Partial Synchrony" (Journal of the ACM, April 1988). The consensus-number framework is from Herlihy, "Wait-Free Synchronization" (TOPLAS, 1991). Specific equivalence claims among consensus, atomic commit, CAS, and total-order broadcast are standard textbook results; the framing here follows DDIA's presentation.

{{#quiz quiz-1.toml}}
