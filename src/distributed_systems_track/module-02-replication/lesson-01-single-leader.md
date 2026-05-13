# Lesson 1: Single-Leader Replication

## Context

The Constellation Catalog is the source of truth for every orbital object Meridian tracks — fifty thousand TLEs, attitude states, link budgets, conjunction predictions. When the catalog was first deployed it ran as a single PostgreSQL instance, and reads were fast and writes were obvious. Three years later, ground stations on five continents need to read the catalog with bounded latency, and the catalog needs to survive a datacenter failure without losing acknowledged writes. The single-instance design no longer fits. Replication is how you get from "one machine holds the truth" to "many machines hold copies of the truth, and the copies eventually agree."

The simplest, most widely deployed replication model is **single-leader replication**: one designated node accepts all writes, applies them to its local state, and propagates them to a set of followers. Every read can be served from any replica that the workload tolerates the staleness of. PostgreSQL streaming replication, MySQL primary-replica, MongoDB replica sets, and the leader-tier of every Raft-based system all use this shape. The shape is so prevalent that most engineers have used it without naming it, and most production database outages — failover storms, replication-lag-induced read anomalies, lost writes after primary failure — are failure modes specific to this shape.

This lesson treats single-leader replication as the baseline that the next two lessons (multi-leader/leaderless, read consistency) will refine and complicate. By the end, you should be able to read a system's replication configuration and predict its behavior under three specific failure modes: a follower crash, a leader crash, and a network partition between the leader and a majority of followers.

## Core Concepts

### The Replication Log: Synchronous vs Asynchronous

The mechanism that keeps followers in sync with the leader is the **replication log**: an ordered stream of write operations that the leader emits and each follower applies in order. The interesting question is **when** the leader considers a write committed: after sending it to the followers, or after the followers acknowledge receipt and durability?

In **synchronous replication**, the leader waits for a designated number of followers to confirm a write before reporting success to the client. The strict version requires every follower to acknowledge; this is unusable in practice because one slow follower blocks all writes. The realistic version requires a quorum — for example, "one synchronous follower plus the leader's local commit," which PostgreSQL calls *quorum_commit*. Synchronous replication gives you durability across machines for every acknowledged write, at the cost of write latency equal to the slowest synchronous follower's round trip.

In **asynchronous replication**, the leader acknowledges the write as soon as its local commit succeeds and propagates to followers in the background. Writes are durable on the leader but not yet on followers when acknowledged. If the leader fails before propagation completes, the acknowledged writes are lost. Asynchronous replication is the default in most systems because it is faster and tolerates slow followers — at the cost of weaker durability guarantees, which manifests as the "lost writes after failover" failure mode.

The catalog's policy should be explicit: the team chose **one synchronous follower** for the catalog primary, on the basis that "we will accept slightly higher write latency in exchange for never losing an acknowledged orbital element." This is a judgment call. The Mission Control telemetry pipeline made the opposite call — asynchronous replication, fast writes, accept that some recent telemetry samples may be lost if the primary fails — because telemetry is high-volume and individual samples are not individually critical. The same database engine supports both policies; the configuration is the contract.

### Replication Log Implementations

Three implementation strategies appear in practice, and each has tradeoffs that matter when you operate the system.

**Statement-based replication** ships the SQL (or equivalent) command and re-executes it on followers. It is conceptually simple and compact, but it breaks for any non-deterministic operation: `NOW()`, `RANDOM()`, sequences, triggers with side effects, or any function whose result depends on local state. MySQL used statement-based replication by default for years and accumulated a long list of incompatible features. Modern systems generally avoid this.

**Write-ahead log (WAL) shipping** ships the byte-level WAL of the storage engine. PostgreSQL streaming replication does this. It is exactly correct because the follower applies the same bytes the leader did, but it tightly couples the leader and follower to the same storage engine version. A WAL produced by PG 17 cannot be replayed on PG 16. This makes rolling upgrades possible (upgrade follower first, then promote) but cross-version replication impossible.

**Logical (row-based) replication** ships logical change records: "row with primary key X now has columns set to Y." This decouples the replication format from the storage engine, allowing replicas to run different versions or even different database systems. It is the basis for change data capture (CDC) systems like Debezium and the mechanism behind many cross-cloud replication setups. The cost is that logical replication is per-row, not per-page; the volume of replication traffic can be much higher than WAL shipping for bulk operations.

The catalog uses PostgreSQL with logical replication to a separate analytics replica, plus WAL streaming to a hot standby. The two replicas serve different purposes and use different mechanisms; the choice is intentional.

### Setting Up New Followers Without Stopping the Leader

Adding a follower to a running cluster is one of the operations that distinguishes a real production system from a toy. The naive answer — "take a snapshot, copy it, start replaying logs from there" — has a subtle correctness issue: between the snapshot and the start of log replay, the leader has accepted writes. You need a consistent point that the snapshot represents and a known position in the log from which to start replay.

DDIA describes the standard sequence: (1) take a snapshot of the leader's database at a moment when the WAL position is known, ideally without taking a lock that blocks writes; (2) copy the snapshot to the new follower; (3) start the follower's WAL replay from the known position; (4) wait for the follower to catch up. PostgreSQL's `pg_basebackup` implements this; MongoDB's initial sync does the same shape. The follower is considered "caught up" when its replay lag is below some threshold, after which it can serve reads.

This procedure is also the basis for "follower restore" — recovering a follower that has fallen behind and cannot catch up from the log because the leader has discarded the older log segments. The same snapshot-and-replay process applies. The operational cost is bandwidth and time; for a large database, a new follower can take hours to bootstrap, during which the cluster is running with reduced redundancy.

### Failover: The Hardest Operation in the System

When the leader fails, the cluster needs a new leader. The mechanics — pick a follower, promote it, redirect writes — sound simple, and they are not. Failover is the operation that loses the most data, breaks the most invariants, and produces the most outages in single-leader systems.

DDIA's enumeration of failover hazards is exhaustive; the highlights:

1. **Lost writes from asynchronous replication.** Any writes the old leader had acknowledged but not yet propagated to the new leader are lost. The catalog's "one synchronous follower" policy is designed precisely to ensure that the synchronous follower is the candidate for promotion, so no acknowledged write is lost.
2. **Split brain.** If the old leader is not actually dead — just unreachable — and the cluster promotes a new leader, both leaders will accept writes. When the partition heals, the two write streams must be reconciled, and there is no general-purpose mechanism for this in a single-leader system. The standard defense is **fencing**: the new leader is given a generation number, and any write tagged with an older generation is rejected by the storage layer.
3. **Coordinator failures.** The component that decides "the leader is dead, promote a follower" is itself a distributed system, and getting it wrong is how you produce more outages than you prevent. Production systems use Raft (Module 3) or ZooKeeper to make the failover decision, because consensus is exactly the right tool for "we all agree on who is leader now."
4. **Hard timeouts and flapping.** If the failover threshold is too aggressive, transient network blips trigger failovers that immediately reverse. If it is too lenient, real failures leave the cluster unavailable for too long. There is no universally correct timeout, and tuning it is one of the standing operational tasks.

The catalog has had two failovers in three years. The first lost two minutes of telemetry writes because asynchronous replication had not caught up. The second succeeded cleanly because the team had since switched to synchronous replication for the primary's first follower. The cost is the higher steady-state write latency. The tradeoff is documented in the runbook.

### Replication Lag: The Anomaly Surface

Even synchronous replication only guarantees that *one* follower has the write. The other followers are still asynchronous, and the time between the leader committing and a follower applying is **replication lag**. Replication lag is the source of the anomalies that Lesson 3 covers in detail (read-after-write, monotonic reads, consistent prefix reads), but the underlying phenomenon is worth naming here: a system that routes reads to followers is a system that will, sometimes, return stale data.

This is not a bug. It is the inherent behavior of any system that prefers read scalability over consistency. The question is whether the staleness is bounded and whether the application can tolerate it. For the catalog, conjunction predictions that read from a follower can tolerate seconds of staleness; orbital element submissions cannot tolerate any staleness on their own read path (a ground station must see its own write). The system handles this by routing the writer's subsequent reads to the leader for a session-scoped window, which is the standard read-your-writes pattern.

## Code Examples

### A Minimal Single-Leader Replicated Counter

The smallest correct shape of single-leader replication is small enough to fit in a single file. This example shows the leader/follower roles, the replication log as a `tokio::broadcast` channel, and the asynchronous-acknowledgment model.

```rust,edition2021
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
struct LogEntry {
    seq: u64,
    delta: i64,
}

struct Leader {
    state: AtomicU64,
    next_seq: AtomicU64,
    // Broadcast channel models the replication log. In production this is a
    // durable WAL on disk; the broadcast channel is a faithful concurrency
    // model of what 'followers subscribe to the log' looks like.
    log: broadcast::Sender<LogEntry>,
}

impl Leader {
    fn new() -> Arc<Self> {
        let (tx, _) = broadcast::channel(1024);
        Arc::new(Self {
            state: AtomicU64::new(0),
            next_seq: AtomicU64::new(0),
            log: tx,
        })
    }

    /// Write path. Returns immediately after local commit and log append -
    /// this is asynchronous replication. To make it synchronous, we would
    /// wait here for a quorum of followers to acknowledge a given seq.
    fn apply(&self, delta: i64) -> u64 {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        // Apply locally. The local commit must happen before the log append
        // is observable - otherwise a follower could see an entry that the
        // leader does not yet have. (In a real WAL system, the order is
        // reversed: log first, then apply, with crash-recovery replay.)
        if delta >= 0 {
            self.state.fetch_add(delta as u64, Ordering::SeqCst);
        } else {
            self.state.fetch_sub((-delta) as u64, Ordering::SeqCst);
        }
        // Best-effort broadcast - if no followers are subscribed, the entry
        // is dropped from the broadcast buffer (this is fine; we still have
        // it locally). In production the log is persisted and followers can
        // catch up by reading the persistent log.
        let _ = self.log.send(LogEntry { seq, delta });
        seq
    }

    fn read(&self) -> u64 {
        // Leader reads are linearizable: the leader has the latest value.
        // Production systems add a 'read index' check to ensure we're still
        // the leader (see Module 3).
        self.state.load(Ordering::SeqCst)
    }

    fn subscribe(&self) -> broadcast::Receiver<LogEntry> {
        self.log.subscribe()
    }
}

struct Follower {
    state: AtomicU64,
    last_applied_seq: AtomicU64,
}

impl Follower {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicU64::new(0),
            last_applied_seq: AtomicU64::new(0),
        })
    }

    async fn run(self: Arc<Self>, mut log: broadcast::Receiver<LogEntry>) {
        // Apply log entries in order. If we lag behind, the broadcast channel
        // will return Lagged, and we'd need to bootstrap from a snapshot.
        // This is the 'follower fell behind, needs reseeding' case from DDIA.
        while let Ok(entry) = log.recv().await {
            if entry.delta >= 0 {
                self.state.fetch_add(entry.delta as u64, Ordering::SeqCst);
            } else {
                self.state.fetch_sub((-entry.delta) as u64, Ordering::SeqCst);
            }
            self.last_applied_seq.store(entry.seq, Ordering::SeqCst);
        }
    }

    fn read(&self) -> u64 {
        // Follower reads may be stale by an unbounded amount, depending on
        // replication lag. The caller must tolerate this or route to leader.
        self.state.load(Ordering::SeqCst)
    }
}

#[tokio::main]
async fn main() {
    let leader = Leader::new();
    let follower = Follower::new();

    let f = follower.clone();
    let log = leader.subscribe();
    tokio::spawn(f.run(log));

    leader.apply(10);
    leader.apply(5);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    println!("leader: {}, follower: {}", leader.read(), follower.read());
    // Both should print 15. If we read the follower immediately after apply,
    // we could see 0 - that's replication lag in action.
}
```

This is small enough to reason about and big enough to demonstrate every shape that matters in production: a write path, a replication log, follower subscription, and the lag window between leader commit and follower apply. The production version replaces `broadcast::channel` with a durable WAL, replaces `AtomicU64` with a real storage engine, and adds the failover coordinator and fencing mechanism described above. The shape, however, is the same.

### A Failover Hazard: Acknowledged Writes Lost Without Synchronous Replication

This snippet captures the failure mode of asynchronous-only replication:

```rust,ignore
// SCENARIO: leader acknowledges write, crashes before propagation completes.

async fn submit_telemetry(leader: &Leader, sample: TelemetrySample) -> Result<()> {
    let seq = leader.apply_async(sample).await?;  // local commit only
    Ok(())  // We return success to the client here.
    // At this point, the write is durable on the leader's disk but the
    // followers have not yet seen it. If the leader's storage fails before
    // the WAL ships, the write is gone - even though the client got an ack.
}

// What synchronous replication adds:
async fn submit_telemetry_safe(leader: &Leader, sample: TelemetrySample) -> Result<()> {
    let seq = leader.apply_async(sample).await?;
    leader.wait_for_quorum(seq).await?;  // block until N followers ack
    Ok(())  // Now safe: the write exists on >=N+1 machines.
}
```

The two functions look almost identical and have radically different operational properties. The first is the source of "we acked the write but it's not there after failover" incidents. The second is the source of "writes are slower than they used to be after we tightened replication policy" tickets. Choose deliberately; document the choice; review it during the postmortem of the first incident that touches it.

### Read-Your-Writes via Session-Pinned Routing

The simplest read-your-writes implementation pins a session's reads to the leader for a window after each write:

```rust,edition2021
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

struct SessionRouter {
    leader_endpoint: String,
    follower_endpoints: Vec<String>,
    last_write: Mutex<Option<Instant>>,
}

impl SessionRouter {
    const STICKY_WINDOW: Duration = Duration::from_secs(10);

    fn route_read(&self) -> &str {
        let last = self.last_write.lock().unwrap();
        match *last {
            // Within the sticky window, route reads to the leader. The leader
            // is linearizable, so the writer is guaranteed to see its own
            // recent write. The cost is leader load.
            Some(t) if t.elapsed() < Self::STICKY_WINDOW => &self.leader_endpoint,
            // Outside the window, follower reads are fine. Replication lag
            // far exceeding STICKY_WINDOW is treated as a separate alert.
            _ => &self.follower_endpoints[0],
        }
    }

    fn note_write(&self) {
        *self.last_write.lock().unwrap() = Some(Instant::now());
    }
}

fn main() {
    let router = SessionRouter {
        leader_endpoint: "leader.catalog.meridian.internal".into(),
        follower_endpoints: vec!["follower-1.catalog.meridian.internal".into()],
        last_write: Mutex::new(None),
    };
    println!("initial read routes to: {}", router.route_read());
    router.note_write();
    println!("post-write read routes to: {}", router.route_read());
}
```

The choice of `STICKY_WINDOW` is operational: too short and replication lag will cause stale reads inside the window; too long and the leader becomes a read bottleneck for every active client. The window should be longer than the 99th-percentile replication lag. Monitoring should alert when actual lag approaches the window value; that alert is the early warning that the policy is about to start producing anomalies.

## Key Takeaways

- Single-leader replication is the default replication model for a reason: it gives you a linearizable single object (the leader), a clear write path, and well-understood failure modes. Almost every production system has a "single leader" tier somewhere, even when it advertises as multi-master or leaderless.
- Synchronous vs asynchronous replication is a durability/latency tradeoff. "One synchronous follower plus the leader" is the practical choice for systems that cannot lose acknowledged writes; pure asynchronous is the choice for high-volume systems where individual writes are not individually critical.
- The replication log implementation (statement-based, WAL-shipping, logical) constrains your operational story. WAL shipping requires version-compatible replicas; logical replication enables cross-version and cross-engine replication at the cost of higher traffic volume.
- Failover is the hardest operation in the system. It loses writes, can produce split brains, and depends on a failure detector whose timeout has no universally correct value. Production failover requires a consensus-based coordinator (Module 3), fencing tokens, and explicit synchronous replication of the candidate replica.
- Replication lag is not a bug; it is the price of follower-served reads. Bound the staleness with monitoring, route session-critical reads to the leader, and document the tolerance every workload expects. The next lesson (read consistency) makes this discipline concrete.

> **Source note:** This lesson is grounded in DDIA 2nd Edition (Kleppmann & Riccomini), Chapter 6, "Replication" — specifically the sections "Single-Leader Replication," "Setting Up New Followers," "Handling Node Outages," and "Implementation of Replication Logs." Specific failure stories (the catalog's two failovers, the synchronous replication policy decision) are synthesized illustrative scenarios consistent with documented industry patterns; they are not real Meridian incidents and the operational numbers (window sizes, lag thresholds) are illustrative.

{{#quiz quiz-1.toml}}
