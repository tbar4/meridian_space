# Lesson 3: Read Consistency and Replication Lag

## Context

Two weeks after enabling follower-served reads in the catalog, the on-call queue acquired three new categories of ticket. *"I submitted a TLE update; the next read returned the old value, but the read after that returned the new one."* *"My terminal showed the satellite as 'active' and then thirty seconds later showed it as 'pending' — it went backward in time."* *"Operator A says the launch is approved; operator B at a different ground station says it isn't. They're both looking at the same satellite record."* All three are correct observations of catalog state. None of them are bugs in the catalog code. All of them are manifestations of **replication lag** producing observable anomalies that the original single-instance catalog could not produce.

Replication lag is not eliminable in any system that serves reads from asynchronous followers; the question is which specific anomalies the application can tolerate and which it cannot. DDIA names three: **read-after-write**, **monotonic reads**, and **consistent prefix reads**. Each describes a class of anomaly, and each has a corresponding "session guarantee" that an application can request — typically at the cost of routing some reads to the leader or pinning a session to a specific replica.

This lesson is the operational counterpart to Lessons 1 and 2. The previous lessons covered *how* replication works (single-leader, multi-leader, leaderless). This one covers *what your users will see when it doesn't work the way they expected*, and what session guarantees you can offer to bound the surprise. By the end, you should be able to diagnose a replication-lag anomaly from a user report alone, and to choose the right session guarantee for each workload in the catalog.

## Core Concepts

### Read-After-Write Consistency (a.k.a. Reading Your Own Writes)

The most common anomaly: a user submits a write, immediately reads the same data, and sees the *previous* value rather than the one they just wrote. The cause is that the write went to the leader and the read went to a follower that has not yet applied the write.

The user-facing symptom is profound confusion. The submission UI shows "save successful" and the next page load shows the old data. Users assume the system has lost their write. Even when the data is correctly written and propagation will complete in milliseconds, the experience makes the system feel broken.

**Read-your-writes** consistency is the guarantee that a client never sees the system in a state older than its own most recent write. It is a per-client guarantee: client A may still see stale data that client B just wrote, but no client sees its own writes regress.

The standard implementation strategies, in order of operational simplicity:

1. **Pin the client's reads to the leader for a window after a write.** Simplest, most reliable, costs leader read load. The catalog's session-pinning router from Lesson 1 implements this.
2. **Track the write's log position on the client and route reads to a replica that has caught up.** The client carries a "last write LSN" token; reads include the token, and any replica behind that LSN forwards the read elsewhere or waits. PostgreSQL exposes this via `pg_last_wal_replay_lsn()`; the application binds it to the user's session.
3. **Track the write's log position server-side, indexed by user.** A coordinator service maps each user to their last write position and routes their reads accordingly. More moving parts; supports stateless clients.

The catalog uses option 1 for ground-station write sessions. The orbital catalog's analytics tier — where the workload is mostly reads, and the writes are bulk imports done by a separate service — does not implement read-your-writes at all, because the analytics user never reads back their own write.

The case where read-your-writes is **not enough**: cross-device sessions. A user submits a write from their phone and reads from their laptop; the laptop has no session token tying it to the recent write. The standard answer is to use the user's account identity rather than the device session, but this requires server-side state tied to identity, which is a significantly more elaborate mechanism than session-token pinning.

### Monotonic Reads

The user reads a record and sees value X. They reload. They see value W, where W is *older* than X. The clock has gone backward — not in real time, but in their observable view of system state.

The cause: the first read landed on a follower close to the leader's LSN; the second read landed on a different follower that was further behind. The catalog's state is monotonic from the leader's perspective, but the follower-routed reads can hop between replicas with different lag profiles, producing an observable regression.

**Monotonic reads** is the guarantee that within a single client session, successive reads never see system state go backward in time. The standard implementation is **read-from-the-same-replica**: pin the client's reads (within a session) to a single follower. The follower may still lag behind the leader, but it cannot go backward. The cost is that pinning reduces load distribution — a single follower may become hot.

A subtler implementation: the client tracks the leader LSN it has observed and refuses reads from replicas behind that LSN. This is the same token-based mechanism as read-your-writes but tracking observed reads rather than just observed writes. Both can be implemented together with a unified "session position token."

### Consistent Prefix Reads

The third anomaly is the most subtle and the easiest to miss in testing. The system contains causally related writes — a question, then an answer; a satellite launch, then a status update — and reads from a replica that has the answer but not the question, or the status update but not the launch. The user sees the second event before the first, which is logically incoherent.

**Consistent prefix reads** is the guarantee that if a sequence of writes happens in a particular order, then any reader sees them in the same order (or doesn't see the later ones yet). It is fundamentally a guarantee about **causality preservation** across replication.

In a single-leader system, this is largely automatic for a single client because the leader emits writes in order and followers apply them in order. The anomaly appears when:

- **Sharded systems** route writes for different keys to different leaders. Causal dependencies that span shards are not preserved by the shard-level ordering. The fix is to track causal dependencies explicitly (e.g., Spanner's TrueTime-based commit timestamps) or to use a single global ordering (which is what consensus protocols give you).
- **Multi-leader systems** with all-to-all topology propagate writes from different leaders along different paths. Without a causal-ordering mechanism (vector clocks!), the order in which a third leader observes the writes may not match the order they were applied.
- **Followers serving reads from different parts of the log.** If a client reads from one follower for key A and another for key B, the per-follower ordering is preserved but the cross-key ordering is not.

The catalog's solution for the multi-leader regime is causal consistency tracking with vector clocks (the mechanism from Module 1 and Lesson 2). Every read carries the vector clock of the reader's session; replicas refuse to serve reads that would violate the reader's observed causal order. This is more sophisticated than read-your-writes or monotonic reads, and it is the right tool when ordering across keys matters operationally.

### Session Guarantees: The Composable Layer

Read-your-writes, monotonic reads, and consistent prefix reads are three of a set of **session guarantees** originally formalized by Terry et al. in the 1994 Bayou paper. The full set is:

1. **Read-your-writes** — a client sees its own writes.
2. **Monotonic reads** — a client doesn't see state regress.
3. **Monotonic writes** — a client's writes are applied in the order they were submitted.
4. **Writes-follow-reads** — if a client read X and then wrote Y, any reader that sees Y also sees X.

These can be offered independently or together. A system that offers all four is providing **causal consistency** at the session level, which is the strongest model achievable without coordination across writers. Strong consistency models like linearizability provide all four plus cross-client guarantees, at the cost of coordination.

The operational pattern: treat session guarantees as a **menu** offered to the application, not a property of the entire database. The catalog's TLE update endpoint requests read-your-writes (writers see their own submissions). The analytics dashboard requests monotonic reads (operators don't see time-traveling state). The conjunction-prediction service requests consistent prefix reads (cause precedes effect). The same underlying database supports all three by varying the routing policy per session.

### Bounded Staleness as an Operational Knob

Independent of session guarantees, the system can offer **bounded staleness**: a guarantee that any read from a follower is no more than N seconds (or N log entries) behind the leader. This is what Azure Cosmos DB calls "bounded staleness consistency" and what Cassandra implements via the consistency level `LOCAL_QUORUM` plus monitoring of replication lag.

Bounded staleness is operationally useful because it gives the application a number to reason about. "Reads may be up to 5 seconds stale" is something a dashboard can communicate to its users; "reads may be arbitrarily stale" is not. The cost is that the system must reject reads from replicas exceeding the staleness bound, which under prolonged replication lag manifests as availability degradation rather than correctness violation. This is a deliberate tradeoff: surface the lag as an alert rather than hide it behind silently stale data.

The catalog publishes a staleness metric: every replica reports its current lag to a metrics pipeline, and any replica exceeding 1 second of lag is removed from the read-routing pool. The pool re-includes the replica once it catches up. The application sees consistent low-lag reads even when individual replicas are temporarily lagging; the cost is fewer replicas during catch-up periods, which is an availability tradeoff the team has accepted.

## Code Examples

### A Session-Token-Based Read Router

This implementation routes reads based on a session token that tracks the highest write LSN the session has observed:

```rust,edition2021
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Lsn(u64);

pub struct ReplicaInfo {
    pub endpoint: String,
    pub current_lsn: Lsn,
}

pub struct ReadRouter {
    leader: String,
    replicas: Mutex<Vec<ReplicaInfo>>,
    // Per-session: the LSN the session has observed. Any read from this
    // session must go to a replica with current_lsn >= session_lsn.
    sessions: Mutex<HashMap<String, Lsn>>,
}

impl ReadRouter {
    pub fn new(leader: String, replicas: Vec<ReplicaInfo>) -> Self {
        Self {
            leader,
            replicas: Mutex::new(replicas),
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Called after a successful write. Records the LSN the leader assigned
    /// so subsequent reads in this session can wait for the followers to
    /// catch up.
    pub fn record_write(&self, session: &str, lsn: Lsn) {
        let mut s = self.sessions.lock().unwrap();
        let entry = s.entry(session.to_string()).or_insert(Lsn(0));
        if lsn > *entry { *entry = lsn; }
    }

    /// Called on read. Returns the endpoint of a replica that satisfies the
    /// session's monotonicity requirement, or the leader if no replica is
    /// sufficiently caught up.
    pub fn route(&self, session: &str) -> String {
        let sessions = self.sessions.lock().unwrap();
        let required = sessions.get(session).copied().unwrap_or(Lsn(0));
        let replicas = self.replicas.lock().unwrap();

        // Prefer the freshest replica that has reached `required`. Falling
        // back to the leader guarantees read-your-writes; the cost is leader
        // load when followers lag.
        replicas
            .iter()
            .filter(|r| r.current_lsn >= required)
            .max_by_key(|r| r.current_lsn)
            .map(|r| r.endpoint.clone())
            .unwrap_or_else(|| self.leader.clone())
    }

    /// Background task updates this from heartbeats; in production this would
    /// poll each replica's pg_last_wal_replay_lsn() equivalent.
    pub fn update_replica_lsn(&self, endpoint: &str, lsn: Lsn) {
        let mut replicas = self.replicas.lock().unwrap();
        if let Some(r) = replicas.iter_mut().find(|r| r.endpoint == endpoint) {
            r.current_lsn = lsn;
        }
    }
}

fn main() {
    let router = ReadRouter::new(
        "leader.catalog".into(),
        vec![
            ReplicaInfo { endpoint: "follower-1".into(), current_lsn: Lsn(100) },
            ReplicaInfo { endpoint: "follower-2".into(), current_lsn: Lsn(95) },
        ],
    );

    // Initial read - no writes yet, any follower works
    println!("initial: {}", router.route("session-A"));

    // Write happens, LSN advances to 110
    router.record_write("session-A", Lsn(110));
    // No follower has caught up to 110 yet - route to leader
    println!("post-write: {}", router.route("session-A"));

    // Follower-1 catches up
    router.update_replica_lsn("follower-1", Lsn(115));
    // Now follower-1 satisfies the session requirement
    println!("after catch-up: {}", router.route("session-A"));
}
```

The key property is that **the session token, not the read endpoint, is what carries the guarantee**. A read from session A and a read from session B can land on different replicas; each is consistent with its own session's history. The replicas don't need to know about session ordering; they just expose their current LSN, and the router picks accordingly.

### Detecting a Consistent-Prefix Violation in Practice

For systems that need consistent prefix reads across shards or replicas, the test harness needs to detect violations. This is a sketch of what such a test looks like:

```rust,ignore
// Test scenario: a satellite launches (writes 'state=active' to key MSS-23)
// and then immediately reports telemetry (writes a telemetry row keyed by
// MSS-23). A reader following the convoy must see the launch before the
// telemetry, even if the two writes go to different shards.

use anyhow::Result;
use std::time::Duration;

async fn assert_consistent_prefix(client: &CatalogClient) -> Result<()> {
    // Write 1: satellite state transition (shard A)
    client.put("satellites/MSS-23/state", "active").await?;

    // Write 2: first telemetry sample (shard B, references MSS-23)
    client.put("telemetry/MSS-23/0001", r#"{"alt": 550}"#).await?;

    // A subsequent reader should not be able to see the telemetry without
    // also seeing the state transition. We check by reading both and
    // asserting either (a) we see state=active AND telemetry, or (b) we see
    // neither (we caught the window before propagation), but never (c)
    // telemetry without state, which would be a consistent-prefix violation.

    for attempt in 0..20 {
        let state = client.get("satellites/MSS-23/state").await?;
        let telemetry = client.get("telemetry/MSS-23/0001").await?;

        match (state, telemetry) {
            (Some(_), None) => return Ok(()),  // both writes visible (a)
            (None, None) => {
                // Propagation in progress; retry. Caps at 20 attempts.
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            (None, Some(_)) => {
                // VIOLATION: saw effect without cause.
                anyhow::bail!("consistent-prefix violation: telemetry visible before state");
            }
            (Some(state), Some(_)) if state == "active" => return Ok(()),
            _ => anyhow::bail!("unexpected state combination"),
        }
    }
    anyhow::bail!("writes never converged");
}
# struct CatalogClient;
# impl CatalogClient {
#     async fn put(&self, _k: &str, _v: &str) -> Result<()> { Ok(()) }
#     async fn get(&self, _k: &str) -> Result<Option<String>> { Ok(None) }
# }
```

The test is intentionally about *what should never be observed*. A passing run does not prove the system is consistent — it proves no violation was observed this time. The way to gain confidence is to run the test under partition injection, replication lag, and adversarial scheduling, and accumulate enough successful runs to bound the violation probability. This is what Jepsen-style testing automates.

### Bounded Staleness Enforcement on the Read Path

The router from earlier can be extended to enforce a maximum staleness:

```rust,edition2021
use std::time::{Duration, Instant};

struct StalenessAwareReplica {
    endpoint: String,
    last_seen_leader_lsn: u64,
    last_seen_at: Instant,
}

impl StalenessAwareReplica {
    fn is_within_bound(&self, max_lag: Duration) -> bool {
        // The replica is considered within bound if its last heartbeat is
        // recent. A stale heartbeat means we don't know how far behind it is,
        // and the conservative choice is to remove it from the pool.
        self.last_seen_at.elapsed() < max_lag
    }
}

struct BoundedStalenessRouter {
    replicas: Vec<StalenessAwareReplica>,
    leader: String,
    max_staleness: Duration,
}

impl BoundedStalenessRouter {
    fn route(&self) -> &str {
        for r in &self.replicas {
            if r.is_within_bound(self.max_staleness) {
                return &r.endpoint;
            }
        }
        // No replica is within the staleness bound. Fall back to the leader.
        // In some configurations the right behavior is to return an error
        // (preserve correctness over availability); here we fall back, which
        // is the right call when the leader is the still-current source.
        &self.leader
    }
}

fn main() {
    let router = BoundedStalenessRouter {
        replicas: vec![
            StalenessAwareReplica {
                endpoint: "follower-1".into(),
                last_seen_leader_lsn: 100,
                last_seen_at: Instant::now(),
            },
        ],
        leader: "leader".into(),
        max_staleness: Duration::from_secs(5),
    };
    println!("routing to: {}", router.route());
}
```

This is the staleness equivalent of a circuit breaker (Module 4): if a replica's freshness signal goes stale, it is removed from the pool until it recovers. The cost is reduced read capacity; the benefit is that the application sees only reads that are within the documented staleness bound.

## Key Takeaways

- Replication lag produces three distinct user-visible anomalies: read-after-write (a writer doesn't see their own write), monotonic reads (state appears to regress), and consistent prefix reads (cause appears after effect). Each has a corresponding session guarantee.
- Read-your-writes is the most commonly needed guarantee. The cheapest implementation is to pin the writer's reads to the leader for a window after each write. The more sophisticated implementations track the leader LSN per session and route to any replica caught up to that LSN.
- Monotonic reads is provided by pinning a session's reads to a single replica, accepting that replica's lag profile in exchange for never going backward in observable state.
- Consistent prefix reads requires causal-order tracking across writes — typically vector clocks (Module 1) or commit timestamps from a coordinated time source like Spanner's TrueTime. This is the guarantee that fails most often in sharded systems where different keys live on different shards.
- Bounded staleness is the operational tool for making lag tolerable: surface lag as a metric, remove lagging replicas from the read pool, alert when no replica is within bound. Replication lag is not a bug; unbounded staleness is.

> **Source note:** This lesson is grounded in DDIA 2nd Edition (Kleppmann & Riccomini), Chapter 6, "Problems with Replication Lag" — specifically the subsections "Reading Your Own Writes," "Monotonic Reads," and "Consistent Prefix Reads." The original session-guarantee taxonomy is from Terry, Demers, Petersen, Spreitzer, Theimer, Welch, "Session Guarantees for Weakly Consistent Replicated Data" (PDIS 1994). Specific implementation details (PostgreSQL's pg_last_wal_replay_lsn, Cosmos DB's bounded staleness) are illustrative and should be verified against current product documentation.

{{#quiz quiz-3.toml}}
