# Module 03 Project — Orbital Raft

## Mission Brief

> **Incident ticket CN-2611-009**
> **Severity:** P1 (track-defining)
> **Reporter:** Constellation Operations, Antarctic Watch
> **Status:** Open

The Antarctic relay path has a 14-minute coverage gap during which Antarctic ground stations lose contact with the rest of the Constellation Network. The catalog's leader-election runbook calls for a human operator to declare promotion during a leader outage. During the November storm, this took 47 minutes — the Antarctic team was offline for the duration of the gap, the next operator on call did not see the page until 06:32 local, and by then a partial split-brain had developed because the Antarctic-side replica had continued accepting writes without quorum.

The fix is to automate leader election with a consensus protocol. You are implementing **Orbital Raft** — a Raft consensus library, in Rust, designed to handle the constellation's specific operational regime: 5-node clusters, intermittent partitions, and recovery without human intervention.

This is the most significant project in the track. The deliverable is a Rust crate, `orbital_raft`, that implements:

1. The Raft state machine (Follower / Candidate / Leader transitions).
2. The Raft RPCs (RequestVote, AppendEntries, InstallSnapshot).
3. Persistent state (current_term, voted_for, log) flushed to disk before any RPC response.
4. Single-server membership changes (add/remove one node at a time).
5. A test harness that injects partitions, drops messages, and verifies safety and liveness properties.

The bar is **correctness under adversarial conditions**, not performance. Production Raft is its own subspecialty; this project demonstrates that you understand the protocol well enough to operate one.

## Repository Layout

```
orbital-raft/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── state.rs            # RaftState, term, role, log
│   ├── rpc.rs              # RequestVote, AppendEntries, InstallSnapshot types
│   ├── node.rs             # RaftNode: the state machine driver
│   ├── transport.rs        # Network abstraction (real or simulated)
│   ├── storage.rs          # Persistent state I/O (fsync-backed file or sled)
│   ├── membership.rs       # Single-server config changes
│   └── snapshot.rs         # Snapshot trigger logic and InstallSnapshot
├── tests/
│   ├── leader_election.rs
│   ├── log_replication.rs
│   ├── partition_safety.rs
│   ├── membership_change.rs
│   └── snapshot_install.rs
└── README.md
```

## Required API

```rust,ignore
// node.rs
pub struct RaftNode<SM: StateMachine> {
    // owns the RaftState, drives election timeouts, dispatches RPCs
}

pub trait StateMachine: Send + Sync {
    fn apply(&self, command: &[u8]) -> Vec<u8>;
    fn snapshot(&self) -> Vec<u8>;
    fn restore(&self, snapshot: &[u8]);
}

impl<SM: StateMachine> RaftNode<SM> {
    pub fn new(
        node_id: String,
        peers: Vec<String>,
        transport: Arc<dyn Transport>,
        storage: Arc<dyn Storage>,
        state_machine: Arc<SM>,
    ) -> Self;

    pub async fn run(&self);
    pub async fn submit(&self, command: Vec<u8>) -> Result<Vec<u8>>;
    pub async fn change_membership(&self, change: ConfigChange) -> Result<()>;
    pub fn is_leader(&self) -> bool;
}

// transport.rs
#[async_trait]
pub trait Transport: Send + Sync {
    async fn request_vote(&self, target: &str, args: RequestVoteArgs) -> Result<RequestVoteReply>;
    async fn append_entries(&self, target: &str, args: AppendEntriesArgs) -> Result<AppendEntriesReply>;
    async fn install_snapshot(&self, target: &str, args: InstallSnapshotArgs) -> Result<InstallSnapshotReply>;
}

// storage.rs
#[async_trait]
pub trait Storage: Send + Sync {
    async fn save_state(&self, term: u64, voted_for: Option<String>) -> Result<()>;
    async fn load_state(&self) -> Result<(u64, Option<String>)>;
    async fn append_log(&self, entries: &[LogEntry]) -> Result<()>;
    async fn load_log(&self) -> Result<Vec<LogEntry>>;
    async fn truncate_log_after(&self, index: u64) -> Result<()>;
    async fn save_snapshot(&self, snapshot: &Snapshot) -> Result<()>;
    async fn load_snapshot(&self) -> Result<Option<Snapshot>>;
}
```

## Acceptance Criteria

- [x] `cargo build --release` completes without warnings under `#![deny(warnings)]`.
- [x] `cargo test --release` passes all integration tests with zero flakes across 50 consecutive runs.
- [x] `cargo clippy -- -D warnings` produces no lints.
- [x] **Leader election test:** start a 5-node cluster, observe that exactly one node becomes leader within 2 election timeouts. After killing the leader, another node becomes leader within 2 election timeouts.
- [x] **Single-leader-per-term test:** across 1000 randomly-seeded test runs, no term ever has two leaders simultaneously.
- [x] **Log replication test:** submit 1000 commands to the leader; verify that all five nodes eventually have identical logs after waiting for replication.
- [x] **Commit-only-on-majority test:** under a partition where the leader is on the minority side, no command submitted to that leader is reported as committed.
- [x] **Election restriction test:** construct a scenario where one node has a longer log but stale terms; verify the cluster does not elect that node leader (the more-up-to-date logs win).
- [x] **Persistence test:** crash all 5 nodes; restart them; verify that the cluster recovers and that no committed entry is lost. (Implementation: simulated crash via dropping the in-memory node state while preserving the on-disk storage.)
- [x] **Membership change test:** start a 3-node cluster; add a 4th node; remove the original first node; verify the cluster maintains availability throughout and the resulting 3-node cluster (nodes 2, 3, 4) is correctly configured.
- [x] **Snapshot test:** configure aggressive snapshot triggers; submit enough commands to trigger snapshotting; bring up a new follower; verify the new follower receives an InstallSnapshot RPC, applies it, and catches up to current state.
- [x] **Partition recovery test:** partition a 5-node cluster into 3 and 2; submit commands to the majority side; heal the partition; verify the minority side's nodes catch up and no committed entry is lost.
- [ ] *(self-assessed)* The code is structured so that the state machine, transport, and storage are independently swappable. Tests verify this by using an in-memory transport and storage that differ from any real-world implementation.
- [ ] *(self-assessed)* The README explains, in plain prose, what guarantees the implementation provides and what it does NOT provide (no linearizable reads via lease, no joint consensus for arbitrary membership changes, etc.). A reader should understand the scope and limitations after one pass.
- [ ] *(self-assessed)* Persistence ordering is correct: any state that must be durable before a response is observably fsynced. A code reviewer should be able to confirm this by inspecting `save_state` and `append_log` and following their callers.

## Expected Output

`cargo test --release leader_election -- --nocapture`:

```text
[t=0.000s] cluster=[A,B,C,D,E] started
[t=0.000s] A: follower, term=0
[t=0.000s] B: follower, term=0
[t=0.150s] D: election timeout, becoming candidate (term=1)
[t=0.155s] D: requested votes from {A,B,C,E}
[t=0.158s] A: vote granted to D (term=1)
[t=0.159s] B: vote granted to D (term=1)
[t=0.162s] C: vote granted to D (term=1)
[t=0.165s] D: received majority votes, becoming leader (term=1)
[t=0.165s] D: sending initial heartbeats
PASS: cluster elected D as leader in term 1 within 2 election timeouts

[t=2.000s] D crashed
[t=2.150s] A: election timeout, becoming candidate (term=2)
[t=2.155s] A: requested votes from {B,C,E}
[t=2.160s] A: received majority votes, becoming leader (term=2)
PASS: cluster re-elected A as leader in term 2 after D's failure
```

## Hints

<details>
<summary>1. Structure the state machine as an event loop</summary>

The cleanest Raft implementation is a single async loop per node that processes events: incoming RPC, election timeout, heartbeat timeout, new client command. A `tokio::select!` over channels for each event type makes the state transitions easy to follow. The loop's body is a giant match on `self.role` and the event kind. Avoid spreading the state machine across many threads — single-loop is correct by construction; multi-threaded is correct only if you're disciplined about locking.

</details>

<details>
<summary>2. Persist BEFORE responding to RPCs</summary>

Raft's safety relies on persistent state being durable before any RPC response that depends on it. In `RequestVote`: persist `voted_for` before returning the reply. In `AppendEntries`: persist log entries before acknowledging. The cost is a disk write per RPC, which in production is mitigated by batching. For this project, an unbatched fsync per RPC is acceptable. Pay attention to the failure mode of "responded but didn't persist" — it manifests as double-voting on crash recovery, which is a safety violation.

</details>

<details>
<summary>3. Testing safety vs liveness separately</summary>

Safety tests assert "no two leaders in same term," "no committed entry lost," "log matching property holds." These tests should pass under any scheduling, any message order, any failure injection. Liveness tests assert "cluster elects a leader within bounded time," "submitted commands eventually commit." These tests require some synchrony — they will fail if you inject permanent message loss. Structure the test suite so that safety tests run under chaotic injection and liveness tests run under bounded injection.

</details>

<details>
<summary>4. Simulating crashes</summary>

A "crash" in tests is implemented as dropping the `RaftNode` while preserving its `Storage`. On restart, construct a new `RaftNode` pointing at the same `Storage` and verify it recovers the term, voted_for, and log. This catches the persistence-correctness bugs that are hardest to find by inspection — particularly the case where you persisted `current_term` but not `voted_for`, allowing a node to vote twice in the same term across a crash.

</details>

<details>
<summary>5. The election restriction test scenario</summary>

To exercise the election restriction: create a 3-node cluster, partition node C from A and B, advance terms on A,B by triggering several elections among them while C is isolated, then submit some commands so A,B have committed entries in term 2 that C does not have. C's log is shorter (no term-2 entries) — even if C's stale term-1 last index is high, the election restriction says A,B should refuse to vote for C because A,B's last log term (2) is higher than C's (1). Verify that healing the partition results in C becoming follower, not leader.

</details>

<details>
<summary>6. Snapshot test pitfalls</summary>

InstallSnapshot is subtle. The receiver must: (a) clear its log up to last_included_index, (b) restore the state machine from the snapshot, (c) set last_applied to last_included_index, (d) be ready to receive subsequent AppendEntries starting from last_included_index + 1. A common bug is to forget step (a) and end up with stale log entries that conflict with the new snapshot. Verify the test asserts the receiver's log is exactly the entries after the snapshot point, not the entries plus the pre-snapshot remnants.

</details>

## Source Anchors

- DDIA 2nd Edition, Chapter 10 — "Consensus," "Consensus in Practice," "Membership and Coordination Services"
- Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm (Extended Version)" (USENIX ATC 2014) — the Raft paper; this project's primary reference
- Ongaro, "Consensus: Bridging Theory and Practice" (Stanford PhD dissertation, 2014) — the deep reference for membership changes, snapshots, and operational concerns
- The etcd Raft library (github.com/etcd-io/raft) — a high-quality production reference implementation; useful for comparing design choices
