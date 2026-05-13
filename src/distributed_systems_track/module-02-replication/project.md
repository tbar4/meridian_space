# Module 02 Project — Replicated Telemetry Store

## Mission Brief

> **Incident ticket CN-2607-014**
> **Severity:** P3
> **Reporter:** Mission Control Platform, Telemetry Group
> **Status:** Open

The telemetry pipeline writes ~10 kHz of samples to the Constellation Catalog. Reads come from three sources: the operations dashboard (operators viewing real-time state), the conjunction-prediction service (reads orbital state and recent telemetry together), and the analytics tier (bulk historical scans). The current single-instance design cannot keep up, and the team is migrating to a leader/follower configuration.

This project is the proving ground for that migration. You will build a Rust crate, `replicated_telemetry_store`, that implements:

1. **Single-leader replication** with one synchronous and N asynchronous followers.
2. **A pluggable read router** that supports three modes — `Leader`, `AnyFollower`, and `SessionConsistent` — corresponding to "linearizable reads at leader cost," "fastest reads, accept staleness," and "read-your-writes via LSN tokens."
3. **A simulated network layer** that lets tests inject replication lag, drop messages, and partition the cluster.
4. **A test suite** that demonstrates each of the three replication-lag anomalies and shows that the corresponding session guarantees eliminate them.

The deliverable does not need to be production-ready storage — an in-memory map is fine. The deliverable must be a correct demonstration that you can reason about replication lag, route reads accordingly, and prove the routing works under adversarial conditions.

## Repository Layout

```
replicated-telemetry-store/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── leader.rs           # Leader: write path, log emission, sync replication wait
│   ├── follower.rs         # Follower: log apply, lag tracking
│   ├── router.rs           # ReadRouter with the three modes
│   ├── network.rs          # Simulated bus with lag injection
│   └── lsn.rs              # Lsn type, ordering, monotonic generator
├── tests/
│   ├── read_after_write.rs
│   ├── monotonic_reads.rs
│   ├── consistent_prefix.rs
│   └── partition_durability.rs
└── README.md
```

## Required API

```rust,ignore
// lsn.rs
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Lsn(pub u64);

// leader.rs
pub struct Leader {
    // synchronous followers - writes wait for these
    // asynchronous followers - writes do not wait
}

impl Leader {
    pub fn new(node_id: String, network: Arc<Network>) -> Self;
    pub fn add_sync_follower(&self, follower_id: &str);
    pub fn add_async_follower(&self, follower_id: &str);
    pub async fn write(&self, key: String, value: Vec<u8>) -> Result<Lsn>;
    pub async fn read(&self, key: &str) -> Option<Vec<u8>>;
    pub fn current_lsn(&self) -> Lsn;
}

// follower.rs
pub struct Follower {
    // applies entries from the leader's log; tracks last applied LSN
}

impl Follower {
    pub fn new(node_id: String, network: Arc<Network>) -> Self;
    pub async fn run(&self);
    pub async fn read(&self, key: &str) -> Option<Vec<u8>>;
    pub fn current_lsn(&self) -> Lsn;
}

// router.rs
pub enum ReadMode {
    Leader,
    AnyFollower,
    SessionConsistent { session_id: String },
}

pub struct ReadRouter {
    leader: Arc<Leader>,
    followers: Vec<Arc<Follower>>,
}

impl ReadRouter {
    pub fn new(leader: Arc<Leader>, followers: Vec<Arc<Follower>>) -> Self;
    pub async fn read(&self, key: &str, mode: ReadMode) -> Option<Vec<u8>>;
    pub fn record_session_write(&self, session_id: &str, lsn: Lsn);
}

// network.rs
pub struct Network {
    // pluggable lag injection per-pair, partition control
}

impl Network {
    pub fn new() -> Self;
    pub fn inject_lag(&self, from: &str, to: &str, lag: Duration);
    pub fn partition(&self, group_a: &[&str], group_b: &[&str]);
    pub fn heal(&self);
}
```

## Acceptance Criteria

- [x] `cargo build --release` completes without warnings under `#![deny(warnings)]`.
- [x] `cargo test` passes all integration tests with zero flakes across 10 consecutive runs.
- [x] `cargo clippy -- -D warnings` produces no lints.
- [x] The leader correctly waits for synchronous-follower acknowledgment before returning from `write()`. Test: inject a 500ms lag on the sync follower path; a write call should take at least 500ms to complete.
- [x] An asynchronous-follower path does not block writes. Test: inject a 5-second lag on the async follower; a write call completes in well under 5 seconds.
- [x] The `Leader` read mode is linearizable: under any lag injection, a read immediately after a write sees the new value.
- [x] The `AnyFollower` read mode demonstrably exhibits read-after-write violations under injected lag. Test: write a value, immediately read with `AnyFollower`; the test asserts that *sometimes* the read returns the previous value when lag is injected.
- [x] The `SessionConsistent` read mode eliminates read-after-write violations. Test: under the same injected lag, a session that writes-then-reads always sees the new value.
- [x] The `SessionConsistent` read mode falls back to the leader when no follower is caught up. Test: with 5-second lag on all followers, session-consistent reads in the first second land on the leader.
- [x] Monotonic reads test: with two followers at different lag levels, repeated reads from an `AnyFollower` mode can demonstrably regress (the test detects the regression). With `SessionConsistent` mode, no regression is observed across 1000 successive reads.
- [x] Partition durability test: with one synchronous follower and one async follower, after partitioning the leader from the async follower and then crashing the leader, the synchronous follower can be promoted with no acknowledged writes lost. The async follower's missing writes are recovered when it rejoins.
- [ ] *(self-assessed)* The README explains the three read modes clearly enough that a new engineer could choose the right mode for a workload they describe.
- [ ] *(self-assessed)* The lag-injection tests are deterministic — running them 100 times produces consistent results, with violations appearing under `AnyFollower` and not under `SessionConsistent` every single time.
- [ ] *(self-assessed)* The code path for synchronous-follower acknowledgment is straightforward to extend to a quorum (e.g., "wait for 2 of 3 followers"). The current `add_sync_follower` API does not preclude this generalization.

## Expected Output

`cargo test --release read_after_write -- --nocapture`:

```text
[setup] leader + 1 sync follower + 2 async followers
[setup] injecting 200ms lag on async-follower-1
[setup] injecting 200ms lag on async-follower-2

[any-follower] write 'MSS-23/state' = 'active' returned in 5ms (sync only)
[any-follower] immediate read returned: None
[any-follower] re-read after 250ms returned: Some('active')
PASS: AnyFollower exhibits read-after-write anomaly under lag

[session-consistent] write 'MSS-23/state' = 'active' returned in 5ms
[session-consistent] immediate read returned: Some('active') from leader (no follower caught up)
[session-consistent] re-read after 250ms returned: Some('active') from async-follower-1
PASS: SessionConsistent eliminates read-after-write anomaly
```

## Hints

<details>
<summary>1. Modeling the synchronous-follower wait</summary>

The leader's `write()` needs to: (1) append to its local state, (2) generate the LSN, (3) dispatch to all followers via the network, (4) wait for synchronous followers' acks, (5) return. The "wait for acks" part is the structural new piece. A `tokio::sync::oneshot` per-write, completed by the sync-follower path on ack, is the cleanest way to model this. The async-follower path completes the same oneshot but the leader's write does not wait for it.

</details>

<details>
<summary>2. Simulating lag without sleeping in tests</summary>

Real `tokio::time::sleep` calls work but make tests slow. Consider `tokio::time::pause()` and `tokio::time::advance()` in tests: you can advance virtual time without actually waiting, which makes a "500ms lag" test complete in microseconds. The lag-injection module should call `tokio::time::sleep`, which respects the virtual-time pause.

</details>

<details>
<summary>3. Testing for "sometimes" anomalies</summary>

The `AnyFollower` mode produces read-after-write violations *sometimes* — depending on which follower is chosen and when. To make the test deterministic, control the follower selection (e.g., a `select_follower` hook that the test can override) and force selection of the lagging follower. The test asserts that selecting a known-lagging follower produces the anomaly; this is far more robust than "run it 1000 times and hope to observe a violation."

</details>

<details>
<summary>4. LSN ordering and session tracking</summary>

The router's `SessionConsistent` mode needs to track each session's last-observed LSN. A `HashMap<String, Lsn>` behind a `Mutex` is the simplest implementation. On `write()`, the router records the returned LSN against the session. On `read()`, the router compares each follower's `current_lsn()` against the session's required LSN. Falling back to the leader when no follower satisfies the requirement is correctness-preserving but costs leader load — that's the documented tradeoff.

</details>

<details>
<summary>5. Partition durability</summary>

For the durability test: have the leader configured with one sync follower and one async follower. Inject lag on the async path. Write 100 entries. Verify the sync follower has all 100; the async follower has < 100. Partition the leader (no further writes can complete). "Crash" the leader by setting its state to a special `failed` flag. Promote the sync follower. Heal the partition. Verify the async follower catches up to the sync follower's state. The test asserts no acknowledged writes were lost.

</details>

<details>
<summary>6. Why the AnyFollower mode is intentionally bad</summary>

It's tempting to make `AnyFollower` smarter — e.g., to fall back to the leader when no follower has the latest LSN. Don't. The point of `AnyFollower` is to model the "fast, possibly stale" mode that real production systems offer for non-critical reads. Making it smarter erases the educational distinction between modes. If you want a hybrid mode, make it a fourth option (`BestEffortFresh` or similar), not a modification of `AnyFollower`.

</details>

## Source Anchors

- DDIA 2nd Edition, Chapter 6 — "Single-Leader Replication," "Problems with Replication Lag"
- Terry, Demers, Petersen, Spreitzer, Theimer, Welch, "Session Guarantees for Weakly Consistent Replicated Data" (PDIS 1994) — the canonical reference for session guarantees
- PostgreSQL streaming replication documentation — for an example of how synchronous and asynchronous followers are configured in a real system
