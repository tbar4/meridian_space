# Module 01 Project — Constellation Clock Sync

## Mission Brief

> **Incident ticket CN-2604-001**
> **Severity:** P2
> **Reporter:** Constellation Operations, Pacific Watch
> **Status:** Open

The MSS-17 attitude-update incident (Lesson 2, opening context) has been triaged. Root cause is confirmed as **wall-clock-based ordering** in the catalog write path: ground stations submit updates tagged with `SystemTime::now()`, and the catalog resolves conflicts by picking the larger timestamp. Pacific NTP drift introduced a 600 ms forward skew that caused a stale Pacific update to clobber a newer Indian Ocean update.

The fix is a clock-synchronization layer that does **not** depend on wall-clock agreement across ground stations. You are building it.

The deliverable is a Rust library, `constellation_clock`, that simulates a four-node cluster (Pacific, Indian Ocean, Atlantic, Antarctic) exchanging messages under injected partitions and delivers a working implementation of:

1. A **Lamport logical clock** for total ordering of events.
2. A **vector clock** for concurrency detection.
3. A **simulated message bus** with controllable partitions and message reordering.
4. A **test harness** that verifies causal ordering is preserved under adversarial network conditions.

This project does not implement physical clock synchronization (PTP, NTP). The point is to show that you can build correct distributed ordering **without** physical clock agreement.

## Repository Layout

```
constellation-clock/
├── Cargo.toml
├── src/
│   ├── lib.rs              # Public API
│   ├── lamport.rs          # LamportClock type
│   ├── vector.rs           # VectorClock type
│   ├── bus.rs              # Simulated network with partition injection
│   └── node.rs             # ClusterNode that uses both clocks
├── tests/
│   ├── lamport_ordering.rs
│   ├── vector_concurrency.rs
│   └── partition_recovery.rs
└── README.md
```

## Required API

```rust,ignore
// lamport.rs
pub struct LamportClock { /* ... */ }
impl LamportClock {
    pub fn new() -> Self;
    pub fn tick(&self) -> u64;
    pub fn observe(&self, incoming: u64) -> u64;
    pub fn current(&self) -> u64;
}

// vector.rs
pub struct VectorClock { /* ... */ }
impl VectorClock {
    pub fn new(node_id: &str) -> Self;
    pub fn tick(&mut self);
    pub fn merge(&mut self, other: &VectorClock);
    pub fn compare(&self, other: &VectorClock) -> Ordering;
    pub fn snapshot(&self) -> HashMap<String, u64>;
}

pub enum Ordering { Before, After, Equal, Concurrent }

// bus.rs
pub struct MessageBus { /* ... */ }
impl MessageBus {
    pub fn new(node_ids: Vec<String>) -> Self;
    pub fn partition(&mut self, group_a: &[&str], group_b: &[&str]);
    pub fn heal(&mut self);
    pub async fn send(&self, from: &str, to: &str, payload: Vec<u8>) -> Result<()>;
    pub async fn recv(&self, node: &str) -> Option<Message>;
}

// node.rs
pub struct ClusterNode { /* ... */ }
impl ClusterNode {
    pub fn new(id: String, bus: Arc<MessageBus>) -> Self;
    pub async fn submit(&mut self, payload: Vec<u8>) -> Event;
    pub async fn run(&mut self);  // background task processing incoming messages
}

pub struct Event {
    pub origin: String,
    pub lamport: u64,
    pub vector: HashMap<String, u64>,
    pub payload: Vec<u8>,
}
```

## Acceptance Criteria

The project is complete when the following checks pass. Verifiable criteria are checked by the test harness; self-assessed items require the engineer's judgment because they involve subjective design decisions.

- [x] `cargo build --release` completes without warnings under `#![deny(warnings)]`.
- [x] `cargo test` passes all integration tests with zero flakes across 10 consecutive runs.
- [x] `cargo clippy -- -D warnings` produces no lints.
- [x] The Lamport clock implementation passes the **monotonic causality test**: for any sequence of `tick()` and `observe()` calls, the returned values strictly increase, and `observe(n)` always returns a value strictly greater than both the current local value and `n`.
- [x] The vector clock implementation passes the **concurrency detection test**: given two clocks `A` and `B` produced by independent tick sequences from a shared starting state, `compare(A, B)` returns `Concurrent`.
- [x] The vector clock implementation passes the **causal precedence test**: if node X sends to node Y and Y receives, then Y's clock after merge dominates the snapshot of X's clock at send time.
- [x] The simulated message bus correctly partitions: after `partition(group_a, group_b)`, messages sent from a node in `group_a` to a node in `group_b` are buffered until `heal()` is called.
- [x] Under a 4-node test where Pacific and Indian Ocean issue concurrent updates during an Antarctic-relay partition, the catalog (modeled as the receiving node's reconcile logic) correctly identifies the two updates as concurrent rather than picking one based on arrival order.
- [x] The recovery test passes: after a 5-second partition with 20 events on each side, healing the partition causes all nodes to converge to the same set of events (no message loss).
- [ ] *(self-assessed)* The code is structured so that swapping the clock implementation (e.g., to a hybrid logical clock) would not require changes to `ClusterNode` or `MessageBus`. The clock is an injected dependency, not a hardcoded type.
- [ ] *(self-assessed)* The README explains the model assumptions clearly enough that another engineer joining the team could read it once and explain to a third party why the system does not depend on NTP.
- [ ] *(self-assessed)* The test for concurrent updates fails informatively if a bug is introduced — that is, the test output explains which two events were expected to be concurrent and what the implementation reported instead.

## Expected Output

When the test harness runs the conjunction simulation (`cargo test --release partition_concurrent_updates -- --nocapture`), the output should look approximately like:

```text
[t=0.000s] Bus: 4 nodes online (pacific, indian_ocean, atlantic, antarctic)
[t=0.100s] Bus: partition initiated (group A: pacific, atlantic; group B: indian_ocean, antarctic)
[t=0.150s] pacific: submit(attitude_X) -> lamport=1, vector={pacific:1}
[t=0.200s] indian_ocean: submit(attitude_Y) -> lamport=1, vector={indian_ocean:1}
[t=2.000s] Bus: partition healed
[t=2.100s] pacific: received from indian_ocean (lamport=1, vector={indian_ocean:1})
[t=2.105s] pacific: reconcile(attitude_X, attitude_Y) -> CONCURRENT
[t=2.110s] pacific: surfaced conflict to application layer
[t=2.150s] indian_ocean: received from pacific (lamport=1, vector={pacific:1})
[t=2.155s] indian_ocean: reconcile(attitude_Y, attitude_X) -> CONCURRENT
PASS: both nodes identified the updates as concurrent
PASS: no silent winner was selected
```

The exact timestamps and formatting are not graded; what is graded is that the test successfully detects concurrency in both directions and that no node silently picks a winner.

## Hints

<details>
<summary>1. Where to start</summary>

Begin with `lamport.rs`. It is the smallest correct piece of the system. Write the type, write its three methods, and write the monotonic-causality unit test before anything else. Once the Lamport clock is solid, the vector clock is structurally similar with a wider state shape.

</details>

<details>
<summary>2. AtomicU64 vs Mutex for the Lamport counter</summary>

The Lamport clock can use `AtomicU64` for the local counter, but the `observe()` operation needs `max(local, incoming) + 1`, which is not a single atomic op. You have two choices: a CAS loop on `AtomicU64` (lock-free, slightly more complex) or a `Mutex<u64>` (simpler, contended under load). For this project, either is acceptable; pick the one whose tradeoffs you can articulate in a code-review conversation.

</details>

<details>
<summary>3. Designing the MessageBus partition model</summary>

The bus needs to track which nodes can deliver to which other nodes. A `HashMap<(String, String), VecDeque<Message>>` per-pair queue model works well: when partitioned, the (from, to) pair's queue continues to receive sends but is not drained until heal. An alternative is a single global queue with a per-message "deliverable_at" timestamp, but the per-pair model maps more directly to the test scenarios.

</details>

<details>
<summary>4. Concurrent updates in tests</summary>

To force concurrency reliably in tests, you cannot rely on real time. Instead, use the bus's partition feature: partition before both nodes submit their events, then heal afterward. Each node's local clock advances independently during the partition, producing vector clocks that the other node has not observed — which is the definition of concurrency.

</details>

<details>
<summary>5. Why test ordering instead of message content</summary>

The acceptance criterion is that concurrent events are *identified* as concurrent, not that any particular winner is picked. Your test should assert that `compare()` returns `Concurrent`, not that a specific value won. Asserting on the winner couples the test to a tiebreaker policy that the project is explicitly trying to make pluggable.

</details>

<details>
<summary>6. Cleanly testing partition recovery</summary>

Recovery has two failure modes worth testing separately: (a) messages sent during the partition are eventually delivered after heal; (b) the vector clocks correctly identify which post-heal merges represent concurrency vs causal precedence. Write one test for each. A single test that asserts everything is harder to debug when one assertion fails.

</details>

## Source Anchors

- DDIA 2nd Edition, Chapter 9 — "Unreliable Clocks," "Knowledge, Truth, and Lies"
- Lamport, "Time, Clocks, and the Ordering of Events in a Distributed System" (CACM, 1978) — the canonical reference for the logical-clock construction; recommended supplemental reading
- DDIA 2nd Edition, Chapter 10 — used briefly for the CAP/PACELC framing of why this matters
