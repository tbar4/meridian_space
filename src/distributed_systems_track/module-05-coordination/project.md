# Module 05 Project — Telemetry Gossip

## Mission Brief

> **Incident ticket CN-2703-018**
> **Severity:** P2
> **Reporter:** Constellation Operations, Edge Fleet
> **Status:** Open

The edge-compute fleet has reached 240 nodes and the central health registry is now the dominant source of intra-cluster traffic. The registry's CPU is at 80% steady-state; query latency for the membership API has crept from 5ms to 110ms over the last quarter; an outage of the registry on March 14 left the entire fleet uncoordinated for 47 minutes because every node's view of "who else is alive" froze.

You are building **Telemetry Gossip**, a Rust crate that replaces the central health registry with a SWIM-based gossip layer. Each node tracks the cluster membership independently via gossip; the central registry remains only as a small consensus-backed source of authoritative cluster configuration (which nodes belong to the cluster); per-node health and load state propagates via gossip with no central bottleneck.

## Repository Layout

```
telemetry-gossip/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── membership.rs       # MemberInfo, MemberStatus, MembershipState
│   ├── swim.rs             # SwimDetector: direct + indirect probe
│   ├── gossip.rs           # PushPullGossip: peer selection + state exchange
│   ├── transport.rs        # Network abstraction (real or simulated)
│   └── node.rs             # GossipNode: integrated SWIM + gossip + state
├── tests/
│   ├── convergence.rs
│   ├── swim_indirect_probe.rs
│   ├── partition_recovery.rs
│   └── scale_simulation.rs
└── README.md
```

## Required API

```rust,ignore
// membership.rs
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MemberStatus { Alive, Suspected, Dead }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemberInfo {
    pub node_id: String,
    pub status: MemberStatus,
    pub generation: u64,  // Lamport-style timestamp for LWW reconciliation
    pub last_observed: Instant,
}

pub struct MembershipState {
    members: Mutex<HashMap<String, MemberInfo>>,
}

impl MembershipState {
    pub fn new() -> Self;
    pub fn upsert(&self, info: MemberInfo);
    pub fn merge(&self, peer_state: &[MemberInfo]);
    pub fn snapshot(&self) -> Vec<MemberInfo>;
    pub fn alive_members(&self) -> Vec<MemberInfo>;
    pub fn member(&self, id: &str) -> Option<MemberInfo>;
}

// swim.rs
pub struct SwimDetector { /* ... */ }
impl SwimDetector {
    pub fn new(transport: Arc<dyn Transport>, indirect_k: usize) -> Self;
    pub async fn probe(&self, target: &str, members: &[MemberInfo]) -> bool;
}

// gossip.rs
pub struct PushPullGossip { /* ... */ }
impl PushPullGossip {
    pub fn new(state: Arc<MembershipState>, transport: Arc<dyn Transport>) -> Self;
    pub async fn gossip_round(&self) -> Result<()>;
}

// node.rs
pub struct GossipNode { /* ... */ }
impl GossipNode {
    pub fn new(
        node_id: String,
        seeds: Vec<String>,
        transport: Arc<dyn Transport>,
    ) -> Self;
    pub async fn start(&self);
    pub fn members(&self) -> Vec<MemberInfo>;
}
```

## Acceptance Criteria

- [x] `cargo build --release` completes without warnings under `#![deny(warnings)]`.
- [x] `cargo test --release` passes all integration tests with zero flakes across 50 consecutive runs.
- [x] `cargo clippy -- -D warnings` produces no lints.
- [x] **Convergence test:** start a 20-node cluster; have node 0 update its own state; verify that all 20 nodes reflect the update within 5 gossip rounds (allowing for the O(log N) propagation bound).
- [x] **State reconciliation test:** two nodes have divergent views of a third node's status (one says Alive, one says Dead). After a single push-pull exchange, both end with the higher-generation value.
- [x] **SWIM indirect probe test:** node A's direct path to node B is broken (network injection drops A→B but not other paths). A probes B directly, fails, falls back to indirect probes via two other nodes, succeeds. A correctly classifies B as alive.
- [x] **Suspicion-state test:** when a target genuinely fails (no node can reach it), the cluster transitions it to Suspected, then after a configurable timeout to Dead. A target that recovers during the Suspected window broadcasts an Alive update that clears the suspicion.
- [x] **Partition recovery test:** partition a 10-node cluster into 7+3. While partitioned, the 3-node side's view freezes (they cannot learn about each other's state); the 7-node side correctly identifies the 3-node side as Dead via SWIM. After the partition heals, the 3-node side learns the 7-node side's state and the 7-node side updates to reflect the 3-node side's recovery.
- [x] **Scale simulation:** simulate a 250-node cluster (in-memory transport, simulated message delivery). Measure: (a) per-node memory used for membership state, (b) network bytes per gossip round, (c) average convergence time for a state change. The test reports these as observable metrics; the README documents them.
- [ ] *(self-assessed)* The README explains the relationship between the consensus-backed "cluster configuration" registry and the gossip-based membership state. A reader should understand which guarantees come from which layer.
- [ ] *(self-assessed)* The SWIM indirect probe respects the configured `K` parameter and does not blow up if fewer than `K` indirect peers are available. Edge cases (1-node cluster, 2-node cluster) are handled gracefully.
- [ ] *(self-assessed)* The state-reconciliation logic handles concurrent updates from the same node (same generation, different status) deterministically. Document the tiebreaker policy.

## Expected Output

`cargo test --release convergence -- --nocapture`:

```text
[t=0.000s] Cluster initialized: 20 nodes (n0..n19), gossip period 100ms
[t=0.100s] All nodes initial state: all members Alive
[t=0.500s] n0: update self status -> generation incremented
[t=0.600s] gossip round 1 complete. Nodes aware of update: 3
[t=0.700s] gossip round 2 complete. Nodes aware of update: 9
[t=0.800s] gossip round 3 complete. Nodes aware of update: 18
[t=0.900s] gossip round 4 complete. Nodes aware of update: 20
PASS: Convergence in 4 rounds (O(log 20) = ~4.3, within bound)
```

## Hints

<details>
<summary>1. Generation numbers and reconciliation</summary>

Each node owns its own generation counter for its own status. When a node updates its own state, it increments its generation and broadcasts. Other nodes adopt the higher generation. For "this node is dead" updates from other nodes about us, the protocol gets more interesting: the suspected node can refute by broadcasting an even-higher generation Alive update. This is the SWIM suspicion-refutation mechanism. Implement it as: the prober proposes generation G+1 with status Suspected; the suspected node refutes by issuing G+2 with status Alive.

</details>

<details>
<summary>2. Peer selection in gossip</summary>

For small clusters (under ~50), every node knows every other and picks uniformly. For larger clusters, partial views — each node tracks a subset, refreshing periodically — are more bandwidth-efficient. The project's required API supports the small-cluster case; extending to partial views is a worthwhile self-assessed improvement once the basics work.

</details>

<details>
<summary>3. Suspicion timeout vs gossip period</summary>

The suspicion timeout should be at least 2–3× the gossip period, so a node has multiple chances to refute. Too short and false-positive suspicions race the refutation; too long and real failures take too long to declare. A 100ms gossip period with a 5s suspicion timeout is a reasonable default for the test environment.

</details>

<details>
<summary>4. Testing convergence deterministically</summary>

Real-time-based convergence tests can be flaky. Instead, drive the simulation with explicit "round" ticks: the test calls `gossip_round()` on each node in sequence, then asserts on the resulting state. This makes the test reproducible and the bound assertion (`O(log N)` rounds) exact.

</details>

<details>
<summary>5. The 250-node scale simulation</summary>

Spawning 250 real tokio tasks is fine but excessive for a unit test. An alternative: keep all 250 nodes in a single test function, drive them sequentially via the simulated transport, and time the rounds. The simulation runs deterministically; the metrics it reports are exact rather than empirical. The "real" performance of the production deployment can be benchmarked separately.

</details>

<details>
<summary>6. Memory budget per node</summary>

Each MemberInfo is roughly 100 bytes (node id, status, generation, timestamp). For 250 members, that's 25KB per node — trivial. The interesting limit is in the gossip payload: 250 × 100 bytes = 25KB per gossip exchange, which at 1Hz per node is 25KB/s per peer pair. Sub-cluster peering or delta-state gossip reduces this; the project's required minimum is the 25KB/s baseline, with optimization noted as a self-assessed improvement.

</details>

## Source Anchors

- Demers et al., "Epidemic Algorithms for Replicated Database Maintenance" (PODC 1987) — original gossip protocol analysis
- Das, Gupta, Motivala, "SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol" (DSN 2002) — SWIM paper
- Hashicorp Serf documentation — production reference for SWIM in Rust-similar ecosystems
- DDIA 2nd Edition, Chapter 6 (anti-entropy section) and Chapter 10 (coordination services)
