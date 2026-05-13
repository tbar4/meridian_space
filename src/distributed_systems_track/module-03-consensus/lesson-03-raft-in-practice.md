# Lesson 3: Raft in Practice — Membership Changes, Log Compaction, Recovery

## Context

The Raft algorithm covered in Lesson 2 is what a correct cluster running on a fixed set of nodes does. Real clusters do not run on fixed sets of nodes. Operators add capacity, retire failing machines, expand from three nodes to five for higher fault tolerance, and shrink back when the workload drops. Real clusters accumulate log entries indefinitely until either disk fills up or replay-on-restart takes hours. Real clusters experience network partitions that split the cluster into pieces, leave the minority partition unable to make progress, and require the operator to understand whether the cluster will heal on its own when the partition resolves.

These are the operational concerns that the basic Raft algorithm does not address by itself. The Raft paper extends the algorithm with mechanisms for **cluster membership changes** (adding or removing nodes safely), **log compaction** (discarding old log entries via snapshots), and **partition behavior** (what happens during and after partition events). This lesson covers all three. The catalog's deployment runs in this operational regime continuously; understanding these mechanisms is what separates a Raft cluster you can operate from one that will produce incidents you cannot diagnose.

By the end of this lesson, you should be able to: describe the joint-consensus mechanism for safely changing cluster membership; identify when log compaction is needed and how snapshots interact with InstallSnapshot RPCs; predict cluster behavior under specific partition scenarios; and recognize the configurations under which a cluster can permanently fail to make progress.

## Core Concepts

### Membership Changes: The Naive Approach Doesn't Work

Suppose the catalog cluster runs on three nodes (A, B, C) and the team wants to expand to five nodes (A, B, C, D, E) for higher fault tolerance. The naive approach — push a configuration change through the cluster as an ordinary log entry — has a subtle but devastating safety failure.

The problem: during the transition, some nodes have applied the new configuration and some have not. If A applies the new configuration first (it now thinks the cluster has 5 nodes), and B and C have not yet applied it (they still think the cluster has 3 nodes), then it is possible to have two disjoint majorities — A plus two of D, E forms a majority under the new configuration; B plus C forms a majority under the old. Both can elect leaders simultaneously. Both can commit different log entries. The safety property is destroyed.

The Raft paper's solution is **joint consensus** — a two-phase membership change that explicitly handles the transition.

### Joint Consensus

The transition from configuration `C_old` to `C_new` proceeds through a transitional state `C_old,new` that requires majorities from **both** the old and new configurations.

Phase 1: The cluster commits a special log entry representing `C_old,new`. Once committed, this entry is the active configuration. From this point on, any decision (leader election, log commit) requires a majority of `C_old` AND a majority of `C_new`. No decision can be made by either side alone.

Phase 2: Once `C_old,new` is committed, the leader proposes the entry representing `C_new` alone. When committed, the cluster transitions fully to the new configuration. `C_old` nodes that are no longer in `C_new` can be removed from the cluster.

The joint-consensus mechanism preserves safety because there is no point at which a decision could be made by a majority of one configuration alone. During the transition, every decision requires overlap with both configurations.

The cost is operational complexity: a membership change requires committing two log entries instead of one, and the cluster is in an unusual state between them. Production Raft libraries (etcd's Raft, hashicorp/raft) implement this for you, but you should understand the shape because the operational signals during a membership change — temporary increase in commit latency, brief windows when the cluster cannot tolerate a failure — are direct consequences of the joint-consensus structure.

### Single-Server Membership Changes (the Simpler Alternative)

A subsequent paper by Ongaro proposed a simpler approach: **single-server membership changes**. Instead of changing membership in arbitrary increments, restrict each change to a single node added or removed at a time. With this restriction, the old and new configurations differ by exactly one node, and any majority of the new configuration overlaps with any majority of the old configuration (because removing or adding one node cannot create disjoint majorities).

This is the approach most production Raft implementations use, because it is simpler to reason about and simpler to implement correctly. To go from 3 nodes to 5, the operator adds one node, waits for the cluster to commit the membership change, then adds the next, and so on. Each step is a normal log entry; no joint-consensus state is needed.

The catalog uses single-server changes. The runbook for "add a new ground-station-tier replica" is: provision the node, register it with the cluster via the membership API, wait for the membership-change entry to commit, repeat. The operational story is much simpler than joint consensus, at the cost of more steps for large changes.

### Log Compaction via Snapshots

A Raft log grows forever. A cluster running for a year, accepting one write per second, accumulates 31 million log entries — terabytes of data and hours of replay time on a fresh follower. The protocol needs a mechanism to discard old entries.

The mechanism is **snapshots**: periodically, each node captures the current state of its state machine, writes it to disk, and discards log entries up to the snapshot point. When a new follower joins, or a follower that has fallen too far behind needs to catch up, the leader sends an `InstallSnapshot` RPC containing the snapshot, after which normal log replication resumes from the snapshot's point.

Three operational details matter:

**When to snapshot.** Production systems snapshot every N log entries or every M bytes. Too frequent and the CPU cost of snapshotting dominates; too rare and the log grows unbounded. The standard heuristic is to snapshot when the log exceeds 10× the snapshot size, balancing snapshot frequency against catch-up cost.

**Concurrent application and snapshotting.** A snapshot of a large state machine takes time. During that time, the cluster continues to accept writes. The state machine implementation must support copy-on-write snapshots or some equivalent mechanism that allows reads (for the snapshot) to proceed concurrently with writes (for the active workload). RocksDB-backed Raft implementations use RocksDB's checkpoint feature for this.

**InstallSnapshot vs incremental catch-up.** When a follower falls behind, the leader first tries to catch it up via normal AppendEntries with earlier indices. If the earlier entries have been compacted away (the leader's log no longer goes that far back), the leader falls back to sending the entire snapshot. This is much more expensive — gigabytes vs kilobytes of network traffic — and a frequent indication of either a chronically lagging follower or a misconfigured snapshot interval.

### Partition Recovery: What the Cluster Does During and After

The way a Raft cluster behaves during a network partition depends on which side of the partition contains the majority.

**Majority side.** Continues to operate normally. It elects a leader (if the partition is brand new and the old leader was on the minority side) or retains the existing leader. Writes commit normally. The minority side's nodes are detected as failed (no AppendEntries acks) but the cluster proceeds without them.

**Minority side.** Cannot commit new entries. The local leader (if any) accepts writes from clients but cannot replicate them to a majority. After some time, the local leader may step down, or may continue trying indefinitely (the behavior here depends on the implementation; some Raft libraries automatically step down after losing majority contact for a sustained period). Clients connected to the minority side experience write failures.

**Read behavior on the minority side.** This is where implementations differ. The strict reading of Raft says reads should also require a majority quorum (a "read index" check) to be linearizable, which means minority-side reads should fail. Some implementations allow reads from the local follower as a degraded mode. The catalog's deployment requires linearizable reads, so minority-side reads fail; this is documented and accepted as a tradeoff for correctness.

**Healing the partition.** When connectivity restores, nodes on the minority side observe heartbeats from the majority side, advance their terms (if necessary), and resume normal operation as followers. Any log entries the minority side appended but did not commit are truncated and replaced by the majority side's log. This is the same truncation behavior as the deposed-leader scenario from Lesson 2.

The pathological case: a partition that splits the cluster into two halves of equal size, with no majority on either side. The cluster makes no progress on either side until the partition heals. This is why production Raft clusters always have an odd number of nodes — a 5-node cluster split 2-2-1 (with two-thirds of the cluster on each side of a different partition) is still recoverable; a 4-node cluster split 2-2 is permanently stalled until the partition heals.

### When the Cluster Permanently Fails to Make Progress

A Raft cluster can be in a state where it cannot make progress even when nominally healthy. The most common cases:

**Majority failure.** If more than half the nodes are permanently dead, no quorum can form. The cluster cannot recover without operator intervention. The recovery procedure is to manually force a configuration change to a configuration where the surviving nodes form a majority. This is invasive and requires manual procedures that vary by implementation.

**Persistent partial synchrony failure.** If the network is degraded such that no leader can maintain heartbeats to a majority within the election timeout, the cluster will spend its time electing and re-electing without committing. The signal is a high election rate with low commit progress. The mitigation is to widen the election timeout or to investigate the underlying network degradation.

**Disk failure on a quorum-essential node.** If a node's persistent log is corrupted and replicas are unavailable, the cluster can lose committed entries. Production systems address this by replicating the log durably to multiple disks per node, by configuring cluster size to tolerate the expected failure rate, and by alerting on disk error rates before they reach catastrophic levels.

The operational discipline is to monitor: leader stability (term changes per hour), commit latency (p50, p99), election frequency, snapshot frequency, and log size per node. A change in any of these metrics is the early signal of a problem that has not yet produced an outage.

### Read-Only Operations: Linearizable Reads in Raft

Raft preserves linearizability for writes by construction. Reads are subtler. The naive approach — let the leader serve reads from its local state — has a flaw: a deposed leader (whose term has been superseded but who has not yet heard about it) can serve stale reads. The Raft paper proposes two mechanisms:

**Read index.** Before serving a read, the leader confirms it is still the leader by sending a heartbeat to all followers and waiting for majority acknowledgment. Only after the heartbeat succeeds does the leader read from its local state. This adds a round-trip to every read but guarantees linearizability.

**Lease reads.** The leader maintains a "lease" that says no other node can become leader for some duration. Within the lease, the leader can serve reads from local state without coordination. The lease is typically tied to the election timeout: if no follower will start an election within `election_timeout`, the leader can serve reads without checking. This is faster but requires bounded clock skew across the cluster — a fact that ties this lesson back to Module 1's discussion of clock unreliability.

The catalog uses read-index for linearizable reads (the orbital catalog and the conjunction-prediction service). It uses local-leader reads (without read-index) for the operational dashboard, where the team has accepted the rare possibility of stale reads as a worthwhile tradeoff for latency.

## Code Examples

### Single-Server Membership Change

The shape of a single-server membership change, as a configuration log entry:

```rust,edition2021
#[derive(Clone, Debug)]
pub struct ConfigChange {
    pub change_type: ChangeType,
    pub node_id: String,
    pub address: String,
}

#[derive(Clone, Copy, Debug)]
pub enum ChangeType { Add, Remove }

#[derive(Clone, Debug)]
pub struct Configuration {
    pub voters: Vec<String>,
}

impl Configuration {
    pub fn apply(&mut self, change: &ConfigChange) -> Result<(), String> {
        match change.change_type {
            ChangeType::Add => {
                if self.voters.contains(&change.node_id) {
                    return Err(format!("node {} already in cluster", change.node_id));
                }
                self.voters.push(change.node_id.clone());
                Ok(())
            }
            ChangeType::Remove => {
                let pos = self.voters.iter().position(|n| n == &change.node_id);
                match pos {
                    None => Err(format!("node {} not in cluster", change.node_id)),
                    Some(p) => {
                        self.voters.remove(p);
                        Ok(())
                    }
                }
            }
        }
    }

    pub fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }
}

fn main() {
    let mut config = Configuration {
        voters: vec!["A".into(), "B".into(), "C".into()],
    };
    println!("initial majority: {} of {}", config.majority(), config.voters.len());
    config.apply(&ConfigChange {
        change_type: ChangeType::Add,
        node_id: "D".into(),
        address: "10.0.0.4".into(),
    }).unwrap();
    println!("after add D: majority {} of {}", config.majority(), config.voters.len());
}
```

The configuration is itself stored as a log entry, applied to the state machine like any other write. The persistence and replication of configuration changes use the same Raft machinery as data writes — which is what makes the membership change safe (it goes through the same election restrictions and majority requirements).

### A Read-Index Implementation

```rust,ignore
// Linearizable read via read-index.

use anyhow::Result;
use std::sync::Arc;

async fn linearizable_read(node: &RaftNode, key: &str) -> Result<Option<Vec<u8>>> {
    // 1. Confirm we are still the leader: send a heartbeat round.
    //    This is the same AppendEntries RPC used for replication, but with
    //    no new entries - just confirming followers still recognize us.
    let confirmed = node.confirm_leadership().await?;
    if !confirmed {
        return Err(anyhow::anyhow!("not leader"));
    }

    // 2. Record the current commit index. We will wait until our state
    //    machine has applied up to this index before serving the read.
    //    (In a high-throughput system, the state machine application can lag
    //    the log commit; we must wait for it to catch up.)
    let read_index = node.commit_index();
    node.wait_until_applied(read_index).await?;

    // 3. Read from local state.
    Ok(node.state_machine_get(key))
}
# struct RaftNode;
# impl RaftNode {
#     async fn confirm_leadership(&self) -> Result<bool> { Ok(true) }
#     fn commit_index(&self) -> u64 { 0 }
#     async fn wait_until_applied(&self, _i: u64) -> Result<()> { Ok(()) }
#     fn state_machine_get(&self, _k: &str) -> Option<Vec<u8>> { None }
# }
```

The cost is a heartbeat round per read. For a 5-node cluster, this is 4 heartbeats and 4 acknowledgments per read — non-trivial but acceptable for most workloads. The catalog's read-index implementation batches reads within a window so that one heartbeat serves many reads, amortizing the cost.

### Snapshot Trigger Logic

```rust,edition2021
pub struct SnapshotPolicy {
    pub min_log_entries_between_snapshots: u64,
    pub log_size_multiplier: u64,  // snapshot when log > N × snapshot_size
}

pub struct SnapshotTrigger {
    policy: SnapshotPolicy,
    last_snapshot_log_index: u64,
    last_snapshot_size_bytes: u64,
}

impl SnapshotTrigger {
    pub fn should_snapshot(&self, current_log_index: u64, current_log_bytes: u64) -> bool {
        let entries_since = current_log_index - self.last_snapshot_log_index;
        if entries_since < self.policy.min_log_entries_between_snapshots {
            return false;
        }
        // Snapshot when the log has grown to N× the previous snapshot size.
        // This bounds the worst-case catch-up cost: a new follower's catch-up
        // is at most one snapshot plus N× snapshot size of log replay.
        current_log_bytes > self.last_snapshot_size_bytes * self.policy.log_size_multiplier
    }

    pub fn note_snapshot_taken(&mut self, log_index: u64, snapshot_bytes: u64) {
        self.last_snapshot_log_index = log_index;
        self.last_snapshot_size_bytes = snapshot_bytes;
    }
}
```

The trigger logic is local to each node; nodes do not coordinate snapshots. A consequence is that different nodes snapshot at different points in time, and a new follower may receive a snapshot from any leader. The InstallSnapshot RPC carries the snapshot's `last_included_index` and `last_included_term` so the receiver knows where to resume log replication from.

## Key Takeaways

- Membership changes use joint consensus (the original Raft mechanism) or single-server changes (the simpler variant most production systems implement). Both preserve safety; single-server is simpler operationally at the cost of requiring step-at-a-time changes.
- Log compaction via snapshots is essential for long-running clusters. The snapshot trigger should balance snapshot CPU cost against new-follower catch-up cost; 10× log-to-snapshot ratio is a standard heuristic.
- Partition behavior is asymmetric: the majority side continues to operate; the minority side cannot commit new entries. Healing the partition causes truncation of uncommitted entries on the minority side. Two equal-sized partitions in an even-numbered cluster permanently stall — use odd-numbered clusters.
- Linearizable reads in Raft require either a read-index round (heartbeat-confirmed leadership before serving the read) or lease reads (within a bounded clock-skew assumption). Local-leader reads without coordination are not linearizable — they may serve stale data from a deposed leader.
- Cluster health metrics — leader stability, commit latency, election frequency, snapshot rate — are the early signals of operational problems. A change in any of these is worth investigating before it produces an outage.

> **Source note:** This lesson is grounded in DDIA 2nd Edition (Kleppmann & Riccomini), Chapter 10, "Consensus in Practice" — and in the original Raft paper (Ongaro & Ousterhout 2014) plus Ongaro's PhD dissertation, "Consensus: Bridging Theory and Practice" (Stanford 2014), which is the authoritative reference for single-server membership changes, log compaction, and the read-index/lease-read mechanisms. The dissertation is the recommended deep-dive for engineers implementing or operating production Raft. Specific operational parameters (snapshot ratios, election timeout bands) are conventional defaults; production deployments tune these based on workload.

{{#quiz quiz-3.toml}}
