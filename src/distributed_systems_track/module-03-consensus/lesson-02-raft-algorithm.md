# Lesson 2: The Raft Algorithm — Leader Election and Log Replication

## Context

The previous lesson made the case that consensus is the right tool for the catalog's leader-election problem and laid out what consensus requires. This lesson covers **Raft** — the specific protocol the Constellation Network will implement. Raft was designed by Diego Ongaro and John Ousterhout at Stanford in 2014, with the explicit goal of being **understandable**. Paxos, the canonical consensus protocol since Lamport's 1989 paper, is famously difficult to teach and famously easy to get wrong in implementation. Raft achieves the same correctness guarantees with a structure that decomposes cleanly into three subproblems: leader election, log replication, and safety.

That decomposition is why Raft is now the default consensus protocol for new systems. etcd, Consul, CockroachDB, TiKV, MongoDB, and the Kafka KRaft mode are all built on Raft or Raft variants. Knowing Raft is no longer optional for engineers working on distributed data systems; it is the lingua franca.

By the end of this lesson, you should be able to: describe the three roles in Raft and the transitions between them; trace a single write through the log-replication path from client to commit; identify the safety property that the leader-election restriction enforces; and recognize the failure modes (split votes, deposed leaders, lost AppendEntries) that the protocol is designed to handle. Lesson 3 will cover the operational concerns — membership changes, log compaction, and partition recovery — that the protocol's basic shape doesn't address.

## Core Concepts

### The Three Roles: Follower, Candidate, Leader

Every Raft node is in one of three states at any time:

**Follower.** The default state. Followers passively receive log entries from a leader, vote on candidate requests, and reset an **election timeout** whenever they hear from a legitimate leader. If the election timeout expires without contact, the follower becomes a candidate.

**Candidate.** A follower that has decided to call an election. Candidates increment their **current term**, vote for themselves, and request votes from all other nodes. If a candidate receives votes from a majority of nodes in the same term, it becomes the leader. If it receives an AppendEntries from a node claiming to be a leader for the current or higher term, it reverts to follower. If the election times out without a winner (a split vote), it starts a new election with an incremented term.

**Leader.** The exclusive writer for the cluster. The leader receives client requests, appends them to its log, replicates the entries to followers, and tells followers when entries are committed. The leader sends periodic heartbeats (empty AppendEntries) to maintain its authority.

The state machine is small enough to memorize, but the dynamics matter. Term numbers monotonically increase across the cluster; a node that hears a higher term than its own immediately reverts to follower and adopts the higher term. This is the mechanism that prevents stale leaders from continuing to act after a new leader is elected.

### Leader Election: How a Cluster Picks a Leader

Election begins when a follower's election timeout fires without a heartbeat from a leader. The timeout is **randomized** within a window (typically 150–300 ms) — this is the protocol's defense against split votes. If every follower had the same timeout, they would all become candidates simultaneously and split the vote.

The candidate increments its current term, votes for itself, and sends a `RequestVote` RPC to all other nodes. A node grants a vote if all of the following are true:

1. The candidate's term is at least as high as the node's current term.
2. The node has not already voted for someone else in this term.
3. The candidate's log is at least as up-to-date as the node's log. (See "The Election Restriction" below.)

If a candidate receives votes from a majority, it becomes leader and immediately sends heartbeats to assert its authority — this stops other followers from timing out and starting their own elections.

A few failure modes the protocol must handle:

**Split vote.** Two candidates each get less than a majority. The election times out, both increment their terms, and randomize their next election timeout. The randomization makes it overwhelmingly likely that one will start its next election before the other, monopolizing the votes.

**Network partition.** A leader on the minority side of a partition cannot get heartbeats acknowledged by a majority. After its own commit-progress check fails (or after another node on the majority side starts an election), the partitioned leader effectively becomes irrelevant — its term will be superseded when the partition heals. This is the "deposed leader" scenario that fencing tokens (Module 5) defend against.

**Cascading election timeouts.** If many followers time out at once, they all become candidates and split the vote repeatedly. The randomized timeout prevents this from being a permanent failure, but it can produce a flurry of failed elections before the cluster converges. This is why the Raft paper recommends timeouts well above the typical network round-trip but well below human attention spans (150–300 ms is the standard band).

### Log Replication: From Client Request to Commit

Once a leader is elected, the catalog's clients submit writes to it. The leader's job is to replicate every write to a majority of nodes before considering it committed. The mechanism:

1. Client sends a request to the leader.
2. Leader appends the entry to its local log with the current term and a monotonically increasing index.
3. Leader sends `AppendEntries` RPCs to each follower, containing the new entry plus a reference to the immediately preceding entry (`prevLogIndex`, `prevLogTerm`).
4. Each follower verifies that its log matches at `prevLogIndex` (the **log matching property**). If yes, it appends the new entry and acknowledges. If no, it rejects, prompting the leader to back up and try with an earlier index.
5. Once the leader sees acknowledgment from a majority of nodes (including itself), the entry is **committed**.
6. Leader applies the entry to its state machine and informs followers (via the `commitIndex` in subsequent AppendEntries) that they can apply too.
7. Leader responds to the client.

Two structural details deserve attention. First, the **log matching property**: if two logs contain an entry with the same index and term, then the logs are identical up to that entry. This is the invariant that lets the protocol detect and repair log divergence efficiently. Followers verify it before appending; leaders use it to back up after rejection.

Second, the **commit point is when a majority has the entry, not when all have it**. A slow follower does not block commits. The slow follower will eventually catch up via subsequent AppendEntries, but the cluster does not wait for it. This is the same shape as the "one synchronous follower plus the leader" pattern from Module 2, generalized to a majority quorum.

### The Election Restriction: The Safety Critical Detail

The most delicate part of Raft — and the one that distinguishes it from naive consensus protocols — is the **election restriction**. A node grants a vote to a candidate only if the candidate's log is **at least as up-to-date** as the voter's log. "Up-to-date" is defined precisely: a log A is more up-to-date than log B if A's last entry has a higher term, or has the same term and a higher index.

The purpose of this restriction is to prevent a leader from being elected that does not have all the committed entries. Without the restriction, a candidate with an incomplete log could win an election and start replacing committed entries with new ones — violating the safety property that committed entries are durable.

The argument that the restriction works: any committed entry exists on a majority of nodes. A candidate that wins an election needs votes from a majority. The two majorities must overlap (any two majorities share at least one node). The overlapping node has the committed entry. The election restriction forces the candidate's log to be at least as up-to-date as the overlapping node's, which means the candidate has the committed entry. Therefore, no leader can be elected that is missing a committed entry.

The proof is in the original Raft paper; the operational consequence is that **committed entries are durable across leader changes**. This is the safety guarantee that consensus must provide and that 2PC-style protocols famously do not.

### Heartbeats and the Cost of Leadership

The leader sends periodic heartbeats (empty AppendEntries) to followers to maintain its authority. Heartbeats are sent at an interval shorter than the election timeout — typically 50 ms when the election timeout is 150–300 ms. The 3× margin gives the protocol some slack for network jitter without spawning unnecessary elections.

The CPU and bandwidth cost of heartbeats is non-trivial at scale. A cluster of 5 nodes generates 4 heartbeats every 50 ms from the leader, plus 4 responses — 160 messages per second of cluster-internal traffic just to maintain leadership. For wide-area Raft clusters (the catalog's twelve ground stations), this is unacceptable, and production systems use longer heartbeat intervals or alternative liveness checks. We will discuss this in Lesson 3 under "Raft for wide-area deployments."

### Why "Safety Always, Liveness When Synchronous"

Raft's design preserves the four consensus properties with one important qualification: **safety holds under any failure model**, including arbitrary message delays, lost messages, network partitions, and node crashes. **Liveness** (the protocol making progress) requires a window of partial synchrony — specifically, the heartbeat must reach a majority of followers within the election timeout often enough that an election can complete.

If the network is permanently asynchronous — heartbeats never arrive in bounded time — Raft will spend its life electing and re-electing without ever committing entries. This is not a Raft bug; it is the FLP impossibility re-expressed. Production systems mitigate by tuning timeouts to the observed network behavior and by alerting when election rate exceeds a baseline. A spike in election count is the canonical signal that the network has degraded faster than the cluster can adapt.

## Code Examples

### Raft State Skeleton

A minimal Raft state representation in Rust. This is not a complete implementation, but the structural shape:

```rust,edition2021
use std::sync::Mutex;
use std::time::Instant;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role { Follower, Candidate, Leader }

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>,
}

pub struct RaftState {
    pub role: Role,
    // Persistent state - must survive crash, written before responding to RPCs
    pub current_term: u64,
    pub voted_for: Option<String>,  // candidate this node voted for in current term
    pub log: Vec<LogEntry>,
    // Volatile state - reset on restart
    pub commit_index: u64,
    pub last_applied: u64,
    // Leader-only volatile state - reset on election
    pub next_index: std::collections::HashMap<String, u64>,
    pub match_index: std::collections::HashMap<String, u64>,
    // Election timing
    pub last_heartbeat: Instant,
}

impl RaftState {
    pub fn new() -> Self {
        Self {
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: Default::default(),
            match_index: Default::default(),
            last_heartbeat: Instant::now(),
        }
    }

    /// Called when this node receives any RPC with a term higher than its own.
    /// This is the universal rule that prevents stale leaders from continuing
    /// to act after a new term has been established.
    pub fn observe_higher_term(&mut self, new_term: u64) {
        if new_term > self.current_term {
            self.current_term = new_term;
            self.voted_for = None;
            self.role = Role::Follower;
        }
    }
}
```

The split into persistent and volatile state is essential. Persistent state (current term, voted_for, log) must be flushed to disk before any RPC response — losing it on crash would violate safety, because the node could vote twice in the same term. Volatile state (commit_index, last_applied) is reconstructable from the log on restart.

### The RequestVote Handler

This is the heart of leader election:

```rust,edition2021
use std::sync::Mutex;

# #[derive(Clone, Copy, PartialEq, Eq, Debug)]
# enum Role { Follower, Candidate, Leader }
# #[derive(Clone, Debug)] struct LogEntry { term: u64, index: u64 }
# struct RaftState { role: Role, current_term: u64, voted_for: Option<String>, log: Vec<LogEntry>, last_heartbeat: std::time::Instant }
# impl RaftState {
#     fn observe_higher_term(&mut self, t: u64) {
#         if t > self.current_term { self.current_term = t; self.voted_for = None; self.role = Role::Follower; }
#     }
# }
pub struct RequestVoteArgs {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

pub fn handle_request_vote(
    state: &Mutex<RaftState>,
    args: RequestVoteArgs,
) -> RequestVoteReply {
    let mut s = state.lock().unwrap();

    // Universal rule: any RPC with a higher term forces revert to follower.
    s.observe_higher_term(args.term);

    // Reject if the candidate's term is stale.
    if args.term < s.current_term {
        return RequestVoteReply {
            term: s.current_term,
            vote_granted: false,
        };
    }

    // Reject if we've already voted for someone else this term.
    if let Some(ref voted) = s.voted_for {
        if voted != &args.candidate_id {
            return RequestVoteReply {
                term: s.current_term,
                vote_granted: false,
            };
        }
    }

    // The election restriction: the candidate's log must be at least as
    // up-to-date as ours. 'Up-to-date' = higher last term, or same last term
    // with higher index.
    let our_last_index = s.log.len() as u64;
    let our_last_term = s.log.last().map(|e| e.term).unwrap_or(0);

    let candidate_log_ok = args.last_log_term > our_last_term
        || (args.last_log_term == our_last_term && args.last_log_index >= our_last_index);

    if !candidate_log_ok {
        return RequestVoteReply {
            term: s.current_term,
            vote_granted: false,
        };
    }

    // Grant the vote. Persist voted_for to disk before returning - if we crash
    // after responding but before persisting, we could vote twice on restart.
    s.voted_for = Some(args.candidate_id.clone());
    // (persist_voted_for(s.voted_for) would go here in a real implementation)

    // Reset heartbeat timer: hearing from a candidate counts as activity,
    // preventing this node from also starting an election.
    s.last_heartbeat = std::time::Instant::now();

    RequestVoteReply {
        term: s.current_term,
        vote_granted: true,
    }
}
```

Every branch in this handler corresponds to a safety case. Skip the term check and you can vote for stale candidates; skip the election restriction and you can elect a leader missing committed entries; skip the persistence and you can double-vote on crash recovery. Raft is a protocol where the code looks short until you account for every case the safety proof requires.

### AppendEntries: The Log Replication Path

The AppendEntries handler is structurally similar but handles a different invariant:

```rust,edition2021
# #[derive(Clone, Debug)] struct LogEntry { term: u64, index: u64 }
# struct RaftState { current_term: u64, log: Vec<LogEntry>, commit_index: u64, last_heartbeat: std::time::Instant }
# impl RaftState {
#     fn observe_higher_term(&mut self, _t: u64) {}
# }
use std::sync::Mutex;

pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
}

pub fn handle_append_entries(
    state: &Mutex<RaftState>,
    args: AppendEntriesArgs,
) -> AppendEntriesReply {
    let mut s = state.lock().unwrap();
    s.observe_higher_term(args.term);

    // Reject stale-term AppendEntries from old leaders.
    if args.term < s.current_term {
        return AppendEntriesReply { term: s.current_term, success: false };
    }

    // Heartbeat received from legitimate leader: reset election timer.
    s.last_heartbeat = std::time::Instant::now();

    // The log matching property check: our log must agree with the leader
    // at prev_log_index. If it doesn't, we reject - the leader will back up
    // and retry with an earlier index until we find a common point.
    if args.prev_log_index > 0 {
        let prev_entry = s.log.iter().find(|e| e.index == args.prev_log_index);
        match prev_entry {
            Some(e) if e.term == args.prev_log_term => {}  // match - continue
            _ => return AppendEntriesReply { term: s.current_term, success: false },
        }
    }

    // Conflict resolution: if an existing entry conflicts with a new one
    // (same index but different term), truncate our log and accept the new
    // entries. This is the mechanism that recovers from leader changes that
    // produced divergent suffixes.
    for new_entry in &args.entries {
        if let Some(pos) = s.log.iter().position(|e| e.index == new_entry.index) {
            if s.log[pos].term != new_entry.term {
                s.log.truncate(pos);
                break;
            }
        }
    }

    // Append entries we don't already have.
    for new_entry in args.entries {
        if !s.log.iter().any(|e| e.index == new_entry.index) {
            s.log.push(new_entry);
        }
    }

    // Advance commit_index to whatever the leader has committed, capped by
    // the index of the last entry we now have.
    if args.leader_commit > s.commit_index {
        let last_new_index = s.log.last().map(|e| e.index).unwrap_or(0);
        s.commit_index = args.leader_commit.min(last_new_index);
    }

    AppendEntriesReply { term: s.current_term, success: true }
}
```

The truncate-and-replace behavior is what allows the cluster to recover from a leader change that produced a divergent suffix. If a new leader has different entries at indices the old leader replicated, the followers' uncommitted suffixes are overwritten — which is safe precisely because the election restriction ensures the new leader had every committed entry.

## Key Takeaways

- Raft has three roles (Follower, Candidate, Leader) and the role transitions are driven by a randomized election timeout. Any RPC with a higher term forces the receiver back to Follower; this is the mechanism that keeps the cluster on a single term.
- Leader election uses a majority-vote protocol with the election restriction: a vote is granted only if the candidate's log is at least as up-to-date as the voter's. This ensures any elected leader has all committed entries.
- Log replication is two-phase: the leader appends to its local log, replicates to followers via AppendEntries, and considers the entry committed once a majority (including itself) has it. The log matching property — same index and term implies identical logs up to that point — is what makes divergence repair tractable.
- Persistent state (current_term, voted_for, log) must be flushed to disk before responding to any RPC. Skipping persistence is one of the easiest ways to violate Raft's safety guarantees on crash recovery.
- Raft preserves safety unconditionally. It preserves liveness only when the network is synchronous enough for a majority to exchange heartbeats within the election timeout. A spike in election count is the canonical signal that the network has degraded.

> **Source note:** This lesson is grounded in DDIA 2nd Edition (Kleppmann & Riccomini), Chapter 10, "Consensus" and "Single-leader replication and consensus" — and in the canonical Raft paper: Diego Ongaro and John Ousterhout, "In Search of an Understandable Consensus Algorithm (Extended Version)" (USENIX ATC 2014). The Raft paper is strongly recommended supplemental reading; it is one of the clearest systems papers ever published. Specific implementation details (timeout values, message formats) follow the paper's conventions; production Raft libraries (etcd, TiKV) deviate in operational specifics that should be checked before using in production.

{{#quiz quiz-2.toml}}
