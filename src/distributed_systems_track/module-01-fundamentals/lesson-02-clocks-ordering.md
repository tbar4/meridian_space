# Lesson 2: Clocks, Ordering, and Causality

## Context

At 04:12Z, the Constellation Operations dashboard showed two conflicting attitude updates for **MSS-17** arriving at the catalog within 80 milliseconds of each other. One was tagged with timestamp `04:11:59.428Z` from the Pacific ground station; the other was tagged `04:12:00.012Z` from the Indian Ocean station. The Pacific update — the "older" one by wall-clock — overwrote the Indian Ocean update because it arrived second. Three orbits later, an operator noticed that the satellite was responding to commands as if its attitude was still in the earlier state. The catalog had silently lost the update.

The root cause was not a network failure. Both messages were delivered intact. The root cause was that the Pacific ground station's NTP daemon had drifted forward by 600 milliseconds, and the catalog's last-write-wins resolver — which trusted wall-clock timestamps to order writes — had been handed a lie that it had no way to detect. The two writes were **concurrent** in the causal sense: neither had observed the other before being submitted. But the system flattened them into a total order on the basis of a clock reading that did not correspond to any real ordering of events.

This is the second foundational truth of distributed systems: **time itself is unreliable**. Wall clocks on different machines disagree, and they disagree silently. There is no global "now." Once you accept this, the question becomes: how do you order events at all? The answer — and it is a deep one — is that you don't order them by time. You order them by **causality**, using mechanisms that do not depend on physical clocks. This lesson introduces the three flavors of clock available to a distributed system, explains when each is safe and when each lies, and walks through the logical-clock algorithms (Lamport, vector) that make causal ordering possible without trusting wall time.

## Core Concepts

### Time-of-Day Clocks vs Monotonic Clocks

Every modern operating system exposes two distinct clock APIs, and they serve different purposes. Confusing them is the source of an enormous amount of distributed-systems pain.

A **time-of-day clock** — `std::time::SystemTime` in Rust, `clock_gettime(CLOCK_REALTIME)` on Linux — returns the current wall-clock time, synchronized to UTC via NTP, PTP, or some other time source. It is useful for displaying timestamps to humans and for cross-machine comparison, *provided you accept that the comparison is approximate*. The catch is that this clock can **jump backward**: if NTP determines that the local clock is ahead of the reference, it will rewind it. A naïve `now() - earlier_now()` calculation can return a negative duration on a wall clock.

A **monotonic clock** — `std::time::Instant` in Rust, `clock_gettime(CLOCK_MONOTONIC)` — is guaranteed to never go backward. It has no defined relationship to UTC; the value is meaningless across processes or machines. But within a single process, the difference between two `Instant` values is a reliable measure of elapsed time. This is the clock to use for timeouts, durations, and rate limiting.

The rule is simple: **wall clocks for "when," monotonic clocks for "how long."** A timeout written with `SystemTime` can fire instantly or never if NTP adjusts the clock during the wait. The Rust standard library reinforces this by making `Instant::elapsed()` infallible while making `SystemTime::elapsed()` return a `Result`.

### Why NTP Cannot Save You

NTP is the protocol that synchronizes wall clocks across the network. It works by sampling reference servers, estimating round-trip time, and adjusting the local clock to compensate. Under good conditions on a stable network, NTP can hold machines within a few milliseconds of each other.

"Good conditions" is doing a lot of work in that sentence. NTP synchronization can fail or drift for many reasons: the network path can be congested (which adds skew because RTT estimation is wrong); the reference server can be wrong (which has happened — leap second bugs have taken down entire fleets); the local clock crystal can drift faster than NTP can compensate (typical drift is around 10 ppm, or about 1 second per day); a virtualized clock in a paused VM can lag behind reality and then jump forward when the VM resumes.

DDIA documents specific cases: Google's MapReduce engineers found that clocks could be off by tens of milliseconds during normal operation and seconds during synchronization storms. Cloudflare experienced a 2017 outage when a leap second was applied incorrectly. The takeaway is not that NTP is bad; it is that **NTP gives you approximate synchronization, never guarantees**, and any algorithm whose correctness depends on tight clock synchronization is one bad NTP day away from misbehaving.

The cleanest summary, from DDIA: a clock reading is best thought of as a value with a **confidence interval**, not a point. Google's Spanner database makes this explicit through its TrueTime API, which returns `TT.now() = (earliest, latest)`. Algorithms that need to compare timestamps across machines wait out the uncertainty interval before committing — they trade latency for correctness. Most systems do not have a TrueTime equivalent, and they pay for the missing uncertainty in subtler ways, like the lost MSS-17 attitude update.

### Lamport Logical Clocks: Counting Events, Not Seconds

If wall clocks are untrustworthy, can we order events without consulting them at all? Yes — and Leslie Lamport showed how in 1978. The construction is so simple it looks like a trick.

Each node maintains a single counter, initially zero. The counter increments on every local event. On sending a message, the sender attaches the current counter value. On receiving a message, the receiver sets its counter to `max(local_counter, received_counter) + 1`. That's it.

The result is that for any two events `a` and `b` where `a` causally precedes `b` (the happens-before relation, written `a → b`), the Lamport timestamp of `a` is strictly less than the Lamport timestamp of `b`. The clock captures causality precisely enough to guarantee that **if `a → b`, then `L(a) < L(b)`**. It does not give you the converse: `L(a) < L(b)` does not imply `a → b`, because two concurrent events can happen to have a particular ordering of timestamps.

Lamport clocks give you a **total order** on events — every pair of events has a defined ordering, with ties broken by node ID — that respects causality. They cannot tell you which pairs are concurrent. For many systems this is enough: a Lamport timestamp is sufficient to deterministically resolve "last write wins" in a way that is independent of wall clocks. Cassandra used Lamport-like timestamps for early versions of its last-write-wins resolution before adopting hybrid logical clocks.

### Vector Clocks: Detecting Concurrency

When you do need to detect concurrency — when "these two events are unrelated and a human needs to make a decision" is a different outcome than "this one happened first" — you need a **vector clock**. Instead of a single counter per node, each node maintains a vector of counters, one per node in the system.

The update rule extends Lamport's: on a local event, the node increments its own slot in the vector. On sending, it attaches the entire vector. On receiving, the receiver does an element-wise max with the received vector and then increments its own slot.

Vector clocks support a richer comparison: given two vector clocks `V1` and `V2`, `V1 < V2` (componentwise, with at least one strict inequality) means `V1 happened-before V2`. If neither `V1 < V2` nor `V2 < V1`, the events are **concurrent** — they have no causal relationship, and the system must either surface the conflict to the application or pick a deterministic tiebreaker that is independent of wall clocks.

The cost is space: vector clocks grow linearly with the number of nodes in the system. For a 48-satellite constellation plus 12 ground stations plus dozens of services, this is tractable. For a system with millions of clients, it isn't, and there are compressed variants (dotted version vectors, hybrid logical clocks) that trade off precision for size. We will encounter Lamport-style hybrid logical clocks again in Module 3 when we look at how Spanner-style systems combine physical and logical time.

### Knowing What You Cannot Know: The Truth About Distributed State

A useful framing from DDIA: in a distributed system, the notion of "the current state" is itself suspect. You do not directly observe state; you observe messages that purport to describe state, with some unknown delay, from a sender whose own view of state is itself derived from messages, and so on. There is no privileged observer.

This is why the rest of the track is structured the way it is. Consensus algorithms (Module 3) exist because no single node's view of the world is authoritative. Coordination primitives like leases (Module 5) exist because two nodes can simultaneously believe they hold an exclusive resource. Failure detection (Module 4) is fundamentally probabilistic because "this node has not responded" is the only signal available, and it does not distinguish "dead" from "slow."

For now, the takeaway is concrete: **wall clocks cannot order events across machines, NTP does not fix this, and you have a choice between logical clocks (which order causally but don't relate to physical time) or accepting that some pairs of events are inherently unordered and surfacing concurrency to the caller**. Choose deliberately. The MSS-17 catalog made the choice silently, and it cost an attitude update.

## Code Examples

### A Lamport Clock for the Catalog

The catalog needs to order updates from multiple ground stations without trusting their wall clocks. A Lamport clock is sufficient if the only requirement is "deterministic last-write-wins across the cluster."

```rust,edition2021
use std::cmp::max;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct LamportClock {
    // AtomicU64 gives us lock-free updates from multiple tasks. The value is
    // the local notion of logical time: it advances on local events and on
    // observing higher timestamps from incoming messages.
    counter: AtomicU64,
}

impl LamportClock {
    pub fn new() -> Self {
        Self { counter: AtomicU64::new(0) }
    }

    /// Called for any local event that should advance logical time. Returns
    /// the timestamp that should be attached to the resulting message or
    /// catalog write.
    pub fn tick(&self) -> u64 {
        // fetch_add returns the *previous* value; we want the new one.
        self.counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Called when receiving a message tagged with `incoming`. Advances the
    /// local clock to max(local, incoming) + 1, which is the Lamport update
    /// rule. Returns the new local value.
    pub fn observe(&self, incoming: u64) -> u64 {
        // We use a CAS loop rather than fetch_max because we need to add 1
        // after taking the max. fetch_max alone would not advance past
        // 'incoming' itself when local < incoming.
        loop {
            let current = self.counter.load(Ordering::SeqCst);
            let new = max(current, incoming) + 1;
            if self
                .counter
                .compare_exchange(current, new, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return new;
            }
            // Lost the race; retry. This is fine for low contention; if many
            // tasks observe simultaneously we'd switch to a more sophisticated
            // structure, but for ground-station message rates this is plenty.
        }
    }
}

fn main() {
    let clock = LamportClock::new();
    let t1 = clock.tick();           // local event
    let t2 = clock.observe(42);      // received message with ts=42
    let t3 = clock.tick();           // another local event
    println!("{t1} {t2} {t3}");      // prints: 1 43 44
}
```

Two things to notice. First, the timestamp `42` from the incoming message **forced our local clock forward**, even though we had only emitted timestamp 1 locally. This is the mechanism that makes causality work: if a remote saw something we haven't seen yet, our clock advances past it. Second, the returned timestamps are total-ordered by `(timestamp, node_id)`: ties are broken by node ID, which guarantees a deterministic global ordering even when two events get the same logical time.

### A Vector Clock for Conflict Detection

When concurrency is a meaningful outcome — "these two ground stations issued attitude commands that haven't seen each other; the operator needs to choose" — a Lamport clock will silently pick one. A vector clock surfaces the conflict.

```rust,edition2021
use std::cmp::max;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorClock {
    // Map from node_id to that node's counter as last observed by us.
    // Missing entries are treated as zero, which lets new nodes join without
    // requiring a global preallocation.
    counters: HashMap<String, u64>,
}

#[derive(Debug, PartialEq)]
pub enum Ordering3 {
    Before,      // self < other (self happened before other)
    After,       // self > other (other happened before self)
    Equal,       // identical
    Concurrent,  // neither precedes the other - true concurrency
}

impl VectorClock {
    pub fn new() -> Self {
        Self { counters: HashMap::new() }
    }

    /// Local event on node `id`. Increments only this node's slot.
    pub fn tick(&mut self, id: &str) {
        *self.counters.entry(id.to_string()).or_insert(0) += 1;
    }

    /// Merge another node's clock into ours (called on message receive).
    /// Element-wise max, then we'd typically call tick() afterward.
    pub fn merge(&mut self, other: &VectorClock) {
        for (node, &count) in &other.counters {
            let entry = self.counters.entry(node.clone()).or_insert(0);
            *entry = max(*entry, count);
        }
    }

    /// Compare two clocks. Returns Concurrent if neither dominates - the
    /// caller now knows this is a real conflict, not just a stale reading.
    pub fn compare(&self, other: &VectorClock) -> Ordering3 {
        let mut self_greater_anywhere = false;
        let mut other_greater_anywhere = false;

        let all_nodes: std::collections::HashSet<&String> =
            self.counters.keys().chain(other.counters.keys()).collect();

        for node in all_nodes {
            let s = self.counters.get(node).copied().unwrap_or(0);
            let o = other.counters.get(node).copied().unwrap_or(0);
            if s > o { self_greater_anywhere = true; }
            if o > s { other_greater_anywhere = true; }
        }

        match (self_greater_anywhere, other_greater_anywhere) {
            (false, false) => Ordering3::Equal,
            (true, false)  => Ordering3::After,
            (false, true)  => Ordering3::Before,
            (true, true)   => Ordering3::Concurrent,
        }
    }
}

fn main() {
    // MSS-17 attitude updates from Pacific and Indian Ocean stations.
    // Neither has observed the other's update before submitting; both
    // start from the same baseline (catalog version).
    let mut pacific = VectorClock::new();
    pacific.tick("catalog"); // baseline, version 1

    let mut indian_ocean = pacific.clone(); // both fork from version 1

    pacific.tick("pacific"); // local update from Pacific
    indian_ocean.tick("indian_ocean"); // concurrent local update

    match pacific.compare(&indian_ocean) {
        Ordering3::Concurrent => {
            // The catalog should surface this to the operator rather than
            // silently picking a winner based on wall-clock arrival order.
            println!("CONCURRENT - operator decision required");
        }
        other => println!("ordered: {:?}", other),
    }
}
```

The Pacific clock is `{catalog: 1, pacific: 1}` and the Indian Ocean clock is `{catalog: 1, indian_ocean: 1}`. Neither dominates — Pacific has a `pacific` count that Indian Ocean doesn't, and vice versa. `compare` returns `Concurrent`. In the real catalog, this is the signal that the system must store both updates as siblings (the way Riak does) or escalate to a human, rather than picking one based on the lie of a wall clock.

### Why You Cannot Use `SystemTime` for This

For completeness, here is the version that fails:

```rust,ignore
// BROKEN: ordering catalog writes by wall-clock timestamp.
use std::time::SystemTime;

struct Update { ts: SystemTime, value: u32 }

fn resolve(a: Update, b: Update) -> Update {
    if a.ts > b.ts { a } else { b }
}
```

This is the MSS-17 bug. Two writes from machines with differently-drifted NTP can produce `a.ts > b.ts` even when `b` causally happened-after `a`, or when they are genuinely concurrent and the choice between them should not be silent. The function compiles, passes unit tests on a single machine, and silently corrupts state in production. It is the canonical failure mode of treating physical time as ordering truth in a distributed system.

## Key Takeaways

- A wall clock (`SystemTime`) can jump backward and disagrees silently across machines; a monotonic clock (`Instant`) never goes backward but is only meaningful within a single process. Use the first for "when," the second for "how long," and never confuse them.
- NTP synchronization is approximate, not guaranteed. A clock reading is best understood as a value with a confidence interval. Algorithms that compare timestamps across machines without accounting for uncertainty will misbehave during NTP storms, leap seconds, and VM pauses.
- A Lamport clock gives you a total order on events that respects causality, using a single integer counter per node. It is sufficient when "last write wins" is acceptable and concurrency does not need to be detected. It cannot tell you whether two events are concurrent.
- A vector clock can detect concurrency, at the cost of O(n) space per timestamp where n is the number of nodes. Use vector clocks when "these two updates have no causal relationship" is operationally meaningful — for example, when concurrent writes should be surfaced as conflicts rather than silently merged.
- "Order by wall-clock timestamp" is one of the most common implicit assumptions in distributed code, and one of the highest-impact bugs when it fails. If you see `if a.ts > b.ts` in code that processes data from multiple machines, treat it as a defect until proven otherwise.

> **Source note:** This lesson is grounded in DDIA 2nd Edition (Kleppmann & Riccomini), Chapter 9, "Unreliable Clocks" and "Knowledge, Truth, and Lies." The Lamport clock construction is from Leslie Lamport, "Time, Clocks, and the Ordering of Events in a Distributed System" (Communications of the ACM, 1978) — the algorithm summary is synthesized; the original paper is the canonical reference. The TrueTime API description draws on the Spanner OSDI 2012 paper; specific numbers (Spanner's clock skew bound, NTP drift rates) are illustrative and should be verified against current published values before publication.

{{#quiz quiz-2.toml}}
