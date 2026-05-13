# Lesson 1: Distributed Locks and Leases

## Context

The Constellation Network has a recurring class of operations that require **mutual exclusion across the cluster**: exactly one ground station at a time should be commanding a given satellite during a pass; exactly one orbital-element-registry node at a time should be processing the daily TLE merge; exactly one scheduler instance at a time should be assigning pass windows. These are textbook mutual-exclusion problems. On a single machine, you would use a mutex. Across a cluster, you cannot — and the reasons why have already been the source of one expensive Meridian incident.

In April 2024, two registry instances each believed they held the daily-merge lock simultaneously. The first instance had requested the lock and proceeded with the merge. Partway through, its host paused for 11 seconds (a stop-the-world garbage collection in a Java sidecar; the merge process was Rust but shared the host). During the pause, the lock service decided the first instance had failed and granted the lock to the second instance. The second instance started merging. When the first instance's host resumed, it continued its merge — completely unaware that its lock had been revoked and reassigned. Two simultaneous merges produced a corrupt TLE database that took six hours to reconstruct.

The bug is not in the lock service. The bug is in treating the lock service like a single-machine mutex. A distributed lock is **fundamentally probabilistic** in a way that a single-machine mutex is not: holding the lock at time T does not mean the holder is the only entity that will believe it holds the lock at T+1. This lesson covers the mechanisms — leases, fencing tokens, and consensus-backed coordination services — that make distributed mutual exclusion safe. By the end, you should be able to use a distributed lock service correctly, identify the failure modes of misuse, and understand why the right answer to "we just need a distributed mutex" is rarely "deploy ZooKeeper."

## Core Concepts

### Why a Distributed Mutex Cannot Look Like a Single-Machine Mutex

The single-machine mutex contract is simple: at any given instant, at most one thread holds the lock. The kernel enforces this by atomically updating the lock's owner field. The thread that holds the lock can rely on its uniqueness without any additional checks.

A distributed lock cannot offer this guarantee. The lock service is a process on a different machine; the holder is connected to it by an unreliable network; the holder's view of "do I still hold the lock" is at best the moment it last heard from the service. Two specific failure modes break the single-machine intuition:

**The holder pauses.** The holder is alive but unresponsive (GC pause, host CPU stall, swap thrash). The lock service times out the holder and reassigns the lock. The first holder resumes and continues executing as if it still holds the lock. Two holders, no exclusion.

**The network partitions.** The holder is on one side of a partition; the lock service is on the other. The holder still believes it holds the lock; the service may have reassigned it. Again, two holders.

The single-machine kernel could not produce either failure mode. The distributed lock service inherently can. The defenses are structural — fencing tokens — not procedural — making the timeout longer.

### Leases: Time-Bounded Locks

A **lease** is a lock that expires automatically after a duration unless renewed. The holder acquires a lease with some TTL; before the TTL elapses, the holder renews the lease (extending its expiry); if the holder fails to renew (because it has crashed or is partitioned), the lease expires and the lock service can grant it to someone else.

The lease pattern is the standard distributed lock primitive. ZooKeeper's ephemeral znodes implement leases (the znode expires when the session times out); etcd's leases are explicit; Consul's session-tied keys are leases. The mechanism shifts the cost of detection from "the lock service detecting a holder is dead" to "the holder proving it is still alive."

The lease duration is the operational knob. Too short: holders spend most of their time renewing rather than doing work, and a brief network blip can spuriously expire the lease. Too long: a failed holder's lock remains unavailable for the full duration after the failure. The standard tradeoff is to choose a lease duration considerably longer than the renewal interval (e.g., 30-second lease, 10-second renewal), so two renewals can be lost before the lease expires.

What leases do **not** fix: the holder-pauses-during-its-work problem. A holder that pauses for longer than the lease TTL has implicitly lost the lease but has no immediate way to know. When the holder resumes, the lease has expired, possibly been granted to another holder, and the original holder may continue its work in violation of the mutual-exclusion contract. Leases alone are insufficient. The fix is fencing.

### Fencing Tokens: The Safety-Critical Pattern

The **fencing token** pattern, popularized by Martin Kleppmann's "How to do distributed locking" (2016), is the mechanism that makes distributed locks safe in the presence of holder pauses and partitions.

The mechanism: when the lock service grants a lease, it issues a monotonically increasing **fencing token** — a number larger than any previously-issued token. The holder includes this token in every request to the protected resource. The resource (e.g., the registry's storage layer) tracks the highest token it has seen and **rejects any request with a lower token**.

The April incident, with fencing tokens, would have played out differently. The first registry instance acquired the lock with token 42 and started merging. After its pause, the lock service had reassigned the lock to the second instance with token 43. When the first instance resumed and submitted a merge write with token 42, the storage layer would have observed `42 < 43` and rejected the write. The second instance's writes with token 43 would have been accepted. The corruption would not have occurred.

Fencing tokens require **cooperation from the protected resource**. The lock service alone cannot enforce fencing — it can issue tokens, but the resource must check them. This is why distributed locks alone are not enough; the entire write path from holder to resource must propagate and validate the token. Systems that retrofit distributed locks onto storage that doesn't understand fencing are vulnerable to exactly the April incident.

### Consensus-Backed Coordination Services

The lock service itself is a distributed system, and getting it wrong is how you produce more outages than you prevent. Production systems use **consensus-backed coordination services**: ZooKeeper, etcd, and Consul.

**ZooKeeper** (Apache, 2008) uses the ZAB protocol (similar in spirit to Raft) to maintain a replicated hierarchical filesystem. Locks are implemented as ephemeral nodes; the first node to create an ephemeral child of a lock path wins the lock; the node automatically disappears when the session ends. Watches let other waiters be notified when the lock-holding node disappears. ZooKeeper's API is one of the most influential designs in the field; many other systems were built on top of it.

**etcd** (CoreOS, 2013) uses Raft and exposes a key-value store with linearizable operations, leases, and watches. Its API is simpler than ZooKeeper's — flat keys instead of hierarchical paths, explicit leases instead of ephemeral nodes — and the deployment is single-binary. etcd is the coordination service for Kubernetes and many other cloud-native systems.

**Consul** (HashiCorp, 2014) provides similar primitives plus service discovery, health checking, and a gossip-based membership layer. It is more opinionated about what coordination is for; its API is shaped around the specific patterns (service registration, health checks, KV configuration) rather than primitives that you compose.

The three services have different operational characteristics — ZooKeeper is heavier and battle-tested; etcd is simpler and fast-iterating; Consul is integrated and opinionated — but the core capability is the same: a consensus-backed store of small data items with strong consistency and watch semantics. The catalog uses etcd for membership and coordination, on the basis that the team values simplicity and the operational story is well-understood.

### What Coordination Services Are For (and What They Are Not For)

Coordination services are designed for **small, slowly-changing, high-value state**: cluster membership, leader election, configuration, lock state, schema versions. The data is tiny (kilobytes, not gigabytes), the write rate is low (hundreds per second across the cluster, not millions), and the consistency guarantees are strong.

They are not designed for:

- **High-throughput data storage.** etcd will not serve as your TLE database. The consensus overhead per write is too high for application data.
- **Large value storage.** A coordination service is not a blob store. Per-value size limits are typically in the kilobyte range.
- **Pub/sub at scale.** While watches exist, they are not designed to drive millions of subscribers. Use Kafka or NATS for that workload.

Treating a coordination service as a general-purpose database is a recurring failure mode. The service collapses under load; the team blames the service rather than recognizing the workload mismatch. The right framing is: coordination services are the consensus layer's user-facing API. Use them for the consensus-shaped problems and use other tools for everything else.

### Lease Renewal and Clock Discipline

Lease renewal has a subtle dependency on time. The lock service measures the lease's age from when it was granted; the holder measures from when it should renew. The two clocks must agree closely enough that the holder renews before the service times out.

This is the same clock-unreliability concern from Module 1. If the holder's clock runs slow, it may delay renewals past the service's timeout. If the service's clock runs fast, it may time out renewals that arrived in real time but appeared late to the service. The defenses:

- **Use monotonic time for renewal intervals.** `Instant::elapsed` on the holder; `time.MonotonicNow()` on the service. Don't use wall-clock time for interval calculations.
- **Renew well before the lease expires.** A 10-second renewal interval with a 30-second lease tolerates one or two missed renewals before the lease expires. If the renewals were "just before expiry," any clock skew would expire the lease.
- **Treat lease expiry as a holder failure mode.** A holder whose lease has expired must stop using its lock — issuing more writes will be rejected by fencing (in a system that uses fencing) and will produce divergence (in a system that doesn't).

The discipline is: leases are not just locks with timeouts; they are leases with explicit renewal and explicit handling of lease loss. Holders should be designed to detect "my lease may have expired" and respond by stopping work and re-acquiring (or surfacing the failure to the operator), rather than continuing as if nothing has changed.

## Code Examples

### A Lease-Based Lock with Fencing Token

```rust,edition2021
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct FencingToken(u64);

pub struct Lease {
    pub holder: String,
    pub token: FencingToken,
    pub expires_at: Instant,
}

pub struct LockService {
    state: Mutex<LockServiceState>,
}

struct LockServiceState {
    current_lease: Option<Lease>,
    next_token: u64,
}

impl LockService {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(LockServiceState {
                current_lease: None,
                next_token: 1,
            }),
        }
    }

    pub fn acquire(&self, holder: &str, ttl: Duration) -> Option<Lease> {
        let mut s = self.state.lock().unwrap();
        // If a lease is active and not expired, deny.
        if let Some(ref existing) = s.current_lease {
            if existing.expires_at > Instant::now() {
                return None;
            }
        }
        // Grant a new lease with the next token.
        let token = FencingToken(s.next_token);
        s.next_token += 1;
        let lease = Lease {
            holder: holder.to_string(),
            token,
            expires_at: Instant::now() + ttl,
        };
        s.current_lease = Some(lease.clone());
        Some(lease)
    }

    pub fn renew(&self, token: FencingToken, ttl: Duration) -> bool {
        let mut s = self.state.lock().unwrap();
        match s.current_lease.as_mut() {
            Some(lease) if lease.token == token && lease.expires_at > Instant::now() => {
                lease.expires_at = Instant::now() + ttl;
                true
            }
            _ => false,
        }
    }
}

impl Clone for Lease {
    fn clone(&self) -> Self {
        Self {
            holder: self.holder.clone(),
            token: self.token,
            expires_at: self.expires_at,
        }
    }
}

// The protected resource validates the fencing token on every write.
pub struct ProtectedStore {
    highest_token_seen: Mutex<FencingToken>,
}

impl ProtectedStore {
    pub fn new() -> Self {
        Self {
            highest_token_seen: Mutex::new(FencingToken(0)),
        }
    }

    pub fn write(&self, token: FencingToken, _data: &[u8]) -> Result<(), &'static str> {
        let mut highest = self.highest_token_seen.lock().unwrap();
        if token < *highest {
            // A delayed write from a deposed holder. Reject it.
            return Err("fencing: stale token rejected");
        }
        *highest = token;
        // ... apply the write ...
        Ok(())
    }
}

fn main() {
    let svc = LockService::new();
    let store = ProtectedStore::new();

    let lease_a = svc.acquire("merger-a", Duration::from_secs(30)).unwrap();
    // merger-a does some work...
    store.write(lease_a.token, b"merge-step-1").unwrap();

    // Simulate merger-a pausing past lease expiry, then merger-b acquiring.
    let lease_b = svc.acquire("merger-b", Duration::from_secs(30));
    // (In reality, merger-b couldn't acquire because lease_a hasn't expired yet
    // in real time. For a test, we'd advance simulated time or set a short TTL.)

    // After merger-a resumes, its write is rejected by fencing.
    // store.write(lease_a.token, b"merge-step-2") would return Err here.
    println!("lease_a token: {:?}, lease_b possibility: {:?}", lease_a.token, lease_b.is_some());
}
```

The structural point: the lock service grants the token, but the store enforces it. Both pieces are required. Skipping either — using a lock service without fencing, or using fencing without a real consensus-backed token issuer — leaves the holes that produce the April incident.

### Lease Renewal Loop

```rust,edition2021
use std::time::Duration;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

# struct LockService;
# struct Lease { token: FencingToken }
# #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)] struct FencingToken(u64);
# impl LockService {
#     fn renew(&self, _t: FencingToken, _d: Duration) -> bool { true }
# }

pub async fn renewal_loop(
    svc: Arc<LockService>,
    initial_lease: Lease,
    renewal_interval: Duration,
    ttl: Duration,
    keep_running: Arc<AtomicBool>,
) -> Result<(), &'static str> {
    let mut current_token = initial_lease.token;

    while keep_running.load(Ordering::SeqCst) {
        // Sleep, using monotonic time. tokio::time::sleep is monotonic.
        tokio::time::sleep(renewal_interval).await;

        // Attempt renewal. If the lock service rejects (lease already expired),
        // we cannot continue safely: someone else may now hold the lease.
        if !svc.renew(current_token, ttl) {
            // Lease lost. Stop work immediately - subsequent writes would be
            // fenced anyway, but better to halt than to perform doomed work.
            return Err("lease lost; cannot continue holding the lock");
        }
    }
    Ok(())
}
```

The renewal loop is small and important. It uses monotonic time (via `tokio::time::sleep`), it stops cleanly on shutdown, and it surfaces lease loss to the caller as an error — not as a silent state change. The caller (the merger, the scheduler, whoever) is responsible for halting work on lease loss.

### A Common Anti-Pattern

For contrast, the anti-pattern:

```rust,ignore
// ANTI-PATTERN: distributed lock without fencing.

async fn do_critical_section(svc: &LockService) -> Result<()> {
    let lease = svc.acquire("worker", Duration::from_secs(30)).await?;
    // ... do work ...
    // PROBLEM: if this code path takes longer than 30 seconds (due to GC, swap,
    // cosmic ray, whatever), the lease expires. The lock service grants the
    // lock to someone else. We continue executing here, oblivious. The
    // critical section is no longer exclusive.
    perform_writes_without_token();  // <-- bug
    svc.release(lease).await?;
    Ok(())
}
# struct LockService;
# struct Lease;
# impl LockService {
#     async fn acquire(&self, _w: &str, _d: std::time::Duration) -> Result<Lease> { Ok(Lease) }
#     async fn release(&self, _l: Lease) -> Result<()> { Ok(()) }
# }
# fn perform_writes_without_token() {}
# use anyhow::Result;
```

The fix is fencing: pass `lease.token` to `perform_writes`, and have the storage layer validate it on every write. The distributed lock alone is not sufficient.

## Key Takeaways

- A distributed lock is not a single-machine mutex. The holder can pause, the network can partition, and the lock service can revoke the lease — all without the holder's immediate knowledge. Code that treats a distributed lock as if it were a single-machine mutex will be wrong under those conditions.
- Leases (time-bounded locks with explicit renewal) are the standard distributed lock primitive. They shift detection from the service to the holder: the holder must prove liveness via renewal. The lease duration is tuned against the renewal interval to tolerate transient renewal failures.
- Fencing tokens make distributed locks safe in the presence of holder pauses. The lock service issues monotonically increasing tokens; the protected resource validates each write's token against the highest seen. Without fencing, leases are insufficient.
- Coordination services (ZooKeeper, etcd, Consul) are the consensus-backed implementations of locks, leases, and related primitives. They are designed for small, slowly-changing, high-value state — not for general-purpose data storage.
- Lease renewal depends on time. Use monotonic clocks for renewal intervals; renew well before lease expiry to tolerate clock skew; treat lease loss as a halt-and-recover event, not a silent state change.

> **Source note:** This lesson is grounded in DDIA 2nd Edition (Kleppmann & Riccomini), Chapter 10, "Membership and Coordination Services" — and in Martin Kleppmann's blog post "How to do distributed locking" (kleppmann.com, 2016), which is the canonical public reference for fencing tokens. ZooKeeper details are from the ZooKeeper documentation and Hunt et al., "ZooKeeper: Wait-free coordination for Internet-scale systems" (USENIX ATC 2010). The April incident is illustrative and not based on a real Meridian event. Specific operational parameters (30-second lease, 10-second renewal) are common defaults but should be calibrated per workload.

{{#quiz quiz-1.toml}}
