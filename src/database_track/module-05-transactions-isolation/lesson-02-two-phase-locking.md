# Lesson 2 — Two-Phase Locking (2PL)

**Module:** Database Internals — M05: Transactions & Isolation  
**Position:** Lesson 2 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapter 12; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 7

> **Source note:** This lesson was synthesized from training knowledge. Verify Petrov's 2PL description and deadlock detection algorithms against Chapter 12.

---
<!-- toc -->
---
## Context

Before MVCC became dominant, lock-based concurrency control was the standard approach to transaction isolation. **Two-Phase Locking (2PL)** is the classical protocol: transactions acquire locks before accessing data, and release them only after completing all operations. The "two phases" are the **growing phase** (acquiring locks) and the **shrinking phase** (releasing locks). A transaction never acquires a new lock after releasing any lock.

2PL provides full serializability — the strongest isolation level. But it comes at a cost: writers block readers, readers block writers, and concurrent throughput drops significantly under contention. For the OOR, where conjunction queries must not block TLE ingestion, 2PL's blocking behavior is problematic. Understanding 2PL is essential context for appreciating why MVCC (Lesson 3) is the preferred approach for read-heavy workloads.

---

## Core Concepts

### Lock Types

**Shared lock (S):** Allows the holder to read the locked resource. Multiple transactions can hold shared locks on the same resource simultaneously. Shared locks prevent writes but allow concurrent reads.

**Exclusive lock (X):** Allows the holder to read and write the locked resource. Only one transaction can hold an exclusive lock at a time. Exclusive locks block both reads and writes from other transactions.

Compatibility matrix:

| | S held | X held |
|---|--------|--------|
| S requested | ✓ grant | ✗ wait |
| X requested | ✗ wait | ✗ wait |

### The Two Phases

**Growing phase:** The transaction acquires locks as needed (shared for reads, exclusive for writes). It never releases any lock during this phase.

**Shrinking phase:** After the transaction decides to commit (or abort), it releases all locks. Once any lock is released, no new locks can be acquired.

**Strict 2PL** is the common variant: all locks are held until the transaction commits or aborts. No locks are released during the shrinking phase — they are all released at once at commit time. This prevents **cascading aborts** (where one transaction's abort forces other transactions that read its uncommitted data to also abort).

### Deadlocks

Two transactions can deadlock if each holds a lock the other needs:

- Transaction A holds an exclusive lock on NORAD 25544 and requests a shared lock on NORAD 43013.
- Transaction B holds an exclusive lock on NORAD 43013 and requests a shared lock on NORAD 25544.
- Neither can proceed. Both are stuck waiting.

Detection strategies:

- **Timeout:** If a lock wait exceeds a threshold, abort the transaction and retry. Simple but imprecise — the timeout may be too long (wasted time) or too short (false positives).
- **Wait-for graph:** Maintain a directed graph of which transactions are waiting for which. A cycle in the graph indicates a deadlock. Abort one transaction in the cycle (typically the youngest or the one with the least work done).

### 2PL Performance Characteristics

Under low contention (few transactions accessing the same keys), 2PL performs well — most lock requests are granted immediately. Under high contention (many transactions accessing overlapping keys), performance degrades:

- Writers block readers: a bulk TLE update holding exclusive locks on 500 objects blocks all conjunction queries that need any of those objects.
- Lock overhead: acquiring and releasing locks, checking the wait-for graph, and managing the lock table all consume CPU.
- Deadlock aborts: wasted work when a deadlock victim is rolled back and retried.

For the OOR's workload — frequent long-running read transactions (conjunction queries) alongside burst write transactions (TLE ingestion) — 2PL would cause conjunction queries to stall during every ingestion burst.

---

## Code Examples

### A Simple Lock Manager

```rust,ignore
use std::collections::HashMap;
use std::sync::{Mutex, Condvar};

#[derive(Debug, Clone, Copy, PartialEq)]
enum LockMode { Shared, Exclusive }

struct LockEntry {
    mode: LockMode,
    holders: Vec<u64>,    // Transaction IDs holding this lock
    wait_queue: Vec<(u64, LockMode)>, // Transactions waiting for this lock
}

struct LockManager {
    locks: Mutex<HashMap<Vec<u8>, LockEntry>>,
    cond: Condvar,
}

impl LockManager {
    fn acquire(&self, txn_id: u64, key: &[u8], mode: LockMode) -> bool {
        let mut locks = self.locks.lock().unwrap();
        loop {
            let entry = locks.entry(key.to_vec()).or_insert_with(|| LockEntry {
                mode: LockMode::Shared,
                holders: Vec::new(),
                wait_queue: Vec::new(),
            });

            let can_grant = match (mode, entry.holders.is_empty()) {
                (_, true) => true, // No holders — any mode is fine
                (LockMode::Shared, false) => entry.mode == LockMode::Shared,
                (LockMode::Exclusive, false) => false,
            };

            if can_grant {
                entry.mode = mode;
                entry.holders.push(txn_id);
                return true;
            }

            // Cannot grant — add to wait queue
            entry.wait_queue.push((txn_id, mode));
            // Block until notified (simplified — real impl checks for deadlock)
            locks = self.cond.wait(locks).unwrap();
        }
    }

    fn release(&self, txn_id: u64, key: &[u8]) {
        let mut locks = self.locks.lock().unwrap();
        if let Some(entry) = locks.get_mut(key) {
            entry.holders.retain(|&id| id != txn_id);
            if entry.holders.is_empty() {
                // Grant to the first waiter
                if let Some((waiter_id, waiter_mode)) = entry.wait_queue.first().copied() {
                    entry.holders.push(waiter_id);
                    entry.mode = waiter_mode;
                    entry.wait_queue.remove(0);
                }
            }
            self.cond.notify_all();
        }
    }
}
```

This simplified lock manager illustrates the core mechanics. Production lock managers use per-key condition variables (not a single global one), hash-based lock tables for O(1) lookup, and wait-for graph tracking for deadlock detection.

---

## Key Takeaways

- Two-phase locking provides serializable isolation by ensuring transactions acquire all locks before releasing any. Strict 2PL holds all locks until commit.
- 2PL's blocking behavior is the fundamental problem: writers block readers and readers block writers. For read-heavy workloads like conjunction queries, this creates unacceptable stalls during concurrent writes.
- Deadlocks are an inherent risk of lock-based concurrency. Detection via wait-for graphs and resolution via transaction abort are standard but add overhead and wasted work.
- 2PL is still used in some systems (MySQL/InnoDB for certain isolation levels, distributed databases for coordination). Understanding it provides essential context for why MVCC is preferred.

---

{{#quiz lesson-02-quiz.toml}}
