# Lesson 1 — ACID Properties and Isolation Levels

**Module:** Database Internals — M05: Transactions & Isolation  
**Position:** Lesson 1 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapter 12; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 7

> **Source note:** This lesson was synthesized from training knowledge. Verify Kleppmann's isolation level taxonomy and anomaly definitions against Chapter 7.

---
<!-- toc -->
---
## Context

The OOR's LSM engine from Modules 3–4 provides durable, crash-recoverable storage for TLE records. But it offers no guarantees about what happens when multiple operations execute concurrently. A conjunction query reading NORAD 43013's TLE while a bulk update is overwriting it can see a partially-updated record — or a mix of old and new versions across different objects. The result is a phantom: a conjunction assessment computed against a catalog state that never actually existed.

**Transactions** are the abstraction that prevents this. A transaction groups multiple operations into a single atomic, isolated unit. The ACID properties define what "correct" means for transactions, and the isolation level determines how strictly concurrent transactions are separated.

---

## Core Concepts

### ACID Properties

**Atomicity:** All operations in a transaction succeed or all fail. If a bulk TLE update covers 500 objects and fails on object 347, the first 346 updates are rolled back. The catalog is never left in a partially-updated state.

**Consistency:** The database moves from one valid state to another. Application-level invariants (e.g., every NORAD ID is unique, every TLE has a valid epoch) are preserved across transactions. Consistency is primarily the application's responsibility — the database enforces it through constraints.

**Isolation:** Concurrent transactions appear to execute serially. A conjunction query running alongside a bulk update sees either the entirely pre-update or entirely post-update catalog — never a mix. The isolation level determines how strictly this is enforced.

**Durability:** Once a transaction commits, its effects survive crashes. This is the WAL's job (Module 4).

### Isolation Levels and Anomalies

Each isolation level prevents a specific set of **anomalies** — situations where concurrent execution produces results that no serial execution could produce.

**Read Uncommitted:** No isolation. A transaction can read another transaction's uncommitted writes. Vulnerable to **dirty reads** (reading data that may be rolled back).

**Read Committed:** A transaction only sees committed data. Prevents dirty reads. Still vulnerable to **non-repeatable reads** (reading the same key twice and getting different values because another transaction committed between the two reads).

**Repeatable Read / Snapshot Isolation:** A transaction sees a consistent snapshot taken at transaction start. Prevents dirty reads and non-repeatable reads. Still vulnerable to **write skew** (two transactions read overlapping data, make disjoint writes, and produce a state that neither would have produced alone).

**Serializable:** Full isolation — concurrent transactions produce results equivalent to some serial ordering. Prevents all anomalies including write skew. Most expensive to enforce.

| Level | Dirty Read | Non-Repeatable Read | Phantom Read | Write Skew |
|-------|-----------|--------------------|--------------| -----------|
| Read Uncommitted | ✗ | ✗ | ✗ | ✗ |
| Read Committed | ✓ | ✗ | ✗ | ✗ |
| Repeatable Read | ✓ | ✓ | ✗/✓ | ✗ |
| Snapshot Isolation | ✓ | ✓ | ✓ | ✗ |
| Serializable | ✓ | ✓ | ✓ | ✓ |

✓ = prevented, ✗ = possible

For the OOR, **snapshot isolation** is the practical target: conjunction queries need a consistent view of the catalog (preventing dirty reads, non-repeatable reads, and phantoms), but full serializability's overhead is unnecessary for a read-dominated workload.

### Write Skew: The Anomaly Snapshot Isolation Misses

Two conjunction analysts each read that the other is on duty. Both decide to go off-duty simultaneously, leaving no one on watch. Each transaction's writes are consistent with its own read snapshot, but the combined result violates the invariant "at least one analyst on duty."

In the OOR context: two concurrent TLE update transactions each read that a different ground station is providing TLE data for NORAD 25544. Each decides to delete the other station's TLE (deduplication). Result: both TLEs are deleted, and the object has no TLE data. Each transaction saw a valid state, but the combined result is invalid.

Snapshot isolation does not prevent this because neither transaction *writes* a key that the other *reads* — they write disjoint keys. The conflict is at the application invariant level, not the data access level. Preventing write skew requires serializable isolation (2PL or SSI).

---

## Code Examples

### Transaction Interface for the OOR

```rust,ignore
/// A transaction handle that provides snapshot isolation.
struct Transaction {
    /// Snapshot timestamp — all reads see data as of this moment.
    read_ts: u64,
    /// Commit timestamp — assigned at commit time.
    write_ts: Option<u64>,
    /// Buffered writes — applied to the engine only on commit.
    write_set: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

impl Transaction {
    fn begin(engine: &LsmEngine) -> Self {
        Self {
            read_ts: engine.current_timestamp(),
            write_ts: None,
            write_set: Vec::new(),
        }
    }

    /// Read a key as of this transaction's snapshot timestamp.
    fn get(&self, key: &[u8], engine: &LsmEngine) -> io::Result<Option<Vec<u8>>> {
        // Read the version of the key that was committed at or before read_ts
        engine.get_at_timestamp(key, self.read_ts)
    }

    /// Buffer a write (applied on commit).
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.write_set.push((key, Some(value)));
    }

    fn delete(&mut self, key: Vec<u8>) {
        self.write_set.push((key, None));
    }

    /// Commit the transaction: apply all buffered writes atomically.
    fn commit(mut self, engine: &mut LsmEngine) -> io::Result<()> {
        let write_ts = engine.next_timestamp();
        self.write_ts = Some(write_ts);

        // Apply all writes with the commit timestamp
        for (key, value) in self.write_set {
            match value {
                Some(val) => engine.put_with_ts(&key, &val, write_ts)?,
                None => engine.delete_with_ts(&key, write_ts)?,
            }
        }
        Ok(())
    }
}
```

The key insight: reads use the `read_ts` (taken at transaction start), so they always see a consistent snapshot. Writes are buffered and applied atomically with a `write_ts` (taken at commit time). Other transactions that started before `write_ts` will not see these writes — they read at their own `read_ts`.

---

## Key Takeaways

- ACID properties define transaction correctness. Atomicity (all-or-nothing), Isolation (concurrent transactions don't interfere), and Durability (committed data survives crashes) are the storage engine's responsibility. Consistency is primarily the application's.
- Snapshot isolation gives each transaction a consistent view of the database as of its start time. This prevents dirty reads, non-repeatable reads, and phantom reads — sufficient for the OOR's conjunction query workload.
- Write skew is the anomaly that snapshot isolation misses. It occurs when two transactions read overlapping data and write disjoint keys, producing a combined result that neither would have produced alone.
- The transaction interface separates read path (snapshot at `read_ts`) from write path (buffered, applied at `write_ts`). This is the foundation for MVCC (Lesson 3).

---

{{#quiz lesson-01-quiz.toml}}
