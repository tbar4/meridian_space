# Lesson 3 — MVCC and Snapshot Isolation

**Module:** Database Internals — M05: Transactions & Isolation  
**Position:** Lesson 3 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapter 13; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 7; [Mini-LSM Week 3](https://skyzh.github.io/mini-lsm/week3-overview.html)

> **Source note:** This lesson was synthesized from training knowledge and the Mini-LSM Week 3 MVCC chapters. Verify Petrov's MVCC version chain description and Kleppmann's snapshot isolation anomaly analysis.

---
<!-- toc -->
---
## Context

MVCC solves the fundamental problem of 2PL — writers blocking readers — by keeping multiple versions of each key. Instead of locking a key and making other transactions wait, the engine stores every version alongside a timestamp. Readers pick the version that matches their snapshot timestamp; writers create new versions without disturbing old ones. Readers never block writers. Writers never block readers. The only conflict is writer-writer on the same key.

For the LSM architecture, MVCC is a natural fit. The LSM already stores sorted key-value pairs — extending keys to include a timestamp is a straightforward encoding change. Mini-LSM's Week 3 implements exactly this: the key format changes from `user_key` to `(user_key, timestamp)`, where newer timestamps sort first. A snapshot read at timestamp T scans for the first version of each key with timestamp ≤ T.

---

## Core Concepts

### Timestamped Keys in LSM

The MVCC key format encodes the user key and a commit timestamp into a single sortable byte string:

```text
MVCC key = [user_key_bytes] [timestamp as big-endian u64, inverted]
```

The timestamp is stored as **big-endian** and **bitwise inverted** (XOR with `u64::MAX`) so that newer timestamps sort *before* older ones in the LSM's byte-order comparison. This means a scan for key "NORAD-25544" encounters the newest version first — exactly what a snapshot read needs.

Example ordering for key "NORAD-25544":
```text
"NORAD-25544" | ts=110 (inverted: 0xFFFFFFFFFFFFFF91)  ← newest, sorts first
"NORAD-25544" | ts=80  (inverted: 0xFFFFFFFFFFFFFFAF)
"NORAD-25544" | ts=50  (inverted: 0xFFFFFFFFFFFFFFCD)  ← oldest, sorts last
```

### Snapshot Read

A transaction with `read_ts = 100` reading key K:

1. Seek to the first MVCC key with prefix K.
2. Scan versions from newest to oldest.
3. Return the first version with `commit_ts ≤ read_ts`.
4. If the first matching version is a tombstone, the key is deleted in this snapshot — return None.

This is O(V) where V is the number of versions of the key. In practice V is small (1–5 for most keys) because compaction garbage-collects old versions.

### Write Path with Timestamps

When a transaction commits with `write_ts = 105`:

1. For each key in the write set, create an MVCC key `(user_key, 105)`.
2. Write all MVCC key-value pairs to the memtable (via the WAL, as in Module 4).
3. These versions become visible to any transaction with `read_ts ≥ 105`.

Write-write conflicts: if two transactions both write the same user key, the later commit must detect the conflict. Simple approach: check if any version of the key with `commit_ts > txn.read_ts` exists at commit time. If so, another transaction has written this key after our snapshot — abort and retry.

### Watermark and Garbage Collection

Old versions accumulate. If every write creates a new version, the database grows without bound. **Garbage collection** removes versions that are no longer visible to any active transaction.

The **watermark** is the minimum `read_ts` among all active transactions. Any version with `commit_ts < watermark` that has a newer version is safe to garbage-collect — no active transaction can ever read it.

```text
Active transactions: read_ts = [100, 150, 200]
Watermark = 100

Key "NORAD-25544" versions:
  ts=200  (value_v3)  ← keep (above watermark, no newer version)
  ts=110  (value_v2)  ← keep (above watermark, may be needed by ts=100..149 txns)
  ts=80   (value_v1)  ← keep (ts=100 txn might need it — 80 ≤ 100 and next version is 110)
  ts=30   (value_v0)  ← GARBAGE: ts=80 exists and 30 < watermark, so no txn will ever read v0

Wait — v0 at ts=30 is still needed if a txn at ts=50 existed. But watermark is 100, meaning
no transaction has read_ts < 100. So v0 (ts=30) is only needed if some transaction reads at
ts=30..79. Since watermark=100 guarantees no such transaction exists, v0 is safe to collect.
```

Garbage collection happens during **compaction**: when the merge iterator encounters multiple versions of the same key, it keeps the newest version per user key that is above the watermark, plus one version at or below the watermark (for transactions at exactly the watermark timestamp). All older versions are dropped.

### Write Batch Atomicity

MVCC writes must be atomic — all keys in a transaction get the same `write_ts`, and they all become visible at once. In the LSM engine, this means all keys in a write batch are logged to the WAL as a single unit and inserted into the memtable together. The `write_ts` is assigned from a global monotonic counter protected by a mutex (as in Mini-LSM's approach).

---

## Code Examples

### MVCC Key Encoding

```rust,ignore
/// Encode a user key and timestamp into an MVCC key.
/// Timestamps are inverted so newer versions sort first.
fn encode_mvcc_key(user_key: &[u8], timestamp: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(user_key.len() + 8);
    key.extend_from_slice(user_key);
    // Invert timestamp: newer (larger) timestamps become smaller bytes,
    // sorting first in ascending byte order.
    key.extend_from_slice(&(!timestamp).to_be_bytes());
    key
}

/// Decode an MVCC key back into user key and timestamp.
fn decode_mvcc_key(mvcc_key: &[u8]) -> (&[u8], u64) {
    let ts_start = mvcc_key.len() - 8;
    let user_key = &mvcc_key[..ts_start];
    let inverted_ts = u64::from_be_bytes(
        mvcc_key[ts_start..].try_into().unwrap()
    );
    (user_key, !inverted_ts)
}
```

### Snapshot Read with MVCC

```rust,ignore
impl LsmEngine {
    /// Read the value of a key at the given snapshot timestamp.
    fn get_at_timestamp(
        &self,
        user_key: &[u8],
        read_ts: u64,
    ) -> io::Result<Option<Vec<u8>>> {
        // Seek to the newest version of this key
        let seek_key = encode_mvcc_key(user_key, u64::MAX);

        // Create a merge iterator over memtable + SSTables
        let mut iter = self.create_merge_iterator(&seek_key)?;

        while let Some((mvcc_key, value)) = iter.next()? {
            let (key, ts) = decode_mvcc_key(&mvcc_key);

            // Stop if we've moved past this user key
            if key != user_key {
                return Ok(None);
            }

            // Skip versions newer than our snapshot
            if ts > read_ts {
                continue;
            }

            // This is the newest version visible to us
            return match value {
                Some(val) => Ok(Some(val)),
                None => Ok(None), // Tombstone — key is deleted in this snapshot
            };
        }

        Ok(None) // Key not found in any source
    }
}
```

The seek to `(user_key, u64::MAX)` positions the iterator at the newest possible version of the key (since `u64::MAX` is the largest timestamp, and inverted it becomes the smallest byte value). The iterator then scans backward through versions until it finds one with `ts ≤ read_ts`.

---

## Key Takeaways

- MVCC stores multiple versions of each key with commit timestamps. Readers select the version matching their snapshot timestamp. Writers create new versions without disturbing old ones.
- In the LSM architecture, MVCC keys are encoded as `(user_key, inverted_timestamp)` so that newer versions sort first in byte order. This makes snapshot reads efficient — the first matching version is the correct one.
- Readers never block writers, and writers never block readers. The only conflict is write-write on the same key, detected at commit time.
- Garbage collection removes old versions that are below the watermark (minimum active `read_ts`). This happens during compaction and is essential for bounding space amplification.
- Write skew remains possible under snapshot isolation. For the OOR, this is an acceptable tradeoff — conjunction queries need consistent snapshots, not full serializability.

---

{{#quiz lesson-03-quiz.toml}}
