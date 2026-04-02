# Project — Conjunction Query Engine with MVCC Snapshot Reads

**Module:** Database Internals — M05: Transactions & Isolation  
**Track:** Orbital Object Registry  
**Estimated effort:** 8–10 hours

---

## SDA Incident Report — OOR-2026-0046

> **Classification:** ENGINEERING DIRECTIVE  
> **Subject:** Add MVCC snapshot isolation to the OOR storage engine  
>  
> Ref: OOR-2026-0046 (stale TLE data in conjunction assessment)  
>  
> Extend the LSM engine with MVCC support. Conjunction queries must see consistent catalog snapshots. Concurrent TLE updates must not block or corrupt reads.

---

## Acceptance Criteria

1. **MVCC key encoding.** Encode user keys with inverted big-endian timestamps. Verify that newer versions sort before older versions in byte order.

2. **Snapshot read correctness.** Insert key "NORAD-25544" at timestamps 50, 80, and 110. Read at timestamps 60, 90, and 120. Verify each read returns the correct version (ts=50, ts=80, ts=110 respectively).

3. **Tombstone visibility.** Insert key "NORAD-99999" at ts=50, delete at ts=80. Read at ts=60 → value. Read at ts=90 → None.

4. **Concurrent reads and writes.** Spawn two threads: one performs 10,000 reads at a fixed snapshot, the other performs 1,000 writes with incrementing timestamps. Verify all reads return consistent results (no torn reads, no version mixing). Writers must not block readers.

5. **Write conflict detection.** Start two transactions with overlapping read timestamps. Both write the same key. The first to commit succeeds; the second detects the conflict and is aborted.

6. **Garbage collection.** Set watermark to 100. Insert versions at ts=30, 70, 90, 120 for a key. Run compaction. Verify that ts=30 is garbage-collected, ts=70 and ts=90 are retained (safety margin), and ts=120 is retained.

7. **Conjunction simulation.** Load 10,000 TLE records. Start a conjunction query (snapshot read over 100 objects). While the query is running, update 50 of those objects. Verify the query sees only the pre-update versions.

---

## Starter Structure

```
conjunction-engine/
├── Cargo.toml
├── src/
│   ├── main.rs           # Entry point
│   ├── mvcc.rs            # MVCC key encoding, Transaction, conflict detection
│   ├── lsm.rs             # Extended with timestamp-aware get/put/scan
│   ├── compaction.rs      # Extended with watermark-aware GC
│   └── (reuse remaining modules from Modules 3–4)
```

---

## Hints

<details>
<summary>Hint 1 — Timestamp encoding</summary>

Use `!timestamp` (bitwise NOT) converted to big-endian bytes, appended to the user key. This makes newer timestamps sort first without modifying the LSM's comparator.

</details>

<details>
<summary>Hint 2 — Write conflict detection</summary>

At commit time, scan the memtable and SSTables for any version of the key with `commit_ts > txn.read_ts`. If found, another transaction wrote this key after our snapshot — abort.

</details>

<details>
<summary>Hint 3 — Watermark computation</summary>

Maintain a `BTreeSet<u64>` of all active transaction read timestamps. The watermark is the minimum value in the set. When a transaction commits or aborts, remove its read_ts. Use a mutex to protect the set.

</details>

---

## What Comes Next

Module 6 (Query Processing) builds structured query execution on top of the MVCC storage engine — scan operators, join algorithms, and the volcano iterator model for composable query plans.
