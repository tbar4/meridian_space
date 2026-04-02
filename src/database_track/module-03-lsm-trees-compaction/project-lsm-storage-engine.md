# Project — LSM-Backed TLE Storage Engine

**Module:** Database Internals — M03: LSM Trees & Compaction  
**Track:** Orbital Object Registry  
**Estimated effort:** 8–10 hours

---
<!-- toc -->
---
## SDA Incident Report — OOR-2026-0044

> **Classification:** ENGINEERING DIRECTIVE  
> **Subject:** Build LSM storage engine prototype for write-optimized TLE ingestion  
>  
> The B+ tree index cannot sustain burst ingestion rates during fragmentation events. Build an LSM-tree-based storage engine that batches writes in a memtable, flushes to immutable SSTables, and uses leveled compaction to bound read amplification. Bloom filters on each SSTable must reduce negative lookup cost to near-zero.

---

## Objective

Build a complete LSM storage engine that:

1. Accepts `put(key, value)` and `delete(key)` into an in-memory memtable
2. Freezes and flushes the memtable to SSTable files when a size threshold is reached
3. Supports `get(key)` by probing the memtable, then SSTables from newest to oldest
4. Implements a simple leveled compaction (merge all L0 into L1) triggered by SSTable count
5. Attaches a bloom filter to each SSTable for fast negative lookups
6. Supports `scan(start_key, end_key)` via a merge iterator over all sources

---

## Acceptance Criteria

1. **Write throughput.** Insert 100,000 TLE records with a 4MB memtable limit. Measure and print the total time and records/second. Target: >50,000 records/second (in-memory memtable inserts).

2. **Memtable flush.** Verify that SSTables are created on disk after the memtable reaches the size threshold. Print the number of SSTables after all inserts.

3. **Point lookup correctness.** After all inserts, look up 1,000 random NORAD IDs and verify each returns the correct TLE record. Look up 1,000 non-existent IDs and verify each returns `None`.

4. **Bloom filter effectiveness.** Report bloom filter hit/miss stats: how many SSTable probes were skipped by the bloom filter during the 1,000 negative lookups. Target: >95% skip rate.

5. **Delete correctness.** Delete 1,000 records. Verify `get` returns `None` for deleted keys. Verify that non-deleted keys adjacent to deleted keys still return correct values.

6. **Compaction.** Trigger compaction (merge L0 SSTables into L1). Verify that the number of SSTables decreases. Verify all keys are still accessible after compaction. Verify that deleted keys (tombstones) are garbage-collected if compaction output is the bottommost level.

7. **Range scan.** Scan NORAD IDs [40000, 40500]. Verify the results are in sorted order and include exactly the expected keys.

---

## Starter Structure

```
lsm-storage/
├── Cargo.toml
├── src/
│   ├── main.rs           # Entry point: runs acceptance criteria
│   ├── memtable.rs        # MemTable: BTreeMap-backed sorted store
│   ├── sstable.rs         # SsTableBuilder, SsTableReader
│   ├── bloom.rs           # BloomFilter: insert, may_contain, serialize
│   ├── merge_iter.rs      # MergeIterator: multi-way merge over sorted sources
│   ├── compaction.rs      # Compaction controller and merge logic
│   └── lsm.rs             # LsmEngine: top-level API (put, get, delete, scan)
```

---

## Hints

<details>
<summary>Hint 1 — SSTable file naming</summary>

Name SSTable files with a monotonically increasing ID: `sst_000001.dat`, `sst_000002.dat`, etc. Higher IDs are newer. The LSM state (which SSTables are active) can be tracked in memory as a `Vec<SstMeta>` per level. Persist the LSM state to a manifest file for crash recovery (or defer this to Module 4).

</details>

<details>
<summary>Hint 2 — Simple L0→L1 compaction trigger</summary>

The simplest compaction trigger: when the number of L0 SSTables reaches 4, merge all L0 SSTables into a single sorted L1 SSTable. This eliminates L0's overlapping key ranges. If L1 already has SSTables, include them in the merge to maintain the non-overlapping invariant.

</details>

<details>
<summary>Hint 3 — Merge iterator design</summary>

Use a `BinaryHeap<Reverse<(key, source_id, value)>>` as the min-heap. The `source_id` breaks ties: lower source ID = newer source. When popping an entry, skip all entries with the same key from older sources (they are superseded).

</details>

<details>
<summary>Hint 4 — Atomicity of SSTable swap</summary>

During compaction, write all output SSTables before modifying the LSM state. Then atomically update the state (swap old inputs for new outputs). If the engine crashes mid-compaction, the old SSTables are still valid — the output SSTables are orphaned files that can be cleaned up. This is the simplest crash-safe compaction strategy without a full WAL/manifest (which Module 4 adds).

</details>

---

## What Comes Next

Module 4 (WAL & Recovery) adds durability. The memtable is volatile — if the process crashes, unflushed writes are lost. The WAL logs every write before it enters the memtable, enabling recovery. The manifest log tracks which SSTables are active, enabling crash-safe compaction.
