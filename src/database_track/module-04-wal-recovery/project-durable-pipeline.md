# Project — Durable TLE Update Pipeline

**Module:** Database Internals — M04: Write-Ahead Logging & Recovery  
**Track:** Orbital Object Registry  
**Estimated effort:** 6–8 hours

---
<!-- toc -->
---
## SDA Incident Report — OOR-2026-0045

> **Classification:** ENGINEERING DIRECTIVE  
> **Subject:** Add WAL-based durability to the LSM storage engine  
>  
> Ref: OOR-2026-0045 (data loss incident after PDU failure)  
>  
> Integrate a write-ahead log into the Module 3 LSM engine. Every mutation must be logged before it enters the memtable. The engine must recover to a consistent state after simulated crashes.

---

## Acceptance Criteria

1. **WAL write path.** Every `put` and `delete` call appends a checksummed record to the WAL before modifying the memtable. Verify by inspecting the WAL file after 1,000 inserts.

2. **Clean recovery.** Insert 5,000 records, gracefully shut down, then recover. All 5,000 records must be accessible after recovery.

3. **Crash recovery.** Insert 5,000 records. Simulate a crash by calling `std::process::abort()` (or simply skipping the shutdown routine). Restart and recover. Records up to the last fsync'd batch must be accessible. Report how many records were recovered vs. the expected count.

4. **Crash during flush.** Insert records until a memtable flush is triggered. Simulate a crash mid-flush (after writing the SSTable but before updating the manifest). Recover and verify all data is intact — the orphaned SSTable is ignored, and the WAL is replayed to reconstruct the memtable.

5. **WAL truncation.** After recovery, trigger a flush and checkpoint. Verify the WAL is truncated — old segments are deleted, and the remaining WAL contains only records above the flushed LSN.

6. **Recovery time.** Measure recovery time for WAL sizes of 10,000, 50,000, and 100,000 records. Report the time for each. Target: recovery < 500ms for 100,000 records.

7. **Manifest correctness.** After multiple flush/compaction/checkpoint cycles, recover the engine and verify the manifest correctly reports the active SSTable set and flushed LSN.

---

## Starter Structure

```
durable-pipeline/
├── Cargo.toml
├── src/
│   ├── main.rs           # Entry point: runs acceptance criteria
│   ├── wal.rs             # WalWriter, WalReader, WalSegmentManager
│   ├── manifest.rs        # ManifestWriter, ManifestReader, checkpoint
│   ├── lsm.rs             # LsmEngine with WAL integration and recovery
│   ├── memtable.rs        # Reuse from Module 3
│   ├── sstable.rs         # Reuse from Module 3
│   ├── bloom.rs           # Reuse from Module 3
│   └── compaction.rs      # Reuse from Module 3
```

---

## Hints

<details>
<summary>Hint 1 — Simulating a crash</summary>

The simplest crash simulation: after writing N records, `drop` the `LsmEngine` without calling any shutdown method, then construct a new `LsmEngine::recover()`. Alternatively, write to a temporary directory, copy/rename files to simulate partial state, and then recover from the copy.

</details>

<details>
<summary>Hint 2 — Manifest format</summary>

Keep the manifest simple: a sequence of newline-delimited JSON records. Each record is either `{"type": "add_sst", "id": 42, "level": 1, "flushed_lsn": 5000}` or `{"type": "remove_sst", "ids": [31, 32, 33]}` or `{"type": "checkpoint", "lsn": 10000, "active_ssts": [42, 43]}`. Parse with `serde_json` or manual string parsing.

</details>

<details>
<summary>Hint 3 — Crash-during-flush simulation</summary>

Write the SSTable file, then abort before writing the manifest entry. On recovery, the manifest doesn't list the SSTable. Scan the data directory for SSTable files not in the manifest and delete them (orphan cleanup). Replay the WAL to reconstruct the memtable.

</details>

---

## What Comes Next

Module 5 (Transactions & Isolation) adds MVCC support — concurrent readers see consistent snapshots while writers continue modifying the database. The WAL and manifest from this module provide the durability foundation that MVCC transactions depend on.
