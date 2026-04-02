# Lesson 2 — Crash Recovery

**Module:** Database Internals — M04: Write-Ahead Logging & Recovery  
**Position:** Lesson 2 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapter 10

> **Source note:** This lesson was synthesized from training knowledge. Verify Petrov's ARIES protocol adaptation for LSM engines against Chapter 10.

---
<!-- toc -->
---
## Context

The WAL ensures every committed operation is recorded on durable storage. After a crash, the engine must use that log to return to a consistent state. This is the job of the **crash recovery** protocol — a deterministic procedure that reads the WAL, determines what was lost, and reconstructs the in-memory state.

For the OOR's LSM engine, recovery is simpler than for a traditional B-tree database because SSTables are immutable. There are no dirty pages to redo or uncommitted transactions to undo — the only volatile state is the memtable. Recovery reconstructs the memtable by replaying WAL records that were not yet flushed to an SSTable.

The classical protocol is **ARIES** (Algorithms for Recovery and Isolation Exploiting Semantics). ARIES has three phases — analysis, redo, and undo. For the LSM engine, we adapt it: the analysis phase determines the recovery starting point, the redo phase replays the WAL into the memtable, and the undo phase is unnecessary (no uncommitted transactions to roll back at this stage).

---

## Core Concepts

### The Manifest

The **manifest** is a metadata file that records the LSM engine's durable state: which SSTables are active, what level each belongs to, and the highest flushed LSN. On recovery, the manifest is the starting point.

The manifest is itself an append-only log. Each entry records a state change:

```text
[1] AddSSTable { id: 1, level: 0, min_key: "a", max_key: "z", flushed_lsn: 500 }
[2] AddSSTable { id: 2, level: 0, min_key: "b", max_key: "y", flushed_lsn: 1000 }
[3] Compaction { removed: [1, 2], added: [3], output_level: 1, flushed_lsn: 1000 }
```

### LSM Recovery Protocol

1. **Read the manifest.** Reconstruct the active SSTable set and determine the highest flushed LSN.

2. **Open the WAL.** Seek to the first record with LSN > flushed_lsn.

3. **Replay WAL records.** For each valid record, apply it to a fresh memtable:
   - `Put` → insert the key-value pair
   - `Delete` → insert a tombstone

4. **Stop at corruption.** If a record's checksum fails, stop replaying. That record and everything after it are discarded.

5. **Resume normal operation.** The memtable now contains all committed-but-unflushed operations.

```text
Recovery timeline:
  [SSTables on disk]  [WAL on disk]
  ├─ flushed to LSN 1000 ─┤─ LSN 1001..1234 valid ─┤─ LSN 1235 corrupt ─┤
                           │                         │
                    Replay into memtable          Stop here
```

### Crash During Compaction

If the engine crashes mid-compaction:

- **Before manifest update:** Old SSTables are still listed as active. Partially-written new SSTables are orphaned files. Recovery uses the old SSTables. Orphans are cleaned up.
- **After manifest update:** New SSTables are active. Old SSTables are marked for deletion. Recovery uses the new SSTables.

The manifest update is the atomicity boundary. The pattern: write data first, then atomically update metadata.

### Crash During Flush

If the engine crashes mid-flush:

- **Before manifest update:** The partial SSTable is orphaned. The WAL still contains all records. Recovery replays the WAL.
- **After manifest update:** The SSTable is complete and active. WAL records up to the flushed LSN are redundant.

### Orphan Cleanup

On startup, the engine scans the data directory for SSTable files not referenced by the manifest. These are orphans from interrupted compactions or flushes. They are deleted before normal operation begins.

---

## Code Examples

### LSM Engine Recovery

```rust,ignore
impl LsmEngine {
    fn recover(db_path: &str) -> io::Result<Self> {
        // Phase 1: Read manifest
        let (sst_state, flushed_lsn) = ManifestReader::replay(
            &format!("{}/MANIFEST", db_path)
        )?;
        eprintln!("Recovery: {} SSTables, flushed LSN = {}", sst_state.total_count(), flushed_lsn);

        // Phase 2: Replay WAL
        let mut memtable = MemTable::new(16 * 1024 * 1024);
        let mut replayed = 0u64;
        let mut next_lsn = flushed_lsn + 1;

        if let Ok(mut reader) = WalReader::open(&format!("{}/WAL", db_path)) {
            loop {
                match reader.next_record() {
                    Ok(Some(record)) => {
                        if record.lsn <= flushed_lsn { continue; }
                        match record.rec_type {
                            WalRecordType::Put => memtable.put(record.key, record.value),
                            WalRecordType::Delete => memtable.delete(record.key),
                        }
                        next_lsn = record.lsn + 1;
                        replayed += 1;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        eprintln!("Recovery: corrupt record ({}), {} replayed", e, replayed);
                        break;
                    }
                }
            }
        }
        eprintln!("Recovery: replayed {} WAL records", replayed);

        // Phase 3: Clean up orphaned SSTables
        cleanup_orphans(db_path, &sst_state)?;

        // Phase 4: Open fresh WAL and manifest for new writes
        let wal = WalWriter::open(&format!("{}/WAL", db_path))?;
        let manifest = ManifestWriter::open(&format!("{}/MANIFEST", db_path))?;

        Ok(Self { active_memtable: memtable, immutable_memtables: Vec::new(),
                  sst_state, wal, manifest, next_lsn })
    }
}

fn cleanup_orphans(db_path: &str, sst_state: &LsmState) -> io::Result<()> {
    let active_ids: std::collections::HashSet<u64> = sst_state.all_sst_ids().collect();
    for entry in std::fs::read_dir(db_path)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with("sst_") && name.ends_with(".dat") {
            let id: u64 = name[4..name.len()-4].parse().unwrap_or(u64::MAX);
            if !active_ids.contains(&id) {
                eprintln!("Recovery: deleting orphaned SSTable {}", name);
                std::fs::remove_file(entry.path())?;
            }
        }
    }
    Ok(())
}
```

The recovery procedure is deterministic: given the same manifest and WAL files, it always produces the same memtable state.

---

## Key Takeaways

- LSM crash recovery is simpler than B-tree recovery because SSTables are immutable. The only volatile state to reconstruct is the memtable.
- The manifest tracks active SSTables and the flushed LSN. It is the recovery starting point and the atomicity boundary for compaction and flush operations.
- Recovery replays WAL records with LSN > flushed_lsn into a fresh memtable. Partial writes are detected by checksum failure and discarded.
- The pattern "write data, then atomically update metadata" applies to both flushes and compactions. The manifest update is the commit point.
- Orphan cleanup on startup removes SSTable files from interrupted operations that were never recorded in the manifest.

---

{{#quiz lesson-02-quiz.toml}}
