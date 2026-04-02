# Lesson 3 — Checkpointing

**Module:** Database Internals — M04: Write-Ahead Logging & Recovery  
**Position:** Lesson 3 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapter 10

> **Source note:** This lesson was synthesized from training knowledge. Verify Petrov's fuzzy checkpoint algorithm and his WAL truncation semantics against Chapter 10.

---

## Context

Without checkpointing, the WAL grows indefinitely. If the engine has been running for 24 hours with 100,000 TLE updates, the WAL contains 100,000 records — all of which must be scanned during recovery to find those with LSN > flushed_lsn. Recovery time grows linearly with WAL size. For a system that must return to service within seconds after a crash (the OOR's conjunction avoidance SLA requires <30s recovery), unbounded WAL growth is unacceptable.

A **checkpoint** snapshots the current LSM state and records the position where recovery should start. After a checkpoint, WAL records before the checkpoint position can be safely deleted, bounding both WAL size and recovery time.

---

## Core Concepts

### What a Checkpoint Records

A checkpoint writes the following to the manifest:

1. **Checkpoint LSN** — the highest LSN that is fully durable (either in an SSTable or committed to the WAL and fsync'd at checkpoint time).
2. **Active SSTable list** — every SSTable currently in the LSM state, with level assignments.
3. **Memtable status** — the LSN range of the current active memtable (not yet flushed).

After the checkpoint, the WAL can be truncated up to the **minimum recovery LSN**: the smallest LSN that might still need replay. This is the lower bound of the active memtable's LSN range at checkpoint time.

### Fuzzy Checkpoints

A **sharp checkpoint** freezes all writes, flushes the memtable, records the state, and then resumes. This guarantees that the checkpoint LSN is fully consistent — but it blocks writes for the duration of the flush (potentially seconds).

A **fuzzy checkpoint** avoids blocking writes:

1. Record the current memtable's LSN range and the active SSTable list.
2. Write this snapshot to the manifest.
3. Continue accepting writes — the memtable keeps growing.

The tradeoff: recovery after a fuzzy checkpoint must replay WAL records from the memtable's start LSN (not the checkpoint LSN), because the memtable was not flushed at checkpoint time. Fuzzy checkpoints are faster (no flush) but result in slightly longer recovery (more WAL records to replay).

In the LSM architecture, fuzzy checkpoints are natural: the memtable flush *is* a form of checkpointing. Every time a memtable is flushed to an SSTable, the flushed LSN advances, and older WAL records become eligible for truncation. Explicit checkpoints are only needed if flush intervals are very long.

### WAL Truncation

After a checkpoint (or flush), WAL records below the minimum recovery LSN can be deleted. Two strategies:

**Segment-based:** The WAL is split into fixed-size segments (e.g., 64MB files). A segment can be deleted when all its records have LSN ≤ the minimum recovery LSN. Simple and efficient — the filesystem handles cleanup.

**Single-file with logical truncation:** The WAL is one file. A "truncation point" is maintained in the manifest. On recovery, records before this point are skipped. The file is physically truncated (or rewritten) during periodic maintenance.

Segment-based is the standard approach (used by RocksDB, Kafka, PostgreSQL's WAL segments). It avoids the complexity of in-place truncation and enables simple space reclamation.

### Recovery Time Analysis

Recovery time = time to read manifest + time to replay WAL records.

Manifest replay is fast (typically <100 entries). WAL replay dominates: each record requires deserialization and a memtable insert. At ~1µs per memtable insert and 100,000 records to replay, recovery takes ~100ms for the replay phase.

Checkpointing bounds this: with checkpoints every 60 seconds and 3,000 writes/sec, the maximum WAL replay is ~180,000 records = ~180ms. Well within the OOR's 30-second recovery SLA.

### Coordinating Checkpoints with Compaction

Checkpoints and compaction both modify the manifest. To prevent conflicts:

1. Acquire a manifest lock before writing a checkpoint or compaction result.
2. Write the manifest entry.
3. Release the lock.

The lock is held briefly (one file write + fsync), so contention is low. The manifest itself is append-only, so there are no conflicting edits — only the ordering of entries matters.

---

## Code Examples

### Checkpoint Implementation

```rust,ignore
impl LsmEngine {
    /// Write a fuzzy checkpoint to the manifest.
    /// This records the current LSM state without blocking writes.
    fn checkpoint(&mut self) -> io::Result<()> {
        // Snapshot the current state
        let active_ssts: Vec<SstMeta> = self.sst_state.all_sstables().cloned().collect();
        let memtable_min_lsn = self.active_memtable_min_lsn();
        let checkpoint_lsn = self.next_lsn - 1;

        // Write checkpoint to manifest
        self.manifest.write_checkpoint(CheckpointEntry {
            checkpoint_lsn,
            memtable_min_lsn,
            active_sstables: active_ssts.iter().map(|s| s.id).collect(),
        })?;
        self.manifest.sync()?;

        eprintln!(
            "Checkpoint at LSN {}: {} active SSTables, \
             WAL replay starts at LSN {}",
            checkpoint_lsn, active_ssts.len(), memtable_min_lsn,
        );

        // Truncate WAL segments that are fully below memtable_min_lsn
        self.wal.truncate_before(memtable_min_lsn)?;

        Ok(())
    }

    fn active_memtable_min_lsn(&self) -> u64 {
        // The earliest LSN in the active memtable is the minimum
        // recovery point — WAL records before this are redundant.
        // If the memtable is empty, use the flushed LSN.
        self.active_memtable
            .min_lsn()
            .unwrap_or(self.sst_state.flushed_lsn())
    }
}
```

The checkpoint does not flush the memtable — it records where the memtable starts (min LSN) so recovery knows where to begin WAL replay. This is the "fuzzy" part: writes continue during and after the checkpoint, but the WAL truncation point is safely advanced.

### WAL Segment Manager

```rust,ignore
/// Manages WAL as a series of fixed-size segments for clean truncation.
struct WalSegmentManager {
    dir: String,
    segment_size: usize,
    active_segment: WalWriter,
    active_segment_id: u64,
}

impl WalSegmentManager {
    /// Truncate (delete) all WAL segments whose max LSN is below the given LSN.
    fn truncate_before(&mut self, min_recovery_lsn: u64) -> io::Result<()> {
        let entries = std::fs::read_dir(&self.dir)?;
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with("wal_") && name.ends_with(".log") {
                // Parse segment ID from filename: wal_000042.log → 42
                let seg_id = name[4..10].parse::<u64>().unwrap_or(u64::MAX);
                // Each segment covers a known LSN range.
                // Conservative: only delete if segment_max_lsn < min_recovery_lsn
                if self.segment_max_lsn(seg_id) < min_recovery_lsn {
                    std::fs::remove_file(entry.path())?;
                    eprintln!("WAL: deleted segment {}", name);
                }
            }
        }
        Ok(())
    }

    fn segment_max_lsn(&self, segment_id: u64) -> u64 {
        // In practice, track this in memory or in the segment header.
        // Simplified: assume segments hold a known max number of records.
        (segment_id + 1) * (self.segment_size as u64 / 103) // ~records per segment
    }
}
```

Segment-based truncation is simple: delete files whose records are all below the recovery starting point. No in-place file modification, no complex bookkeeping. The filesystem handles space reclamation.

---

## Key Takeaways

- Checkpoints bound WAL size and recovery time by recording a recovery starting point. Without checkpoints, the WAL grows indefinitely and recovery scans the entire log.
- Fuzzy checkpoints avoid blocking writes — they snapshot the LSM state without flushing the memtable. The tradeoff is slightly longer recovery (WAL replay from the memtable's start LSN, not the checkpoint LSN).
- In an LSM engine, every memtable flush is implicitly a checkpoint — it advances the flushed LSN and makes earlier WAL records eligible for truncation.
- WAL segments (fixed-size log files) enable clean truncation by deleting entire segment files. This is simpler and more efficient than truncating a single growing file.
- Recovery time for the OOR: ~180ms worst case with 60-second checkpoint intervals and 3,000 writes/sec. Well within the 30-second conjunction avoidance SLA.

---

{{#quiz lesson-03-quiz.toml}}
