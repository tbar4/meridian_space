# Lesson 2 — Compaction Strategies

**Module:** Database Internals — M03: LSM Trees & Compaction  
**Position:** Lesson 2 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapter 7; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 3; [Mini-LSM Week 2](https://skyzh.github.io/mini-lsm/week2-overview.html)

> **Source note:** This lesson was synthesized from training knowledge and the Mini-LSM course compaction chapters. Verify the specific amplification formulas against Petrov Chapter 7 and the RocksDB Tuning Guide.

---
<!-- toc -->
---
## Context

Without compaction, the LSM engine accumulates SSTables indefinitely. Every flush creates a new SSTable in Level 0 (L0). After 100 flushes, there are 100 L0 SSTables — and a point lookup must check all of them. Read amplification grows linearly with the number of SSTables. Space amplification grows too: deleted keys still consume space in older SSTables, and updated keys have multiple versions.

**Compaction** is the background process that merges SSTables to reduce read and space amplification. It reads multiple SSTables, merge-sorts their entries (applying tombstones and keeping only the newest version of each key), and writes the result as fewer, larger SSTables. The merged input files are then deleted.

The compaction strategy determines *which* SSTables to merge and *when*. Different strategies make different tradeoffs between three amplification factors:

- **Read amplification (RA):** How many SSTables must be checked for a single read. Lower RA = faster reads.
- **Write amplification (WA):** How many times each byte of user data is written to disk over its lifetime. Lower WA = faster ingestion.
- **Space amplification (SA):** How much extra disk space is used beyond the logical data size. Lower SA = less storage cost.

No strategy minimizes all three simultaneously — this is the fundamental tradeoff of LSM design. Choosing a strategy means choosing which factor to sacrifice for the workload at hand.

---

## Core Concepts

### Level 0 and the Flush Problem

When a memtable is flushed, it becomes an L0 SSTable. L0 is special: its SSTables have **overlapping key ranges** (each SSTable contains whatever keys were in the memtable at freeze time, which can be any subset of the key space). This means a point lookup at L0 must check *every* L0 SSTable — there's no way to narrow the search by key range.

All compaction strategies share a common first step: merge L0 SSTables into L1, where SSTables have **non-overlapping key ranges**. In L1 and below, a point lookup can determine which SSTable(s) to check based on key range alone (binary search on SSTable boundaries), reducing read amplification.

```text
L0: [a-z] [b-y] [c-x]    ← overlapping, must check all
L1: [a-f] [g-m] [n-z]    ← non-overlapping, check at most 1
L2: [a-c] [d-f] [g-i] ...← non-overlapping, check at most 1
```

### Leveled Compaction

**Leveled compaction** (default in RocksDB, used by LevelDB) organizes SSTables into levels with exponentially increasing size targets. Each level's total size is a fixed multiple (the **size ratio**, typically 10) of the level above:

- L1 target: 256MB
- L2 target: 2.56GB (10 × L1)
- L3 target: 25.6GB (10 × L2)

When a level exceeds its target size, the engine picks one SSTable from that level and merges it with all overlapping SSTables in the next level.

**Key property:** Within each level (L1+), SSTables have non-overlapping key ranges. This means at most one SSTable per level needs to be checked for a point lookup.

**Amplification characteristics:**
- **Read amplification:** O(L) where L is the number of levels. With size ratio 10, a 1TB database has ~4 levels → ~4 SSTable reads per lookup. Excellent.
- **Write amplification:** High — in the worst case, a single SSTable is rewritten once per level transition. With size ratio 10 and 4 levels, write amplification is approximately `10 × L = 40x`. Each byte of data is rewritten ~10 times per level hop.
- **Space amplification:** Low — each key has at most one copy per level, and compaction removes obsolete versions. Typically 1.1–1.2x.

Leveled compaction is optimal for read-heavy workloads with limited tolerance for space overhead — exactly the OOR's conjunction query workload after the initial bulk ingestion.

### Tiered (Universal) Compaction

**Tiered compaction** (RocksDB's "Universal" mode, used by Cassandra) groups SSTables into **tiers** (sorted runs) of similar size. When the number of tiers at a size level reaches a threshold, they are merged into a single larger tier.

**Key property:** Each tier is a sorted run (non-overlapping internally), but different tiers can overlap with each other. A read must check one SSTable per tier.

**Amplification characteristics:**
- **Read amplification:** O(T) where T is the number of tiers. Worse than leveled because tiers overlap.
- **Write amplification:** Low — each compaction merges tiers of similar size, so each byte is rewritten fewer times. Typically 2–5x for well-configured tiering.
- **Space amplification:** High — during compaction, both the input tiers and the output tier exist simultaneously, requiring up to 2x the logical data size in temporary space. Permanent space amplification is also higher because multiple tiers may hold different versions of the same key.

Tiered compaction is optimal for write-heavy workloads where ingestion throughput matters more than read latency — exactly the OOR's burst ingestion during a fragmentation event.

### FIFO Compaction

**FIFO compaction** simply deletes the oldest SSTable when total storage exceeds a threshold. No merging occurs. This is appropriate for time-series data with a natural expiration window — if the OOR only needs TLE records from the last 7 days, FIFO compaction automatically ages out older data.

**Amplification characteristics:**
- **Read amplification:** High — all SSTables persist until aged out.
- **Write amplification:** 1x — data is written once (the initial flush) and never rewritten.
- **Space amplification:** Bounded by the retention window.

FIFO is unsuitable for the OOR's catalog (which must retain all active objects indefinitely) but useful for the telemetry stream (which has natural time-based expiration).

### Amplification Tradeoff Summary

| Strategy | Read Amp | Write Amp | Space Amp | Best For |
|----------|----------|-----------|-----------|----------|
| Leveled | Low (O(L)) | High (~10L) | Low (~1.1x) | Read-heavy, space-sensitive |
| Tiered | Medium (O(T)) | Low (~2-5x) | High (~2x) | Write-heavy, burst ingestion |
| FIFO | High | Minimal (1x) | Time-bounded | TTL data, append-only streams |

The RocksDB wiki summarizes the tradeoff space clearly: it is generally impossible to minimize all three amplification factors simultaneously. The compaction strategy is a knob that slides between them.

### Compaction Mechanics: The Merge Process

Regardless of strategy, the actual compaction operation is the same:

1. Select input SSTables (determined by the strategy).
2. Create a merge iterator over all input SSTables.
3. For each key in sorted order:
   - If multiple versions exist, keep only the newest.
   - If the newest version is a tombstone *and* there are no older SSTables that might contain the key, drop the tombstone (garbage collection).
   - Otherwise, write the entry to the output SSTable(s).
4. Split output into new SSTables when they reach the target file size.
5. Atomically update the LSM metadata to swap old SSTables for new ones.
6. Delete the old input SSTables.

Step 5 is critical for crash safety — if the engine crashes during compaction, it must not lose data. Either the old SSTables or the new ones should be the active set, never a mix. This is typically handled by writing a **manifest** (metadata log) that records which SSTables are active, and updating it atomically (via rename or WAL).

### Compaction Scheduling

Compaction runs in background threads and must be scheduled carefully:

- **Too little compaction:** Read amplification grows as uncompacted SSTables accumulate.
- **Too much compaction:** Write bandwidth is consumed by compaction, starving foreground writes.
- **Compaction during peak load:** The background I/O from compaction interferes with foreground query latency.

Production engines use rate limiting (RocksDB's `rate_limiter`) to cap compaction I/O bandwidth, and priority scheduling to defer compaction during high-load periods. The SILK paper (USENIX ATC '19) formalized this as a latency-aware compaction scheduler.

For the OOR, the practical guideline: run compaction aggressively during quiet periods (between pass windows) and throttle during conjunction query bursts.

---

## Code Examples

### A Simple Leveled Compaction Controller

This determines which SSTables to compact and when, based on level size targets.

```rust,ignore
/// Metadata for an SSTable in the LSM state.
#[derive(Debug, Clone)]
struct SstMeta {
    id: u64,
    level: usize,
    size_bytes: u64,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
}

/// LSM state: tracks all active SSTables by level.
struct LsmState {
    /// L0 SSTables (overlapping key ranges, newest first)
    l0_sstables: Vec<SstMeta>,
    /// L1+ levels: each level is a vec of non-overlapping SSTables sorted by key range
    levels: Vec<Vec<SstMeta>>,
    /// Size ratio between adjacent levels (typically 10)
    size_ratio: u64,
    /// L1 target size in bytes
    l1_target_bytes: u64,
}

/// A compaction task: which SSTables to merge and where to put the output.
struct CompactionTask {
    input_ssts: Vec<SstMeta>,
    output_level: usize,
}

impl LsmState {
    /// Determine if compaction is needed and generate a task.
    fn generate_compaction_task(&self) -> Option<CompactionTask> {
        // Priority 1: Too many L0 SSTables (merge all L0 into L1)
        if self.l0_sstables.len() >= 4 {
            let mut inputs: Vec<SstMeta> = self.l0_sstables.clone();
            // Include all L1 SSTables that overlap with any L0 SSTable
            let l0_min = inputs.iter().map(|s| &s.min_key).min().unwrap().clone();
            let l0_max = inputs.iter().map(|s| &s.max_key).max().unwrap().clone();
            if let Some(l1) = self.levels.get(0) {
                for sst in l1 {
                    if sst.max_key >= l0_min && sst.min_key <= l0_max {
                        inputs.push(sst.clone());
                    }
                }
            }
            return Some(CompactionTask {
                input_ssts: inputs,
                output_level: 1,
            });
        }

        // Priority 2: A level exceeds its target size
        for (i, level) in self.levels.iter().enumerate() {
            let level_num = i + 1; // levels[0] = L1
            let target = self.l1_target_bytes * self.size_ratio.pow(i as u32);
            let actual: u64 = level.iter().map(|s| s.size_bytes).sum();

            if actual > target {
                // Pick the SSTable with the most overlap in the next level
                // (simplified: pick the first SSTable)
                if let Some(sst) = level.first() {
                    let mut inputs = vec![sst.clone()];
                    // Add overlapping SSTables from the next level
                    if let Some(next_level) = self.levels.get(i + 1) {
                        for next_sst in next_level {
                            if next_sst.max_key >= sst.min_key
                                && next_sst.min_key <= sst.max_key
                            {
                                inputs.push(next_sst.clone());
                            }
                        }
                    }
                    return Some(CompactionTask {
                        input_ssts: inputs,
                        output_level: level_num + 1,
                    });
                }
            }
        }

        None // No compaction needed
    }
}
```

The L0-to-L1 compaction merges *all* L0 SSTables with the overlapping portion of L1. This is the most expensive compaction operation (L0 SSTables overlap each other, so the entire key range may be involved), but it's necessary to establish the non-overlapping property at L1.

The level-to-level compaction picks a single SSTable from the overfull level and merges it with the overlapping SSTables in the next level. Production implementations (RocksDB) cycle through SSTables in key order to ensure uniform compaction across the key space, preventing hot spots.

---

## Key Takeaways

- Compaction is the LSM engine's background maintenance process — it merges SSTables to reduce read amplification and space amplification at the cost of write amplification.
- The three amplification factors (read, write, space) are fundamentally in tension. No compaction strategy minimizes all three. Leveled compaction favors reads; tiered favors writes; FIFO favors write throughput for TTL data.
- Leveled compaction organizes SSTables into levels with non-overlapping key ranges and exponentially increasing size targets. Write amplification of ~10x per level is the cost of O(L) read amplification.
- Tiered compaction groups SSTables into sorted runs of similar size. Write amplification of 2-5x is the reward for tolerating higher read and space amplification.
- L0 is special: SSTables have overlapping key ranges and must all be checked on every read. Flushing L0 to L1 is the highest-priority compaction task.
- Compaction scheduling must balance background I/O against foreground query latency. Throttle during peak load, compact aggressively during quiet periods.

---

{{#quiz lesson-02-quiz.toml}}
