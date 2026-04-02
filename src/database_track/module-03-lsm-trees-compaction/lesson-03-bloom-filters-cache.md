# Lesson 3 — Bloom Filters, Block Cache, and Read Optimization

**Module:** Database Internals — M03: LSM Trees & Compaction  
**Position:** Lesson 3 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapters 7–8; [Mini-LSM Week 1 Day 7](https://skyzh.github.io/mini-lsm/week1-overview.html)

> **Source note:** This lesson was synthesized from training knowledge. Verify the bloom filter bits-per-key formula and Petrov's block cache eviction policies against the source texts.

---

## Context

The LSM read path from Lesson 1 checks every SSTable from newest to oldest. For a database with 50 SSTables, a negative lookup (key doesn't exist) requires 50 SSTable probes — each involving reading an index block and potentially a data block from disk. Even with leveled compaction limiting the effective probe count to one SSTable per level, a 4-level LSM still reads 4 SSTables per negative lookup.

Two optimizations close the read performance gap between LSM trees and B+ trees:

1. **Bloom filters** — a probabilistic data structure attached to each SSTable that answers "is this key possibly in this SSTable?" in O(1) with no disk I/O. A "no" answer is definitive (the key is definitely not there), so the engine skips the entire SSTable. With a 1% false positive rate, 99% of unnecessary SSTable probes are eliminated.

2. **Block cache** — an in-memory cache of recently-read SSTable data blocks and index blocks. Hot blocks stay in memory, eliminating disk reads for frequently-accessed keys. Combined with index block pinning (keeping all index blocks in memory permanently), this makes most SSTable lookups a single data block read.

Together, these two techniques reduce the practical I/O cost of an LSM point lookup to approximately 1 disk read — competitive with the B+ tree.

---

## Core Concepts

### Bloom Filters

A bloom filter is a bit array of `m` bits with `k` independent hash functions. To **add** a key, hash it with all `k` functions and set the corresponding bits. To **check** a key, hash it and verify all `k` bits are set. If any bit is 0, the key is definitely not in the set. If all bits are 1, the key is *probably* in the set (but might be a false positive).

The false positive probability for a bloom filter with `m` bits, `k` hash functions, and `n` inserted keys is approximately:

```
FPR ≈ (1 - e^(-kn/m))^k
```

The optimal number of hash functions for a given `m/n` (bits per key) ratio is `k = (m/n) × ln(2)`.

**Practical sizing for the OOR:**

| Bits per key | False positive rate | Memory per 10,000 keys |
|-------------|--------------------|-----------------------|
| 5 | 9.2% | 6.1 KB |
| 10 | 0.82% | 12.2 KB |
| 14 | 0.08% | 17.1 KB |
| 20 | 0.0006% | 24.4 KB |

**10 bits per key** is the standard choice (RocksDB default) — it provides a ~1% false positive rate at modest memory cost. For the OOR's 100,000 keys, the bloom filter per SSTable adds ~122 KB, trivially small compared to the SSTable data itself.

A bloom filter is stored in the SSTable's **meta block** and loaded into memory when the SSTable is opened. It is never written to disk again — it's read-only once created. This means the filter is computed during the SSTable build (compaction or flush) and persists for the SSTable's lifetime.

### Bloom Filter in the Read Path

When the engine needs to check an SSTable for a key:

1. Consult the in-memory bloom filter for that SSTable.
2. If the filter says "no" → skip the SSTable entirely. Zero disk I/O.
3. If the filter says "maybe" → read the index block, find the candidate data block, read the data block, and search for the key.

For a negative lookup across 50 SSTables with a 1% FPR:
- Without bloom filters: 50 SSTable probes (50 index reads + up to 50 data reads).
- With bloom filters: ~0.5 SSTable probes on average (50 × 0.01 = 0.5 false positives).

This transforms the LSM's worst-case operation into a near-best-case: negative lookups, which were O(L × SSTables) disk reads, become O(0.5) disk reads on average.

### Block Cache

The **block cache** is an LRU cache (similar to the buffer pool from Module 1) that stores recently-accessed SSTable blocks in memory. Unlike the buffer pool, which caches full pages, the block cache stores individual SSTable blocks (typically 4KB) keyed by `(sst_id, block_offset)`.

Two categories of blocks are cached:

- **Data blocks** — the actual key-value data. Cached on demand (when a read hits the block).
- **Index blocks** — the SSTable's internal index mapping last-key-per-block to block offset. Frequently accessed (every point lookup into the SSTable reads the index block first).

Many engines **pin** index blocks and filter blocks in the cache — they are loaded when the SSTable is opened and never evicted. This guarantees that every SSTable lookup requires at most one disk read (for the data block), because the filter and index are always in memory.

The block cache sits above the OS page cache and provides the engine with workload-aware caching. Like the buffer pool, it exists because the OS page cache doesn't understand SSTable access patterns — it can't distinguish between a hot data block and a cold compaction input.

### Prefix Bloom Filters

Standard bloom filters answer "is this exact key in the SSTable?" For range scans, you need a different question: "does this SSTable contain any keys with this prefix?" A **prefix bloom filter** hashes key prefixes instead of full keys, enabling prefix-based filtering.

For the OOR, a prefix bloom on the first 3 bytes of the NORAD ID would let range scans skip SSTables that don't contain any keys in the target range. The false positive rate is higher (more keys share a prefix than match exactly), but the I/O savings for range scans are significant.

### Combining Optimizations: End-to-End Read Path

A fully optimized LSM point lookup:

1. Check active memtable (in-memory, O(log N)).
2. Check immutable memtables (in-memory, O(log N) each).
3. For each SSTable, newest to oldest:
   a. Check the bloom filter (in-memory, O(k) hash operations). If negative → skip.
   b. Read the index block (in block cache → 0 disk I/O if pinned).
   c. Binary search the index block for the target data block.
   d. Read the data block (in block cache → 0 I/O if hot; 1 disk read if cold).
   e. Search the data block for the key.
4. First match (value or tombstone) terminates the search.

For a positive lookup on a hot key: 0 disk reads (everything in cache). For a negative lookup: 0 disk reads (bloom filters reject all SSTables). For a cold positive lookup: 1 disk read (the data block; index and filter are pinned).

---

## Code Examples

### A Simple Bloom Filter for SSTable Key Filtering

This implements the core bloom filter operations — build during SSTable creation, query during reads.

```rust,ignore
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// A bloom filter for probabilistic key membership testing.
struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
    num_hashes: u32,
}

impl BloomFilter {
    /// Create a bloom filter sized for `num_keys` with the given
    /// bits-per-key ratio. Optimal hash count is computed automatically.
    fn new(num_keys: usize, bits_per_key: usize) -> Self {
        let num_bits = num_keys * bits_per_key;
        let num_bytes = (num_bits + 7) / 8;
        // Optimal k = bits_per_key * ln(2) ≈ bits_per_key * 0.693
        let num_hashes = ((bits_per_key as f64) * 0.693).ceil() as u32;
        let num_hashes = num_hashes.max(1).min(30); // Clamp to [1, 30]

        Self {
            bits: vec![0u8; num_bytes],
            num_bits,
            num_hashes,
        }
    }

    /// Add a key to the bloom filter.
    fn insert(&mut self, key: &[u8]) {
        for i in 0..self.num_hashes {
            let bit_pos = self.hash(key, i) % self.num_bits;
            self.bits[bit_pos / 8] |= 1 << (bit_pos % 8);
        }
    }

    /// Check if a key might be in the set.
    /// Returns false → definitely not in the set.
    /// Returns true → possibly in the set (check the SSTable).
    fn may_contain(&self, key: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let bit_pos = self.hash(key, i) % self.num_bits;
            if self.bits[bit_pos / 8] & (1 << (bit_pos % 8)) == 0 {
                return false; // Definitive: key is NOT in the set
            }
        }
        true // All bits set — key is PROBABLY in the set
    }

    /// Generate the i-th hash of a key using double hashing.
    /// h(i) = h1 + i * h2, where h1 and h2 are independent hashes.
    fn hash(&self, key: &[u8], i: u32) -> usize {
        let mut h1 = DefaultHasher::new();
        key.hash(&mut h1);
        let hash1 = h1.finish();

        let mut h2 = DefaultHasher::new();
        // Mix in a constant to get an independent second hash
        (key, 0xDEADBEEFu32).hash(&mut h2);
        let hash2 = h2.finish();

        (hash1.wrapping_add((i as u64).wrapping_mul(hash2))) as usize
    }

    /// Serialize the bloom filter for storage in the SSTable meta block.
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + 4 + self.bits.len());
        buf.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        buf.extend_from_slice(&self.num_hashes.to_le_bytes());
        buf.extend_from_slice(&self.bits);
        buf
    }
}
```

The **double hashing** technique generates `k` hash values from just two base hashes: `h(i) = h1 + i × h2`. This is mathematically equivalent to using `k` independent hash functions for bloom filter purposes (proven by Kirsch and Mitzenmacher, 2006) and much cheaper to compute.

The `DefaultHasher` is SipHash, which is well-distributed but not the fastest. Production bloom filters use xxHash or wyhash for speed. The algorithm is the same regardless of hash function — only throughput changes.

### Integrating the Bloom Filter into SSTable Reads

Modifying the LSM read path to consult bloom filters before reading SSTable blocks.

```rust,ignore
/// Check an SSTable for a key, using the bloom filter to skip if possible.
fn check_sstable(
    sst: &SsTableReader,
    key: &[u8],
    block_cache: &mut BlockCache,
) -> io::Result<Option<Option<Vec<u8>>>> {
    // Step 1: Bloom filter check (in-memory, zero I/O)
    if !sst.bloom_filter.may_contain(key) {
        return Ok(None); // Definitely not in this SSTable
    }

    // Step 2: Index block lookup (cached or pinned, usually zero I/O)
    let block_handle = sst.find_block_for_key(key, block_cache)?;

    // Step 3: Data block read (1 disk read if not cached)
    let data_block = block_cache.get_or_load(
        sst.id,
        block_handle.offset,
        block_handle.size,
        &sst.file,
    )?;

    // Step 4: Search the data block for the key
    match data_block.find(key) {
        Some(entry) => Ok(Some(entry.value.clone())), // Found (value or tombstone)
        None => Ok(None), // Key not in this block (bloom filter false positive)
    }
}
```

When the bloom filter returns `false` (line 4), the entire SSTable is skipped — no index read, no data read, no disk I/O. This is the single biggest read-path optimization in the LSM architecture.

When the bloom filter returns `true` but the key isn't actually in the SSTable (false positive), the engine performs an unnecessary index + data block read. At 1% FPR, this happens once per 100 negative probes per SSTable — rare enough to be negligible.

---

## Key Takeaways

- Bloom filters eliminate 99% of unnecessary SSTable probes for negative lookups at 10 bits per key. This transforms the LSM's worst case (negative lookups checking every SSTable) into a near-zero-I/O operation.
- The false positive rate is tunable via bits-per-key: 10 bits → ~1% FPR, 14 bits → ~0.1% FPR. The OOR should use 10 bits per key as the default, matching RocksDB's default.
- Block cache stores recently-accessed SSTable blocks in memory. Pinning index and filter blocks guarantees that every SSTable lookup costs at most 1 disk read (for the data block).
- The fully optimized LSM read path: bloom filter (in-memory) → index block (pinned in cache) → data block (cached or 1 disk read). For hot keys, this is 0 disk reads — competitive with B+ tree performance.
- Prefix bloom filters extend filtering to range scans by hashing key prefixes instead of full keys. Higher false positive rate but significant I/O savings for prefix-based range queries.

---

{{#quiz lesson-03-quiz.toml}}
