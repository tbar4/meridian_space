# Lesson 1 — Memtables and Sorted String Tables

**Module:** Database Internals — M03: LSM Trees & Compaction  
**Position:** Lesson 1 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapter 7; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 3; [Mini-LSM Week 1](https://skyzh.github.io/mini-lsm/week1-overview.html)

> **Source note:** This lesson was synthesized from training knowledge and the Mini-LSM course structure. Verify Petrov's specific SSTable format description and Kleppmann's LSM compaction cost analysis against the source texts.

---

## Context

The B+ tree from Module 2 provides O(log N) reads but suffers under write-heavy workloads: every insert modifies a leaf page in place, and node splits amplify writes further. For the Orbital Object Registry's burst ingestion scenario (8,000 new objects in 45 minutes), the B+ tree's write amplification of 10–20x is the bottleneck.

The **Log-Structured Merge Tree (LSM)** eliminates in-place updates entirely. All writes go to an in-memory sorted structure called a **memtable**. When the memtable reaches a size threshold, it is frozen (becomes **immutable**) and flushed to disk as a **Sorted String Table (SSTable)** — a file of sorted key-value pairs that is never modified after creation. Reads probe the memtable first, then search SSTables from newest to oldest.

This architecture makes writes trivially fast: inserting a key-value pair is a single in-memory operation (an insert into a skip list or B-tree in RAM). The cost is shifted to reads, which must check multiple SSTables, and to background compaction, which merges SSTables to keep read amplification bounded. The entire LSM design is a bet that write throughput matters more than read latency for many workloads — and for TLE burst ingestion, it does.

---

## Core Concepts

### The Memtable

The memtable is a mutable, in-memory sorted data structure that buffers incoming writes. Common implementations:

- **Skip list** (used by LevelDB, RocksDB): O(log N) insert and lookup, lock-free concurrent reads, good cache behavior. The standard choice.
- **Red-black tree or B-tree in memory**: O(log N) operations, but harder to make lock-free.
- **Sorted vector**: O(N) insert (shift), O(log N) lookup. Only viable for very small memtables.

For the OOR, a `BTreeMap<Vec<u8>, Vec<u8>>` is the simplest correct implementation. Production engines use skip lists for concurrent access, but the algorithm is the same: maintain sorted order in memory, flush to disk when full.

Key operations:

- **Put(key, value)**: Insert or update a key in the memtable. O(log N).
- **Delete(key)**: Insert a **tombstone** — a special marker that indicates the key has been deleted. The tombstone must persist through SSTables so that older versions of the key are masked.
- **Get(key)**: Look up a key. Returns the value, or the tombstone if deleted, or None if the key is not in the memtable.

The tombstone design is critical: a delete cannot simply remove the key from the memtable, because older SSTables on disk may still contain the key. Without a tombstone, a read would miss the memtable (key not present), then find the old value in an SSTable and incorrectly return it.

### Memtable Freeze and Flush

When the memtable reaches its size threshold (typically 4–64MB), the engine:

1. **Freezes** the current memtable — it becomes immutable (no more writes).
2. Creates a **new active memtable** for incoming writes.
3. In the background, **flushes** the immutable memtable to disk as an SSTable.

The freeze-then-flush pattern ensures writes are never blocked by disk I/O. The only latency-sensitive operation is the in-memory insert. Multiple immutable memtables can exist simultaneously (queued for flush), but each consumes memory, so the engine must flush faster than new memtables are created.

```text
Write path:
  Put(key, value)
       │
       ▼
  Active Memtable (mutable, in-memory)
       │ (size threshold reached)
       ▼
  Immutable Memtable (frozen, in-memory)
       │ (background flush)
       ▼
  SSTable on disk (sorted, immutable)
```

### SSTable Format

An SSTable is a file containing sorted key-value pairs organized into **data blocks**. The standard layout:

```text
┌─────────────────────────────────────────────┐
│ Data Block 0: [k0:v0, k1:v1, k2:v2, ...]   │
│ Data Block 1: [k3:v3, k4:v4, k5:v5, ...]   │
│ ...                                          │
│ Data Block N: [kM:vM, ...]                   │
├─────────────────────────────────────────────┤
│ Meta Block: Bloom filter (optional)          │
├─────────────────────────────────────────────┤
│ Index Block: [block_0_last_key → offset,     │
│               block_1_last_key → offset,     │
│               ...                          ] │
├─────────────────────────────────────────────┤
│ Footer: index_offset, meta_offset, magic     │
└─────────────────────────────────────────────┘
```

**Data blocks** (typically 4KB each) contain sorted key-value pairs. Keys within a block can use **prefix compression** — store the shared prefix once and encode each key as a delta.

**Index block** maps the last key of each data block to the block's file offset. A point lookup binary-searches the index block to find which data block might contain the key, then searches within that block.

**Meta block** stores auxiliary data — most importantly, a bloom filter for fast negative lookups (Lesson 3).

**Footer** is the last few bytes of the file, containing offsets to the index and meta blocks. The reader starts by reading the footer, then uses its offsets to locate everything else.

SSTables are **immutable** — once written, they are never modified. Updates and deletes are handled by writing new SSTables that supersede older ones. This immutability is the source of LSM's concurrency simplicity: readers can access any SSTable without locks (the file doesn't change under them), and the only coordination needed is between the flush/compaction writers and the metadata that tracks which SSTables are active.

### The Read Path

To read a key from an LSM engine:

1. Check the **active memtable**. If found, return immediately.
2. Check each **immutable memtable** from newest to oldest.
3. Check each **SSTable** from newest to oldest (L0, then L1, then L2, ...).
4. If the key is not found anywhere, it does not exist.

At each level, finding a tombstone means the key was deleted — stop searching and return "not found." This is why tombstones must be ordered correctly: a tombstone in a newer SSTable masks the key's value in all older SSTables.

The worst case is a negative lookup (key doesn't exist): the engine must check every memtable and every SSTable before concluding the key is absent. This is where bloom filters (Lesson 3) provide the biggest win — they let the engine skip entire SSTables in O(1) per filter check.

### The Merge Iterator

Range scans (and compaction) require merging sorted streams from multiple sources — the memtable and several SSTables. The **merge iterator** (also called a multi-way merge) takes N sorted iterators and produces a single sorted stream:

1. Maintain a min-heap of the current key from each source.
2. Pop the smallest key. If multiple sources have the same key, take the one from the newest source (the memtable or the most recent SSTable).
3. Advance the source that produced the popped key.

The newest-wins rule is what makes updates and deletes work correctly: a newer value for the same key supersedes the older one, and a newer tombstone masks the older value.

---

## Code Examples

### A Simple Memtable Backed by BTreeMap

The memtable stores key-value pairs in sorted order. Tombstones are represented as `None` values.

```rust,ignore
use std::collections::BTreeMap;

/// Memtable: in-memory sorted store for LSM writes.
/// Keys are byte slices. Values are `Option<Vec<u8>>` where
/// `None` represents a tombstone (deleted key).
struct MemTable {
    map: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    size_bytes: usize,
    size_limit: usize,
}

impl MemTable {
    fn new(size_limit: usize) -> Self {
        Self {
            map: BTreeMap::new(),
            size_bytes: 0,
            size_limit,
        }
    }

    /// Insert or update a key-value pair.
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let entry_size = key.len() + value.len();
        self.size_bytes += entry_size;
        self.map.insert(key, Some(value));
    }

    /// Mark a key as deleted (insert a tombstone).
    fn delete(&mut self, key: Vec<u8>) {
        let entry_size = key.len();
        self.size_bytes += entry_size;
        self.map.insert(key, None); // None = tombstone
    }

    /// Look up a key. Returns:
    /// - Some(Some(value)) if the key exists
    /// - Some(None) if the key is tombstoned (deleted)
    /// - None if the key is not in this memtable
    fn get(&self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.map.get(key)
    }

    /// True if the memtable has reached its size limit and should be frozen.
    fn should_flush(&self) -> bool {
        self.size_bytes >= self.size_limit
    }

    /// Iterate all entries in sorted order for flushing to an SSTable.
    fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Option<Vec<u8>>)> {
        self.map.iter()
    }
}
```

The three-valued return from `get` is essential: `None` means "this memtable has no information about this key — keep searching older sources." `Some(None)` means "this key was deleted — stop searching." `Some(Some(value))` means "here's the value." Collapsing the first two cases would make deleted keys reappear from older SSTables.

### SSTable Builder: Flushing the Memtable to Disk

When the memtable is frozen, its entries are written to an SSTable file in sorted order.

```rust,ignore
use std::io::{self, Write, BufWriter};
use std::fs::File;

const BLOCK_SIZE: usize = 4096;

/// Builds an SSTable file from sorted key-value pairs.
struct SsTableBuilder {
    writer: BufWriter<File>,
    /// Index entries: (last_key_in_block, block_offset)
    index: Vec<(Vec<u8>, u64)>,
    current_block: Vec<u8>,
    current_block_offset: u64,
    entry_count: usize,
}

impl SsTableBuilder {
    fn new(path: &str) -> io::Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            index: Vec::new(),
            current_block: Vec::new(),
            current_block_offset: 0,
            entry_count: 0,
        })
    }

    /// Add a key-value pair. Keys must be added in sorted order.
    fn add(&mut self, key: &[u8], value: Option<&[u8]>) -> io::Result<()> {
        // Encode the entry: [key_len: u16][key][is_tombstone: u8][value_len: u16][value]
        let is_tombstone = value.is_none();
        let val = value.unwrap_or(&[]);

        self.current_block.extend_from_slice(&(key.len() as u16).to_le_bytes());
        self.current_block.extend_from_slice(key);
        self.current_block.push(if is_tombstone { 1 } else { 0 });
        self.current_block.extend_from_slice(&(val.len() as u16).to_le_bytes());
        self.current_block.extend_from_slice(val);

        self.entry_count += 1;

        // If the block is full, flush it
        if self.current_block.len() >= BLOCK_SIZE {
            self.flush_block(key)?;
        }
        Ok(())
    }

    fn flush_block(&mut self, last_key: &[u8]) -> io::Result<()> {
        let offset = self.current_block_offset;
        self.writer.write_all(&self.current_block)?;
        self.index.push((last_key.to_vec(), offset));
        self.current_block_offset += self.current_block.len() as u64;
        self.current_block.clear();
        Ok(())
    }

    /// Finalize the SSTable: write the index block and footer.
    fn finish(mut self) -> io::Result<()> {
        // Flush any remaining data in the current block
        if !self.current_block.is_empty() {
            let last_key = self.index.last()
                .map(|(k, _)| k.clone())
                .unwrap_or_default();
            self.flush_block(&last_key)?;
        }

        // Write the index block
        let index_offset = self.current_block_offset;
        for (key, offset) in &self.index {
            self.writer.write_all(&(key.len() as u16).to_le_bytes())?;
            self.writer.write_all(key)?;
            self.writer.write_all(&offset.to_le_bytes())?;
        }

        // Write the footer: index_offset + magic
        self.writer.write_all(&index_offset.to_le_bytes())?;
        self.writer.write_all(b"OORSST01")?; // 8-byte magic
        self.writer.flush()?;

        Ok(())
    }
}
```

The builder writes entries into fixed-size blocks and records the last key and offset of each block in the index. The footer at the end of the file lets the reader locate the index without scanning the entire file. This is the same layout used by LevelDB and RocksDB's table format (with more compression and filtering in production).

Notice that `add` requires keys in sorted order — the caller (the memtable flush code) is responsible for iterating the memtable in order. Violating this invariant produces a corrupt SSTable where binary search returns wrong results.

---

## Key Takeaways

- The LSM write path is: active memtable → freeze → immutable memtable → background flush → SSTable on disk. Writes are never blocked by disk I/O — they complete as soon as the in-memory insert finishes.
- Deletes are tombstones, not removals. A tombstone must persist through SSTables to mask older versions of the key. Compaction eventually garbage-collects tombstones once no older version exists.
- SSTables are immutable sorted files partitioned into data blocks with an index block for O(log B) block lookup (where B is the number of blocks). Immutability enables lock-free concurrent reads.
- The read path checks memtable first, then SSTables from newest to oldest. Negative lookups (key doesn't exist) are the worst case — they must check every source. Bloom filters (Lesson 3) mitigate this.
- The merge iterator produces a single sorted stream from multiple sources, with newest-wins semantics for duplicate keys. This is the core data structure for both reads and compaction.

---

{{#quiz lesson-01-quiz.toml}}
