# Lesson 2 — Buffer Pool Management

**Module:** Database Internals — M01: Storage Engine Fundamentals  
**Position:** Lesson 2 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapter 5; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 3

> **Source note:** This lesson was synthesized from training knowledge. The following concepts would benefit from verification against the source books: Petrov's specific CLOCK algorithm variant, his framing of the buffer pool vs. OS page cache tradeoff, and the exact dirty page flush protocols described in Chapter 5.

---

## Context

The page I/O layer from Lesson 1 reads and writes pages directly to disk. Every `read_page` call triggers a system call, a disk seek (on HDD) or a flash translation layer lookup (on SSD), and a DMA transfer. For the Orbital Object Registry's conjunction query workload — which repeatedly accesses the same hot set of TLE records during a pass window — going to disk for every page request is unacceptable. A conjunction check against 100 objects would issue 100+ page reads, taking 50–100ms on SSD and over a second on spinning disk.

The **buffer pool** solves this by caching recently-accessed pages in memory. It sits between the storage engine's upper layers (B-tree, LSM, query processor) and the page I/O layer. When a page is requested, the buffer pool checks whether it's already in memory. If so, it returns a pointer to the cached copy — no disk I/O required. If not, it evicts a cold page to make room, reads the requested page from disk, caches it, and returns it. For a well-tuned buffer pool with a hot working set that fits in memory, the hit rate exceeds 99%, and the storage engine operates almost entirely from RAM.

The buffer pool exists even though the OS already has a page cache. The difference is **control**: the OS page cache uses a generic LRU policy that doesn't know about access patterns specific to the storage engine (sequential scan flooding, index traversal locality). A purpose-built buffer pool can use workload-aware eviction, pin pages during multi-step operations, and track dirty pages for coordinated flushing.

---

## Core Concepts

### Buffer Pool Architecture

The buffer pool is a fixed-size array of **frames** — each frame holds one page-sized buffer plus metadata. The metadata tracks:

- **Page ID** — which on-disk page is currently loaded in this frame
- **Pin count** — how many active references exist to this frame. A pinned page cannot be evicted.
- **Dirty flag** — whether the page has been modified since it was loaded from disk. Dirty pages must be written back before eviction.
- **Reference bit** (for CLOCK) — whether the page has been accessed recently

The buffer pool also maintains a **page table** — a hash map from page ID to frame index — for O(1) lookups. When the engine requests page 42, the buffer pool checks the page table. If page 42 maps to frame 7, the engine gets a reference to frame 7's buffer. If page 42 is not in the page table, the buffer pool must evict a page and load 42 from disk.

```text
Page Table (HashMap<PageId, FrameId>)
┌─────────┬─────────┐
│ Page 42 │ Frame 7 │
│ Page 13 │ Frame 2 │
│ Page 99 │ Frame 0 │
│   ...   │   ...   │
└─────────┴─────────┘

Frame Array
┌─────────┬─────────┬─────────┬─────────┐
│ Frame 0 │ Frame 1 │ Frame 2 │ Frame 3 │ ...
│ pg=99   │ (empty) │ pg=13   │ (empty) │
│ pin=1   │         │ pin=0   │         │
│ dirty=N │         │ dirty=Y │         │
└─────────┴─────────┴─────────┴─────────┘
```

### Eviction Policies: LRU

**Least Recently Used (LRU)** evicts the page that hasn't been accessed for the longest time. The intuition: if a page hasn't been needed recently, it's unlikely to be needed soon. LRU is implemented with a doubly-linked list — on every access, the page is moved to the head of the list. On eviction, the tail page is removed.

LRU's weakness is **scan flooding**: a single sequential scan over the entire database evicts every hot page from the buffer pool, even if those pages are accessed hundreds of times per second by other queries. After the scan completes, every subsequent request misses the buffer pool and goes to disk. This is catastrophic for the OOR — a full catalog export scan would evict the conjunction query hot set.

Mitigations: LRU-K (track the K-th most recent access, not just the most recent), 2Q (separate queues for first-access and re-access pages), or ARC (adaptive replacement cache). PostgreSQL uses a clock-sweep approximation. MySQL/InnoDB uses a two-list LRU with a "young" and "old" sublist.

### Eviction Policies: CLOCK

**CLOCK** approximates LRU without the overhead of maintaining a linked list. Each frame has a single **reference bit**. When a page is accessed, its reference bit is set to 1. When the buffer pool needs to evict, it sweeps through the frames in circular order (like a clock hand):

1. If the current frame's reference bit is 1, clear it to 0 and advance the hand.
2. If the current frame's reference bit is 0 and the page is not pinned and not dirty (or if dirty, flush it first), evict this page.

CLOCK is cheaper per operation than LRU (no linked list manipulation on every access — just set a bit) and provides comparable hit rates for most workloads. It's the default in many systems.

The weakness is the same as LRU: a full scan sets every reference bit to 1, requiring the clock hand to sweep the entire pool before any page can be evicted. **CLOCK-sweep with a scan-resistant enhancement** (used by PostgreSQL) mitigates this by not setting the reference bit for pages accessed during a sequential scan.

### Page Pinning

A page is **pinned** when the engine is actively using it and it must not be evicted. The pin count tracks how many concurrent users hold a reference to the page. A page is evictable only when `pin_count == 0`.

Pinning is critical for correctness: if the engine is in the middle of reading a B-tree node and the buffer pool evicts that page, the engine reads garbage. The protocol:

1. **Fetch** a page: buffer pool loads or finds it, increments pin count, returns a handle.
2. **Use** the page: engine reads or writes the page data.
3. **Unpin** the page: engine decrements pin count when done. If the engine modified the page, it marks it dirty.

Failing to unpin a page is a resource leak — the page can never be evicted, and eventually the buffer pool fills with pinned pages and all fetch requests fail. In Rust, RAII handles this naturally: the page handle decrements the pin count in its `Drop` implementation.

### Dirty Page Flushing

A dirty page has been modified in memory but not yet written back to disk. The buffer pool tracks dirty pages and flushes them in two scenarios:

1. **Eviction flush**: when a dirty page is selected for eviction, it must be written to disk before the frame can be reused.
2. **Background flush**: a periodic background thread scans for dirty pages and writes them to disk proactively, reducing the chance that an eviction will stall on a synchronous write.

The buffer pool does **not** call `fsync` after every flush. Durability is the WAL's responsibility (Module 4). The buffer pool's flush is an optimization — it keeps the page file reasonably up-to-date so that crash recovery doesn't have to replay the entire WAL.

---

## Code Examples

### A Simple LRU Buffer Pool for the Orbital Object Registry

This buffer pool caches OOR pages in memory and evicts the least recently used unpinned page when the pool is full.

```rust,ignore
use std::collections::{HashMap, VecDeque};
use std::io;

const PAGE_SIZE: usize = 4096;

/// Metadata for a single buffer pool frame.
struct Frame {
    page_id: Option<u32>,
    data: [u8; PAGE_SIZE],
    pin_count: u32,
    is_dirty: bool,
}

impl Frame {
    fn new() -> Self {
        Self {
            page_id: None,
            data: [0u8; PAGE_SIZE],
            pin_count: 0,
            is_dirty: false,
        }
    }
}

/// LRU buffer pool backed by the OOR page file.
struct BufferPool {
    frames: Vec<Frame>,
    page_table: HashMap<u32, usize>,  // page_id -> frame_index
    /// LRU order: front = most recently used, back = least recently used.
    /// Contains frame indices. Only unpinned frames participate in LRU.
    lru_list: VecDeque<usize>,
    page_file: PageFile,  // From Lesson 1
}

impl BufferPool {
    fn new(num_frames: usize, page_file: PageFile) -> Self {
        let mut frames = Vec::with_capacity(num_frames);
        let mut lru_list = VecDeque::with_capacity(num_frames);
        for i in 0..num_frames {
            frames.push(Frame::new());
            lru_list.push_back(i); // All frames start as free (evictable)
        }
        Self {
            frames,
            page_table: HashMap::new(),
            lru_list,
            page_file,
        }
    }

    /// Fetch a page into the buffer pool. Returns the frame index.
    /// The page is pinned — caller MUST call `unpin` when done.
    fn fetch_page(&mut self, page_id: u32) -> io::Result<usize> {
        // Fast path: page is already in the pool
        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            self.frames[frame_idx].pin_count += 1;
            self.move_to_front(frame_idx);
            return Ok(frame_idx);
        }

        // Slow path: need to load from disk. Find an evictable frame.
        let frame_idx = self.find_evict_target()?;

        // If the frame holds a dirty page, flush it before reuse
        if let Some(old_page_id) = self.frames[frame_idx].page_id {
            if self.frames[frame_idx].is_dirty {
                self.page_file.write_page(
                    old_page_id,
                    &self.frames[frame_idx].data,
                )?;
            }
            self.page_table.remove(&old_page_id);
        }

        // Load the requested page into the frame
        self.page_file.read_page(page_id, &mut self.frames[frame_idx].data)?;
        self.frames[frame_idx].page_id = Some(page_id);
        self.frames[frame_idx].pin_count = 1;
        self.frames[frame_idx].is_dirty = false;
        self.page_table.insert(page_id, frame_idx);
        self.move_to_front(frame_idx);

        Ok(frame_idx)
    }

    /// Unpin a page. Caller must indicate whether the page was modified.
    fn unpin(&mut self, frame_idx: usize, is_dirty: bool) {
        let frame = &mut self.frames[frame_idx];
        assert!(frame.pin_count > 0, "unpin called on unpinned frame");
        frame.pin_count -= 1;
        if is_dirty {
            frame.is_dirty = true;
        }
    }

    /// Find the least recently used unpinned frame.
    fn find_evict_target(&self) -> io::Result<usize> {
        // Scan from the back (LRU end) for an unpinned frame
        for &frame_idx in self.lru_list.iter().rev() {
            if self.frames[frame_idx].pin_count == 0 {
                return Ok(frame_idx);
            }
        }
        Err(io::Error::new(
            io::ErrorKind::Other,
            "buffer pool exhausted: all frames are pinned",
        ))
    }

    /// Move a frame to the front of the LRU list (most recently used).
    fn move_to_front(&mut self, frame_idx: usize) {
        self.lru_list.retain(|&idx| idx != frame_idx);
        self.lru_list.push_front(frame_idx);
    }
}
```

The `move_to_front` implementation is O(n) because `VecDeque::retain` scans the entire list. A production buffer pool uses an intrusive doubly-linked list for O(1) LRU updates — Rust crates like `intrusive-collections` provide this. The O(n) approach is correct and sufficient for understanding the algorithm; the optimization matters only when the buffer pool has thousands of frames and fetch rates exceed 100k/sec.

Notice the pin-count assert in `unpin`: a double-unpin is a logic bug that must crash immediately in development. In production, this would be `debug_assert!` to avoid panicking on a user-facing code path.

### RAII Page Handle for Automatic Unpinning

Rust's ownership system prevents the "forgot to unpin" bug class entirely. A page handle unpins automatically when it goes out of scope.

```rust,ignore
/// RAII handle to a pinned buffer pool page.
/// Automatically unpins the page when dropped.
struct PageHandle<'a> {
    pool: &'a mut BufferPool,
    frame_idx: usize,
    dirty: bool,
}

impl<'a> PageHandle<'a> {
    fn data(&self) -> &[u8; PAGE_SIZE] {
        &self.pool.frames[self.frame_idx].data
    }

    fn data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.dirty = true;
        &mut self.pool.frames[self.frame_idx].data
    }
}

impl<'a> Drop for PageHandle<'a> {
    fn drop(&mut self) {
        self.pool.unpin(self.frame_idx, self.dirty);
    }
}
```

This is one of the places where Rust's borrow checker provides a genuine advantage over C/C++ buffer pool implementations. In C, every code path that fetches a page must remember to unpin it — including error paths, early returns, and exception-like `longjmp` flows. In Rust, the `Drop` implementation runs unconditionally when the handle leaves scope. The borrow checker also prevents holding a `&mut` reference to the page data after the handle is dropped, which would alias freed memory in C.

The tradeoff: the `&'a mut BufferPool` borrow means you can only hold one `PageHandle` at a time with this design. A production buffer pool uses `Arc<Mutex<...>>` or unsafe interior mutability to allow multiple concurrent page handles — we'll revisit this pattern when we implement B-tree traversal in Module 2.

---

## Key Takeaways

- The buffer pool is a fixed-size array of page-sized frames with a hash map for O(1) page-to-frame lookup. It eliminates disk I/O for hot pages and is the single largest performance lever in any storage engine.
- LRU eviction is simple but vulnerable to scan flooding. CLOCK approximates LRU at lower cost per operation. Production engines use hybrid policies (LRU-K, 2Q, ARC) to resist scan-induced cache pollution.
- Page pinning prevents eviction during active use. In Rust, RAII handles make pin leaks impossible — the `Drop` implementation guarantees unpinning on all code paths, including panics.
- Dirty pages are flushed on eviction and by background threads. The buffer pool does not call `fsync` — durability is the WAL's job.
- The "all frames pinned" error means the buffer pool is undersized for the workload's concurrency level. In the OOR, this can happen during peak conjunction checking if every active query holds a page pin simultaneously.

---

{{#quiz lesson-02-quiz.toml}}
