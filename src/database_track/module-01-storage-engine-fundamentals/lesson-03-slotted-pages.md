# Lesson 3 — Slotted Pages

**Module:** Database Internals — M01: Storage Engine Fundamentals  
**Position:** Lesson 3 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapter 3; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 3

> **Source note:** This lesson was synthesized from training knowledge. The following concepts would benefit from verification against the source books: Petrov's specific slotted page layout, his terminology for the slot directory vs. cell pointer array, and the compaction algorithm described in Chapter 3.

---
<!-- toc -->
---
## Context

The page format from Lesson 1 stores records at fixed offsets. This works for fixed-size records — and if every TLE record were exactly 140 bytes, that would be sufficient. But real TLE data is messier: newer objects have additional metadata fields (drag coefficients, maneuver flags, covariance matrices), older legacy records omit optional fields, and record sizes will grow as ESA adds new conjunction assessment data. A format that requires all records to be the same size either wastes space (padding every record to the maximum) or breaks when schema evolves.

The **slotted page** layout solves this by decoupling record identity from record position. Records are addressed by **slot numbers**, and a slot directory at the beginning of the page maps each slot to the record's actual byte offset and length within the page. Records grow from the end of the page toward the front, while the slot directory grows from the front toward the end. They meet in the middle — and when they collide, the page is full.

This layout is the standard for every major relational database (PostgreSQL, MySQL/InnoDB, SQLite) and many key-value stores. Understanding it is prerequisite for everything that follows: B-tree nodes (Module 2) are slotted pages, WAL log records reference slot IDs (Module 4), and MVCC version chains (Module 5) track records by their page-and-slot address.

---

## Core Concepts

### Slotted Page Layout

A slotted page has three regions:

```text
┌──────────────────────────────────────────────┐
│ Page Header (17 bytes — from Lesson 1)       │
├──────────────────────────────────────────────┤
│ Slot Directory (grows →)                     │
│ [slot 0: offset, len] [slot 1: offset, len] │
├──────────────────────────────────────────────┤
│                                              │
│           Free Space                         │
│                                              │
├──────────────────────────────────────────────┤
│ Records (grow ←)                             │
│ [record 1 data] [record 0 data]             │
└──────────────────────────────────────────────┘
```

**Slot directory:** An array of `(offset, length)` pairs, one per record. Slot 0 is the first entry. Each entry is 4 bytes (2 bytes offset + 2 bytes length), supporting records up to 65,535 bytes and offsets within a 64KB page. For 4KB pages, this is more than sufficient.

**Records:** Stored at the end of the page, growing backward (toward lower offsets). The first record inserted goes at the very end of the page; subsequent records are placed just before the previous one.

**Free space:** The gap between the end of the slot directory and the start of the record region. As records are inserted, the free space shrinks from both sides. The page is full when `slot_directory_end >= record_region_start`.

### Record Addressing: (PageId, SlotId)

Higher layers of the storage engine refer to records by a **Record ID (RID)**: a `(page_id, slot_id)` pair. This identifier is stable — it doesn't change when records are moved within the page during compaction, because the slot directory is updated to reflect the new offset. External references (B-tree leaf pointers, index entries) store RIDs, not raw byte offsets.

This indirection is what makes the slotted page powerful: the engine can rearrange the physical layout of records within a page (to reclaim fragmented space) without invalidating any external references. The slot ID stays the same; only the offset in the slot directory changes.

When a record is deleted, its slot entry is marked as **tombstoned** (offset set to a sentinel like `0xFFFF`) but not removed from the directory. Removing it would shift all subsequent slot IDs by one, invalidating every external reference to those slots. Tombstoned slots can be reused for future inserts.

### Insertion

To insert a record of `N` bytes:

1. Check if there is enough free space: `free_space >= N + 4` (4 bytes for the new slot entry).
2. Find the next available slot. If there's a tombstoned slot, reuse it. Otherwise, append a new entry to the directory.
3. Write the record at `record_region_start - N`.
4. Update the slot entry with `(offset, N)`.
5. Update the page header: increment record count, adjust `free_space_offset`.

If the free space check fails, the page is full. The caller must either split the page (in a B-tree) or allocate a new page (in a heap file).

### Deletion and Fragmentation

Deleting a record tombstones its slot entry and marks the record's bytes as reclaimable. But it doesn't shift other records — doing so would change their offsets and require updating every other slot entry that points past the deleted record.

This creates **internal fragmentation**: there are N bytes of garbage between valid records. Over time, a page can have plenty of total free space but no contiguous block large enough for a new record.

```text
Before delete:
[Header][Slot 0][Slot 1][Slot 2]   [free]   [Rec 2][Rec 1][Rec 0]

After deleting record 1:
[Header][Slot 0][TOMB ][Slot 2]   [free]   [Rec 2][DEAD ][Rec 0]
                                             ← gap here cannot be used
                                               unless compacted →
```

### Page Compaction

**Compaction** reclaims fragmented space by sliding all live records to the end of the page (closing the gaps left by deleted records) and updating their slot directory entries to reflect the new offsets. After compaction, all free space is contiguous.

The algorithm:

1. Collect all live records (slot entries that are not tombstoned), sorted by their current offset in descending order.
2. Starting from the end of the page, write each record contiguously.
3. Update each slot entry with the new offset.
4. Update the page header's `free_space_offset`.

Compaction is an in-page operation — it never spills to disk or affects other pages. It runs when an insert fails due to fragmentation (total free space is sufficient, but contiguous free space is not). Some engines compact proactively during quiet periods to avoid stalling inserts.

### Overflow Pages

A single record might exceed the page's usable space (4,079 bytes in a 4KB page). This shouldn't happen for TLE records (140 bytes), but the engine must handle it for forward compatibility — covariance matrices and conjunction assessment reports can be kilobytes.

The solution: when a record is too large, store the first portion in the primary page and the remainder in one or more **overflow pages**. The slot entry points to the in-page portion, which contains a pointer to the overflow chain. This is sometimes called **TOAST** (The Oversized Attribute Storage Technique) in PostgreSQL terminology.

For the Orbital Object Registry, overflow pages are unlikely but should be supported. The implementation can be deferred until the schema actually requires it.

---

## Code Examples

### A Slotted Page Implementation for TLE Records

This implements the core slotted page logic: insert, lookup, delete, and compaction.

```rust,ignore
const PAGE_SIZE: usize = 4096;
const HEADER_SIZE: usize = 17;
const SLOT_SIZE: usize = 4; // 2 bytes offset + 2 bytes length
const TOMBSTONE: u16 = 0xFFFF;

/// A slotted page that stores variable-length records.
struct SlottedPage {
    data: [u8; PAGE_SIZE],
}

impl SlottedPage {
    fn new(page_id: u32) -> Self {
        let mut page = Self {
            data: [0u8; PAGE_SIZE],
        };
        // Initialize header (simplified — reuse PageHeader from Lesson 1)
        let mut header = PageHeader::new(page_id, PageType::Data);
        header.serialize(&mut page.data);
        page
    }

    /// Number of slots (including tombstoned ones).
    fn slot_count(&self) -> u16 {
        u16::from_le_bytes(self.data[9..11].try_into().unwrap())
    }

    fn set_slot_count(&mut self, count: u16) {
        self.data[9..11].copy_from_slice(&count.to_le_bytes());
    }

    /// Byte offset where the record region begins (records grow downward).
    fn record_region_start(&self) -> u16 {
        u16::from_le_bytes(self.data[11..13].try_into().unwrap())
    }

    fn set_record_region_start(&mut self, offset: u16) {
        self.data[11..13].copy_from_slice(&offset.to_le_bytes());
    }

    /// Read a slot directory entry.
    fn read_slot(&self, slot_id: u16) -> (u16, u16) {
        let base = HEADER_SIZE + (slot_id as usize) * SLOT_SIZE;
        let offset = u16::from_le_bytes(
            self.data[base..base + 2].try_into().unwrap()
        );
        let length = u16::from_le_bytes(
            self.data[base + 2..base + 4].try_into().unwrap()
        );
        (offset, length)
    }

    fn write_slot(&mut self, slot_id: u16, offset: u16, length: u16) {
        let base = HEADER_SIZE + (slot_id as usize) * SLOT_SIZE;
        self.data[base..base + 2].copy_from_slice(&offset.to_le_bytes());
        self.data[base + 2..base + 4].copy_from_slice(&length.to_le_bytes());
    }

    /// Free space available for new records + slot entries.
    fn free_space(&self) -> usize {
        let slot_dir_end = HEADER_SIZE + (self.slot_count() as usize) * SLOT_SIZE;
        let rec_start = self.record_region_start() as usize;
        if rec_start > slot_dir_end {
            rec_start - slot_dir_end
        } else {
            0
        }
    }

    /// Insert a record. Returns the slot ID on success.
    fn insert(&mut self, record: &[u8]) -> Option<u16> {
        let needed = record.len() + SLOT_SIZE; // record bytes + new slot entry
        if self.free_space() < needed {
            return None; // Page full — caller should try compaction or new page
        }

        // Find a tombstoned slot to reuse, or allocate a new one
        let slot_id = self.find_free_slot();

        // Place the record at the end of the record region
        let new_offset = self.record_region_start() - record.len() as u16;
        let start = new_offset as usize;
        let end = start + record.len();
        self.data[start..end].copy_from_slice(record);

        // Update the slot directory
        self.write_slot(slot_id, new_offset, record.len() as u16);
        self.set_record_region_start(new_offset);

        Some(slot_id)
    }

    /// Look up a record by slot ID. Returns None if the slot is
    /// tombstoned or out of range.
    fn get(&self, slot_id: u16) -> Option<&[u8]> {
        if slot_id >= self.slot_count() {
            return None;
        }
        let (offset, length) = self.read_slot(slot_id);
        if offset == TOMBSTONE {
            return None; // Deleted record
        }
        let start = offset as usize;
        let end = start + length as usize;
        Some(&self.data[start..end])
    }

    /// Delete a record by tombstoning its slot entry.
    fn delete(&mut self, slot_id: u16) -> bool {
        if slot_id >= self.slot_count() {
            return false;
        }
        let (offset, _) = self.read_slot(slot_id);
        if offset == TOMBSTONE {
            return false; // Already deleted
        }
        self.write_slot(slot_id, TOMBSTONE, 0);
        true
    }

    /// Find a tombstoned slot to reuse, or allocate a new one.
    fn find_free_slot(&mut self) -> u16 {
        let count = self.slot_count();
        for i in 0..count {
            let (offset, _) = self.read_slot(i);
            if offset == TOMBSTONE {
                return i;
            }
        }
        // No tombstoned slots — extend the directory
        self.set_slot_count(count + 1);
        count
    }
}
```

Key design decisions: the slot directory grows forward from the header, records grow backward from the end of the page, and the two regions meet in the middle. This maximizes usable space — there's no fixed boundary between "slot space" and "record space." A page with few large records uses most of its space for record data; a page with many small records uses more for the slot directory.

The `insert` method does not attempt compaction automatically. The caller is responsible for detecting "free space exists but is fragmented" and calling `compact()` before retrying. This keeps the insert path simple and predictable.

### Page Compaction: Defragmenting Live Records

When deletes have fragmented the record region, compaction slides all live records together and reclaims the gaps.

```rust,ignore
impl SlottedPage {
    /// Compact the page: slide all live records to the end,
    /// eliminating gaps from deleted records.
    fn compact(&mut self) {
        let slot_count = self.slot_count();

        // Collect live records: (slot_id, data_copy)
        let mut live_records: Vec<(u16, Vec<u8>)> = Vec::new();
        for i in 0..slot_count {
            let (offset, length) = self.read_slot(i);
            if offset != TOMBSTONE {
                let start = offset as usize;
                let end = start + length as usize;
                live_records.push((i, self.data[start..end].to_vec()));
            }
        }

        // Rewrite records contiguously from the end of the page
        let mut cursor = PAGE_SIZE as u16;
        for (slot_id, record) in &live_records {
            cursor -= record.len() as u16;
            let start = cursor as usize;
            let end = start + record.len();
            self.data[start..end].copy_from_slice(record);
            self.write_slot(*slot_id, cursor, record.len() as u16);
        }

        self.set_record_region_start(cursor);
    }
}
```

This implementation copies live records into a temporary `Vec` and writes them back. A more memory-efficient approach would sort records by offset and slide them in-place, but the copy approach is correct, simple, and fast enough for 4KB pages. The total data moved is at most 4,079 bytes — negligible compared to the cost of a single disk I/O.

After compaction, the page's free space is contiguous. An insert that failed before compaction (due to fragmentation) will succeed after it — assuming the total free space is sufficient.

---

## Key Takeaways

- Slotted pages decouple record identity (slot ID) from physical position (byte offset). Records can be moved within the page without invalidating external references.
- The `(page_id, slot_id)` Record ID is the stable address used by B-tree leaf nodes, index entries, and MVCC version chains. Every higher layer depends on this abstraction.
- Deletions create internal fragmentation. Compaction reclaims fragmented space by sliding live records together — an in-page operation that touches no other pages.
- Tombstoning (not removing) deleted slot entries preserves slot ID stability. A removed slot would shift all subsequent IDs, breaking every external reference.
- The "free space" calculation must account for both record bytes and slot directory growth. An insert that appears to fit by record size alone may fail because there's no room for the new slot entry.

---

{{#quiz lesson-03-quiz.toml}}
