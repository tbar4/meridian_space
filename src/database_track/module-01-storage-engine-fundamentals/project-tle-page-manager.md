# Project — TLE Record Page Manager

**Module:** Database Internals — M01: Storage Engine Fundamentals  
**Track:** Orbital Object Registry  
**Estimated effort:** 4–6 hours

---

## SDA Incident Report — OOR-2026-0042

> **Classification:** ENGINEERING DIRECTIVE  
> **Subject:** Prototype page manager for the Orbital Object Registry  
>  
> Ref: OOR-2026-0041 (TLE index latency deficiency)  
>  
> The first deliverable in the OOR storage engine build is a **page manager** capable of reading and writing TLE records to a custom binary page format. This component sits at the bottom of the storage stack — every subsequent module builds on it. The page manager must demonstrate correct page layout, buffer pool caching, slotted page record management, and integrity verification via checksums.

---
<!-- toc -->
---
## Objective

Build a `PageManager` that:

1. Manages a database file composed of fixed-size 4KB pages
2. Implements a buffer pool with LRU or CLOCK eviction
3. Uses slotted pages for variable-length TLE record storage
4. Verifies page integrity with CRC32 checksums on every read
5. Supports insert, lookup by Record ID `(page_id, slot_id)`, delete, and page compaction

---

## TLE Record Format

For this project, a TLE record is a byte blob with the following structure:

```rust,ignore
/// A Two-Line Element record for a tracked orbital object.
struct TleRecord {
    /// NORAD catalog number (unique object ID, e.g., 25544 for ISS)
    norad_id: u32,
    /// International designator (e.g., "98067A")
    intl_designator: [u8; 8],
    /// Epoch year + fractional day (e.g., 24045.5 = Feb 14 2024, 12:00 UTC)
    epoch: f64,
    /// Mean motion (revolutions per day)
    mean_motion: f64,
    /// Eccentricity (dimensionless, 0–1)
    eccentricity: f64,
    /// Inclination (degrees)
    inclination: f64,
    /// Right ascension of ascending node (degrees)
    raan: f64,
    /// Argument of perigee (degrees)
    arg_perigee: f64,
    /// Mean anomaly (degrees)
    mean_anomaly: f64,
    /// Drag term (B* coefficient)
    bstar: f64,
    /// Element set number (for provenance tracking)
    element_set: u16,
    /// Revolution number at epoch
    rev_number: u32,
}
```

Serialized size: 4 + 8 + (8 × 8) + 2 + 4 = 82 bytes. Use little-endian encoding for all fields. You may add a 2-byte record-length prefix if your slotted page implementation requires it.

---

## Acceptance Criteria

1. **Page I/O correctness.** Pages are written to and read from a file at the correct offsets. A page written at `page_id * 4096` is read back identically.

2. **Checksum verification.** Every `read_page` call computes a CRC32 over the page body and compares it to the stored checksum. A tampered page (any bit flipped in the body) is detected and returns an error.

3. **Buffer pool hit rate.** Insert 200 TLE records across multiple pages, then read them back in the same order. The buffer pool (configured with 8 frames) should achieve a hit rate above 90% on the read pass. Print the hit/miss counts.

4. **Slotted page insert and lookup.** Insert 40 records into a single page. Look up each by its `(page_id, slot_id)` and verify the data matches.

5. **Delete and compaction.** Delete every other record (slots 0, 2, 4, ...). Verify that lookups to deleted slots return `None`. Compact the page and verify that all remaining records are still accessible by their original slot IDs.

6. **Page full handling.** Insert records until a page reports full. Verify that the failure is detected before corrupting any data. Allocate a new page and continue inserting.

7. **Deterministic output.** The program runs without external dependencies beyond `std` and `crc32fast`. Output includes the buffer pool hit/miss stats and a summary of records inserted/read/deleted.

---

## Starter Structure

```
tle-page-manager/
├── Cargo.toml
├── src/
│   ├── main.rs          # Entry point: runs the acceptance criteria
│   ├── page.rs          # PageHeader, SlottedPage, checksums
│   ├── buffer_pool.rs   # BufferPool, Frame, eviction policy
│   ├── page_file.rs     # PageFile: raw I/O to the database file
│   └── tle.rs           # TleRecord serialization/deserialization
```

---

## Hints

<details>
<summary>Hint 1 — Serializing TLE records</summary>

Use `to_le_bytes()` for each field and concatenate them into a `Vec<u8>`. For deserialization, slice the byte buffer at the known offsets and use `from_le_bytes()`. Do not use `serde` or `bincode` — the point of this project is to understand raw binary layout.

```rust,ignore
impl TleRecord {
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(82);
        buf.extend_from_slice(&self.norad_id.to_le_bytes());
        buf.extend_from_slice(&self.intl_designator);
        buf.extend_from_slice(&self.epoch.to_le_bytes());
        // ... remaining fields
        buf
    }
}
```
</details>

<details>
<summary>Hint 2 — Buffer pool sizing</summary>

With 200 TLE records at 82 bytes each and ~49 records per page (4,079 usable bytes / 82 bytes ≈ 49, minus slot overhead), you need approximately 5 pages. An 8-frame buffer pool can hold the entire working set — but only if pages aren't evicted prematurely. Make sure your LRU implementation correctly promotes re-accessed pages.

</details>

<details>
<summary>Hint 3 — Compaction correctness check</summary>

After compacting, iterate all slot IDs and verify:
- Live records return the same data as before compaction
- Tombstoned slots still return `None`
- The page's total free space increased (fragmentation reclaimed)
- The page's contiguous free space equals total free space (no more gaps)

</details>

<details>
<summary>Hint 4 — Checksum verification testing</summary>

To test checksum detection, write a valid page to disk, then flip a single bit in the page body using raw file I/O. Read the page back through the buffer pool and verify that it returns a checksum error, not corrupted data.

```rust,ignore
// Flip bit 0 of byte 20 in page 1
let offset = 1 * PAGE_SIZE + 20;
file.seek(SeekFrom::Start(offset as u64))?;
let mut byte = [0u8; 1];
file.read_exact(&mut byte)?;
byte[0] ^= 0x01; // flip lowest bit
file.seek(SeekFrom::Start(offset as u64))?;
file.write_all(&byte)?;
```
</details>

---

## Reference Implementation

<details>
<summary>Reveal full reference implementation</summary>

The reference implementation is intentionally omitted for this project. The three lessons provide all the code building blocks — your job is to integrate them into a working system. If you get stuck:

1. Start with `page_file.rs` — get raw page I/O working first
2. Add `page.rs` — implement `PageHeader` and `SlottedPage` from Lesson 1 and 3
3. Add `buffer_pool.rs` — wrap the page file with caching from Lesson 2
4. Add `tle.rs` — serialization is straightforward byte manipulation
5. Wire them together in `main.rs` — run each acceptance criterion sequentially

</details>

---

## What Comes Next

The page manager you build here is used directly by Module 2. B-tree nodes are stored as slotted pages in the buffer pool. The `(page_id, slot_id)` Record ID becomes the leaf-node pointer format in the B+ tree index.
