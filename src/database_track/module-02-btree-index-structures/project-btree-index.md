# Project — B+ Tree TLE Index Engine

**Module:** Database Internals — M02: B-Tree Index Structures  
**Track:** Orbital Object Registry  
**Estimated effort:** 6–8 hours

---
<!-- toc -->
---
## SDA Incident Report — OOR-2026-0043

> **Classification:** ENGINEERING DIRECTIVE  
> **Subject:** Build ordered index for NORAD catalog ID lookups and range scans  
>  
> The page manager from Module 1 stores TLE records but requires full scans for key-based access. Build a B+ tree index over NORAD catalog IDs that provides O(log N) point lookups and efficient range scans via a leaf-level linked list. The index must integrate with the existing page manager and buffer pool.

---

## Objective

Build a B+ tree index that:

1. Uses the page manager and buffer pool from Module 1 — each B+ tree node is stored as a page
2. Supports point lookups by NORAD catalog ID in O(log N) page reads
3. Supports range scans over NORAD ID ranges using the leaf-level linked list
4. Handles inserts with automatic node splitting and split propagation
5. Handles deletes with tombstoning (lazy rebalancing is acceptable)
6. Provides a bulk-load operation for initial catalog construction

---

## Acceptance Criteria

1. **Point lookup correctness.** Insert 10,000 TLE records with random NORAD IDs. Look up each by ID and verify the returned Record ID matches.

2. **Range scan correctness.** Insert NORAD IDs 1–10,000. Scan range [5000, 5100] and verify exactly 101 records returned in sorted order.

3. **Split handling.** Insert records until at least 3 leaf splits occur. Verify the tree remains balanced (all leaves at same depth) and all records retrievable.

4. **Bulk-load efficiency.** Bulk-load 100,000 sorted records. Verify leaf fill factor above 95%. Compare leaf count to one-by-one insertion.

5. **Delete correctness.** Delete 1,000 records by NORAD ID. Verify lookups return `None` for deleted keys and remaining records are unaffected.

6. **Integration with buffer pool.** Run full test suite with only 16 buffer pool frames. Verify correctness under eviction pressure.

7. **Deterministic output.** Print tree height, leaf count, fill factor, and buffer pool hit/miss stats after each test phase.

---

## Starter Structure

```
btree-index/
├── Cargo.toml
├── src/
│   ├── main.rs           # Entry point: runs acceptance criteria
│   ├── btree.rs           # BPlusTree: insert, lookup, range_scan, delete, bulk_load
│   ├── node.rs            # InternalNode, LeafNode: serialization, split, merge
│   ├── page.rs            # Reuse from Module 1
│   ├── buffer_pool.rs     # Reuse from Module 1
│   ├── page_file.rs       # Reuse from Module 1
│   └── tle.rs             # Reuse from Module 1
```

---

## Hints

<details>
<summary>Hint 1 — Node serialization format</summary>

Use a 1-byte discriminant at the start of each node page to distinguish internal from leaf. Internal: `[type=0][key_count][child_0][key_0][child_1]...`. Leaf: `[type=1][key_count][next_leaf][prev_leaf][key_0][rid_0]...`.

</details>

<details>
<summary>Hint 2 — Ancestor path for split propagation</summary>

During root-to-leaf traversal, push each internal node's page ID onto a `Vec<u32>`. After a leaf split, pop ancestors one at a time to insert the promoted key. If the ancestor splits too, continue popping. Empty stack = create new root.

</details>

<details>
<summary>Hint 3 — Bulk-load algorithm</summary>

1. Sort all records by NORAD ID.
2. Pack keys into leaf nodes at capacity, write each, record its page ID and last key.
3. Build internal nodes bottom-up: group separators into internal node pages.
4. Repeat step 3 until one root remains.
5. Link leaves into a doubly-linked list.

</details>

<details>
<summary>Hint 4 — Buffer pool pressure during splits</summary>

Split propagation can pin the split node, the new node, and the parent simultaneously (3 frames). With a 16-frame pool and a 2-level tree, this is safe. But unpin nodes as soon as you've serialized them back — don't hold all three longer than necessary.

</details>

---

## What Comes Next

Module 3 introduces LSM trees — a fundamentally different approach. Where B+ trees update pages in-place, LSM trees batch writes in memory and flush immutable files. You'll understand when each is appropriate for the OOR workload.
