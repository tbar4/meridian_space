# Lesson 3 — B+ Trees and Range Scans

**Module:** Database Internals — M02: B-Tree Index Structures  
**Position:** Lesson 3 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapters 5–6; *Designing Data-Intensive Applications* — Martin Kleppmann, Chapter 3

> **Source note:** This lesson was synthesized from training knowledge. Verify Petrov's treatment of B+ tree leaf linking, his comparison of B-tree vs. B+ tree I/O cost for range scans, and his coverage of prefix compression in Chapter 6.

---

## Context

The B-tree from Lessons 1 and 2 stores key-value pairs in both internal and leaf nodes. This is correct and complete for point lookups, but it has a significant limitation for range scans: the data is distributed across all levels of the tree. Scanning NORAD IDs 40000–40500 requires traversing internal nodes to find the start, then potentially bouncing between internal and leaf nodes to collect all matching records.

The **B+ tree** variant solves this by separating concerns: internal nodes contain *only* separator keys and child pointers (they are a pure navigation structure), and *all* data records live in leaf nodes. Leaf nodes are linked into a doubly-linked list, so a range scan only needs one tree traversal (to find the starting leaf) followed by a sequential walk along the leaf chain.

This is why virtually every production relational database uses B+ trees, not plain B-trees, as their primary index structure. The leaf-level linked list turns range scans from O(N log N) (re-traversing from the root for each key) into O(log N + K) where K is the number of matching keys — a massive improvement for the OOR's conjunction query workload, which frequently scans orbital parameter ranges.

---

## Core Concepts

### B+ Tree vs. B-Tree

| Property | B-Tree | B+ Tree |
|----------|--------|---------|
| Data location | All nodes (internal + leaf) | Leaf nodes only |
| Internal node contents | Keys + values + child pointers | Keys + child pointers only |
| Range scan support | Must re-traverse or backtrack | Sequential leaf walk |
| Branching factor | Lower (values consume space in internal nodes) | Higher (internal nodes hold more keys) |
| Point lookup I/O | Potentially fewer reads (data can be in internal nodes) | Always traverses to leaf |
| Disk space for keys | Each key stored once | Separator keys duplicated in internal nodes |

For the OOR, the B+ tree's higher branching factor (more keys per internal node) and efficient range scans outweigh the slight overhead of always traversing to leaf nodes. The conjunction query workload is dominated by range scans over orbital parameter ranges, not single-key lookups.

### Leaf-Level Linked List

B+ tree leaf nodes are connected in a linked list sorted by key order. Each leaf stores a pointer to its right sibling (and optionally its left sibling for reverse scans):

```text
Root: [30 | 60]
       /    |    \
     /      |      \
 [10,20] → [30,40,50] → [60,70,80,90]
  (leaf 1)    (leaf 2)       (leaf 3)
```

A range scan for keys 25–55:

1. Tree traversal: root → leaf 2 (first key ≥ 25 is 30).
2. Sequential scan: read leaf 2 (keys 30, 40, 50), follow `next` pointer to leaf 3 (key 60 > 55, stop).
3. Total I/O: 2 page reads (root + leaf 2) + 1 page read (leaf 3) = 3 pages. The root is cached, so practical I/O is 2 page reads.

Without the linked list, the engine would need to return to the root for each key, or implement a complex in-order traversal using a stack of parent pointers.

### Separator Keys in Internal Nodes

In a B+ tree, internal node keys are **separators** — they do not need to be exact copies of leaf keys. They only need to correctly direct traffic. If the left child's maximum key is 29 and the right child's minimum key is 30, any value in the range [30, ...] works as a separator. Some implementations use **abbreviated separators** (the shortest key that correctly divides the two children) to fit more entries per internal node.

This means a delete from a leaf does not necessarily require updating the parent's separator. If you delete key 30 from a leaf, the separator in the parent can remain 30 — it still correctly directs traffic because all keys in the right child are ≥ 30 (the next key might be 31). The separator only needs updating if the split boundary itself changes.

### Prefix and Suffix Compression

Leaf nodes in a B+ tree often contain keys with significant commonality — for example, NORAD IDs in the range 40000–40499 share the prefix "400". **Prefix compression** stores the common prefix once and encodes each key as a delta from the prefix. **Suffix truncation** removes key suffixes that are unnecessary for correct comparison within the node.

For the OOR's `u32` NORAD IDs, these optimizations provide modest savings (4-byte keys have limited prefix sharing). They become critical for composite keys or string keys — a B+ tree indexing international designators like "2024-001A" through "2024-999Z" would benefit substantially.

### Bulk Loading

Building a B+ tree by inserting records one at a time produces nodes at ~50-67% average fill. **Bulk loading** builds the tree bottom-up from sorted data:

1. Sort all records by key.
2. Pack records into leaf nodes at near-100% fill.
3. Build internal nodes bottom-up by taking separator keys from each pair of adjacent leaves.
4. Repeat upward until the root is created.

Bulk loading is O(N) in the number of records and produces an optimally-packed tree. For the OOR's initial catalog load (100,000 records), bulk loading fills ~200 leaf pages at 100% fill. One-by-one inserts would produce ~300-400 pages at 50-67% fill.

---

## Code Examples

### B+ Tree Leaf Node with Sibling Links

Extending the leaf node from Lesson 1 with linked-list pointers for range scan support.

```rust,ignore
/// B+ tree leaf node with sibling links for range scans.
#[derive(Debug)]
struct BPlusLeafNode {
    page_id: u32,
    keys: Vec<u32>,
    values: Vec<RecordId>,
    /// Forward pointer to the next leaf (right sibling)
    next_leaf: Option<u32>,
    /// Backward pointer for reverse scans (optional)
    prev_leaf: Option<u32>,
}

impl BPlusLeafNode {
    /// Range scan: iterate all keys in [start, end] starting from this leaf.
    /// Returns a lazy iterator that follows the leaf chain.
    fn range_scan_from<'a>(
        &'a self,
        start: u32,
        end: u32,
        buffer_pool: &'a mut BufferPool,
    ) -> impl Iterator<Item = io::Result<(u32, RecordId)>> + 'a {
        // Find the first key >= start in this leaf
        let start_idx = self.keys.partition_point(|&k| k < start);

        // Yield matching keys from this leaf and follow the chain
        BPlusRangeScanIterator {
            current_keys: &self.keys[start_idx..],
            current_values: &self.values[start_idx..],
            idx: 0,
            end_key: end,
            next_leaf_page: self.next_leaf,
            buffer_pool,
            done: false,
        }
    }
}
```

The range scan iterator is lazy — it reads the next leaf page only when the current leaf's keys are exhausted. This avoids reading leaf pages beyond what the caller actually consumes (important if the caller stops early after finding a match).

### Range Scan Iterator

The iterator follows the leaf chain, reading one page at a time.

```rust,ignore
/// Iterator over a range of B+ tree leaf entries.
/// Follows the leaf-level linked list until the end key is exceeded.
struct BPlusRangeScanIterator<'a> {
    current_leaf: Option<BPlusLeafNode>,
    idx: usize,
    end_key: u32,
    buffer_pool: &'a mut BufferPool,
    done: bool,
}

impl<'a> BPlusRangeScanIterator<'a> {
    fn next_entry(&mut self) -> Option<io::Result<(u32, RecordId)>> {
        loop {
            if self.done {
                return None;
            }

            let leaf = self.current_leaf.as_ref()?;

            // Check if there are more entries in the current leaf
            if self.idx < leaf.keys.len() {
                let key = leaf.keys[self.idx];
                if key > self.end_key {
                    self.done = true;
                    return None;
                }
                let rid = leaf.values[self.idx];
                self.idx += 1;
                return Some(Ok((key, rid)));
            }

            // Current leaf exhausted — follow the chain
            match leaf.next_leaf {
                None => {
                    self.done = true;
                    return None;
                }
                Some(next_page_id) => {
                    match self.load_leaf(next_page_id) {
                        Ok(next_leaf) => {
                            self.current_leaf = Some(next_leaf);
                            self.idx = 0;
                            // Loop back to yield from the new leaf
                        }
                        Err(e) => {
                            self.done = true;
                            return Some(Err(e));
                        }
                    }
                }
            }
        }
    }

    fn load_leaf(&mut self, page_id: u32) -> io::Result<BPlusLeafNode> {
        let frame_idx = self.buffer_pool.fetch_page(page_id)?;
        let data = self.buffer_pool.frame_data(frame_idx);
        let leaf = deserialize_leaf_node(data)?;
        self.buffer_pool.unpin(frame_idx, false);
        Ok(leaf)
    }
}
```

This pattern — a struct that holds iteration state and lazily loads pages — is the **volcano iterator model** that we'll formalize in Module 6 (Query Processing). Every B+ tree range scan in every database engine works this way: traverse to the starting leaf, then pull one record at a time from the leaf chain, fetching the next page only when the current one is exhausted.

---

## Key Takeaways

- B+ trees store all data in leaf nodes and use internal nodes purely for navigation. This maximizes the internal node branching factor and enables efficient range scans via the leaf-level linked list.
- Range scans are O(log N + K): one tree traversal to the starting leaf, then K sequential leaf reads. This is the primary advantage over plain B-trees and hash indices.
- Separator keys in internal nodes don't need to be exact copies of leaf keys — any value that correctly directs traffic works. This enables prefix compression and abbreviated separators.
- Bulk loading produces a B+ tree at near-100% fill factor in O(N) time, compared to O(N log N) for one-by-one inserts at ~50-67% fill. Always bulk-load when building an index from scratch.
- The leaf-level scan iterator is the first instance of the volcano iterator pattern — a pull-based interface that lazily fetches pages on demand. This pattern recurs throughout the query processing stack.

---

{{#quiz lesson-03-quiz.toml}}
