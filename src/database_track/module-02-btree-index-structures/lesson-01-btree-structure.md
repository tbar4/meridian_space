# Lesson 1 — B-Tree Structure: Keys, Pointers, and Invariants

**Module:** Database Internals — M02: B-Tree Index Structures  
**Position:** Lesson 1 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapters 2 and 4

> **Source note:** This lesson was synthesized from training knowledge. The following concepts would benefit from verification against the source books: Petrov's specific notation for B-tree order vs. branching factor, and his framing of the fill factor invariant.

---
<!-- toc -->
---
## Context

A heap file of slotted pages provides O(1) access by Record ID `(page_id, slot_id)`, but O(N) access by key — finding NORAD ID 25544 requires scanning every data page. For the Orbital Object Registry, this is the difference between a 0.05ms indexed lookup and a 5ms full scan. At 500 conjunction checks per second, indexed lookups consume 25ms of I/O per second. Full scans consume 2,500ms — the system spends more time scanning than computing.

The **B-tree** is a balanced, sorted, multi-way tree optimized for disk-based storage. Each node occupies one page, and the tree's branching factor (number of children per node) is determined by how many keys fit in a page. A B-tree with a branching factor of 200 and 100,000 records has a height of 3 — any record can be found in at most 3 page reads. Compare this to a binary search tree, which would have height ~17 and require 17 page reads.

B-trees were invented in 1970 by Bayer and McCreight specifically for disk-based access patterns. Every modern relational database and most file systems use B-tree variants as their primary index structure.

---

## Core Concepts

### Tree Structure

A B-tree of order `m` has the following properties:

- Every internal node has at most `m` children and at most `m - 1` keys.
- Every internal node (except the root) has at least `⌈m/2⌉` children.
- The root has at least 2 children (unless it is a leaf).
- All leaves are at the same depth.
- Keys within each node are sorted in ascending order.

The keys in an internal node serve as **separators** — they direct the search to the correct child. For a node with keys `[K₁, K₂, K₃]` and children `[C₀, C₁, C₂, C₃]`:

- All keys in subtree `C₀` are `< K₁`
- All keys in subtree `C₁` are `≥ K₁` and `< K₂`
- All keys in subtree `C₂` are `≥ K₂` and `< K₃`
- All keys in subtree `C₃` are `≥ K₃`

```text
                    [30 | 60]
                   /    |    \
          [10|20]    [40|50]    [70|80|90]
          /  |  \    /  |  \    /  |  |  \
        ... ... ... ... ... ... ... ... ... ...
```

### Branching Factor and Tree Height

The branching factor determines how wide the tree is — and therefore how shallow. For the Orbital Object Registry:

- Page size: 4KB
- Key size: 4 bytes (NORAD ID as `u32`)
- Child pointer size: 4 bytes (page ID as `u32`)
- Node header overhead: ~20 bytes

Usable space per node: `4096 - 20 = 4076` bytes. Each key-pointer pair: `4 + 4 = 8` bytes. Maximum keys per node: `4076 / 8 ≈ 509`. So the branching factor is approximately **510**.

Tree height for N records with branching factor B: `h = ⌈log_B(N)⌉ + 1` (counting from root to leaf, inclusive).

| Records | B=510 Height | Page Reads per Lookup |
|---------|-------------|----------------------|
| 100,000 | 2 | 2 |
| 1,000,000 | 2 | 2 |
| 100,000,000 | 3 | 3 |

With branching factor 510, the entire 100,000-record OOR catalog is reachable in **2 page reads** (root + leaf). The root node is almost always cached in the buffer pool, so in practice most lookups require only **1 disk read** (the leaf node).

### Point Lookup Algorithm

To find key K:

1. Start at the root node.
2. Binary search the node's keys to find the correct child pointer.
3. Follow the child pointer to the next level.
4. Repeat until a leaf node is reached.
5. Binary search the leaf node for K.

Each level requires one page read and one binary search. Binary search within a node is O(log m) comparisons — negligible compared to the page read cost.

### Node Layout on Disk

Each B-tree node is stored as a page in the page file. The node layout within a page:

```text
┌────────────────────────────────────────────┐
│ Node Header                                │
│  - node_type: u8 (Internal=0, Leaf=1)      │
│  - key_count: u16                          │
│  - parent_page_id: u32 (for split propagation) │
├────────────────────────────────────────────┤
│ Key-Pointer Pairs (for internal nodes):    │
│  [child_0] [key_0] [child_1] [key_1] ...  │
│                                            │
│ Key-Value Pairs (for leaf nodes):          │
│  [key_0] [rid_0] [key_1] [rid_1] ...      │
│  (RID = page_id + slot_id of data record)  │
└────────────────────────────────────────────┘
```

Internal nodes store `(child_page_id, key)` pairs. Leaf nodes store `(key, record_id)` pairs where the record ID points to the actual TLE record in a data page (from Module 1).

### The Fill Factor Invariant

The minimum fill requirement (at least `⌈m/2⌉` children per internal node) is what guarantees the tree stays balanced. Without it, degenerate deletions could produce a tree where one branch is much deeper than another, destroying the O(log N) guarantee.

The fill factor also ensures space efficiency — every node is at least half full, so the tree uses at most 2x the minimum space needed. In practice, B-trees maintain an average fill factor of ~67% (between the 50% minimum and 100% maximum), and bulk-loaded trees can achieve >90%.

---

## Code Examples

### B-Tree Node Representation

This defines the on-disk layout for B-tree nodes in the Orbital Object Registry, where keys are NORAD catalog IDs (`u32`) and values are Record IDs.

```rust,ignore
/// Record ID: a pointer to a TLE record in a data page.
#[derive(Debug, Clone, Copy, PartialEq)]
struct RecordId {
    page_id: u32,
    slot_id: u16,
}

/// A B-tree node stored in a single page.
#[derive(Debug)]
enum BTreeNode {
    Internal(InternalNode),
    Leaf(LeafNode),
}

#[derive(Debug)]
struct InternalNode {
    page_id: u32,
    /// Separator keys. `keys[i]` is the boundary between children[i] and children[i+1].
    keys: Vec<u32>,
    /// Child page IDs. `children.len() == keys.len() + 1`.
    children: Vec<u32>,
}

#[derive(Debug)]
struct LeafNode {
    page_id: u32,
    /// Sorted key-value pairs. Keys are NORAD IDs, values are Record IDs.
    keys: Vec<u32>,
    values: Vec<RecordId>,
}

impl InternalNode {
    /// Find the child page that could contain the given key.
    fn find_child(&self, key: u32) -> u32 {
        // Binary search for the first separator key > search key.
        // The child to the left of that separator is the correct subtree.
        let pos = self.keys.partition_point(|&k| k <= key);
        self.children[pos]
    }
}

impl LeafNode {
    /// Point lookup: find the Record ID for a given NORAD ID.
    fn find(&self, key: u32) -> Option<RecordId> {
        match self.keys.binary_search(&key) {
            Ok(idx) => Some(self.values[idx]),
            Err(_) => None,
        }
    }
}
```

The `partition_point` method is the correct choice for internal node search — it finds the insertion point, which corresponds to the child that owns the search key's range. Using `binary_search` would be wrong: duplicate separator keys (from splits) would match incorrectly, and `binary_search` returns an arbitrary match when duplicates exist.

### Traversal: Root-to-Leaf Lookup

A complete point lookup traverses from the root to a leaf, reading one page per level.

```rust,ignore
/// Look up a NORAD ID in the B-tree. Returns the Record ID if found.
fn btree_lookup(
    root_page_id: u32,
    key: u32,
    buffer_pool: &mut BufferPool,
) -> io::Result<Option<RecordId>> {
    let mut current_page_id = root_page_id;

    loop {
        let frame_idx = buffer_pool.fetch_page(current_page_id)?;
        let page_data = buffer_pool.frame_data(frame_idx);
        let node = deserialize_node(page_data)?;
        // Unpin immediately — we've extracted the data we need.
        // In a real implementation, we'd hold the pin during the
        // search for concurrency safety (see Module 5).
        buffer_pool.unpin(frame_idx, false);

        match node {
            BTreeNode::Internal(internal) => {
                current_page_id = internal.find_child(key);
                // Continue traversal — follow the child pointer
            }
            BTreeNode::Leaf(leaf) => {
                return Ok(leaf.find(key));
            }
        }
    }
}
```

This reads at most `h` pages where `h` is the tree height. For the OOR (100k records, branching factor ~510), `h = 2`. The root page is almost always cached in the buffer pool (it's accessed by every lookup), so the typical cost is 1 disk read — just the leaf page.

The comment about unpinning immediately is important: in a concurrent engine (Module 5), you'd hold the pin while searching to prevent the page from being evicted mid-traversal. For single-threaded Module 2, immediate unpin is safe and keeps the buffer pool available.

---

## Key Takeaways

- A B-tree with branching factor B and N records has height O(log_B(N)). With B ≈ 500 (common for 4KB pages with small keys), a tree indexing 100 million records is only 4 levels deep.
- The fill factor invariant (nodes at least half full) guarantees balanced height and prevents degenerate trees. Splits and merges (Lesson 2) maintain this invariant.
- Internal nodes contain separator keys and child pointers. Leaf nodes contain the actual key-to-record-ID mapping. The search algorithm binary-searches within each node and follows pointers down the tree.
- The branching factor is determined by page size and key/pointer sizes. Larger pages or smaller keys mean a wider tree and fewer I/O operations per lookup.
- Root and upper-level internal nodes are almost always cached in the buffer pool, so the practical I/O cost of a lookup is usually just 1 page read (the leaf).

---

{{#quiz lesson-01-quiz.toml}}
