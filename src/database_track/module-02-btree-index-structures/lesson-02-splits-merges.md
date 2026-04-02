# Lesson 2 — Node Splits and Merges

**Module:** Database Internals — M02: B-Tree Index Structures  
**Position:** Lesson 2 of 3  
**Source:** *Database Internals* — Alex Petrov, Chapters 4–5

> **Source note:** This lesson was synthesized from training knowledge. Verify Petrov's specific split/merge algorithm variants and his treatment of lazy vs. eager rebalancing against Chapters 4–5.

---

## Context

A B-tree that only grows (inserts, never deletes) would eventually have every node at 100% capacity. The next insert into a full node would fail — unless the tree can restructure itself. **Node splitting** is the mechanism: when a node overflows, it divides into two half-full nodes and promotes a separator key to the parent. This maintains the B-tree invariant (all leaves at the same depth) and keeps every node between half and completely full.

The reverse operation — **node merging** — handles deletions. When a node drops below the minimum fill factor (half full), it either borrows keys from a sibling or merges with a sibling. Without merging, a delete-heavy workload could leave the tree full of nearly-empty nodes, wasting space and degrading scan performance.

For the OOR, inserts happen when new objects are cataloged or existing TLEs are updated (≈1,000/day for routine operations, burst to 10,000+ during fragmentation events). Deletes happen when objects re-enter the atmosphere or are reclassified. The split/merge machinery ensures the index stays balanced through both workload patterns.

---

## Core Concepts

### Leaf Node Split

When an insert arrives at a full leaf node:

1. Allocate a new leaf page from the page manager.
2. Move the upper half of the keys (and their record IDs) to the new node.
3. The **median key** becomes the separator — it is promoted to the parent internal node.
4. The parent inserts the separator key with a pointer to the new node.

```text
Before split (leaf full, max 4 keys):
  Parent: [...| 30 |...]
               |
  Leaf:   [10, 20, 30, 40]   ← inserting 25

After split:
  Parent: [...| 25 | 30 |...]
               |       |
  Left:   [10, 20]    Right: [25, 30, 40]
```

The choice of median matters: promoting the middle key keeps both new nodes as close to half-full as possible, maximizing the number of inserts before the next split. Some implementations promote the first key of the right node instead — simpler but slightly less balanced.

### Internal Node Split

If the parent is also full when receiving the promoted separator, the parent itself must split. This propagation continues upward until either a non-full ancestor is found or the root splits. A root split is the only operation that increases the tree's height:

1. Split the root into two children.
2. Create a new root with one separator key pointing to the two children.
3. Tree height increases by 1.

Root splits are rare — for a B-tree with branching factor 510, the root doesn't split until it contains 509 keys, meaning the tree holds at least 260,000 records at height 2 before needing height 3.

### The Cost of Splits

A single insert can trigger a cascade of splits from leaf to root. In the worst case (every ancestor is full), inserting one key causes `h` splits — one per level. Each split writes 2 pages (the original node and the new node) plus modifies the parent, so the worst-case write amplification is `2h + 1` page writes for one insert.

In practice, cascading splits are rare. The average cost of an insert is approximately 1.5 page writes: one for the leaf update and 0.5 for the amortized split cost (since splits happen once per ~B/2 inserts).

### Deletion and Underflow

When a key is deleted from a leaf:

1. Remove the key and its record ID from the leaf.
2. If the leaf is still at least half full, done.
3. If the leaf is below half full (**underflow**), rebalance.

Rebalancing options, tried in order:

- **Redistribute from a sibling:** If an adjacent sibling has more than the minimum number of keys, transfer one key from the sibling through the parent. This keeps both nodes at valid fill levels.
- **Merge with a sibling:** If both the underflowing node and its sibling are at minimum, merge them into one node and remove the separator from the parent.

Merge reduces the parent's key count by one. If the parent then underflows, the same process propagates upward. A merge at the root level (when the root has only one child) reduces the tree height by 1.

### Redistribution vs. Merge

```text
Redistribution (sibling has spare keys):
  Parent: [...| 30 |...]          Parent: [...| 25 |...]
           |       |         →              |       |
  Left: [10]    Right: [25,30,40]   Left: [10,25]  Right: [30,40]

Merge (both at minimum):
  Parent: [...| 30 |...]          Parent: [...|...]
           |       |         →           |
  Left: [10]    Right: [30]        Merged: [10,30]
```

Redistribution is preferred because it doesn't change the tree structure — no nodes are created or destroyed, no parent keys are removed. Merge is the fallback when redistribution isn't possible.

### Lazy vs. Eager Rebalancing

Not all implementations rebalance immediately on underflow. **Lazy rebalancing** tolerates slightly-underfull nodes, deferring merges until a periodic compaction pass or until the node becomes completely empty. This reduces write amplification at the cost of slightly lower space efficiency and slightly higher scan costs (more nodes to traverse).

PostgreSQL's B-tree implementation, for example, does not merge on every delete — it marks deleted entries as "dead" and reclaims space during `VACUUM`. This is partly because merge operations require exclusive locks on multiple nodes, which would block concurrent readers.

For the OOR, lazy rebalancing is the pragmatic choice: the delete rate (~100/day for atmospheric re-entries) is low enough that occasional underfull nodes have negligible impact on scan performance.

---

## Code Examples

### Leaf Node Split

Splitting a full leaf node during insert, promoting the median key to the parent.

```rust,ignore
impl LeafNode {
    /// Split this leaf and return (median_key, new_right_leaf).
    /// After split, `self` retains the lower half of the keys.
    fn split(&mut self, new_page_id: u32) -> (u32, LeafNode) {
        let mid = self.keys.len() / 2;

        // The median key is promoted to the parent as a separator
        let median_key = self.keys[mid];

        // Right half moves to the new node
        let right_keys = self.keys.split_off(mid);
        let right_values = self.values.split_off(mid);

        let right = LeafNode {
            page_id: new_page_id,
            keys: right_keys,
            values: right_values,
            // In a B+ tree, link the leaves (see Lesson 3)
            next_leaf: self.next_leaf,
        };

        // Update the left node's forward pointer to the new right sibling
        self.next_leaf = Some(new_page_id);

        (median_key, right)
    }
}
```

`split_off(mid)` is the correct choice here — it takes the elements from index `mid` to the end in O(1) amortized time (it's a `Vec` truncation + ownership transfer). The left node retains elements `[0..mid)` and the right node gets `[mid..]`. The median key is promoted to the parent but also remains in the right leaf — in a B+ tree, leaf nodes hold all keys, and internal nodes hold copies as separators.

### Inserting into a B-Tree with Split Propagation

A top-level insert that handles splits propagating up the tree.

```rust,ignore
/// Insert a key-value pair into the B+ tree.
/// If the root splits, creates a new root and increases tree height.
fn btree_insert(
    root_page_id: &mut u32,
    key: u32,
    rid: RecordId,
    buffer_pool: &mut BufferPool,
) -> io::Result<()> {
    // Traverse to the leaf, collecting the path of ancestors
    let (leaf_page_id, ancestors) = find_leaf_with_path(
        *root_page_id, key, buffer_pool
    )?;

    // Attempt to insert into the leaf
    let overflow = insert_into_leaf(leaf_page_id, key, rid, buffer_pool)?;

    if let Some((promoted_key, new_page_id)) = overflow {
        // Leaf split occurred — propagate up
        propagate_split(
            root_page_id, promoted_key, new_page_id,
            &ancestors, buffer_pool
        )?;
    }

    Ok(())
}

/// Propagate a split upward through the ancestors.
fn propagate_split(
    root_page_id: &mut u32,
    mut promoted_key: u32,
    mut new_child_page_id: u32,
    ancestors: &[u32],  // page IDs from root to parent-of-leaf
    buffer_pool: &mut BufferPool,
) -> io::Result<()> {
    // Walk ancestors from bottom (parent of leaf) to top (root)
    for &ancestor_page_id in ancestors.iter().rev() {
        let overflow = insert_into_internal(
            ancestor_page_id, promoted_key, new_child_page_id,
            buffer_pool,
        )?;

        match overflow {
            None => return Ok(()), // Ancestor had room — done
            Some((key, page_id)) => {
                promoted_key = key;
                new_child_page_id = page_id;
                // Continue propagating
            }
        }
    }

    // If we reach here, the root itself split.
    // Create a new root pointing to the old root and the new child.
    let new_root_page = buffer_pool.allocate_page()?;
    let new_root = InternalNode {
        page_id: new_root_page,
        keys: vec![promoted_key],
        children: vec![*root_page_id, new_child_page_id],
    };
    serialize_and_write_node(&new_root, buffer_pool)?;
    *root_page_id = new_root_page;

    Ok(())
}
```

The ancestor path is collected during the initial traversal. This avoids re-traversing the tree during split propagation, which would be both slower and incorrect under concurrent modifications (a problem we'll address in Module 5).

The `root_page_id` is passed as `&mut u32` because a root split changes it — the old root becomes a child of the new root. In a production engine, the root page ID is stored in the file header page and updated atomically with WAL protection.

---

## Key Takeaways

- Node splits maintain the B-tree invariant by dividing overfull nodes and promoting a separator key to the parent. Splits propagate upward; root splits are the only operation that increases tree height.
- The amortized cost of an insert is ~1.5 page writes. The worst case (full cascade) is 2h+1 writes but occurs rarely — once per ~B/2 inserts per level.
- Deletions trigger rebalancing when a node drops below half full. Redistribution (borrowing from a sibling) is preferred; merging is the fallback. Merges propagate upward like splits.
- Lazy rebalancing (deferring merges) reduces write amplification and lock contention at the cost of slightly underfull nodes. Most production B-tree implementations use some form of lazy deletion.
- The ancestor path must be collected during traversal for split propagation. Re-traversing the tree after a split is both slower and unsafe under concurrent access.

---

{{#quiz lesson-02-quiz.toml}}
