//! B+ Tree index — values stored exclusively at leaf level.
//!
//! B+ Trees differ from classic B-Trees in two important ways:
//! 1. **Internal nodes store only routing keys** (no record pointers).
//! 2. **Leaf nodes are linked in a sorted doubly-linked sequence**, enabling
//!    efficient O(log n + k) range scans that follow the leaf chain instead
//!    of backtracking through the tree.
//!
//! This makes B+ Trees the preferred index structure for range-heavy workloads
//! and is the reason almost every relational database uses them for its
//! primary / clustered indices.
//!
//! # Arena-based implementation
//!
//! Same arena (flat `Vec<BPlusNode>`) strategy as the B-Tree.  Node indices
//! are stable `usize` values unaffected by `Vec` reallocations.
//!
//! # Node invariants (minimum degree `t`)
//! * Every non-root node has at least `t−1` keys and at most `2t−1` keys.
//! * Internal nodes have exactly one more child than key.
//! * Leaf nodes hold (key, Vec<RecordId>) pairs and a `next_leaf` pointer.
//! * All leaves are at the same depth.
//!
//! # Key operations
//! | Operation   | Complexity          |
//! |-------------|---------------------|
//! | search      | O(t · log_t n)      |
//! | insert      | O(t · log_t n)      |
//! | delete      | O(t · log_t n)      |
//! | range_scan  | O(log n + k)        |

use std::io;

use serde::{Deserialize, Serialize};

use crate::index::config::BTREE_MIN_DEGREE;
use crate::index::index_trait::{IndexKey, IndexTrait, RecordId, TreeBasedIndex};
use crate::index::paged_store;

// ─── Node ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BPlusNode {
    keys: Vec<IndexKey>,
    /// Record lists — only populated in leaf nodes.
    values: Vec<Vec<RecordId>>,
    /// Child node indices — only populated in internal nodes.
    children: Vec<usize>,
    /// Link to the next leaf node (leaf nodes only).
    next_leaf: Option<usize>,
    is_leaf: bool,
    dead: bool,
}

impl BPlusNode {
    fn new_leaf() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            next_leaf: None,
            is_leaf: true,
            dead: false,
        }
    }
}

// ─── Public index type ────────────────────────────────────────────────────────

/// B+ Tree secondary index.
///
/// All key-record pairs reside at the leaf level; internal nodes carry
/// routing keys only.  Leaves are linked in ascending key order for O(1)-per-
/// entry range traversal after the initial tree descent.
#[derive(Debug, Serialize, Deserialize)]
pub struct BPlusTree {
    nodes: Vec<BPlusNode>,
    root: usize,
    t: usize,
    entry_count: usize,
}

impl BPlusTree {
    /// Create a new, empty B+ Tree with minimum degree `t`.
    pub fn new(t: usize) -> Self {
        assert!(t >= 2, "minimum degree t must be >= 2");
        let root = BPlusNode::new_leaf();
        Self {
            nodes: vec![root],
            root: 0,
            t,
            entry_count: 0,
        }
    }

    /// Create with the default minimum degree from [`config`](crate::index::config).
    pub fn with_defaults() -> Self {
        Self::new(BTREE_MIN_DEGREE)
    }

    /// Load a persisted B+ Tree from the paged file at `path`.
    pub fn load(path: &str) -> io::Result<Self> {
        let mut index = Self::with_defaults();
        paged_store::load_entries_stream(path, |key, rid| index.insert(key, rid))?;
        Ok(index)
    }

    fn collect_entries(&self) -> Vec<(IndexKey, RecordId)> {
        let mut out = Vec::new();
        let mut leaf_idx = self.leftmost_leaf();

        loop {
            let leaf = &self.nodes[leaf_idx];
            if leaf.dead {
                break;
            }
            for (k, rids) in leaf.keys.iter().zip(leaf.values.iter()) {
                for rid in rids {
                    out.push((k.clone(), rid.clone()));
                }
            }
            match leaf.next_leaf {
                Some(next) => leaf_idx = next,
                None => break,
            }
        }

        out
    }

    fn validate_subtree(&self, node_idx: usize, seen: &mut [bool]) -> io::Result<()> {
        if node_idx >= self.nodes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bplus_tree: child pointer out of bounds",
            ));
        }
        if seen[node_idx] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bplus_tree: cycle detected in node graph",
            ));
        }
        seen[node_idx] = true;

        let node = &self.nodes[node_idx];
        if node.dead {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bplus_tree: reachable node is marked dead",
            ));
        }
        for window in node.keys.windows(2) {
            if window[0] >= window[1] {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bplus_tree: keys are not strictly sorted",
                ));
            }
        }

        if node.is_leaf {
            if node.values.len() != node.keys.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bplus_tree: leaf values length does not match keys length",
                ));
            }
            if !node.children.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bplus_tree: leaf node contains child pointers",
                ));
            }
            for rids in &node.values {
                if rids.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "bplus_tree: leaf contains key with empty record list",
                    ));
                }
            }
            if let Some(next) = node.next_leaf {
                if next >= self.nodes.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "bplus_tree: next_leaf pointer out of bounds",
                    ));
                }
                if !self.nodes[next].is_leaf {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "bplus_tree: next_leaf points to a non-leaf node",
                    ));
                }
            }
            return Ok(());
        }

        if !node.values.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bplus_tree: internal nodes must not store record lists",
            ));
        }
        if node.children.len() != node.keys.len() + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bplus_tree: internal child count must be keys + 1",
            ));
        }

        for &child_idx in &node.children {
            self.validate_subtree(child_idx, seen)?;
        }
        Ok(())
    }

    // ─── Navigation ──────────────────────────────────────────────────────────

    /// Descend from `start_node` to the leaf that should contain `key`,
    /// recording the path as `(node_idx, child_slot_taken)` pairs.
    fn find_leaf_with_path(
        &self,
        key: &IndexKey,
    ) -> (usize, Vec<(usize, usize)>) {
        let mut path = Vec::new();
        let mut cur = self.root;

        while !self.nodes[cur].is_leaf {
            // slot = first directory index whose key > search key
            let slot = self.nodes[cur].keys.partition_point(|k| k <= key);
            path.push((cur, slot));
            cur = self.nodes[cur].children[slot];
        }
        (cur, path)
    }

    // ─── Insert ──────────────────────────────────────────────────────────────

    fn do_insert(&mut self, key: IndexKey, rid: RecordId) {
        let (leaf_idx, path) = self.find_leaf_with_path(&key);

        // Insert into leaf.
        let pos = self.nodes[leaf_idx].keys.partition_point(|k| k < &key);
        if pos < self.nodes[leaf_idx].keys.len() && self.nodes[leaf_idx].keys[pos] == key {
            if !self.nodes[leaf_idx].values[pos].contains(&rid) {
                self.nodes[leaf_idx].values[pos].push(rid);
            }
            return;
        }
        self.nodes[leaf_idx].keys.insert(pos, key);
        self.nodes[leaf_idx].values.insert(pos, vec![rid]);

        // Split if leaf overflows (has 2t keys).
        if self.nodes[leaf_idx].keys.len() >= 2 * self.t {
            self.split_leaf_propagate(leaf_idx, path);
        }
    }

    /// Split a leaf node and propagate the push-up key through the path.
    fn split_leaf_propagate(&mut self, leaf_idx: usize, path: Vec<(usize, usize)>) {
        let t = self.t;
        // Right half starts at index `t`.
        let right_keys = self.nodes[leaf_idx].keys.split_off(t);
        let right_vals = self.nodes[leaf_idx].values.split_off(t);
        let old_next = self.nodes[leaf_idx].next_leaf;

        let new_leaf_idx = self.nodes.len();
        self.nodes.push(BPlusNode {
            keys: right_keys,
            values: right_vals,
            children: Vec::new(),
            next_leaf: old_next,
            is_leaf: true,
            dead: false,
        });
        self.nodes[leaf_idx].next_leaf = Some(new_leaf_idx);

        // The key copied up to the parent is the first key of the right leaf.
        let push_up = self.nodes[new_leaf_idx].keys[0].clone();
        self.propagate_split(leaf_idx, new_leaf_idx, push_up, path);
    }

    /// Push a split result up through the path, splitting internal nodes as
    /// needed.
    fn propagate_split(
        &mut self,
        _left_idx: usize,
        right_idx: usize,
        push_up: IndexKey,
        mut path: Vec<(usize, usize)>,
    ) {
        if path.is_empty() {
            // Root was a leaf (or the split propagated to the root); create a
            // new root.
            let old_root = self.root;
            let new_root = self.nodes.len();
            self.nodes.push(BPlusNode {
                keys: vec![push_up],
                values: Vec::new(),
                children: vec![old_root, right_idx],
                next_leaf: None,
                is_leaf: false,
                dead: false,
            });
            self.root = new_root;
            return;
        }

        let (parent, slot) = path.pop().unwrap();
        // Insert push_up key and the new right child into the parent.
        self.nodes[parent].keys.insert(slot, push_up);
        self.nodes[parent].children.insert(slot + 1, right_idx);

        if self.nodes[parent].keys.len() < 2 * self.t {
            return; // No overflow.
        }

        // Split the internal node.
        let t = self.t;
        let mid = t - 1; // Index of the key that moves up.

        let push_up_key = self.nodes[parent].keys[mid].clone();

        // Right half of the internal node (keys after the median).
        let right_keys = self.nodes[parent].keys.split_off(mid + 1);
        self.nodes[parent].keys.truncate(mid); // Drop median from left half.
        let right_children = self.nodes[parent].children.split_off(mid + 1);

        let new_internal = self.nodes.len();
        self.nodes.push(BPlusNode {
            keys: right_keys,
            values: Vec::new(),
            children: right_children,
            next_leaf: None,
            is_leaf: false,
            dead: false,
        });

        self.propagate_split(parent, new_internal, push_up_key, path);
    }

    // ─── Delete ──────────────────────────────────────────────────────────────

    /// Remove a specific `(key, rid)` pair.
    /// Returns whether the pair was found.  Handles leaf underflow via
    /// sibling borrowing or merging with recursive internal-node fix.
    fn do_delete(&mut self, key: &IndexKey, rid: &RecordId) -> bool {
        let (leaf_idx, mut path) = self.find_leaf_with_path(key);

        let pos = self.nodes[leaf_idx].keys.partition_point(|k| k < key);
        if pos >= self.nodes[leaf_idx].keys.len() || &self.nodes[leaf_idx].keys[pos] != key {
            return false;
        }

        let before = self.nodes[leaf_idx].values[pos].len();
        self.nodes[leaf_idx].values[pos].retain(|r| r != rid);
        if self.nodes[leaf_idx].values[pos].len() == before {
            return false; // rid was not in the list.
        }

        // If the entry still has other record IDs, no structural change needed.
        if !self.nodes[leaf_idx].values[pos].is_empty() {
            return true;
        }

        // Remove the now-empty key from the leaf.
        self.nodes[leaf_idx].keys.remove(pos);
        self.nodes[leaf_idx].values.remove(pos);

        // Fix potential leaf underflow.
        let min_keys = self.t - 1;
        if leaf_idx != self.root && self.nodes[leaf_idx].keys.len() < min_keys {
            self.fix_leaf_underflow(leaf_idx, &mut path);
        }

        // Shrink tree height if root became empty.
        if !self.nodes[self.root].is_leaf && self.nodes[self.root].keys.is_empty() {
            self.root = self.nodes[self.root].children[0];
        }

        true
    }

    /// Fix an underflowing leaf by borrowing from a sibling or merging.
    fn fix_leaf_underflow(&mut self, leaf_idx: usize, path: &mut Vec<(usize, usize)>) {
        if path.is_empty() {
            return;
        }
        let (parent, slot) = *path.last().unwrap();

        // Try to borrow from the right sibling.
        if slot + 1 < self.nodes[parent].children.len() {
            let right_sib = self.nodes[parent].children[slot + 1];
            if self.nodes[right_sib].keys.len() > self.t - 1 {
                let borrow_key = self.nodes[right_sib].keys.remove(0);
                let borrow_val = self.nodes[right_sib].values.remove(0);
                // Update routing key in parent to the new first key of the right sibling.
                self.nodes[parent].keys[slot] = self.nodes[right_sib].keys[0].clone();
                self.nodes[leaf_idx].keys.push(borrow_key);
                self.nodes[leaf_idx].values.push(borrow_val);
                return;
            }
        }

        // Try to borrow from the left sibling.
        if slot > 0 {
            let left_sib = self.nodes[parent].children[slot - 1];
            if self.nodes[left_sib].keys.len() > self.t - 1 {
                let n_left = self.nodes[left_sib].keys.len();
                let borrow_key = self.nodes[left_sib].keys.remove(n_left - 1);
                let borrow_val = self.nodes[left_sib].values.remove(n_left - 1);
                // Routing key for this leaf becomes the new first key of leaf.
                self.nodes[parent].keys[slot - 1] = borrow_key.clone();
                self.nodes[leaf_idx].keys.insert(0, borrow_key);
                self.nodes[leaf_idx].values.insert(0, borrow_val);
                return;
            }
        }

        // Must merge — prefer merging with the right sibling.
        path.pop();
        if slot + 1 < self.nodes[parent].children.len() {
            let right_sib = self.nodes[parent].children[slot + 1];
            let right_keys = std::mem::take(&mut self.nodes[right_sib].keys);
            let right_vals = std::mem::take(&mut self.nodes[right_sib].values);
            let right_next = self.nodes[right_sib].next_leaf;
            self.nodes[right_sib].dead = true;

            self.nodes[leaf_idx].keys.extend(right_keys);
            self.nodes[leaf_idx].values.extend(right_vals);
            self.nodes[leaf_idx].next_leaf = right_next;

            // Remove the separator key and the right sibling from the parent.
            self.nodes[parent].keys.remove(slot);
            self.nodes[parent].children.remove(slot + 1);
        } else {
            // Merge into left sibling.
            let left_sib = self.nodes[parent].children[slot - 1];
            let curr_keys = std::mem::take(&mut self.nodes[leaf_idx].keys);
            let curr_vals = std::mem::take(&mut self.nodes[leaf_idx].values);
            let curr_next = self.nodes[leaf_idx].next_leaf;
            self.nodes[leaf_idx].dead = true;

            self.nodes[left_sib].keys.extend(curr_keys);
            self.nodes[left_sib].values.extend(curr_vals);
            self.nodes[left_sib].next_leaf = curr_next;

            self.nodes[parent].keys.remove(slot - 1);
            self.nodes[parent].children.remove(slot);
        }

        // Fix internal node underflow upwards.
        let min_keys = self.t - 1;
        if parent != self.root && self.nodes[parent].keys.len() < min_keys {
            self.fix_internal_underflow(parent, path);
        }
    }

    /// Fix an underflowing internal node (after a child merge).
    fn fix_internal_underflow(&mut self, node_idx: usize, path: &mut Vec<(usize, usize)>) {
        if path.is_empty() {
            return;
        }
        let (parent, slot) = *path.last().unwrap();

        // Try to borrow from the right internal sibling.
        if slot + 1 < self.nodes[parent].children.len() {
            let right_sib = self.nodes[parent].children[slot + 1];
            if self.nodes[right_sib].keys.len() > self.t - 1 {
                // Rotate left: pull separator down, push right's first key up.
                let sep = self.nodes[parent].keys[slot].clone();
                let right_first_key = self.nodes[right_sib].keys.remove(0);
                let right_first_child = self.nodes[right_sib].children.remove(0);
                self.nodes[parent].keys[slot] = right_first_key;
                self.nodes[node_idx].keys.push(sep);
                self.nodes[node_idx].children.push(right_first_child);
                path.pop();
                return;
            }
        }

        // Try to borrow from the left internal sibling.
        if slot > 0 {
            let left_sib = self.nodes[parent].children[slot - 1];
            if self.nodes[left_sib].keys.len() > self.t - 1 {
                let sep = self.nodes[parent].keys[slot - 1].clone();
                let n_left = self.nodes[left_sib].keys.len();
                let left_last_key = self.nodes[left_sib].keys.remove(n_left - 1);
                let n_left_ch = self.nodes[left_sib].children.len();
                let left_last_child = self.nodes[left_sib].children.remove(n_left_ch - 1);
                self.nodes[parent].keys[slot - 1] = left_last_key;
                self.nodes[node_idx].keys.insert(0, sep);
                self.nodes[node_idx].children.insert(0, left_last_child);
                path.pop();
                return;
            }
        }

        // Merge with a sibling.
        path.pop();
        if slot + 1 < self.nodes[parent].children.len() {
            // Merge with right sibling: pull separator from parent down.
            let right_sib = self.nodes[parent].children[slot + 1];
            let sep = self.nodes[parent].keys.remove(slot);
            self.nodes[parent].children.remove(slot + 1);

            self.nodes[node_idx].keys.push(sep);
            let right_keys = std::mem::take(&mut self.nodes[right_sib].keys);
            let right_children = std::mem::take(&mut self.nodes[right_sib].children);
            self.nodes[right_sib].dead = true;
            self.nodes[node_idx].keys.extend(right_keys);
            self.nodes[node_idx].children.extend(right_children);
        } else {
            // Merge into left sibling.
            let left_sib = self.nodes[parent].children[slot - 1];
            let sep = self.nodes[parent].keys.remove(slot - 1);
            self.nodes[parent].children.remove(slot);

            self.nodes[left_sib].keys.push(sep);
            let curr_keys = std::mem::take(&mut self.nodes[node_idx].keys);
            let curr_children = std::mem::take(&mut self.nodes[node_idx].children);
            self.nodes[node_idx].dead = true;
            self.nodes[left_sib].keys.extend(curr_keys);
            self.nodes[left_sib].children.extend(curr_children);
        }

        let min_keys = self.t - 1;
        if parent != self.root && self.nodes[parent].keys.len() < min_keys {
            self.fix_internal_underflow(parent, path);
        }
    }

    // ─── Range scan ──────────────────────────────────────────────────────────

    /// Find the leftmost leaf that could contain a key ≥ `start`.
    fn find_range_start_leaf(&self, start: &IndexKey) -> usize {
        let mut cur = self.root;
        while !self.nodes[cur].is_leaf {
            let slot = self.nodes[cur].keys.partition_point(|k| k <= start);
            cur = self.nodes[cur].children[slot];
        }
        cur
    }

    // ─── Min / Max ───────────────────────────────────────────────────────────

    fn leftmost_leaf(&self) -> usize {
        let mut cur = self.root;
        while !self.nodes[cur].is_leaf {
            cur = self.nodes[cur].children[0];
        }
        cur
    }

    fn rightmost_leaf(&self) -> usize {
        let mut cur = self.root;
        while !self.nodes[cur].is_leaf {
            let last_child = *self.nodes[cur].children.last().unwrap();
            cur = last_child;
        }
        cur
    }
}

// ─── Trait implementations ────────────────────────────────────────────────────

impl IndexTrait for BPlusTree {
    fn insert(&mut self, key: IndexKey, record_id: RecordId) -> io::Result<()> {
        self.entry_count += 1;
        self.do_insert(key, record_id);
        Ok(())
    }

    fn search(&self, key: &IndexKey) -> io::Result<Vec<RecordId>> {
        let (leaf_idx, _) = self.find_leaf_with_path(key);
        let pos = self.nodes[leaf_idx].keys.partition_point(|k| k < key);
        if pos < self.nodes[leaf_idx].keys.len() && &self.nodes[leaf_idx].keys[pos] == key {
            return Ok(self.nodes[leaf_idx].values[pos].clone());
        }
        Ok(Vec::new())
    }

    fn delete(&mut self, key: &IndexKey, record_id: &RecordId) -> io::Result<bool> {
        let removed = self.do_delete(key, record_id);
        if removed {
            self.entry_count = self.entry_count.saturating_sub(1);
        }
        Ok(removed)
    }

    fn save(&self, path: &str) -> io::Result<()> {
        paged_store::save_entries(path, self.all_entries()?.into_iter())
    }

    fn entry_count(&self) -> usize {
        self.entry_count
    }

    fn index_type_name(&self) -> &'static str {
        "bplus_tree"
    }

    fn all_entries(&self) -> io::Result<Vec<(IndexKey, RecordId)>> {
        Ok(self.collect_entries())
    }

    fn validate_structure(&self) -> io::Result<()> {
        if self.t < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bplus_tree: minimum degree must be >= 2",
            ));
        }
        if self.root >= self.nodes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bplus_tree: root index out of bounds",
            ));
        }
        if self.nodes[self.root].dead {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bplus_tree: root is marked dead",
            ));
        }

        let mut seen = vec![false; self.nodes.len()];
        self.validate_subtree(self.root, &mut seen)?;

        // Verify leaf chain has no cycle.
        let mut chain_seen = vec![false; self.nodes.len()];
        let mut leaf_idx = self.leftmost_leaf();
        loop {
            if chain_seen[leaf_idx] {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bplus_tree: cycle detected in leaf chain",
                ));
            }
            chain_seen[leaf_idx] = true;
            let node = &self.nodes[leaf_idx];
            if !node.is_leaf {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bplus_tree: non-leaf found in leaf chain",
                ));
            }
            match node.next_leaf {
                Some(next) => leaf_idx = next,
                None => break,
            }
        }

        Ok(())
    }
}

impl TreeBasedIndex for BPlusTree {
    fn range_scan(&self, start: &IndexKey, end: &IndexKey) -> io::Result<Vec<RecordId>> {
        let mut result = Vec::new();
        let mut leaf_idx = self.find_range_start_leaf(start);

        loop {
            let node = &self.nodes[leaf_idx];
            if node.dead {
                break;
            }
            let mut all_past_end = true;
            for (k, v) in node.keys.iter().zip(node.values.iter()) {
                if k > end {
                    break;
                }
                if k >= start {
                    result.extend_from_slice(v);
                    all_past_end = false;
                } else {
                    all_past_end = false;
                }
            }
            if all_past_end {
                break;
            }
            match node.next_leaf {
                Some(next) => leaf_idx = next,
                None => break,
            }
        }
        Ok(result)
    }

    fn min_key(&self) -> Option<IndexKey> {
        let leaf = self.leftmost_leaf();
        self.nodes[leaf].keys.first().cloned()
    }

    fn max_key(&self) -> Option<IndexKey> {
        let leaf = self.rightmost_leaf();
        self.nodes[leaf].keys.last().cloned()
    }
}
