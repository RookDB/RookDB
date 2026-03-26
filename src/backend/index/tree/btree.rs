//! Classic B-Tree index (Knuth / CLRS definition).
//!
//! Values (record identifiers) are stored at **every node level**, including
//! internal nodes.  This makes point lookups slightly cheaper than B+ Trees
//! when the key is found high in the tree, but range scans require visiting
//! all node levels.
//!
//! # Node invariants (minimum degree `t`)
//! * Every non-root node has at least `t−1` keys and at most `2t−1` keys.
//! * Every internal node with `k` keys has exactly `k+1` children.
//! * All leaves are at the same depth.
//!
//! # Arena-based implementation
//!
//! Nodes are stored in a flat `Vec<BTreeNode>` (the *arena*).  References
//! between nodes are represented as indices into this arena, which avoids
//! Rust lifetime / borrow-checker issues with recursive tree structures while
//! remaining cache-friendly.
//!
//! Deleted or merged nodes are left as tombstoned entries in the arena (they
//! are no longer reachable from the root).  This is a standard trade-off for
//! arena allocators.
//!
//! # Key operations
//! | Operation | Complexity   |
//! |-----------|-------------|
//! | search    | O(t · log_t n) |
//! | insert    | O(t · log_t n) |
//! | delete    | O(t · log_t n) |
//! | range_scan| O(t · log_t n + k) where k = result count |

use std::fs;
use std::io;

use serde::{Deserialize, Serialize};

use crate::index::config::BTREE_MIN_DEGREE;
use crate::index::index_trait::{IndexKey, IndexTrait, RecordId, TreeBasedIndex};

// ─── Node ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BTreeNode {
    keys: Vec<IndexKey>,
    /// `values[i]` is the list of `RecordId`s for `keys[i]`.
    values: Vec<Vec<RecordId>>,
    /// Child node indices.  Empty for leaf nodes.
    children: Vec<usize>,
    is_leaf: bool,
    /// Tombstone flag: set when a node is merged / abandoned.
    dead: bool,
}

impl BTreeNode {
    fn new_leaf() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
            is_leaf: true,
            dead: false,
        }
    }
}

// ─── Public index type ────────────────────────────────────────────────────────

/// Classic B-Tree secondary index.
///
/// Supports point lookup, insert, delete, and in-order range scan.
/// Backed by a flat arena of [`BTreeNode`]s for safe Rust ownership.
#[derive(Debug, Serialize, Deserialize)]
pub struct BTree {
    nodes: Vec<BTreeNode>,
    root: usize,
    /// Minimum degree.  Each non-root node holds `t−1` to `2t−1` keys.
    t: usize,
    entry_count: usize,
}

impl BTree {
    /// Create a new, empty B-Tree with minimum degree `t`.
    pub fn new(t: usize) -> Self {
        assert!(t >= 2, "minimum degree t must be >= 2");
        let root = BTreeNode::new_leaf();
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

    /// Load a persisted B-Tree from the JSON file at `path`.
    pub fn load(path: &str) -> io::Result<Self> {
        let data = fs::read_to_string(path)?;
        serde_json::from_str(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn collect_entries_from_node(&self, node_idx: usize, out: &mut Vec<(IndexKey, RecordId)>) {
        let node = &self.nodes[node_idx];
        if node.dead {
            return;
        }

        if node.is_leaf {
            for (key, rids) in node.keys.iter().zip(node.values.iter()) {
                for rid in rids {
                    out.push((key.clone(), rid.clone()));
                }
            }
            return;
        }

        for i in 0..node.keys.len() {
            self.collect_entries_from_node(node.children[i], out);
            for rid in &node.values[i] {
                out.push((node.keys[i].clone(), rid.clone()));
            }
        }
        self.collect_entries_from_node(*node.children.last().unwrap(), out);
    }

    fn validate_subtree(&self, node_idx: usize, seen: &mut [bool]) -> io::Result<()> {
        if node_idx >= self.nodes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "btree: child pointer out of bounds",
            ));
        }

        if seen[node_idx] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "btree: cycle detected in node graph",
            ));
        }
        seen[node_idx] = true;

        let node = &self.nodes[node_idx];
        if node.dead {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "btree: reachable node is marked dead",
            ));
        }

        if node.values.len() != node.keys.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "btree: values length does not match keys length",
            ));
        }
        for window in node.keys.windows(2) {
            if window[0] >= window[1] {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "btree: keys are not strictly sorted",
                ));
            }
        }
        for rids in &node.values {
            if rids.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "btree: found key with empty record list",
                ));
            }
        }

        if node.is_leaf {
            if !node.children.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "btree: leaf node contains child pointers",
                ));
            }
            return Ok(());
        }

        if node.children.len() != node.keys.len() + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "btree: internal node child count must be keys + 1",
            ));
        }

        for &child_idx in &node.children {
            self.validate_subtree(child_idx, seen)?;
        }

        Ok(())
    }

    // ─── Search ──────────────────────────────────────────────────────────────

    fn search_node(&self, node_idx: usize, key: &IndexKey) -> Vec<RecordId> {
        let node = &self.nodes[node_idx];
        let pos = node.keys.partition_point(|k| k < key);

        if pos < node.keys.len() && &node.keys[pos] == key {
            return node.values[pos].clone();
        }

        if node.is_leaf {
            return Vec::new();
        }

        self.search_node(node.children[pos], key)
    }

    // ─── Insert ──────────────────────────────────────────────────────────────

    fn insert_non_full(&mut self, node_idx: usize, key: IndexKey, rid: RecordId) {
        let n = self.nodes[node_idx].keys.len();
        let pos = self.nodes[node_idx].keys.partition_point(|k| k < &key);

        if self.nodes[node_idx].is_leaf {
            if pos < n && self.nodes[node_idx].keys[pos] == key {
                if !self.nodes[node_idx].values[pos].contains(&rid) {
                    self.nodes[node_idx].values[pos].push(rid);
                }
            } else {
                self.nodes[node_idx].keys.insert(pos, key);
                self.nodes[node_idx].values.insert(pos, vec![rid]);
            }
            return;
        }

        // Key found at this internal node?
        if pos < n && self.nodes[node_idx].keys[pos] == key {
            if !self.nodes[node_idx].values[pos].contains(&rid) {
                self.nodes[node_idx].values[pos].push(rid);
            }
            return;
        }

        // Descend, splitting full child proactively.
        let child_idx = self.nodes[node_idx].children[pos];
        if self.nodes[child_idx].keys.len() == 2 * self.t - 1 {
            self.split_child(node_idx, pos);
            // Determine which sub-child to descend into after the split.
            let n = self.nodes[node_idx].keys.len();
            let mid_key = &self.nodes[node_idx].keys[pos];
            if key == *mid_key {
                if !self.nodes[node_idx].values[pos].contains(&rid) {
                    self.nodes[node_idx].values[pos].push(rid);
                }
                return;
            }
            let child_pos = if &key > mid_key { pos + 1 } else { pos };
            let _ = n; // suppress unused warning
            let child_idx = self.nodes[node_idx].children[child_pos];
            self.insert_non_full(child_idx, key, rid);
        } else {
            self.insert_non_full(child_idx, key, rid);
        }
    }

    /// Split the `i`-th child of `parent_idx` (which must be full: 2t−1 keys).
    fn split_child(&mut self, parent_idx: usize, i: usize) {
        let t = self.t;
        let child_idx = self.nodes[parent_idx].children[i];

        // Clone the data we need for the new right node before mutating.
        let median_key = self.nodes[child_idx].keys[t - 1].clone();
        let median_val = self.nodes[child_idx].values[t - 1].clone();
        let right_keys = self.nodes[child_idx].keys[t..].to_vec();
        let right_vals = self.nodes[child_idx].values[t..].to_vec();
        let right_children = if self.nodes[child_idx].is_leaf {
            Vec::new()
        } else {
            self.nodes[child_idx].children[t..].to_vec()
        };
        let is_leaf = self.nodes[child_idx].is_leaf;

        // Truncate the child to its left half.
        self.nodes[child_idx].keys.truncate(t - 1);
        self.nodes[child_idx].values.truncate(t - 1);
        if !is_leaf {
            self.nodes[child_idx].children.truncate(t);
        }

        // Register the new right node in the arena.
        let new_idx = self.nodes.len();
        self.nodes.push(BTreeNode {
            keys: right_keys,
            values: right_vals,
            children: right_children,
            is_leaf,
            dead: false,
        });

        // Insert the median key into the parent.
        self.nodes[parent_idx].keys.insert(i, median_key);
        self.nodes[parent_idx].values.insert(i, median_val);
        self.nodes[parent_idx].children.insert(i + 1, new_idx);
    }

    // ─── Delete ──────────────────────────────────────────────────────────────

    /// Remove a specific `(key, rid)` pair.
    ///
    /// Returns `true` if the pair was present and removed; `false` otherwise.
    /// The key entry is structurally removed from the tree only when its
    /// last `RecordId` is deleted.
    fn delete_rid_from_node(
        &mut self,
        node_idx: usize,
        key: &IndexKey,
        rid: &RecordId,
    ) -> (bool, bool) {
        // Returns (rid_was_removed, key_became_empty)
        let n = self.nodes[node_idx].keys.len();
        let pos = self.nodes[node_idx].keys.partition_point(|k| k < key);

        // Key found at this node.
        if pos < n && &self.nodes[node_idx].keys[pos] == key {
            let before = self.nodes[node_idx].values[pos].len();
            self.nodes[node_idx].values[pos].retain(|r| r != rid);
            let removed = self.nodes[node_idx].values[pos].len() < before;
            let empty = self.nodes[node_idx].values[pos].is_empty();
            return (removed, empty);
        }

        // Key not at this node; descend to correct child.
        if self.nodes[node_idx].is_leaf {
            return (false, false); // Not found.
        }

        self.delete_rid_from_node(self.nodes[node_idx].children[pos], key, rid)
    }

    /// Structurally remove an empty key from the tree (no more RecordIds).
    ///
    /// Implements the CLRS B-Tree deletion algorithm with pre-emptive
    /// child augmentation (ensure children have ≥ t keys before descending).
    fn delete_key(&mut self, key: &IndexKey) -> bool {
        if self.nodes[self.root].keys.is_empty() {
            return false;
        }
        let found = self.delete_from_subtree(self.root, key);
        // Shrink tree height if root became empty.
        if !self.nodes[self.root].is_leaf && self.nodes[self.root].keys.is_empty() {
            self.root = self.nodes[self.root].children[0];
        }
        found
    }

    fn delete_from_subtree(&mut self, node_idx: usize, key: &IndexKey) -> bool {
        let n = self.nodes[node_idx].keys.len();
        let pos = self.nodes[node_idx].keys.partition_point(|k| k < key);
        let found_here = pos < n && &self.nodes[node_idx].keys[pos] == key;

        if self.nodes[node_idx].is_leaf {
            if found_here {
                self.nodes[node_idx].keys.remove(pos);
                self.nodes[node_idx].values.remove(pos);
                return true;
            }
            return false;
        }

        if found_here {
            // Case 2: key in internal node.
            let left_child = self.nodes[node_idx].children[pos];
            let right_child = self.nodes[node_idx].children[pos + 1];

            if self.nodes[left_child].keys.len() >= self.t {
                // Case 2a: predecessor from left subtree.
                let (pred_key, pred_val) = self.extract_max(left_child);
                self.nodes[node_idx].keys[pos] = pred_key;
                self.nodes[node_idx].values[pos] = pred_val;
                return true;
            }
            if self.nodes[right_child].keys.len() >= self.t {
                // Case 2b: successor from right subtree.
                let (succ_key, succ_val) = self.extract_min(right_child);
                self.nodes[node_idx].keys[pos] = succ_key;
                self.nodes[node_idx].values[pos] = succ_val;
                return true;
            }
            // Case 2c: both children have t−1 keys; merge them.
            self.merge_children(node_idx, pos);
            let merged = self.nodes[node_idx].children[pos];
            return self.delete_from_subtree(merged, key);
        }

        // Case 3: key not at this node; descend to appropriate child.
        let child_idx = self.nodes[node_idx].children[pos];
        if self.nodes[child_idx].keys.len() < self.t {
            self.fix_child(node_idx, pos);
            // After fix the structure may have changed; re-search from here.
            return self.delete_from_subtree(node_idx, key);
        }
        self.delete_from_subtree(child_idx, key)
    }

    /// Ensure `children[i]` of `node_idx` has at least `t` keys before
    /// descending into it.
    fn fix_child(&mut self, node_idx: usize, i: usize) {
        let n = self.nodes[node_idx].keys.len();
        let left_rich = i > 0 && self.nodes[self.nodes[node_idx].children[i - 1]].keys.len() >= self.t;
        let right_rich = i < n && self.nodes[self.nodes[node_idx].children[i + 1]].keys.len() >= self.t;

        if left_rich {
            self.rotate_right(node_idx, i);
        } else if right_rich {
            self.rotate_left(node_idx, i);
        } else if i > 0 {
            self.merge_children(node_idx, i - 1);
        } else {
            self.merge_children(node_idx, i);
        }
    }

    /// Rotate: pull `keys[i-1]` down to the front of `children[i]`,
    /// push `children[i-1]`'s last key up to `keys[i-1]`.
    fn rotate_right(&mut self, parent_idx: usize, i: usize) {
        let left_idx = self.nodes[parent_idx].children[i - 1];
        let child_idx = self.nodes[parent_idx].children[i];

        let sep_key = self.nodes[parent_idx].keys[i - 1].clone();
        let sep_val = self.nodes[parent_idx].values[i - 1].clone();

        let last_key = self.nodes[left_idx].keys.pop().unwrap();
        let last_val = self.nodes[left_idx].values.pop().unwrap();

        self.nodes[parent_idx].keys[i - 1] = last_key;
        self.nodes[parent_idx].values[i - 1] = last_val;

        self.nodes[child_idx].keys.insert(0, sep_key);
        self.nodes[child_idx].values.insert(0, sep_val);

        if !self.nodes[left_idx].is_leaf {
            let last_child = self.nodes[left_idx].children.pop().unwrap();
            self.nodes[child_idx].children.insert(0, last_child);
        }
    }

    /// Rotate: pull `keys[i]` down to the end of `children[i]`,
    /// push `children[i+1]`'s first key up to `keys[i]`.
    fn rotate_left(&mut self, parent_idx: usize, i: usize) {
        let child_idx = self.nodes[parent_idx].children[i];
        let right_idx = self.nodes[parent_idx].children[i + 1];

        let sep_key = self.nodes[parent_idx].keys[i].clone();
        let sep_val = self.nodes[parent_idx].values[i].clone();

        let first_key = self.nodes[right_idx].keys.remove(0);
        let first_val = self.nodes[right_idx].values.remove(0);

        self.nodes[parent_idx].keys[i] = first_key;
        self.nodes[parent_idx].values[i] = first_val;

        self.nodes[child_idx].keys.push(sep_key);
        self.nodes[child_idx].values.push(sep_val);

        if !self.nodes[right_idx].is_leaf {
            let first_child = self.nodes[right_idx].children.remove(0);
            self.nodes[child_idx].children.push(first_child);
        }
    }

    /// Merge `children[i]` and `children[i+1]`, pulling `keys[i]` down as the
    /// median key.  After the merge `children[i+1]` is tombstoned.
    fn merge_children(&mut self, parent_idx: usize, i: usize) {
        let left_idx = self.nodes[parent_idx].children[i];
        let right_idx = self.nodes[parent_idx].children[i + 1];

        let sep_key = self.nodes[parent_idx].keys.remove(i);
        let sep_val = self.nodes[parent_idx].values.remove(i);
        self.nodes[parent_idx].children.remove(i + 1);

        // Merge separator + right node into left node.
        self.nodes[left_idx].keys.push(sep_key);
        self.nodes[left_idx].values.push(sep_val);

        let right_keys = std::mem::take(&mut self.nodes[right_idx].keys);
        let right_vals = std::mem::take(&mut self.nodes[right_idx].values);
        let right_children = std::mem::take(&mut self.nodes[right_idx].children);

        self.nodes[left_idx].keys.extend(right_keys);
        self.nodes[left_idx].values.extend(right_vals);
        self.nodes[left_idx].children.extend(right_children);

        // Tombstone the now-empty right node.
        self.nodes[right_idx].dead = true;
    }

    /// Extract (and remove) the maximum key from the subtree rooted at `node_idx`.
    fn extract_max(&mut self, node_idx: usize) -> (IndexKey, Vec<RecordId>) {
        if self.nodes[node_idx].is_leaf {
            let n = self.nodes[node_idx].keys.len();
            let key = self.nodes[node_idx].keys.remove(n - 1);
            let val = self.nodes[node_idx].values.remove(n - 1);
            return (key, val);
        }
        let num_children = self.nodes[node_idx].children.len();
        let last_child = self.nodes[node_idx].children[num_children - 1];
        if self.nodes[last_child].keys.len() < self.t {
            self.fix_child(node_idx, num_children - 1);
        }
        // Re-read after potential fix.
        let num_children = self.nodes[node_idx].children.len();
        let last_child = self.nodes[node_idx].children[num_children - 1];
        self.extract_max(last_child)
    }

    /// Extract (and remove) the minimum key from the subtree rooted at `node_idx`.
    fn extract_min(&mut self, node_idx: usize) -> (IndexKey, Vec<RecordId>) {
        if self.nodes[node_idx].is_leaf {
            let key = self.nodes[node_idx].keys.remove(0);
            let val = self.nodes[node_idx].values.remove(0);
            return (key, val);
        }
        let first_child = self.nodes[node_idx].children[0];
        if self.nodes[first_child].keys.len() < self.t {
            self.fix_child(node_idx, 0);
        }
        let first_child = self.nodes[node_idx].children[0];
        self.extract_min(first_child)
    }

    // ─── Range scan ──────────────────────────────────────────────────────────

    fn range_node(
        &self,
        node_idx: usize,
        start: &IndexKey,
        end: &IndexKey,
        result: &mut Vec<RecordId>,
    ) {
        let node = &self.nodes[node_idx];
        if node.dead {
            return;
        }
        let n = node.keys.len();

        for i in 0..n {
            // Visit left child before key[i] if needed.
            if !node.is_leaf {
                // Only visit child[i] if it could contain keys < end.
                if &node.keys[i] > start || (i == 0) {
                    self.range_node(node.children[i], start, end, result);
                }
            }
            if &node.keys[i] >= start && &node.keys[i] <= end {
                result.extend_from_slice(&node.values[i]);
            }
            if &node.keys[i] > end {
                return; // All subsequent keys are also > end.
            }
        }
        // Visit the rightmost child.
        if !node.is_leaf && n > 0 {
            let last_key = node.keys.last().unwrap();
            if last_key < end {
                let last_child = *node.children.last().unwrap();
                self.range_node(last_child, start, end, result);
            }
        }
    }

    // ─── Min / Max helpers ───────────────────────────────────────────────────

    fn leftmost_key(&self, node_idx: usize) -> Option<IndexKey> {
        let node = &self.nodes[node_idx];
        if node.dead || node.keys.is_empty() {
            return None;
        }
        if node.is_leaf {
            return Some(node.keys[0].clone());
        }
        self.leftmost_key(node.children[0])
    }

    fn rightmost_key(&self, node_idx: usize) -> Option<IndexKey> {
        let node = &self.nodes[node_idx];
        if node.dead || node.keys.is_empty() {
            return None;
        }
        if node.is_leaf {
            return Some(node.keys.last().unwrap().clone());
        }
        let last_child = *node.children.last().unwrap();
        self.rightmost_key(last_child)
    }
}

// ─── Trait implementations ────────────────────────────────────────────────────

impl IndexTrait for BTree {
    fn insert(&mut self, key: IndexKey, record_id: RecordId) -> io::Result<()> {
        // If root is full, split it first.
        if self.nodes[self.root].keys.len() == 2 * self.t - 1 {
            let old_root = self.root;
            let new_root_idx = self.nodes.len();
            self.nodes.push(BTreeNode {
                keys: Vec::new(),
                values: Vec::new(),
                children: vec![old_root],
                is_leaf: false,
                dead: false,
            });
            self.root = new_root_idx;
            self.split_child(new_root_idx, 0);
        }
        self.entry_count += 1;
        let root = self.root;
        self.insert_non_full(root, key, record_id);
        Ok(())
    }

    fn search(&self, key: &IndexKey) -> io::Result<Vec<RecordId>> {
        Ok(self.search_node(self.root, key))
    }

    fn delete(&mut self, key: &IndexKey, record_id: &RecordId) -> io::Result<bool> {
        let (removed, key_empty) = self.delete_rid_from_node(self.root, key, record_id);
        if removed {
            self.entry_count = self.entry_count.saturating_sub(1);
            if key_empty {
                self.delete_key(key);
            }
        }
        Ok(removed)
    }

    fn save(&self, path: &str) -> io::Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, json)
    }

    fn entry_count(&self) -> usize {
        self.entry_count
    }

    fn index_type_name(&self) -> &'static str {
        "btree"
    }

    fn all_entries(&self) -> io::Result<Vec<(IndexKey, RecordId)>> {
        let mut out = Vec::new();
        self.collect_entries_from_node(self.root, &mut out);
        Ok(out)
    }

    fn validate_structure(&self) -> io::Result<()> {
        if self.t < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "btree: minimum degree must be >= 2",
            ));
        }
        if self.root >= self.nodes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "btree: root index out of bounds",
            ));
        }
        if self.nodes[self.root].dead {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "btree: root is marked dead",
            ));
        }

        let mut seen = vec![false; self.nodes.len()];
        self.validate_subtree(self.root, &mut seen)
    }
}

impl TreeBasedIndex for BTree {
    fn range_scan(&self, start: &IndexKey, end: &IndexKey) -> io::Result<Vec<RecordId>> {
        let mut result = Vec::new();
        self.range_node(self.root, start, end, &mut result);
        Ok(result)
    }

    fn min_key(&self) -> Option<IndexKey> {
        self.leftmost_key(self.root)
    }

    fn max_key(&self) -> Option<IndexKey> {
        self.rightmost_key(self.root)
    }
}
