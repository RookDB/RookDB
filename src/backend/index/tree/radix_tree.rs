//! Radix tree (compressed trie) index.
//!
//! A radix tree compresses path-compressed trie edges: consecutive single-
//! child nodes are merged into a single edge labelled with a multi-byte
//! prefix string.  This gives O(k) operations where k is the key length in
//! bytes — independent of the number of keys stored.
//!
//! # Key encoding
//!
//! All `IndexKey` variants are converted to a byte slice via
//! `IndexKey::as_bytes()` before insertion.  The encoding is chosen so that
//! lexicographic byte order matches the natural sort order of each key type:
//! * **Int**  : sign-bit-flipped big-endian `u64` → preserves signed ordering.
//! * **Float**: IEEE-754 bits with sign-bit flip (positive) or full flip
//!   (negative) → preserves IEEE-754 ordering.
//! * **Text** : raw UTF-8 bytes → lexicographic = alphabetical.
//!
//! This means range scans on the byte representation are semantically correct
//! for all supported key types.
//!
//! # Structure
//!
//! Each `RadixNode` stores:
//! * A **prefix** byte string for the edge from its parent.
//! * A **sorted** map of first-byte → child node for branching.
//! * An optional list of `RecordId`s if this node is a terminal (marks the
//!   end of a stored key).
//!
//! `std::collections::BTreeMap` is used for children so that in-order
//! iteration over children is always sorted — required for correct range scans.
//!
//! # Complexity
//! | Operation  | Complexity        |
//! |------------|------------------|
//! | search     | O(k)              |
//! | insert     | O(k)              |
//! | delete     | O(k)              |
//! | range_scan | O(k + k · output) |
//! where k = key length in bytes.

use std::collections::BTreeMap;
use std::io;

use serde::{Deserialize, Serialize};

use crate::index::index_trait::{IndexKey, IndexTrait, RecordId, TreeBasedIndex};
use crate::index::paged_store;

// ─── Node ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RadixNode {
    /// Compressed edge label from parent to this node.
    prefix: Vec<u8>,
    /// Children keyed by the first byte of the remaining search string.
    children: BTreeMap<u8, Box<RadixNode>>,
    /// Record IDs stored at this terminal node, along with the original key
    /// for reconstruction during iteration.
    terminal: Option<(IndexKey, Vec<RecordId>)>,
}

impl RadixNode {
    fn new(prefix: Vec<u8>) -> Self {
        Self {
            prefix,
            children: BTreeMap::new(),
            terminal: None,
        }
    }

    /// Length of the longest common prefix of two byte slices.
    fn lcp(a: &[u8], b: &[u8]) -> usize {
        a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
    }

    /// Insert `(remaining_bytes, key, rid)` into the subtree rooted here.
    fn insert(&mut self, remaining: &[u8], original_key: IndexKey, rid: RecordId) {
        let lcp = Self::lcp(remaining, &self.prefix);

        if lcp < self.prefix.len() {
            // Split this node: create a new intermediate node with the common
            // prefix, move this node down with the diverging suffix.
            let common = self.prefix[..lcp].to_vec();
            let old_suffix = self.prefix[lcp..].to_vec();

            // Clone this node's data into a new child.
            let mut old_node = RadixNode::new(old_suffix.clone());
            old_node.children = std::mem::take(&mut self.children);
            old_node.terminal = self.terminal.take();

            // Reset this node to the common prefix.
            self.prefix = common;
            self.children.insert(old_suffix[0], Box::new(old_node));
        }

        // Advance past this node's prefix.
        let rest = &remaining[self.prefix.len()..];

        if rest.is_empty() {
            // This node is the terminal for the inserted key.
            match &mut self.terminal {
                Some((_, records)) => {
                    if !records.contains(&rid) {
                        records.push(rid);
                    }
                }
                t @ None => {
                    *t = Some((original_key, vec![rid]));
                }
            }
            return;
        }

        // Descend into or create the appropriate child.
        let first_byte = rest[0];
        if let Some(child) = self.children.get_mut(&first_byte) {
            child.insert(rest, original_key, rid);
        } else {
            let mut new_child = RadixNode::new(rest.to_vec());
            new_child.terminal = Some((original_key, vec![rid]));
            self.children.insert(first_byte, Box::new(new_child));
        }
    }

    /// Search for `remaining_bytes` in this subtree.
    fn search<'a>(&'a self, remaining: &[u8]) -> Option<&'a Vec<RecordId>> {
        let lcp = Self::lcp(remaining, &self.prefix);
        if lcp < self.prefix.len() {
            return None; // Prefix mismatch.
        }
        let rest = &remaining[self.prefix.len()..];
        if rest.is_empty() {
            return self.terminal.as_ref().map(|(_, r)| r);
        }
        let first = rest[0];
        self.children.get(&first)?.search(rest)
    }

    /// Delete a specific `(remaining_bytes, rid)` pair.
    /// Returns `(was_removed, node_is_now_empty)`.
    fn delete(&mut self, remaining: &[u8], rid: &RecordId) -> (bool, bool) {
        let lcp = Self::lcp(remaining, &self.prefix);
        if lcp < self.prefix.len() {
            return (false, false);
        }
        let rest = &remaining[self.prefix.len()..];

        if rest.is_empty() {
            if let Some((_, records)) = &mut self.terminal {
                let before = records.len();
                records.retain(|r| r != rid);
                let removed = records.len() < before;
                if records.is_empty() {
                    self.terminal = None;
                }
                let empty = self.terminal.is_none() && self.children.is_empty();
                return (removed, empty);
            }
            return (false, false);
        }

        let first = rest[0];
        let child_empty = if let Some(child) = self.children.get_mut(&first) {
            let (removed, child_empty) = child.delete(rest, rid);
            if !removed {
                return (false, false);
            }
            child_empty
        } else {
            return (false, false);
        };

        if child_empty {
            self.children.remove(&first);
            // Optionally compress: if we now have exactly one child and no
            // terminal, merge this node with the child.
            self.try_compress();
        }

        let empty = self.terminal.is_none() && self.children.is_empty();
        (true, empty)
    }

    /// If this node has no terminal and exactly one child, merge the child's
    /// prefix into this node and adopt its children and terminal.
    fn try_compress(&mut self) {
        if self.terminal.is_none() && self.children.len() == 1 {
            let (_, child) = self.children.iter().next().unwrap();
            let mut combined_prefix = self.prefix.clone();
            combined_prefix.extend_from_slice(&child.prefix);
            let child = self.children.values_mut().next().unwrap();
            let new_children = std::mem::take(&mut child.children);
            let new_terminal = child.terminal.take();
            self.prefix = combined_prefix;
            self.children = new_children;
            self.terminal = new_terminal;
        }
    }

    /// Collect all `RecordId`s whose full key bytes fall in `[start_bytes, end_bytes]`.
    /// `accumulated` holds the bytes from ancestor nodes for comparison.
    fn collect_range(
        &self,
        accumulated: &[u8],
        start: &[u8],
        end: &[u8],
        result: &mut Vec<RecordId>,
    ) {
        // Build the full byte prefix up to this node.
        let mut full: Vec<u8> = accumulated.to_vec();
        full.extend_from_slice(&self.prefix);

        // Prune: if full > end, nothing in this subtree can be in range.
        if full.as_slice() > end {
            return;
        }

        // Emit terminal value if within range.
        if let Some((_, records)) = &self.terminal {
            if full.as_slice() >= start && full.as_slice() <= end {
                result.extend_from_slice(records);
            }
        }

        // Recurse into children in sorted byte order.
        for child in self.children.values() {
            child.collect_range(&full, start, end, result);
        }
    }

    /// Collect the minimum key stored in this subtree.
    fn min_key(&self) -> Option<IndexKey> {
        if let Some((k, _)) = &self.terminal {
            return Some(k.clone());
        }
        for child in self.children.values() {
            if let Some(k) = child.min_key() {
                return Some(k);
            }
        }
        None
    }

    /// Collect the maximum key stored in this subtree.
    fn max_key(&self) -> Option<IndexKey> {
        let mut result = None;
        for child in self.children.values() {
            if let Some(k) = child.max_key() {
                result = Some(k);
            }
        }
        if result.is_none() {
            if let Some((k, _)) = &self.terminal {
                return Some(k.clone());
            }
        }
        result
    }

    /// Count total stored RecordIds in this subtree.
    fn count(&self) -> usize {
        let here: usize = self.terminal.as_ref().map(|(_, v)| v.len()).unwrap_or(0);
        let below: usize = self.children.values().map(|c| c.count()).sum();
        here + below
    }

    fn collect_entries(&self, out: &mut Vec<(IndexKey, RecordId)>) {
        if let Some((k, rids)) = &self.terminal {
            for rid in rids {
                out.push((k.clone(), rid.clone()));
            }
        }
        for child in self.children.values() {
            child.collect_entries(out);
        }
    }

    fn validate_node(&self, is_root: bool) -> io::Result<()> {
        if !is_root && self.prefix.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "radix_tree: non-root node has empty prefix",
            ));
        }

        if let Some((_, rids)) = &self.terminal {
            if rids.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "radix_tree: terminal node has empty record list",
                ));
            }
        }

        for (edge_byte, child) in &self.children {
            if child.prefix.is_empty() || child.prefix[0] != *edge_byte {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "radix_tree: child prefix does not match edge label",
                ));
            }
            child.validate_node(false)?;
        }

        Ok(())
    }
}

// ─── Public index type ────────────────────────────────────────────────────────

/// Radix (compressed trie) tree index.
///
/// Provides O(k) operations where `k` is the key length in bytes.  Well-suited
/// for dense string-key workloads or when prefix queries are relevant.
/// Range scans operate in lexicographic byte order which matches the natural
/// sort order for all supported key types (due to the byte encoding in
/// `IndexKey::as_bytes`).
#[derive(Debug, Serialize, Deserialize)]
pub struct RadixTree {
    /// Virtual root with an empty prefix.
    root: RadixNode,
}

impl RadixTree {
    /// Create a new, empty radix tree.
    pub fn new() -> Self {
        Self {
            root: RadixNode::new(Vec::new()),
        }
    }

    /// Load a persisted radix tree from the paged file at `path`.
    pub fn load(path: &str) -> io::Result<Self> {
        let mut index = Self::new();
        paged_store::load_entries_stream(path, |key, rid| index.insert(key, rid))?;
        Ok(index)
    }
}

impl Default for RadixTree {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Trait implementations ────────────────────────────────────────────────────

impl IndexTrait for RadixTree {
    fn insert(&mut self, key: IndexKey, record_id: RecordId) -> io::Result<()> {
        let bytes = key.as_bytes();
        self.root.insert(&bytes, key, record_id);
        Ok(())
    }

    fn search(&self, key: &IndexKey) -> io::Result<Vec<RecordId>> {
        let bytes = key.as_bytes();
        Ok(self.root.search(&bytes).cloned().unwrap_or_default())
    }

    fn delete(&mut self, key: &IndexKey, record_id: &RecordId) -> io::Result<bool> {
        let bytes = key.as_bytes();
        let (removed, _) = self.root.delete(&bytes, record_id);
        Ok(removed)
    }

    fn save(&self, path: &str) -> io::Result<()> {
        paged_store::save_entries(path, self.all_entries()?.into_iter())
    }

    fn entry_count(&self) -> usize {
        self.root.count()
    }

    fn index_type_name(&self) -> &'static str {
        "radix_tree"
    }

    fn all_entries(&self) -> io::Result<Vec<(IndexKey, RecordId)>> {
        let mut out = Vec::new();
        self.root.collect_entries(&mut out);
        Ok(out)
    }

    fn validate_structure(&self) -> io::Result<()> {
        self.root.validate_node(true)
    }
}

impl TreeBasedIndex for RadixTree {
    fn range_scan(&self, start: &IndexKey, end: &IndexKey) -> io::Result<Vec<RecordId>> {
        let start_bytes = start.as_bytes();
        let end_bytes = end.as_bytes();
        let mut result = Vec::new();
        self.root
            .collect_range(&[], &start_bytes, &end_bytes, &mut result);
        Ok(result)
    }

    fn min_key(&self) -> Option<IndexKey> {
        self.root.min_key()
    }

    fn max_key(&self) -> Option<IndexKey> {
        self.root.max_key()
    }
}
