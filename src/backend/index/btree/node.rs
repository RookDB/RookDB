//! `BTreeNode` — internal/leaf page images (framing in `codec.rs`).
//!
//! Moved verbatim from the original `btree.rs`; items the tree core touches
//! are `pub(crate)`.

use crate::backend::error::{RookError, RookResult};
use std::io::{self};

use super::codec::{
    BTREE_PAGE_SIZE, CHILD_SIZE, KEY_LEN_SIZE, LEAF_HEADER, PAGE_HEADER, PAGE_TYPE_INTERNAL,
    PAGE_TYPE_LEAF, VALUE_SIZE,
};

// ─── NodeView ────────────────────────────────────────────────────────────────

/// Borrowed, allocation-free view over a serialized node page.
///
/// [`BTreeNode::deserialize`] copies every key into its own `Vec`, which made
/// allocation the dominant cost of a lookup. Searches and descents only need
/// to walk the keys once, so they read the page through this view instead;
/// the owned `BTreeNode` is still used wherever a node is restructured.
pub(crate) struct NodeView<'a> {
    buf: &'a [u8],
    num_keys: usize,
    is_leaf: bool,
}

impl<'a> NodeView<'a> {
    /// Validate the header and the fixed-width (children / values) area.
    pub(crate) fn parse(buf: &'a [u8]) -> RookResult<Self> {
        if buf.len() < PAGE_HEADER {
            return Err(RookError::io(
                io::ErrorKind::UnexpectedEof,
                "Page too short for header",
            ));
        }
        let page_type = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let num_keys = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
        let is_leaf = match page_type {
            PAGE_TYPE_INTERNAL => {
                if PAGE_HEADER + (num_keys + 1) * CHILD_SIZE > buf.len() {
                    return Err(RookError::io(
                        io::ErrorKind::UnexpectedEof,
                        "Truncated children",
                    ));
                }
                false
            }
            PAGE_TYPE_LEAF => {
                if LEAF_HEADER + num_keys * VALUE_SIZE > buf.len() {
                    return Err(RookError::io(
                        io::ErrorKind::UnexpectedEof,
                        "Truncated values",
                    ));
                }
                true
            }
            _ => {
                return Err(RookError::io(
                    io::ErrorKind::InvalidData,
                    format!("Unknown page type: {}", page_type),
                ));
            }
        };
        Ok(Self {
            buf,
            num_keys,
            is_leaf,
        })
    }

    pub(crate) fn is_leaf(&self) -> bool {
        self.is_leaf
    }

    pub(crate) fn num_keys(&self) -> usize {
        self.num_keys
    }

    /// Child page id `i` of an internal node (`i <= num_keys`).
    pub(crate) fn child(&self, i: usize) -> u32 {
        let off = PAGE_HEADER + i * CHILD_SIZE;
        u32::from_le_bytes([
            self.buf[off],
            self.buf[off + 1],
            self.buf[off + 2],
            self.buf[off + 3],
        ])
    }

    /// Heap location `i` of a leaf node (`i < num_keys`).
    pub(crate) fn value(&self, i: usize) -> (u32, u32) {
        let off = LEAF_HEADER + i * VALUE_SIZE;
        let b = self.buf;
        (
            u32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]),
            u32::from_le_bytes([b[off + 4], b[off + 5], b[off + 6], b[off + 7]]),
        )
    }

    /// Next leaf in the chain (0 = none).
    pub(crate) fn next_leaf(&self) -> u32 {
        u32::from_le_bytes([self.buf[8], self.buf[9], self.buf[10], self.buf[11]])
    }

    /// Byte offset of the first key.
    pub(crate) fn keys_start(&self) -> usize {
        if self.is_leaf {
            LEAF_HEADER + self.num_keys * VALUE_SIZE
        } else {
            PAGE_HEADER + (self.num_keys + 1) * CHILD_SIZE
        }
    }

    /// Walk the keys in order as `(offset, [u16 len][bytes])`.
    pub(crate) fn keys(&self) -> KeyIter<'a> {
        KeyIter {
            buf: self.buf,
            off: self.keys_start(),
            remaining: self.num_keys,
        }
    }

    /// Whether every key on the page is exactly `width` bytes long.
    ///
    /// When it is, key `i` sits at a computable offset and the node can be
    /// binary-searched; see [`NodeView::int_key`]. The check reads only the
    /// length prefixes, so it is cheap next to decoding the keys.
    pub(crate) fn has_uniform_keys(&self, width: usize) -> bool {
        let entry = KEY_LEN_SIZE + width;
        let start = self.keys_start();
        let Some(area) = self.buf.get(start..start + self.num_keys * entry) else {
            return false;
        };
        // Every descent runs this on every node it visits, so the loop is
        // kept free of branches and bounds checks rather than stopping at
        // the first mismatch, which a healthy page never has.
        let prefix = (width as u16).to_le_bytes();
        area.chunks_exact(entry).fold(0u8, |diff, key| {
            diff | (key[0] ^ prefix[0]) | (key[1] ^ prefix[1])
        }) == 0
    }

    /// Key `i` as a sign-extended integer. Only valid on a page for which
    /// [`NodeView::has_uniform_keys`] returned true for `width` (2, 4 or 8).
    pub(crate) fn int_key(&self, i: usize, width: usize) -> i64 {
        let off = self.keys_start() + i * (KEY_LEN_SIZE + width) + KEY_LEN_SIZE;
        int_from_le(&self.buf[off..off + width])
    }

    /// Number of leading keys for which `pred` holds, by binary search.
    /// Same preconditions as [`NodeView::int_key`]; `pred` must be true for
    /// a prefix of the (sorted) keys and false for the rest.
    pub(crate) fn int_partition_point(&self, width: usize, pred: impl Fn(i64) -> bool) -> usize {
        let (mut lo, mut hi) = (0, self.num_keys);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if pred(self.int_key(mid, width)) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Offset one past the last key, i.e. the bytes this node occupies.
    pub(crate) fn used_bytes(&self) -> RookResult<usize> {
        let mut end = self.keys_start();
        for entry in self.keys() {
            let (off, key) = entry?;
            end = off + key.len();
        }
        Ok(end)
    }

    /// Whether the node can take one more entry with a key of this size.
    pub(crate) fn has_space_for(&self, encoded_key_size: usize) -> RookResult<bool> {
        let pointer = if self.is_leaf { VALUE_SIZE } else { CHILD_SIZE };
        Ok(self.used_bytes()? + pointer + encoded_key_size <= BTREE_PAGE_SIZE)
    }
}

/// Decode a little-endian two's-complement integer of 2, 4 or 8 bytes.
pub(crate) fn int_from_le(bytes: &[u8]) -> i64 {
    match bytes.len() {
        2 => i16::from_le_bytes([bytes[0], bytes[1]]) as i64,
        4 => i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64,
        _ => {
            let mut raw = [0u8; 8];
            raw.copy_from_slice(&bytes[..8]);
            i64::from_le_bytes(raw)
        }
    }
}

/// Iterator over the length-prefixed keys of a [`NodeView`].
pub(crate) struct KeyIter<'a> {
    buf: &'a [u8],
    off: usize,
    remaining: usize,
}

impl<'a> Iterator for KeyIter<'a> {
    type Item = RookResult<(usize, &'a [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let off = self.off;
        if off + KEY_LEN_SIZE > self.buf.len() {
            self.remaining = 0;
            return Some(Err(RookError::io(
                io::ErrorKind::UnexpectedEof,
                "Truncated key length",
            )));
        }
        let key_len = u16::from_le_bytes([self.buf[off], self.buf[off + 1]]) as usize;
        let end = off + KEY_LEN_SIZE + key_len;
        if end > self.buf.len() {
            self.remaining = 0;
            return Some(Err(RookError::io(
                io::ErrorKind::UnexpectedEof,
                "Truncated key data",
            )));
        }
        self.off = end;
        Some(Ok((off, &self.buf[off..end])))
    }
}

// ─── BTreeNode ───────────────────────────────────────────────────────────────

/// In-memory representation of a B+ Tree node.
#[derive(Debug, Clone)]
pub enum BTreeNode {
    /// An internal (branch) node: contains keys and child page pointers.
    Internal {
        num_keys: u32,
        /// Encoded keys in sorted order, each as `[u16 len][key_bytes]`.
        keys: Vec<Vec<u8>>,
        /// Child page IDs: length = num_keys + 1.
        children: Vec<u32>,
    },
    /// A leaf node: contains keys and heap tuple identifiers.
    Leaf {
        num_keys: u32,
        /// Encoded keys in sorted order.
        keys: Vec<Vec<u8>>,
        /// Heap tuple identifiers: (page_id, slot_id) per entry.
        values: Vec<(u32, u32)>,
        /// Next leaf in the linked list (0 = none).
        next_leaf: u32,
        /// Previous leaf in the linked list (0 = none).
        prev_leaf: u32,
    },
}

impl BTreeNode {
    /// Create a new empty leaf node.
    pub(crate) fn new_leaf() -> Self {
        BTreeNode::Leaf {
            num_keys: 0,
            keys: Vec::new(),
            values: Vec::new(),
            next_leaf: 0,
            prev_leaf: 0,
        }
    }

    /// Create a new empty internal node.
    pub(crate) fn new_internal() -> Self {
        BTreeNode::Internal {
            num_keys: 0,
            keys: Vec::new(),
            children: Vec::new(),
        }
    }

    /// Serialize this node into a byte buffer for writing to disk.
    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; BTREE_PAGE_SIZE];
        match self {
            BTreeNode::Internal {
                num_keys,
                keys,
                children,
            } => {
                // Header
                buf[0..4].copy_from_slice(&PAGE_TYPE_INTERNAL.to_le_bytes());
                buf[4..8].copy_from_slice(&num_keys.to_le_bytes());

                // Children (starts at offset 8)
                let children_start = PAGE_HEADER;
                for (i, &child) in children.iter().enumerate() {
                    let off = children_start + i * CHILD_SIZE;
                    if off + CHILD_SIZE <= BTREE_PAGE_SIZE {
                        buf[off..off + CHILD_SIZE].copy_from_slice(&child.to_le_bytes());
                    }
                }

                // Keys (after children)
                let mut key_off = children_start + children.len() * CHILD_SIZE;
                for key in keys {
                    if key_off + key.len() <= BTREE_PAGE_SIZE {
                        buf[key_off..key_off + key.len()].copy_from_slice(key);
                        key_off += key.len();
                    }
                }
            }
            BTreeNode::Leaf {
                num_keys,
                keys,
                values,
                next_leaf,
                prev_leaf,
            } => {
                // Header
                buf[0..4].copy_from_slice(&PAGE_TYPE_LEAF.to_le_bytes());
                buf[4..8].copy_from_slice(&num_keys.to_le_bytes());
                buf[8..12].copy_from_slice(&next_leaf.to_le_bytes());
                buf[12..16].copy_from_slice(&prev_leaf.to_le_bytes());

                // Values (starts at offset 16)
                let values_start = LEAF_HEADER;
                for (i, &(page_id, slot_id)) in values.iter().enumerate() {
                    let off = values_start + i * VALUE_SIZE;
                    if off + VALUE_SIZE <= BTREE_PAGE_SIZE {
                        buf[off..off + 4].copy_from_slice(&page_id.to_le_bytes());
                        buf[off + 4..off + 8].copy_from_slice(&slot_id.to_le_bytes());
                    }
                }

                // Keys (after values)
                let mut key_off = values_start + values.len() * VALUE_SIZE;
                for key in keys {
                    if key_off + key.len() <= BTREE_PAGE_SIZE {
                        buf[key_off..key_off + key.len()].copy_from_slice(key);
                        key_off += key.len();
                    }
                }
            }
        }
        buf
    }

    /// Deserialize a node from a byte buffer.
    pub(crate) fn deserialize(buf: &[u8]) -> RookResult<Self> {
        if buf.len() < PAGE_HEADER {
            return Err(RookError::io(
                io::ErrorKind::UnexpectedEof,
                "Page too short for header",
            ));
        }

        let page_type = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let num_keys = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let nk = num_keys as usize;

        match page_type {
            PAGE_TYPE_INTERNAL => {
                let children_start = PAGE_HEADER;
                let num_children = nk + 1;

                // Read children
                let mut children = Vec::with_capacity(num_children);
                for i in 0..num_children {
                    let off = children_start + i * CHILD_SIZE;
                    if off + CHILD_SIZE > buf.len() {
                        return Err(RookError::io(
                            io::ErrorKind::UnexpectedEof,
                            "Truncated children",
                        ));
                    }
                    let child =
                        u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
                    children.push(child);
                }

                // Read keys
                let mut key_off = children_start + num_children * CHILD_SIZE;
                let mut keys = Vec::with_capacity(nk);
                for _ in 0..nk {
                    if key_off + KEY_LEN_SIZE > buf.len() {
                        return Err(RookError::io(
                            io::ErrorKind::UnexpectedEof,
                            "Truncated key length",
                        ));
                    }
                    let key_len = u16::from_le_bytes([buf[key_off], buf[key_off + 1]]) as usize;
                    let total_entry = KEY_LEN_SIZE + key_len;
                    if key_off + total_entry > buf.len() {
                        return Err(RookError::io(
                            io::ErrorKind::UnexpectedEof,
                            "Truncated key data",
                        ));
                    }
                    keys.push(buf[key_off..key_off + total_entry].to_vec());
                    key_off += total_entry;
                }

                Ok(BTreeNode::Internal {
                    num_keys,
                    keys,
                    children,
                })
            }
            PAGE_TYPE_LEAF => {
                if buf.len() < LEAF_HEADER {
                    return Err(RookError::io(
                        io::ErrorKind::UnexpectedEof,
                        "Page too short for leaf header",
                    ));
                }
                let next_leaf = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
                let prev_leaf = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);

                // Read values
                let values_start = LEAF_HEADER;
                let mut values = Vec::with_capacity(nk);
                for i in 0..nk {
                    let off = values_start + i * VALUE_SIZE;
                    if off + VALUE_SIZE > buf.len() {
                        return Err(RookError::io(
                            io::ErrorKind::UnexpectedEof,
                            "Truncated values",
                        ));
                    }
                    let page_id =
                        u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
                    let slot_id = u32::from_le_bytes([
                        buf[off + 4],
                        buf[off + 5],
                        buf[off + 6],
                        buf[off + 7],
                    ]);
                    values.push((page_id, slot_id));
                }

                // Read keys
                let mut key_off = values_start + nk * VALUE_SIZE;
                let mut keys = Vec::with_capacity(nk);
                for _ in 0..nk {
                    if key_off + KEY_LEN_SIZE > buf.len() {
                        return Err(RookError::io(
                            io::ErrorKind::UnexpectedEof,
                            "Truncated key length",
                        ));
                    }
                    let key_len = u16::from_le_bytes([buf[key_off], buf[key_off + 1]]) as usize;
                    let total_entry = KEY_LEN_SIZE + key_len;
                    if key_off + total_entry > buf.len() {
                        return Err(RookError::io(
                            io::ErrorKind::UnexpectedEof,
                            "Truncated key data",
                        ));
                    }
                    keys.push(buf[key_off..key_off + total_entry].to_vec());
                    key_off += total_entry;
                }

                Ok(BTreeNode::Leaf {
                    num_keys,
                    keys,
                    values,
                    next_leaf,
                    prev_leaf,
                })
            }
            _ => Err(RookError::io(
                io::ErrorKind::InvalidData,
                format!("Unknown page type: {}", page_type),
            )),
        }
    }
}
