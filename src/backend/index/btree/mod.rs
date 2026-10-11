//! B+ Tree — Page-based balanced tree for disk-resident key-value storage.
//!
//! # Design
//!
//! - 8 KB pages stored in a separate `.idx` file per index.
//! - Nodes are either **Internal** (page_type=0) or **Leaf** (page_type=1).
//! - Leaves are linked into a doubly-linked list for efficient range scans.
//! - Keys are variable-length: `[key_len: u16][encoded_key_bytes]`.
//! - Values are heap tuple identifiers `(page_id: u32, slot_id: u32)`.
//! - Page-level write latching (via `PageWriteLock`) protects splits.
//!
//! # Page Format
//!
//! **Header (shared):**
//! | Offset | Size | Field |
//! |--------|------|-------|
//! | 0 | 4 | page_type: 0=internal, 1=leaf |
//! | 4 | 4 | num_keys |
//!
//! **Internal node (after header):**
//! | Offset | Size | Field |
//! |--------|------|-------|
//! | 8 | 4×(n+1) | child_0 … child_n |
//! | 8+4×(n+1) | * | key_0_len(2) + key_0_bytes  …  key_n-1_len(2) + key_n-1_bytes |
//!
//! **Leaf node (after header):**
//! | Offset | Size | Field |
//! |--------|------|-------|
//! | 8 | 4 | next_leaf_page_id |
//! | 12 | 4 | prev_leaf_page_id |
//! | 16 | 8×n | (page_id, slot_id) per entry |
//! | 16+8×n | * | key_0_len(2) + key_bytes …  key_n-1_len(2) + key_bytes |

use std::cmp::Ordering;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::PathBuf;

mod codec;
mod node;

pub(crate) use codec::{
    BTREE_PAGE_SIZE, KEY_LEN_SIZE, decode_key, encode_key, encode_key_multi, read_root_sidecar,
    write_root_sidecar,
};
use codec::{CHILD_SIZE, LEAF_HEADER, PAGE_HEADER, VALUE_SIZE};
pub use node::BTreeNode;
use node::{NodeView, int_from_le};

use crate::backend::buffer_manager::buffer_pool::BufferPool;
use crate::backend::page::page_lock::PageWriteLock;
use crate::backend::table::table_file::file_identity_from_file;
use crate::types::Comparable;
use crate::types::DataType;
use crate::types::value::DataValue;

// ─── Constants ───────────────────────────────────────────────────────────────

/// Default capacity for an index buffer pool in 8 KB frames.
pub const INDEX_POOL_CAPACITY: usize = 256;

/// Largest encoded key an index accepts: a third of a leaf's payload, so a
/// byte-balanced split always yields two halves that fit and every internal
/// node keeps at least two keys.
pub const MAX_ENCODED_KEY_SIZE: usize = (BTREE_PAGE_SIZE - LEAF_HEADER) / 3 - VALUE_SIZE;

/// Encoded size of an index key, for callers that must reject an oversized
/// key before changing anything.
pub fn encoded_key_len(keys: &[DataValue]) -> io::Result<usize> {
    match keys {
        [single] => Ok(encode_key(single).len()),
        _ => encode_key_multi(keys).map(|k| k.len()),
    }
}

/// Index at which to split `keys` so both halves hold about the same number
/// of bytes. `pointer_size` is the per-entry value or child pointer. Always
/// leaves at least one key on each side.
fn balanced_split_point(keys: &[Vec<u8>], pointer_size: usize) -> usize {
    let total: usize = keys.iter().map(|k| k.len() + pointer_size).sum();
    let mut left = 0;
    let mut split = keys.len() / 2;
    for (i, key) in keys.iter().enumerate() {
        left += key.len() + pointer_size;
        if left * 2 >= total {
            split = i + 1;
            break;
        }
    }
    split.clamp(1, keys.len().saturating_sub(1).max(1))
}

/// Refuse to serialize a node that does not fit its page: `serialize` would
/// otherwise drop the overflowing keys without a trace.
fn check_node_fits(node: &BTreeNode) -> io::Result<()> {
    let size = match node {
        BTreeNode::Internal { keys, children, .. } => {
            PAGE_HEADER + children.len() * CHILD_SIZE + keys.iter().map(Vec::len).sum::<usize>()
        }
        BTreeNode::Leaf { keys, values, .. } => {
            LEAF_HEADER + values.len() * VALUE_SIZE + keys.iter().map(Vec::len).sum::<usize>()
        }
    };
    if size > BTREE_PAGE_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "B-tree node of {} bytes does not fit a {}-byte page",
                size, BTREE_PAGE_SIZE
            ),
        ));
    }
    Ok(())
}
// ─── BTree ───────────────────────────────────────────────────────────────────

/// A page-based B+ Tree index stored in a `.idx` file.
///
/// Provides point lookups, range scans, and ordered insertion.
pub struct BTree {
    /// Path to the `.idx` file.
    #[allow(dead_code)]
    file_path: PathBuf,
    /// Buffer pool managing 8 KB index pages.
    pool: BufferPool,
    /// Stable file identity used for PageWriteLock.
    file_id: u64,
    /// The data type(s) of the indexed column(s). A single entry is a plain
    /// single-column index; multiple entries form a composite key where each
    /// value contributes one `[len][bytes]` segment in order.
    key_types: Vec<DataType>,
    /// Cached root page ID.
    root_page_id: u32,
    /// Total number of pages in the index file.
    total_pages: u32,
}

impl BTree {
    /// Create a new B+ Tree index file.
    pub fn create(file_path: PathBuf, key_type: DataType) -> io::Result<Self> {
        Self::create_composite(file_path, vec![key_type])
    }

    /// Create a new composite-key B+ Tree index file.
    pub fn create_composite(file_path: PathBuf, key_types: Vec<DataType>) -> io::Result<Self> {
        log::info!(
            "[BTree::create] Creating new index at {:?} ({} col)",
            file_path,
            key_types.len()
        );

        if key_types.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Composite index requires at least one key column",
            ));
        }

        // Remove existing file if present
        if file_path.exists() {
            std::fs::remove_file(&file_path)?;
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&file_path)?;

        let file_id = file_identity_from_file(&file)?;

        // Create the root node (empty leaf)
        let root = BTreeNode::new_leaf();
        let buf = root.serialize();
        file.write_all(&buf)?;
        file.flush()?;
        file.sync_all()?;

        // Persist the root pointer sidecar so reopen lands on the real root.
        write_root_sidecar(&file_path, 0)?;

        let pool = BufferPool::with_file_raw(INDEX_POOL_CAPACITY, file, file_path.clone(), 1);

        log::info!("[BTree::create] Created with 1 page, root=0");
        Ok(Self {
            file_path,
            pool,
            file_id,
            key_types,
            root_page_id: 0,
            total_pages: 1,
        })
    }

    /// Open an existing B+ Tree index file.
    pub fn open(file_path: PathBuf) -> io::Result<Self> {
        log::info!("[BTree::open] Opening index at {:?}", file_path);

        if !file_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Index file not found",
            ));
        }

        let file = OpenOptions::new().read(true).write(true).open(&file_path)?;

        let file_id = file_identity_from_file(&file)?;
        let file_size = file.metadata()?.len();
        let total_pages = (file_size as usize / BTREE_PAGE_SIZE) as u32;

        let pool =
            BufferPool::with_file_raw(INDEX_POOL_CAPACITY, file, file_path.clone(), total_pages);

        // The key_type is not stored in the file yet — callers must provide it
        // In a full implementation, type info would be stored in a header page.
        // For now, use a placeholder (will be set explicitly).
        let key_types = vec![DataType::Int];

        // Restore the persisted root pointer. Without it, every reopened
        // index descended into the original leftmost leaf and searched via
        // the next_leaf chain — O(n) lookups on any real tree.
        let root_page_id = read_root_sidecar(&file_path).unwrap_or(0);

        log::info!(
            "[BTree::open] Opened with {} pages, root={}",
            total_pages,
            root_page_id
        );
        Ok(Self {
            file_path,
            pool,
            file_id,
            key_types,
            root_page_id,
            total_pages,
        })
    }

    /// Set the key type (required after opening if the placeholder was used).
    pub fn set_key_type(&mut self, ty: DataType) {
        self.key_types = vec![ty];
    }

    /// Set the composite key types (required after opening).
    pub fn set_key_types(&mut self, types: Vec<DataType>) {
        assert!(!types.is_empty(), "key_types must not be empty");
        self.key_types = types;
    }

    /// Return the single-column key type (first segment for composites).
    pub fn key_type(&self) -> &DataType {
        &self.key_types[0]
    }

    /// Debug: root page id.
    pub fn root_page_id(&self) -> u32 {
        self.root_page_id
    }

    /// Debug: decoded separator keys stored in the root node.
    pub fn debug_root_keys(&mut self) -> Vec<String> {
        let node = self.read_node(self.root_page_id).expect("read root");
        match node {
            BTreeNode::Internal { keys, children, .. } => {
                let _ = children;
                keys.iter()
                    .map(|k| {
                        let (v, _) = decode_key(k, &self.key_types[0])
                            .map_err(|e| format!("decode err: {}", e))
                            .unwrap_or((DataValue::Int(-1), 0));
                        format!("{:?}", v)
                    })
                    .collect()
            }
            BTreeNode::Leaf { keys, .. } => vec![format!("<leaf with {} keys>", keys.len())],
        }
    }

    /// Return all key segment types.
    pub fn key_types(&self) -> &[DataType] {
        &self.key_types
    }

    // ─── Composite Key Comparison Helpers ──────────────────────────────────

    /// Encode lookup/insert keys in the format this tree stores:
    /// bare `[len][bytes]` for single-column trees (legacy on-disk
    /// compatibility), or the framed composite envelope otherwise.
    fn encode_key_for(&self, keys: &[DataValue]) -> io::Result<Vec<u8>> {
        if self.key_types.len() == 1 {
            if keys.len() != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "single-column index takes exactly one key value",
                ));
            }
            Ok(encode_key(&keys[0]))
        } else {
            encode_key_multi(keys)
        }
    }

    /// Compare two encoded keys segment-by-segment using `key_types`.
    ///
    /// Encoded keys are opaque `[len][bytes]` concatenations; decoding each
    /// segment with its declared type preserves value ordering (plain byte
    /// comparison would not — little-endian integers sort wrong as bytes).
    ///
    /// Single-column trees store bare segments (legacy format); composite
    /// trees wrap the whole key in an envelope (see `encode_key_multi`).
    fn cmp_encoded_keys(&self, a: &[u8], b: &[u8]) -> io::Result<Ordering> {
        use std::cmp::Ordering as O;
        if self.key_types.len() == 1 {
            let (va, _) = decode_key(a, &self.key_types[0])?;
            let (vb, _) = decode_key(b, &self.key_types[0])?;
            return va
                .compare(&vb)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
        }
        // Composite: strip the outer envelope from both sides, then walk
        // inner segments in lockstep.
        if a.len() < KEY_LEN_SIZE || b.len() < KEY_LEN_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Truncated composite key envelope",
            ));
        }
        let mut ia = KEY_LEN_SIZE;
        let mut ib = KEY_LEN_SIZE;
        let mut seg = 0usize;
        loop {
            match (ia >= a.len(), ib >= b.len()) {
                (true, true) => return Ok(O::Equal),
                (true, false) => return Ok(O::Less),
                (false, true) => return Ok(O::Greater),
                (false, false) => {}
            }
            // Segments align positionally: type for the current segment index.
            let ty = &self.key_types[seg % self.key_types.len()];
            let (va, ca) = decode_key(&a[ia..], ty)?;
            let (vb, cb) = decode_key(&b[ib..], ty)?;
            let c = va
                .compare(&vb)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            if c != O::Equal {
                return Ok(c);
            }
            ia += ca;
            ib += cb;
            seg += 1;
        }
    }

    /// Compare an encoded key against a slice of typed values (one per key
    /// segment). Used by range bounds and lookups supplied as values.
    fn cmp_encoded_vs_values(&self, encoded: &[u8], values: &[DataValue]) -> io::Result<Ordering> {
        use std::cmp::Ordering as O;
        debug_assert_eq!(
            values.len(),
            self.key_types.len(),
            "value/key arity mismatch"
        );
        if self.key_types.len() == 1 {
            let (ev, _) = decode_key(encoded, &self.key_types[0])?;
            return ev
                .compare(&values[0])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()));
        }
        if encoded.len() < KEY_LEN_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Truncated composite key envelope",
            ));
        }
        let mut off = KEY_LEN_SIZE;
        for (i, v) in values.iter().enumerate() {
            if off >= encoded.len() {
                return Ok(O::Less);
            }
            let (ev, consumed) = decode_key(&encoded[off..], &self.key_types[i])?;
            let c = ev
                .compare(v)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            if c != O::Equal {
                return Ok(c);
            }
            off += consumed;
        }
        Ok(O::Equal)
    }

    /// Scan all entries in the B+ Tree by traversing the leaf linked list.
    ///
    /// Returns every `(page_id, slot_id)` pair stored in the index,
    /// in key order. This implements a full index scan.
    pub fn scan_all(&mut self) -> io::Result<Vec<(u32, u32)>> {
        let mut results = Vec::new();

        // Find the leftmost leaf by always following child[0]
        let first_leaf = self.find_leftmost_leaf(self.root_page_id)?;

        // Scan all leaves via the doubly-linked list
        let mut current = first_leaf;
        loop {
            let buf = self.pool.read_page_bytes(current)?;
            let view = NodeView::parse(&buf)?;
            if !view.is_leaf() {
                unreachable!("Expected leaf node in scan_all");
            }
            results.extend((0..view.num_keys()).map(|i| view.value(i)));
            let next_leaf = view.next_leaf();
            if next_leaf == 0 {
                break;
            }
            current = next_leaf;
        }

        Ok(results)
    }

    /// Navigate to the leftmost leaf by always following child[0] from the root.
    fn find_leftmost_leaf(&mut self, page_id: u32) -> io::Result<u32> {
        let mut page_id = page_id;
        loop {
            let buf = self.pool.read_page_bytes(page_id)?;
            let view = NodeView::parse(&buf)?;
            if view.is_leaf() {
                return Ok(page_id);
            }
            page_id = view.child(0);
        }
    }

    /// Key width in bytes when this is a single-column integer index.
    fn int_key_width(&self) -> Option<usize> {
        match self.key_types.as_slice() {
            [DataType::SmallInt] => Some(2),
            [DataType::Int] => Some(4),
            [DataType::BigInt] => Some(8),
            _ => None,
        }
    }

    /// Key width when `view` can be binary-searched as plain integers: a
    /// single-column integer index whose keys on this page all have the
    /// declared width. Comparing them as `i64` matches `DataValue::compare`,
    /// which widens SMALLINT/INT/BIGINT before comparing.
    fn int_layout(&self, view: &NodeView<'_>) -> Option<usize> {
        let width = self.int_key_width()?;
        view.has_uniform_keys(width).then_some(width)
    }

    /// [`BTree::int_layout`] plus the integer held by `encoded_key`, when the
    /// encoded key has exactly the declared width.
    fn int_fast_path(&self, view: &NodeView<'_>, encoded_key: &[u8]) -> Option<(usize, i64)> {
        let width = self.int_layout(view)?;
        if encoded_key.len() != KEY_LEN_SIZE + width
            || u16::from_le_bytes([encoded_key[0], encoded_key[1]]) as usize != width
        {
            return None;
        }
        Some((width, int_from_le(&encoded_key[KEY_LEN_SIZE..])))
    }

    /// Pick the child of an internal node to descend into for `encoded_key`:
    /// the child left of the first separator greater than the key, or the
    /// last child when no separator is greater. Returns `(index, page_id)`.
    fn pick_child(&self, view: &NodeView<'_>, encoded_key: &[u8]) -> io::Result<(usize, u32)> {
        if let Some((width, target)) = self.int_fast_path(view, encoded_key) {
            let child_idx = view.int_partition_point(width, |k| k <= target);
            return Ok((child_idx, view.child(child_idx)));
        }

        let mut child_idx = view.num_keys();
        for (i, entry) in view.keys().enumerate() {
            let (_, key) = entry?;
            if self.cmp_encoded_keys(key, encoded_key)? == Ordering::Greater {
                child_idx = i;
                break;
            }
        }
        Ok((child_idx, view.child(child_idx)))
    }

    /// Pick the leftmost child that can hold `encoded_key`: the child left of
    /// the first separator that is not less than the key. Duplicates of a key
    /// can sit on both sides of an equal separator, so readers start here and
    /// walk right along the leaf chain.
    fn pick_child_lower(&self, view: &NodeView<'_>, encoded_key: &[u8]) -> io::Result<u32> {
        if let Some((width, target)) = self.int_fast_path(view, encoded_key) {
            let child_idx = view.int_partition_point(width, |k| k < target);
            return Ok(view.child(child_idx));
        }

        let mut child_idx = view.num_keys();
        for (i, entry) in view.keys().enumerate() {
            let (_, key) = entry?;
            if self.cmp_encoded_keys(key, encoded_key)? != Ordering::Less {
                child_idx = i;
                break;
            }
        }
        Ok(view.child(child_idx))
    }

    /// Return the total number of pages in the index file.
    pub fn total_pages(&self) -> u32 {
        self.pool.total_pages()
    }

    // ─── Page I/O ──────────────────────────────────────────────────────────

    /// Read a node page via the buffer pool (cached in memory on hits).
    fn read_node(&mut self, page_id: u32) -> io::Result<BTreeNode> {
        let buf = self.pool.read_page_bytes(page_id)?;
        BTreeNode::deserialize(&buf)
    }

    /// Write a node page through the buffer pool (marks page dirty).
    fn write_node(&mut self, page_id: u32, node: &BTreeNode) -> io::Result<()> {
        check_node_fits(node)?;
        let buf = node.serialize();
        self.pool.write_page_bytes(page_id, &buf)?;
        Ok(())
    }

    /// Allocate a new page (appends to file and caches in buffer pool).
    fn allocate_page(&mut self, node: &BTreeNode) -> io::Result<u32> {
        check_node_fits(node)?;
        let buf = node.serialize();
        let page_id = self.pool.allocate_raw_page(&buf)?;
        self.total_pages = self.pool.total_pages();
        Ok(page_id)
    }

    /// Sync all changes to durable storage.
    pub fn sync(&mut self) -> io::Result<()> {
        self.pool.flush_all()?;
        Ok(())
    }

    /// Write dirty pages to the index file without fsync.
    pub fn write_through(&mut self) -> io::Result<()> {
        self.pool.write_dirty()
    }

    // ─── Public Search API ─────────────────────────────────────────────────

    /// Search for a key in the B+ Tree.
    ///
    /// Returns `Ok(Some((page_id, slot_id)))` if the key was found, or `Ok(None)` if not.
    pub fn search(&mut self, key: &DataValue) -> io::Result<Option<(u32, u32)>> {
        self.search_keys(std::slice::from_ref(key))
    }

    /// Search for a (possibly composite) key in the B+ Tree.
    pub fn search_keys(&mut self, keys: &[DataValue]) -> io::Result<Option<(u32, u32)>> {
        let encoded = self.encode_key_for(keys)?;
        self.search_in_tree(self.root_page_id, &encoded)
    }

    /// Range search: find all entries with keys in `[low, high]`.
    ///
    /// Returns a vector of heap tuple identifiers.
    pub fn search_range(
        &mut self,
        low: &DataValue,
        high: &DataValue,
    ) -> io::Result<Vec<(u32, u32)>> {
        self.search_range_keys(std::slice::from_ref(low), std::slice::from_ref(high))
    }

    /// Range search over composite keys: find entries with key segments
    /// lexicographically within `[low_parts, high_parts]`.
    pub fn search_range_keys(
        &mut self,
        low: &[DataValue],
        high: &[DataValue],
    ) -> io::Result<Vec<(u32, u32)>> {
        let low_encoded = self.encode_key_for(low)?;
        let mut results = Vec::new();

        // Navigate to the leaf containing `low`
        let leaf_id = self.find_first_leaf(self.root_page_id, &low_encoded)?;

        // Scan the leaf and subsequent leaves
        let mut current_leaf = leaf_id;
        loop {
            let buf = self.pool.read_page_bytes(current_leaf)?;
            let view = NodeView::parse(&buf)?;
            if !view.is_leaf() {
                unreachable!("Expected leaf node in range scan");
            }
            if let (Some(width), [low_value], [high_value]) = (self.int_layout(&view), low, high)
                && let (Some(low_int), Some(high_int)) =
                    (int_value(low_value), int_value(high_value))
            {
                let first = view.int_partition_point(width, |k| k < low_int);
                for i in first..view.num_keys() {
                    if view.int_key(i, width) > high_int {
                        // Keys are sorted — we've passed the range
                        return Ok(results);
                    }
                    results.push(view.value(i));
                }
                let next_leaf = view.next_leaf();
                if next_leaf == 0 {
                    break;
                }
                current_leaf = next_leaf;
                continue;
            }

            for (i, entry) in view.keys().enumerate() {
                let (_, key) = entry?;
                let cmp_low = self.cmp_encoded_vs_values(key, low)?;
                let cmp_high = self.cmp_encoded_vs_values(key, high)?;
                if cmp_low != Ordering::Less && cmp_high != Ordering::Greater {
                    results.push(view.value(i));
                } else if cmp_high == Ordering::Greater {
                    // Keys are sorted — we've passed the range
                    return Ok(results);
                }
            }
            let next_leaf = view.next_leaf();
            if next_leaf == 0 {
                break;
            }
            current_leaf = next_leaf;
        }

        Ok(results)
    }

    /// Check whether a key exists in the tree.
    pub fn contains(&mut self, key: &DataValue) -> io::Result<bool> {
        self.search(key).map(|opt| opt.is_some())
    }

    /// Check whether a (possibly composite) key exists in the tree.
    pub fn contains_keys(&mut self, keys: &[DataValue]) -> io::Result<bool> {
        self.search_keys(keys).map(|opt| opt.is_some())
    }

    // ─── Public Search All API ────────────────────────────────────────────

    /// Search for all entries matching a key in the B+ Tree.
    ///
    /// Returns all `(page_id, slot_id)` pairs whose key equals the given key.
    /// For indexes without duplicates, this returns zero or one entry.
    /// For non-unique indexes, this returns all matching entries.
    pub fn search_all(&mut self, key: &DataValue) -> io::Result<Vec<(u32, u32)>> {
        self.search_all_keys(std::slice::from_ref(key))
    }

    /// Search for all entries matching a (possibly composite) key.
    pub fn search_all_keys(&mut self, keys: &[DataValue]) -> io::Result<Vec<(u32, u32)>> {
        let encoded = self.encode_key_for(keys)?;
        self.search_all_in_tree(self.root_page_id, &encoded)
    }

    /// Internal search_all: return all entries matching encoded_key.
    fn search_all_in_tree(
        &mut self,
        page_id: u32,
        encoded_key: &[u8],
    ) -> io::Result<Vec<(u32, u32)>> {
        let mut leaf_id = self.find_first_leaf(page_id, encoded_key)?;
        let mut results = Vec::new();
        // Duplicates can span several leaves: keep walking the chain until a
        // larger key shows up.
        loop {
            let buf = self.pool.read_page_bytes(leaf_id)?;
            let view = NodeView::parse(&buf)?;
            let mut past_key = false;
            if let Some((width, target)) = self.int_fast_path(&view, encoded_key) {
                let first = view.int_partition_point(width, |k| k < target);
                for i in first..view.num_keys() {
                    if view.int_key(i, width) != target {
                        past_key = true;
                        break;
                    }
                    results.push(view.value(i));
                }
            } else {
                for (i, entry) in view.keys().enumerate() {
                    let (_, key) = entry?;
                    let cmp = self.cmp_encoded_keys(key, encoded_key)?;
                    if cmp == Ordering::Equal {
                        results.push(view.value(i));
                    } else if cmp == Ordering::Greater {
                        past_key = true;
                        break;
                    }
                }
            }
            let next_leaf = view.next_leaf();
            if past_key || next_leaf == 0 {
                return Ok(results);
            }
            leaf_id = next_leaf;
        }
    }

    // ─── Public Delete API ─────────────────────────────────────────────────

    /// Delete a key from the B+ Tree.
    ///
    /// Searches for an entry matching BOTH the key and the heap location
    /// `(page_id, slot_id)`, and removes it if found. Matching by value is
    /// essential for non-unique indexes where the same key can appear in
    /// multiple entries.
    ///
    /// Does NOT rebalance or merge underfull nodes — the tree remains
    /// searchable but may have reduced space utilization.
    ///
    /// Returns `true` if the entry was found and deleted, `false` if not found.
    pub fn delete(&mut self, key: &DataValue, page_id: u32, slot_id: u32) -> io::Result<bool> {
        self.delete_keys(std::slice::from_ref(key), page_id, slot_id)
    }

    /// Delete a (possibly composite) key from the B+ Tree.
    pub fn delete_keys(
        &mut self,
        keys: &[DataValue],
        page_id: u32,
        slot_id: u32,
    ) -> io::Result<bool> {
        let encoded = self.encode_key_for(keys)?;
        self.delete_from_tree(self.root_page_id, &encoded, page_id, slot_id)
    }

    /// Internal delete: search and remove a specific (key, page_id, slot_id) entry.
    fn delete_from_tree(
        &mut self,
        page_id: u32,
        encoded_key: &[u8],
        value_page_id: u32,
        value_slot_id: u32,
    ) -> io::Result<bool> {
        let mut leaf_id = self.find_first_leaf(page_id, encoded_key)?;
        // Entries for one key can span several leaves, so walk the chain until
        // the entry is found or a larger key shows the key is exhausted.
        let (mut buf, num_keys, index, key_off, key_len, used) = loop {
            let buf = self.pool.read_page_bytes(leaf_id)?;
            let (num_keys, found, used, past_key, next_leaf) =
                self.locate_leaf_entry(&buf, encoded_key, value_page_id, value_slot_id)?;
            if let Some((index, key_off, key_len)) = found {
                break (buf, num_keys, index, key_off, key_len, used);
            }
            if past_key || next_leaf == 0 {
                return Ok(false); // Key not found
            }
            leaf_id = next_leaf;
        };

        // Close the 8-byte gap left by the value (this also slides the keys
        // before the removed one), then the gap left by the key itself.
        let value_off = LEAF_HEADER + index * VALUE_SIZE;
        buf.copy_within(value_off + VALUE_SIZE..key_off, value_off);
        buf.copy_within(key_off + key_len..used, key_off - VALUE_SIZE);
        let new_used = used - VALUE_SIZE - key_len;
        buf[new_used..used].fill(0);
        buf[4..8].copy_from_slice(&((num_keys - 1) as u32).to_le_bytes());

        self.pool.write_page_bytes(leaf_id, &buf)?;
        Ok(true)
    }

    /// Locate the entry matching both the key and the heap location in one
    /// leaf page. Returns `(num_keys, (index, key offset, key len), end of the
    /// key area, whether a larger key was seen, next leaf)`.
    #[allow(clippy::type_complexity)]
    fn locate_leaf_entry(
        &self,
        buf: &[u8],
        encoded_key: &[u8],
        value_page_id: u32,
        value_slot_id: u32,
    ) -> io::Result<(usize, Option<(usize, usize, usize)>, usize, bool, u32)> {
        {
            let view = NodeView::parse(buf)?;
            let mut found: Option<(usize, usize, usize)> = None; // (index, offset, len)
            let mut past_key = false;
            let mut used = view.keys_start();
            let fast = self.int_fast_path(&view, encoded_key);
            if let Some((width, target)) = fast {
                let entry = KEY_LEN_SIZE + width;
                used = view.keys_start() + view.num_keys() * entry;
                let first = view.int_partition_point(width, |k| k < target);
                for i in first..view.num_keys() {
                    if view.int_key(i, width) != target {
                        past_key = true;
                        break;
                    }
                    if view.value(i) == (value_page_id, value_slot_id) {
                        found = Some((i, view.keys_start() + i * entry, entry));
                        break;
                    }
                }
            }
            for (i, entry) in view.keys().enumerate() {
                if fast.is_some() {
                    break;
                }
                let (off, key) = entry?;
                used = off + key.len();
                if found.is_some() || past_key {
                    continue;
                }
                let cmp = self.cmp_encoded_keys(key, encoded_key)?;
                if cmp == Ordering::Equal && view.value(i) == (value_page_id, value_slot_id) {
                    // Exact match — remove this specific entry
                    found = Some((i, off, key.len()));
                } else if cmp == Ordering::Greater {
                    // Key not in this leaf (past sorted position)
                    past_key = true;
                }
            }
            Ok((view.num_keys(), found, used, past_key, view.next_leaf()))
        }
    }

    // ─── Public Insert API ─────────────────────────────────────────────────

    /// Insert a key-value pair into the B+ Tree.
    ///
    /// The value is a heap tuple identifier `(page_id, slot_id)`.
    /// If the key already exists, the value is overwritten.
    pub fn insert(&mut self, key: &DataValue, page_id: u32, slot_id: u32) -> io::Result<()> {
        self.insert_keys(std::slice::from_ref(key), page_id, slot_id)
    }

    /// Insert a (possibly composite) key-value pair into the B+ Tree.
    pub fn insert_keys(
        &mut self,
        keys: &[DataValue],
        page_id: u32,
        slot_id: u32,
    ) -> io::Result<()> {
        debug_assert_eq!(
            keys.len(),
            self.key_types.len(),
            "key arity must match index arity"
        );
        let encoded = self.encode_key_for(keys)?;
        if encoded.len() > MAX_ENCODED_KEY_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "index key of {} bytes exceeds the maximum of {} bytes",
                    encoded.len(),
                    MAX_ENCODED_KEY_SIZE
                ),
            ));
        }

        // Acquire page write lock for the root
        let file_id = self.file_id;
        let _lock = PageWriteLock::acquire(file_id, self.root_page_id);

        let result = self.insert_internal(self.root_page_id, &encoded, page_id, slot_id)?;

        // Check if the root was split
        if let Some((new_key, new_child)) = result {
            // Create a new internal node as the new root
            let old_root = self.root_page_id;
            let mut new_root_node = BTreeNode::new_internal();
            match &mut new_root_node {
                BTreeNode::Internal {
                    num_keys,
                    keys,
                    children,
                } => {
                    *num_keys = 1;
                    keys.push(new_key);
                    children.push(old_root);
                    children.push(new_child);
                }
                _ => unreachable!(),
            }

            let new_root_id = self.allocate_page(&new_root_node)?;
            self.root_page_id = new_root_id;
            write_root_sidecar(&self.file_path, new_root_id)?;
        }

        Ok(())
    }

    // ─── Internal Tree Operations ──────────────────────────────────────────

    /// Recursive search within the tree starting from the given page.
    fn search_in_tree(
        &mut self,
        page_id: u32,
        encoded_key: &[u8],
    ) -> io::Result<Option<(u32, u32)>> {
        let mut leaf_id = self.find_first_leaf(page_id, encoded_key)?;
        loop {
            let buf = self.pool.read_page_bytes(leaf_id)?;
            let view = NodeView::parse(&buf)?;

            if let Some((width, target)) = self.int_fast_path(&view, encoded_key) {
                let first = view.int_partition_point(width, |k| k < target);
                if first < view.num_keys() {
                    let hit = view.int_key(first, width) == target;
                    return Ok(hit.then(|| view.value(first)));
                }
            } else {
                for (i, entry) in view.keys().enumerate() {
                    let (_, key) = entry?;
                    let cmp = self.cmp_encoded_keys(key, encoded_key)?;
                    if cmp == Ordering::Equal {
                        return Ok(Some(view.value(i)));
                    }
                    if cmp == Ordering::Greater {
                        return Ok(None);
                    }
                }
            }
            // Every key in this leaf is smaller: the match, if any, starts in
            // the next leaf.
            let next_leaf = view.next_leaf();
            if next_leaf == 0 {
                return Ok(None);
            }
            leaf_id = next_leaf;
        }
    }

    /// Find the first leaf that can contain `encoded_key`. Entries equal to
    /// the key may continue into the following leaves.
    fn find_first_leaf(&mut self, page_id: u32, encoded_key: &[u8]) -> io::Result<u32> {
        let mut page_id = page_id;
        loop {
            let buf = self.pool.read_page_bytes(page_id)?;
            let view = NodeView::parse(&buf)?;
            if view.is_leaf() {
                return Ok(page_id);
            }
            page_id = self.pick_child_lower(&view, encoded_key)?;
        }
    }

    /// Convert the first key segment from on-disk encoded bytes to a DataValue.
    #[allow(dead_code)]
    fn key_from_encoded(&self, encoded: &[u8]) -> DataValue {
        let (key, _) = decode_key(encoded, &self.key_types[0]).unwrap_or((DataValue::Int(0), 0));
        key
    }

    /// Internal insert: returns `Ok(None)` if no split, or `Ok(Some((middle_key, new_child_id)))` if split.
    fn insert_internal(
        &mut self,
        page_id: u32,
        encoded_key: &[u8],
        value_page_id: u32,
        value_slot_id: u32,
    ) -> io::Result<Option<(Vec<u8>, u32)>> {
        let buf = self.pool.read_page_bytes(page_id)?;
        let view = NodeView::parse(&buf)?;

        if view.is_leaf() {
            if view.has_space_for(encoded_key.len())? {
                self.insert_into_leaf(page_id, buf, encoded_key, value_page_id, value_slot_id)?;
                Ok(None)
            } else {
                let (new_key, new_leaf_id) =
                    self.split_leaf(page_id, encoded_key, value_page_id, value_slot_id)?;
                Ok(Some((new_key, new_leaf_id)))
            }
        } else {
            // Internal node — find child to descend into
            let (child_idx, child_id) = self.pick_child(&view, encoded_key)?;
            drop(buf);

            // Acquire write lock for the child before modifying
            let file_id = self.file_id;
            let _lock = PageWriteLock::acquire(file_id, child_id);

            // Recursively insert
            let result =
                self.insert_internal(child_id, encoded_key, value_page_id, value_slot_id)?;

            // If child was split, we need to insert the promoted key into this node
            if let Some((promoted_key, new_child_id)) = result {
                // Re-read node to check space with the actual promoted key size
                // (the promoted key may have a different size than encoded_key for varlen types)
                let current_node = self.read_node(page_id)?;
                if current_node.has_space_for(promoted_key.len()) {
                    self.insert_into_internal(page_id, child_idx, &promoted_key, new_child_id)?;
                    Ok(None)
                } else {
                    let (new_key, new_sibling_id) =
                        self.split_internal(page_id, child_idx, &promoted_key, new_child_id)?;
                    Ok(Some((new_key, new_sibling_id)))
                }
            } else {
                Ok(None)
            }
        }
    }

    /// Insert a key-value pair into a leaf node (assumes space is available).
    ///
    /// Supports duplicate keys: if the key already exists, the new entry is
    /// inserted alongside existing entries with the same key, preserving
    /// the sorted order and allowing multiple values per key.
    fn insert_into_leaf(
        &mut self,
        page_id: u32,
        mut buf: Vec<u8>,
        encoded_key: &[u8],
        value_page_id: u32,
        value_slot_id: u32,
    ) -> io::Result<()> {
        // Find the insertion position and the end of the key area.
        // For duplicate keys, insert AFTER all entries with the same key
        // so that the sort order is: key < == == < <
        let (num_keys, insert_pos, key_off, used) = {
            let view = NodeView::parse(&buf)?;
            if !view.is_leaf() {
                unreachable!("insert_into_leaf called on non-leaf");
            }
            let mut insert_at: Option<(usize, usize)> = None; // (index, offset)
            let mut used = view.keys_start();
            let fast = self.int_fast_path(&view, encoded_key);
            if let Some((width, target)) = fast {
                let entry = KEY_LEN_SIZE + width;
                used = view.keys_start() + view.num_keys() * entry;
                let pos = view.int_partition_point(width, |k| k <= target);
                if pos < view.num_keys() {
                    insert_at = Some((pos, view.keys_start() + pos * entry));
                }
            }
            for (i, entry) in view.keys().enumerate() {
                if fast.is_some() {
                    break;
                }
                let (off, key) = entry?;
                used = off + key.len();
                if insert_at.is_none()
                    && self.cmp_encoded_keys(key, encoded_key)? == Ordering::Greater
                {
                    insert_at = Some((i, off));
                }
            }
            let (pos, off) = insert_at.unwrap_or((view.num_keys(), used));
            (view.num_keys(), pos, off, used)
        };

        let key_len = encoded_key.len();
        if used + VALUE_SIZE + key_len > buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "insert_into_leaf: entry does not fit in the leaf page",
            ));
        }

        // Shift bytes right, back to front, to open 8 bytes in the value
        // array and `key_len` bytes in the key area, then fill both gaps.
        let keys_start = LEAF_HEADER + num_keys * VALUE_SIZE;
        let value_off = LEAF_HEADER + insert_pos * VALUE_SIZE;
        buf.copy_within(key_off..used, key_off + VALUE_SIZE + key_len);
        buf.copy_within(keys_start..key_off, keys_start + VALUE_SIZE);
        buf[key_off + VALUE_SIZE..key_off + VALUE_SIZE + key_len].copy_from_slice(encoded_key);
        buf.copy_within(value_off..keys_start, value_off + VALUE_SIZE);
        buf[value_off..value_off + 4].copy_from_slice(&value_page_id.to_le_bytes());
        buf[value_off + 4..value_off + VALUE_SIZE].copy_from_slice(&value_slot_id.to_le_bytes());
        buf[4..8].copy_from_slice(&((num_keys + 1) as u32).to_le_bytes());

        self.pool.write_page_bytes(page_id, &buf)?;
        Ok(())
    }

    /// Insert a key and new child pointer into an internal node (assumes space is available).
    fn insert_into_internal(
        &mut self,
        page_id: u32,
        child_idx: usize,
        promoted_key: &[u8],
        new_child_id: u32,
    ) -> io::Result<()> {
        let mut node = self.read_node(page_id)?;

        match &mut node {
            BTreeNode::Internal {
                num_keys,
                keys,
                children,
                ..
            } => {
                // The promoted key goes at position child_idx in the key array.
                // The new child goes at position child_idx + 1 in the children array.
                keys.insert(child_idx, promoted_key.to_vec());
                children.insert(child_idx + 1, new_child_id);
                *num_keys += 1;
            }
            _ => unreachable!("insert_into_internal called on non-leaf"),
        }

        self.write_node(page_id, &node)?;
        Ok(())
    }

    /// Split a leaf node that is full. Distributes keys between the old node and a new sibling.
    /// Returns the middle key and the new sibling's page ID.
    fn split_leaf(
        &mut self,
        page_id: u32,
        encoded_key: &[u8],
        value_page_id: u32,
        value_slot_id: u32,
    ) -> io::Result<(Vec<u8>, u32)> {
        let node = self.read_node(page_id)?;

        let (old_keys, old_values, old_next_leaf, old_prev_leaf) = match &node {
            BTreeNode::Leaf {
                keys,
                values,
                next_leaf,
                prev_leaf,
                ..
            } => (keys.clone(), values.clone(), *next_leaf, *prev_leaf),
            _ => unreachable!(),
        };

        // Build combined entries
        let mut all_keys = old_keys;
        let mut all_values = old_values;

        let target_encoded = encoded_key.to_vec();
        let mut insert_pos = all_keys.len();
        for (i, key) in all_keys.iter().enumerate() {
            let cmp = self.cmp_encoded_keys(key, encoded_key)?;
            if cmp == Ordering::Greater {
                insert_pos = i;
                break;
            }
        }
        all_keys.insert(insert_pos, target_encoded);
        all_values.insert(insert_pos, (value_page_id, value_slot_id));

        // Split point: balance the halves by bytes, not by entry count, so
        // variable-length keys cannot leave one half larger than a page.
        let split_point = balanced_split_point(&all_keys, VALUE_SIZE);

        // First key in the new node (promoted to parent)
        let promoted_key = all_keys[split_point].clone();

        // Left half stays in the original node
        let left_keys = all_keys[..split_point].to_vec();
        let left_values = all_values[..split_point].to_vec();

        // Right half goes to a new node
        let right_keys = all_keys[split_point..].to_vec();
        let right_values = all_values[split_point..].to_vec();

        // Update the old node (now the left sibling)
        let left_node = BTreeNode::Leaf {
            num_keys: left_keys.len() as u32,
            keys: left_keys,
            values: left_values,
            next_leaf: 0, // Will be set after allocating the new node
            prev_leaf: old_prev_leaf,
        };

        // Allocate the right sibling
        let right_node = BTreeNode::Leaf {
            num_keys: right_keys.len() as u32,
            keys: right_keys,
            values: right_values,
            next_leaf: old_next_leaf,
            prev_leaf: 0, // Set after left node is written
        };

        // Write the new sibling first so we know its page ID
        let new_leaf_id = self.allocate_page(&right_node)?;

        // Update left node's next_leaf to point to the new sibling
        let left_node = match left_node {
            BTreeNode::Leaf {
                num_keys,
                keys,
                values,
                next_leaf: _,
                prev_leaf,
            } => BTreeNode::Leaf {
                num_keys,
                keys,
                values,
                next_leaf: new_leaf_id,
                prev_leaf,
            },
            _ => unreachable!(),
        };
        self.write_node(page_id, &left_node)?;

        // Update the right sibling's prev_leaf
        let right_node = match right_node {
            BTreeNode::Leaf {
                num_keys,
                keys,
                values,
                next_leaf,
                prev_leaf: _,
            } => BTreeNode::Leaf {
                num_keys,
                keys,
                values,
                next_leaf,
                prev_leaf: page_id,
            },
            _ => unreachable!(),
        };
        self.write_node(new_leaf_id, &right_node)?;

        // If there was a next leaf, update its prev_leaf to point to the new sibling
        if old_next_leaf != 0
            && old_next_leaf < self.total_pages
            && let Ok(mut next_node) = self.read_node(old_next_leaf)
        {
            if let BTreeNode::Leaf { prev_leaf, .. } = &mut next_node {
                *prev_leaf = new_leaf_id;
            }
            self.write_node(old_next_leaf, &next_node)?;
        }

        Ok((promoted_key, new_leaf_id))
    }

    /// Split an internal node. Similar to split_leaf but handles children pointers.
    fn split_internal(
        &mut self,
        page_id: u32,
        child_idx: usize,
        promoted_key: &[u8],
        new_child_id: u32,
    ) -> io::Result<(Vec<u8>, u32)> {
        let node = self.read_node(page_id)?;

        let (old_keys, old_children) = match &node {
            BTreeNode::Internal { keys, children, .. } => (keys.clone(), children.clone()),
            _ => unreachable!(),
        };

        // Build combined entries
        let mut all_keys = old_keys;
        let mut all_children = old_children;

        all_keys.insert(child_idx, promoted_key.to_vec());
        all_children.insert(child_idx + 1, new_child_id);

        // Split point
        let split_point = balanced_split_point(&all_keys, CHILD_SIZE);

        // The middle key is promoted to the parent
        let promoted_to_parent = all_keys[split_point].clone();

        // Left half (becomes the existing node)
        let left_keys = all_keys[..split_point].to_vec();
        let left_children = all_children[..=split_point].to_vec();

        // Right half (new sibling node)
        let right_keys = all_keys[split_point + 1..].to_vec();
        let right_children = all_children[split_point + 1..].to_vec();

        // Update the old node
        let left_node = BTreeNode::Internal {
            num_keys: left_keys.len() as u32,
            keys: left_keys,
            children: left_children,
        };
        self.write_node(page_id, &left_node)?;

        // Create the new sibling
        let right_node = BTreeNode::Internal {
            num_keys: right_keys.len() as u32,
            keys: right_keys,
            children: right_children,
        };
        let new_sibling_id = self.allocate_page(&right_node)?;

        Ok((promoted_to_parent, new_sibling_id))
    }

    /// Print the tree structure for debugging.
    #[allow(dead_code)]
    pub fn print_tree(&mut self) -> io::Result<()> {
        println!("=== B+ Tree ===");
        println!("Root page: {}", self.root_page_id);
        println!("Total pages: {}", self.total_pages);
        self.print_node(self.root_page_id, 0)?;
        println!("==============");
        Ok(())
    }

    fn print_node(&mut self, page_id: u32, depth: usize) -> io::Result<()> {
        let indent = "  ".repeat(depth);
        let node = self.read_node(page_id)?;
        match &node {
            BTreeNode::Internal {
                num_keys,
                keys,
                children,
            } => {
                println!(
                    "{}[Internal] page={}, num_keys={}, children={:?}",
                    indent, page_id, num_keys, children
                );
                for (i, key) in keys.iter().enumerate() {
                    let (val, _) =
                        decode_key(key, &self.key_types[0]).unwrap_or((DataValue::Int(0), 0));
                    println!("{}  key[{}] = {}", indent, i, val);
                }
                // Recursively print children
                for &child in children {
                    self.print_node(child, depth + 1)?;
                }
            }
            BTreeNode::Leaf {
                num_keys,
                keys,
                values,
                next_leaf,
                prev_leaf,
            } => {
                println!(
                    "{}[Leaf] page={}, num_keys={}, next={}, prev={}",
                    indent, page_id, num_keys, next_leaf, prev_leaf
                );
                for (i, (key, val)) in keys.iter().zip(values.iter()).enumerate() {
                    let (dv, _) =
                        decode_key(key, &self.key_types[0]).unwrap_or((DataValue::Int(0), 0));
                    println!(
                        "{}  entry[{}]: key={}, tid=({},{})",
                        indent, i, dv, val.0, val.1
                    );
                }
            }
        }
        Ok(())
    }
}

impl Drop for BTree {
    fn drop(&mut self) {
        if let Err(e) = self.sync() {
            log::error!("[BTree::drop] Error syncing on drop: {}", e);
        }
    }
}

/// The value as an `i64` if it is one of the integer types.
fn int_value(value: &DataValue) -> Option<i64> {
    match value {
        DataValue::SmallInt(v) => Some(*v as i64),
        DataValue::Int(v) => Some(*v as i64),
        DataValue::BigInt(v) => Some(*v),
        _ => None,
    }
}

// ─── Helper function to decode a key from a leaf entry ───────────────────────

/// Decode a DataValue from an encoded key byte slice.
pub fn decode_btree_key(encoded: &[u8], ty: &DataType) -> Result<(DataValue, usize), String> {
    decode_key(encoded, ty).map_err(|e| e.to_string())
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_index_path() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("rookdb_btree_test_{}.idx", nanos))
    }

    fn setup_btree() -> (PathBuf, BTree) {
        let path = unique_index_path();
        let btree = BTree::create(path.clone(), DataType::Int).unwrap();
        (path, btree)
    }

    fn cleanup(path: &PathBuf) {
        fs::remove_file(path).ok();
    }

    #[test]
    fn test_create_and_open() {
        let path = unique_index_path();
        let mut btree = BTree::create(path.clone(), DataType::Int).unwrap();
        assert_eq!(btree.total_pages(), 1);
        assert_eq!(btree.root_page_id, 0);

        // Search on empty tree
        let result = btree.search(&DataValue::Int(42)).unwrap();
        assert_eq!(result, None);

        cleanup(&path);
    }

    #[test]
    fn test_insert_and_search_single() {
        let (path, mut btree) = setup_btree();

        btree.insert(&DataValue::Int(42), 1, 5).unwrap();
        let result = btree.search(&DataValue::Int(42)).unwrap();
        assert_eq!(result, Some((1, 5)));

        // Non-existent key
        let result = btree.search(&DataValue::Int(999)).unwrap();
        assert_eq!(result, None);

        cleanup(&path);
    }

    #[test]
    fn test_insert_multiple_and_search() {
        let (path, mut btree) = setup_btree();

        btree.insert(&DataValue::Int(10), 1, 0).unwrap();
        btree.insert(&DataValue::Int(20), 2, 1).unwrap();
        btree.insert(&DataValue::Int(30), 3, 2).unwrap();

        assert_eq!(btree.search(&DataValue::Int(10)).unwrap(), Some((1, 0)));
        assert_eq!(btree.search(&DataValue::Int(20)).unwrap(), Some((2, 1)));
        assert_eq!(btree.search(&DataValue::Int(30)).unwrap(), Some((3, 2)));

        cleanup(&path);
    }

    #[test]
    fn test_insert_duplicate_keys() {
        let (path, mut btree) = setup_btree();

        // Insert two entries with the same key but different values
        btree.insert(&DataValue::Int(42), 1, 5).unwrap();
        btree.insert(&DataValue::Int(42), 2, 10).unwrap();

        // search() returns the FIRST entry with this key
        let result = btree.search(&DataValue::Int(42)).unwrap();
        assert_eq!(result, Some((1, 5))); // First entry preserved

        // search_all() returns ALL entries with this key
        let all = btree.search_all(&DataValue::Int(42)).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0], (1, 5));
        assert_eq!(all[1], (2, 10));

        cleanup(&path);
    }

    #[test]
    fn test_delete_specific_duplicate_entry() {
        let (path, mut btree) = setup_btree();

        // Insert three entries: two with key=42, one with key=99
        btree.insert(&DataValue::Int(42), 1, 5).unwrap();
        btree.insert(&DataValue::Int(99), 3, 0).unwrap();
        btree.insert(&DataValue::Int(42), 2, 10).unwrap();

        // Verify three total entries
        let all_42 = btree.search_all(&DataValue::Int(42)).unwrap();
        assert_eq!(all_42.len(), 2);

        // Delete one specific entry (42, page=1, slot=5)
        let deleted = btree.delete(&DataValue::Int(42), 1, 5).unwrap();
        assert!(deleted);

        // Verify only the other duplicate remains
        let all_42 = btree.search_all(&DataValue::Int(42)).unwrap();
        assert_eq!(all_42.len(), 1);
        assert_eq!(all_42[0], (2, 10));

        // Key 99 should be untouched
        assert_eq!(btree.search(&DataValue::Int(99)).unwrap(), Some((3, 0)));

        cleanup(&path);
    }

    #[test]
    fn test_delete_nonexistent_entry() {
        let (path, mut btree) = setup_btree();
        btree.insert(&DataValue::Int(42), 1, 5).unwrap();

        // Wrong page_id should not match
        let deleted = btree.delete(&DataValue::Int(42), 999, 0).unwrap();
        assert!(!deleted);

        // Original entry should be untouched
        assert_eq!(btree.search(&DataValue::Int(42)).unwrap(), Some((1, 5)));

        cleanup(&path);
    }

    #[test]
    fn test_contains() {
        let (path, mut btree) = setup_btree();

        btree.insert(&DataValue::Int(100), 1, 0).unwrap();
        assert!(btree.contains(&DataValue::Int(100)).unwrap());
        assert!(!btree.contains(&DataValue::Int(200)).unwrap());

        cleanup(&path);
    }

    #[test]
    fn test_range_search() {
        let (path, mut btree) = setup_btree();

        for i in 0..20 {
            btree.insert(&DataValue::Int(i * 10), i as u32, 0).unwrap();
        }

        // Search range [50, 150]
        let results = btree
            .search_range(&DataValue::Int(50), &DataValue::Int(150))
            .unwrap();
        // Expect: 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150
        // Our data: 0, 10, 20, ..., 190
        // In range: 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150
        assert_eq!(results.len(), 11);
        assert_eq!(results[0], (5, 0)); // key=50
        assert_eq!(results[10], (15, 0)); // key=150

        cleanup(&path);
    }

    #[test]
    fn test_many_inserts_trigger_split() {
        let (path, mut btree) = setup_btree();

        // Insert enough keys to trigger splits
        // With 8KB pages and 4-byte INT keys (2+4 bytes each), we can fit ~800 keys per leaf
        // Insert 2000 keys to force multiple splits and a multi-level tree
        for i in 0..2000u32 {
            btree.insert(&DataValue::Int(i as i32), i, 0).unwrap();
        }

        // Verify all keys are searchable
        for i in 0..2000u32 {
            let result = btree.search(&DataValue::Int(i as i32)).unwrap();
            assert!(result.is_some(), "Key {} should exist", i);
            assert_eq!(result.unwrap().0, i);
        }

        // Verify tree structure (should have multiple levels now)
        assert!(
            btree.total_pages > 1,
            "Tree should have multiple pages after 2000 inserts"
        );

        cleanup(&path);
    }

    #[test]
    fn test_insert_out_of_order() {
        let (path, mut btree) = setup_btree();

        // Insert keys in reverse order
        for i in (0..100u32).rev() {
            btree.insert(&DataValue::Int(i as i32), i, 0).unwrap();
        }

        // Verify all keys are present
        for i in 0..100u32 {
            let result = btree.search(&DataValue::Int(i as i32)).unwrap();
            assert!(result.is_some(), "Key {} should exist", i);
            assert_eq!(result.unwrap().0, i);
        }

        cleanup(&path);
    }

    #[test]
    fn test_persistence() {
        let path = unique_index_path();
        let key_type = DataType::Int;

        {
            let mut btree = BTree::create(path.clone(), key_type.clone()).unwrap();
            for i in 0u32..50 {
                btree.insert(&DataValue::Int(i as i32), i, 0).unwrap();
            }
            // Explicitly sync
            btree.sync().unwrap();
        }

        // Re-open the file
        {
            let mut btree = BTree::open(path.clone()).unwrap();
            btree.set_key_type(key_type.clone());

            // Verify keys survived
            for i in 0u32..50 {
                let result = btree.search(&DataValue::Int(i as i32)).unwrap();
                assert!(result.is_some(), "Key {} should survive reopen", i);
                assert_eq!(result.unwrap().0, i);
            }
        }

        cleanup(&path);
    }

    #[test]
    fn test_string_keys() {
        let path = unique_index_path();
        let key_type = DataType::Varchar(100);

        let mut btree = BTree::create(path.clone(), key_type).unwrap();

        btree
            .insert(&DataValue::Varchar("apple".to_string()), 1, 0)
            .unwrap();
        btree
            .insert(&DataValue::Varchar("banana".to_string()), 2, 1)
            .unwrap();
        btree
            .insert(&DataValue::Varchar("cherry".to_string()), 3, 2)
            .unwrap();

        let result = btree
            .search(&DataValue::Varchar("banana".to_string()))
            .unwrap();
        assert_eq!(result, Some((2, 1)));

        let result = btree
            .search(&DataValue::Varchar("grape".to_string()))
            .unwrap();
        assert_eq!(result, None);

        cleanup(&path);
    }

    #[test]
    fn test_range_search_strings() {
        let path = unique_index_path();
        let key_type = DataType::Varchar(100);

        let mut btree = BTree::create(path.clone(), key_type).unwrap();

        let fruits = [
            "apple",
            "banana",
            "cherry",
            "date",
            "elderberry",
            "fig",
            "grape",
        ];
        for (i, fruit) in fruits.iter().enumerate() {
            btree
                .insert(&DataValue::Varchar(fruit.to_string()), i as u32, 0)
                .unwrap();
        }

        let results = btree
            .search_range(
                &DataValue::Varchar("banana".to_string()),
                &DataValue::Varchar("fig".to_string()),
            )
            .unwrap();

        // Range [banana, fig] inclusive → 5 items
        assert_eq!(results.len(), 5); // banana, cherry, date, elderberry, fig
        assert_eq!(results[0], (1, 0)); // banana
        assert_eq!(results[4], (5, 0)); // fig

        cleanup(&path);
    }

    #[test]
    fn test_large_string_keys() {
        let path = unique_index_path();
        let key_type = DataType::Varchar(1000);

        let mut btree = BTree::create(path.clone(), key_type).unwrap();

        // Insert keys that are long enough to cause splits with fewer entries
        for i in 0..100u32 {
            let s = format!(
                "key_{}_with_padding_{}_done",
                i,
                "x".repeat(i as usize % 50)
            );
            btree.insert(&DataValue::Varchar(s.clone()), i, 0).unwrap();
        }

        for i in 0..100u32 {
            let s = format!(
                "key_{}_with_padding_{}_done",
                i,
                "x".repeat(i as usize % 50)
            );
            let result = btree.search(&DataValue::Varchar(s)).unwrap();
            assert!(result.is_some(), "Key {} should exist", i);
        }

        cleanup(&path);
    }

    #[test]
    fn test_scan_all_empty() {
        let (path, mut btree) = setup_btree();
        let results = btree.scan_all().unwrap();
        assert!(results.is_empty());
        cleanup(&path);
    }

    #[test]
    fn test_scan_all_single() {
        let (path, mut btree) = setup_btree();
        btree.insert(&DataValue::Int(42), 1, 5).unwrap();
        let results = btree.scan_all().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (1, 5));
        cleanup(&path);
    }

    #[test]
    fn test_scan_all_multiple() {
        let (path, mut btree) = setup_btree();
        let mut expected = Vec::new();
        for i in 0u32..100 {
            btree.insert(&DataValue::Int(i as i32), i, i * 10).unwrap();
            expected.push((i, i * 10));
        }
        let results = btree.scan_all().unwrap();
        assert_eq!(results.len(), 100);
        // Results should be in key order
        for (i, &(page_id, slot_id)) in results.iter().enumerate() {
            assert_eq!(page_id, i as u32, "page_id mismatch at index {}", i);
            assert_eq!(slot_id, (i as u32) * 10, "slot_id mismatch at index {}", i);
        }
        cleanup(&path);
    }

    /// Every page must be exactly what decode → encode would produce, and a
    /// full scan must match a sorted model. Leaf inserts and deletes edit the
    /// page bytes directly, so this is what guards those edits.
    fn check_against_model(btree: &mut BTree, model: &[(DataValue, (u32, u32))]) {
        let expected: Vec<(u32, u32)> = model.iter().map(|(_, rid)| *rid).collect();
        assert_eq!(btree.scan_all().unwrap(), expected);
        for page_id in 0..btree.total_pages() {
            let bytes = btree.pool.read_page_bytes(page_id).unwrap();
            let round_trip = BTreeNode::deserialize(&bytes).unwrap().serialize();
            assert!(bytes == round_trip, "page {} is not canonical", page_id);
        }
    }

    fn lcg(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *state >> 33
    }

    fn run_random_inserts_and_deletes(key_type: DataType, make_key: impl Fn(u32) -> DataValue) {
        let path = std::env::temp_dir().join(format!(
            "rookdb_btree_raw_{}_{:?}_{}.idx",
            std::process::id(),
            std::thread::current().id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let mut btree = BTree::create(path.clone(), key_type).unwrap();

        // Unique keys in a shuffled order; the model stays sorted by key number.
        let mut order: Vec<u32> = (0..3000).collect();
        let mut seed = 0x5EED_u64;
        for i in (1..order.len()).rev() {
            order.swap(i, (lcg(&mut seed) as usize) % (i + 1));
        }

        let mut model: Vec<(u32, DataValue, (u32, u32))> = Vec::new();
        let as_pairs = |m: &[(u32, DataValue, (u32, u32))]| -> Vec<(DataValue, (u32, u32))> {
            m.iter().map(|(_, k, rid)| (k.clone(), *rid)).collect()
        };

        for (step, &n) in order.iter().enumerate() {
            let rid = (n + 1, n * 3);
            btree.insert(&make_key(n), rid.0, rid.1).unwrap();
            let pos = model.partition_point(|(m, _, _)| *m < n);
            model.insert(pos, (n, make_key(n), rid));
            if step % 500 == 499 {
                check_against_model(&mut btree, &as_pairs(&model));
            }
        }
        check_against_model(&mut btree, &as_pairs(&model));
        assert!(btree.total_pages() > 1, "test must exercise splits");

        // Delete two thirds, interleaved with re-inserts of some of them.
        for (step, &n) in order.iter().enumerate() {
            if n % 3 == 0 {
                continue;
            }
            let pos = model.partition_point(|(m, _, _)| *m < n);
            let (_, key, rid) = model.remove(pos);
            assert!(btree.delete(&key, rid.0, rid.1).unwrap());
            // Wrong heap location or absent key: nothing is removed.
            assert!(!btree.delete(&key, rid.0, rid.1).unwrap());
            if n % 7 == 0 {
                btree.insert(&key, rid.0, rid.1).unwrap();
                model.insert(pos, (n, key, rid));
            }
            if step % 500 == 499 {
                check_against_model(&mut btree, &as_pairs(&model));
            }
        }
        check_against_model(&mut btree, &as_pairs(&model));

        for (n, key, rid) in model.iter().step_by(37) {
            assert_eq!(btree.search(key).unwrap(), Some(*rid), "key {}", n);
            assert_eq!(btree.search_range(key, key).unwrap(), vec![*rid]);
            assert_eq!(btree.search_all(key).unwrap(), vec![*rid]);
        }
        // Keys that were deleted are no longer found.
        for &n in order
            .iter()
            .filter(|n| **n % 3 != 0 && **n % 7 != 0)
            .step_by(41)
        {
            assert_eq!(btree.search(&make_key(n)).unwrap(), None, "key {}", n);
        }
        // Ranges spanning several leaves, including bounds that are absent.
        for (lo, hi) in [(0usize, model.len() - 1), (10, 700), (333, 334), (900, 900)] {
            let expected: Vec<(u32, u32)> = model[lo..=hi].iter().map(|(_, _, r)| *r).collect();
            assert_eq!(
                btree.search_range(&model[lo].1, &model[hi].1).unwrap(),
                expected,
                "range {}..={}",
                lo,
                hi
            );
        }
        cleanup(&path);
    }

    #[test]
    fn test_raw_leaf_edits_fixed_width_keys() {
        run_random_inserts_and_deletes(DataType::Int, |n| DataValue::Int(n as i32 - 1500));
    }

    #[test]
    fn test_raw_leaf_edits_smallint_and_bigint_keys() {
        run_random_inserts_and_deletes(DataType::SmallInt, |n| {
            DataValue::SmallInt(n as i16 - 1500)
        });
        run_random_inserts_and_deletes(DataType::BigInt, |n| {
            DataValue::BigInt((n as i64 - 1500) * 5_000_000_000)
        });
    }

    #[test]
    fn test_integer_range_bounds_of_another_integer_type() {
        let (path, mut btree) = setup_btree();
        for i in -50i32..50 {
            btree
                .insert(&DataValue::Int(i), 1, (i + 50) as u32)
                .unwrap();
        }
        // Bounds are widened before comparing, as DataValue::compare does.
        let hits = btree
            .search_range(&DataValue::SmallInt(-3), &DataValue::BigInt(2))
            .unwrap();
        assert_eq!(hits, (47u32..=52).map(|slot| (1, slot)).collect::<Vec<_>>());
        // A bound outside the key type's range is still handled.
        let all = btree
            .search_range(&DataValue::BigInt(i64::MIN), &DataValue::BigInt(i64::MAX))
            .unwrap();
        assert_eq!(all.len(), 100);
        // An inverted range is empty.
        assert!(
            btree
                .search_range(&DataValue::Int(10), &DataValue::Int(-10))
                .unwrap()
                .is_empty()
        );
        cleanup(&path);
    }

    #[test]
    fn test_raw_leaf_edits_variable_width_keys() {
        run_random_inserts_and_deletes(DataType::Varchar(40), |n| {
            DataValue::Varchar(format!("k{:05}{}", n, "x".repeat((n % 13) as usize)))
        });
    }

    #[test]
    fn test_raw_leaf_edits_duplicate_keys() {
        let (path, mut btree) = setup_btree();
        let mut model: Vec<(DataValue, (u32, u32))> = Vec::new();
        // Duplicates go after existing equal keys, so insertion order is kept.
        for (key, slot) in [(5, 0), (3, 1), (5, 2), (9, 3), (5, 4), (3, 5)] {
            btree.insert(&DataValue::Int(key), 1, slot).unwrap();
            let pos = model.partition_point(|(k, _)| match k {
                DataValue::Int(existing) => *existing <= key,
                _ => unreachable!(),
            });
            model.insert(pos, (DataValue::Int(key), (1, slot)));
        }
        check_against_model(&mut btree, &model);
        assert_eq!(
            btree.search_all(&DataValue::Int(5)).unwrap(),
            vec![(1, 0), (1, 2), (1, 4)]
        );

        // Deleting one duplicate removes exactly the entry with that location.
        assert!(btree.delete(&DataValue::Int(5), 1, 2).unwrap());
        model.retain(|(_, rid)| *rid != (1, 2));
        check_against_model(&mut btree, &model);
        assert_eq!(
            btree.search_all(&DataValue::Int(5)).unwrap(),
            vec![(1, 0), (1, 4)]
        );
        cleanup(&path);
    }
}
