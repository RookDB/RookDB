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

use std::fs::{self, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};

use serde::{Deserialize, Serialize};

use crate::index::config::BTREE_MIN_DEGREE;
use crate::index::index_trait::{IndexKey, IndexTrait, RecordId, TreeBasedIndex};
use crate::index::paged_store;
use crate::page::PAGE_SIZE;

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

const BPLUS_DISK_MAGIC: [u8; 8] = *b"RDBIDXV1";
const BPLUS_DISK_NODE_FORMAT_VERSION: u16 = 2;
const BPLUS_DISK_HEADER_SIZE: usize = 64;
const BPLUS_NODE_PAGE_HEADER_SIZE: usize = 16;

#[derive(Debug, Clone)]
struct DiskBPlusHeader {
    root_page: u32,
    node_page_count: u32,
    entry_count: u64,
    t: u32,
}

#[derive(Debug, Clone)]
struct DiskBPlusNode {
    is_leaf: bool,
    dead: bool,
    keys: Vec<IndexKey>,
    values: Vec<Vec<RecordId>>,
    children_pages: Vec<u32>,
    next_leaf_page: Option<u32>,
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

    fn io_invalid_data(msg: impl Into<String>) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, msg.into())
    }

    fn read_header_page(file: &mut std::fs::File) -> io::Result<Vec<u8>> {
        file.seek(SeekFrom::Start(0))?;
        let mut page = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut page)?;
        Ok(page)
    }

    fn parse_disk_version(header_page: &[u8]) -> io::Result<u16> {
        if header_page.len() != PAGE_SIZE {
            return Err(Self::io_invalid_data("invalid B+Tree header page length"));
        }
        if header_page[0..8] != BPLUS_DISK_MAGIC {
            return Err(Self::io_invalid_data("invalid B+Tree index file magic"));
        }

        let header_size = u16::from_le_bytes([header_page[10], header_page[11]]) as usize;
        if header_size > PAGE_SIZE {
            return Err(Self::io_invalid_data("invalid B+Tree index header size"));
        }

        let stored_page_size = u32::from_le_bytes([
            header_page[12],
            header_page[13],
            header_page[14],
            header_page[15],
        ]) as usize;
        if stored_page_size != PAGE_SIZE {
            return Err(Self::io_invalid_data(format!(
                "B+Tree page size mismatch: expected {}, found {}",
                PAGE_SIZE, stored_page_size
            )));
        }

        Ok(u16::from_le_bytes([header_page[8], header_page[9]]))
    }

    fn parse_node_format_header(header_page: &[u8]) -> io::Result<DiskBPlusHeader> {
        let root_page = u32::from_le_bytes([
            header_page[16],
            header_page[17],
            header_page[18],
            header_page[19],
        ]);
        let node_page_count = u32::from_le_bytes([
            header_page[20],
            header_page[21],
            header_page[22],
            header_page[23],
        ]);
        let entry_count = u64::from_le_bytes(
            header_page[24..32]
                .try_into()
                .map_err(|_| Self::io_invalid_data("failed to decode B+Tree entry_count"))?,
        );
        let t = u32::from_le_bytes([
            header_page[32],
            header_page[33],
            header_page[34],
            header_page[35],
        ]);

        if node_page_count == 0 && root_page != 0 {
            return Err(Self::io_invalid_data(
                "B+Tree root page must be 0 when node page count is 0",
            ));
        }

        Ok(DiskBPlusHeader {
            root_page,
            node_page_count,
            entry_count,
            t,
        })
    }

    fn page_to_node_index(page_no: u32, node_page_count: usize, field: &str) -> io::Result<usize> {
        if page_no == 0 {
            return Err(Self::io_invalid_data(format!(
                "{} cannot reference header page",
                field
            )));
        }

        let idx = usize::try_from(page_no - 1)
            .map_err(|_| Self::io_invalid_data(format!("{} conversion overflow", field)))?;
        if idx >= node_page_count {
            return Err(Self::io_invalid_data(format!(
                "{} points out of bounds: page {} (node pages={})",
                field, page_no, node_page_count
            )));
        }
        Ok(idx)
    }

    fn encode_key(key: &IndexKey, out: &mut Vec<u8>) -> io::Result<()> {
        match key {
            IndexKey::Int(v) => {
                out.push(0);
                out.extend_from_slice(&v.to_le_bytes());
            }
            IndexKey::Float(v) => {
                out.push(1);
                out.extend_from_slice(&v.to_bits().to_le_bytes());
            }
            IndexKey::Text(text) => {
                out.push(2);
                let bytes = text.as_bytes();
                let len_u32 = u32::try_from(bytes.len())
                    .map_err(|_| Self::io_invalid_data("B+Tree text key length overflow"))?;
                out.extend_from_slice(&len_u32.to_le_bytes());
                out.extend_from_slice(bytes);
            }
        }
        Ok(())
    }

    fn read_u32_from_payload(
        page: &[u8],
        cursor: &mut usize,
        end: usize,
        field: &str,
    ) -> io::Result<u32> {
        if *cursor + 4 > end {
            return Err(Self::io_invalid_data(format!(
                "truncated B+Tree payload while reading {}",
                field
            )));
        }

        let value = u32::from_le_bytes(
            page[*cursor..*cursor + 4]
                .try_into()
                .map_err(|_| Self::io_invalid_data(format!("failed to decode {}", field)))?,
        );
        *cursor += 4;
        Ok(value)
    }

    fn decode_key(page: &[u8], cursor: &mut usize, end: usize) -> io::Result<IndexKey> {
        if *cursor >= end {
            return Err(Self::io_invalid_data("truncated B+Tree key tag"));
        }

        let tag = page[*cursor];
        *cursor += 1;

        match tag {
            0 => {
                if *cursor + 8 > end {
                    return Err(Self::io_invalid_data("truncated B+Tree INT key"));
                }
                let raw: [u8; 8] = page[*cursor..*cursor + 8]
                    .try_into()
                    .map_err(|_| Self::io_invalid_data("failed to decode B+Tree INT key"))?;
                *cursor += 8;
                Ok(IndexKey::Int(i64::from_le_bytes(raw)))
            }
            1 => {
                if *cursor + 8 > end {
                    return Err(Self::io_invalid_data("truncated B+Tree FLOAT key"));
                }
                let raw: [u8; 8] = page[*cursor..*cursor + 8]
                    .try_into()
                    .map_err(|_| Self::io_invalid_data("failed to decode B+Tree FLOAT key"))?;
                *cursor += 8;
                Ok(IndexKey::Float(f64::from_bits(u64::from_le_bytes(raw))))
            }
            2 => {
                let text_len = Self::read_u32_from_payload(page, cursor, end, "B+Tree TEXT length")?
                    as usize;
                if *cursor + text_len > end {
                    return Err(Self::io_invalid_data("truncated B+Tree TEXT key bytes"));
                }
                let text = String::from_utf8(page[*cursor..*cursor + text_len].to_vec())
                    .map_err(|_| Self::io_invalid_data("B+Tree TEXT key contains invalid UTF-8"))?;
                *cursor += text_len;
                Ok(IndexKey::Text(text))
            }
            _ => Err(Self::io_invalid_data(format!(
                "unsupported B+Tree key tag {}",
                tag
            ))),
        }
    }

    fn serialize_node_page(&self, node_idx: usize) -> io::Result<Vec<u8>> {
        let node = self
            .nodes
            .get(node_idx)
            .ok_or_else(|| Self::io_invalid_data("B+Tree node index out of bounds during save"))?;

        if node.is_leaf && !node.children.is_empty() {
            return Err(Self::io_invalid_data("B+Tree leaf node cannot have children"));
        }
        if node.is_leaf && node.values.len() != node.keys.len() {
            return Err(Self::io_invalid_data(
                "B+Tree leaf values length must match keys length",
            ));
        }

        let key_count = u16::try_from(node.keys.len())
            .map_err(|_| Self::io_invalid_data("B+Tree key count overflow"))?;
        let child_count = if node.is_leaf {
            0u16
        } else {
            u16::try_from(node.children.len())
                .map_err(|_| Self::io_invalid_data("B+Tree child count overflow"))?
        };

        let next_leaf_page = if node.is_leaf {
            match node.next_leaf {
                Some(next_idx) => u32::try_from(next_idx + 1)
                    .map_err(|_| Self::io_invalid_data("B+Tree next_leaf page overflow"))?,
                None => 0,
            }
        } else {
            0
        };

        let mut payload = Vec::new();
        for key in &node.keys {
            Self::encode_key(key, &mut payload)?;
        }

        if node.is_leaf {
            for rids in &node.values {
                let rid_count = u32::try_from(rids.len())
                    .map_err(|_| Self::io_invalid_data("B+Tree RID count overflow"))?;
                payload.extend_from_slice(&rid_count.to_le_bytes());
                for rid in rids {
                    payload.extend_from_slice(&rid.page_no.to_le_bytes());
                    payload.extend_from_slice(&rid.item_id.to_le_bytes());
                }
            }
        } else {
            for child_idx in &node.children {
                let child_page = u32::try_from(child_idx + 1)
                    .map_err(|_| Self::io_invalid_data("B+Tree child page overflow"))?;
                payload.extend_from_slice(&child_page.to_le_bytes());
            }
        }

        if payload.len() > PAGE_SIZE - BPLUS_NODE_PAGE_HEADER_SIZE {
            return Err(Self::io_invalid_data(format!(
                "B+Tree node {} payload is too large for a single page ({})",
                node_idx,
                payload.len()
            )));
        }

        let payload_used = u32::try_from(payload.len())
            .map_err(|_| Self::io_invalid_data("B+Tree payload length overflow"))?;

        let mut page = vec![0u8; PAGE_SIZE];
        page[0] = u8::from(node.is_leaf);
        page[1] = u8::from(node.dead);
        page[2..4].copy_from_slice(&key_count.to_le_bytes());
        page[4..6].copy_from_slice(&child_count.to_le_bytes());
        page[6..8].copy_from_slice(&0u16.to_le_bytes());
        page[8..12].copy_from_slice(&next_leaf_page.to_le_bytes());
        page[12..16].copy_from_slice(&payload_used.to_le_bytes());
        page[BPLUS_NODE_PAGE_HEADER_SIZE..BPLUS_NODE_PAGE_HEADER_SIZE + payload.len()]
            .copy_from_slice(&payload);

        Ok(page)
    }

    fn parse_node_page(page: &[u8]) -> io::Result<DiskBPlusNode> {
        if page.len() != PAGE_SIZE {
            return Err(Self::io_invalid_data("invalid B+Tree node page length"));
        }

        let is_leaf = page[0] != 0;
        let dead = page[1] != 0;
        let key_count = u16::from_le_bytes([page[2], page[3]]) as usize;
        let child_count = u16::from_le_bytes([page[4], page[5]]) as usize;
        let next_leaf_raw = u32::from_le_bytes([page[8], page[9], page[10], page[11]]);
        let payload_used = u32::from_le_bytes([page[12], page[13], page[14], page[15]]) as usize;

        if payload_used > PAGE_SIZE - BPLUS_NODE_PAGE_HEADER_SIZE {
            return Err(Self::io_invalid_data(
                "B+Tree node payload exceeds page capacity",
            ));
        }

        let mut cursor = BPLUS_NODE_PAGE_HEADER_SIZE;
        let payload_end = BPLUS_NODE_PAGE_HEADER_SIZE + payload_used;

        let mut keys = Vec::with_capacity(key_count);
        for _ in 0..key_count {
            keys.push(Self::decode_key(page, &mut cursor, payload_end)?);
        }

        let mut values = Vec::new();
        let mut children_pages = Vec::new();

        if is_leaf {
            values.reserve(key_count);
            for _ in 0..key_count {
                let rid_count = Self::read_u32_from_payload(page, &mut cursor, payload_end, "RID count")?
                    as usize;
                let mut rids = Vec::with_capacity(rid_count);
                for _ in 0..rid_count {
                    let page_no =
                        Self::read_u32_from_payload(page, &mut cursor, payload_end, "RID page_no")?;
                    let item_id =
                        Self::read_u32_from_payload(page, &mut cursor, payload_end, "RID item_id")?;
                    rids.push(RecordId::new(page_no, item_id));
                }
                values.push(rids);
            }
        } else {
            children_pages.reserve(child_count);
            for _ in 0..child_count {
                children_pages.push(Self::read_u32_from_payload(
                    page,
                    &mut cursor,
                    payload_end,
                    "child page",
                )?);
            }
        }

        if cursor != payload_end {
            return Err(Self::io_invalid_data(
                "B+Tree node payload parsing left trailing bytes",
            ));
        }

        Ok(DiskBPlusNode {
            is_leaf,
            dead,
            keys,
            values,
            children_pages,
            next_leaf_page: if is_leaf && next_leaf_raw != 0 {
                Some(next_leaf_raw)
            } else {
                None
            },
        })
    }

    fn read_node_page_from_file(file: &mut std::fs::File, page_no: u32) -> io::Result<DiskBPlusNode> {
        if page_no == 0 {
            return Err(Self::io_invalid_data("node page 0 is reserved for header"));
        }
        let offset = page_no as u64 * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset))?;
        let mut page = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut page)?;
        Self::parse_node_page(&page)
    }

    fn load_v1_entry_format(path: &str) -> io::Result<Self> {
        let mut index = Self::with_defaults();
        paged_store::load_entries_stream(path, |key, rid| index.insert(key, rid))?;
        Ok(index)
    }

    fn load_node_format(path: &str, header: &DiskBPlusHeader) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;

        let node_page_count = usize::try_from(header.node_page_count)
            .map_err(|_| Self::io_invalid_data("B+Tree node page count conversion overflow"))?;

        if node_page_count == 0 {
            return Ok(Self::with_defaults());
        }

        let root = Self::page_to_node_index(header.root_page, node_page_count, "root_page")?;

        let mut disk_nodes = Vec::with_capacity(node_page_count);
        for page_no in 1..=header.node_page_count {
            disk_nodes.push(Self::read_node_page_from_file(&mut file, page_no)?);
        }

        let mut nodes = Vec::with_capacity(node_page_count);
        for disk_node in &disk_nodes {
            let mut children = Vec::with_capacity(disk_node.children_pages.len());
            for &child_page in &disk_node.children_pages {
                children.push(Self::page_to_node_index(
                    child_page,
                    node_page_count,
                    "child pointer",
                )?);
            }

            let next_leaf = match disk_node.next_leaf_page {
                Some(next_page) => Some(Self::page_to_node_index(
                    next_page,
                    node_page_count,
                    "next_leaf pointer",
                )?),
                None => None,
            };

            nodes.push(BPlusNode {
                keys: disk_node.keys.clone(),
                values: disk_node.values.clone(),
                children,
                next_leaf,
                is_leaf: disk_node.is_leaf,
                dead: disk_node.dead,
            });
        }

        let t = usize::try_from(header.t)
            .map_err(|_| Self::io_invalid_data("B+Tree minimum degree conversion overflow"))?;
        if t < 2 {
            return Err(Self::io_invalid_data(
                "B+Tree minimum degree must be >= 2 in persisted header",
            ));
        }

        let entry_count = usize::try_from(header.entry_count)
            .map_err(|_| Self::io_invalid_data("B+Tree entry_count conversion overflow"))?;

        Ok(Self {
            nodes,
            root,
            t,
            entry_count,
        })
    }

    fn save_node_format(&self, path: &str) -> io::Result<()> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)?;

        file.write_all(&vec![0u8; PAGE_SIZE])?;

        for node_idx in 0..self.nodes.len() {
            let page = self.serialize_node_page(node_idx)?;
            file.write_all(&page)?;
        }

        let root_page = u32::try_from(self.root + 1)
            .map_err(|_| Self::io_invalid_data("B+Tree root page conversion overflow"))?;
        let node_page_count = u32::try_from(self.nodes.len())
            .map_err(|_| Self::io_invalid_data("B+Tree node page count overflow"))?;
        let entry_count = u64::try_from(self.entry_count)
            .map_err(|_| Self::io_invalid_data("B+Tree entry_count conversion overflow"))?;
        let t_u32 = u32::try_from(self.t)
            .map_err(|_| Self::io_invalid_data("B+Tree minimum degree conversion overflow"))?;

        let mut header = vec![0u8; PAGE_SIZE];
        header[0..8].copy_from_slice(&BPLUS_DISK_MAGIC);
        header[8..10].copy_from_slice(&BPLUS_DISK_NODE_FORMAT_VERSION.to_le_bytes());
        header[10..12].copy_from_slice(&(BPLUS_DISK_HEADER_SIZE as u16).to_le_bytes());
        header[12..16].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        header[16..20].copy_from_slice(&root_page.to_le_bytes());
        header[20..24].copy_from_slice(&node_page_count.to_le_bytes());
        header[24..32].copy_from_slice(&entry_count.to_le_bytes());
        header[32..36].copy_from_slice(&t_u32.to_le_bytes());

        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header)?;
        file.flush()?;

        Ok(())
    }

    /// Point lookup directly from disk using root-to-leaf traversal.
    ///
    /// This avoids loading the entire B+Tree into memory for a single key.
    pub fn search_on_disk(path: &str, key: &IndexKey) -> io::Result<Vec<RecordId>> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let header_page = Self::read_header_page(&mut file)?;
        let version = Self::parse_disk_version(&header_page)?;

        if version == 1 {
            // Backward compatibility with entry-stream format.
            let loaded = Self::load_v1_entry_format(path)?;
            return loaded.search(key);
        }

        if version != BPLUS_DISK_NODE_FORMAT_VERSION {
            return Err(Self::io_invalid_data(format!(
                "unsupported B+Tree on-disk version {}",
                version
            )));
        }

        let header = Self::parse_node_format_header(&header_page)?;
        if header.node_page_count == 0 || header.root_page == 0 {
            return Ok(Vec::new());
        }

        let node_page_count = usize::try_from(header.node_page_count)
            .map_err(|_| Self::io_invalid_data("B+Tree node page count conversion overflow"))?;
        let mut current_page = header.root_page;
        let mut hops = 0usize;

        loop {
            if hops > node_page_count {
                return Err(Self::io_invalid_data(
                    "B+Tree traversal exceeded node count; possible cycle",
                ));
            }
            hops += 1;

            let node = Self::read_node_page_from_file(&mut file, current_page)?;
            if node.dead {
                return Err(Self::io_invalid_data(format!(
                    "B+Tree traversal reached dead node page {}",
                    current_page
                )));
            }

            if node.is_leaf {
                let pos = node.keys.partition_point(|k| k < key);
                if pos < node.keys.len() && &node.keys[pos] == key {
                    return Ok(node.values[pos].clone());
                }
                return Ok(Vec::new());
            }

            let slot = node.keys.partition_point(|k| k <= key);
            if slot >= node.children_pages.len() {
                return Err(Self::io_invalid_data(
                    "B+Tree child slot out of bounds during traversal",
                ));
            }

            let next_page = node.children_pages[slot];
            let _ = Self::page_to_node_index(next_page, node_page_count, "child pointer")?;
            current_page = next_page;
        }
    }

    /// Load a persisted B+ Tree from the paged file at `path`.
    pub fn load(path: &str) -> io::Result<Self> {
        let mut file = OpenOptions::new().read(true).open(path)?;
        let header_page = Self::read_header_page(&mut file)?;
        let version = Self::parse_disk_version(&header_page)?;

        if version == 1 {
            return Self::load_v1_entry_format(path);
        }

        if version != BPLUS_DISK_NODE_FORMAT_VERSION {
            return Err(Self::io_invalid_data(format!(
                "unsupported B+Tree on-disk version {}",
                version
            )));
        }

        let header = Self::parse_node_format_header(&header_page)?;
        Self::load_node_format(path, &header)
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
        self.save_node_format(path)
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
