//! Bottom-up bulk load: build a whole tree from its entries at once.
//!
//! Inserting a table's keys one by one descends the tree, scans a leaf for
//! the slot and splits pages half full. A bulk load sorts the entries once,
//! packs leaves left to right, then builds each internal level from the
//! level below, writing every page once.

use crate::backend::error::{RookError, RookResult};
use std::cmp::Ordering;
use std::io;

use super::codec::{
    CHILD_SIZE, KEY_LEN_SIZE, LEAF_HEADER, PAGE_HEADER, VALUE_SIZE, write_root_sidecar,
};
use super::node::{NodeView, int_from_le};
use super::order::KeyOrder;
use super::{BTREE_PAGE_SIZE, BTree, BTreeNode, MAX_ENCODED_KEY_SIZE, encode_key_for};
use crate::types::DataType;
use crate::types::value::DataValue;

/// Bytes a bulk-loaded leaf is filled to, leaving room for later inserts
/// before the first split (PostgreSQL's default leaf fillfactor, 90%).
const LEAF_FILL: usize = BTREE_PAGE_SIZE * 9 / 10;
/// Bytes a bulk-loaded internal node is filled to (PostgreSQL's 70%).
const INTERNAL_FILL: usize = BTREE_PAGE_SIZE * 7 / 10;

/// One entry: its encoded key at `keys[off..off + len]` and its heap tuple.
struct Entry {
    off: usize,
    len: u32,
    page_id: u32,
    slot_id: u32,
}

/// The entries of a tree to be built by [`BTree::bulk_load`], gathered in
/// any order. Keys are encoded into one shared buffer rather than a vector
/// each, so a table's worth of entries costs little more than its key bytes.
pub struct BulkLoad {
    key_arity: usize,
    keys: Vec<u8>,
    entries: Vec<Entry>,
}

impl BulkLoad {
    /// An empty load for a tree whose keys have `key_arity` columns.
    pub fn new(key_arity: usize) -> Self {
        Self {
            key_arity,
            keys: Vec::new(),
            entries: Vec::new(),
        }
    }

    /// Add the entry `keys -> (page_id, slot_id)`. Rejects an oversized key
    /// as [`BTree::insert_keys`] does.
    pub fn push(&mut self, keys: &[DataValue], page_id: u32, slot_id: u32) -> RookResult<()> {
        let encoded = encode_key_for(self.key_arity, keys)?;
        if encoded.len() > MAX_ENCODED_KEY_SIZE {
            return Err(RookError::io(
                io::ErrorKind::InvalidInput,
                format!(
                    "index key of {} bytes exceeds the maximum of {} bytes",
                    encoded.len(),
                    MAX_ENCODED_KEY_SIZE
                ),
            ));
        }
        self.entries.push(Entry {
            off: self.keys.len(),
            len: encoded.len() as u32,
            page_id,
            slot_id,
        });
        self.keys.extend_from_slice(&encoded);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn key(&self, e: &Entry) -> &[u8] {
        &self.keys[e.off..e.off + e.len as usize]
    }

    /// Sort the entries by key. The sort is stable, so equal keys keep the
    /// order they were pushed in, as inserting them one by one would.
    fn sort(&mut self, key_types: &[DataType]) -> RookResult<()> {
        let order = KeyOrder::new(key_types);
        let keys = &self.keys;
        // A single-column integer key, every one of the declared width,
        // sorts as the integer it holds, read in place (as the tree's own
        // integer search does) instead of decoding both sides per compare.
        if let Some(width) = order.int_key_width()
            && self
                .entries
                .iter()
                .all(|e| e.len as usize == KEY_LEN_SIZE + width)
        {
            self.entries
                .sort_by_key(|e| int_from_le(&keys[e.off + KEY_LEN_SIZE..e.off + e.len as usize]));
            return Ok(());
        }
        let mut error = None;
        self.entries.sort_by(|a, b| {
            let ka = &keys[a.off..a.off + a.len as usize];
            let kb = &keys[b.off..b.off + b.len as usize];
            order.cmp_encoded_keys(ka, kb).unwrap_or_else(|e| {
                error.get_or_insert(e);
                Ordering::Equal
            })
        });
        error.map_or(Ok(()), Err)
    }
}

/// Split items `0..count` into consecutive runs whose bytes, `header` plus
/// `size(i, first_in_run)` per item, stay within `fill`. A run closes only
/// once it holds `min` items.
fn pack(
    count: usize,
    header: usize,
    fill: usize,
    min: usize,
    size: impl Fn(usize, bool) -> usize,
) -> Vec<(usize, usize)> {
    let mut runs = Vec::new();
    let mut start = 0;
    let mut used = header;
    for i in 0..count {
        let first = i == start;
        let add = size(i, first);
        if !first && i - start >= min && used + add > fill {
            runs.push((start, i));
            start = i;
            used = header + size(i, true);
        } else {
            used += add;
        }
    }
    if start < count {
        runs.push((start, count));
    }
    runs
}

impl BTree {
    /// Build this tree from `load`. The tree must be empty, as
    /// [`BTree::create`] leaves it. Returns the number of entries loaded.
    pub fn bulk_load(&mut self, mut load: BulkLoad) -> RookResult<usize> {
        if load.key_arity != self.key_types.len() {
            return Err(RookError::io(
                io::ErrorKind::InvalidInput,
                "bulk load key arity does not match the index",
            ));
        }
        let empty = self.root_page_id == 0
            && self.pool.total_pages() == 1
            && NodeView::parse(&self.pool.page_bytes(0)?)?.num_keys() == 0;
        if !empty {
            return Err(RookError::io(
                io::ErrorKind::InvalidInput,
                "bulk load needs an empty index",
            ));
        }
        if load.is_empty() {
            return Ok(0);
        }
        load.sort(&self.key_types)?;

        // Leaves. Page 0, the empty root, becomes the first leaf: the leaf
        // chain ends at page id 0, so no other leaf may sit there. The rest
        // are appended in order, leaf i at page `base + i - 1`.
        let leaves = pack(load.len(), LEAF_HEADER, LEAF_FILL, 1, |i, _| {
            VALUE_SIZE + load.entries[i].len as usize
        });
        let base = self.pool.total_pages();
        let page_of = |i: usize| if i == 0 { 0 } else { base + i as u32 - 1 };
        // (smallest key, page) of each node of the level being built on.
        let mut level: Vec<(&[u8], u32)> = Vec::with_capacity(leaves.len());
        for (i, &(start, end)) in leaves.iter().enumerate() {
            let entries = &load.entries[start..end];
            let node = BTreeNode::Leaf {
                num_keys: entries.len() as u32,
                keys: entries.iter().map(|e| load.key(e).to_vec()).collect(),
                values: entries.iter().map(|e| (e.page_id, e.slot_id)).collect(),
                next_leaf: if i + 1 < leaves.len() {
                    page_of(i + 1)
                } else {
                    0
                },
                prev_leaf: if i > 0 { page_of(i - 1) } else { 0 },
            };
            self.place(page_of(i), &node)?;
            level.push((load.key(&entries[0]), page_of(i)));
        }

        // Internal levels, until one node is left: the root. A node's
        // separators are the smallest keys of its children after the first,
        // the key a split would have promoted.
        while level.len() > 1 {
            let mut runs = pack(level.len(), PAGE_HEADER, INTERNAL_FILL, 2, |i, first| {
                CHILD_SIZE + if first { 0 } else { level[i].0.len() }
            });
            // A last node with one child would have no separator: take one
            // more child from its neighbour. Keys are at most a third of a
            // page, so a node filled to INTERNAL_FILL has three or more
            // children and keeps two.
            if let [.., prev, last] = runs.as_mut_slice()
                && last.1 - last.0 == 1
            {
                prev.1 -= 1;
                last.0 -= 1;
            }
            let mut parents = Vec::with_capacity(runs.len());
            for (start, end) in runs {
                let children = &level[start..end];
                let node = BTreeNode::Internal {
                    num_keys: (children.len() - 1) as u32,
                    keys: children[1..].iter().map(|c| c.0.to_vec()).collect(),
                    children: children.iter().map(|c| c.1).collect(),
                };
                let page_id = self.pool.total_pages();
                self.place(page_id, &node)?;
                parents.push((children[0].0, page_id));
            }
            level = parents;
        }

        let root = level[0].1;
        if root != 0 {
            write_root_sidecar(&self.file_path, root)?;
        }
        self.root_page_id = root;
        Ok(load.len())
    }

    /// Write `node` at `page_id`: page 0 is rewritten in place, any other
    /// page must be the next one appended.
    fn place(&mut self, page_id: u32, node: &BTreeNode) -> RookResult<()> {
        if page_id == 0 {
            return self.write_node(0, node);
        }
        let allocated = self.allocate_page(node)?;
        if allocated != page_id {
            return Err(RookError::io(
                io::ErrorKind::Other,
                format!(
                    "bulk load expected page {} but appended {}",
                    page_id, allocated
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempIndex(PathBuf);

    impl TempIndex {
        fn new(tag: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            Self(std::env::temp_dir().join(format!("rookdb_bulk_{}_{}.idx", tag, nanos)))
        }
    }

    impl Drop for TempIndex {
        fn drop(&mut self) {
            std::fs::remove_file(&self.0).ok();
            std::fs::remove_file(super::super::root_sidecar_path(&self.0)).ok();
        }
    }

    type Row = (Vec<DataValue>, u32, u32);

    /// The same rows through `insert_keys` and through `bulk_load`.
    fn build_both(
        tag: &str,
        types: &[DataType],
        rows: &[Row],
    ) -> (TempIndex, BTree, TempIndex, BTree) {
        let (ia, ib) = (
            TempIndex::new(&format!("{tag}_ins")),
            TempIndex::new(&format!("{tag}_bulk")),
        );
        let mut inserted = BTree::create_composite(ia.0.clone(), types.to_vec()).unwrap();
        for (k, p, s) in rows {
            inserted.insert_keys(k, *p, *s).unwrap();
        }
        let mut bulk = BTree::create_composite(ib.0.clone(), types.to_vec()).unwrap();
        let mut load = BulkLoad::new(types.len());
        for (k, p, s) in rows {
            load.push(k, *p, *s).unwrap();
        }
        assert_eq!(bulk.bulk_load(load).unwrap(), rows.len());
        (ia, inserted, ib, bulk)
    }

    fn assert_same(rows: &[Row], a: &mut BTree, b: &mut BTree) {
        assert_eq!(a.scan_all().unwrap(), b.scan_all().unwrap());
        for (k, _, _) in rows.iter().step_by(7) {
            assert_eq!(
                a.search_all_keys(k).unwrap(),
                b.search_all_keys(k).unwrap(),
                "{:?}",
                k
            );
        }
    }

    fn ints(n: u32, distinct: u32) -> Vec<Row> {
        // A fixed permutation, so keys arrive out of order.
        (0..n)
            .map(|i| {
                let k = (i.wrapping_mul(2_654_435_761) % distinct) as i32;
                (vec![DataValue::Int(k)], i / 100, i % 100)
            })
            .collect()
    }

    #[test]
    fn empty_and_single_entry() {
        let (_a, mut a, _b, mut b) = build_both("empty", &[DataType::Int], &[]);
        assert_same(&[], &mut a, &mut b);
        assert_eq!(b.root_page_id(), 0);
        let one = vec![(vec![DataValue::Int(5)], 3, 4)];
        let (_a, mut a, _b, mut b) = build_both("one", &[DataType::Int], &one);
        assert_same(&one, &mut a, &mut b);
        assert_eq!(b.search(&DataValue::Int(5)).unwrap(), Some((3, 4)));
    }

    #[test]
    fn duplicates_across_leaves_keep_heap_order() {
        // 30 distinct keys over 20K rows: each key's run spans several leaves.
        let rows = ints(20_000, 30);
        let (_a, mut a, _b, mut b) = build_both("dups", &[DataType::Int], &rows);
        assert_same(&rows, &mut a, &mut b);
        let lo = [DataValue::Int(10)];
        let hi = [DataValue::Int(12)];
        assert_eq!(
            a.search_range_keys(&lo, &hi).unwrap(),
            b.search_range_keys(&lo, &hi).unwrap()
        );
        assert!(
            b.total_pages() < a.total_pages(),
            "packed leaves need fewer pages"
        );
    }

    #[test]
    fn varchar_and_composite_keys() {
        let text: Vec<Row> = (0..5_000u32)
            .map(|i| {
                let s = format!("{}-{}", (i * 7919) % 1000, "x".repeat((i % 40) as usize));
                (vec![DataValue::Varchar(s)], i, 0)
            })
            .collect();
        let (_a, mut a, _b, mut b) = build_both("text", &[DataType::Varchar(64)], &text);
        assert_same(&text, &mut a, &mut b);

        let pairs: Vec<Row> = ints(8_000, 500)
            .into_iter()
            .map(|(k, p, s)| {
                (
                    vec![k[0].clone(), DataValue::Int((p * 100 + s) as i32 % 3)],
                    p,
                    s,
                )
            })
            .collect();
        let types = [DataType::Int, DataType::Int];
        let (_a, mut a, _b, mut b) = build_both("pairs", &types, &pairs);
        assert_same(&pairs, &mut a, &mut b);
    }

    #[test]
    fn inserts_and_deletes_after_a_bulk_load() {
        let rows = ints(30_000, 30_000);
        let (_a, mut a, _b, mut b) = build_both("after", &[DataType::Int], &rows);
        let extra = ints(10_000, 7);
        for (k, p, s) in &extra {
            a.insert_keys(k, p + 1000, *s).unwrap();
            b.insert_keys(k, p + 1000, *s).unwrap();
        }
        for (k, p, s) in rows.iter().step_by(3) {
            assert!(a.delete_keys(k, *p, *s).unwrap());
            assert!(b.delete_keys(k, *p, *s).unwrap());
        }
        assert_same(&rows, &mut a, &mut b);
        assert_same(&extra, &mut a, &mut b);
    }

    #[test]
    fn a_bulk_loaded_tree_reopens() {
        let rows = ints(50_000, 50_000);
        let ix = TempIndex::new("reopen");
        let expected = {
            let mut t = BTree::create(ix.0.clone(), DataType::Int).unwrap();
            let mut load = BulkLoad::new(1);
            for (k, p, s) in &rows {
                load.push(k, *p, *s).unwrap();
            }
            t.bulk_load(load).unwrap();
            assert_ne!(t.root_page_id(), 0, "50K keys need internal levels");
            t.sync().unwrap();
            t.scan_all().unwrap()
        };
        let mut t = BTree::open(ix.0.clone()).unwrap();
        t.set_key_type(DataType::Int);
        assert_eq!(t.scan_all().unwrap(), expected);
        for (k, p, s) in rows.iter().step_by(101) {
            assert!(t.search_all_keys(k).unwrap().contains(&(*p, *s)));
        }
    }

    #[test]
    fn only_an_empty_tree_is_bulk_loaded() {
        let ix = TempIndex::new("nonempty");
        let mut t = BTree::create(ix.0.clone(), DataType::Int).unwrap();
        t.insert(&DataValue::Int(1), 0, 0).unwrap();
        let mut load = BulkLoad::new(1);
        load.push(&[DataValue::Int(2)], 0, 1).unwrap();
        assert!(t.bulk_load(load).is_err());
        assert!(
            BulkLoad::new(1)
                .push(&[DataValue::Varchar("x".repeat(4000))], 0, 0)
                .is_err()
        );
    }
}
