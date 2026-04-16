//! High-level index management: `AnyIndex` enum dispatcher and helpers for
//! building, loading, saving, and querying indices regardless of algorithm.
//!
//! # Design rationale
//!
//! Rust trait objects (`Box<dyn IndexTrait>`) cannot easily include the static
//! `load(path) -> Self` constructor (which needs a concrete type).  An enum
//! wrapper solves this by enumerating every concrete index type and forwarding
//! method calls to the inner value.
//!
//! # Usage
//!
//! ```rust,ignore
//! // Build a B+ Tree index on column "age" from an existing table.
//! let idx = AnyIndex::build_from_table(
//!     &catalog, "mydb", "employees", "age", "age_idx",
//!     &IndexAlgorithm::BPlusTree,
//! )?;
//! idx.save(&index_file_path("mydb", "employees", "age_idx"))?;
//!
//! // Later: load and query.
//! let idx = AnyIndex::load(path, &IndexAlgorithm::BPlusTree)?;
//! let records = idx.search(&IndexKey::Int(30))?;
//! ```

use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom};
use std::collections::HashMap;

use crate::catalog::types::{Catalog, Column, IndexAlgorithm};
use crate::index::config::{DEFAULT_HASH_INDEX, DEFAULT_TREE_INDEX, HashIndexType, TreeIndexType};
use crate::heap::{init_table, insert_tuple};
use crate::index::hash::{ChainedHashIndex, ExtendibleHashIndex, LinearHashIndex, StaticHashIndex};
use crate::index::index_trait::{HashBasedIndex, IndexKey, IndexTrait, RecordId, TreeBasedIndex};
use crate::index::tree::{BPlusTree, BTree, LsmTreeIndex, RadixTree, SkipListIndex};
use crate::layout::TABLE_FILE_TEMPLATE;
use crate::page::{PAGE_HEADER_SIZE, ITEM_ID_SIZE};
use crate::table::page_count;

// ─── AnyIndex ─────────────────────────────────────────────────────────────────

/// A type-erased index value that wraps any concrete index implementation.
///
/// All `IndexTrait` methods are forwarded to the inner variant.  Tree-based
/// variants additionally expose `range_scan`, `min_key`, and `max_key` through
/// `AnyIndex::range_scan`.
pub enum AnyIndex {
    StaticHash(StaticHashIndex),
    ChainedHash(ChainedHashIndex),
    ExtendibleHash(ExtendibleHashIndex),
    LinearHash(LinearHashIndex),
    BTree(BTree),
    BPlusTree(BPlusTree),
    RadixTree(RadixTree),
    SkipList(SkipListIndex),
    LsmTree(LsmTreeIndex),
}

impl AnyIndex {
    // ─── Construction ────────────────────────────────────────────────────────

    /// Create an empty `AnyIndex` of the type specified by `algorithm`.
    pub fn new_empty(algorithm: &IndexAlgorithm) -> Self {
        match algorithm {
            IndexAlgorithm::StaticHash => Self::StaticHash(StaticHashIndex::with_defaults()),
            IndexAlgorithm::ChainedHash => Self::ChainedHash(ChainedHashIndex::with_defaults()),
            IndexAlgorithm::ExtendibleHash => {
                Self::ExtendibleHash(ExtendibleHashIndex::with_defaults())
            }
            IndexAlgorithm::LinearHash => Self::LinearHash(LinearHashIndex::with_defaults()),
            IndexAlgorithm::BTree => Self::BTree(BTree::with_defaults()),
            IndexAlgorithm::BPlusTree => Self::BPlusTree(BPlusTree::with_defaults()),
            IndexAlgorithm::RadixTree => Self::RadixTree(RadixTree::new()),
            IndexAlgorithm::SkipList => Self::SkipList(SkipListIndex::new()),
            IndexAlgorithm::LsmTree => Self::LsmTree(LsmTreeIndex::with_defaults()),
        }
    }

    /// Create an empty index using the system-wide default algorithm for the
    /// index family (hash or tree) implied by `family`.
    ///
    /// `family` is either `"hash"` or `"tree"`.  Any other string falls back
    /// to the default tree index.
    pub fn new_default(family: &str) -> Self {
        if family == "hash" {
            match DEFAULT_HASH_INDEX {
                HashIndexType::Static => Self::StaticHash(StaticHashIndex::with_defaults()),
                HashIndexType::Extendible => {
                    Self::ExtendibleHash(ExtendibleHashIndex::with_defaults())
                }
                HashIndexType::Linear => Self::LinearHash(LinearHashIndex::with_defaults()),
            }
        } else {
            match DEFAULT_TREE_INDEX {
                TreeIndexType::BTree => Self::BTree(BTree::with_defaults()),
                TreeIndexType::BPlusTree => Self::BPlusTree(BPlusTree::with_defaults()),
                TreeIndexType::RadixTree => Self::RadixTree(RadixTree::new()),
            }
        }
    }

    /// Load a previously saved index from `path`.  The `algorithm` field in
    /// the catalog's `IndexEntry` tells us which concrete type to deserialise.
    pub fn load(path: &str, algorithm: &IndexAlgorithm) -> io::Result<Self> {
        match algorithm {
            IndexAlgorithm::StaticHash => {
                Ok(Self::StaticHash(StaticHashIndex::load(path)?))
            }
            IndexAlgorithm::ChainedHash => {
                Ok(Self::ChainedHash(ChainedHashIndex::load(path)?))
            }
            IndexAlgorithm::ExtendibleHash => {
                Ok(Self::ExtendibleHash(ExtendibleHashIndex::load(path)?))
            }
            IndexAlgorithm::LinearHash => {
                Ok(Self::LinearHash(LinearHashIndex::load(path)?))
            }
            IndexAlgorithm::BTree => Ok(Self::BTree(BTree::load(path)?)),
            IndexAlgorithm::BPlusTree => Ok(Self::BPlusTree(BPlusTree::load(path)?)),
            IndexAlgorithm::RadixTree => Ok(Self::RadixTree(RadixTree::load(path)?)),
            IndexAlgorithm::SkipList => Ok(Self::SkipList(SkipListIndex::load(path)?)),
            IndexAlgorithm::LsmTree => Ok(Self::LsmTree(LsmTreeIndex::load(path)?)),
        }
    }

    // ─── IndexTrait forwarding ────────────────────────────────────────────────

    pub fn insert(&mut self, key: IndexKey, record_id: RecordId) -> io::Result<()> {
        match self {
            Self::StaticHash(i) => i.insert(key, record_id),
            Self::ChainedHash(i) => i.insert(key, record_id),
            Self::ExtendibleHash(i) => i.insert(key, record_id),
            Self::LinearHash(i) => i.insert(key, record_id),
            Self::BTree(i) => i.insert(key, record_id),
            Self::BPlusTree(i) => i.insert(key, record_id),
            Self::RadixTree(i) => i.insert(key, record_id),
            Self::SkipList(i) => i.insert(key, record_id),
            Self::LsmTree(i) => i.insert(key, record_id),
        }
    }

    pub fn search(&self, key: &IndexKey) -> io::Result<Vec<RecordId>> {
        match self {
            Self::StaticHash(i) => i.search(key),
            Self::ChainedHash(i) => i.search(key),
            Self::ExtendibleHash(i) => i.search(key),
            Self::LinearHash(i) => i.search(key),
            Self::BTree(i) => i.search(key),
            Self::BPlusTree(i) => i.search(key),
            Self::RadixTree(i) => i.search(key),
            Self::SkipList(i) => i.search(key),
            Self::LsmTree(i) => i.search(key),
        }
    }

    /// Perform a point lookup directly against the persisted index file when
    /// the algorithm supports efficient on-disk traversal.
    pub fn search_on_disk(
        path: &str,
        algorithm: &IndexAlgorithm,
        key: &IndexKey,
    ) -> io::Result<Vec<RecordId>> {
        match algorithm {
            IndexAlgorithm::BPlusTree => BPlusTree::search_on_disk(path, key),
            _ => {
                let index = Self::load(path, algorithm)?;
                index.search(key)
            }
        }
    }

    pub fn delete(&mut self, key: &IndexKey, record_id: &RecordId) -> io::Result<bool> {
        match self {
            Self::StaticHash(i) => i.delete(key, record_id),
            Self::ChainedHash(i) => i.delete(key, record_id),
            Self::ExtendibleHash(i) => i.delete(key, record_id),
            Self::LinearHash(i) => i.delete(key, record_id),
            Self::BTree(i) => i.delete(key, record_id),
            Self::BPlusTree(i) => i.delete(key, record_id),
            Self::RadixTree(i) => i.delete(key, record_id),
            Self::SkipList(i) => i.delete(key, record_id),
            Self::LsmTree(i) => i.delete(key, record_id),
        }
    }

    pub fn save(&self, path: &str) -> io::Result<()> {
        match self {
            Self::StaticHash(i) => i.save(path),
            Self::ChainedHash(i) => i.save(path),
            Self::ExtendibleHash(i) => i.save(path),
            Self::LinearHash(i) => i.save(path),
            Self::BTree(i) => i.save(path),
            Self::BPlusTree(i) => i.save(path),
            Self::RadixTree(i) => i.save(path),
            Self::SkipList(i) => i.save(path),
            Self::LsmTree(i) => i.save(path),
        }
    }

    pub fn entry_count(&self) -> usize {
        match self {
            Self::StaticHash(i) => i.entry_count(),
            Self::ChainedHash(i) => i.entry_count(),
            Self::ExtendibleHash(i) => i.entry_count(),
            Self::LinearHash(i) => i.entry_count(),
            Self::BTree(i) => i.entry_count(),
            Self::BPlusTree(i) => i.entry_count(),
            Self::RadixTree(i) => i.entry_count(),
            Self::SkipList(i) => i.entry_count(),
            Self::LsmTree(i) => i.entry_count(),
        }
    }

    pub fn index_type_name(&self) -> &'static str {
        match self {
            Self::StaticHash(i) => i.index_type_name(),
            Self::ChainedHash(i) => i.index_type_name(),
            Self::ExtendibleHash(i) => i.index_type_name(),
            Self::LinearHash(i) => i.index_type_name(),
            Self::BTree(i) => i.index_type_name(),
            Self::BPlusTree(i) => i.index_type_name(),
            Self::RadixTree(i) => i.index_type_name(),
            Self::SkipList(i) => i.index_type_name(),
            Self::LsmTree(i) => i.index_type_name(),
        }
    }

    pub fn all_entries(&self) -> io::Result<Vec<(IndexKey, RecordId)>> {
        match self {
            Self::StaticHash(i) => i.all_entries(),
            Self::ChainedHash(i) => i.all_entries(),
            Self::ExtendibleHash(i) => i.all_entries(),
            Self::LinearHash(i) => i.all_entries(),
            Self::BTree(i) => i.all_entries(),
            Self::BPlusTree(i) => i.all_entries(),
            Self::RadixTree(i) => i.all_entries(),
            Self::SkipList(i) => i.all_entries(),
            Self::LsmTree(i) => i.all_entries(),
        }
    }

    pub fn validate_structure(&self) -> io::Result<()> {
        match self {
            Self::StaticHash(i) => i.validate_structure(),
            Self::ChainedHash(i) => i.validate_structure(),
            Self::ExtendibleHash(i) => i.validate_structure(),
            Self::LinearHash(i) => i.validate_structure(),
            Self::BTree(i) => i.validate_structure(),
            Self::BPlusTree(i) => i.validate_structure(),
            Self::RadixTree(i) => i.validate_structure(),
            Self::SkipList(i) => i.validate_structure(),
            Self::LsmTree(i) => i.validate_structure(),
        }
    }

    // ─── Tree-only operations ─────────────────────────────────────────────────

    /// Perform a range scan on a tree-based index.
    ///
    /// Returns `Err` with `ErrorKind::Unsupported` for hash-based indices
    /// (hash indices do not support ordered range queries).
    pub fn range_scan(
        &self,
        start: &IndexKey,
        end: &IndexKey,
    ) -> io::Result<Vec<RecordId>> {
        match self {
            Self::BTree(i) => i.range_scan(start, end),
            Self::BPlusTree(i) => i.range_scan(start, end),
            Self::RadixTree(i) => i.range_scan(start, end),
            Self::SkipList(i) => i.range_scan(start, end),
            Self::LsmTree(i) => i.range_scan(start, end),
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "range_scan is not supported by hash-based index '{}'",
                    self.index_type_name()
                ),
            )),
        }
    }

    /// `true` if this index supports ordered range scans.
    pub fn supports_range_scan(&self) -> bool {
        matches!(
            self,
            Self::BTree(_)
                | Self::BPlusTree(_)
                | Self::RadixTree(_)
                | Self::SkipList(_)
                | Self::LsmTree(_)
        )
    }

    // ─── Hash-only statistics ─────────────────────────────────────────────────

    /// Current load factor for hash-based indices; `None` for tree indices.
    pub fn load_factor(&self) -> Option<f64> {
        match self {
            Self::StaticHash(i) => Some(i.load_factor()),
            Self::ChainedHash(i) => Some(i.load_factor()),
            Self::ExtendibleHash(i) => Some(i.load_factor()),
            Self::LinearHash(i) => Some(i.load_factor()),
            _ => None,
        }
    }

    // ─── Build from table ─────────────────────────────────────────────────────

    /// Scan all existing tuples in `table_name` and populate a fresh index on
    /// `column_name`.
    ///
    /// Column values are decoded using the same encoding that `load_csv` uses:
    /// * `INT`  : 4-byte little-endian `i32`
    /// * `TEXT` : 10-byte fixed-width, space-padded UTF-8
    ///
    /// Returns the populated `AnyIndex` ready to be saved to disk.
    pub fn build_from_table(
        catalog: &Catalog,
        db_name: &str,
        table_name: &str,
        column_name: &str,
        algorithm: &IndexAlgorithm,
    ) -> io::Result<Self> {
        let db = catalog.databases.get(db_name).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name))
        })?;
        let table = db.tables.get(table_name).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name))
        })?;

        // Compute the byte offset and type of the indexed column.
        let (col_offset, col_type) = {
            let mut offset = 0usize;
            let mut found = None;
            for col in &table.columns {
                if col.name == column_name {
                    found = Some((offset, col.data_type.clone()));
                    break;
                }
                offset += column_byte_size(&col.data_type);
            }
            found.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Column '{}' not found in table '{}'", column_name, table_name),
                )
            })?
        };

        let table_path = TABLE_FILE_TEMPLATE
            .replace("{database}", db_name)
            .replace("{table}", table_name);

        let mut file = OpenOptions::new().read(true).open(&table_path)?;
        let total_pages = page_count(&mut file)?;

        let mut index = Self::new_empty(algorithm);

        // Scan every data page (page 0 is the header; data starts at page 1).
        for page_num in 1..total_pages {
            let page_offset = page_num as u64 * crate::page::PAGE_SIZE as u64;
            file.seek(SeekFrom::Start(page_offset))?;

            let mut page_bytes = vec![0u8; crate::page::PAGE_SIZE];
            file.read_exact(&mut page_bytes)?;

            let lower = u32::from_le_bytes(page_bytes[0..4].try_into().unwrap()) as usize;
            let num_items =
                (lower - PAGE_HEADER_SIZE as usize) / ITEM_ID_SIZE as usize;

            for item_id in 0..num_items as u32 {
                let slot_base =
                    PAGE_HEADER_SIZE as usize + item_id as usize * ITEM_ID_SIZE as usize;
                let tuple_offset = u32::from_le_bytes(
                    page_bytes[slot_base..slot_base + 4].try_into().unwrap(),
                ) as usize;
                let tuple_len = u32::from_le_bytes(
                    page_bytes[slot_base + 4..slot_base + 8].try_into().unwrap(),
                ) as usize;

                if tuple_offset + tuple_len > page_bytes.len() {
                    continue; // Corrupt slot; skip.
                }

                let tuple = &page_bytes[tuple_offset..tuple_offset + tuple_len];

                if let Some(key) = extract_key(tuple, col_offset, &col_type) {
                    let rid = RecordId::new(page_num, item_id);
                    index.insert(key, rid)?;
                }
            }
        }

        Ok(index)
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Return the on-disk byte size of a column given its data type.
fn column_byte_size(data_type: &str) -> usize {
    match data_type {
        "INT" => 4,
        "TEXT" => 10,
        "BOOL" | "BOOLEAN" => 1,
        _ => 0,
    }
}

/// Extract an `IndexKey` from raw tuple bytes at the given byte offset.
fn extract_key(tuple: &[u8], offset: usize, data_type: &str) -> Option<IndexKey> {
    match data_type {
        "INT" => {
            if offset + 4 > tuple.len() {
                return None;
            }
            let val = i32::from_le_bytes(tuple[offset..offset + 4].try_into().unwrap());
            Some(IndexKey::Int(val as i64))
        }
        "TEXT" => {
            if offset + 10 > tuple.len() {
                return None;
            }
            let text = String::from_utf8_lossy(&tuple[offset..offset + 10])
                .trim()
                .to_string();
            Some(IndexKey::Text(text))
        }
        _ => None,
    }
}

/// Construct the canonical on-disk file path for an index.
///
/// Stored alongside the table file in `database/base/{db}/{table}_{index}.idx`.
pub fn index_file_path(db_name: &str, table_name: &str, index_name: &str) -> String {
    format!(
        "database/base/{}/{}_{}.idx",
        db_name, table_name, index_name
    )
}

fn key_from_tuple(tuple: &[u8], columns: &[Column], column_name: &str) -> io::Result<IndexKey> {
    let mut offset = 0usize;
    for col in columns {
        if col.name == column_name {
            return extract_key(tuple, offset, &col.data_type).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "failed to extract key for column '{}' with type '{}'",
                        column_name, col.data_type
                    ),
                )
            });
        }
        let size = column_byte_size(&col.data_type);
        if size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported column type '{}'", col.data_type),
            ));
        }
        offset = offset.checked_add(size).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "tuple offset overflow")
        })?;
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("column '{}' not found", column_name),
    ))
}

pub fn rebuild_table_indexes(catalog: &Catalog, db_name: &str, table_name: &str) -> io::Result<usize> {
    let table = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(table_name))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{}.{}' not found", db_name, table_name),
            )
        })?;

    for idx in &table.indexes {
        let rebuilt = AnyIndex::build_from_table(
            catalog,
            db_name,
            table_name,
            &idx.column_name,
            &idx.algorithm,
        )?;
        rebuilt.save(&index_file_path(db_name, table_name, &idx.index_name))?;
    }

    Ok(table.indexes.len())
}

pub fn maintain_clustered_index_layout(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
) -> io::Result<bool> {
    let table = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(table_name))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{}.{}' not found", db_name, table_name),
            )
        })?;

    if let Some(clustered) = table.indexes.iter().find(|idx| idx.is_clustered) {
        cluster_table_by_index(catalog, db_name, table_name, &clustered.index_name)?;
        return Ok(true);
    }

    Ok(false)
}

pub fn validate_all_table_indexes(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
) -> io::Result<usize> {
    let table = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(table_name))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{}.{}' not found", db_name, table_name),
            )
        })?;

    for idx in &table.indexes {
        validate_index_consistency(catalog, db_name, table_name, &idx.index_name)?;
    }
    Ok(table.indexes.len())
}

pub fn add_tuple_to_all_indexes(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    tuple: &[u8],
    record_id: RecordId,
) -> io::Result<usize> {
    let table = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(table_name))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{}.{}' not found", db_name, table_name),
            )
        })?;

    for idx in &table.indexes {
        let key = key_from_tuple(tuple, &table.columns, &idx.column_name)?;
        let path = index_file_path(db_name, table_name, &idx.index_name);
        let mut index = AnyIndex::load(&path, &idx.algorithm)?;
        index.insert(key, record_id.clone())?;
        index.save(&path)?;
    }

    Ok(table.indexes.len())
}

pub fn remove_tuple_from_all_indexes(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    tuple: &[u8],
    record_id: RecordId,
) -> io::Result<usize> {
    let table = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(table_name))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{}.{}' not found", db_name, table_name),
            )
        })?;

    for idx in &table.indexes {
        let key = key_from_tuple(tuple, &table.columns, &idx.column_name)?;
        let path = index_file_path(db_name, table_name, &idx.index_name);
        let mut index = AnyIndex::load(&path, &idx.algorithm)?;
        index.delete(&key, &record_id)?;
        index.save(&path)?;
    }

    Ok(table.indexes.len())
}

pub fn cluster_table_by_index(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    index_name: &str,
) -> io::Result<()> {
    let table = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(table_name))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{}.{}' not found", db_name, table_name),
            )
        })?;

    let index_entry = table
        .indexes
        .iter()
        .find(|i| i.index_name == index_name)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("index '{}' not found", index_name),
            )
        })?;

    let mut tuples = scan_live_tuples(db_name, table_name)?;
    tuples.sort_by(|a, b| {
        let ka = key_from_tuple(&a.1, &table.columns, &index_entry.column_name)
            .unwrap_or(IndexKey::Text(String::new()));
        let kb = key_from_tuple(&b.1, &table.columns, &index_entry.column_name)
            .unwrap_or(IndexKey::Text(String::new()));
        ka.cmp(&kb)
    });

    let table_path = TABLE_FILE_TEMPLATE
        .replace("{database}", db_name)
        .replace("{table}", table_name);
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&table_path)?;
    init_table(&mut file)?;

    for (_, tuple) in tuples {
        insert_tuple(&mut file, &tuple)?;
    }

    rebuild_table_indexes(catalog, db_name, table_name)?;
    Ok(())
}

pub fn validate_index_consistency(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    index_name: &str,
) -> io::Result<()> {
    let table = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(table_name))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("table '{}.{}' not found", db_name, table_name),
            )
        })?;

    let entry = table
        .indexes
        .iter()
        .find(|i| i.index_name == index_name)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("index '{}' not found", index_name),
            )
        })?;

    let index_path = index_file_path(db_name, table_name, index_name);
    let index = AnyIndex::load(&index_path, &entry.algorithm)?;

    index.validate_structure()?;

    let mut expected: HashMap<IndexKey, Vec<RecordId>> = HashMap::new();
    for (rid, tuple) in scan_live_tuples(db_name, table_name)? {
        let key = key_from_tuple(&tuple, &table.columns, &entry.column_name)?;
        expected.entry(key).or_default().push(rid);
    }

    let mut actual: HashMap<IndexKey, Vec<RecordId>> = HashMap::new();
    for (key, rid) in index.all_entries()? {
        actual.entry(key).or_default().push(rid);
    }

    normalize_entry_map(&mut expected);
    normalize_entry_map(&mut actual);

    if expected != actual {
        let missing = collect_map_diff(&expected, &actual);
        let stale = collect_map_diff(&actual, &expected);
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "index '{}' consistency failed: {} missing entries, {} stale entries",
                index_name,
                missing,
                stale,
            ),
        ));
    }

    let expected_entry_count: usize = expected.values().map(|rids| rids.len()).sum();
    if index.entry_count() != expected_entry_count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "index '{}' entry_count mismatch: index={}, expected={}",
                index_name,
                index.entry_count(),
                expected_entry_count,
            ),
        ));
    }

    Ok(())
}

fn normalize_entry_map(map: &mut HashMap<IndexKey, Vec<RecordId>>) {
    for rids in map.values_mut() {
        rids.sort_by_key(|rid| (rid.page_no, rid.item_id));
        rids.dedup();
    }
}

fn collect_map_diff(
    a: &HashMap<IndexKey, Vec<RecordId>>,
    b: &HashMap<IndexKey, Vec<RecordId>>,
) -> usize {
    let mut count = 0usize;
    for (key, a_rids) in a {
        match b.get(key) {
            Some(b_rids) => {
                count += a_rids.iter().filter(|rid| !b_rids.contains(rid)).count();
            }
            None => {
                count += a_rids.len();
            }
        }
    }
    count
}

fn scan_live_tuples(db_name: &str, table_name: &str) -> io::Result<Vec<(RecordId, Vec<u8>)>> {
    let table_path = TABLE_FILE_TEMPLATE
        .replace("{database}", db_name)
        .replace("{table}", table_name);

    let mut file = OpenOptions::new().read(true).open(&table_path)?;
    let total_pages = page_count(&mut file)?;
    let mut tuples = Vec::new();

    for page_num in 1..total_pages {
        let page_offset = page_num as u64 * crate::page::PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(page_offset))?;

        let mut page_bytes = vec![0u8; crate::page::PAGE_SIZE];
        file.read_exact(&mut page_bytes)?;

        let lower = u32::from_le_bytes(page_bytes[0..4].try_into().unwrap()) as usize;
        let num_items = (lower - PAGE_HEADER_SIZE as usize) / ITEM_ID_SIZE as usize;

        for item_id in 0..num_items as u32 {
            let slot_base = PAGE_HEADER_SIZE as usize + item_id as usize * ITEM_ID_SIZE as usize;
            let tuple_offset =
                u32::from_le_bytes(page_bytes[slot_base..slot_base + 4].try_into().unwrap())
                    as usize;
            let tuple_len = u32::from_le_bytes(
                page_bytes[slot_base + 4..slot_base + 8].try_into().unwrap(),
            ) as usize;

            if tuple_len == 0 || tuple_offset + tuple_len > page_bytes.len() {
                continue;
            }

            tuples.push((
                RecordId::new(page_num, item_id),
                page_bytes[tuple_offset..tuple_offset + tuple_len].to_vec(),
            ));
        }
    }

    Ok(tuples)
}
