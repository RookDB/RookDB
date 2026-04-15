//! In-memory LRU catalog cache.
//!
//! The cache stores frequently-accessed catalog entries so read-only metadata
//! lookups avoid redundant page reads.  It is invalidated eagerly on every DDL
//! operation (CREATE / ALTER / DROP).

use std::collections::HashMap;

use crate::catalog::types::{Constraint, CatalogDataType, Database, Index, Table};

// ─────────────────────────────────────────────────────────────
// Internal key types
// ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CacheKey {
    Database(String),
    Table(u32, String), // (db_oid, table_name)
    Constraints(u32),   // table_oid
    Indexes(u32),       // table_oid
    Type(u32),          // type_oid
}

// ─────────────────────────────────────────────────────────────
// Cache structure
// ─────────────────────────────────────────────────────────────

/// LRU in-memory cache for catalog metadata.
///
/// All maps store *cloned* copies of their respective structs.
/// The `access_order` vec implements a simple append-on-access LRU:
/// when max_cache_size is reached the front entry (oldest) is evicted.
#[derive(Debug)]
pub struct CatalogCache {
    /// Database entries keyed by name
    pub databases: HashMap<String, Database>,
    /// Table entries keyed by (db_oid, table_name)
    pub tables: HashMap<(u32, String), Table>,
    /// Constraint lists per table OID
    pub constraints: HashMap<u32, Vec<Constraint>>,
    /// Index lists per table OID
    pub indexes: HashMap<u32, Vec<Index>>,
    /// Data-type metadata keyed by type OID
    pub types: HashMap<u32, CatalogDataType>,
    /// Secondary lookup: type name → type OID
    pub type_names: HashMap<String, u32>,

    access_order: Vec<CacheKey>,
    pub max_cache_size: usize,
}

impl CatalogCache {
    pub fn new(max_size: usize) -> Self {
        CatalogCache {
            databases: HashMap::new(),
            tables: HashMap::new(),
            constraints: HashMap::new(),
            indexes: HashMap::new(),
            types: HashMap::new(),
            type_names: HashMap::new(),
            access_order: Vec::new(),
            max_cache_size: max_size,
        }
    }

    /// Default instance used by `Catalog::new()`.
    pub fn default_instance() -> Self {
        Self::new(256)
    }

    // ──────────────────────────────────────────────────────────────
    // Database access
    // ──────────────────────────────────────────────────────────────

    pub fn get_database(&mut self, name: &str) -> Option<&Database> {
        if self.databases.contains_key(name) {
            self.update_access(CacheKey::Database(name.to_string()));
            self.databases.get(name)
        } else {
            None
        }
    }

    pub fn insert_database(&mut self, name: String, db: Database) {
        self.evict_if_needed();
        self.databases.insert(name.clone(), db);
        self.update_access(CacheKey::Database(name));
    }

    pub fn invalidate_database(&mut self, name: &str) {
        self.databases.remove(name);
        self.access_order
            .retain(|k| k != &CacheKey::Database(name.to_string()));
    }

    // ──────────────────────────────────────────────────────────────
    // Table access
    // ──────────────────────────────────────────────────────────────

    pub fn get_table(&mut self, db_oid: u32, table_name: &str) -> Option<&Table> {
        let key = (db_oid, table_name.to_string());
        if self.tables.contains_key(&key) {
            self.update_access(CacheKey::Table(db_oid, table_name.to_string()));
            self.tables.get(&key)
        } else {
            None
        }
    }

    pub fn insert_table(&mut self, db_oid: u32, table_name: String, table: Table) {
        self.evict_if_needed();
        self.tables.insert((db_oid, table_name.clone()), table);
        self.update_access(CacheKey::Table(db_oid, table_name));
    }

    pub fn invalidate_table(&mut self, db_oid: u32, table_name: &str) {
        let key = (db_oid, table_name.to_string());
        let ckey = CacheKey::Table(db_oid, table_name.to_string());
        self.tables.remove(&key);
        self.access_order.retain(|k| k != &ckey);
    }

    // ──────────────────────────────────────────────────────────────
    // Constraints
    // ──────────────────────────────────────────────────────────────

    pub fn get_constraints(&mut self, table_oid: u32) -> Option<&Vec<Constraint>> {
        if self.constraints.contains_key(&table_oid) {
            self.update_access(CacheKey::Constraints(table_oid));
            self.constraints.get(&table_oid)
        } else {
            None
        }
    }

    pub fn insert_constraints(&mut self, table_oid: u32, constraints: Vec<Constraint>) {
        self.evict_if_needed();
        self.constraints.insert(table_oid, constraints);
        self.update_access(CacheKey::Constraints(table_oid));
    }

    pub fn invalidate_constraints(&mut self, table_oid: u32) {
        self.constraints.remove(&table_oid);
        self.access_order
            .retain(|k| k != &CacheKey::Constraints(table_oid));
    }

    // ──────────────────────────────────────────────────────────────
    // Indexes
    // ──────────────────────────────────────────────────────────────

    pub fn get_indexes(&mut self, table_oid: u32) -> Option<&Vec<Index>> {
        if self.indexes.contains_key(&table_oid) {
            self.update_access(CacheKey::Indexes(table_oid));
            self.indexes.get(&table_oid)
        } else {
            None
        }
    }

    pub fn insert_indexes(&mut self, table_oid: u32, indexes: Vec<Index>) {
        self.evict_if_needed();
        self.indexes.insert(table_oid, indexes);
        self.update_access(CacheKey::Indexes(table_oid));
    }

    pub fn invalidate_indexes(&mut self, table_oid: u32) {
        self.indexes.remove(&table_oid);
        self.access_order
            .retain(|k| k != &CacheKey::Indexes(table_oid));
    }

    // ──────────────────────────────────────────────────────────────
    // Types
    // ──────────────────────────────────────────────────────────────

    pub fn get_type_by_oid(&mut self, type_oid: u32) -> Option<&CatalogDataType> {
        if self.types.contains_key(&type_oid) {
            self.update_access(CacheKey::Type(type_oid));
            self.types.get(&type_oid)
        } else {
            None
        }
    }

    pub fn get_type_by_name(&self, name: &str) -> Option<&CatalogDataType> {
        let oid = self.type_names.get(name)?;
        self.types.get(oid)
    }

    pub fn insert_type(&mut self, dt: CatalogDataType) {
        self.evict_if_needed();
        let oid = dt.type_oid;
        let name = dt.type_name.clone();
        self.update_access(CacheKey::Type(oid));
        self.types.insert(oid, dt);
        self.type_names.insert(name, oid);
    }

    // ──────────────────────────────────────────────────────────────
    // Internal helpers
    // ──────────────────────────────────────────────────────────────

    fn update_access(&mut self, key: CacheKey) {
        self.access_order.retain(|k| k != &key);
        self.access_order.push(key);
    }

    fn evict_if_needed(&mut self) {
        while self.access_order.len() >= self.max_cache_size {
            if let Some(oldest) = self.access_order.first().cloned() {
                self.access_order.remove(0);
                match &oldest {
                    CacheKey::Database(n) => {
                        self.databases.remove(n);
                    }
                    CacheKey::Table(oid, n) => {
                        self.tables.remove(&(*oid, n.clone()));
                    }
                    CacheKey::Constraints(oid) => {
                        self.constraints.remove(oid);
                    }
                    CacheKey::Indexes(oid) => {
                        self.indexes.remove(oid);
                    }
                    CacheKey::Type(oid) => {
                        if let Some(dt) = self.types.remove(oid) {
                            self.type_names.remove(&dt.type_name);
                        }
                    }
                }
            } else {
                break;
            }
        }
    }

    /// Discard every entry in the cache (used after bulk DDL operations)
    pub fn invalidate_all(&mut self) {
        self.databases.clear();
        self.tables.clear();
        self.constraints.clear();
        self.indexes.clear();
        self.types.clear();
        self.type_names.clear();
        self.access_order.clear();
    }
}
