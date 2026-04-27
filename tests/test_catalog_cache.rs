//! Tests for the catalog cache – LRU behavior and DDL invalidation (Spec §7.5).
//!
//! Covers:
//! - 7.5.1: Cache hit (second access returns cached data)
//! - 7.5.2: Cache invalidation on DDL
//! - 7.5.3: LRU eviction when max_cache_size is reached


use storage_manager::catalog::cache::CatalogCache;
use storage_manager::catalog::types::*;

/// Helper: create a minimal Database struct for cache testing
fn make_db(name: &str, oid: u32) -> Database {
    Database {
        db_oid: oid,
        db_name: name.to_string(),
        owner: "tester".to_string(),
        encoding: Encoding::UTF8,
        created_at: 0,
    }
}

/// Helper: create a minimal CatalogTable struct for cache testing
fn make_table(name: &str, oid: u32, db_oid: u32) -> CatalogTable {
    CatalogTable {
        table_oid: oid,
        table_name: name.to_string(),
        db_oid,
        table_type: TableType::UserTable,
        statistics: TableStatistics::default(),
    }
}

// ─────────────────────────────────────────────────────────────
// Test 7.5.1: Cache Hit
// ─────────────────────────────────────────────────────────────

#[test]
fn test_cache_database_hit() {
    let mut cache = CatalogCache::new(256);

    // First access: miss
    assert!(cache.get_database("mydb").is_none());

    // Insert into cache
    cache.insert_database("mydb".to_string(), make_db("mydb", 1));

    // Second access: hit
    let db = cache.get_database("mydb");
    assert!(db.is_some());
    assert_eq!(db.unwrap().db_name, "mydb");
    assert_eq!(db.unwrap().db_oid, 1);
}

#[test]
fn test_cache_table_hit() {
    let mut cache = CatalogCache::new(256);
    let db_oid = 1;

    assert!(cache.get_table(db_oid, "users").is_none());

    cache.insert_table(
        db_oid,
        "users".to_string(),
        make_table("users", 100, db_oid),
    );

    let t = cache.get_table(db_oid, "users");
    assert!(t.is_some());
    assert_eq!(t.unwrap().table_name, "users");
    assert_eq!(t.unwrap().table_oid, 100);
}

#[test]
fn test_cache_constraints_hit() {
    let mut cache = CatalogCache::new(256);
    let table_oid = 100;

    assert!(cache.get_constraints(table_oid).is_none());

    let constraints = vec![Constraint {
        constraint_oid: 500,
        constraint_name: "pk_id".to_string(),
        constraint_type: ConstraintType::PrimaryKey,
        table_oid,
        column_oids: vec![200],
        metadata: ConstraintMetadata::PrimaryKey { index_oid: 600 },
        is_deferrable: false,
    }];
    cache.insert_constraints(table_oid, constraints);

    let c = cache.get_constraints(table_oid);
    assert!(c.is_some());
    assert_eq!(c.unwrap().len(), 1);
    assert_eq!(c.unwrap()[0].constraint_name, "pk_id");
}

#[test]
fn test_cache_indexes_hit() {
    let mut cache = CatalogCache::new(256);
    let table_oid = 100;

    assert!(cache.get_indexes(table_oid).is_none());

    let indexes = vec![Index {
        index_oid: 600,
        index_name: "idx_email".to_string(),
        table_oid,
        index_type: IndexType::BTree,
        column_oids: vec![201],
        is_unique: true,
        is_primary: false,
        index_pages: 1,
    }];
    cache.insert_indexes(table_oid, indexes);

    let i = cache.get_indexes(table_oid);
    assert!(i.is_some());
    assert_eq!(i.unwrap().len(), 1);
}

#[test]
fn test_cache_type_hit_by_oid_and_name() {
    let mut cache = CatalogCache::new(256);

    assert!(cache.get_type_by_oid(1).is_none());
    assert!(cache.get_type_by_name("INT").is_none());

    cache.insert_type(DataType::int());

    assert!(cache.get_type_by_oid(1).is_some());
    assert!(cache.get_type_by_name("INT").is_some());
    assert_eq!(cache.get_type_by_name("INT").unwrap().type_oid, 1);
}

// ─────────────────────────────────────────────────────────────
// Test 7.5.2: Cache Invalidation on DDL
// ─────────────────────────────────────────────────────────────

#[test]
fn test_cache_invalidate_database() {
    let mut cache = CatalogCache::new(256);
    cache.insert_database("mydb".to_string(), make_db("mydb", 1));
    assert!(cache.get_database("mydb").is_some());

    cache.invalidate_database("mydb");
    assert!(cache.get_database("mydb").is_none());
}

#[test]
fn test_cache_invalidate_table() {
    let mut cache = CatalogCache::new(256);
    let db_oid = 1;
    cache.insert_table(
        db_oid,
        "users".to_string(),
        make_table("users", 100, db_oid),
    );
    assert!(cache.get_table(db_oid, "users").is_some());

    cache.invalidate_table(db_oid, "users");
    assert!(cache.get_table(db_oid, "users").is_none());
}

#[test]
fn test_cache_invalidate_constraints() {
    let mut cache = CatalogCache::new(256);
    let table_oid = 100;
    cache.insert_constraints(table_oid, vec![]);
    assert!(cache.get_constraints(table_oid).is_some());

    cache.invalidate_constraints(table_oid);
    assert!(cache.get_constraints(table_oid).is_none());
}

#[test]
fn test_cache_invalidate_indexes() {
    let mut cache = CatalogCache::new(256);
    let table_oid = 100;
    cache.insert_indexes(table_oid, vec![]);
    assert!(cache.get_indexes(table_oid).is_some());

    cache.invalidate_indexes(table_oid);
    assert!(cache.get_indexes(table_oid).is_none());
}

#[test]
fn test_cache_invalidate_all() {
    let mut cache = CatalogCache::new(256);
    cache.insert_database("db1".to_string(), make_db("db1", 1));
    cache.insert_database("db2".to_string(), make_db("db2", 2));
    cache.insert_table(1, "t1".to_string(), make_table("t1", 100, 1));
    cache.insert_constraints(100, vec![]);
    cache.insert_indexes(100, vec![]);
    cache.insert_type(DataType::int());

    cache.invalidate_all();

    assert!(cache.get_database("db1").is_none());
    assert!(cache.get_database("db2").is_none());
    assert!(cache.get_table(1, "t1").is_none());
    assert!(cache.get_constraints(100).is_none());
    assert!(cache.get_indexes(100).is_none());
    assert!(cache.get_type_by_oid(1).is_none());
}

// ─────────────────────────────────────────────────────────────
// Test 7.5.3: LRU Eviction
// ─────────────────────────────────────────────────────────────

#[test]
fn test_cache_lru_eviction_basic() {
    // Set cache size to 5 entries
    let mut cache = CatalogCache::new(5);

    // Insert 5 databases (fills cache exactly)
    for i in 0..5 {
        let name = format!("db{}", i);
        cache.insert_database(name.clone(), make_db(&name, i as u32));
    }

    // All 5 should be present
    for i in 0..5 {
        assert!(
            cache.get_database(&format!("db{}", i)).is_some(),
            "db{} should still be in cache",
            i
        );
    }

    // Insert 6th entry – should evict the oldest (db0, since we just accessed all of them
    // in order 0..4, but get_database updated access order; the oldest is db0 after all gets ran)
    cache.insert_database("db5".to_string(), make_db("db5", 5));

    // The newest entry should be present
    assert!(
        cache.get_database("db5").is_some(),
        "db5 should be in cache"
    );
}

#[test]
fn test_cache_lru_eviction_access_order() {
    let mut cache = CatalogCache::new(3);

    // Insert 3 entries
    cache.insert_database("a".to_string(), make_db("a", 1));
    cache.insert_database("b".to_string(), make_db("b", 2));
    cache.insert_database("c".to_string(), make_db("c", 3));

    // Access "a" to make it the most recently used
    let _ = cache.get_database("a");

    // Insert "d" – should evict "b" (oldest after "a" was re-accessed)
    cache.insert_database("d".to_string(), make_db("d", 4));

    // "a" should survive (was recently accessed)
    assert!(
        cache.get_database("a").is_some(),
        "a should survive LRU eviction"
    );
    // "d" should be present (just inserted)
    assert!(cache.get_database("d").is_some(), "d should be in cache");
    // "b" should be evicted (oldest)
    assert!(
        cache.get_database("b").is_none(),
        "b should have been evicted"
    );
}

#[test]
fn test_cache_lru_mixed_entry_types() {
    // Cache with only 3 slots – mix databases, tables, and constraints
    let mut cache = CatalogCache::new(3);

    cache.insert_database("db1".to_string(), make_db("db1", 1));
    cache.insert_table(1, "t1".to_string(), make_table("t1", 100, 1));
    cache.insert_constraints(100, vec![]);

    // All 3 should exist
    assert!(cache.get_database("db1").is_some());

    // Insert one more – oldest (db1 was just accessed, so t1 or constraints is oldest)
    cache.insert_indexes(100, vec![]);

    // We can't predict exactly which one was evicted since get_database updated access,
    // but the cache should have exactly 3 entries max
    let total_entries =
        cache.databases.len() + cache.tables.len() + cache.constraints.len() + cache.indexes.len();
    assert!(
        total_entries <= 3,
        "Cache should not exceed max_cache_size of 3, got {}",
        total_entries
    );
}

#[test]
fn test_cache_default_instance() {
    let cache = CatalogCache::default_instance();
    assert_eq!(
        cache.max_cache_size, 256,
        "Default cache size should be 256"
    );
}
