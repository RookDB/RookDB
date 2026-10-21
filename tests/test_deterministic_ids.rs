//! Integration tests for deterministic db_id/table_id assignment
//! (ANALYSIS.md Tier 1 #3).
//!
//! `populate_system_tables()` used to iterate the catalog HashMaps directly;
//! Rust's `RandomState` re-seeds every HashMap instance, so IDs differed per
//! process (and even per save within one process). IDs are now assigned in
//! sorted-name order, which these tests verify by round-tripping a catalog
//! several times and comparing the generated ID tables.

mod common;

use common::load_catalog;
use storage_manager::catalog::{Column, create_database, create_table, save_catalog};
use storage_manager::types::DataType;

fn plain_col(name: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: DataType::Int,
        nullable: true,
        constraints: Default::default(),
    }
}

/// Read `(name, id)` pairs from a system table (sys_databases / sys_tables).
fn read_id_pairs(
    sys_table: &str,
    schema: &[storage_manager::types::DataType],
    name_idx: usize,
) -> Vec<(String, i32)> {
    use storage_manager::types::{DataValue, deserialize_nullable_row};
    let path = common::path(format!("database/system/{}.dat", sys_table));
    let heap = storage_manager::heap::HeapManager::open(path).expect("open system table");
    let mut pairs = Vec::new();
    for result in heap.scan() {
        if let Ok((_, _, raw)) = result
            && let Ok(row) = deserialize_nullable_row(schema, &raw)
        {
            let id = match row.first() {
                Some(Some(DataValue::Int(id))) => *id,
                _ => continue,
            };
            let name = match row.get(name_idx) {
                Some(Some(DataValue::Varchar(s))) => s.clone(),
                Some(Some(DataValue::Char(s))) => s.clone(),
                _ => continue,
            };
            pairs.push((name, id));
        }
    }
    pairs
}

fn current_db_ids() -> Vec<(String, i32)> {
    read_id_pairs(
        "databases",
        storage_manager::backend::system_table::SYS_DATABASES_SCHEMA,
        1,
    )
}

fn current_table_ids() -> Vec<(String, i32)> {
    read_id_pairs(
        "tables",
        storage_manager::backend::system_table::SYS_TABLES_SCHEMA,
        2,
    )
}

#[test]
fn database_and_table_ids_are_stable_across_repeated_saves() {
    let _ws = common::TestWorkspace::new("detids", "stable");

    // Insert in deliberately NON-sorted order.
    for db in ["zeta", "alpha", "mid"] {
        let mut catalog = load_catalog();
        create_database(&mut catalog, db);
    }
    for (db, table) in [
        ("zeta", "widgets"),
        ("alpha", "users"),
        ("alpha", "accounts"),
        ("mid", "orders"),
        ("zeta", "apple_t"),
    ] {
        let mut catalog = load_catalog();
        create_table(&mut catalog, db, table, vec![plain_col("id")]).unwrap();
    }

    let dbs_first = current_db_ids();
    let tbls_first = current_table_ids();

    // Repeated save/load cycles must produce byte-identical ID assignment.
    for cycle in 1..=4 {
        let catalog = load_catalog();
        save_catalog(&catalog).unwrap();
        let dbs = current_db_ids();
        let tbls = current_table_ids();
        assert_eq!(dbs, dbs_first, "cycle {}: database ids changed", cycle);
        assert_eq!(tbls, tbls_first, "cycle {}: table ids changed", cycle);
    }
}

#[test]
fn ids_are_assigned_in_sorted_name_order() {
    let _ws = common::TestWorkspace::new("detids", "sorted");

    for db in ["b_db", "a_db", "c_db"] {
        let mut catalog = load_catalog();
        create_database(&mut catalog, db);
    }
    {
        let mut catalog = load_catalog();
        create_table(&mut catalog, "a_db", "zebra", vec![plain_col("id")]).unwrap();
        create_table(&mut catalog, "a_db", "aardvark", vec![plain_col("id")]).unwrap();
        create_table(&mut catalog, "a_db", "mango", vec![plain_col("id")]).unwrap();
    }

    let mut dbs = current_db_ids();
    dbs.sort();
    assert_eq!(
        dbs,
        vec![
            ("a_db".to_string(), 1),
            ("b_db".to_string(), 2),
            ("c_db".to_string(), 3),
        ],
        "database ids must follow sorted name order"
    );

    // Only a_db owns tables, so their ids start at 1 and follow name order.
    let mut tables_of_a: Vec<(String, i32)> = current_table_ids()
        .into_iter()
        .filter(|(n, _)| ["zebra", "aardvark", "mango"].iter().any(|x| x == n))
        .collect();
    tables_of_a.sort_by_key(|(_, id)| *id);
    assert_eq!(
        tables_of_a,
        vec![
            ("aardvark".to_string(), 1),
            ("mango".to_string(), 2),
            ("zebra".to_string(), 3),
        ],
        "table ids within a database must follow sorted name order"
    );
}
