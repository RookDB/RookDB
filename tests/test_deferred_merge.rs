use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;

use storage_manager::catalog::types::{Catalog, Column, Database, SortDirection, SortKey};
use storage_manager::catalog::{create_table, save_catalog};
use storage_manager::ordered::{
    append_delta_tuple, merge_if_needed, ordered_scan, scan_all_delta_tuples,
};

fn make_tuple(id: i32, name: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(14);
    buf.extend_from_slice(&id.to_le_bytes());
    let mut name_bytes = name.as_bytes().to_vec();
    name_bytes.resize(10, b' ');
    buf.extend_from_slice(&name_bytes[..10]);
    buf
}

fn extract_int(tuple: &[u8]) -> i32 {
    i32::from_le_bytes(tuple[0..4].try_into().unwrap())
}

fn setup_catalog(db_name: &str) -> Catalog {
    let global_dir = "database/global";
    let db_dir = format!("database/base/{}", db_name);
    let _ = fs::remove_dir_all(&db_dir);
    fs::create_dir_all(global_dir).unwrap();
    fs::create_dir_all(&db_dir).unwrap();

    let mut catalog = Catalog {
        databases: HashMap::new(),
    };
    catalog.databases.insert(
        db_name.to_string(),
        Database {
            tables: HashMap::new(),
        },
    );
    save_catalog(&catalog);
    catalog
}

#[test]
fn test_merge_not_triggered_below_threshold() {
    let db = "test_delta_db1";
    let table = "test_delta_tbl1";
    let mut catalog = setup_catalog(db);

    let columns = vec![
        Column {
            name: "id".to_string(),
            data_type: "INT".to_string(),
        },
        Column {
            name: "name".to_string(),
            data_type: "TEXT".to_string(),
        },
    ];
    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    create_table(&mut catalog, db, table, columns, Some(sort_keys));

    for i in 1..=499 {
        append_delta_tuple(db, table, &make_tuple(i, "x")).unwrap();
    }

    {
        let tbl = catalog
            .databases
            .get_mut(db)
            .unwrap()
            .tables
            .get_mut(table)
            .unwrap();
        tbl.delta_current_tuples = Some(499);
    }

    let merged = merge_if_needed(&mut catalog, db, table).unwrap();
    assert!(!merged);

    let delta = scan_all_delta_tuples(db, table).unwrap();
    assert_eq!(delta.len(), 499);

    let _ = fs::remove_file(format!("database/base/{}/{}.dat", db, table));
    let _ = fs::remove_file(format!("database/base/{}/{}.delta", db, table));
    let _ = fs::remove_dir_all(format!("database/base/{}", db));
}

#[test]
fn test_merge_triggered_at_threshold() {
    let db = "test_delta_db2";
    let table = "test_delta_tbl2";
    let mut catalog = setup_catalog(db);

    let columns = vec![
        Column {
            name: "id".to_string(),
            data_type: "INT".to_string(),
        },
        Column {
            name: "name".to_string(),
            data_type: "TEXT".to_string(),
        },
    ];
    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];

    create_table(&mut catalog, db, table, columns, Some(sort_keys));

    for i in (1..=500).rev() {
        append_delta_tuple(db, table, &make_tuple(i, "x")).unwrap();
    }

    {
        let tbl = catalog
            .databases
            .get_mut(db)
            .unwrap()
            .tables
            .get_mut(table)
            .unwrap();
        tbl.delta_current_tuples = Some(500);
    }

    let merged = merge_if_needed(&mut catalog, db, table).unwrap();
    assert!(merged);

    let delta = scan_all_delta_tuples(db, table).unwrap();
    assert_eq!(delta.len(), 0);

    let tbl = catalog
        .databases
        .get(db)
        .unwrap()
        .tables
        .get(table)
        .unwrap();
    assert_eq!(tbl.delta_current_tuples, Some(0));

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(format!("database/base/{}/{}.dat", db, table))
        .unwrap();
    let rows = ordered_scan(&mut file, &catalog, db, table).unwrap();
    assert_eq!(rows.len(), 500);
    let ids: Vec<i32> = rows.iter().map(|t| extract_int(t)).collect();
    assert_eq!(ids.first(), Some(&1));
    assert_eq!(ids.last(), Some(&500));

    let _ = fs::remove_file(format!("database/base/{}/{}.dat", db, table));
    let _ = fs::remove_file(format!("database/base/{}/{}.delta", db, table));
    let _ = fs::remove_dir_all(format!("database/base/{}", db));
}
