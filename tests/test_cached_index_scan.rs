//! Index scans look keys up through the process-wide cached B+ Tree handle
//! instead of opening the index file per query. These tests check that a
//! scan always reflects the latest inserts, deletes and index rebuilds made
//! through that same handle or around it.

mod common;

use common::load_catalog;
use rook_parser::parse_sql;
use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::backend::executor::row_select::{
    parse_where_text, register_where_parser, select_matching_pointers,
};
use storage_manager::catalog::{Catalog, Column, create_database, create_table, save_catalog};
use storage_manager::executor::create_index::create_index;
use storage_manager::executor::delete_by_pointers;
use storage_manager::insert_single_tuple;
use storage_manager::types::DataType;

const DB: &str = "cached_idx_db";

fn column(name: &str, data_type: DataType) -> Column {
    Column {
        name: name.to_string(),
        data_type,
        nullable: true,
        constraints: Default::default(),
    }
}

fn setup() -> Catalog {
    register_where_parser(rook_parser::parse_where_text);
    let mut catalog = load_catalog();
    create_database(&mut catalog, DB);
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        DB,
        "items",
        vec![
            column("id", DataType::Int),
            column("label", DataType::Varchar(16)),
        ],
    );
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();
    create_index(&catalog, DB, "items", "idx_id", &[String::from("id")]).unwrap();
    load_catalog()
}

fn insert(catalog: &Catalog, ids: std::ops::Range<i32>) {
    for i in ids {
        let row = [i.to_string(), format!("item_{}", i)];
        let refs: Vec<&str> = row.iter().map(|s| s.as_str()).collect();
        insert_single_tuple(catalog, DB, "items", &refs).unwrap();
    }
}

fn ids(catalog: &Catalog, sql: &str) -> Vec<String> {
    let plan = parse_sql(sql).unwrap();
    let logical = storage_manager::planner::plan_query(&plan, catalog, DB).unwrap();
    execute_plan_collect(&logical, catalog, DB)
        .unwrap()
        .iter()
        .map(|t| match &t.values[0] {
            Some(dv) => format!("{}", dv),
            None => "NULL".to_string(),
        })
        .collect()
}

#[test]
fn scans_see_rows_inserted_between_queries() {
    let _ws = common::TestWorkspace::new("cachedidx", "inserts");
    let catalog = setup();

    insert(&catalog, 0..100);
    assert_eq!(ids(&catalog, "SELECT id FROM items WHERE id = 42"), ["42"]);
    assert!(ids(&catalog, "SELECT id FROM items WHERE id = 2500").is_empty());

    // Enough rows to split the root several times after the first lookup.
    insert(&catalog, 100..3000);
    assert_eq!(ids(&catalog, "SELECT id FROM items WHERE id = 42"), ["42"]);
    assert_eq!(
        ids(&catalog, "SELECT id FROM items WHERE id = 2500"),
        ["2500"]
    );
    assert_eq!(
        ids(
            &catalog,
            "SELECT id FROM items WHERE id >= 1498 AND id <= 1502"
        ),
        ["1498", "1499", "1500", "1501", "1502"]
    );
}

#[test]
fn scans_see_deletes_and_survive_vacuum() {
    let _ws = common::TestWorkspace::new("cachedidx", "deletes");
    let catalog = setup();
    insert(&catalog, 0..600);
    assert_eq!(
        ids(&catalog, "SELECT id FROM items WHERE id = 300"),
        ["300"]
    );

    let sel = parse_where_text("id >= 200 AND id < 400").unwrap();
    let ptrs = select_matching_pointers(&catalog, DB, "items", sel).unwrap();
    assert_eq!(
        delete_by_pointers(&catalog, DB, "items", &ptrs)
            .unwrap()
            .deleted_count,
        200
    );
    assert!(ids(&catalog, "SELECT id FROM items WHERE id = 300").is_empty());
    assert_eq!(
        ids(&catalog, "SELECT id FROM items WHERE id = 199"),
        ["199"]
    );
    assert_eq!(
        ids(&catalog, "SELECT id FROM items WHERE id = 400"),
        ["400"]
    );

    // VACUUM renumbers slots and rebuilds the index file from scratch.
    let catalog = load_catalog();
    storage_manager::backend::executor::vacuum::vacuum_table(&catalog, DB, "items").unwrap();
    assert!(ids(&catalog, "SELECT id FROM items WHERE id = 300").is_empty());
    assert_eq!(
        ids(&catalog, "SELECT id FROM items WHERE id = 599"),
        ["599"]
    );
    assert_eq!(
        ids(&catalog, "SELECT COUNT(*) FROM items"),
        ["400".to_string()]
    );

    insert(&catalog, 200..210);
    assert_eq!(
        ids(&catalog, "SELECT id FROM items WHERE id = 205"),
        ["205"]
    );
}
