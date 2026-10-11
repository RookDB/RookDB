//! UPDATE writes a same-length row image over the old one instead of
//! deleting and re-inserting it. These tests pin what that must preserve:
//! the row's heap location, index correctness, and dead-tuple accounting.

mod common;

use rook_parser::parse_set_clause;
use rook_parser::parse_sql;
use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::backend::executor::row_select::{
    parse_where_text, register_where_parser, select_matching_pointers,
};
use storage_manager::backend::table::read_dead_tuple_count;
use storage_manager::catalog::{
    Catalog, Column, create_database, create_table, load_catalog, save_catalog,
};
use storage_manager::executor::create_index::create_index;
use storage_manager::executor::update_by_pointers;
use storage_manager::insert_single_tuple;
use storage_manager::types::DataType;

const DB: &str = "inplace_db";

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
        "staff",
        vec![
            column("id", DataType::Int),
            column("name", DataType::Varchar(24)),
            column("salary", DataType::Int),
        ],
    );
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();
    create_index(&catalog, DB, "staff", "idx_id", &[String::from("id")]).unwrap();
    let catalog = load_catalog();
    for i in 0..50 {
        let row = [
            i.to_string(),
            format!("user_{:03}", i),
            (1000 + i).to_string(),
        ];
        let refs: Vec<&str> = row.iter().map(|s| s.as_str()).collect();
        insert_single_tuple(&catalog, DB, "staff", &refs).unwrap();
    }
    storage_manager::backend::cache::checkpoint();
    catalog
}

fn pointers(catalog: &Catalog, predicate: &str) -> Vec<(u32, u32)> {
    let sel = parse_where_text(predicate).unwrap();
    select_matching_pointers(catalog, DB, "staff", sel).unwrap()
}

fn update(catalog: &Catalog, predicate: &str, set: &str) -> usize {
    let ptrs = pointers(catalog, predicate);
    let assignments = parse_set_clause(set).unwrap();
    update_by_pointers(catalog, DB, "staff", &ptrs, &assignments)
        .unwrap()
        .updated_count
}

fn query(catalog: &Catalog, sql: &str) -> Vec<Vec<String>> {
    let plan = parse_sql(sql).unwrap();
    let logical = storage_manager::planner::plan_query(&plan, catalog, DB).unwrap();
    execute_plan_collect(&logical, catalog, DB)
        .unwrap()
        .iter()
        .map(|t| {
            t.values
                .iter()
                .map(|v| match v {
                    Some(dv) => format!("{}", dv),
                    None => "NULL".to_string(),
                })
                .collect()
        })
        .collect()
}

fn dead_tuples() -> u32 {
    storage_manager::backend::cache::checkpoint();
    let mut file =
        std::fs::File::open(format!("database/base/{}/staff.dat", DB)).expect("open heap");
    read_dead_tuple_count(&mut file).unwrap()
}

#[test]
fn same_length_update_keeps_the_row_in_place() {
    let _ws = common::TestWorkspace::new("inplace", "same_len");
    let catalog = setup();

    let before = pointers(&catalog, "id = 7");
    assert_eq!(before.len(), 1);

    assert_eq!(update(&catalog, "id = 7", "salary = salary + 1"), 1);

    assert_eq!(pointers(&catalog, "id = 7"), before, "row must not move");
    assert_eq!(dead_tuples(), 0, "an in-place update leaves no dead tuple");
    assert_eq!(
        query(&catalog, "SELECT id, salary FROM staff WHERE id = 7"),
        vec![vec!["7".to_string(), "1008".to_string()]]
    );
    assert_eq!(
        query(&catalog, "SELECT COUNT(*) FROM staff"),
        vec![vec!["50".to_string()]]
    );
}

#[test]
fn length_changing_update_still_replaces_the_row() {
    let _ws = common::TestWorkspace::new("inplace", "diff_len");
    let catalog = setup();

    assert_eq!(update(&catalog, "id = 8", "name = 'a_much_longer_name'"), 1);

    // A longer row cannot reuse its old bytes: the old version is marked
    // dead (counted here) and a new version is inserted.
    assert_eq!(dead_tuples(), 1);
    assert_eq!(pointers(&catalog, "id = 8").len(), 1);
    assert_eq!(
        query(&catalog, "SELECT id, name FROM staff WHERE id = 8"),
        vec![vec!["8".to_string(), "'a_much_longer_name'".to_string()]]
    );
    assert_eq!(
        query(&catalog, "SELECT COUNT(*) FROM staff"),
        vec![vec!["50".to_string()]]
    );
}

#[test]
fn in_place_update_of_an_indexed_key_moves_the_index_entry() {
    let _ws = common::TestWorkspace::new("inplace", "key_change");
    let catalog = setup();

    let before = pointers(&catalog, "id = 9");
    assert_eq!(update(&catalog, "id = 9", "id = 900"), 1);

    assert!(
        query(&catalog, "SELECT id FROM staff WHERE id = 9").is_empty(),
        "old key must be gone from the index"
    );
    assert_eq!(
        query(&catalog, "SELECT id, name FROM staff WHERE id = 900"),
        vec![vec!["900".to_string(), "'user_009'".to_string()]]
    );
    assert_eq!(pointers(&catalog, "id = 900"), before, "row must not move");
    assert_eq!(dead_tuples(), 0);
}

#[test]
fn repeated_in_place_updates_over_many_rows() {
    let _ws = common::TestWorkspace::new("inplace", "many");
    let catalog = setup();

    for _ in 0..3 {
        assert_eq!(
            update(&catalog, "id >= 0 AND id < 25", "salary = salary + 1"),
            25
        );
    }

    assert_eq!(dead_tuples(), 0);
    assert_eq!(
        query(&catalog, "SELECT salary FROM staff WHERE id = 24"),
        vec![vec!["1027".to_string()]]
    );
    assert_eq!(
        query(&catalog, "SELECT salary FROM staff WHERE id = 25"),
        vec![vec!["1025".to_string()]]
    );
    assert_eq!(
        query(&catalog, "SELECT COUNT(*) FROM staff"),
        vec![vec!["50".to_string()]]
    );
}

#[test]
fn set_accepts_any_select_expression() {
    let _ws = common::TestWorkspace::new("inplace", "exprs");
    let catalog = setup();

    assert_eq!(
        update(
            &catalog,
            "id = 4",
            "name = UPPER(name) || '!', salary = CASE WHEN salary > 1000 THEN salary * 2 ELSE 0 END"
        ),
        1
    );
    assert_eq!(
        query(&catalog, "SELECT name, salary FROM staff WHERE id = 4"),
        vec![vec!["'USER_004!'".to_string(), "2008".to_string()]]
    );
}

#[test]
fn set_expressions_read_the_row_before_the_update() {
    let _ws = common::TestWorkspace::new("inplace", "swap");
    let catalog = setup();

    // Both right-hand sides see the old values, so this swaps the columns.
    assert_eq!(update(&catalog, "id = 5", "id = salary, salary = id"), 1);
    assert_eq!(
        query(&catalog, "SELECT id, salary FROM staff WHERE salary = 5"),
        vec![vec!["1005".to_string(), "5".to_string()]]
    );
}

#[test]
fn set_keeps_text_exactly_as_given() {
    let _ws = common::TestWorkspace::new("inplace", "text");
    let catalog = setup();

    // Leading/trailing spaces and enclosing quotes are part of the value.
    assert_eq!(update(&catalog, "id = 3", "name = '  ''x''  '"), 1);
    assert_eq!(
        query(&catalog, "SELECT LENGTH(name) FROM staff WHERE id = 3"),
        vec![vec!["7".to_string()]]
    );
}
