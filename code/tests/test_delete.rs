//! Comprehensive tests for DELETE functionality.
//!
//! Covers:
//!   A. parse_where_clause  — parser only, no I/O
//!      A1.  empty / whitespace-only   → None (delete all)
//!      A2.  single equality           id = 5
//!      A3.  no spaces around op       id=5
//!      A4.  extra surrounding spaces  "  id   =   5  "
//!      A5.  !=  operator
//!      A6.  < / <= / > / >= operators
//!      A7.  simple AND
//!      A8.  simple OR
//!      A9.  AND + OR left-to-right     a AND b OR c → [[a,b],[c]]
//!      A10. nested (a OR b) AND c      DNF expansion → [[a,c],[b,c]]
//!      A11. nested (a AND b) AND (c OR d) → [[a,b,c],[a,b,d]]
//!      A12. BETWEEN                   id BETWEEN 3 AND 7  →  [[id>=3,id<=7]]
//!      A13. BETWEEN no spaces         id BETWEEN 3 AND 7
//!      A14. IN list                   id IN (1, 2, 3)
//!      A15. IN no spaces around commas id IN (1,2,3)
//!      A16. IN with extra spaces      id IN ( 1 , 2 , 3 )
//!      A17. NOT IN
//!      A18. LIKE  %pattern%
//!      A19. LIKE  prefix%
//!      A20. LIKE  _single_char
//!      A21. NOT LIKE
//!      A22. combined: (id > 3 AND id < 8) OR name LIKE %A%
//!
//!   B. delete_tuples  — integration tests against a real temp file
//!      B1.  delete single row by equality
//!      B2.  delete by range (id < 3)
//!      B3.  delete by NOT IN
//!      B4.  delete by IN
//!      B5.  delete by LIKE
//!      B6.  delete by NOT LIKE
//!      B7.  delete ALL rows (empty condition groups)
//!      B8.  delete already-deleted rows is idempotent (0 additional)
//!      B9.  delete by AND condition
//!      B10. delete by OR condition
//!      B11. RETURNING * collects deleted rows
//!      B12. delete by BETWEEN

use std::collections::HashMap;
use std::fs::{remove_file, OpenOptions};

use storage_manager::catalog::types::{Catalog, Column, Database, Table};
use storage_manager::executor::{
    delete_tuples, parse_where_clause, ColumnValue, Operator,
};
use storage_manager::heap::{init_table, insert_tuple};
use storage_manager::page::{PAGE_HEADER_SIZE, ITEM_ID_SIZE, SLOT_FLAG_DELETED};
use storage_manager::table::page_count;
use storage_manager::disk::read_page;
use storage_manager::page::Page;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Serialize one row (id:INT, name:TEXT-10) into raw bytes.
fn make_row(id: i32, name: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&id.to_le_bytes());
    let mut text = name.as_bytes().to_vec();
    text.resize(10, b' ');
    text.truncate(10);
    bytes.extend_from_slice(&text);
    bytes
}

/// Build a minimal in-memory Catalog with one table (id:INT, name:TEXT).
fn make_catalog(db: &str, table: &str) -> Catalog {
    let mut databases = HashMap::new();
    let mut tables = HashMap::new();
    tables.insert(
        table.to_string(),
        Table {
            columns: vec![
                Column { name: "id".into(),   data_type: "INT".into()  },
                Column { name: "name".into(), data_type: "TEXT".into() },
            ],
        },
    );
    databases.insert(db.to_string(), Database { tables });
    Catalog { databases }
}

/// Create a temp table file with `n` rows (id = 1..=n, name = "row_NN"),
/// return a handle to it (read+write).
fn setup_table(path: &str, n: u32) -> std::fs::File {
    let _ = remove_file(path);
    let mut file = OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(path)
        .expect("setup_table: open failed");

    init_table(&mut file).expect("setup_table: init_table failed");

    for i in 1..=n {
        let name = format!("row_{:02}", i);
        let row = make_row(i as i32, &name);
        insert_tuple(&mut file, &row).expect("setup_table: insert_tuple failed");
    }
    file
}

/// Count live (non-deleted) tuples across all data pages.
fn count_live(file: &mut std::fs::File) -> usize {
    let total = page_count(file).unwrap();
    let mut live = 0usize;
    for p in 1..total {
        let mut page = Page::new();
        read_page(file, &mut page, p).unwrap();
        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let n = ((lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE) as usize;
        for i in 0..n {
            let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
            let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());
            if flags & SLOT_FLAG_DELETED == 0 {
                live += 1;
            }
        }
    }
    live
}

// ===========================================================================
// A. parse_where_clause  —  parser-only tests
// ===========================================================================

// A1 – empty string → None  (means DELETE ALL)
#[test]
fn parse_empty_is_none() {
    assert!(parse_where_clause("").is_none());
    assert!(parse_where_clause("   ").is_none());
    assert!(parse_where_clause("\t\n").is_none());
}

// A2 – simple equality with spaces
#[test]
fn parse_single_eq_spaced() {
    let groups = parse_where_clause("id = 5").unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].len(), 1);
    assert!(matches!(groups[0][0].operator, Operator::Eq));
    assert_eq!(groups[0][0].column, "id");
    assert!(matches!(groups[0][0].value, ColumnValue::Int(5)));
}

// A3 – no spaces: id=5
#[test]
fn parse_single_eq_no_spaces() {
    let groups = parse_where_clause("id=5").unwrap();
    assert_eq!(groups.len(), 1);
    assert!(matches!(groups[0][0].value, ColumnValue::Int(5)));
}

// A4 – extra surrounding spaces
#[test]
fn parse_single_eq_extra_spaces() {
    let groups = parse_where_clause("  id   =   5  ").unwrap();
    assert_eq!(groups[0][0].column, "id");
    assert!(matches!(groups[0][0].value, ColumnValue::Int(5)));
}

// A5 – != operator
#[test]
fn parse_ne_operator() {
    let groups = parse_where_clause("id != 3").unwrap();
    assert!(matches!(groups[0][0].operator, Operator::Ne));
}

// A6 – comparison operators
#[test]
fn parse_comparison_operators() {
    for (expr, expected_op) in &[
        ("id < 3",  Operator::Lt),
        ("id <= 3", Operator::Le),
        ("id > 3",  Operator::Gt),
        ("id >= 3", Operator::Ge),
    ] {
        let groups = parse_where_clause(expr).unwrap();
        assert!(
            std::mem::discriminant(&groups[0][0].operator) == std::mem::discriminant(expected_op),
            "operator mismatch for: {}", expr
        );
    }
}

// A7 – simple AND → one group with two conditions
#[test]
fn parse_and_two_conds() {
    let groups = parse_where_clause("id > 3 AND id < 8").unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].len(), 2);
    assert!(matches!(groups[0][0].operator, Operator::Gt));
    assert!(matches!(groups[0][1].operator, Operator::Lt));
}

// A8 – simple OR → two groups each with one condition
#[test]
fn parse_or_two_conds() {
    let groups = parse_where_clause("id = 1 OR id = 10").unwrap();
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].len(), 1);
    assert_eq!(groups[1].len(), 1);
    assert!(matches!(groups[0][0].value, ColumnValue::Int(1)));
    assert!(matches!(groups[1][0].value, ColumnValue::Int(10)));
}

// A9 – a AND b OR c → [[a,b],[c]]
#[test]
fn parse_and_then_or() {
    let groups = parse_where_clause("id > 1 AND id < 5 OR id = 9").unwrap();
    // group 0: [id>1, id<5]   group 1: [id=9]
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].len(), 2);
    assert_eq!(groups[1].len(), 1);
    assert!(matches!(groups[1][0].value, ColumnValue::Int(9)));
}

// A10 – (a OR b) AND c  DNF → [[a,c],[b,c]]
#[test]
fn parse_nested_or_and() {
    let groups = parse_where_clause("(id = 1 OR id = 2) AND id < 10").unwrap();
    assert_eq!(groups.len(), 2, "expected 2 DNF groups");
    // Both groups must contain a condition with id < 10
    for g in &groups {
        assert!(g.iter().any(|c| matches!(c.operator, Operator::Lt)));
    }
    // Group 1 has id=1, group 2 has id=2
    assert!(groups[0].iter().any(|c| matches!(&c.value, ColumnValue::Int(1))));
    assert!(groups[1].iter().any(|c| matches!(&c.value, ColumnValue::Int(2))));
}

// A11 – (a AND b) AND (c OR d)  DNF → [[a,b,c],[a,b,d]]
#[test]
fn parse_nested_and_of_or() {
    let groups = parse_where_clause("(id >= 3 AND id <= 9) AND (id = 3 OR id = 9)").unwrap();
    assert_eq!(groups.len(), 2);
    for g in &groups {
        assert!(g.iter().any(|c| matches!(c.operator, Operator::Ge)));
        assert!(g.iter().any(|c| matches!(c.operator, Operator::Le)));
    }
}

// A12 – BETWEEN expands to two conditions
#[test]
fn parse_between() {
    let groups = parse_where_clause("id BETWEEN 3 AND 7").unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].len(), 2, "BETWEEN should produce two conditions");
    let ops: Vec<_> = groups[0].iter().map(|c| &c.operator).collect();
    assert!(ops.iter().any(|o| matches!(o, Operator::Ge)));
    assert!(ops.iter().any(|o| matches!(o, Operator::Le)));
    let vals: Vec<i32> = groups[0].iter().filter_map(|c| {
        if let ColumnValue::Int(n) = &c.value { Some(*n) } else { None }
    }).collect();
    assert!(vals.contains(&3));
    assert!(vals.contains(&7));
}

// A13 – BETWEEN no spaces around numbers
#[test]
fn parse_between_no_spaces() {
    let groups = parse_where_clause("id BETWEEN 3 AND 7").unwrap();
    assert_eq!(groups[0].len(), 2);
}

// A14 – IN list with spaces
#[test]
fn parse_in_spaced() {
    let groups = parse_where_clause("id IN (1, 2, 3)").unwrap();
    assert_eq!(groups.len(), 1);
    assert!(matches!(groups[0][0].operator, Operator::In));
    if let ColumnValue::List(items) = &groups[0][0].value {
        assert_eq!(items.len(), 3);
        assert!(matches!(items[0], ColumnValue::Int(1)));
        assert!(matches!(items[1], ColumnValue::Int(2)));
        assert!(matches!(items[2], ColumnValue::Int(3)));
    } else {
        panic!("expected List value");
    }
}

// A15 – IN no spaces around commas
#[test]
fn parse_in_no_spaces() {
    let groups = parse_where_clause("id IN (1,2,3)").unwrap();
    assert!(matches!(groups[0][0].operator, Operator::In));
    if let ColumnValue::List(items) = &groups[0][0].value {
        assert_eq!(items.len(), 3);
    } else { panic!("expected List"); }
}

// A16 – IN with extra spaces inside parens
#[test]
fn parse_in_extra_spaces() {
    let groups = parse_where_clause("id IN ( 1 , 2 , 3 )").unwrap();
    if let ColumnValue::List(items) = &groups[0][0].value {
        assert_eq!(items.len(), 3);
    } else { panic!("expected List"); }
}

// A17 – NOT IN
#[test]
fn parse_not_in() {
    let groups = parse_where_clause("id NOT IN (1, 2)").unwrap();
    assert!(matches!(groups[0][0].operator, Operator::NotIn));
    if let ColumnValue::List(items) = &groups[0][0].value {
        assert_eq!(items.len(), 2);
    } else { panic!("expected List"); }
}

// A18 – LIKE %pattern%
#[test]
fn parse_like_both_wildcards() {
    let groups = parse_where_clause("name LIKE %hello%").unwrap();
    assert!(matches!(groups[0][0].operator, Operator::Like));
    assert_eq!(groups[0][0].column, "name");
    if let ColumnValue::Text(p) = &groups[0][0].value {
        assert!(p.contains("hello"));
    } else { panic!("expected Text"); }
}

// A19 – LIKE prefix%
#[test]
fn parse_like_prefix() {
    let groups = parse_where_clause("name LIKE row%").unwrap();
    assert!(matches!(groups[0][0].operator, Operator::Like));
}

// A20 – LIKE with _ (single char wildcard)
#[test]
fn parse_like_single_char() {
    let groups = parse_where_clause("name LIKE r_w_01").unwrap();
    assert!(matches!(groups[0][0].operator, Operator::Like));
}

// A21 – NOT LIKE
#[test]
fn parse_not_like() {
    let groups = parse_where_clause("name NOT LIKE %X%").unwrap();
    assert!(matches!(groups[0][0].operator, Operator::NotLike));
}

// A22 – combined: (id > 3 AND id < 8) OR name LIKE %A%
#[test]
fn parse_combined_range_or_like() {
    let groups = parse_where_clause("(id > 3 AND id < 8) OR name LIKE %A%").unwrap();
    assert_eq!(groups.len(), 2);
    assert_eq!(groups[0].len(), 2); // id>3 AND id<8
    assert_eq!(groups[1].len(), 1); // name LIKE %A%
    assert!(matches!(groups[1][0].operator, Operator::Like));
}

// ===========================================================================
// B. delete_tuples  —  integration tests
// ===========================================================================

const DB: &str = "_testdb_delete";
const TBL: &str = "_tbl_delete";

fn tmp_path(suffix: &str) -> String {
    format!("test_delete_{}.bin", suffix)
}

// B1 – delete single row by equality
#[test]
fn delete_single_by_eq() {
    let path = tmp_path("b1");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id = 5").unwrap();

    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 1);
    assert_eq!(count_live(&mut file), 9);
    let _ = remove_file(&path);
}

// B2 – delete by range  id < 3  → deletes 1, 2
#[test]
fn delete_by_range_lt() {
    let path = tmp_path("b2");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id < 3").unwrap();

    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 2);
    assert_eq!(count_live(&mut file), 8);
    let _ = remove_file(&path);
}

// B3 – delete by NOT IN
#[test]
fn delete_by_not_in() {
    let path = tmp_path("b3");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id NOT IN (1, 2)").unwrap();

    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 3); // 3, 4, 5
    assert_eq!(count_live(&mut file), 2);
    let _ = remove_file(&path);
}

// B4 – delete by IN
#[test]
fn delete_by_in() {
    let path = tmp_path("b4");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id IN (2, 4, 6, 8, 10)").unwrap();

    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 5);
    assert_eq!(count_live(&mut file), 5);
    let _ = remove_file(&path);
}

// B5 – delete by LIKE  (rows with name matching %row_0%)
#[test]
fn delete_by_like() {
    let path = tmp_path("b5");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    // rows 1-9 have names "row_01".."row_09" (contain "row_0")
    let groups = parse_where_clause("name LIKE %row_0%").unwrap();

    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 9); // row_01..row_09
    assert_eq!(count_live(&mut file), 1); // only row_10
    let _ = remove_file(&path);
}

// B6 – delete by NOT LIKE  (delete the one row that does NOT match %row_0%)
#[test]
fn delete_by_not_like() {
    let path = tmp_path("b6");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("name NOT LIKE %row_0%").unwrap();

    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 1); // only row_10 doesn't match
    assert_eq!(count_live(&mut file), 9);
    let _ = remove_file(&path);
}

// B7 – delete ALL rows (empty condition_groups Vec)
#[test]
fn delete_all_rows() {
    let path = tmp_path("b7");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);

    let result = delete_tuples(&catalog, DB, TBL, &mut file, &[], false).unwrap();
    assert_eq!(result.deleted_count, 10);
    assert_eq!(count_live(&mut file), 0);
    let _ = remove_file(&path);
}

// B8 – deleting already-deleted rows does NOT double-count
#[test]
fn delete_already_deleted_is_idempotent() {
    let path = tmp_path("b8");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id = 3").unwrap();

    let r1 = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(r1.deleted_count, 1);

    let r2 = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(r2.deleted_count, 0, "second delete must find 0 rows (row already flagged)");
    let _ = remove_file(&path);
}

// B9 – delete by AND condition  id >= 4 AND id <= 6  → 3 rows
#[test]
fn delete_by_and_range() {
    let path = tmp_path("b9");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id >= 4 AND id <= 6").unwrap();

    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 3);
    assert_eq!(count_live(&mut file), 7);
    let _ = remove_file(&path);
}

// B10 – delete by OR  id = 1 OR id = 10  → 2 rows
#[test]
fn delete_by_or() {
    let path = tmp_path("b10");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id = 1 OR id = 10").unwrap();

    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 2);
    assert_eq!(count_live(&mut file), 8);
    let _ = remove_file(&path);
}

// B11 – RETURNING * collects the deleted rows
#[test]
fn delete_returning_star() {
    let path = tmp_path("b11");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id IN (2, 4)").unwrap();

    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, true).unwrap();
    assert_eq!(result.deleted_count, 2);
    assert_eq!(result.returning_rows.len(), 2);
    // returned rows should have id=2 and id=4
    let ids: Vec<&str> = result.returning_rows.iter()
        .flat_map(|row| row.iter())
        .filter(|(k, _)| k == "id")
        .map(|(_, v)| v.as_str())
        .collect();
    assert!(ids.contains(&"2"));
    assert!(ids.contains(&"4"));
    let _ = remove_file(&path);
}

// B12 – delete by BETWEEN  id BETWEEN 3 AND 7  → 5 rows
#[test]
fn delete_by_between() {
    let path = tmp_path("b12");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id BETWEEN 3 AND 7").unwrap();

    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 5);
    assert_eq!(count_live(&mut file), 5);
    let _ = remove_file(&path);
}

// ===========================================================================
// C. TEXT-type operator tests
// ===========================================================================

// C1 – TEXT equality  name = 'row_05'
#[test]
fn delete_text_eq() {
    let path = tmp_path("c1");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("name = row_05").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 1);
    let _ = remove_file(&path);
}

// C2 – TEXT inequality  name != row_05  → deletes 9 rows
#[test]
fn delete_text_ne() {
    let path = tmp_path("c2");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("name != row_05").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 9);
    let _ = remove_file(&path);
}

// C3 – TEXT greater-than (lexicographic): name > row_05
//       row_06..row_10 > row_05 → 5 rows (row_06,row_07,row_08,row_09,row_10)
#[test]
fn delete_text_gt() {
    let path = tmp_path("c3");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("name > row_05").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    // row_06,row_07,row_08,row_09,row_10 all > "row_05" lexicographically
    assert_eq!(result.deleted_count, 5);
    let _ = remove_file(&path);
}

// C4 – TEXT less-than (lexicographic): name < row_05
//       row_01..row_04 < row_05 → 4 rows
#[test]
fn delete_text_lt() {
    let path = tmp_path("c4");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("name < row_05").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 4); // row_01,row_02,row_03,row_04
    let _ = remove_file(&path);
}

// C5 – TEXT >=  name >= row_08  → row_08,row_09,row_10 = 3 rows
#[test]
fn delete_text_ge() {
    let path = tmp_path("c5");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("name >= row_08").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 3);
    let _ = remove_file(&path);
}

// C6 – TEXT <=  name <= row_03  → row_01,row_02,row_03 = 3 rows
#[test]
fn delete_text_le() {
    let path = tmp_path("c6");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("name <= row_03").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 3);
    let _ = remove_file(&path);
}

// C7 – TEXT BETWEEN (lexicographic): name BETWEEN row_03 AND row_07 → 5 rows
#[test]
fn delete_text_between() {
    let path = tmp_path("c7");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    // row_03,row_04,row_05,row_06,row_07  (row_0X strings are all between row_03 and row_07)
    let groups = parse_where_clause("name BETWEEN row_03 AND row_07").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 5);
    let _ = remove_file(&path);
}

// C8 – TEXT IN list
#[test]
fn delete_text_in() {
    let path = tmp_path("c8");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("name IN (row_01, row_03, row_05)").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 3);
    let _ = remove_file(&path);
}

// C9 – TEXT NOT IN list  → deletes all except the listed names
#[test]
fn delete_text_not_in() {
    let path = tmp_path("c9");
    let mut file = setup_table(&path, 5); // row_01..row_05
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("name NOT IN (row_01, row_02)").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 3); // row_03,row_04,row_05
    let _ = remove_file(&path);
}

// C10 – TEXT case-insensitive equality
#[test]
fn delete_text_eq_case_insensitive() {
    let path = tmp_path("c10");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    // rows are stored as "row_01" etc. — uppercase comparison should still match
    let groups = parse_where_clause("name = ROW_01").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 1, "TEXT = should be case-insensitive");
    let _ = remove_file(&path);
}

// ===========================================================================
// D. LIKE edge cases & LIKE-on-INT behaviour
// ===========================================================================

// D1 – LIKE: exact match (no wildcards)
#[test]
fn delete_like_exact_no_wildcard() {
    let path = tmp_path("d1");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("name LIKE row_03").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 1);
    let _ = remove_file(&path);
}

// D2 – LIKE: trailing wildcard  row_%  → matches all 10
#[test]
fn delete_like_trailing_wildcard() {
    let path = tmp_path("d2");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("name LIKE row_%").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 10);
    let _ = remove_file(&path);
}

// D3 – LIKE: _ single-char wildcard  row_0_  → matches row_01..row_09 (9 rows)
#[test]
fn delete_like_single_char_wildcard() {
    let path = tmp_path("d3");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("name LIKE row_0_").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 9); // not row_10
    let _ = remove_file(&path);
}

// D4 – LIKE on INT column → never matches (not an error, just 0 deletions)
#[test]
fn delete_like_on_int_column_never_matches() {
    let path = tmp_path("d4");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    // 'id' is INT — LIKE on INT should never match
    let groups = parse_where_clause("id LIKE 1%").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 0, "LIKE on INT column must never match");
    assert_eq!(count_live(&mut file), 10);
    let _ = remove_file(&path);
}

// D5 – NOT LIKE on INT column → never matches either
#[test]
fn delete_not_like_on_int_column_never_matches() {
    let path = tmp_path("d5");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id NOT LIKE 1%").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 0, "NOT LIKE on INT column must never match");
    let _ = remove_file(&path);
}

// ===========================================================================
// E. Combined / nested condition edge cases
// ===========================================================================

// E1 – INT AND TEXT combined  id = 5 AND name = row_05
#[test]
fn delete_int_and_text_combined() {
    let path = tmp_path("e1");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id = 5 AND name = row_05").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 1);
    let _ = remove_file(&path);
}

// E2 – INT AND TEXT — mismatch (id=5 but name=row_99 doesn't exist) → 0
#[test]
fn delete_int_and_text_no_match() {
    let path = tmp_path("e2");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id = 5 AND name = row_99").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 0);
    let _ = remove_file(&path);
}

// E3 – INT OR TEXT  id = 1 OR name = row_10  → 2 rows
#[test]
fn delete_int_or_text() {
    let path = tmp_path("e3");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id = 1 OR name = row_10").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 2);
    let _ = remove_file(&path);
}

// E4 – deeply nested: (id >= 2 AND id <= 4) AND (name LIKE row_0% )
//      DNF → [[id>=2,id<=4,name LIKE row_0%]]
//      All of rows 2,3,4 match → 3 deletions
#[test]
fn delete_nested_and_with_like() {
    let path = tmp_path("e4");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("(id >= 2 AND id <= 4) AND name LIKE row_0%").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 3);
    let _ = remove_file(&path);
}

// E5 – (id = 1 OR id = 2) AND (name LIKE row_0%)
//      DNF → [[id=1,name LIKE row_0%],[id=2,name LIKE row_0%]]
//      Both row_01 and row_02 match → 2 deletions
#[test]
fn delete_or_expanded_with_like() {
    let path = tmp_path("e5");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("(id = 1 OR id = 2) AND name LIKE row_0%").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 2);
    let _ = remove_file(&path);
}

// E6 – triple OR  id = 1 OR id = 5 OR id = 10  → 3 rows
#[test]
fn delete_triple_or() {
    let path = tmp_path("e6");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id = 1 OR id = 5 OR id = 10").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 3);
    let _ = remove_file(&path);
}

// E7 – extra whitespace everywhere: "  id   >=   3   AND   id   <=   5  "
#[test]
fn delete_extra_whitespace_in_clause() {
    let path = tmp_path("e7");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("  id   >=   3   AND   id   <=   5  ").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 3);
    let _ = remove_file(&path);
}

// E8 – IN with mixed spacing: "id IN (1,  2,   3)"
#[test]
fn delete_in_mixed_spacing() {
    let path = tmp_path("e8");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let groups = parse_where_clause("id IN (1,  2,   3)").unwrap();
    let result = delete_tuples(&catalog, DB, TBL, &mut file, &groups, false).unwrap();
    assert_eq!(result.deleted_count, 3);
    let _ = remove_file(&path);
}
