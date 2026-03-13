//! Comprehensive tests for UPDATE functionality.
//!
//! Covers:
//!   A. parse_set_clause  — parser only, no I/O
//!      A1.  empty string               → None
//!      A2.  whitespace only            → None
//!      A3.  literal int                id = 5
//!      A4.  literal int no spaces      id=5
//!      A5.  literal text quoted        name = 'Alice'
//!      A6.  literal text unquoted      name = Alice
//!      A7.  arithmetic add + spaces    id = id + 1
//!      A8.  arithmetic add no spaces   id=id+1
//!      A9.  arithmetic subtract        id = id - 3
//!      A10. arithmetic multiply int    id = id * 2
//!      A11. arithmetic multiply float  salary = salary * 1.10
//!      A12. arithmetic divide          id = id / 2
//!      A13. multiple assignments       id = 5, name = 'Bob'
//!      A14. multiple with arithmetic   id = id + 10, name = 'X'
//!      A15. extra surrounding spaces   "  id  =  5  "
//!
//!   B. update_tuples  — integration tests against a real temp file
//!      B1.  literal int update, single row
//!      B2.  literal text update, single row
//!      B3.  literal update, no matching rows (0 updates)
//!      B4.  update all rows (empty condition groups)
//!      B5.  arithmetic add  id = id + 100
//!      B6.  arithmetic subtract  id = id - 1
//!      B7.  arithmetic multiply  id = id * 2
//!      B8.  arithmetic divide id = id / 2
//!      B9.  multi-column update  id = 99, name = 'XYZ'
//!      B10. update with WHERE AND range
//!      B11. update with WHERE IN
//!      B12. update with WHERE BETWEEN
//!      B13. update with WHERE LIKE
//!      B14. update with WHERE OR
//!      B15. RETURNING * shows updated rows (after values)
//!      B16. update already-deleted rows → 0 (skipped)

use std::collections::HashMap;
use std::fs::{remove_file, OpenOptions};
use std::io::{Read, Seek, SeekFrom};

use storage_manager::catalog::types::{Catalog, Column, Database, Table};
use storage_manager::executor::{
    delete_tuples, update_tuples, parse_where_clause, parse_set_clause,
    SetExpr, ArithOp, ColumnValue,
};
use storage_manager::heap::{init_table, insert_tuple};
use storage_manager::page::{Page, PAGE_HEADER_SIZE, ITEM_ID_SIZE, SLOT_FLAG_DELETED};
use storage_manager::disk::read_page;
use storage_manager::table::page_count;

// ---------------------------------------------------------------------------
// Helpers (same structure as test_delete.rs)
// ---------------------------------------------------------------------------

fn make_row(id: i32, name: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&id.to_le_bytes());
    let mut text = name.as_bytes().to_vec();
    text.resize(10, b' ');
    text.truncate(10);
    bytes.extend_from_slice(&text);
    bytes
}

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

fn setup_table(path: &str, n: u32) -> std::fs::File {
    let _ = remove_file(path);
    let mut file = OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(path)
        .expect("setup_table: open failed");
    init_table(&mut file).expect("setup_table: init_table failed");
    for i in 1..=n {
        let name = format!("row_{:02}", i);
        insert_tuple(&mut file, &make_row(i as i32, &name))
            .expect("setup_table: insert failed");
    }
    file
}

/// Read the id values of all live rows (for verification after update).
fn live_ids(file: &mut std::fs::File) -> Vec<i32> {
    let total = page_count(file).unwrap();
    let mut ids = Vec::new();
    for p in 1..total {
        let mut page = Page::new();
        read_page(file, &mut page, p).unwrap();
        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let n = ((lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE) as usize;
        for i in 0..n {
            let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
            let flags  = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());
            if flags & SLOT_FLAG_DELETED != 0 { continue; }
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
            let id = i32::from_le_bytes(page.data[offset..offset + 4].try_into().unwrap());
            ids.push(id);
        }
    }
    ids
}

const DB: &str = "_testdb_update";
const TBL: &str = "_tbl_update";

fn tmp(suffix: &str) -> String { format!("test_update_{}.bin", suffix) }

// ===========================================================================
// A. parse_set_clause — parser tests
// ===========================================================================

// A1/A2 – empty → None
#[test]
fn set_parse_empty_is_none() {
    assert!(parse_set_clause("").is_none());
    assert!(parse_set_clause("   ").is_none());
}

// A3 – literal int with spaces
#[test]
fn set_parse_literal_int() {
    let asgns = parse_set_clause("id = 5").unwrap();
    assert_eq!(asgns.len(), 1);
    assert_eq!(asgns[0].column, "id");
    assert!(matches!(&asgns[0].expr, SetExpr::Literal(ColumnValue::Int(5))));
}

// A4 – literal int no spaces
#[test]
fn set_parse_literal_int_no_spaces() {
    let asgns = parse_set_clause("id=5").unwrap();
    assert!(matches!(&asgns[0].expr, SetExpr::Literal(ColumnValue::Int(5))));
}

// A5 – literal text quoted
#[test]
fn set_parse_literal_text_quoted() {
    let asgns = parse_set_clause("name = 'Alice'").unwrap();
    assert_eq!(asgns[0].column, "name");
    if let SetExpr::Literal(ColumnValue::Text(s)) = &asgns[0].expr {
        assert_eq!(s, "Alice");
    } else { panic!("expected Literal Text"); }
}

// A6 – literal text unquoted
#[test]
fn set_parse_literal_text_unquoted() {
    let asgns = parse_set_clause("name = Alice").unwrap();
    if let SetExpr::Literal(ColumnValue::Text(s)) = &asgns[0].expr {
        assert_eq!(s, "Alice");
    } else { panic!("expected Literal Text"); }
}

// A7 – arithmetic add with spaces
#[test]
fn set_parse_arith_add_spaced() {
    let asgns = parse_set_clause("id = id + 1").unwrap();
    match &asgns[0].expr {
        SetExpr::Expr { src_col, op, rhs_i, .. } => {
            assert_eq!(src_col, "id");
            assert!(matches!(op, ArithOp::Add));
            assert_eq!(*rhs_i, 1);
        }
        _ => panic!("expected Expr"),
    }
}

// A8 – arithmetic add no spaces
#[test]
fn set_parse_arith_add_no_spaces() {
    let asgns = parse_set_clause("id=id+1").unwrap();
    assert!(matches!(&asgns[0].expr, SetExpr::Expr { op: ArithOp::Add, .. }));
}

// A9 – arithmetic subtract
#[test]
fn set_parse_arith_sub() {
    let asgns = parse_set_clause("id = id - 3").unwrap();
    match &asgns[0].expr {
        SetExpr::Expr { op, rhs_i, .. } => {
            assert!(matches!(op, ArithOp::Sub));
            assert_eq!(*rhs_i, 3);
        }
        _ => panic!("expected Expr"),
    }
}

// A10 – arithmetic multiply int
#[test]
fn set_parse_arith_mul_int() {
    let asgns = parse_set_clause("id = id * 2").unwrap();
    assert!(matches!(&asgns[0].expr, SetExpr::Expr { op: ArithOp::Mul, .. }));
}

// A11 – arithmetic multiply float
#[test]
fn set_parse_arith_mul_float() {
    let asgns = parse_set_clause("id = id * 1.10").unwrap();
    match &asgns[0].expr {
        SetExpr::Expr { op, rhs_f, .. } => {
            assert!(matches!(op, ArithOp::Mul));
            assert!((rhs_f - 1.10).abs() < 1e-9);
        }
        _ => panic!("expected Expr"),
    }
}

// A12 – arithmetic divide
#[test]
fn set_parse_arith_div() {
    let asgns = parse_set_clause("id = id / 2").unwrap();
    assert!(matches!(&asgns[0].expr, SetExpr::Expr { op: ArithOp::Div, .. }));
}

// A13 – multiple literal assignments
#[test]
fn set_parse_multiple_literals() {
    let asgns = parse_set_clause("id = 5, name = 'Bob'").unwrap();
    assert_eq!(asgns.len(), 2);
    assert_eq!(asgns[0].column, "id");
    assert_eq!(asgns[1].column, "name");
    assert!(matches!(&asgns[0].expr, SetExpr::Literal(ColumnValue::Int(5))));
    if let SetExpr::Literal(ColumnValue::Text(s)) = &asgns[1].expr {
        assert_eq!(s, "Bob");
    } else { panic!("expected Text"); }
}

// A14 – multiple: one arithmetic, one literal
#[test]
fn set_parse_multiple_mixed() {
    let asgns = parse_set_clause("id = id + 10, name = 'X'").unwrap();
    assert_eq!(asgns.len(), 2);
    assert!(matches!(&asgns[0].expr, SetExpr::Expr { op: ArithOp::Add, .. }));
    assert!(matches!(&asgns[1].expr, SetExpr::Literal(ColumnValue::Text(_))));
}

// A15 – extra surrounding spaces
#[test]
fn set_parse_extra_spaces() {
    let asgns = parse_set_clause("  id  =  5  ").unwrap();
    assert!(matches!(&asgns[0].expr, SetExpr::Literal(ColumnValue::Int(5))));
}

// ===========================================================================
// B. update_tuples — integration tests
// ===========================================================================

// B1 – literal int update, single row by id
#[test]
fn update_literal_int_single() {
    let path = tmp("b1");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("id = 99").unwrap();
    let groups = parse_where_clause("id = 3").unwrap();

    let result = update_tuples(&catalog, DB, TBL, &mut file, &assignments, &groups, false).unwrap();
    assert_eq!(result.updated_count, 1);

    let ids = live_ids(&mut file);
    assert!(ids.contains(&99), "id should have been updated to 99");
    assert!(!ids.contains(&3), "original id=3 should be gone");
    let _ = remove_file(&path);
}

// B2 – literal text update
#[test]
fn update_literal_text_single() {
    let path = tmp("b2");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("name = Updated").unwrap();
    let groups = parse_where_clause("id = 2").unwrap();

    let result = update_tuples(&catalog, DB, TBL, &mut file, &assignments, &groups, true).unwrap();
    assert_eq!(result.updated_count, 1);
    // RETURNING row should contain name = "Updated"
    let updated_name = result.returning_rows[0].iter()
        .find(|(k, _)| k == "name").map(|(_, v)| v.as_str()).unwrap_or("");
    assert!(updated_name.trim() == "Updated", "name mismatch: '{}'", updated_name);
    let _ = remove_file(&path);
}

// B3 – no matching rows → 0 updates
#[test]
fn update_no_match_is_zero() {
    let path = tmp("b3");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("id = 99").unwrap();
    let groups = parse_where_clause("id = 99").unwrap(); // doesn't exist

    let result = update_tuples(&catalog, DB, TBL, &mut file, &assignments, &groups, false).unwrap();
    assert_eq!(result.updated_count, 0);
    let _ = remove_file(&path);
}

// B4 – update ALL rows (empty condition groups)
#[test]
fn update_all_rows() {
    let path = tmp("b4");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("id = 0").unwrap();

    let result = update_tuples(&catalog, DB, TBL, &mut file, &assignments, &[], false).unwrap();
    assert_eq!(result.updated_count, 5);

    let ids = live_ids(&mut file);
    assert!(ids.iter().all(|&id| id == 0), "all ids should be 0 now");
    let _ = remove_file(&path);
}

// B5 – arithmetic add  id = id + 100
#[test]
fn update_arith_add() {
    let path = tmp("b5");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("id = id + 100").unwrap();
    let groups = parse_where_clause("id < 4").unwrap(); // rows 1, 2, 3

    let result = update_tuples(&catalog, DB, TBL, &mut file, &assignments, &groups, false).unwrap();
    assert_eq!(result.updated_count, 3);

    let ids = live_ids(&mut file);
    // rows 1, 2, 3 become 101, 102, 103
    assert!(ids.contains(&101));
    assert!(ids.contains(&102));
    assert!(ids.contains(&103));
    // rows 4, 5 unchanged
    assert!(ids.contains(&4));
    assert!(ids.contains(&5));
    let _ = remove_file(&path);
}

// B6 – arithmetic subtract id = id - 1
#[test]
fn update_arith_sub() {
    let path = tmp("b6");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("id = id - 1").unwrap();

    let result = update_tuples(&catalog, DB, TBL, &mut file, &assignments, &[], false).unwrap();
    assert_eq!(result.updated_count, 5);

    let ids = live_ids(&mut file);
    // 1→0, 2→1, 3→2, 4→3, 5→4
    assert!(ids.contains(&0));
    assert!(ids.contains(&4));
    assert!(!ids.contains(&5));
    let _ = remove_file(&path);
}

// B7 – arithmetic multiply  id = id * 2
#[test]
fn update_arith_mul() {
    let path = tmp("b7");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("id = id * 2").unwrap();

    update_tuples(&catalog, DB, TBL, &mut file, &assignments, &[], false).unwrap();

    let ids = live_ids(&mut file);
    // 1→2, 2→4, 3→6, 4→8, 5→10
    assert!(ids.contains(&2));
    assert!(ids.contains(&4));
    assert!(ids.contains(&10));
    assert!(!ids.contains(&1));
    let _ = remove_file(&path);
}

// B8 – arithmetic divide  id = id / 2  (integer division)
#[test]
fn update_arith_div() {
    let path = tmp("b8");
    let mut file = setup_table(&path, 6);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("id = id / 2").unwrap();

    update_tuples(&catalog, DB, TBL, &mut file, &assignments, &[], false).unwrap();

    let ids = live_ids(&mut file);
    // 2→1, 4→2, 6→3
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
    let _ = remove_file(&path);
}

// B9 – update two columns at once
#[test]
fn update_multi_column() {
    let path = tmp("b9");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("id = 50, name = 'MultiUp'").unwrap();
    let groups = parse_where_clause("id = 3").unwrap();

    let result = update_tuples(&catalog, DB, TBL, &mut file, &assignments, &groups, true).unwrap();
    assert_eq!(result.updated_count, 1);

    let row = &result.returning_rows[0];
    let id_val   = row.iter().find(|(k,_)| k=="id").map(|(_,v)| v.as_str()).unwrap_or("");
    let name_val = row.iter().find(|(k,_)| k=="name").map(|(_,v)| v.as_str()).unwrap_or("");
    assert_eq!(id_val, "50");
    assert!(name_val.trim() == "MultiUp", "name was '{}'", name_val);
    let _ = remove_file(&path);
}

// B10 – update with WHERE AND range
#[test]
fn update_where_and_range() {
    let path = tmp("b10");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("id = id + 1000").unwrap();
    let groups = parse_where_clause("id >= 4 AND id <= 6").unwrap();

    let result = update_tuples(&catalog, DB, TBL, &mut file, &assignments, &groups, false).unwrap();
    assert_eq!(result.updated_count, 3);

    let ids = live_ids(&mut file);
    assert!(ids.contains(&1004));
    assert!(ids.contains(&1005));
    assert!(ids.contains(&1006));
    assert!(!ids.contains(&4));
    let _ = remove_file(&path);
}

// B11 – update with WHERE IN
#[test]
fn update_where_in() {
    let path = tmp("b11");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("id = 0").unwrap();
    let groups = parse_where_clause("id IN (2, 4, 6)").unwrap();

    let result = update_tuples(&catalog, DB, TBL, &mut file, &assignments, &groups, false).unwrap();
    assert_eq!(result.updated_count, 3);

    let ids = live_ids(&mut file);
    let zero_count = ids.iter().filter(|&&id| id == 0).count();
    assert_eq!(zero_count, 3);
    let _ = remove_file(&path);
}

// B12 – update with WHERE BETWEEN
#[test]
fn update_where_between() {
    let path = tmp("b12");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("id = id * 10").unwrap();
    let groups = parse_where_clause("id BETWEEN 3 AND 5").unwrap();

    let result = update_tuples(&catalog, DB, TBL, &mut file, &assignments, &groups, false).unwrap();
    assert_eq!(result.updated_count, 3);

    let ids = live_ids(&mut file);
    assert!(ids.contains(&30));
    assert!(ids.contains(&40));
    assert!(ids.contains(&50));
    let _ = remove_file(&path);
}

// B13 – update with WHERE LIKE
#[test]
fn update_where_like() {
    let path = tmp("b13");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("name = Matched").unwrap();
    // rows 1-9 → name like row_0X
    let groups = parse_where_clause("name LIKE %row_0%").unwrap();

    let result = update_tuples(&catalog, DB, TBL, &mut file, &assignments, &groups, false).unwrap();
    assert_eq!(result.updated_count, 9);
    let _ = remove_file(&path);
}

// B14 – update with WHERE OR
#[test]
fn update_where_or() {
    let path = tmp("b14");
    let mut file = setup_table(&path, 10);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("id = id + 500").unwrap();
    let groups = parse_where_clause("id = 1 OR id = 10").unwrap();

    let result = update_tuples(&catalog, DB, TBL, &mut file, &assignments, &groups, false).unwrap();
    assert_eq!(result.updated_count, 2);

    let ids = live_ids(&mut file);
    assert!(ids.contains(&501));
    assert!(ids.contains(&510));
    let _ = remove_file(&path);
}

// B15 – RETURNING * shows after-update values
#[test]
fn update_returning_star() {
    let path = tmp("b15");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);
    let assignments = parse_set_clause("id = 77").unwrap();
    let groups = parse_where_clause("id = 2").unwrap();

    let result = update_tuples(&catalog, DB, TBL, &mut file, &assignments, &groups, true).unwrap();
    assert_eq!(result.returning_rows.len(), 1);
    let id_after = result.returning_rows[0].iter()
        .find(|(k,_)| k == "id").map(|(_, v)| v.as_str()).unwrap_or("0");
    assert_eq!(id_after, "77");
    let _ = remove_file(&path);
}

// B16 – update skips soft-deleted rows
#[test]
fn update_skips_deleted_rows() {
    let path = tmp("b16");
    let mut file = setup_table(&path, 5);
    let catalog = make_catalog(DB, TBL);

    // First soft-delete id=3
    let del_groups = parse_where_clause("id = 3").unwrap();
    delete_tuples(&catalog, DB, TBL, &mut file, &del_groups, false).unwrap();

    // Now try to update all rows with id <= 5 (should skip the deleted one)
    let assignments = parse_set_clause("id = id + 100").unwrap();
    let upd_groups  = parse_where_clause("id <= 5").unwrap();
    let result = update_tuples(&catalog, DB, TBL, &mut file, &assignments, &upd_groups, false).unwrap();

    assert_eq!(result.updated_count, 4, "deleted row should not be updated");
    let _ = remove_file(&path);
}
