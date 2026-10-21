//! Comprehensive tests for DELETE functionality.
//!
//! MIGRATION NOTE (legacy-executor retirement step 4): the hand-rolled DNF
//! WHERE parser (`parse_where_clause*`) was removed — WHERE strings are now
//! parsed by the real SQL grammar (`rook-parser`) and row selection runs on
//! the Volcano engine via `row_select::select_matching_pointers`, followed
//! by pointer-based deletion.
//!
//! The former section-A parser characterisation tests died with the parser
//! (the grammar is sqlparser's responsibility now). Every *semantic*
//! deletion scenario from the old suite is preserved below; two deliberate
//! behaviour notes versus the legacy matcher:
//!
//!   - String literals must be quoted (`name = 'row_05'`) — real SQL.
//!   - Text comparison is CASE-SENSITIVE (`'ROW_01'` does not match
//!     `'row_01'`). The legacy matcher folded case; standard SQL does not.
//!
//! LIKE on a non-string column evaluates to UNKNOWN (never matches), same
//! observable outcome as before.

mod common;

use std::collections::HashMap;
use std::fs::{OpenOptions, remove_file};

use common::page_count;
use storage_manager::backend::executor::delete_by_pointers;
use storage_manager::backend::executor::row_select::{parse_where_text, select_matching_pointers};
use storage_manager::catalog::types::{Catalog, Column, Database, Table};
use storage_manager::disk::read_page;
use storage_manager::heap::HeapManager;
use storage_manager::page::Page;
use storage_manager::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, SLOT_FLAG_DELETED};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const DB: &str = "_testdb_delete";
const TBL: &str = "_tbl_delete";

fn table_path() -> std::path::PathBuf {
    common::path(format!("database/base/{}/{}.dat", DB, TBL))
}

/// Build a minimal in-memory Catalog with one table (id:INT, name:TEXT).
fn make_catalog() -> Catalog {
    let mut databases = HashMap::new();
    let mut tables = HashMap::new();
    tables.insert(
        TBL.to_string(),
        Table {
            columns: vec![
                Column {
                    name: "id".into(),
                    data_type: storage_manager::types::datatype::DataType::Int,
                    nullable: false,
                    constraints: storage_manager::catalog::types::Constraints::default(),
                },
                Column {
                    name: "name".into(),
                    data_type: storage_manager::types::datatype::DataType::Varchar(10),
                    nullable: false,
                    constraints: storage_manager::catalog::types::Constraints::default(),
                },
            ],
        },
    );
    databases.insert(
        DB.to_string(),
        Database {
            tables,
            views: HashMap::new(),
        },
    );
    Catalog::with_databases(common::engine(), databases)
}

/// Create the table file at its canonical location with `n` rows
/// (id = 1..=n, name = "row_NN") inserted through the engine's own
/// type-aware writer.
fn setup_table(n: u32) {
    let path = table_path();
    std::fs::create_dir_all(common::path(format!("database/base/{}", DB))).expect("create db dir");
    let _ = remove_file(&path);
    let _ = remove_file(format!("{}.fsm", path.display()));

    HeapManager::create(std::path::PathBuf::from(&path)).expect("HeapManager::create");

    let catalog = make_catalog();
    for i in 1..=n {
        let name = format!("row_{:02}", i);
        let id_str = i.to_string();
        assert!(
            storage_manager::insert_single_tuple(&catalog, DB, TBL, &[&id_str, &name]).unwrap(),
            "setup insert failed for row {}",
            i
        );
    }

    // Flush so direct page readers below see the rows.
    let mut heap = HeapManager::open(std::path::PathBuf::from(&path)).unwrap();
    heap.flush().unwrap();
}

/// Count live (non-deleted) tuples across all data pages.
fn count_live() -> usize {
    // DELETE edits pages in the buffer pool; write them out before reading the file.
    storage_manager::backend::cache::checkpoint().unwrap();
    let mut file = OpenOptions::new().read(true).open(table_path()).unwrap();
    let total = page_count(&mut file).unwrap();
    let mut live = 0usize;
    for p in 1..total {
        let mut page = Page::new();
        read_page(&mut file, &mut page, p).unwrap();
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

/// Parse `where_text` with the real SQL grammar, select matching pointers on
/// the Volcano engine, then delete them. Empty text = DELETE ALL.
fn exec_delete(catalog: &Catalog, where_text: &str) -> storage_manager::executor::DeleteResult {
    let selection = parse_where_text(where_text).expect("parse WHERE");
    let pointers = select_matching_pointers(catalog, DB, TBL, selection).expect("select pointers");
    let result = delete_by_pointers(catalog, DB, TBL, &pointers).expect("delete_by_pointers");
    // Rows are formatted for RETURNING only when asked for.
    assert!(result.returning_rows.is_empty());
    result
}

// ===========================================================================
// B. Deletion semantics through the Volcano selection path
// ===========================================================================

#[test]
fn delete_single_by_eq() {
    let _ws = common::TestWorkspace::new("del", "case0");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "id = 5");
    assert_eq!(result.deleted_count, 1);
    assert_eq!(count_live(), 9);
}

#[test]
fn delete_by_range_lt() {
    let _ws = common::TestWorkspace::new("del", "case1");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "id < 3");
    assert_eq!(result.deleted_count, 2); // ids 1, 2
    assert_eq!(count_live(), 8);
}

#[test]
fn delete_by_not_in() {
    let _ws = common::TestWorkspace::new("del", "case2");
    setup_table(5);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "id NOT IN (1, 2)");
    assert_eq!(result.deleted_count, 3); // 3, 4, 5
    assert_eq!(count_live(), 2);
}

#[test]
fn delete_by_in() {
    let _ws = common::TestWorkspace::new("del", "case3");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "id IN (2, 4, 6, 8, 10)");
    assert_eq!(result.deleted_count, 5);
    assert_eq!(count_live(), 5);
}

#[test]
fn delete_by_like_contains() {
    let _ws = common::TestWorkspace::new("del", "case4");
    setup_table(10);
    let catalog = make_catalog();

    // rows 1-9 have names "row_01".."row_09" (contain "row_0")
    let result = exec_delete(&catalog, "name LIKE '%row_0%'");
    assert_eq!(result.deleted_count, 9); // row_01..row_09
    assert_eq!(count_live(), 1); // only row_10
}

#[test]
fn delete_by_not_like() {
    let _ws = common::TestWorkspace::new("del", "case5");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "name NOT LIKE '%row_0%'");
    assert_eq!(result.deleted_count, 1); // only row_10 doesn't match
    assert_eq!(count_live(), 9);
}

#[test]
fn delete_all_rows() {
    let _ws = common::TestWorkspace::new("del", "case6");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "");
    assert_eq!(result.deleted_count, 10);
    assert_eq!(count_live(), 0);
}

#[test]
fn delete_already_deleted_is_idempotent() {
    let _ws = common::TestWorkspace::new("del", "case7");
    setup_table(5);
    let catalog = make_catalog();

    let r1 = exec_delete(&catalog, "id = 3");
    assert_eq!(r1.deleted_count, 1);

    // Second pass re-selects first: the flagged slot is no longer live, so
    // nothing matches and nothing is double-counted.
    let r2 = exec_delete(&catalog, "id = 3");
    assert_eq!(
        r2.deleted_count, 0,
        "second delete must find 0 rows (row already flagged)"
    );
}

#[test]
fn delete_by_and_range() {
    let _ws = common::TestWorkspace::new("del", "case8");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "id >= 4 AND id <= 6");
    assert_eq!(result.deleted_count, 3);
    assert_eq!(count_live(), 7);
}

#[test]
fn delete_by_or() {
    let _ws = common::TestWorkspace::new("del", "case9");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "id = 1 OR id = 10");
    assert_eq!(result.deleted_count, 2);
    assert_eq!(count_live(), 8);
}

#[test]
fn delete_returning_star() {
    let _ws = common::TestWorkspace::new("del", "case10");
    setup_table(5);
    let catalog = make_catalog();

    let selection = parse_where_text("id IN (2, 4)").expect("parse WHERE");
    let pointers = select_matching_pointers(&catalog, DB, TBL, selection).expect("select pointers");
    let result = storage_manager::backend::executor::delete::delete_by_pointers_returning(
        &catalog, DB, TBL, &pointers,
    )
    .expect("delete_by_pointers_returning");
    assert_eq!(result.deleted_count, 2);
    assert_eq!(result.returning_rows.len(), 2);

    let ids: Vec<&str> = result
        .returning_rows
        .iter()
        .flat_map(|row| row.iter())
        .filter(|(k, _)| k == "id")
        .map(|(_, v)| v.as_str())
        .collect();
    assert!(ids.contains(&"2"));
    assert!(ids.contains(&"4"));
}

#[test]
fn delete_by_between() {
    let _ws = common::TestWorkspace::new("del", "case11");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "id BETWEEN 3 AND 7");
    assert_eq!(result.deleted_count, 5);
    assert_eq!(count_live(), 5);
}

// ===========================================================================
// C. TEXT-type operator tests (values must be quoted — real SQL)
// ===========================================================================

#[test]
fn delete_text_eq() {
    let _ws = common::TestWorkspace::new("del", "case12");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "name = 'row_05'");
    assert_eq!(result.deleted_count, 1);
}

#[test]
fn delete_text_ne() {
    let _ws = common::TestWorkspace::new("del", "case13");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "name != 'row_05'");
    assert_eq!(result.deleted_count, 9);
}

#[test]
fn delete_text_gt() {
    let _ws = common::TestWorkspace::new("del", "case14");
    setup_table(10);
    let catalog = make_catalog();

    // row_06..row_10 sort lexicographically above "row_05"
    let result = exec_delete(&catalog, "name > 'row_05'");
    assert_eq!(result.deleted_count, 5);
}

#[test]
fn delete_text_lt() {
    let _ws = common::TestWorkspace::new("del", "case15");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "name < 'row_05'");
    assert_eq!(result.deleted_count, 4); // row_01..row_04
}

#[test]
fn delete_text_ge() {
    let _ws = common::TestWorkspace::new("del", "case16");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "name >= 'row_08'");
    assert_eq!(result.deleted_count, 3);
}

#[test]
fn delete_text_le() {
    let _ws = common::TestWorkspace::new("del", "case17");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "name <= 'row_03'");
    assert_eq!(result.deleted_count, 3);
}

#[test]
fn delete_text_between() {
    let _ws = common::TestWorkspace::new("del", "case18");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "name BETWEEN 'row_03' AND 'row_07'");
    assert_eq!(result.deleted_count, 5);
}

#[test]
fn delete_text_in() {
    let _ws = common::TestWorkspace::new("del", "case19");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "name IN ('row_01', 'row_03', 'row_05')");
    assert_eq!(result.deleted_count, 3);
}

#[test]
fn delete_text_not_in() {
    let _ws = common::TestWorkspace::new("del", "case20");
    setup_table(5); // row_01..row_05
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "name NOT IN ('row_01', 'row_02')");
    assert_eq!(result.deleted_count, 3); // row_03..row_05
}

#[test]
fn delete_text_eq_case_sensitivity() {
    let _ws = common::TestWorkspace::new("del", "case21");
    setup_table(5);
    let catalog = make_catalog();

    // BEHAVIOUR CHANGE vs the legacy matcher: text comparison follows SQL
    // and is case-sensitive — 'ROW_01' is a different string from 'row_01'.
    let result = exec_delete(&catalog, "name = 'ROW_01'");
    assert_eq!(
        result.deleted_count, 0,
        "text comparison must be case-sensitive"
    );
    assert_eq!(count_live(), 5);

    let result = exec_delete(&catalog, "name = 'row_01'");
    assert_eq!(result.deleted_count, 1);
}

// ===========================================================================
// D. LIKE edge cases & LIKE-on-INT behaviour
// ===========================================================================

#[test]
fn delete_like_exact_no_wildcard() {
    let _ws = common::TestWorkspace::new("del", "case22");
    setup_table(5);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "name LIKE 'row_03'");
    assert_eq!(result.deleted_count, 1);
}

#[test]
fn delete_like_trailing_wildcard() {
    let _ws = common::TestWorkspace::new("del", "case23");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "name LIKE 'row_%'");
    assert_eq!(result.deleted_count, 10);
}

#[test]
fn delete_like_single_char_wildcard() {
    let _ws = common::TestWorkspace::new("del", "case24");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "name LIKE 'row_0_'");
    assert_eq!(result.deleted_count, 9); // not row_10
}

#[test]
fn delete_like_on_int_column_is_a_type_error() {
    let _ws = common::TestWorkspace::new("del", "case25");
    setup_table(10);
    let catalog = make_catalog();

    // No implicit casts: LIKE / NOT LIKE on an INT column is an error and
    // deletes nothing, rather than a predicate that never matches.
    for where_text in ["id LIKE '1%'", "id NOT LIKE '1%'"] {
        let selection = parse_where_text(where_text).expect("parse WHERE");
        let err = select_matching_pointers(&catalog, DB, TBL, selection)
            .expect_err("LIKE on an INT column must fail");
        assert!(err.to_string().contains("LIKE needs a string"), "{}", err);
    }
    assert_eq!(count_live(), 10);
}

// ===========================================================================
// E. Combined / nested condition edge cases
// ===========================================================================

#[test]
fn delete_int_and_text_combined() {
    let _ws = common::TestWorkspace::new("del", "case27");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "id = 5 AND name = 'row_05'");
    assert_eq!(result.deleted_count, 1);
}

#[test]
fn delete_int_and_text_no_match() {
    let _ws = common::TestWorkspace::new("del", "case28");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "id = 5 AND name = 'row_99'");
    assert_eq!(result.deleted_count, 0);
}

#[test]
fn delete_int_or_text() {
    let _ws = common::TestWorkspace::new("del", "case29");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "id = 1 OR name = 'row_10'");
    assert_eq!(result.deleted_count, 2);
}

#[test]
fn delete_nested_and_with_like() {
    let _ws = common::TestWorkspace::new("del", "case30");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "(id >= 2 AND id <= 4) AND name LIKE 'row_0%'");
    assert_eq!(result.deleted_count, 3);
}

#[test]
fn delete_or_expanded_with_like() {
    let _ws = common::TestWorkspace::new("del", "case31");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "(id = 1 OR id = 2) AND name LIKE 'row_0%'");
    assert_eq!(result.deleted_count, 2);
}

#[test]
fn delete_triple_or() {
    let _ws = common::TestWorkspace::new("del", "case32");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "id = 1 OR id = 5 OR id = 10");
    assert_eq!(result.deleted_count, 3);
}

#[test]
fn delete_extra_whitespace_in_clause() {
    let _ws = common::TestWorkspace::new("del", "case33");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "  id   >=   3   AND   id   <=   5  ");
    assert_eq!(result.deleted_count, 3);
}

#[test]
fn delete_in_mixed_spacing() {
    let _ws = common::TestWorkspace::new("del", "case34");
    setup_table(10);
    let catalog = make_catalog();

    let result = exec_delete(&catalog, "id IN (1,  2,   3)");
    assert_eq!(result.deleted_count, 3);
}
