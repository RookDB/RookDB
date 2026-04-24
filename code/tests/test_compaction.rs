//! Comprehensive tests for soft-delete flags and compaction.
//!
//! Covers:
//!   C1.  DELETE sets SLOT_FLAG_DELETED bit — lower/upper unchanged
//!   C2.  show_tuples / live count drops after soft-delete
//!   C3.  compaction_table returns pages_compacted = 1
//!   C4.  lower decreases after compaction (fewer slot entries)
//!   C5.  upper increases after compaction (data bytes recovered)
//!   C6.  live count same after compaction (compaction doesn't lose rows)
//!   C7.  second compaction on same page returns 0 (idempotent)
//!   C8.  delete all + compact → lower == PAGE_HEADER_SIZE, upper == PAGE_SIZE
//!   C9.  interleaved delete-compact-insert-delete cycle
//!   C10. compaction of table where NO rows are deleted → 0 pages rewritten

use std::collections::HashMap;
use std::fs::{create_dir_all, remove_dir_all, remove_file, OpenOptions};

use storage_manager::catalog::types::{Catalog, Column, Database, Table};
use storage_manager::executor::{delete_tuples, compaction_table, parse_where_clause};
use storage_manager::heap::{init_table, insert_tuple};
use storage_manager::page::{Page, PAGE_HEADER_SIZE, ITEM_ID_SIZE, PAGE_SIZE, SLOT_FLAG_DELETED};
use storage_manager::disk::read_page;
use storage_manager::table::page_count;

// ---------------------------------------------------------------------------
// Helpers
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

/// Create the table file at the canonical path `database/base/{db}/{table}.dat`
/// (required by `compaction_table`).  Returns the open file handle.
fn setup_canonical(db: &str, table: &str, n: u32) -> std::fs::File {
    let dir  = format!("database/base/{}", db);
    let path = format!("{}/{}.dat", dir, table);
    create_dir_all(&dir).expect("create_dir_all failed");
    let _ = remove_file(&path);

    let mut file = OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(&path)
        .expect("setup_canonical: open failed");

    init_table(&mut file).expect("setup_canonical: init_table failed");
    for i in 1..=n {
        let row = make_row(i as i32, &format!("row_{:02}", i));
        insert_tuple(&mut file, &row).expect("insert_tuple failed");
    }
    file
}

fn cleanup_db(db: &str) {
    let _ = remove_dir_all(format!("database/base/{}", db));
}

/// Count slots in page 1 by their SLOT_FLAG_DELETED flag.
/// Returns (live, deleted).
fn slot_counts(file: &mut std::fs::File) -> (usize, usize) {
    let total = page_count(file).unwrap();
    let (mut live, mut dead) = (0, 0);
    for p in 1..total {
        let mut page = Page::new();
        read_page(file, &mut page, p).unwrap();
        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let n = ((lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE) as usize;
        for i in 0..n {
            let base = PAGE_HEADER_SIZE as usize + i * ITEM_ID_SIZE as usize;
            let flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());
            if flags & SLOT_FLAG_DELETED != 0 { dead += 1; } else { live += 1; }
        }
    }
    (live, dead)
}

/// Read lower/upper pointers from data page 1.
fn page1_bounds(file: &mut std::fs::File) -> (u32, u32) {
    let mut page = Page::new();
    read_page(file, &mut page, 1).unwrap();
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
    (lower, upper)
}

// ===========================================================================
// Tests
// ===========================================================================

// C1 – soft-delete sets SLOT_FLAG_DELETED, lower/upper unchanged
#[test]
fn soft_delete_sets_flag_no_layout_change() {
    let db = "_tc_c1"; let tbl = "t";
    let mut file = setup_canonical(db, tbl, 5);
    let cat = make_catalog(db, tbl);

    let (lower_before, upper_before) = page1_bounds(&mut file);
    println!("[COMPACTION] Before soft-delete: page1 lower={} upper={}", lower_before, upper_before);
    let groups = parse_where_clause("id = 3").unwrap();
    delete_tuples(&cat, db, tbl, &mut file, &groups, false).unwrap();
    let (lower_after, upper_after) = page1_bounds(&mut file);
    println!("[COMPACTION] After soft-delete:  page1 lower={} upper={}", lower_after, upper_after);
    println!("[COMPACTION] Page layout unchanged — soft-delete only sets flags, no physical movement.");

    // layout must not have changed
    assert_eq!(lower_before, lower_after, "lower changed after soft-delete");
    assert_eq!(upper_before, upper_after, "upper changed after soft-delete");

    // flag must be set
    let (live, dead) = slot_counts(&mut file);
    println!("[COMPACTION] Slot counts: live={} dead={} — row id=3 is logically invisible.", live, dead);
    assert_eq!(dead, 1);
    assert_eq!(live, 4);
    cleanup_db(db);
}

// C2 – live count drops immediately after soft-delete (flag hidden from scan)
#[test]
fn soft_delete_hides_rows_from_live_count() {
    let db = "_tc_c2"; let tbl = "t";
    let mut file = setup_canonical(db, tbl, 10);
    let cat = make_catalog(db, tbl);

    let (live_before, _) = slot_counts(&mut file);
    assert_eq!(live_before, 10);

    let groups = parse_where_clause("id IN (1, 3, 5, 7, 9)").unwrap();
    delete_tuples(&cat, db, tbl, &mut file, &groups, false).unwrap();

    let (live_after, dead_after) = slot_counts(&mut file);
    assert_eq!(live_after, 5);
    assert_eq!(dead_after, 5);
    cleanup_db(db);
}

// C3 – compaction_table returns 1 for a page with deleted rows
#[test]
fn compaction_returns_correct_page_count() {
    let db = "_tc_c3"; let tbl = "t";
    let mut file = setup_canonical(db, tbl, 5);
    let cat = make_catalog(db, tbl);

    let groups = parse_where_clause("id = 2").unwrap();
    delete_tuples(&cat, db, tbl, &mut file, &groups, false).unwrap();
    drop(file);

    let pages = compaction_table(db, tbl).unwrap();
    assert_eq!(pages, 1);
    cleanup_db(db);
}

// C4 – lower decreases after compaction (fewer slot entries)
#[test]
fn compaction_decreases_lower() {
    let db = "_tc_c4"; let tbl = "t";
    let mut file = setup_canonical(db, tbl, 6);
    let cat = make_catalog(db, tbl);

    let (lower_before, _) = page1_bounds(&mut file);
    println!("[COMPACTION] Before delete: lower={} (3 slots will be removed)", lower_before);

    let groups = parse_where_clause("id IN (1, 2, 3)").unwrap();
    delete_tuples(&cat, db, tbl, &mut file, &groups, false).unwrap();
    drop(file);

    println!("[COMPACTION] Running compaction_table — rebuilding page with only live rows ...");
    compaction_table(db, tbl).unwrap();

    let mut file2 = OpenOptions::new().read(true).write(true)
        .open(format!("database/base/{}/{}.dat", db, tbl)).unwrap();
    let (lower_after, _) = page1_bounds(&mut file2);
    println!("[COMPACTION] After compaction: lower={} (decreased by {} bytes = 3 slots x 8B)",
        lower_after, lower_before - lower_after);

    // 3 slots removed → lower should decrease by 3 × ITEM_ID_SIZE (8)
    assert!(
        lower_after < lower_before,
        "lower should decrease: before={} after={}", lower_before, lower_after
    );
    assert_eq!(lower_before - lower_after, 3 * 8, "expected 3 slots removed");
    cleanup_db(db);
}

// C5 – upper increases after compaction (data area reclaimed)
#[test]
fn compaction_increases_upper() {
    let db = "_tc_c5"; let tbl = "t";
    let mut file = setup_canonical(db, tbl, 6);
    let cat = make_catalog(db, tbl);

    let (_, upper_before) = page1_bounds(&mut file);

    let groups = parse_where_clause("id IN (1, 2, 3)").unwrap();
    delete_tuples(&cat, db, tbl, &mut file, &groups, false).unwrap();
    drop(file);

    compaction_table(db, tbl).unwrap();

    let mut file2 = OpenOptions::new().read(true).write(true)
        .open(format!("database/base/{}/{}.dat", db, tbl)).unwrap();
    let (_, upper_after) = page1_bounds(&mut file2);

    assert!(
        upper_after > upper_before,
        "upper should increase: before={} after={}", upper_before, upper_after
    );
    cleanup_db(db);
}

// C6 – live count unchanged after compaction (rows not lost)
#[test]
fn compaction_preserves_live_rows() {
    let db = "_tc_c6"; let tbl = "t";
    let mut file = setup_canonical(db, tbl, 8);
    let cat = make_catalog(db, tbl);

    let groups = parse_where_clause("id IN (2, 4, 6)").unwrap();
    delete_tuples(&cat, db, tbl, &mut file, &groups, false).unwrap();
    drop(file);

    println!("[COMPACTION] Deleted id=2,4,6 (3 dead slots). Running compaction ...");
    compaction_table(db, tbl).unwrap();

    let mut file2 = OpenOptions::new().read(true).write(true)
        .open(format!("database/base/{}/{}.dat", db, tbl)).unwrap();
    let (live, dead) = slot_counts(&mut file2);
    println!("[COMPACTION] After compaction: live={} dead={} — physical page now clean.", live, dead);
    assert_eq!(live, 5, "5 live rows should remain");
    assert_eq!(dead, 0, "no dead rows after compaction");
    cleanup_db(db);
}

// C7 – second compaction on same page returns 0 (idempotent)
#[test]
fn compaction_is_idempotent() {
    let db = "_tc_c7"; let tbl = "t";
    let mut file = setup_canonical(db, tbl, 5);
    let cat = make_catalog(db, tbl);

    let groups = parse_where_clause("id = 3").unwrap();
    delete_tuples(&cat, db, tbl, &mut file, &groups, false).unwrap();
    drop(file);

    let first  = compaction_table(db, tbl).unwrap();
    println!("[COMPACTION] First compaction:  pages_rewritten={}", first);
    let second = compaction_table(db, tbl).unwrap();
    println!("[COMPACTION] Second compaction: pages_rewritten={} (idempotent — nothing to do)", second);
    assert_eq!(first,  1, "first compaction should rewrite 1 page");
    assert_eq!(second, 0, "second compaction should rewrite 0 pages (nothing to do)");
    cleanup_db(db);
}

// C8 – delete all + compact → lower == PAGE_HEADER_SIZE, upper == PAGE_SIZE
#[test]
fn compaction_after_delete_all_gives_empty_page() {
    let db = "_tc_c8"; let tbl = "t";
    let mut file = setup_canonical(db, tbl, 5);
    let cat = make_catalog(db, tbl);

    delete_tuples(&cat, db, tbl, &mut file, &[], false).unwrap();
    drop(file);

    compaction_table(db, tbl).unwrap();

    let mut file2 = OpenOptions::new().read(true).write(true)
        .open(format!("database/base/{}/{}.dat", db, tbl)).unwrap();
    let (lower, upper) = page1_bounds(&mut file2);
    assert_eq!(lower, PAGE_HEADER_SIZE, "empty page: lower should be PAGE_HEADER_SIZE");
    assert_eq!(upper, PAGE_SIZE as u32,  "empty page: upper should be PAGE_SIZE");
    cleanup_db(db);
}

// C9 – interleaved delete-compact-delete cycle
#[test]
fn compaction_interleaved_cycle() {
    let db = "_tc_c9"; let tbl = "t";
    let mut file = setup_canonical(db, tbl, 6);
    let cat = make_catalog(db, tbl);

    // Step 1: delete rows 1, 2
    let g1 = parse_where_clause("id IN (1, 2)").unwrap();
    delete_tuples(&cat, db, tbl, &mut file, &g1, false).unwrap();
    drop(file);
    println!("[COMPACTION] Cycle step 1: deleted id=1,2.");

    // Step 2: compact
    let p1 = compaction_table(db, tbl).unwrap();
    println!("[COMPACTION] Cycle step 2: compaction_table() → {} page(s) rewritten. FSM updated.", p1);
    assert_eq!(p1, 1);

    // Step 3: delete rows 3, 4 (which are now at the front)
    let mut file2 = OpenOptions::new().read(true).write(true)
        .open(format!("database/base/{}/{}.dat", db, tbl)).unwrap();
    let g2 = parse_where_clause("id IN (3, 4)").unwrap();
    delete_tuples(&cat, db, tbl, &mut file2, &g2, false).unwrap();
    drop(file2);
    println!("[COMPACTION] Cycle step 3: deleted id=3,4 (new dead slots inserted after first compaction).");

    // Step 4: compact again
    let p2 = compaction_table(db, tbl).unwrap();
    println!("[COMPACTION] Cycle step 4: compaction_table() → {} page(s) rewritten.", p2);
    assert_eq!(p2, 1);

    // Verify only 5, 6 remain
    let mut file3 = OpenOptions::new().read(true).write(true)
        .open(format!("database/base/{}/{}.dat", db, tbl)).unwrap();
    let (live, dead) = slot_counts(&mut file3);
    println!("[COMPACTION] Final state: live={} dead={} (only rows 5 and 6 remain).", live, dead);
    assert_eq!(live, 2);
    assert_eq!(dead, 0);
    cleanup_db(db);
}

// C10 – compaction of table with NO deleted rows → 0 pages rewritten
#[test]
fn compaction_nothing_to_compact() {
    let db = "_tc_c10"; let tbl = "t";
    let _file = setup_canonical(db, tbl, 5); // no deletes
    drop(_file);

    let pages = compaction_table(db, tbl).unwrap();
    assert_eq!(pages, 0, "no deleted rows → nothing to compact");
    cleanup_db(db);
}
