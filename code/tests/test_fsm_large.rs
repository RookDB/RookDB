//! Test: FSM space-reuse holds at scale – 1200-row table loaded from CSV.
//!
//! Counterpart to `test_fsm_reuse.rs`, which uses a small (~3-page) table.
//! This test exercises the same invariant but with a full 1200-row table
//! loaded from `examples/large.csv` (~4 data pages).
//!
//! Scenario:
//!   1. Load all 1200 rows from `examples/large.csv` via `insert_raw_tuple`
//!      (so every insert goes through HeapManager + FSM from the start).
//!   2. Confirm the table spans at least 4 data pages.
//!   3. Delete the entire middle page (page 2 worth of rows: ids 372–742).
//!   4. Run `compaction_table` – reclaims dead slots, updates FSM.
//!   5. Re-insert the same 371 rows.
//!   6. Assert: total page count has NOT grown – freed space was reused.

use std::collections::HashMap;
use std::fs::{create_dir_all, remove_dir_all, remove_file, File};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use storage_manager::catalog::types::{Catalog, Column, Database, Table};
use storage_manager::executor::{compaction_table, delete_tuples, parse_where_clause};
use storage_manager::executor::api::insert_raw_tuple;
use storage_manager::heap::HeapManager;
use storage_manager::table::page_count;

// ─── constants ────────────────────────────────────────────────────────────

const DB:  &str = "_fsm_large_db";
const TBL: &str = "students";

/// Path to the large CSV, relative to the `code/` crate root where `cargo test`
/// is executed.
const CSV_PATH: &str = "examples/large.csv";

/// Ids of the rows that belong exclusively to data page 2.
/// Each page holds ≈ 371 rows (PAGE_SIZE=8192, row=14B, item-id=8B → 371 rows/page).
const PAGE2_ID_START: i32 = 372;
const PAGE2_ID_END:   i32 = 742;

// ─── helpers ─────────────────────────────────────────────────────────────

fn make_catalog() -> Catalog {
    let mut tables = HashMap::new();
    tables.insert(TBL.to_string(), Table {
        columns: vec![
            Column { name: "id".into(),   data_type: "INT".into()  },
            Column { name: "name".into(), data_type: "TEXT".into() },
        ],
    });
    let mut databases = HashMap::new();
    databases.insert(DB.to_string(), Database { tables });
    Catalog { databases }
}

/// Serialize (id: i32, name: &str) -> 14-byte tuple (id + 10-byte padded name).
fn make_row(id: i32, name: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(14);
    bytes.extend_from_slice(&id.to_le_bytes());
    let mut text = name.as_bytes().to_vec();
    text.resize(10, b' ');
    text.truncate(10);
    bytes.extend_from_slice(&text);
    bytes
}

/// Create the table at its canonical path and return an open file handle.
fn setup_table() -> std::fs::File {
    let dir  = format!("database/base/{}", DB);
    let path = format!("{}/{}.dat", dir, TBL);
    let _ = remove_dir_all(&dir);
    create_dir_all(&dir).expect("create_dir_all failed");
    let _ = remove_file(format!("{}/{}.dat.fsm", dir, TBL));
    HeapManager::create(PathBuf::from(&path)).expect("HeapManager::create failed");
    File::options().read(true).write(true).open(&path).expect("open table failed")
}

fn reopen() -> std::fs::File {
    File::options()
        .read(true).write(true)
        .open(format!("database/base/{}/{}.dat", DB, TBL))
        .expect("reopen failed")
}

fn cleanup() {
    let _ = remove_dir_all(format!("database/base/{}", DB));
}

// ─────────────────────────────────────────────────────────────────────────
// Test
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn fsm_reuse_after_large_csv_load() {
    let mut file = setup_table();

    // ── Step 1: load all rows from large.csv ─────────────────────────────
    let csv = BufReader::new(File::open(CSV_PATH).expect("large.csv not found"));
    let mut loaded = 0usize;
    for (line_idx, line) in csv.lines().enumerate() {
        let line = line.expect("csv read error");
        if line_idx == 0 { continue; } // skip header
        let mut parts = line.splitn(2, ',');
        let id:   i32  = parts.next().unwrap_or("0").trim().parse().expect("id parse");
        let name: &str = parts.next().unwrap_or("").trim();
        insert_raw_tuple(DB, TBL, &make_row(id, name)).expect("insert_raw_tuple failed");
        loaded += 1;
    }
    println!("[FSM LARGE] Loaded {} rows from large.csv via insert_raw_tuple.", loaded);
    assert_eq!(loaded, 1200, "expected 1200 rows from large.csv");

    let pages_before = page_count(&mut file).unwrap();
    // 1200 rows / 371 per page = 4 data pages + 1 header = 5 total
    println!("[FSM LARGE] Table spans {} pages (1 header + {} data pages, ~371 rows/page).", pages_before, pages_before - 1);
    assert!(
        pages_before >= 5,
        "expected ≥ 5 pages after loading 1200 rows, got {}",
        pages_before
    );

    // ── Step 2: delete the entire middle page (ids PAGE2_ID_START..=PAGE2_ID_END)
    let cat = make_catalog();
    let where_clause = format!(
        "id >= {} AND id <= {}",
        PAGE2_ID_START, PAGE2_ID_END
    );
    let groups = parse_where_clause(&where_clause).unwrap();
    println!("[FSM LARGE] Soft-deleting rows with ids {}..{} (entire page 2) ...", PAGE2_ID_START, PAGE2_ID_END);
    let del_result = delete_tuples(&cat, DB, TBL, &mut file, &groups, false)
        .expect("delete_tuples failed");

    let deleted = del_result.deleted_count;
    println!("[FSM LARGE] Soft-deleted {} rows. Slots marked dead, page still on disk.", deleted);
    assert!(
        deleted >= 300,
        "expected ≥ 300 rows deleted from page 2, got {}",
        deleted
    );

    drop(file);

    // ── Step 3: compact – physically reclaim dead slots and update FSM ───
    println!("[FSM LARGE] Running compaction_table() — rewrites dead slots, updates FSM ...");
    let compacted = compaction_table(DB, TBL).expect("compaction_table failed");
    println!("[FSM LARGE] compaction_table() → {} page(s) rewritten. Page 2 free space now tracked in FSM.", compacted);
    assert!(compacted >= 1, "expected at least 1 page compacted");

    // ── Step 4: re-insert the same number of rows ─────────────────────────
    // These inserts must route to the freed page 2, not append new pages.
    println!("[FSM LARGE] Re-inserting {} rows — HeapManager should route all to freed page 2 via FSM ...", deleted);
    for i in 0..deleted {
        let row = make_row(
            PAGE2_ID_START + i as i32,
            &format!("re_{:05}", i),
        );
        insert_raw_tuple(DB, TBL, &row).expect("re-insert failed");
    }
    println!("[FSM LARGE] Re-insertion done. Checking page count ...");

    // ── Step 5: page count must NOT have grown ───────────────────────────
    let mut file = reopen();
    let pages_after = page_count(&mut file).unwrap();
    println!("[FSM LARGE] pages_before={} pages_after={}. {} — FSM space reuse {}.",
        pages_before, pages_after,
        if pages_after <= pages_before { "No new pages allocated" } else { "UNEXPECTED page growth" },
        if pages_after <= pages_before { "confirmed" } else { "FAILED" });
    assert!(
        pages_after <= pages_before,
        "page count grew from {} to {} – FSM did not reuse freed space",
        pages_before, pages_after
    );

    cleanup();
}
