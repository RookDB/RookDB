//! Test: page-level write lock blocks concurrent DELETE / UPDATE on the same page.
//!
//! Each test runs two back-to-back rounds on a fresh table:
//!
//!   Round A  (baseline) – no background thread; measure how long the
//!              operation naturally takes (typically < 10 ms).
//!
//!   Round B  (locked)   – a background thread holds PageWriteLock on
//!              page 1 for HOLD_MS (5 s) before the same operation runs.
//!              The operation must wait for the lock to be released.
//!
//!   Assertion: locked_time − baseline_time ≥ GAP_MS (4.5 s).
//!
//! This relative comparison is robust: it proves the lock actually caused
//! the delay without depending on absolute timing of the host machine.

use std::collections::HashMap;
use std::fs::{create_dir_all, remove_dir_all, remove_file, OpenOptions};
use std::thread;
use std::time::{Duration, Instant};

use storage_manager::catalog::types::{Catalog, Column, Database, Table};
use storage_manager::executor::{
    delete_tuples, parse_where_clause,
    update_tuples, parse_set_clause,
};
#[allow(deprecated)]
use storage_manager::heap::init_table;
use storage_manager::heap::insert_tuple;
use storage_manager::page::page_lock::PageWriteLock;
use storage_manager::table::file_identity_from_file;

// ─── constants ────────────────────────────────────────────────────────────

const TBL: &str = "t";

/// How long the background thread holds the lock in the "locked" round.
const HOLD_MS: u64 = 5_000;
/// How long before the main thread starts its operation, so the background
/// thread has had time to acquire the lock first.
const DELAY_MS: u64 = 50;
/// Minimum required gap between the locked and baseline durations.
const GAP_MS: u64 = 4_500;

// ─── helpers ─────────────────────────────────────────────────────────────

fn tmp(tag: &str) -> String {
    format!("test_page_lock_{}.bin", tag)
}

fn make_row(id: i32, name: &str) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&id.to_le_bytes());
    let mut t = name.as_bytes().to_vec();
    t.resize(10, b' ');
    t.truncate(10);
    b.extend_from_slice(&t);
    b
}

fn make_catalog(db: &str) -> Catalog {
    let mut tables = HashMap::new();
    tables.insert(TBL.to_string(), Table {
        columns: vec![
            Column { name: "id".into(),   data_type: "INT".into()  },
            Column { name: "name".into(), data_type: "TEXT".into() },
        ],
    });
    let mut databases = HashMap::new();
    databases.insert(db.to_string(), Database { tables });
    Catalog { databases }
}

/// Create a fresh temp file (not under database/base/) with `n` rows.
/// Row = 14 B data + 8 B ItemID = 22 B; ~372 rows fit on page 1.
/// n ≤ 300 guarantees every row stays on page 1.
#[allow(deprecated)]
fn setup_file(path: &str, n: u32) -> std::fs::File {
    let _ = remove_file(path);
    let mut file = OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(path)
        .expect("setup_file: open");
    init_table(&mut file).expect("setup_file: init_table");
    for i in 1..=n {
        let row = make_row(i as i32, &format!("r{:04}", i));
        insert_tuple(&mut file, &row).expect("setup_file: insert_tuple");
    }
    file
}

/// Create a table under the canonical `database/base/{db}/{table}.dat` path
/// (required by `update_tuples`, which re-inserts rows via `insert_raw_tuple`).
#[allow(deprecated)]
fn setup_canonical(db: &str, n: u32) -> std::fs::File {
    let dir  = format!("database/base/{}", db);
    let path = format!("{}/{}.dat", dir, TBL);
    let _ = remove_dir_all(&dir);
    create_dir_all(&dir).unwrap();
    let mut file = OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(&path)
        .unwrap();
    init_table(&mut file).unwrap();
    for i in 1..=n {
        let row = make_row(i as i32, &format!("r{:04}", i));
        insert_tuple(&mut file, &row).unwrap();
    }
    file
}

// ─────────────────────────────────────────────────────────────────────────
// Test 1: DELETE – locked round takes ≥ 4.5 s longer than baseline
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn delete_lock_adds_at_least_4500ms_over_baseline() {
    let db = "_lock_del_db";
    let groups = parse_where_clause("id <= 50").unwrap();

    // ── Round A: baseline (no lock) ────────────────────────────────────
    let path_a = tmp("del_base");
    let cat_a  = make_catalog(db);
    let mut file_a = setup_file(&path_a, 200);

    println!("[PAGE LOCK] Round A: no lock held. Running DELETE WHERE id <= 50 ...");
    let t0 = Instant::now();
    let r_a = delete_tuples(&cat_a, db, TBL, &mut file_a, &groups, false).unwrap();
    let baseline = t0.elapsed();
    println!("[PAGE LOCK] Round A complete: deleted={} in {:?} (baseline, no contention).", r_a.deleted_count, baseline);

    let _ = remove_file(&path_a);
    assert_eq!(r_a.deleted_count, 50, "baseline: expected 50 deletions");

    // ── Round B: locked (background holds lock for HOLD_MS) ────────────
    let path_b = tmp("del_lock");
    let cat_b  = make_catalog(db);
    let mut file_b = setup_file(&path_b, 200);
    let file_id = file_identity_from_file(&file_b).unwrap();

    let bg = thread::spawn(move || {
        println!("[PAGE LOCK] Background thread: acquiring PageWriteLock on page 1 ...");
        let _lock = PageWriteLock::acquire(file_id, 1);
        println!("[PAGE LOCK] Background thread: lock HELD for {}ms. Main thread will block.", HOLD_MS);
        thread::sleep(Duration::from_millis(HOLD_MS));
        println!("[PAGE LOCK] Background thread: releasing lock now.");
        // lock released on drop
    });
    thread::sleep(Duration::from_millis(DELAY_MS)); // let bg acquire the lock first

    println!("[PAGE LOCK] Round B: same DELETE attempting PageWriteLock on page 1 ...");
    println!("[PAGE LOCK] >>> BLOCKED — waiting for background thread to release ...");
    let t1 = Instant::now();
    let r_b = delete_tuples(&cat_b, db, TBL, &mut file_b, &groups, false).unwrap();
    let locked_time = t1.elapsed();
    println!("[PAGE LOCK] >>> UNBLOCKED — lock released. DELETE completed in {:?}.", locked_time);
    println!("[PAGE LOCK] Extra delay from lock: {:?} (expected ≥ 4500ms).",
        locked_time.saturating_sub(baseline));

    bg.join().unwrap();
    let _ = remove_file(&path_b);
    assert_eq!(r_b.deleted_count, 50, "locked: expected 50 deletions");

    // ── Assert: the lock caused a delay of at least GAP_MS ─────────────
    let gap = locked_time.saturating_sub(baseline);
    assert!(
        gap >= Duration::from_millis(GAP_MS),
        "gap between locked ({:?}) and baseline ({:?}) was {:?} — expected ≥ {} ms; \
         delete did not block on the page lock",
        locked_time, baseline, gap, GAP_MS
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Test 2: UPDATE – locked round takes ≥ 4.5 s longer than baseline
// ─────────────────────────────────────────────────────────────────────────

/// `update_tuples` re-inserts the modified tuple via `insert_raw_tuple`,
/// which resolves the table file by the canonical `database/base/` path.
/// Both rounds therefore use that layout (different db names so they are
/// independent and can both clean up safely).
#[allow(deprecated)]
#[test]
fn update_lock_adds_at_least_4500ms_over_baseline() {
    let where_groups = parse_where_clause("id <= 50").unwrap();
    let set_clause   = parse_set_clause("name = 'updated'").unwrap();

    // ── Round A: baseline (no lock) ────────────────────────────────────
    let db_a = "_lock_upd_base";
    let cat_a = make_catalog(db_a);
    let mut file_a = setup_canonical(db_a, 200);

    println!("[PAGE LOCK] UPDATE Round A: no lock. Running UPDATE SET name=\'updated\' WHERE id <= 50 ...");
    let t0 = Instant::now();
    let r_a = update_tuples(&cat_a, db_a, TBL, &mut file_a, &set_clause, &where_groups, false)
        .unwrap();
    let baseline = t0.elapsed();
    println!("[PAGE LOCK] UPDATE Round A complete: updated={} in {:?} (baseline).", r_a.updated_count, baseline);

    let _ = remove_dir_all(format!("database/base/{}", db_a));
    assert_eq!(r_a.updated_count, 50, "baseline: expected 50 updates");

    // ── Round B: locked (background holds lock for HOLD_MS) ────────────
    let db_b = "_lock_upd_lock";
    let cat_b = make_catalog(db_b);
    let mut file_b = setup_canonical(db_b, 200);
    let file_id = file_identity_from_file(&file_b).unwrap();

    let bg = thread::spawn(move || {
        println!("[PAGE LOCK] Background thread: acquiring PageWriteLock on page 1 (UPDATE test) ...");
        let _lock = PageWriteLock::acquire(file_id, 1);
        println!("[PAGE LOCK] Background thread: lock HELD for {}ms.", HOLD_MS);
        thread::sleep(Duration::from_millis(HOLD_MS));
        println!("[PAGE LOCK] Background thread: releasing lock.");
    });
    thread::sleep(Duration::from_millis(DELAY_MS));

    println!("[PAGE LOCK] UPDATE Round B: trying PageWriteLock on page 1 ...");
    println!("[PAGE LOCK] >>> BLOCKED — waiting for background thread to release ...");
    let t1 = Instant::now();
    let r_b = update_tuples(&cat_b, db_b, TBL, &mut file_b, &set_clause, &where_groups, false)
        .unwrap();
    let locked_time = t1.elapsed();
    println!("[PAGE LOCK] >>> UNBLOCKED. UPDATE completed in {:?}.", locked_time);
    println!("[PAGE LOCK] Extra delay: {:?} (expected ≥ 4500ms).", locked_time.saturating_sub(baseline));

    bg.join().unwrap();
    let _ = remove_dir_all(format!("database/base/{}", db_b));
    assert_eq!(r_b.updated_count, 50, "locked: expected 50 updates");

    // ── Assert: the lock caused a delay of at least GAP_MS ─────────────
    let gap = locked_time.saturating_sub(baseline);
    assert!(
        gap >= Duration::from_millis(GAP_MS),
        "gap between locked ({:?}) and baseline ({:?}) was {:?} — expected ≥ {} ms; \
         update did not block on the page lock",
        locked_time, baseline, gap, GAP_MS
    );
}
