//! Several sessions on one data directory in one process.
//!
//! Each `Session` keeps its own copy of the catalog, and a DDL statement
//! saves the whole catalog from that copy. These tests check that a session
//! sees the schema other sessions committed before its statement runs, that
//! no session's DDL drops another's changes, and that sessions on different
//! threads run their statements one at a time.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use storage_manager::backend::engine::Engine;
use storage_manager::backend::error::RookResult;
use storage_manager::backend::session::{NO_DATABASE, Session, StatementResult};
use storage_manager::catalog::{init_catalog_in, load_catalog_in};
use storage_manager::types::DataValue;

/// A fresh directory under the system temp dir, removed on drop.
struct TempRoot(PathBuf);

impl TempRoot {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "rookdb_multi_session_{}_{}",
            std::process::id(),
            tag
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        TempRoot(dir)
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A session on `root`'s catalog as it is on disk now, with `db` selected.
fn session(root: &Path, db: &str) -> Session {
    let mut s = Session::new(load_catalog_in(&Engine::open(root)));
    run(&mut s, &format!("USE {}", db));
    s
}

/// A directory holding database `db`.
fn setup(tag: &str, db: &str) -> TempRoot {
    let root = TempRoot::new(tag);
    let engine = Engine::open(&root.0);
    init_catalog_in(&engine).expect("init catalog");
    let mut s = Session::new(load_catalog_in(&engine));
    run(&mut s, &format!("CREATE DATABASE {}", db));
    root
}

fn try_run(s: &mut Session, sql: &str) -> RookResult<StatementResult> {
    s.execute(rook_parser::parse_sql(sql).unwrap())
}

fn run(s: &mut Session, sql: &str) -> StatementResult {
    try_run(s, sql).unwrap_or_else(|e| panic!("{}: {}", sql, e))
}

/// The tables of `db` in the catalog on disk.
fn tables_on_disk(root: &Path, db: &str) -> BTreeSet<String> {
    load_catalog_in(&Engine::open(root)).databases[db]
        .tables
        .keys()
        .cloned()
        .collect()
}

fn column_names(result: &StatementResult) -> Vec<String> {
    result
        .rows
        .as_ref()
        .unwrap()
        .0
        .iter()
        .map(|c| c.name.to_lowercase())
        .collect()
}

fn count(s: &mut Session, sql: &str) -> i64 {
    let result = run(s, sql);
    match result.rows.unwrap().1[0].values[0] {
        Some(DataValue::BigInt(n)) => n,
        Some(DataValue::Int(n)) => n as i64,
        ref other => panic!("{}: unexpected {:?}", sql, other),
    }
}

#[test]
fn ddl_in_one_session_keeps_tables_another_created() {
    let root = setup("lost_update", "db");
    let mut a = session(&root.0, "db");
    let mut b = session(&root.0, "db");

    run(&mut a, "CREATE TABLE x (id INT)");
    // b's catalog was loaded before x existed. Saving it must not drop x.
    run(&mut b, "CREATE TABLE y (id INT)");

    let on_disk = tables_on_disk(&root.0, "db");
    assert!(on_disk.contains("x"), "x was lost: {:?}", on_disk);
    assert!(on_disk.contains("y"), "y was lost: {:?}", on_disk);

    // Each session can use the other's table.
    run(&mut a, "INSERT INTO y VALUES (1)");
    run(&mut b, "INSERT INTO x VALUES (2)");
    assert_eq!(count(&mut a, "SELECT COUNT(*) FROM x"), 1);
    assert_eq!(count(&mut b, "SELECT COUNT(*) FROM y"), 1);
}

#[test]
fn a_session_sees_schema_changes_another_committed() {
    let root = setup("stale_read", "db");
    let mut a = session(&root.0, "db");
    let mut b = session(&root.0, "db");

    run(&mut a, "CREATE TABLE t (id INT)");
    run(&mut a, "INSERT INTO t VALUES (1), (2)");
    assert_eq!(count(&mut b, "SELECT COUNT(*) FROM t"), 2);

    run(&mut a, "ALTER TABLE t ADD COLUMN name VARCHAR(10)");
    let result = run(&mut b, "SELECT * FROM t");
    assert_eq!(column_names(&result), ["id", "name"]);
    run(&mut b, "INSERT INTO t VALUES (3, 'three')");
    assert_eq!(
        count(&mut a, "SELECT COUNT(*) FROM t WHERE name = 'three'"),
        1
    );

    run(&mut a, "ALTER TABLE t DROP COLUMN name");
    let result = run(&mut b, "SELECT * FROM t");
    assert_eq!(column_names(&result), ["id"]);

    run(&mut a, "DROP TABLE t");
    let err = try_run(&mut b, "SELECT * FROM t").expect_err("t was dropped");
    assert!(err.to_string().contains("not found"), "{}", err);
}

#[test]
fn a_dropped_database_is_no_longer_selected_in_other_sessions() {
    let root = setup("dropped_db", "db");
    let mut a = session(&root.0, "db");
    let mut b = session(&root.0, "db");
    run(&mut b, "CREATE TABLE t (id INT)");

    run(&mut a, "CREATE DATABASE other");
    run(&mut a, "USE other");
    run(&mut a, "DROP DATABASE db");

    let err = try_run(&mut b, "SELECT * FROM t").expect_err("db was dropped");
    assert!(err.to_string().contains(NO_DATABASE), "{}", err);
    assert_eq!(b.current_db, None);
}

#[test]
fn a_sessions_own_ddl_is_visible_to_it() {
    let root = setup("own_ddl", "db");
    let mut a = session(&root.0, "db");
    for i in 0..5 {
        run(&mut a, &format!("CREATE TABLE t{} (id INT)", i));
        run(&mut a, &format!("INSERT INTO t{} VALUES ({})", i, i));
    }
    for i in 0..5 {
        assert_eq!(count(&mut a, &format!("SELECT COUNT(*) FROM t{}", i)), 1);
    }
    assert_eq!(tables_on_disk(&root.0, "db").len(), 5);
}

#[test]
fn sessions_on_threads_lose_no_ddl() {
    // Small: DDL waits on fsync, and statements now run one at a time.
    // Without turns, the first concurrent CREATE TABLEs already failed.
    const THREADS: usize = 4;
    const TABLES: usize = 4;
    let root = setup("threads", "db");

    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let root = &root.0;
            scope.spawn(move || {
                let mut s = session(root, "db");
                for i in 0..TABLES {
                    let name = format!("t_{}_{}", t, i);
                    run(&mut s, &format!("CREATE TABLE {} (id INT, v INT)", name));
                    run(
                        &mut s,
                        &format!("INSERT INTO {} VALUES ({}, {})", name, t, i),
                    );
                    assert_eq!(count(&mut s, &format!("SELECT COUNT(*) FROM {}", name)), 1);
                }
            });
        }
    });

    let on_disk = tables_on_disk(&root.0, "db");
    let expected: BTreeSet<String> = (0..THREADS)
        .flat_map(|t| (0..TABLES).map(move |i| format!("t_{}_{}", t, i)))
        .collect();
    let missing: Vec<_> = expected.difference(&on_disk).collect();
    assert!(missing.is_empty(), "tables lost: {:?}", missing);

    let mut s = session(&root.0, "db");
    for name in &expected {
        assert_eq!(
            count(&mut s, &format!("SELECT COUNT(*) FROM {}", name)),
            1,
            "{}",
            name
        );
    }
}

#[test]
fn readers_on_threads_run_alongside_schema_changes() {
    const READERS: usize = 3;
    const CHANGES: usize = 6;
    let root = setup("threads_readers", "db");
    {
        let mut s = session(&root.0, "db");
        run(&mut s, "CREATE TABLE stable (id INT PRIMARY KEY, v INT)");
        let rows: Vec<String> = (0..100).map(|i| format!("({}, {})", i, i * 3)).collect();
        run(
            &mut s,
            &format!("INSERT INTO stable VALUES {}", rows.join(", ")),
        );
    }
    let writer_done = std::sync::atomic::AtomicBool::new(false);

    std::thread::scope(|scope| {
        let root_ref = &root.0;
        let done = &writer_done;
        scope.spawn(move || {
            let mut s = session(root_ref, "db");
            for i in 0..CHANGES {
                // Each change saves the whole catalog, `stable` included.
                run(&mut s, &format!("CREATE TABLE churn_{} (id INT)", i));
                run(&mut s, &format!("ALTER TABLE churn_{} ADD COLUMN c INT", i));
            }
            done.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        for r in 0..READERS {
            scope.spawn(move || {
                let mut s = session(root_ref, "db");
                let mut k = r;
                loop {
                    let finished = done.load(std::sync::atomic::Ordering::SeqCst);
                    let id = k % 100;
                    let result = run(&mut s, &format!("SELECT v FROM stable WHERE id = {}", id));
                    let v = &result.rows.unwrap().1[0].values[0];
                    assert_eq!(*v, Some(DataValue::Int((id * 3) as i32)), "reader {}", r);
                    assert_eq!(count(&mut s, "SELECT COUNT(*) FROM stable"), 100);
                    k += 7;
                    if finished {
                        break;
                    }
                }
                // Once the writer is done, every change is visible.
                for i in 0..CHANGES {
                    let result = run(&mut s, &format!("SELECT * FROM churn_{}", i));
                    assert_eq!(column_names(&result), ["id", "c"], "reader {}", r);
                }
            });
        }
    });
}

#[test]
fn sessions_on_threads_read_and_write_one_table() {
    const THREADS: usize = 4;
    const ROWS: usize = 50;
    let root = setup("threads_dml", "db");
    {
        let mut s = session(&root.0, "db");
        run(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, who INT)");
    }

    std::thread::scope(|scope| {
        for t in 0..THREADS {
            let root = &root.0;
            scope.spawn(move || {
                let mut s = session(root, "db");
                for i in 0..ROWS {
                    let id = t * ROWS + i;
                    run(&mut s, &format!("INSERT INTO t VALUES ({}, {})", id, t));
                    let mine = count(&mut s, &format!("SELECT COUNT(*) FROM t WHERE who = {}", t));
                    assert_eq!(mine, i as i64 + 1, "thread {} after {} inserts", t, i + 1);
                }
            });
        }
    });

    let mut s = session(&root.0, "db");
    assert_eq!(
        count(&mut s, "SELECT COUNT(*) FROM t"),
        (THREADS * ROWS) as i64
    );
}
