//! A catalog save rebuilds every system table in `system.new` and swaps it
//! in with two renames. These tests leave the directories as a crash at each
//! step would and check that loading recovers a whole catalog, old or new,
//! and that an unreadable catalog is an error rather than an empty one.

mod common;

use std::path::Path;

use storage_manager::backend::engine::Engine;
use storage_manager::backend::session::Session;
use storage_manager::catalog::try_load_catalog_in;

const SYSTEM_TABLES: [&str; 6] = [
    "databases",
    "tables",
    "columns",
    "constraints",
    "indexes",
    "views",
];

fn run(s: &mut Session, sql: &str) {
    s.execute(rook_parser::parse_sql(sql).unwrap())
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
}

/// Tables of `crash_db`, sorted.
fn tables(engine: &std::sync::Arc<Engine>) -> Vec<String> {
    let catalog = try_load_catalog_in(engine).expect("load catalog");
    let mut names: Vec<String> = catalog
        .databases
        .get("crash_db")
        .map(|db| db.tables.keys().cloned().collect())
        .unwrap_or_default();
    names.sort();
    names
}

/// Drop every cached handle on the system tables, as a process exit would,
/// so the test can move the files underneath.
fn release_system(engine: &Engine) {
    for name in SYSTEM_TABLES {
        let path = engine.paths().system_table(name);
        storage_manager::backend::cache::release_file(&path).unwrap();
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

/// A workspace whose committed catalog holds `crash_db.t1`.
fn workspace(tag: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("atomic_catalog", tag);
    let mut s = Session::new(common::load_catalog());
    run(&mut s, "CREATE DATABASE crash_db");
    run(&mut s, "USE crash_db");
    run(&mut s, "CREATE TABLE t1 (id INT PRIMARY KEY)");
    (ws, s)
}

#[test]
fn save_leaves_only_the_system_directory() {
    let (ws, _s) = workspace("clean");
    let paths = ws.engine().paths();
    assert!(paths.system_dir().exists());
    assert!(!paths.system_staging_dir().exists());
    assert!(!paths.system_old_dir().exists());
    assert_eq!(tables(ws.engine()), ["t1"]);
}

#[test]
fn crash_between_the_renames_keeps_the_new_catalog() {
    let (ws, mut s) = workspace("between");
    let paths = ws.engine().paths().clone();
    let snapshot = ws.path().join("snapshot");
    release_system(ws.engine());
    copy_dir(&paths.system_dir(), &snapshot);

    run(&mut s, "CREATE TABLE t2 (id INT)");
    release_system(ws.engine());
    // `system` became `system.old`; `system.new` was not yet renamed.
    std::fs::rename(paths.system_dir(), paths.system_staging_dir()).unwrap();
    std::fs::rename(&snapshot, paths.system_old_dir()).unwrap();

    assert_eq!(tables(ws.engine()), ["t1", "t2"]);
    assert!(!paths.system_staging_dir().exists());
    assert!(!paths.system_old_dir().exists());

    // The recovered catalog takes further DDL.
    let mut s = Session::new(common::load_catalog());
    run(&mut s, "USE crash_db");
    run(&mut s, "CREATE TABLE t3 (id INT)");
    assert_eq!(tables(ws.engine()), ["t1", "t2", "t3"]);
}

#[test]
fn crash_while_staging_keeps_the_old_catalog() {
    let (ws, _s) = workspace("staging");
    let paths = ws.engine().paths().clone();
    release_system(ws.engine());
    // A half-written `system.new` beside the committed `system`.
    std::fs::create_dir_all(paths.system_staging_dir()).unwrap();
    std::fs::write(paths.system_staging_dir().join("tables.dat"), b"partial").unwrap();

    assert_eq!(tables(ws.engine()), ["t1"]);
    assert!(!paths.system_staging_dir().exists());
}

#[test]
fn crash_after_parking_the_old_catalog_restores_it() {
    let (ws, _s) = workspace("parked");
    let paths = ws.engine().paths().clone();
    release_system(ws.engine());
    // `system` renamed to `system.old` with no `system.new` to replace it.
    std::fs::rename(paths.system_dir(), paths.system_old_dir()).unwrap();

    assert_eq!(tables(ws.engine()), ["t1"]);
    assert!(paths.system_dir().exists());
    assert!(!paths.system_old_dir().exists());
}

#[test]
fn unreadable_system_table_is_an_error_not_an_empty_catalog() {
    let (ws, _s) = workspace("unreadable");
    release_system(ws.engine());
    std::fs::write(ws.engine().paths().system_table("tables"), b"garbage").unwrap();

    assert!(try_load_catalog_in(ws.engine()).is_err());
}
