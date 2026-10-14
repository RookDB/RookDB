//! Engines on different data directories, used side by side in one process
//! without changing the working directory, keep separate data and caches.

use std::path::{Path, PathBuf};

use storage_manager::backend::engine::Engine;
use storage_manager::backend::session::Session;
use storage_manager::catalog::{init_catalog_in, load_catalog_in};
use storage_manager::types::DataValue;

/// A fresh directory under the system temp dir, removed on drop.
struct TempRoot(PathBuf);

impl TempRoot {
    fn new(tag: &str) -> Self {
        let dir =
            std::env::temp_dir().join(format!("rookdb_engine_{}_{}", std::process::id(), tag));
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

fn session(root: &Path) -> Session {
    let engine = Engine::open(root);
    init_catalog_in(&engine);
    Session::new(load_catalog_in(&engine))
}

fn run(s: &mut Session, sql: &str) {
    s.execute(rook_parser::parse_sql(sql).unwrap())
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
}

fn ids(s: &mut Session) -> Vec<i32> {
    let result = s
        .execute(rook_parser::parse_sql("SELECT id FROM t ORDER BY id").unwrap())
        .unwrap();
    result
        .rows
        .unwrap()
        .1
        .into_iter()
        .map(|t| match t.values[0] {
            Some(DataValue::Int(v)) => v,
            ref other => panic!("unexpected {:?}", other),
        })
        .collect()
}

#[test]
fn two_directories_in_one_process_stay_separate() {
    let a = TempRoot::new("a");
    let b = TempRoot::new("b");
    let cwd = std::env::current_dir().unwrap();

    let mut sa = session(&a.0);
    let mut sb = session(&b.0);
    // Same database and table names in both directories.
    for s in [&mut sa, &mut sb] {
        run(s, "CREATE DATABASE iso_db");
        run(s, "USE iso_db");
        run(s, "CREATE TABLE t (id INT PRIMARY KEY)");
    }
    run(&mut sa, "INSERT INTO t VALUES (1), (2)");
    run(&mut sb, "INSERT INTO t VALUES (10)");
    run(&mut sb, "DELETE FROM t WHERE id = 10");
    run(&mut sb, "INSERT INTO t VALUES (20), (30)");

    assert_eq!(ids(&mut sa), [1, 2]);
    assert_eq!(ids(&mut sb), [20, 30]);

    // Each engine wrote under its own root and nothing under the working
    // directory, which the test never changed.
    assert_eq!(std::env::current_dir().unwrap(), cwd);
    assert!(a.0.join("database/base/iso_db/t.dat").exists());
    assert!(b.0.join("database/base/iso_db/t.dat").exists());
    assert!(!cwd.join("database/base/iso_db").exists());

    // A catalog reloaded from a directory shares that directory's engine.
    let reloaded = load_catalog_in(&Engine::open(&a.0));
    assert!(std::ptr::eq(
        reloaded.engine().as_ref(),
        sa.catalog.engine().as_ref()
    ));
}
