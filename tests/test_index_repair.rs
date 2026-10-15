//! Index maintenance that fails does not leave an index out of step with its
//! table (C8): the statement's row changes stand and the table's indexes are
//! rebuilt from its rows.

mod common;

use storage_manager::backend::session::{Session, StatementResult};
use storage_manager::types::DataValue;

fn session() -> Session {
    common::init_catalog();
    Session::new(common::load_catalog())
}

fn run(s: &mut Session, sql: &str) -> Result<StatementResult, String> {
    let plan = rook_parser::parse_sql(sql)?;
    s.execute(plan).map_err(|e| e.to_string())
}

fn ok(s: &mut Session, sql: &str) {
    run(s, sql).unwrap_or_else(|e| panic!("{} failed: {}", sql, e));
}

fn ids(s: &mut Session, sql: &str) -> Vec<i32> {
    let (_, rows) = run(s, sql)
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e))
        .rows
        .expect("a query returns rows");
    rows.into_iter()
        .map(|t| match t.values[0] {
            Some(DataValue::Int(v)) => v,
            ref other => panic!("not an INT: {:?}", other),
        })
        .collect()
}

/// Take the index file away behind the engine's back, as a failed write
/// or a lost file would.
fn lose_index_file() -> std::path::PathBuf {
    let engine = common::engine();
    let path = engine.paths().index("app", "t", "t_k");
    std::fs::remove_file(&path).unwrap();
    storage_manager::backend::cache::evict_btree(&engine, &path);
    path
}

#[test]
fn a_failed_index_update_rebuilds_the_indexes() {
    let _ws = common::TestWorkspace::new("index_repair", "dml");
    let mut s = session();
    ok(&mut s, "CREATE DATABASE app");
    ok(&mut s, "USE app");
    ok(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, k INT)");
    ok(&mut s, "CREATE INDEX t_k ON t (k)");
    let values: Vec<String> = (1..=50).map(|i| format!("({}, {})", i, i % 5)).collect();
    ok(
        &mut s,
        &format!("INSERT INTO t VALUES {}", values.join(", ")),
    );

    let path = lose_index_file();
    ok(&mut s, "DELETE FROM t WHERE id = 3");
    assert!(path.exists(), "the index was not rebuilt after DELETE");
    assert_eq!(
        ids(&mut s, "SELECT id FROM t WHERE k = 3 ORDER BY id"),
        [8, 13, 18, 23, 28, 33, 38, 43, 48]
    );

    // A row change finds the rebuilt index again; then it is lost again.
    ok(&mut s, "INSERT INTO t VALUES (51, 1)");
    lose_index_file();
    ok(&mut s, "UPDATE t SET k = 99 WHERE id = 8");
    assert!(path.exists(), "the index was not rebuilt after UPDATE");
    assert_eq!(ids(&mut s, "SELECT id FROM t WHERE k = 99"), [8]);
    assert_eq!(
        ids(&mut s, "SELECT id FROM t WHERE k = 3 ORDER BY id"),
        [13, 18, 23, 28, 33, 38, 43, 48]
    );
    assert_eq!(
        ids(&mut s, "SELECT id FROM t WHERE k = 1 ORDER BY id"),
        [1, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51]
    );
}

/// The same loss with nothing cached about the table, as after a restart:
/// the index list comes from `sys_indexes` alone, which must still name the
/// index whose file is gone, or the index silently stops being maintained.
#[test]
fn a_lost_index_file_is_rebuilt_with_a_cold_cache() {
    let _ws = common::TestWorkspace::new("index_repair", "cold");
    let mut s = session();
    ok(&mut s, "CREATE DATABASE app");
    ok(&mut s, "USE app");
    ok(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, k INT)");
    ok(&mut s, "CREATE INDEX t_k ON t (k)");
    ok(&mut s, "INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (4, 3)");

    let path = lose_index_file();
    common::engine().invalidate_metadata();
    ok(&mut s, "DELETE FROM t WHERE id = 3");
    assert!(path.exists(), "the index was not rebuilt after DELETE");
    assert_eq!(ids(&mut s, "SELECT id FROM t WHERE k = 3"), [4]);
}
