//! Schema changes keep a table's indexes (G15): ALTER TABLE ADD COLUMN,
//! DROP COLUMN and TRUNCATE rebuild them, RENAME COLUMN follows the rename,
//! and an index on a dropped column is dropped with it.

mod common;

use storage_manager::backend::executor::create_index::load_table_indexes_multi;
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

fn rows(s: &mut Session, sql: &str) -> Vec<Vec<Option<DataValue>>> {
    let (_, rows) = run(s, sql)
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e))
        .rows
        .expect("a query returns rows");
    rows.into_iter().map(|t| t.values).collect()
}

/// The table's indexes as `name(col,col)`, sorted.
fn indexes(table: &str) -> Vec<String> {
    let mut v: Vec<String> = load_table_indexes_multi(&common::engine(), "app", table)
        .unwrap()
        .into_iter()
        .map(|(name, cols, unique)| {
            format!(
                "{}({}){}",
                name,
                cols.join(","),
                if unique { " unique" } else { "" }
            )
        })
        .collect();
    v.sort();
    v
}

fn setup(s: &mut Session) {
    ok(s, "CREATE DATABASE app");
    ok(s, "USE app");
    ok(
        s,
        "CREATE TABLE t (id INT PRIMARY KEY, k INT, name VARCHAR(10))",
    );
    ok(s, "CREATE INDEX t_k ON t (k)");
    ok(s, "CREATE INDEX t_kn ON t (k, name)");
    ok(
        s,
        "INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 10, 'c'), (4, 30, 'd')",
    );
}

#[test]
fn add_column_rebuilds_indexes() {
    let _ws = common::TestWorkspace::new("alter_indexes", "add");
    let mut s = session();
    setup(&mut s);
    let before = indexes("t");
    assert_eq!(before.len(), 3, "{:?}", before);

    ok(&mut s, "ALTER TABLE t ADD COLUMN z INT DEFAULT 7");
    assert_eq!(indexes("t"), before);
    assert_eq!(
        rows(&mut s, "SELECT id, z FROM t WHERE k = 10 ORDER BY id"),
        [
            vec![Some(DataValue::Int(1)), Some(DataValue::Int(7))],
            vec![Some(DataValue::Int(3)), Some(DataValue::Int(7))],
        ]
    );
    // The primary key still rejects duplicates and new rows are indexed.
    assert!(run(&mut s, "INSERT INTO t VALUES (1, 99, 'x', 1)").is_err());
    ok(&mut s, "INSERT INTO t VALUES (5, 10, 'e', 1)");
    assert_eq!(rows(&mut s, "SELECT id FROM t WHERE k = 10").len(), 3);
}

#[test]
fn drop_column_rebuilds_the_others_and_drops_its_indexes() {
    let _ws = common::TestWorkspace::new("alter_indexes", "drop");
    let mut s = session();
    setup(&mut s);
    ok(&mut s, "CREATE INDEX t_name ON t (name)");

    ok(&mut s, "ALTER TABLE t DROP COLUMN name");
    let after = indexes("t");
    assert!(after.iter().any(|i| i.starts_with("t_k(k)")), "{:?}", after);
    assert!(!after.iter().any(|i| i.contains("name")), "{:?}", after);
    assert_eq!(after.len(), 2, "{:?}", after);
    assert_eq!(
        rows(&mut s, "SELECT id FROM t WHERE k = 20"),
        [vec![Some(DataValue::Int(2))]]
    );
    // An index of the same name can be created again.
    ok(&mut s, "ALTER TABLE t ADD COLUMN name VARCHAR(10)");
    ok(&mut s, "CREATE INDEX t_name ON t (name)");
}

#[test]
fn truncate_keeps_empty_indexes() {
    let _ws = common::TestWorkspace::new("alter_indexes", "truncate");
    let mut s = session();
    setup(&mut s);
    let before = indexes("t");

    ok(&mut s, "TRUNCATE TABLE t");
    // A later catalog save must not forget them either.
    ok(&mut s, "CREATE TABLE other (x INT)");
    assert_eq!(indexes("t"), before);
    assert!(rows(&mut s, "SELECT id FROM t WHERE k = 10").is_empty());
    ok(&mut s, "INSERT INTO t VALUES (1, 10, 'a')");
    assert!(run(&mut s, "INSERT INTO t VALUES (1, 11, 'b')").is_err());
    assert_eq!(
        rows(&mut s, "SELECT id FROM t WHERE k = 10"),
        [vec![Some(DataValue::Int(1))]]
    );
}

#[test]
fn rename_column_renames_index_columns() {
    let _ws = common::TestWorkspace::new("alter_indexes", "rename");
    let mut s = session();
    setup(&mut s);

    ok(&mut s, "ALTER TABLE t RENAME COLUMN k TO kk");
    let after = indexes("t");
    assert!(after.contains(&"t_k(kk)".to_string()), "{:?}", after);
    assert!(after.contains(&"t_kn(kk,name)".to_string()), "{:?}", after);
    assert_eq!(
        rows(&mut s, "SELECT id FROM t WHERE kk = 30"),
        [vec![Some(DataValue::Int(4))]]
    );
}

#[test]
fn rename_column_keeps_a_check_on_a_quoted_name_enforced() {
    let _ws = common::TestWorkspace::new("alteridx", "check_quoted");
    let mut s = session();
    ok(&mut s, "CREATE DATABASE app");
    ok(&mut s, "USE app");
    ok(
        &mut s,
        r#"CREATE TABLE t (id INT, "my col" INT, CHECK ("my col" > 0 AND id <> 99))"#,
    );
    ok(
        &mut s,
        r#"ALTER TABLE t RENAME COLUMN "my col" TO "your col""#,
    );
    ok(&mut s, "INSERT INTO t VALUES (1, 5)");
    let err = run(&mut s, "INSERT INTO t VALUES (2, -1)").expect_err("CHECK still holds");
    assert!(err.contains("CHECK"), "{}", err);
    let err = run(&mut s, "INSERT INTO t VALUES (99, 1)").expect_err("CHECK still holds");
    assert!(err.contains("CHECK"), "{}", err);
    assert_eq!(rows(&mut s, r#"SELECT "your col" FROM t"#).len(), 1);
}
