//! ORDER BY keys that are not in the SELECT list: columns, expressions and
//! a mix of the two.

mod common;

use storage_manager::backend::session::{Session, StatementResult};
use storage_manager::types::DataValue;

fn session() -> Session {
    storage_manager::backend::cache::register_check_parser(rook_parser::parse_check_expr);
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

/// The first column of every row, as text.
fn firsts(s: &mut Session, sql: &str) -> Vec<String> {
    let (_, rows) = run(s, sql)
        .unwrap_or_else(|e| panic!("{} failed: {}", sql, e))
        .rows
        .expect("a query returns rows");
    rows.into_iter()
        .map(|t| match &t.values[0] {
            Some(DataValue::Int(v)) => v.to_string(),
            Some(DataValue::Varchar(v)) => v.clone(),
            Some(v) => v.to_string(),
            None => "NULL".to_string(),
        })
        .collect()
}

#[test]
fn order_by_keys_outside_the_select_list() {
    let _ws = common::TestWorkspace::new("order_hidden", "keys");
    let mut s = session();
    ok(&mut s, "CREATE DATABASE app");
    ok(&mut s, "USE app");
    ok(&mut s, "CREATE TABLE t (id INT, name VARCHAR(10), grp INT)");
    ok(
        &mut s,
        "INSERT INTO t VALUES (1, 'b', 1), (2, 'A', 2), (3, 'a', 1), (4, 'B', 2), (5, 'c', 1)",
    );
    let cases: &[(&str, &[&str])] = &[
        (
            "SELECT id FROM t ORDER BY grp DESC, id",
            &["2", "4", "1", "3", "5"],
        ),
        (
            "SELECT id FROM t ORDER BY UPPER(name), id",
            &["2", "3", "1", "4", "5"],
        ),
        (
            "SELECT id, name FROM t ORDER BY UPPER(name), grp DESC",
            &["2", "3", "4", "1", "5"],
        ),
        (
            "SELECT id FROM t ORDER BY UPPER(name), grp DESC",
            &["2", "3", "4", "1", "5"],
        ),
        (
            "SELECT id FROM t ORDER BY grp * -1, id",
            &["2", "4", "1", "3", "5"],
        ),
        (
            "SELECT name FROM t ORDER BY grp, UPPER(name) DESC, id",
            &["c", "b", "a", "B", "A"],
        ),
    ];
    let mut failures = Vec::new();
    for (sql, want) in cases {
        match run(&mut s, sql) {
            Err(e) => failures.push(format!("{}: error {}", sql, e)),
            Ok(_) => {
                let got = firsts(&mut s, sql);
                if got != *want {
                    failures.push(format!("{}: got {:?}, want {:?}", sql, got, want));
                }
            }
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n"));
}
