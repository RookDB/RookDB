//! What a statement tells the user: output column names, DDL messages and
//! type errors are SQL, not internal Rust values.

mod common;

use storage_manager::backend::error::RookResult;
use storage_manager::backend::session::{Session, StatementResult};

fn exec(s: &mut Session, sql: &str) -> RookResult<StatementResult> {
    s.execute(rook_parser::parse_sql(sql).unwrap())
}

fn run(s: &mut Session, sql: &str) -> StatementResult {
    exec(s, sql).unwrap_or_else(|e| panic!("{}: {}", sql, e))
}

fn column_names(s: &mut Session, sql: &str) -> Vec<String> {
    let (columns, _) = run(s, sql).rows.expect("rows");
    columns.into_iter().map(|c| c.name).collect()
}

fn session(tag: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("messages", tag);
    let mut s = Session::new(common::load_catalog());
    run(&mut s, "CREATE DATABASE msg_db");
    run(&mut s, "USE msg_db");
    run(&mut s, "CREATE TABLE t (id INT, s VARCHAR(10))");
    run(&mut s, "INSERT INTO t VALUES (1, 'a')");
    (ws, s)
}

#[test]
fn constant_columns_are_named_by_their_sql_text() {
    let (_ws, mut s) = session("constants");
    assert_eq!(column_names(&mut s, "SELECT 1"), ["1"]);
    assert_eq!(column_names(&mut s, "SELECT 1 UNION SELECT 1"), ["1"]);
    assert_eq!(
        column_names(&mut s, "SELECT 'it''s', 2.5, TRUE, NULL, id FROM t"),
        ["'it''s'", "2.5", "TRUE", "NULL", "id"]
    );
}

#[test]
fn alter_table_reports_the_change_in_words() {
    let (_ws, mut s) = session("alter");
    let added = run(&mut s, "ALTER TABLE t ADD COLUMN c INT").messages;
    assert!(
        added
            .iter()
            .any(|m| m == "Altered table 't': added column 'c' INT."),
        "{:?}",
        added
    );
    let renamed = run(&mut s, "ALTER TABLE t RENAME COLUMN c TO c2").messages;
    assert!(
        renamed
            .iter()
            .any(|m| m == "Altered table 't': renamed column 'c' to 'c2'."),
        "{:?}",
        renamed
    );
}

#[test]
fn like_on_a_non_string_is_a_type_error() {
    let (_ws, mut s) = session("like");
    let err = exec(&mut s, "SELECT * FROM t WHERE id LIKE '%'").expect_err("LIKE on INT must fail");
    assert!(err.to_string().contains("LIKE needs a string"), "{}", err);
    // Strings still match.
    assert_eq!(
        run(&mut s, "SELECT * FROM t WHERE s LIKE 'a%'")
            .rows
            .unwrap()
            .1
            .len(),
        1
    );
}

#[test]
fn mysql_index_entries_are_reported_as_a_warning() {
    let (_ws, mut s) = session("mysql_index");
    let created = run(
        &mut s,
        "CREATE TABLE z (id INT, s VARCHAR(10) DEFAULT 'a,b', INDEX idx_s (s))",
    );
    assert!(
        created
            .messages
            .iter()
            .any(|m| m.starts_with("Warning: MySQL index definition(s) ignored")),
        "{:?}",
        created.messages
    );
    run(&mut s, "INSERT INTO z (id) VALUES (1)");
    let (_, rows) = run(&mut s, "SELECT s FROM z").rows.unwrap();
    assert_eq!(
        rows[0].values[0],
        Some(storage_manager::types::DataValue::Varchar("a,b".into()))
    );
}
