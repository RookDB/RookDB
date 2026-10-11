//! A constant division by zero is an error, exactly like `x / 0` on a column.
//! The optimizer used to fold it to NULL, so `SELECT 10 / 0` failed typing
//! with "Cannot infer type for NULL literal" and `WHERE 10 / 0 = 1` matched
//! nothing instead of failing.

mod common;

use storage_manager::backend::session::Session;

fn error(s: &mut Session, sql: &str) -> String {
    match s.execute(rook_parser::parse_sql(sql).unwrap()) {
        Ok(_) => panic!("{}: expected an error", sql),
        Err(e) => e.to_string(),
    }
}

fn session(tag: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("div0", tag);
    let mut s = Session::new(common::load_catalog());
    for sql in [
        "CREATE DATABASE dz",
        "USE dz",
        "CREATE TABLE a (id INT, f DOUBLE PRECISION)",
        "INSERT INTO a VALUES (1, 1.5), (2, 2.5)",
    ] {
        s.execute(rook_parser::parse_sql(sql).unwrap())
            .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    }
    (ws, s)
}

#[test]
fn constant_division_by_zero_reports_division_by_zero() {
    let (_ws, mut s) = session("const");
    let by_column = error(&mut s, "SELECT id / 0 FROM a");
    assert!(by_column.contains("Division by zero"), "{}", by_column);

    for sql in [
        "SELECT 10 / 0 FROM a",
        "SELECT 10.0 / 0 FROM a",
        "SELECT 10 / 0.0 FROM a",
        "SELECT id + 10 / 0 FROM a",
        "SELECT id FROM a WHERE 10 / 0 = 1",
        "SELECT id FROM a ORDER BY 10 / 0",
        "SELECT SUM(10 / 0) FROM a",
        "SELECT 10 % 0 FROM a",
        "SELECT MOD(10, 0) FROM a",
        "SELECT id % 0 FROM a",
    ] {
        assert_eq!(error(&mut s, sql), by_column, "{}", sql);
    }
}

#[test]
fn constant_division_still_folds_when_the_divisor_is_not_zero() {
    let (_ws, mut s) = session("fold");
    let result = s
        .execute(rook_parser::parse_sql("SELECT 10 / 4, 10 / 4.0 FROM a WHERE id = 1").unwrap())
        .unwrap();
    let (_, rows) = result.rows.unwrap();
    let shown: Vec<String> = rows[0]
        .values
        .iter()
        .map(|v| v.as_ref().unwrap().to_string())
        .collect();
    assert_eq!(shown, ["2", "2.5"]);
}
