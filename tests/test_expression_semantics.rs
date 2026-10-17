//! SQL expression semantics through the whole pipeline: constant folding,
//! GROUP BY expressions, aggregates nested in expressions, declared types,
//! NUMERIC literals and string functions.

mod common;

use storage_manager::backend::session::Session;
use storage_manager::types::datatype::DataType;

/// Run `sql`; the result rows as text (`NULL` for NULL), or the error.
fn try_run(s: &mut Session, sql: &str) -> Result<Vec<Vec<String>>, String> {
    let plan = rook_parser::parse_sql(sql)?;
    let result = s.execute(plan).map_err(|e| e.to_string())?;
    Ok(result
        .rows
        .map(|(_, rows)| {
            rows.into_iter()
                .map(|t| {
                    t.values
                        .iter()
                        .map(|v| v.as_ref().map_or("NULL".to_string(), |d| d.to_string()))
                        .collect()
                })
                .collect()
        })
        .unwrap_or_default())
}

fn run(s: &mut Session, sql: &str) -> Vec<Vec<String>> {
    try_run(s, sql).unwrap_or_else(|e| panic!("{}: {}", sql, e))
}

/// The declared types of a query's output columns.
fn types(s: &mut Session, sql: &str) -> Vec<DataType> {
    let result = s.execute(rook_parser::parse_sql(sql).unwrap()).unwrap();
    let (schema, _) = result.rows.expect("rows");
    schema.into_iter().map(|c| c.data_type).collect()
}

fn rows(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|r| r.iter().map(|v| v.to_string()).collect())
        .collect()
}

fn session(tag: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("exprsem", tag);
    let mut s = Session::new(common::load_catalog());
    for sql in [
        "CREATE DATABASE es",
        "USE es",
        "CREATE TABLE a (id INT, x INT, s VARCHAR(10), g VARCHAR(5))",
        "INSERT INTO a VALUES (1, NULL, 'p', 'a'), (2, 5, 'q', 'a'), (3, 0, 'r', 'b')",
        "CREATE TABLE b (id VARCHAR(10), y INT, k INT, v INT)",
        "INSERT INTO b VALUES ('one', 1, 5, 100), ('two', 2, 0, 200)",
    ] {
        run(&mut s, sql);
    }
    (ws, s)
}

#[test]
fn product_with_zero_keeps_null_and_errors() {
    let (_ws, mut s) = session("zero");
    assert_eq!(
        run(&mut s, "SELECT id, x * 0, 0 * x FROM a ORDER BY id"),
        rows(&[&["1", "NULL", "NULL"], &["2", "0", "0"], &["3", "0", "0"]])
    );
    assert_eq!(
        run(&mut s, "SELECT id FROM a WHERE x * 0 = 0 ORDER BY id"),
        rows(&[&["2"], &["3"]])
    );
    assert!(try_run(&mut s, "SELECT (1 / 0) * 0").is_err());
    assert!(try_run(&mut s, "SELECT s + 0 FROM a").is_err());
}

#[test]
fn constant_overflow_is_an_error() {
    let (_ws, mut s) = session("overflow");
    assert!(try_run(&mut s, "SELECT 2147483647 + 1").is_err());
    assert!(try_run(&mut s, "SELECT 9223372036854775807 + 1").is_err());
    assert_eq!(
        run(&mut s, "SELECT 3000000000 + 1"),
        rows(&[&["3000000001"]])
    );
    assert!(try_run(&mut s, "SELECT ABS(-2147483648)").is_err());
}

#[test]
fn group_by_scalar_functions_and_case() {
    let (_ws, mut s) = session("groupby");
    assert_eq!(
        run(
            &mut s,
            "SELECT UPPER(g), COUNT(*) FROM a GROUP BY UPPER(g) ORDER BY 1"
        ),
        rows(&[&["'A'", "2"], &["'B'", "1"]])
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT x % 2, COUNT(*) FROM a WHERE x IS NOT NULL GROUP BY x % 2 ORDER BY 1"
        ),
        rows(&[&["0", "1"], &["1", "1"]])
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT CASE WHEN x > 1 THEN 'big' ELSE 'small' END, COUNT(*) FROM a \
             GROUP BY CASE WHEN x > 1 THEN 'big' ELSE 'small' END ORDER BY 2"
        ),
        rows(&[&["'big'", "1"], &["'small'", "2"]])
    );
    assert!(try_run(&mut s, "SELECT COUNT(*) FROM a GROUP BY COUNT(*)").is_err());
    let err = try_run(&mut s, "SELECT MIN(id), g FROM a").unwrap_err();
    assert!(
        err.contains("must appear in the GROUP BY clause"),
        "{}",
        err
    );
}

#[test]
fn aggregates_inside_expressions() {
    let (_ws, mut s) = session("nested_agg");
    assert_eq!(
        run(
            &mut s,
            "SELECT ABS(SUM(x) - 10), COALESCE(MAX(x), 0), MAX(x) > 0 FROM a"
        ),
        rows(&[&["5", "5", "true"]])
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT g, COALESCE(SUM(x), -1) FROM a GROUP BY g ORDER BY g"
        ),
        rows(&[&["'a'", "5"], &["'b'", "0"]])
    );
}

#[test]
fn and_or_short_circuit_in_select_list() {
    let (_ws, mut s) = session("short_circuit");
    assert_eq!(
        run(
            &mut s,
            "SELECT id, x <> 0 AND 10 / x >= 1 FROM a ORDER BY id"
        ),
        rows(&[&["1", "NULL"], &["2", "true"], &["3", "false"]])
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM a WHERE CASE WHEN x = 0 THEN FALSE ELSE 10 / x >= 1 END"
        ),
        rows(&[&["2"]])
    );
}

#[test]
fn declared_types_follow_the_expression() {
    let (_ws, mut s) = session("types");
    assert_eq!(
        types(
            &mut s,
            "SELECT a.id, b.id, b.id || '!' FROM a JOIN b ON a.id = b.y"
        ),
        [
            DataType::Int,
            DataType::Varchar(10),
            DataType::Varchar(u16::MAX)
        ]
    );
    assert_eq!(
        types(
            &mut s,
            "SELECT COALESCE(NULL, x), CASE WHEN x IS NULL THEN NULL ELSE x END FROM a"
        ),
        [DataType::Int, DataType::Int]
    );
    assert_eq!(types(&mut s, "SELECT CURRENT_DATE"), [DataType::Date]);
    assert_eq!(
        run(&mut s, "SELECT CURRENT_DATE > DATE '2020-01-01'"),
        rows(&[&["true"]])
    );
}

#[test]
fn order_by_correlated_subquery_on_unselected_column() {
    let (_ws, mut s) = session("order_subquery");
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM a ORDER BY (SELECT v FROM b WHERE b.k = a.x) DESC"
        ),
        rows(&[&["3"], &["2"], &["1"]])
    );
}

#[test]
fn numeric_literals_round_as_written() {
    let (_ws, mut s) = session("numeric");
    run(&mut s, "CREATE TABLE n (d DECIMAL(6,2))");
    run(&mut s, "INSERT INTO n VALUES (1.005), (-1.005), (2.675)");
    assert_eq!(
        run(&mut s, "SELECT d FROM n"),
        rows(&[&["1.01"], &["-1.01"], &["2.68"]])
    );
}

#[test]
fn substring_start_before_one_counts_toward_the_length() {
    let (_ws, mut s) = session("substring");
    assert_eq!(
        run(
            &mut s,
            "SELECT SUBSTRING('hello', 0, 2), SUBSTRING('hello', -1, 3), SUBSTRING('hello', 2)"
        ),
        rows(&[&["'h'", "'h'", "'ello'"]])
    );
    assert_eq!(run(&mut s, "SELECT s FROM a WHERE s LIKE NULL"), rows(&[]));
}

#[test]
fn date_plus_integer_is_a_date() {
    let (_ws, mut s) = session("date_days");
    run(&mut s, "CREATE TABLE d (v DATE)");
    run(&mut s, "INSERT INTO d VALUES ('2024-02-28')");
    assert_eq!(
        run(&mut s, "SELECT v + 1, 2 + v, v - 28, v + 0 FROM d"),
        rows(&[&["2024-02-29", "2024-03-01", "2024-01-31", "2024-02-28"]])
    );
    assert_eq!(types(&mut s, "SELECT v + 1 FROM d"), [DataType::Date]);
}

#[test]
fn simple_case_matches_the_operand() {
    let (_ws, mut s) = session("simplecase");
    assert_eq!(
        run(
            &mut s,
            "SELECT id, CASE x WHEN 5 THEN 'five' WHEN 0 THEN 'zero' ELSE 'other' END \
             FROM a ORDER BY id"
        ),
        rows(&[&["1", "'other'"], &["2", "'five'"], &["3", "'zero'"]])
    );
}

#[test]
fn limit_offset_forms() {
    let (_ws, mut s) = session("limit");
    assert_eq!(
        run(&mut s, "SELECT id FROM a ORDER BY id LIMIT 1, 2"),
        rows(&[&["2"], &["3"]])
    );
    assert_eq!(
        run(&mut s, "SELECT id FROM a ORDER BY id OFFSET 2"),
        rows(&[&["3"]])
    );
    assert!(try_run(&mut s, "SELECT id FROM a LIMIT -1").is_err());
    assert!(try_run(&mut s, "SELECT id FROM a LIMIT 1 + 1").is_err());
}

#[test]
fn in_subquery_matches_across_numeric_types_and_keeps_null_logic() {
    let (_ws, mut s) = session("inlist");
    run(&mut s, "CREATE TABLE n (big BIGINT, d DOUBLE PRECISION)");
    run(&mut s, "INSERT INTO n VALUES (5, 0.0), (NULL, NULL)");
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM a WHERE x IN (SELECT big FROM n) ORDER BY id"
        ),
        rows(&[&["2"]])
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM a WHERE x IN (SELECT d FROM n) ORDER BY id"
        ),
        rows(&[&["3"]])
    );
    // The NULL in the list makes every non-member UNKNOWN.
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM a WHERE x NOT IN (SELECT big FROM n)"
        ),
        Vec::<Vec<String>>::new()
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM a WHERE x NOT IN (SELECT big FROM n WHERE big IS NOT NULL) \
             ORDER BY id"
        ),
        rows(&[&["3"]])
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT id FROM a WHERE x NOT IN (SELECT big FROM n WHERE big > 100) ORDER BY id"
        ),
        rows(&[&["1"], &["2"], &["3"]])
    );
    assert!(try_run(&mut s, "SELECT id FROM a WHERE x IN (SELECT s FROM a)").is_err());
}
