// ============================================================================
// test_in_like.rs — IN clause and LIKE pattern matching
//
// IN tests:
//   - x IN (1,2,3) → True/False
//   - x IN (1,NULL) no match → Unknown (has_null)
//   - x IN (1,NULL) match   → True (short-circuits past NULL)
//   - NULL IN (...)          → Unknown (column is null)
//   - Empty list             → False / Unknown for NULL
//   - Large list (linear scan)
//
// LIKE fast-path tests (StartsWith / EndsWith / Contains):
//   - 'abc%'  → StartsWith
//   - '%xyz'  → EndsWith
//   - '%mid%' → Contains
//   - 'hello' → exact (no wildcards)
//
// LIKE regex-fallback (pattern with `_`):
//   - 'h_llo'    → single char wildcard
//   - 'a%b_c'    → mixed % and _
//
// LIKE error / NULL:
//   - LIKE on INT column → Err at plan time
//   - LIKE on NULL column → Unknown
// ============================================================================

use storage_manager::catalog::types::{Column, Table};
use storage_manager::executor::selection::{
    ColumnReference, Constant, Expr, Predicate, ComparisonOp,
    SelectionExecutor, TriValue,
};
use storage_manager::types::{DataType, serialize_nullable_row};

// ── helpers ───────────────────────────────────────────────────────────────────

fn col(name: &str) -> Box<Expr> {
    Box::new(Expr::Column(ColumnReference::new(name.to_string())))
}

// Note: Predicate::In takes Vec<Expr> (not Box), so these return Expr (not Box<Expr>)
fn int_e(v: i32) -> Expr { Expr::Constant(Constant::Int(v)) }
fn null_e() -> Expr      { Expr::Constant(Constant::Null) }

fn int_table() -> (Vec<DataType>, Table) {
    (
        vec![DataType::Int],
        Table { columns: vec![Column::new("x".to_string(), DataType::Int)] },
    )
}

fn varchar_table() -> (Vec<DataType>, Table) {
    (
        vec![DataType::Varchar(128)],
        Table { columns: vec![Column::new("s".to_string(), DataType::Varchar(128))] },
    )
}

fn int_row(v: i32, schema: &[DataType]) -> Vec<u8> {
    serialize_nullable_row(schema, &[Some(&v.to_string())]).unwrap()
}
fn null_row(schema: &[DataType]) -> Vec<u8> {
    serialize_nullable_row(schema, &[None]).unwrap()
}
fn text_row(s: &str, schema: &[DataType]) -> Vec<u8> {
    serialize_nullable_row(schema, &[Some(s)]).unwrap()
}

// ── IN with integers ──────────────────────────────────────────────────────────

#[test]
fn in_each_listed_value_returns_true() {
    let (schema, table) = int_table();
    let pred = Predicate::In(col("x"), vec![int_e(1), int_e(2), int_e(3)]);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(1, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&int_row(2, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&int_row(3, &schema)).unwrap(), TriValue::True);
}

#[test]
fn in_no_match_no_null_is_false() {
    let (schema, table) = int_table();
    let pred = Predicate::In(col("x"), vec![int_e(1), int_e(2), int_e(3)]);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(4, &schema)).unwrap(), TriValue::False);
    assert_eq!(exec.evaluate_tuple(&int_row(0, &schema)).unwrap(), TriValue::False);
}

#[test]
fn in_no_match_with_null_in_list_is_unknown() {
    // x=5 IN (1, NULL) → no exact match, but has_null=true → Unknown
    let (schema, table) = int_table();
    let pred = Predicate::In(col("x"), vec![int_e(1), null_e()]);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(5, &schema)).unwrap(), TriValue::Unknown);
}

#[test]
fn in_match_with_null_in_list_is_true() {
    // x=1 IN (1, NULL) → match found → True (short-circuit before reaching NULL)
    let (schema, table) = int_table();
    let pred = Predicate::In(col("x"), vec![int_e(1), null_e()]);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(1, &schema)).unwrap(), TriValue::True);
}

#[test]
fn null_column_in_any_list_is_unknown() {
    // NULL IN (1, 2, 3) → Unknown (column itself is null)
    let (schema, table) = int_table();
    let pred = Predicate::In(col("x"), vec![int_e(1), int_e(2), int_e(3)]);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::Unknown);
}

#[test]
fn null_column_in_null_list_is_unknown() {
    // NULL IN (1, NULL) → column is null → Unknown
    let (schema, table) = int_table();
    let pred = Predicate::In(col("x"), vec![int_e(1), null_e()]);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::Unknown);
}

#[test]
fn in_empty_list_non_null_column_is_false() {
    let (schema, table) = int_table();
    let pred = Predicate::In(col("x"), vec![]);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(1, &schema)).unwrap(), TriValue::False);
}

#[test]
fn in_empty_list_null_column_is_unknown() {
    let (schema, table) = int_table();
    let pred = Predicate::In(col("x"), vec![]);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::Unknown);
}

#[test]
fn in_single_element_list_works() {
    let (schema, table) = int_table();
    let pred = Predicate::In(col("x"), vec![int_e(42)]);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&int_row(42, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&int_row(0, &schema)).unwrap(), TriValue::False);
}

#[test]
fn in_large_list_linear_scan_correct() {
    // 50-element IN list: verify all elements match and 50 does not
    let (schema, table) = int_table();
    let list: Vec<Expr> = (0..50).map(int_e).collect();
    let pred = Predicate::In(col("x"), list);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    for v in 0..50i32 {
        assert_eq!(
            exec.evaluate_tuple(&int_row(v, &schema)).unwrap(),
            TriValue::True,
            "x={v} should match"
        );
    }
    assert_eq!(exec.evaluate_tuple(&int_row(50, &schema)).unwrap(), TriValue::False);
}

// ── LIKE — StartsWith fast path ───────────────────────────────────────────────

#[test]
fn like_startswith_matches() {
    let (schema, table) = varchar_table();
    let pred = Predicate::Like(col("s"), "hello%".to_string(), None);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&text_row("hello world", &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&text_row("hello", &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&text_row("hell", &schema)).unwrap(), TriValue::False);
    assert_eq!(exec.evaluate_tuple(&text_row("world hello", &schema)).unwrap(), TriValue::False);
}

#[test]
fn like_bare_percent_matches_everything() {
    let (schema, table) = varchar_table();
    let pred = Predicate::Like(col("s"), "%".to_string(), None);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&text_row("anything", &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&text_row("", &schema)).unwrap(), TriValue::True);
}

// ── LIKE — EndsWith fast path ─────────────────────────────────────────────────

#[test]
fn like_endswith_matches() {
    let (schema, table) = varchar_table();
    let pred = Predicate::Like(col("s"), "%world".to_string(), None);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&text_row("hello world", &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&text_row("world", &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&text_row("hello worlds", &schema)).unwrap(), TriValue::False);
}

// ── LIKE — Contains fast path ─────────────────────────────────────────────────

#[test]
fn like_contains_matches() {
    let (schema, table) = varchar_table();
    let pred = Predicate::Like(col("s"), "%mid%".to_string(), None);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&text_row("prefix mid suffix", &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&text_row("mid", &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&text_row("no match here", &schema)).unwrap(), TriValue::False);
}

// ── LIKE — exact match (no wildcards) ────────────────────────────────────────

#[test]
fn like_exact_no_wildcards() {
    let (schema, table) = varchar_table();
    let pred = Predicate::Like(col("s"), "hello".to_string(), None);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&text_row("hello", &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&text_row("hello!", &schema)).unwrap(), TriValue::False);
    assert_eq!(exec.evaluate_tuple(&text_row("hell", &schema)).unwrap(), TriValue::False);
}

// ── LIKE — Regex fallback (`_` = exactly one character) ──────────────────────

#[test]
fn like_underscore_is_single_char_wildcard() {
    let (schema, table) = varchar_table();
    let pred = Predicate::Like(col("s"), "h_llo".to_string(), None);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&text_row("hello", &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&text_row("hallo", &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&text_row("hllo", &schema)).unwrap(), TriValue::False);  // too short
    assert_eq!(exec.evaluate_tuple(&text_row("heello", &schema)).unwrap(), TriValue::False); // too long
}

#[test]
fn like_mixed_percent_and_underscore_regex() {
    // 'a%b_c' — both % and _ triggers the regex path
    let (schema, table) = varchar_table();
    let pred = Predicate::Like(col("s"), "a%b_c".to_string(), None);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&text_row("aXYZbZc", &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&text_row("abXc", &schema)).unwrap(), TriValue::True);   // 'a' + '' + 'b' + 'X' + 'c'
    assert_eq!(exec.evaluate_tuple(&text_row("abc", &schema)).unwrap(), TriValue::False);   // no char for _
}

// ── LIKE — NULL handling ──────────────────────────────────────────────────────

#[test]
fn like_on_null_column_is_unknown() {
    let (schema, table) = varchar_table();
    let pred = Predicate::Like(col("s"), "abc%".to_string(), None);
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&null_row(&schema)).unwrap(), TriValue::Unknown);
}

// ── LIKE on INT column is a planning error ────────────────────────────────────

#[test]
fn like_on_int_column_is_err_at_planning() {
    let (_, table) = int_table();
    let pred = Predicate::Like(col("x"), "abc%".to_string(), None);
    let result = SelectionExecutor::new(pred, table);
    assert!(result.is_err(), "LIKE on INT column should fail at plan time");
}
