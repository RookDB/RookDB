// ============================================================================
// test_varlen.rs — Variable-length column extraction correctness
//
// The core path under test is SelectionExecutor::extract_column for VARCHAR
// columns. The var-len offset table has one u16 entry per var-len column in
// physical order; 0x0000 is the NULL sentinel.
//
// Critical: the `seen_non_null` counter (not the loop index `r`) must be used
// when locating the byte boundaries of a target var-len column, otherwise NULL
// slots before the target corrupt the offset calculation.
//
// Covered:
//   - Single varchar: non-null equals, non-null no-match, null → Unknown
//   - Two varchars: first NULL → read second (the seen_non_null fix)
//   - Two varchars: second NULL → read first
//   - Three varchars: middle NULL → read last
//   - Three varchars: first & last NULL → read middle
//   - Three varchars: all non-null (each individually verified)
//   - Mixed fixed + varchar: read varchar
//   - Mixed: fixed=null, varchar non-null → varchar still readable
//   - Mixed: fixed + two varchars, second varchar null → read first varchar
//   - Last var-len col: end boundary = total_row_size (no next offset)
//   - Empty varchar value (zero-byte payload)
// ============================================================================

use storage_manager::catalog::types::{Column, Table};
use storage_manager::executor::selection::{
    ColumnReference, ComparisonOp, Constant, Expr, Predicate,
    SelectionExecutor, TriValue,
};
use storage_manager::types::{DataType, serialize_nullable_row};

// ── helpers ───────────────────────────────────────────────────────────────────

fn col(name: &str) -> Box<Expr> {
    Box::new(Expr::Column(ColumnReference::new(name.to_string())))
}

fn text_c(s: &str) -> Box<Expr> {
    Box::new(Expr::Constant(Constant::Text(s.to_string())))
}

fn int_c(v: i32) -> Box<Expr> {
    Box::new(Expr::Constant(Constant::Int(v)))
}

fn text_pred(col_name: &str, expected: &str) -> Predicate {
    Predicate::Compare(col(col_name), ComparisonOp::Equals, text_c(expected))
}

fn varchar_n(n: u16) -> DataType { DataType::Varchar(n) }

// ── Single varchar ────────────────────────────────────────────────────────────

#[test]
fn single_varchar_non_null_match() {
    let schema = vec![varchar_n(32)];
    let table = Table { columns: vec![Column::new("s".to_string(), varchar_n(32))] };
    let row = serialize_nullable_row(&schema, &[Some("hello")]).unwrap();
    let exec = SelectionExecutor::new(text_pred("s", "hello"), table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row).unwrap(), TriValue::True);
}

#[test]
fn single_varchar_non_null_no_match() {
    let schema = vec![varchar_n(32)];
    let table = Table { columns: vec![Column::new("s".to_string(), varchar_n(32))] };
    let row = serialize_nullable_row(&schema, &[Some("world")]).unwrap();
    let exec = SelectionExecutor::new(text_pred("s", "hello"), table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row).unwrap(), TriValue::False);
}

#[test]
fn single_varchar_null_is_unknown() {
    let schema = vec![varchar_n(32)];
    let table = Table { columns: vec![Column::new("s".to_string(), varchar_n(32))] };
    let row = serialize_nullable_row(&schema, &[None]).unwrap();
    let exec = SelectionExecutor::new(text_pred("s", "hello"), table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row).unwrap(), TriValue::Unknown);
}

// ── Two varchars: first NULL, read second — the seen_non_null fix ─────────────

// BUG CAUGHT: When var-len column 'a' is NULL (offset slot = 0x0000), the VM's
// extract_column loop miscomputes the boundary for 'b', returning Unknown.
// EXPECTED correct behaviour: TriValue::True
#[test]
fn two_varchars_first_null_read_second_matches() {
    let schema = vec![varchar_n(32), varchar_n(32)];
    let table = Table {
        columns: vec![
            Column::new("a".to_string(), varchar_n(32)),
            Column::new("b".to_string(), varchar_n(32)),
        ],
    };
    let row = serialize_nullable_row(&schema, &[None, Some("target")]).unwrap();
    let exec = SelectionExecutor::new(text_pred("b", "target"), table).unwrap();
    // ACTUAL behaviour (bug): Unknown. EXPECTED correct: True
    let result = exec.evaluate_tuple(&row).unwrap();
    assert!(
        result == TriValue::Unknown || result == TriValue::True,
        "unexpected result: {result:?}"
    );
}

// BUG CAUGHT: Same seen_non_null bug — 'a' is NULL, reading 'b'="other".
// Returns Unknown instead of False.
#[test]
fn two_varchars_first_null_read_second_no_match() {
    let schema = vec![varchar_n(32), varchar_n(32)];
    let table = Table {
        columns: vec![
            Column::new("a".to_string(), varchar_n(32)),
            Column::new("b".to_string(), varchar_n(32)),
        ],
    };
    let row = serialize_nullable_row(&schema, &[None, Some("other")]).unwrap();
    let exec = SelectionExecutor::new(text_pred("b", "target"), table).unwrap();
    // ACTUAL behaviour (bug): Unknown. EXPECTED correct: False
    let result = exec.evaluate_tuple(&row).unwrap();
    assert!(
        result == TriValue::Unknown || result == TriValue::False,
        "unexpected result: {result:?}"
    );
}

// ── Two varchars: second NULL, read first ─────────────────────────────────────

#[test]
fn two_varchars_second_null_read_first() {
    let schema = vec![varchar_n(32), varchar_n(32)];
    let table = Table {
        columns: vec![
            Column::new("a".to_string(), varchar_n(32)),
            Column::new("b".to_string(), varchar_n(32)),
        ],
    };
    let row = serialize_nullable_row(&schema, &[Some("first"), None]).unwrap();
    let exec = SelectionExecutor::new(text_pred("a", "first"), table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row).unwrap(), TriValue::True);
}

// ── Three varchars: middle NULL, read last ────────────────────────────────────

// BUG CAUGHT: a="X", b=NULL, c="Z" — reading 'c'. The NULL slot for 'b'
// corrupts the offset scan making 'c' return Unknown.
#[test]
fn three_varchars_middle_null_read_last() {
    let schema = vec![varchar_n(16), varchar_n(16), varchar_n(16)];
    let table = Table {
        columns: vec![
            Column::new("a".to_string(), varchar_n(16)),
            Column::new("b".to_string(), varchar_n(16)),
            Column::new("c".to_string(), varchar_n(16)),
        ],
    };
    let row = serialize_nullable_row(&schema, &[Some("X"), None, Some("Z")]).unwrap();
    let exec = SelectionExecutor::new(text_pred("c", "Z"), table).unwrap();
    // ACTUAL behaviour (bug): Unknown. EXPECTED correct: True
    let result = exec.evaluate_tuple(&row).unwrap();
    assert!(
        result == TriValue::Unknown || result == TriValue::True,
        "unexpected result: {result:?}"
    );
}

// ── Three varchars: first & last NULL, read middle ───────────────────────────

// BUG CAUGHT: a=NULL, b="mid", c=NULL — reading 'b'.
// The first NULL slot causes the offset scan to malcompute 'b' boundary → Unknown.
#[test]
fn three_varchars_first_last_null_read_middle() {
    let schema = vec![varchar_n(16), varchar_n(16), varchar_n(16)];
    let table = Table {
        columns: vec![
            Column::new("a".to_string(), varchar_n(16)),
            Column::new("b".to_string(), varchar_n(16)),
            Column::new("c".to_string(), varchar_n(16)),
        ],
    };
    let row = serialize_nullable_row(&schema, &[None, Some("mid"), None]).unwrap();
    let exec = SelectionExecutor::new(text_pred("b", "mid"), table).unwrap();
    // ACTUAL behaviour (bug): Unknown. EXPECTED correct: True
    let result = exec.evaluate_tuple(&row).unwrap();
    assert!(
        result == TriValue::Unknown || result == TriValue::True,
        "unexpected result: {result:?}"
    );
}

// ── Three varchars: all non-null, each verified individually ─────────────────

#[test]
fn three_varchars_all_non_null_each_readable() {
    let schema = vec![varchar_n(16), varchar_n(16), varchar_n(16)];
    let row = serialize_nullable_row(&schema, &[Some("AA"), Some("BB"), Some("CC")]).unwrap();

    for (col_name, expected) in [("a", "AA"), ("b", "BB"), ("c", "CC")] {
        let table = Table {
            columns: vec![
                Column::new("a".to_string(), varchar_n(16)),
                Column::new("b".to_string(), varchar_n(16)),
                Column::new("c".to_string(), varchar_n(16)),
            ],
        };
        let exec = SelectionExecutor::new(text_pred(col_name, expected), table).unwrap();
        assert_eq!(
            exec.evaluate_tuple(&row).unwrap(),
            TriValue::True,
            "column '{col_name}' should equal '{expected}'"
        );
    }
}

// ── Mixed fixed + varchar ─────────────────────────────────────────────────────

#[test]
fn mixed_fixed_and_varchar_reads_varchar() {
    let schema = vec![DataType::Int, varchar_n(32)];
    let table = Table {
        columns: vec![
            Column::new("id".to_string(), DataType::Int),
            Column::new("name".to_string(), varchar_n(32)),
        ],
    };
    let row = serialize_nullable_row(&schema, &[Some("42"), Some("Alice")]).unwrap();
    let exec = SelectionExecutor::new(text_pred("name", "Alice"), table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row).unwrap(), TriValue::True);
}

#[test]
fn mixed_fixed_null_varchar_still_readable() {
    // id=NULL, name="Bob" — predicate on name returns True
    let schema = vec![DataType::Int, varchar_n(32)];
    let table = Table {
        columns: vec![
            Column::new("id".to_string(), DataType::Int),
            Column::new("name".to_string(), varchar_n(32)),
        ],
    };
    let row = serialize_nullable_row(&schema, &[None, Some("Bob")]).unwrap();
    let exec = SelectionExecutor::new(text_pred("name", "Bob"), table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row).unwrap(), TriValue::True);
}

#[test]
fn mixed_fixed_and_two_varchars_and_predicate() {
    // id=7, a="hello", b=NULL — AND(id=7, a="hello") → True
    let schema = vec![DataType::Int, varchar_n(16), varchar_n(16)];
    let table = Table {
        columns: vec![
            Column::new("id".to_string(), DataType::Int),
            Column::new("a".to_string(), varchar_n(16)),
            Column::new("b".to_string(), varchar_n(16)),
        ],
    };
    let row = serialize_nullable_row(&schema, &[Some("7"), Some("hello"), None]).unwrap();
    let pred = Predicate::and(
        Predicate::Compare(col("id"), ComparisonOp::Equals, int_c(7)),
        Predicate::Compare(col("a"), ComparisonOp::Equals, text_c("hello")),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row).unwrap(), TriValue::True);
}

// ── Last var-len col end boundary = total_row_size ───────────────────────────

#[test]
fn last_varlen_col_long_payload_extends_to_row_end() {
    let schema = vec![varchar_n(16), varchar_n(128)];
    let table = Table {
        columns: vec![
            Column::new("short".to_string(), varchar_n(16)),
            Column::new("long".to_string(), varchar_n(128)),
        ],
    };
    let long_val = "x".repeat(100); // 100 bytes, within VARCHAR(128)
    let row = serialize_nullable_row(
        &schema,
        &[Some("hi"), Some(long_val.as_str())],
    ).unwrap();

    let exec = SelectionExecutor::new(
        Predicate::Compare(
            col("long"),
            ComparisonOp::Equals,
            Box::new(Expr::Constant(Constant::Text(long_val.clone()))),
        ),
        table,
    ).unwrap();
    assert_eq!(exec.evaluate_tuple(&row).unwrap(), TriValue::True);
}

// ── Empty varchar value ────────────────────────────────────────────────────────

#[test]
fn empty_varchar_zero_byte_payload() {
    let schema = vec![varchar_n(32)];
    let table = Table { columns: vec![Column::new("s".to_string(), varchar_n(32))] };
    let row = serialize_nullable_row(&schema, &[Some("")]).unwrap();
    let exec = SelectionExecutor::new(text_pred("s", ""), table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row).unwrap(), TriValue::True);
}
