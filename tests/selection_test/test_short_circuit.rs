// ============================================================================
// test_short_circuit.rs — AND/OR jump short-circuit correctness
//
// The VM compiles:
//   AND → left | JumpIfFalse(end) | right | And
//   OR  → left | JumpIfTrue(end)  | right | Or
//
// Short-circuit means: when LHS already determines the result, the RHS
// bytecode is SKIPPED entirely via the Jump instruction.
//
// We verify result correctness across all True/False/Unknown cross-products
// and with deeply nested predicates.
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
fn int_c(v: i32) -> Box<Expr> {
    Box::new(Expr::Constant(Constant::Int(v)))
}

fn two_int_schema() -> (Vec<DataType>, Table) {
    (
        vec![DataType::Int, DataType::Int],
        Table {
            columns: vec![
                Column::new("a".to_string(), DataType::Int),
                Column::new("b".to_string(), DataType::Int),
            ],
        },
    )
}

fn row2(a: i32, b: i32, schema: &[DataType]) -> Vec<u8> {
    serialize_nullable_row(schema, &[Some(&a.to_string()), Some(&b.to_string())]).unwrap()
}

fn row_a_null_b(b: i32, schema: &[DataType]) -> Vec<u8> {
    serialize_nullable_row(schema, &[None, Some(&b.to_string())]).unwrap()
}

// ── AND: all four result combinations ────────────────────────────────────────

#[test]
fn and_true_true_is_true() {
    let (schema, table) = two_int_schema();
    let pred = Predicate::and(
        Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(1)),
        Predicate::Compare(col("b"), ComparisonOp::Equals, int_c(1)),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row2(1, 1, &schema)).unwrap(), TriValue::True);
}

#[test]
fn and_true_false_is_false() {
    let (schema, table) = two_int_schema();
    let pred = Predicate::and(
        Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(1)),
        Predicate::Compare(col("b"), ComparisonOp::Equals, int_c(1)),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row2(1, 0, &schema)).unwrap(), TriValue::False);
}

#[test]
fn and_false_rhs_skipped_still_false() {
    // a=0 → LHS False → JumpIfFalse fires → RHS not evaluated → result = False
    let (schema, table) = two_int_schema();
    let pred = Predicate::and(
        Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(1)), // False for a=0
        Predicate::Compare(col("b"), ComparisonOp::Equals, int_c(1)), // irrelevant
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row2(0, 1, &schema)).unwrap(), TriValue::False); // b=1 skipped
    assert_eq!(exec.evaluate_tuple(&row2(0, 0, &schema)).unwrap(), TriValue::False); // b=0 skipped
}

#[test]
fn and_false_const_rhs_is_false_regardless_of_rhs() {
    // FALSE AND TRUE must = False (constant TRUE on RHS, short-circuited)
    let (schema, table) = two_int_schema();
    let pred = Predicate::and(
        Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(999)), // always False (a=0)
        Predicate::Compare(int_c(1), ComparisonOp::Equals, int_c(1)),   // always True  (const)
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row2(0, 0, &schema)).unwrap(), TriValue::False);
}

// ── OR: all four result combinations ─────────────────────────────────────────

#[test]
fn or_false_false_is_false() {
    let (schema, table) = two_int_schema();
    let pred = Predicate::or(
        Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(1)),
        Predicate::Compare(col("b"), ComparisonOp::Equals, int_c(999)),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row2(0, 0, &schema)).unwrap(), TriValue::False);
}

#[test]
fn or_false_true_is_true() {
    let (schema, table) = two_int_schema();
    let pred = Predicate::or(
        Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(1)),
        Predicate::Compare(col("b"), ComparisonOp::Equals, int_c(999)),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row2(0, 999, &schema)).unwrap(), TriValue::True);
}

#[test]
fn or_true_rhs_skipped_still_true() {
    // a=1 → LHS True → JumpIfTrue fires → RHS not evaluated → result = True
    let (schema, table) = two_int_schema();
    let pred = Predicate::or(
        Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(1)),
        Predicate::Compare(col("b"), ComparisonOp::Equals, int_c(999)), // irrelevant
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row2(1, 0, &schema)).unwrap(), TriValue::True);   // b=0 skipped
    assert_eq!(exec.evaluate_tuple(&row2(1, 999, &schema)).unwrap(), TriValue::True); // b=999 skipped
}

// ── AND with Unknown (3VL rules apply even with short-circuit) ─────────────────

#[test]
fn unknown_and_false_is_false() {
    // a=NULL → Unknown; b=0 → b=999? False; Unknown AND False = False
    let (schema, table) = two_int_schema();
    let pred = Predicate::and(
        Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(5)), // Unknown (a=NULL)
        Predicate::Compare(col("b"), ComparisonOp::Equals, int_c(999)), // b=0 → False
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    let row = row_a_null_b(0, &schema);
    assert_eq!(exec.evaluate_tuple(&row).unwrap(), TriValue::False);
}

#[test]
fn unknown_and_true_is_unknown() {
    // a=NULL → Unknown; 1=1 → True; Unknown AND True = Unknown
    let (schema, table) = two_int_schema();
    let pred = Predicate::and(
        Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(5)), // Unknown
        Predicate::Compare(int_c(1), ComparisonOp::Equals, int_c(1)), // True (const)
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    let row = row_a_null_b(0, &schema);
    assert_eq!(exec.evaluate_tuple(&row).unwrap(), TriValue::Unknown);
}

#[test]
fn unknown_or_true_is_true() {
    // a=NULL → Unknown; 1=1 → True; Unknown OR True = True
    let (schema, table) = two_int_schema();
    let pred = Predicate::or(
        Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(5)), // Unknown
        Predicate::Compare(int_c(1), ComparisonOp::Equals, int_c(1)), // True (const)
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    let row = row_a_null_b(0, &schema);
    assert_eq!(exec.evaluate_tuple(&row).unwrap(), TriValue::True);
}

#[test]
fn unknown_or_false_is_unknown() {
    // a=NULL → Unknown; 1=2 → False; Unknown OR False = Unknown
    let (schema, table) = two_int_schema();
    let pred = Predicate::or(
        Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(5)), // Unknown
        Predicate::Compare(int_c(1), ComparisonOp::Equals, int_c(2)), // False (const)
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    let row = row_a_null_b(0, &schema);
    assert_eq!(exec.evaluate_tuple(&row).unwrap(), TriValue::Unknown);
}

// ── Deeply nested AND chain ───────────────────────────────────────────────────

#[test]
fn deep_and_chain_first_false_terminates_chain() {
    // ((a=1 AND a=2) AND a=3) AND a=4 — no value satisfies all
    let (schema, table) = two_int_schema();
    let pred = Predicate::and(
        Predicate::and(
            Predicate::and(
                Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(1)),
                Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(2)),
            ),
            Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(3)),
        ),
        Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(4)),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row2(0, 0, &schema)).unwrap(), TriValue::False);
    assert_eq!(exec.evaluate_tuple(&row2(1, 0, &schema)).unwrap(), TriValue::False);
}

// ── Deeply nested OR chain ────────────────────────────────────────────────────

#[test]
fn deep_or_chain_first_true_terminates_chain() {
    // ((a=1 OR a=2) OR a=3) OR a=4
    let (schema, table) = two_int_schema();
    let pred = Predicate::or(
        Predicate::or(
            Predicate::or(
                Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(1)),
                Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(2)),
            ),
            Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(3)),
        ),
        Predicate::Compare(col("a"), ComparisonOp::Equals, int_c(4)),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row2(1, 0, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&row2(4, 0, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&row2(5, 0, &schema)).unwrap(), TriValue::False);
}

// ── Mixed AND/OR nesting ──────────────────────────────────────────────────────

#[test]
fn and_or_mixed_nesting() {
    // (a > 10 OR b > 10) AND a < 100
    let (schema, table) = two_int_schema();
    let pred = Predicate::and(
        Predicate::or(
            Predicate::Compare(col("a"), ComparisonOp::GreaterThan, int_c(10)),
            Predicate::Compare(col("b"), ComparisonOp::GreaterThan, int_c(10)),
        ),
        Predicate::Compare(col("a"), ComparisonOp::LessThan, int_c(100)),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();

    assert_eq!(exec.evaluate_tuple(&row2(50, 0, &schema)).unwrap(), TriValue::True);   // a>10 and a<100
    assert_eq!(exec.evaluate_tuple(&row2(5, 20, &schema)).unwrap(), TriValue::True);   // b>10 and a<100
    assert_eq!(exec.evaluate_tuple(&row2(5, 5, &schema)).unwrap(), TriValue::False);   // neither > 10
    assert_eq!(exec.evaluate_tuple(&row2(150, 0, &schema)).unwrap(), TriValue::False); // a>=100 fails
}

// ── NOT inside AND ─────────────────────────────────────────────────────────────

#[test]
fn not_inside_and_chain() {
    // NOT(a > 100) AND a > 0
    let (schema, table) = two_int_schema();
    let pred = Predicate::and(
        Predicate::not(Predicate::Compare(col("a"), ComparisonOp::GreaterThan, int_c(100))),
        Predicate::Compare(col("a"), ComparisonOp::GreaterThan, int_c(0)),
    );
    let exec = SelectionExecutor::new(pred, table).unwrap();
    assert_eq!(exec.evaluate_tuple(&row2(50, 0, &schema)).unwrap(), TriValue::True);
    assert_eq!(exec.evaluate_tuple(&row2(101, 0, &schema)).unwrap(), TriValue::False); // NOT(false)=true → LHS true, but wait: NOT(a>100) with a=101 → NOT(True)=False → short-circuit
    assert_eq!(exec.evaluate_tuple(&row2(0, 0, &schema)).unwrap(), TriValue::False);  // a>0 is False
}
