// Constant folding tests
// normalize_expr() folds constant pairs at planning time (Add/Sub/Mul/Div of two constants).
// NULL operands prevent folding; division by zero is NOT folded (returns error at runtime).

use storage_manager::catalog::{Column, Table};
use storage_manager::executor::selection::*;

fn int_schema() -> Table {
    Table {
        columns: vec![
            Column { name: "id".to_string(), data_type: "INT".to_string() },
        ],
    }
}

fn make_int_tuple(id: i32) -> Vec<u8> {
    let num_cols = 1usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8usize;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let total_length = data_start + 4;
    let mut t = vec![0u8; total_length];
    t[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0;
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&4u32.to_le_bytes());
    t[data_start..data_start+4].copy_from_slice(&id.to_le_bytes());
    t
}

// ── tests ────────────────────────────────────────────────────────────────────

#[test]
fn test_constant_fold_add() {
    // 3 + 7 = 10 folded at planning time; predicate: id > (3+7) ≡ id > 10
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Add(
            Box::new(Expr::Constant(Constant::Int(3))),
            Box::new(Expr::Constant(Constant::Int(7))),
        )),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(11)).unwrap(), TriValue::True);
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(10)).unwrap(), TriValue::False);
}

#[test]
fn test_constant_fold_sub() {
    // 20 - 5 = 15 folded; predicate: id = 15
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Sub(
            Box::new(Expr::Constant(Constant::Int(20))),
            Box::new(Expr::Constant(Constant::Int(5))),
        )),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(15)).unwrap(), TriValue::True);
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(14)).unwrap(), TriValue::False);
}

#[test]
fn test_constant_fold_mul() {
    // 4 * 5 = 20 folded; predicate: id <= 20
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::LessOrEqual,
        Box::new(Expr::Mul(
            Box::new(Expr::Constant(Constant::Int(4))),
            Box::new(Expr::Constant(Constant::Int(5))),
        )),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(20)).unwrap(), TriValue::True);
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(21)).unwrap(), TriValue::False);
}

#[test]
fn test_constant_fold_div_int_promotes_float() {
    // 10 / 4 = 2.5 (Float) folded; predicate: id > 2.5 using float constant
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Div(
            Box::new(Expr::Constant(Constant::Int(10))),
            Box::new(Expr::Constant(Constant::Int(4))),
        )),
    );
    // executor creation validates typing; result should be ok
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    // id=3 > 2.5 → True; id=2 > 2.5 → False
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(3)).unwrap(), TriValue::True);
    assert_eq!(executor.evaluate_tuple(&make_int_tuple(2)).unwrap(), TriValue::False);
}

#[test]
fn test_null_constant_prevents_folding() {
    // Null in arithmetic: id > (Null + 5) — runtime result must be Unknown
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Add(
            Box::new(Expr::Constant(Constant::Null)),
            Box::new(Expr::Constant(Constant::Int(5))),
        )),
    );
    // Type inference sees Null+Int → Null type; executor accepts it (Null type allowed)
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let result = executor.evaluate_tuple(&make_int_tuple(100)).unwrap();
    // NULL operand → comparison with NULL → Unknown
    assert_eq!(result, TriValue::Unknown);
}

#[test]
fn test_division_by_zero_not_folded_returns_unknown() {
    // Dividing a non-null int column by zero should produce an error/Unknown at runtime,
    // not be folded at planning time.
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Div(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            Box::new(Expr::Constant(Constant::Int(0))),
        )),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(1))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    // evaluate_tuple either returns Unknown or Err — both are acceptable for div-by-zero.
    // The predicate must NOT be True or False (it is indeterminate).
    let result = executor.evaluate_tuple(&make_int_tuple(10));
    match result {
        Ok(TriValue::True) | Ok(TriValue::False) => {
            panic!("Division by zero should not yield a definite boolean result");
        }
        _ => {} // Unknown or Err are both acceptable
    }
}
