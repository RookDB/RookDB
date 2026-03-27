// Type inference tests
// infer_expr_type() is exercised indirectly via SelectionExecutor::new.
// Tests validate planning-time type checking behaviour.

use storage_manager::catalog::{Column, Table};
use storage_manager::executor::selection::*;

fn int_schema() -> Table {
    Table { columns: vec![Column { name: "id".to_string(), data_type: "INT".to_string() }] }
}

fn float_schema() -> Table {
    Table { columns: vec![Column { name: "price".to_string(), data_type: "FLOAT".to_string() }] }
}

fn text_schema() -> Table {
    Table { columns: vec![Column { name: "name".to_string(), data_type: "TEXT".to_string() }] }
}

fn mixed_schema() -> Table {
    Table {
        columns: vec![
            Column { name: "id".to_string(),    data_type: "INT".to_string()   },
            Column { name: "price".to_string(),  data_type: "FLOAT".to_string() },
        ],
    }
}

// ── tests ────────────────────────────────────────────────────────────────────

#[test]
fn test_int_div_int_infers_float_accepted() {
    // Div(INT, INT) infers Float — should be accepted when compared with Float constant.
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Div(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            Box::new(Expr::Constant(Constant::Int(2))),
        )),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Float(1.5))),
    );
    assert!(SelectionExecutor::new(pred, schema).is_ok(),
        "INT / INT infers Float; comparing with Float constant must succeed");
}

#[test]
fn test_int_mul_int_infers_int() {
    // Mul(INT, INT) infers Int; comparing with Int constant is valid.
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Mul(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            Box::new(Expr::Constant(Constant::Int(3))),
        )),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(100))),
    );
    assert!(SelectionExecutor::new(pred, schema).is_ok());
}

#[test]
fn test_int_add_float_infers_float() {
    // Add(INT col, FLOAT col) infers Float; compare with Float constant is valid.
    let schema = mixed_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Add(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            Box::new(Expr::Column(ColumnReference::new("price".to_string()))),
        )),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Float(100.0))),
    );
    assert!(SelectionExecutor::new(pred, schema).is_ok());
}

#[test]
fn test_float_div_float_infers_float() {
    // Div(FLOAT, FLOAT) infers Float; compare with Float constant is valid.
    let schema = float_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Div(
            Box::new(Expr::Column(ColumnReference::new("price".to_string()))),
            Box::new(Expr::Constant(Constant::Float(2.0))),
        )),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Float(10.0))),
    );
    assert!(SelectionExecutor::new(pred, schema).is_ok());
}

#[test]
fn test_text_in_arithmetic_rejected() {
    // TEXT + INT is invalid — must fail at planning.
    let schema = text_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Add(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
            Box::new(Expr::Constant(Constant::Int(1))),
        )),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    assert!(SelectionExecutor::new(pred, schema).is_err(),
        "TEXT in arithmetic must be rejected");
}

#[test]
fn test_text_vs_int_comparison_rejected() {
    // Comparing TEXT column directly with INT constant violates strict type check.
    let schema = text_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    assert!(SelectionExecutor::new(pred, schema).is_err(),
        "TEXT vs INT direct comparison must be rejected");
}

#[test]
fn test_int_vs_float_constant_accepted() {
    // INT column vs Float constant is promoted and accepted.
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Float(3.14))),
    );
    assert!(SelectionExecutor::new(pred, schema).is_ok(),
        "INT column vs FLOAT constant should be accepted");
}

#[test]
fn test_null_constant_type_is_valid() {
    // Comparing with Null constant is always valid (NULL semantics).
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Null)),
    );
    assert!(SelectionExecutor::new(pred, schema).is_ok());
}
