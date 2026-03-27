// Expression evaluation tests
// Tests arithmetic expressions (Add, Sub, Mul, Div) in predicate operands.

use storage_manager::catalog::{Column, Table};
use storage_manager::executor::selection::*;

// ── helpers ─────────────────────────────────────────────────────────────────

fn int_schema() -> Table {
    Table {
        columns: vec![
            Column { name: "a".to_string(), data_type: "INT".to_string() },
            Column { name: "b".to_string(), data_type: "INT".to_string() },
        ],
    }
}

fn float_schema() -> Table {
    Table {
        columns: vec![
            Column { name: "x".to_string(), data_type: "FLOAT".to_string() },
            Column { name: "y".to_string(), data_type: "FLOAT".to_string() },
        ],
    }
}

fn mixed_schema() -> Table {
    Table {
        columns: vec![
            Column { name: "id".to_string(),     data_type: "INT".to_string()   },
            Column { name: "price".to_string(),   data_type: "FLOAT".to_string() },
        ],
    }
}

/// Build a 2-column INT tuple.
fn make_int_tuple(a: i32, b: i32) -> Vec<u8> {
    let num_cols = 2usize;
    let null_bitmap_len = 1usize;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8usize;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let total_length = data_start + 8;
    let mut t = vec![0u8; total_length];
    t[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0; // null bitmap
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&4u32.to_le_bytes());
    t[os+8..os+12].copy_from_slice(&8u32.to_le_bytes());
    t[data_start..data_start+4].copy_from_slice(&a.to_le_bytes());
    t[data_start+4..data_start+8].copy_from_slice(&b.to_le_bytes());
    t
}

/// Build a 2-column FLOAT tuple.
fn make_float_tuple(x: f64, y: f64) -> Vec<u8> {
    let num_cols = 2usize;
    let null_bitmap_len = 1usize;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8usize;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let total_length = data_start + 16;
    let mut t = vec![0u8; total_length];
    t[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0;
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&8u32.to_le_bytes());
    t[os+8..os+12].copy_from_slice(&16u32.to_le_bytes());
    t[data_start..data_start+8].copy_from_slice(&x.to_le_bytes());
    t[data_start+8..data_start+16].copy_from_slice(&y.to_le_bytes());
    t
}

/// Build an INT + FLOAT mixed tuple.
fn make_mixed_tuple(id: i32, price: f64) -> Vec<u8> {
    let num_cols = 2usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8usize;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let total_length = data_start + 12; // 4 (int) + 8 (float)
    let mut t = vec![0u8; total_length];
    t[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0;
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&4u32.to_le_bytes());
    t[os+8..os+12].copy_from_slice(&12u32.to_le_bytes());
    t[data_start..data_start+4].copy_from_slice(&id.to_le_bytes());
    t[data_start+4..data_start+12].copy_from_slice(&price.to_le_bytes());
    t
}

// ── tests ────────────────────────────────────────────────────────────────────

#[test]
fn test_expr_add_int_constant() {
    // a + 10 > 50  →  tuple with a=45 should pass (45+10=55 > 50)
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Add(
            Box::new(Expr::Column(ColumnReference::new("a".to_string()))),
            Box::new(Expr::Constant(Constant::Int(10))),
        )),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let t = make_int_tuple(45, 0);
    assert_eq!(executor.evaluate_tuple(&t).unwrap(), TriValue::True);
}

#[test]
fn test_expr_add_int_constant_fail() {
    // a + 10 > 50  →  tuple with a=30 should fail (30+10=40 ≤ 50)
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Add(
            Box::new(Expr::Column(ColumnReference::new("a".to_string()))),
            Box::new(Expr::Constant(Constant::Int(10))),
        )),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let t = make_int_tuple(30, 0);
    assert_eq!(executor.evaluate_tuple(&t).unwrap(), TriValue::False);
}

#[test]
fn test_expr_sub_int() {
    // a - 5 < 20  →  a=24 → 24-5=19 < 20, True
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Sub(
            Box::new(Expr::Column(ColumnReference::new("a".to_string()))),
            Box::new(Expr::Constant(Constant::Int(5))),
        )),
        ComparisonOp::LessThan,
        Box::new(Expr::Constant(Constant::Int(20))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let t = make_int_tuple(24, 0);
    assert_eq!(executor.evaluate_tuple(&t).unwrap(), TriValue::True);
}

#[test]
fn test_expr_mul_float() {
    // x * 2.0 > 100.0  →  x=60.0 → 120.0 > 100.0, True
    let schema = float_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Mul(
            Box::new(Expr::Column(ColumnReference::new("x".to_string()))),
            Box::new(Expr::Constant(Constant::Float(2.0))),
        )),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Float(100.0))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let t = make_float_tuple(60.0, 0.0);
    assert_eq!(executor.evaluate_tuple(&t).unwrap(), TriValue::True);
}

#[test]
fn test_expr_div_int_promotes_float() {
    // id / 2 > 3.0 — Int/Int div produces Float at runtime
    // id=8 → 8/2=4.0 > 3.0, True
    let schema = mixed_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Div(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            Box::new(Expr::Constant(Constant::Int(2))),
        )),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Float(3.0))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let t = make_mixed_tuple(8, 0.0);
    assert_eq!(executor.evaluate_tuple(&t).unwrap(), TriValue::True);
}

#[test]
fn test_expr_chained_add_mul() {
    // (a + 5) * 2 > 100  →  a=50 → (50+5)*2=110 > 100, True
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Mul(
            Box::new(Expr::Add(
                Box::new(Expr::Column(ColumnReference::new("a".to_string()))),
                Box::new(Expr::Constant(Constant::Int(5))),
            )),
            Box::new(Expr::Constant(Constant::Int(2))),
        )),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(100))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let t = make_int_tuple(50, 0);
    assert_eq!(executor.evaluate_tuple(&t).unwrap(), TriValue::True);
}

#[test]
fn test_expr_int_plus_float_coercion() {
    // id + price > 100.0 (INT col + FLOAT col)
    // id=40, price=65.0 → 105.0 > 100.0, True
    let schema = mixed_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Add(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            Box::new(Expr::Column(ColumnReference::new("price".to_string()))),
        )),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Float(100.0))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let t = make_mixed_tuple(40, 65.0);
    assert_eq!(executor.evaluate_tuple(&t).unwrap(), TriValue::True);
}

#[test]
fn test_expr_null_operand_returns_unknown() {
    // NULL operand in arithmetic → Unknown at runtime
    // Build tuple with id=NULL, price=50.0; predicate: id + 10 > 5
    let schema = mixed_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Add(
            Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
            Box::new(Expr::Constant(Constant::Int(10))),
        )),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(5))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();

    // Build null-id tuple: null bitmap bit 0 set
    let num_cols = 2usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8usize;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let total_length = data_start + 12;
    let mut t = vec![0u8; total_length];
    t[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0b00000001; // id is NULL
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&4u32.to_le_bytes());
    t[os+8..os+12].copy_from_slice(&12u32.to_le_bytes());
    t[data_start+4..data_start+12].copy_from_slice(&50.0f64.to_le_bytes());

    assert_eq!(executor.evaluate_tuple(&t).unwrap(), TriValue::Unknown);
}

#[test]
fn test_expr_text_column_in_arithmetic_rejected() {
    // TEXT column used in arithmetic → type mismatch at planning time
    let schema = Table {
        columns: vec![
            Column { name: "name".to_string(), data_type: "TEXT".to_string() },
        ],
    };
    let pred = Predicate::Compare(
        Box::new(Expr::Add(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
            Box::new(Expr::Constant(Constant::Int(1))),
        )),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    // Should fail during SelectionExecutor::new type checking
    let result = SelectionExecutor::new(pred, schema);
    assert!(result.is_err(), "TEXT in arithmetic must be rejected at planning time");
}
