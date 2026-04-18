//! Tests for Value compare, arith, and cast.

use storage_manager::executor::value::{ArithOp, CmpOp, Value};

// ─── compare ────────────────────────────────────────────────────────────────

#[test]
fn test_int_comparisons() {
    let a = Value::Int(10);
    let b = Value::Int(20);
    assert!(a.compare(CmpOp::Lt, &b).unwrap());
    assert!(b.compare(CmpOp::Gt, &a).unwrap());
    assert!(a.compare(CmpOp::Ne, &b).unwrap());
    assert!(a.compare(CmpOp::Eq, &Value::Int(10)).unwrap());
    assert!(a.compare(CmpOp::Le, &Value::Int(10)).unwrap());
}

#[test]
fn test_float_comparisons() {
    let a = Value::Float(1.5);
    let b = Value::Float(2.5);
    assert!(a.compare(CmpOp::Lt, &b).unwrap());
    assert!(!a.compare(CmpOp::Gt, &b).unwrap());
}

#[test]
fn test_text_comparisons() {
    let a = Value::Text("apple".to_string());
    let b = Value::Text("banana".to_string());
    assert!(a.compare(CmpOp::Lt, &b).unwrap());
    assert!(a.compare(CmpOp::Ne, &b).unwrap());
    assert!(a.compare(CmpOp::Eq, &Value::Text("apple".to_string())).unwrap());
}

#[test]
fn test_mixed_int_float_comparison() {
    // Int and Float should coerce for comparison
    let a = Value::Int(5);
    let b = Value::Float(5.0);
    assert!(a.compare(CmpOp::Eq, &b).unwrap());
    assert!(Value::Int(3).compare(CmpOp::Lt, &Value::Float(3.5)).unwrap());
}

#[test]
fn test_null_propagation_in_compare() {
    // Any comparison with NULL returns false (SQL semantics)
    assert!(!Value::Null.compare(CmpOp::Eq, &Value::Int(1)).unwrap());
    assert!(!Value::Int(1).compare(CmpOp::Eq, &Value::Null).unwrap());
    assert!(!Value::Null.compare(CmpOp::Lt, &Value::Null).unwrap());
}

// ─── arith ──────────────────────────────────────────────────────────────────

#[test]
fn test_int_arithmetic() {
    assert_eq!(Value::Int(3).arith(ArithOp::Add, &Value::Int(4)).unwrap(), Value::Int(7));
    assert_eq!(Value::Int(10).arith(ArithOp::Sub, &Value::Int(3)).unwrap(), Value::Int(7));
    assert_eq!(Value::Int(3).arith(ArithOp::Mul, &Value::Int(4)).unwrap(), Value::Int(12));
    assert_eq!(Value::Int(10).arith(ArithOp::Div, &Value::Int(2)).unwrap(), Value::Int(5));
}

#[test]
fn test_float_arithmetic() {
    let r = Value::Float(1.5).arith(ArithOp::Add, &Value::Float(2.5)).unwrap();
    if let Value::Float(f) = r { assert!((f - 4.0).abs() < 1e-10); } else { panic!("Expected Float"); }
}

#[test]
fn test_division_by_zero() {
    assert!(Value::Int(5).arith(ArithOp::Div, &Value::Int(0)).is_err());
    assert!(Value::Float(5.0).arith(ArithOp::Div, &Value::Float(0.0)).is_err());
}

#[test]
fn test_string_concat_via_arith() {
    let result = Value::Text("hello".to_string())
        .arith(ArithOp::Add, &Value::Text(" world".to_string()))
        .unwrap();
    assert_eq!(result, Value::Text("hello world".to_string()));
}

#[test]
fn test_date_add_days() {
    // Date(0) + 10 days = Date(10)
    let result = Value::Date(0).arith(ArithOp::Add, &Value::Int(10)).unwrap();
    assert_eq!(result, Value::Date(10));
}

#[test]
fn test_date_diff() {
    let result = Value::Date(30).arith(ArithOp::Sub, &Value::Date(10)).unwrap();
    assert_eq!(result, Value::Int(20));
}

#[test]
fn test_null_arith_propagates() {
    assert_eq!(
        Value::Null.arith(ArithOp::Add, &Value::Int(5)).unwrap(),
        Value::Null
    );
    assert_eq!(
        Value::Int(5).arith(ArithOp::Add, &Value::Null).unwrap(),
        Value::Null
    );
}

// ─── cast ───────────────────────────────────────────────────────────────────

use storage_manager::catalog::types::DataType;

#[test]
fn test_cast_int_to_float() {
    let r = Value::Int(42).cast(&DataType::Float).unwrap();
    if let Value::Float(f) = r { assert!((f - 42.0).abs() < 1e-10); } else { panic!(); }
}

#[test]
fn test_cast_float_to_int() {
    let r = Value::Float(3.9).cast(&DataType::Int).unwrap();
    assert_eq!(r, Value::Int(3));
}

#[test]
fn test_cast_text_to_int() {
    assert_eq!(Value::Text("123".to_string()).cast(&DataType::Int).unwrap(), Value::Int(123));
}

#[test]
fn test_cast_int_to_text() {
    assert_eq!(
        Value::Int(99).cast(&DataType::Text).unwrap(),
        Value::Text("99".to_string())
    );
}

#[test]
fn test_cast_null_stays_null() {
    assert_eq!(Value::Null.cast(&DataType::Int).unwrap(), Value::Null);
    assert_eq!(Value::Null.cast(&DataType::Text).unwrap(), Value::Null);
}

#[test]
fn test_cast_text_to_bool() {
    assert_eq!(Value::Text("true".to_string()).cast(&DataType::Bool).unwrap(), Value::Bool(true));
    assert_eq!(Value::Text("false".to_string()).cast(&DataType::Bool).unwrap(), Value::Bool(false));
}
