//! Tests for expression evaluation covering all cases in the spec.

use storage_manager::executor::expr::{eval_expr, like_match, Expr};
use storage_manager::executor::value::Value;
use storage_manager::catalog::types::DataType;

fn row(vals: Vec<Value>) -> Vec<Value> { vals }

// ─── Basic leaf nodes ───────────────────────────────────────────────────────

#[test]
fn test_const() {
    let r = row(vec![]);
    assert_eq!(eval_expr(&Expr::int(42), &r).unwrap(), Value::Int(42));
    assert_eq!(eval_expr(&Expr::text("hi"), &r).unwrap(), Value::Text("hi".to_string()));
    assert_eq!(eval_expr(&Expr::null(), &r).unwrap(), Value::Null);
}

#[test]
fn test_column_ref() {
    let r = row(vec![Value::Int(10), Value::Text("alice".to_string())]);
    assert_eq!(eval_expr(&Expr::col(0), &r).unwrap(), Value::Int(10));
    assert_eq!(eval_expr(&Expr::col(1), &r).unwrap(), Value::Text("alice".to_string()));
}

#[test]
fn test_out_of_bounds_column_returns_null() {
    let r = row(vec![Value::Int(1)]);
    assert_eq!(eval_expr(&Expr::col(99), &r).unwrap(), Value::Null);
}

// ─── Column-to-column comparison ────────────────────────────────────────────
// e.g. salary > bonus

#[test]
fn test_column_to_column_gt() {
    // salary=100, bonus=50 → salary > bonus is true
    let r = row(vec![Value::Int(100), Value::Int(50)]);
    let expr = Expr::gt(Expr::col(0), Expr::col(1));
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Bool(true));
}

#[test]
fn test_column_to_column_eq() {
    let r = row(vec![Value::Int(42), Value::Int(42)]);
    let expr = Expr::eq(Expr::col(0), Expr::col(1));
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Bool(true));
}

#[test]
fn test_column_to_column_text() {
    let r = row(vec![Value::Text("z".to_string()), Value::Text("a".to_string())]);
    let expr = Expr::gt(Expr::col(0), Expr::col(1));
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Bool(true));
}

// ─── Expression-to-constant ─────────────────────────────────────────────────
// e.g. age + 1 > 18

#[test]
fn test_expr_to_const_age_check() {
    // age=18 → age + 1 > 18 → 19 > 18 → true
    let r = row(vec![Value::Int(18)]);
    let expr = Expr::gt(
        Expr::add(Expr::col(0), Expr::int(1)),
        Expr::int(18),
    );
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Bool(true));
}

#[test]
fn test_expr_to_const_false() {
    // age=17 → age + 1 > 18 → 18 > 18 → false
    let r = row(vec![Value::Int(17)]);
    let expr = Expr::gt(
        Expr::add(Expr::col(0), Expr::int(1)),
        Expr::int(18),
    );
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Bool(false));
}

// ─── Arithmetic expressions ─────────────────────────────────────────────────

#[test]
fn test_nested_arithmetic() {
    // (a + b) * c where a=2, b=3, c=4 → 20
    let r = row(vec![Value::Int(2), Value::Int(3), Value::Int(4)]);
    let expr = Expr::mul(
        Expr::add(Expr::col(0), Expr::col(1)),
        Expr::col(2),
    );
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Int(20));
}

#[test]
fn test_subtraction_and_division() {
    let r = row(vec![Value::Int(20), Value::Int(4)]);
    let expr = Expr::div(Expr::col(0), Expr::col(1));
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Int(5));
}

#[test]
fn test_negation() {
    let r = row(vec![Value::Int(7)]);
    let expr = Expr::Neg(Box::new(Expr::col(0)));
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Int(-7));
}

// ─── Boolean combinators ────────────────────────────────────────────────────

#[test]
fn test_and_short_circuit() {
    // false AND (would panic) → false without evaluating right side
    let r = row(vec![Value::Int(0)]);
    let expr = Expr::and(
        Expr::eq(Expr::col(0), Expr::int(1)),  // false
        Expr::eq(Expr::int(1), Expr::int(1)),
    );
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Bool(false));
}

#[test]
fn test_or_short_circuit() {
    let r = row(vec![Value::Int(1)]);
    let expr = Expr::or(
        Expr::eq(Expr::col(0), Expr::int(1)),  // true
        Expr::eq(Expr::col(0), Expr::int(99)), // not evaluated
    );
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Bool(true));
}

#[test]
fn test_not() {
    let r = row(vec![]);
    let expr = Expr::not(Expr::bool_val(false));
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Bool(true));
}

// ─── NULL checks ────────────────────────────────────────────────────────────

#[test]
fn test_is_null() {
    let r = row(vec![Value::Null, Value::Int(1)]);
    assert_eq!(eval_expr(&Expr::IsNull(Box::new(Expr::col(0))), &r).unwrap(), Value::Bool(true));
    assert_eq!(eval_expr(&Expr::IsNull(Box::new(Expr::col(1))), &r).unwrap(), Value::Bool(false));
}

#[test]
fn test_is_not_null() {
    let r = row(vec![Value::Null, Value::Int(1)]);
    assert_eq!(eval_expr(&Expr::IsNotNull(Box::new(Expr::col(0))), &r).unwrap(), Value::Bool(false));
    assert_eq!(eval_expr(&Expr::IsNotNull(Box::new(Expr::col(1))), &r).unwrap(), Value::Bool(true));
}

// ─── String operations ──────────────────────────────────────────────────────

#[test]
fn test_like_match() {
    assert!(like_match("hello world", "%world"));
    assert!(like_match("hello world", "hello%"));
    assert!(like_match("hello world", "%lo wo%"));
    assert!(like_match("hello", "h_llo"));
    assert!(!like_match("hello", "world%"));
    assert!(like_match("abc", "%"));
    assert!(like_match("", "%"));
}

#[test]
fn test_like_expr() {
    let r = row(vec![Value::Text("hello world".to_string())]);
    let expr = Expr::Like(
        Box::new(Expr::col(0)),
        Box::new(Expr::text("hello%")),
    );
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Bool(true));
}

#[test]
fn test_upper_lower() {
    let r = row(vec![Value::Text("Hello World".to_string())]);
    assert_eq!(
        eval_expr(&Expr::Upper(Box::new(Expr::col(0))), &r).unwrap(),
        Value::Text("HELLO WORLD".to_string())
    );
    assert_eq!(
        eval_expr(&Expr::Lower(Box::new(Expr::col(0))), &r).unwrap(),
        Value::Text("hello world".to_string())
    );
}

#[test]
fn test_length() {
    let r = row(vec![Value::Text("hello".to_string())]);
    assert_eq!(
        eval_expr(&Expr::Length(Box::new(Expr::col(0))), &r).unwrap(),
        Value::Int(5)
    );
}

#[test]
fn test_concat() {
    let r = row(vec![Value::Text("foo".to_string()), Value::Text("bar".to_string())]);
    let expr = Expr::Concat(Box::new(Expr::col(0)), Box::new(Expr::col(1)));
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Text("foobar".to_string()));
}

#[test]
fn test_trim() {
    let r = row(vec![Value::Text("  spaces  ".to_string())]);
    assert_eq!(
        eval_expr(&Expr::Trim(Box::new(Expr::col(0))), &r).unwrap(),
        Value::Text("spaces".to_string())
    );
}

#[test]
fn test_substring() {
    // SUBSTRING("hello world", 7, 5) → "world"
    let r = row(vec![Value::Text("hello world".to_string())]);
    let expr = Expr::Substring(
        Box::new(Expr::col(0)),
        Box::new(Expr::int(7)),
        Box::new(Expr::int(5)),
    );
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Text("world".to_string()));
}

// ─── Date arithmetic ─────────────────────────────────────────────────────────

#[test]
fn test_date_add_expr() {
    let r = row(vec![Value::Date(100)]);
    let expr = Expr::DateAdd(Box::new(Expr::col(0)), Box::new(Expr::int(30)));
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Date(130));
}

#[test]
fn test_date_diff_expr() {
    let r = row(vec![Value::Date(200), Value::Date(150)]);
    let expr = Expr::DateDiff(Box::new(Expr::col(0)), Box::new(Expr::col(1)));
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Int(50));
}

// ─── Cast ───────────────────────────────────────────────────────────────────

#[test]
fn test_cast_expr() {
    let r = row(vec![Value::Text("42".to_string())]);
    let expr = Expr::Cast(Box::new(Expr::col(0)), DataType::Int);
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Int(42));
}

// ─── Between ────────────────────────────────────────────────────────────────

#[test]
fn test_between() {
    let r = row(vec![Value::Int(5)]);
    let expr = Expr::Between(
        Box::new(Expr::col(0)),
        Box::new(Expr::int(1)),
        Box::new(Expr::int(10)),
    );
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Bool(true));

    let r2 = row(vec![Value::Int(11)]);
    assert_eq!(eval_expr(&expr, &r2).unwrap(), Value::Bool(false));
}

// ─── In / Not In ────────────────────────────────────────────────────────────

#[test]
fn test_in_expr() {
    let r = row(vec![Value::Int(3)]);
    let expr = Expr::In(
        Box::new(Expr::col(0)),
        vec![Expr::int(1), Expr::int(2), Expr::int(3)],
    );
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Bool(true));

    let r2 = row(vec![Value::Int(9)]);
    assert_eq!(eval_expr(&expr, &r2).unwrap(), Value::Bool(false));
}

#[test]
fn test_not_in_expr() {
    let r = row(vec![Value::Int(5)]);
    let expr = Expr::NotIn(
        Box::new(Expr::col(0)),
        vec![Expr::int(1), Expr::int(2), Expr::int(3)],
    );
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Bool(true));
}

#[test]
fn test_null_in_list_returns_false() {
    let r = row(vec![Value::Null]);
    let expr = Expr::In(
        Box::new(Expr::col(0)),
        vec![Expr::null(), Expr::int(1)],
    );
    // SQL: NULL IN (...) → false
    assert_eq!(eval_expr(&expr, &r).unwrap(), Value::Bool(false));
}
