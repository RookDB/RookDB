//! Predicate / expression layer used by `show_tuples` to filter rows.
//!
//! Phase 1 supports only `=` and `!=`. The evaluator returns `Option<bool>` so
//! AND/OR/NOT honour SQL three-valued logic (TRUE / FALSE / UNKNOWN). Path
//! extraction on JSON and JSONB columns is delegated to `serde_json_path`.

use serde_json::Value;
use serde_json_path::JsonPath;

use crate::executor::json_utils::parse_to_serde;

/// A unified scalar produced by evaluating an `Expr` against a materialised tuple.
#[derive(Debug, Clone)]
pub enum Datum {
    Null,
    Int(i32),
    Text(String),
    Bool(bool),
    Number(f64),
    Json(Value),
    /// A JSON column kept as raw text until a path predicate forces a parse.
    JsonText(String),
}

impl Datum {
    /// Build a Text datum, trimming TEXT-column space padding so literal
    /// comparisons match the stored representation.
    pub fn text(s: impl Into<String>) -> Self {
        Datum::Text(s.into().trim().to_string())
    }
}

/// Equality / inequality.
#[derive(Debug, Clone, Copy)]
pub enum CmpOp {
    Eq,
    Ne,
}

/// An expression that produces a `Datum` from a tuple.
#[derive(Debug, Clone)]
pub enum Expr {
    Column(usize),
    /// Apply a JSONPath query to the value of a JSON / JSONB column.
    JsonPath(usize, JsonPath),
    Literal(Datum),
}

/// A predicate evaluated per tuple. AND/OR/NOT use SQL 3-valued logic.
#[derive(Debug, Clone)]
pub enum Predicate {
    Cmp(Expr, CmpOp, Expr),
    IsNull(Expr),
}

/// Evaluate an expression against a materialised tuple.
pub fn eval_expr(expr: &Expr, tuple: &[Datum]) -> Datum {
    match expr {
        Expr::Column(idx) => tuple.get(*idx).cloned().unwrap_or(Datum::Null),
        Expr::Literal(d) => d.clone(),
        Expr::JsonPath(idx, path) => {
            let value = match tuple.get(*idx) {
                Some(Datum::Json(v)) => v.clone(),
                Some(Datum::JsonText(t)) => match parse_to_serde(t) {
                    Ok(v) => v,
                    Err(_) => return Datum::Null,
                },
                _ => return Datum::Null,
            };
            match path.query(&value).first() {
                Some(node) => json_to_datum(node),
                None => Datum::Null,
            }
        }
    }
}

/// Convert a `serde_json::Value` extracted by a path into the `Datum` we
/// compare on. Scalars unwrap into typed datums; composites stay as `Json`.
fn json_to_datum(v: &Value) -> Datum {
    match v {
        Value::Null => Datum::Null,
        Value::Bool(b) => Datum::Bool(*b),
        Value::Number(n) => match n.as_f64() {
            Some(f) => Datum::Number(f),
            None => Datum::Null,
        },
        Value::String(s) => Datum::Text(s.clone()),
        other => Datum::Json(other.clone()),
    }
}

/// Evaluate a predicate. Returns `None` for SQL UNKNOWN.
pub fn evaluate(pred: &Predicate, tuple: &[Datum]) -> Option<bool> {
    match pred {
        Predicate::Cmp(lhs, op, rhs) => {
            let l = eval_expr(lhs, tuple);
            let r = eval_expr(rhs, tuple);
            datum_eq(&l, &r).map(|eq| match op {
                CmpOp::Eq => eq,
                CmpOp::Ne => !eq,
            })
        }
        Predicate::IsNull(expr) => Some(matches!(eval_expr(expr, tuple), Datum::Null)),
    }
}

/// Equality between two datums. Returns `None` (UNKNOWN) when either side is
/// NULL, matching SQL semantics. Numeric kinds compare across `Int`/`Number`;
/// `Text` trims trailing padding before compare.
pub fn datum_eq(a: &Datum, b: &Datum) -> Option<bool> {
    match (a, b) {
        (Datum::Null, _) | (_, Datum::Null) => None,
        (Datum::Bool(x), Datum::Bool(y)) => Some(x == y),
        (Datum::Int(x), Datum::Int(y)) => Some(x == y),
        (Datum::Number(x), Datum::Number(y)) => Some(x == y),
        (Datum::Int(x), Datum::Number(y)) | (Datum::Number(y), Datum::Int(x)) => {
            Some((*x as f64) == *y)
        }
        (Datum::Text(x), Datum::Text(y)) => Some(x.trim() == y.trim()),
        (Datum::JsonText(x), Datum::JsonText(y)) => Some(x == y),
        (Datum::Json(x), Datum::Json(y)) => Some(x == y),
        // Mixed Text vs Number: stringy JSON values like "2024" should still
        // match a numeric literal 2024.
        (Datum::Text(s), Datum::Number(n)) | (Datum::Number(n), Datum::Text(s)) => {
            Some(s.trim().parse::<f64>().ok() == Some(*n))
        }
        (Datum::Text(s), Datum::Int(i)) | (Datum::Int(i), Datum::Text(s)) => {
            Some(s.trim().parse::<i32>().ok() == Some(*i))
        }
        // Mixed Text vs JsonText: parse the text side once and compare structurally.
        (Datum::Text(s), Datum::Json(v)) | (Datum::Json(v), Datum::Text(s)) => {
            Some(matches!(v, Value::String(js) if js == s.trim()))
        }
        _ => Some(false),
    }
}
