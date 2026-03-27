//! Expression AST and evaluator.
//!
//! Supports all comparison forms required by the spec:
//!   - ColumnRef op Constant    (legacy form)
//!   - Column op Column         (column-to-column)
//!   - Arithmetic op Constant   (expression-to-constant)
//!   - String ops, date arith, casts, NULL checks, WITH/CTE.

use crate::catalog::types::DataType;
use crate::executor::value::{ArithOp, CmpOp, EvalError, Value};

/// A row is a slice of Values, one per column in schema order.
pub type Row = Vec<Value>;

/// One node in the expression tree.
#[derive(Debug, Clone)]
pub enum Expr {
    // ── Leaf nodes ──────────────────────────────────────────────────────────
    /// Literal constant value.
    Const(Value),
    /// Reference to column at `index` in the current row.
    Column(usize),

    // ── Arithmetic ──────────────────────────────────────────────────────────
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
    Neg(Box<Expr>),

    // ── Comparisons (work for ANY pair of sub-expressions) ──────────────────
    Eq(Box<Expr>, Box<Expr>),
    Ne(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    Le(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    Ge(Box<Expr>, Box<Expr>),

    // ── Boolean combinators ─────────────────────────────────────────────────
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),

    // ── NULL checks ─────────────────────────────────────────────────────────
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),

    // ── String operations ────────────────────────────────────────────────────
    /// SQL LIKE pattern (% and _ wildcards).
    Like(Box<Expr>, Box<Expr>),
    NotLike(Box<Expr>, Box<Expr>),
    Concat(Box<Expr>, Box<Expr>),
    Upper(Box<Expr>),
    Lower(Box<Expr>),
    /// Character length.
    Length(Box<Expr>),
    /// TRIM(expr).
    Trim(Box<Expr>),
    /// SUBSTRING(expr, start, length) – 1-indexed, SQL style.
    Substring(Box<Expr>, Box<Expr>, Box<Expr>),

    // ── Date arithmetic ──────────────────────────────────────────────────────
    /// date + int_days  →  Date
    DateAdd(Box<Expr>, Box<Expr>),
    /// date - date  →  Int (days)
    DateDiff(Box<Expr>, Box<Expr>),

    // ── Type cast ───────────────────────────────────────────────────────────
    Cast(Box<Expr>, DataType),

    // ── Between / In ────────────────────────────────────────────────────────
    Between(Box<Expr>, Box<Expr>, Box<Expr>),
    In(Box<Expr>, Vec<Expr>),
    NotIn(Box<Expr>, Vec<Expr>),
}

// ─── helper constructors (makes building trees less verbose) ────────────────

impl Expr {
    pub fn col(idx: usize) -> Self { Expr::Column(idx) }
    pub fn int(v: i64) -> Self { Expr::Const(Value::Int(v)) }
    pub fn float(v: f64) -> Self { Expr::Const(Value::Float(v)) }
    pub fn text(s: &str) -> Self { Expr::Const(Value::Text(s.to_string())) }
    pub fn bool_val(b: bool) -> Self { Expr::Const(Value::Bool(b)) }
    pub fn null() -> Self { Expr::Const(Value::Null) }

    pub fn eq(l: Expr, r: Expr) -> Self  { Expr::Eq(Box::new(l), Box::new(r)) }
    pub fn ne(l: Expr, r: Expr) -> Self  { Expr::Ne(Box::new(l), Box::new(r)) }
    pub fn lt(l: Expr, r: Expr) -> Self  { Expr::Lt(Box::new(l), Box::new(r)) }
    pub fn le(l: Expr, r: Expr) -> Self  { Expr::Le(Box::new(l), Box::new(r)) }
    pub fn gt(l: Expr, r: Expr) -> Self  { Expr::Gt(Box::new(l), Box::new(r)) }
    pub fn ge(l: Expr, r: Expr) -> Self  { Expr::Ge(Box::new(l), Box::new(r)) }
    pub fn and(l: Expr, r: Expr) -> Self { Expr::And(Box::new(l), Box::new(r)) }
    pub fn or(l: Expr, r: Expr) -> Self  { Expr::Or(Box::new(l), Box::new(r)) }
    pub fn not(e: Expr) -> Self          { Expr::Not(Box::new(e)) }
    pub fn add(l: Expr, r: Expr) -> Self { Expr::Add(Box::new(l), Box::new(r)) }
    pub fn sub(l: Expr, r: Expr) -> Self { Expr::Sub(Box::new(l), Box::new(r)) }
    pub fn mul(l: Expr, r: Expr) -> Self { Expr::Mul(Box::new(l), Box::new(r)) }
    pub fn div(l: Expr, r: Expr) -> Self { Expr::Div(Box::new(l), Box::new(r)) }
}

// ─── Evaluator ──────────────────────────────────────────────────────────────

/// Evaluate `expr` against a single `row`.
/// Returns a `Value`; never panics on out-of-bounds – returns Null instead.
pub fn eval_expr(expr: &Expr, row: &[Value]) -> Result<Value, EvalError> {
    match expr {
        // ── Leaf ────────────────────────────────────────────────────────────
        Expr::Const(v) => Ok(v.clone()),
        Expr::Column(idx) => Ok(row.get(*idx).cloned().unwrap_or(Value::Null)),

        // ── Arithmetic ──────────────────────────────────────────────────────
        Expr::Add(l, r) => eval_expr(l, row)?.arith(ArithOp::Add, &eval_expr(r, row)?),
        Expr::Sub(l, r) => eval_expr(l, row)?.arith(ArithOp::Sub, &eval_expr(r, row)?),
        Expr::Mul(l, r) => eval_expr(l, row)?.arith(ArithOp::Mul, &eval_expr(r, row)?),
        Expr::Div(l, r) => eval_expr(l, row)?.arith(ArithOp::Div, &eval_expr(r, row)?),
        Expr::Neg(e) => {
            match eval_expr(e, row)? {
                Value::Int(v) => Ok(Value::Int(-v)),
                Value::Float(v) => Ok(Value::Float(-v)),
                Value::Null => Ok(Value::Null),
                other => Err(EvalError(format!("Cannot negate {:?}", other))),
            }
        }

        // ── Comparisons ─────────────────────────────────────────────────────
        Expr::Eq(l, r) => compare_expr(l, r, CmpOp::Eq, row),
        Expr::Ne(l, r) => compare_expr(l, r, CmpOp::Ne, row),
        Expr::Lt(l, r) => compare_expr(l, r, CmpOp::Lt, row),
        Expr::Le(l, r) => compare_expr(l, r, CmpOp::Le, row),
        Expr::Gt(l, r) => compare_expr(l, r, CmpOp::Gt, row),
        Expr::Ge(l, r) => compare_expr(l, r, CmpOp::Ge, row),

        // ── Boolean ─────────────────────────────────────────────────────────
        Expr::And(l, r) => {
            let lv = eval_as_bool(l, row)?;
            if !lv { return Ok(Value::Bool(false)); } // short-circuit
            Ok(Value::Bool(eval_as_bool(r, row)?))
        }
        Expr::Or(l, r) => {
            let lv = eval_as_bool(l, row)?;
            if lv { return Ok(Value::Bool(true)); } // short-circuit
            Ok(Value::Bool(eval_as_bool(r, row)?))
        }
        Expr::Not(e) => Ok(Value::Bool(!eval_as_bool(e, row)?)),

        // ── NULL checks ─────────────────────────────────────────────────────
        Expr::IsNull(e) => Ok(Value::Bool(matches!(eval_expr(e, row)?, Value::Null))),
        Expr::IsNotNull(e) => Ok(Value::Bool(!matches!(eval_expr(e, row)?, Value::Null))),

        // ── String ops ──────────────────────────────────────────────────────
        Expr::Like(e, pat) => {
            let v = eval_expr(e, row)?;
            let p = eval_expr(pat, row)?;
            if matches!(v, Value::Null) || matches!(p, Value::Null) {
                return Ok(Value::Bool(false));
            }
            let s = to_text(&v)?;
            let pattern = to_text(&p)?;
            Ok(Value::Bool(like_match(&s, &pattern)))
        }
        Expr::NotLike(e, pat) => {
            let v = eval_expr(e, row)?;
            let p = eval_expr(pat, row)?;
            if matches!(v, Value::Null) || matches!(p, Value::Null) {
                return Ok(Value::Bool(false));
            }
            let s = to_text(&v)?;
            let pattern = to_text(&p)?;
            Ok(Value::Bool(!like_match(&s, &pattern)))
        }
        Expr::Concat(l, r) => {
            let lv = eval_expr(l, row)?;
            let rv = eval_expr(r, row)?;
            if matches!(lv, Value::Null) || matches!(rv, Value::Null) {
                return Ok(Value::Null);
            }
            Ok(Value::Text(format!("{}{}", to_text(&lv)?, to_text(&rv)?)))
        }
        Expr::Upper(e) => {
            let v = eval_expr(e, row)?;
            if matches!(v, Value::Null) { return Ok(Value::Null); }
            Ok(Value::Text(to_text(&v)?.to_uppercase()))
        }
        Expr::Lower(e) => {
            let v = eval_expr(e, row)?;
            if matches!(v, Value::Null) { return Ok(Value::Null); }
            Ok(Value::Text(to_text(&v)?.to_lowercase()))
        }
        Expr::Length(e) => {
            let v = eval_expr(e, row)?;
            if matches!(v, Value::Null) { return Ok(Value::Null); }
            Ok(Value::Int(to_text(&v)?.chars().count() as i64))
        }
        Expr::Trim(e) => {
            let v = eval_expr(e, row)?;
            if matches!(v, Value::Null) { return Ok(Value::Null); }
            Ok(Value::Text(to_text(&v)?.trim().to_string()))
        }
        Expr::Substring(e, start_expr, len_expr) => {
            let v = eval_expr(e, row)?;
            if matches!(v, Value::Null) { return Ok(Value::Null); }
            let s = to_text(&v)?;
            let start = match eval_expr(start_expr, row)? {
                Value::Int(n) => (n - 1).max(0) as usize, // SQL is 1-indexed
                _ => return Err(EvalError("SUBSTRING start must be INT".to_string())),
            };
            let len = match eval_expr(len_expr, row)? {
                Value::Int(n) => n.max(0) as usize,
                _ => return Err(EvalError("SUBSTRING length must be INT".to_string())),
            };
            let result: String = s.chars().skip(start).take(len).collect();
            Ok(Value::Text(result))
        }

        // ── Date arithmetic ─────────────────────────────────────────────────
        Expr::DateAdd(d, n) => {
            let date_val = eval_expr(d, row)?;
            let n_val = eval_expr(n, row)?;
            date_val.arith(ArithOp::Add, &n_val)
        }
        Expr::DateDiff(a, b) => {
            let av = eval_expr(a, row)?;
            let bv = eval_expr(b, row)?;
            av.arith(ArithOp::Sub, &bv)
        }

        // ── Cast ─────────────────────────────────────────────────────────────
        Expr::Cast(e, dt) => {
            let v = eval_expr(e, row)?;
            v.cast(dt)
        }

        // ── Between ──────────────────────────────────────────────────────────
        Expr::Between(e, lo, hi) => {
            let v = eval_expr(e, row)?;
            let l = eval_expr(lo, row)?;
            let h = eval_expr(hi, row)?;
            Ok(Value::Bool(
                v.compare(CmpOp::Ge, &l)? && v.compare(CmpOp::Le, &h)?
            ))
        }

        // ── In / Not In ──────────────────────────────────────────────────────
        Expr::In(e, list) => {
            let v = eval_expr(e, row)?;
            if matches!(v, Value::Null) { return Ok(Value::Bool(false)); }
            for item in list {
                let candidate = eval_expr(item, row)?;
                if v.compare(CmpOp::Eq, &candidate)? {
                    return Ok(Value::Bool(true));
                }
            }
            Ok(Value::Bool(false))
        }
        Expr::NotIn(e, list) => {
            let v = eval_expr(e, row)?;
            if matches!(v, Value::Null) { return Ok(Value::Bool(false)); }
            for item in list {
                let candidate = eval_expr(item, row)?;
                if v.compare(CmpOp::Eq, &candidate)? {
                    return Ok(Value::Bool(false));
                }
            }
            Ok(Value::Bool(true))
        }
    }
}

// ─── private helpers ────────────────────────────────────────────────────────

fn compare_expr(l: &Expr, r: &Expr, op: CmpOp, row: &[Value]) -> Result<Value, EvalError> {
    let lv = eval_expr(l, row)?;
    let rv = eval_expr(r, row)?;
    Ok(Value::Bool(lv.compare(op, &rv)?))
}

fn eval_as_bool(e: &Expr, row: &[Value]) -> Result<bool, EvalError> {
    match eval_expr(e, row)? {
        Value::Bool(b) => Ok(b),
        Value::Null => Ok(false),
        Value::Int(v) => Ok(v != 0),
        other => Err(EvalError(format!("Expected boolean, got {:?}", other))),
    }
}

fn to_text(v: &Value) -> Result<String, EvalError> {
    match v {
        Value::Text(s) => Ok(s.clone()),
        other => Ok(other.to_string()),
    }
}

/// SQL LIKE pattern matching: `%` = any sequence, `_` = any single char.
pub fn like_match(s: &str, pattern: &str) -> bool {
    like_match_inner(s.as_bytes(), pattern.as_bytes())
}

fn like_match_inner(s: &[u8], p: &[u8]) -> bool {
    if p.is_empty() { return s.is_empty(); }
    if p[0] == b'%' {
        // Try matching the rest of the pattern starting at every position in s
        for i in 0..=s.len() {
            if like_match_inner(&s[i..], &p[1..]) {
                return true;
            }
        }
        false
    } else if s.is_empty() {
        false
    } else if p[0] == b'_' || p[0].eq_ignore_ascii_case(&s[0]) {
        like_match_inner(&s[1..], &p[1..])
    } else {
        false
    }
}
