//! Expression types and evaluation for the Volcano execution engine.
//!
//! Expressions evaluate against deserialised tuples and produce values.
//! This module also re-exports the predicate sub-module.

pub mod convert;
pub mod correlated;
mod functions;
pub mod predicate;

#[cfg(test)]
pub mod tests;

pub use convert::{expr_from_ast, predicate_from_ast};
use functions::evaluate_scalar_function;
pub use predicate::{BooleanTest, ComparisonOp, Predicate, evaluate_predicate};

use std::cell::RefCell;
use std::rc::Rc;

use crate::backend::error::{ResultContext, RookError, RookResult};
use crate::types::value::DataValue;

use super::operators::PhysicalOperator;
use super::tuple::{ColumnInfo, Tuple};

// ── Expressions ───────────────────────────────────────────────────────────────

/// An expression that can be evaluated against a deserialised tuple.
#[derive(Clone)]
pub enum Expr {
    /// A reference to a column by name, with optional table qualifier.
    ///
    /// Example: `Expr::Column { table: Some("t1"), column: "id".into() }`
    /// for a qualified reference `t1.id`, or `{ table: None, column: "name".into() }`
    /// for an unqualified reference `name`.
    Column {
        table: Option<String>,
        column: String,
    },
    Constant(DataValue),
    Null,
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
    /// SQL `CAST(expr AS type)` — explicit type conversion.
    Cast(Box<Expr>, crate::types::datatype::DataType),
    /// A parameter whose value is set from an outer query row (correlated subquery binding).
    /// The `Rc<RefCell<Option<DataValue>>>` is shared between the predicate evaluator
    /// and the physical planner, allowing the value to be set per outer row.
    CorrelatedParam(Rc<RefCell<Option<DataValue>>>),
    /// A correlated scalar subquery evaluated once per outer tuple.
    CorrelatedScalarSubquery {
        inner_plan: Rc<RefCell<Box<dyn PhysicalOperator>>>,
        params: Vec<Rc<RefCell<Option<DataValue>>>>,
        outer_col_indices: Vec<usize>,
    },
    /// A correlated scalar subquery run as a full plan per outer key (the
    /// general case: joins, expressions over outer columns, ...).
    SubqueryScalar(Rc<correlated::CorrelatedSubquery>),
    /// `CASE WHEN cond1 THEN expr1 [WHEN cond2 THEN expr2] [ELSE exprN] END`.
    Case {
        when_then_pairs: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    /// Scalar function call: `UPPER(expr)`, `LENGTH(expr)`, `TRIM(expr)`, etc.
    /// Arguments are pre-evaluated `Expr` nodes that get evaluated against the
    /// current tuple before the function is applied.
    Function {
        name: String,
        args: Vec<Expr>,
    },
    Eq(Box<Expr>, Box<Expr>),
    Ne(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    Le(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    Ge(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
}

impl std::fmt::Debug for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Column { table, column } => f
                .debug_struct("Column")
                .field("table", table)
                .field("column", column)
                .finish(),
            Expr::Constant(dv) => f.debug_tuple("Constant").field(dv).finish(),
            Expr::Null => write!(f, "Null"),
            Expr::Add(l, r) => f.debug_tuple("Add").field(l).field(r).finish(),
            Expr::Sub(l, r) => f.debug_tuple("Sub").field(l).field(r).finish(),
            Expr::Mul(l, r) => f.debug_tuple("Mul").field(l).field(r).finish(),
            Expr::Div(l, r) => f.debug_tuple("Div").field(l).field(r).finish(),
            Expr::Cast(e, dt) => f.debug_tuple("Cast").field(e).field(dt).finish(),
            Expr::CorrelatedParam(p) => f.debug_tuple("CorrelatedParam").field(p).finish(),
            Expr::SubqueryScalar(sub) => f.debug_tuple("SubqueryScalar").field(sub).finish(),
            Expr::CorrelatedScalarSubquery {
                inner_plan: _,
                params,
                outer_col_indices,
            } => f
                .debug_struct("CorrelatedScalarSubquery")
                .field("inner_plan", &"<op>")
                .field("param_count", &params.len())
                .field("outer_col_indices", outer_col_indices)
                .finish(),
            Expr::Case {
                when_then_pairs,
                else_result,
            } => f
                .debug_struct("Case")
                .field("when_then_pairs", when_then_pairs)
                .field("else_result", else_result)
                .finish(),
            Expr::Function { name, args } => f
                .debug_struct("Function")
                .field("name", name)
                .field("args", args)
                .finish(),
            Expr::Eq(l, r) => f.debug_tuple("Eq").field(l).field(r).finish(),
            Expr::Ne(l, r) => f.debug_tuple("Ne").field(l).field(r).finish(),
            Expr::Lt(l, r) => f.debug_tuple("Lt").field(l).field(r).finish(),
            Expr::Le(l, r) => f.debug_tuple("Le").field(l).field(r).finish(),
            Expr::Gt(l, r) => f.debug_tuple("Gt").field(l).field(r).finish(),
            Expr::Ge(l, r) => f.debug_tuple("Ge").field(l).field(r).finish(),
            Expr::And(l, r) => f.debug_tuple("And").field(l).field(r).finish(),
            Expr::Or(l, r) => f.debug_tuple("Or").field(l).field(r).finish(),
            Expr::Not(e) => f.debug_tuple("Not").field(e).finish(),
            Expr::IsNull(e) => f.debug_tuple("IsNull").field(e).finish(),
            Expr::IsNotNull(e) => f.debug_tuple("IsNotNull").field(e).finish(),
        }
    }
}

impl Expr {
    /// Evaluate this expression against a tuple and schema, returning the resulting value
    /// (or `None` for NULL).
    pub fn evaluate(&self, tuple: &Tuple, schema: &[ColumnInfo]) -> RookResult<Option<DataValue>> {
        match self {
            Expr::CorrelatedScalarSubquery {
                inner_plan,
                params,
                outer_col_indices,
            } => {
                for (param, idx) in params.iter().zip(outer_col_indices.iter()) {
                    let outer_val = tuple.values.get(*idx).and_then(|v| v.clone());
                    *param.borrow_mut() = outer_val;
                }
                let mut plan_ref = inner_plan.borrow_mut();
                plan_ref
                    .reset()
                    .context("Correlated scalar subquery reset error")?;
                match plan_ref
                    .next()
                    .context("Correlated scalar subquery error")?
                {
                    None => Ok(None),
                    Some(mut t) => {
                        let val = t.values.drain(..).next().flatten();
                        if plan_ref
                            .next()
                            .context("Correlated scalar subquery error")?
                            .is_some()
                        {
                            return Err(RookError::Evaluation(
                                "Scalar subquery returned more than one row".to_string(),
                            ));
                        }
                        Ok(val)
                    }
                }
            }
            Expr::SubqueryScalar(sub) => {
                let rows = sub.rows(tuple)?;
                match rows.len() {
                    0 => Ok(None),
                    1 => Ok(rows[0].values.first().cloned().flatten()),
                    _ => Err(RookError::Evaluation(
                        "Scalar subquery returned more than one row".to_string(),
                    )),
                }
            }
            Expr::Column { table, column } => {
                // Look up the column by name in the schema (case-insensitive).
                // ColumnInfo.table is populated with actual table names by scan/join
                // operators, so table-qualified references like `t1.id` vs `t2.id`
                // resolve to different columns directly.
                //
                // Resolution rules:
                // 1. If Expr has a table qualifier (e.g. `t1.id`):
                //    Match only columns where BOTH the table name AND column name match.
                //    This correctly disambiguates `t1.id` from `t2.id` in join schemas.
                // 2. If Expr has no table qualifier (e.g. `id`):
                //    Match the first column with the given name, regardless of table.
                //    The planner already rejected names that match more than one
                //    (`expr_from_ast`), so this is a plain lookup, not a check.
                let idx = column_position(schema, table.as_deref(), column).ok_or_else(|| {
                    let wanted = match table {
                        Some(t) => format!("{}.{}", t, column),
                        None => column.clone(),
                    };
                    let available: Vec<String> = schema
                        .iter()
                        .map(|c| match &c.table {
                            Some(t) => format!("{}.{}", t, c.name),
                            None => c.name.clone(),
                        })
                        .collect();
                    format!(
                        "Column '{}' not found (available: {})",
                        wanted,
                        available.join(", ")
                    )
                })?;
                Ok(tuple
                    .values
                    .get(idx)
                    .ok_or_else(|| {
                        format!(
                            "Column '{}' index {} out of bounds (arity {})",
                            column,
                            idx,
                            tuple.arity()
                        )
                    })?
                    .clone())
            }
            Expr::Constant(dv) => Ok(Some(dv.clone())),
            Expr::Null => Ok(None),
            Expr::Add(l, r) => {
                let lv = l.evaluate(tuple, schema)?;
                let rv = r.evaluate(tuple, schema)?;
                arithmetic_op(
                    lv,
                    rv,
                    |a, b| {
                        a.checked_add(b).ok_or_else(|| {
                            RookError::Evaluation("Integer addition overflow".to_string())
                        })
                    },
                    |a, b| {
                        a.checked_add(b).ok_or_else(|| {
                            RookError::Evaluation("BigInt addition overflow".to_string())
                        })
                    },
                    |a, b| {
                        a.checked_add(b).ok_or_else(|| {
                            RookError::Evaluation("SmallInt addition overflow".to_string())
                        })
                    },
                    |a, b| Ok(a + b),
                    |a, b| Ok(a + b),
                    numeric_add,
                )
            }
            Expr::Sub(l, r) => {
                let lv = l.evaluate(tuple, schema)?;
                let rv = r.evaluate(tuple, schema)?;
                arithmetic_op(
                    lv,
                    rv,
                    |a, b| {
                        a.checked_sub(b).ok_or_else(|| {
                            RookError::Evaluation("Integer subtraction overflow".to_string())
                        })
                    },
                    |a, b| {
                        a.checked_sub(b).ok_or_else(|| {
                            RookError::Evaluation("BigInt subtraction overflow".to_string())
                        })
                    },
                    |a, b| {
                        a.checked_sub(b).ok_or_else(|| {
                            RookError::Evaluation("SmallInt subtraction overflow".to_string())
                        })
                    },
                    |a, b| Ok(a - b),
                    |a, b| Ok(a - b),
                    numeric_sub,
                )
            }
            Expr::Mul(l, r) => {
                let lv = l.evaluate(tuple, schema)?;
                let rv = r.evaluate(tuple, schema)?;
                arithmetic_op(
                    lv,
                    rv,
                    |a, b| {
                        a.checked_mul(b).ok_or_else(|| {
                            RookError::Evaluation("Integer multiplication overflow".to_string())
                        })
                    },
                    |a, b| {
                        a.checked_mul(b).ok_or_else(|| {
                            RookError::Evaluation("BigInt multiplication overflow".to_string())
                        })
                    },
                    |a, b| {
                        a.checked_mul(b).ok_or_else(|| {
                            RookError::Evaluation("SmallInt multiplication overflow".to_string())
                        })
                    },
                    |a, b| Ok(a * b),
                    |a, b| Ok(a * b),
                    numeric_mul,
                )
            }
            Expr::Div(l, r) => {
                let lv = l.evaluate(tuple, schema)?;
                let rv = r.evaluate(tuple, schema)?;
                arithmetic_op(
                    lv,
                    rv,
                    |a, b| {
                        if b == 0 {
                            Err("Division by zero".into())
                        } else {
                            a.checked_div(b)
                                .ok_or_else(|| "Integer division overflow".into())
                        }
                    },
                    |a, b| {
                        if b == 0 {
                            Err("Division by zero".into())
                        } else {
                            a.checked_div(b)
                                .ok_or_else(|| "BigInt division overflow".into())
                        }
                    },
                    |a, b| {
                        if b == 0 {
                            Err("Division by zero".into())
                        } else {
                            a.checked_div(b)
                                .ok_or_else(|| "SmallInt division overflow".into())
                        }
                    },
                    |a, b| {
                        if b == 0.0 {
                            Err("Division by zero".into())
                        } else {
                            Ok(a / b)
                        }
                    },
                    |a, b| {
                        if b == 0.0 {
                            Err("Division by zero".into())
                        } else {
                            Ok(a / b)
                        }
                    },
                    numeric_div,
                )
            }
            Expr::Cast(inner, target_type) => {
                let val = inner.evaluate(tuple, schema)?;
                match val {
                    None => Ok(None), // CAST(NULL AS type) → NULL
                    Some(dv) => {
                        let result = crate::types::functions::cast(&dv, target_type)
                            .context("CAST error")?;
                        Ok(Some(result))
                    }
                }
            }
            Expr::CorrelatedParam(param_cell) => Ok(param_cell.borrow().clone()),
            Expr::Case {
                when_then_pairs,
                else_result,
            } => {
                for (cond, res) in when_then_pairs {
                    let cond_val = cond.evaluate(tuple, schema)?;
                    match cond_val {
                        Some(DataValue::Bool(true)) => return res.evaluate(tuple, schema),
                        _ => continue, // false or NULL → try next WHEN
                    }
                }
                // No WHEN matched, use ELSE or NULL
                match else_result {
                    Some(else_expr) => else_expr.evaluate(tuple, schema),
                    None => Ok(None),
                }
            }
            Expr::Function { name, args } => evaluate_scalar_function(name, args, tuple, schema),
            Expr::Eq(l, r) => {
                let lv = l.evaluate(tuple, schema)?;
                let rv = r.evaluate(tuple, schema)?;
                match (lv, rv) {
                    (Some(l), Some(r)) => {
                        let ord = crate::types::Comparable::compare(&l, &r)
                            .context("Comparison error")?;
                        Ok(Some(DataValue::Bool(ord == std::cmp::Ordering::Equal)))
                    }
                    _ => Ok(None),
                }
            }
            Expr::Ne(l, r) => {
                let lv = l.evaluate(tuple, schema)?;
                let rv = r.evaluate(tuple, schema)?;
                match (lv, rv) {
                    (Some(l), Some(r)) => {
                        let ord = crate::types::Comparable::compare(&l, &r)
                            .context("Comparison error")?;
                        Ok(Some(DataValue::Bool(ord != std::cmp::Ordering::Equal)))
                    }
                    _ => Ok(None),
                }
            }
            Expr::Lt(l, r) => {
                let lv = l.evaluate(tuple, schema)?;
                let rv = r.evaluate(tuple, schema)?;
                match (lv, rv) {
                    (Some(l), Some(r)) => {
                        let ord = crate::types::Comparable::compare(&l, &r)
                            .context("Comparison error")?;
                        Ok(Some(DataValue::Bool(ord == std::cmp::Ordering::Less)))
                    }
                    _ => Ok(None),
                }
            }
            Expr::Le(l, r) => {
                let lv = l.evaluate(tuple, schema)?;
                let rv = r.evaluate(tuple, schema)?;
                match (lv, rv) {
                    (Some(l), Some(r)) => {
                        let ord = crate::types::Comparable::compare(&l, &r)
                            .context("Comparison error")?;
                        Ok(Some(DataValue::Bool(ord != std::cmp::Ordering::Greater)))
                    }
                    _ => Ok(None),
                }
            }
            Expr::Gt(l, r) => {
                let lv = l.evaluate(tuple, schema)?;
                let rv = r.evaluate(tuple, schema)?;
                match (lv, rv) {
                    (Some(l), Some(r)) => {
                        let ord = crate::types::Comparable::compare(&l, &r)
                            .context("Comparison error")?;
                        Ok(Some(DataValue::Bool(ord == std::cmp::Ordering::Greater)))
                    }
                    _ => Ok(None),
                }
            }
            Expr::Ge(l, r) => {
                let lv = l.evaluate(tuple, schema)?;
                let rv = r.evaluate(tuple, schema)?;
                match (lv, rv) {
                    (Some(l), Some(r)) => {
                        let ord = crate::types::Comparable::compare(&l, &r)
                            .context("Comparison error")?;
                        Ok(Some(DataValue::Bool(ord != std::cmp::Ordering::Less)))
                    }
                    _ => Ok(None),
                }
            }
            Expr::And(l, r) => {
                let lv = l.evaluate(tuple, schema)?;
                let rv = r.evaluate(tuple, schema)?;
                match (lv, rv) {
                    (Some(DataValue::Bool(false)), _) | (_, Some(DataValue::Bool(false))) => {
                        Ok(Some(DataValue::Bool(false)))
                    }
                    (Some(DataValue::Bool(true)), Some(DataValue::Bool(true))) => {
                        Ok(Some(DataValue::Bool(true)))
                    }
                    _ => Ok(None),
                }
            }
            Expr::Or(l, r) => {
                let lv = l.evaluate(tuple, schema)?;
                let rv = r.evaluate(tuple, schema)?;
                match (lv, rv) {
                    (Some(DataValue::Bool(true)), _) | (_, Some(DataValue::Bool(true))) => {
                        Ok(Some(DataValue::Bool(true)))
                    }
                    (Some(DataValue::Bool(false)), Some(DataValue::Bool(false))) => {
                        Ok(Some(DataValue::Bool(false)))
                    }
                    _ => Ok(None),
                }
            }
            Expr::Not(inner) => {
                let v = inner.evaluate(tuple, schema)?;
                match v {
                    Some(DataValue::Bool(b)) => Ok(Some(DataValue::Bool(!b))),
                    _ => Ok(None),
                }
            }
            Expr::IsNull(inner) => {
                let v = inner.evaluate(tuple, schema)?;
                Ok(Some(DataValue::Bool(v.is_none())))
            }
            Expr::IsNotNull(inner) => {
                let v = inner.evaluate(tuple, schema)?;
                Ok(Some(DataValue::Bool(v.is_some())))
            }
        }
    }
}

/// Helper: apply a binary arithmetic operation to two nullable values.
/// Supports INT, BIGINT, SMALLINT, DOUBLE PRECISION, and cross-type promotion.
fn scale_numeric_unscaled(unscaled: i128, current_scale: u8, target_scale: u8) -> RookResult<i128> {
    if current_scale == target_scale {
        Ok(unscaled)
    } else if target_scale > current_scale {
        let diff = (target_scale - current_scale) as u32;
        let factor = 10_i128
            .checked_pow(diff)
            .ok_or_else(|| RookError::Evaluation("Numeric scale factor overflow".to_string()))?;
        unscaled
            .checked_mul(factor)
            .ok_or_else(|| RookError::Evaluation("Numeric scaling overflow".to_string()))
    } else {
        let diff = (current_scale - target_scale) as u32;
        let factor = 10_i128
            .checked_pow(diff)
            .ok_or_else(|| RookError::Evaluation("Numeric scale factor overflow".to_string()))?;
        Ok(unscaled / factor)
    }
}

fn numeric_add(
    a: crate::types::value::NumericValue,
    b: crate::types::value::NumericValue,
) -> RookResult<crate::types::value::NumericValue> {
    let target_scale = a.scale.max(b.scale);
    let a_unscaled = scale_numeric_unscaled(a.unscaled, a.scale, target_scale)?;
    let b_unscaled = scale_numeric_unscaled(b.unscaled, b.scale, target_scale)?;
    let unscaled = a_unscaled
        .checked_add(b_unscaled)
        .ok_or_else(|| RookError::Evaluation("Numeric addition overflow".to_string()))?;
    Ok(crate::types::value::NumericValue {
        unscaled,
        scale: target_scale,
    })
}

fn numeric_sub(
    a: crate::types::value::NumericValue,
    b: crate::types::value::NumericValue,
) -> RookResult<crate::types::value::NumericValue> {
    let target_scale = a.scale.max(b.scale);
    let a_unscaled = scale_numeric_unscaled(a.unscaled, a.scale, target_scale)?;
    let b_unscaled = scale_numeric_unscaled(b.unscaled, b.scale, target_scale)?;
    let unscaled = a_unscaled
        .checked_sub(b_unscaled)
        .ok_or_else(|| RookError::Evaluation("Numeric subtraction overflow".to_string()))?;
    Ok(crate::types::value::NumericValue {
        unscaled,
        scale: target_scale,
    })
}

fn numeric_mul(
    a: crate::types::value::NumericValue,
    b: crate::types::value::NumericValue,
) -> RookResult<crate::types::value::NumericValue> {
    let target_scale = a
        .scale
        .checked_add(b.scale)
        .ok_or_else(|| RookError::Evaluation("Numeric scale overflow".to_string()))?;
    let unscaled = a
        .unscaled
        .checked_mul(b.unscaled)
        .ok_or_else(|| RookError::Evaluation("Numeric multiplication overflow".to_string()))?;
    Ok(crate::types::value::NumericValue {
        unscaled,
        scale: target_scale,
    })
}

fn numeric_div(
    a: crate::types::value::NumericValue,
    b: crate::types::value::NumericValue,
) -> RookResult<crate::types::value::NumericValue> {
    if b.unscaled == 0 {
        return Err(RookError::Evaluation("Division by zero".to_string()));
    }
    let target_scale = a.scale.max(b.scale);
    let shift = target_scale as i32 + b.scale as i32 - a.scale as i32;
    let a_shifted = if shift >= 0 {
        let factor = 10_i128
            .checked_pow(shift as u32)
            .ok_or_else(|| RookError::Evaluation("Numeric scale factor overflow".to_string()))?;
        a.unscaled
            .checked_mul(factor)
            .ok_or_else(|| RookError::Evaluation("Numeric division overflow".to_string()))?
    } else {
        let factor = 10_i128
            .checked_pow((-shift) as u32)
            .ok_or_else(|| RookError::Evaluation("Numeric scale factor overflow".to_string()))?;
        a.unscaled / factor
    };
    let unscaled = a_shifted
        .checked_div(b.unscaled)
        .ok_or_else(|| RookError::Evaluation("Numeric division overflow".to_string()))?;
    Ok(crate::types::value::NumericValue {
        unscaled,
        scale: target_scale,
    })
}

fn numeric_rem(
    a: crate::types::value::NumericValue,
    b: crate::types::value::NumericValue,
) -> RookResult<crate::types::value::NumericValue> {
    if b.unscaled == 0 {
        return Err(RookError::Evaluation("Division by zero".to_string()));
    }
    let target_scale = a.scale.max(b.scale);
    let a_unscaled = scale_numeric_unscaled(a.unscaled, a.scale, target_scale)?;
    let b_unscaled = scale_numeric_unscaled(b.unscaled, b.scale, target_scale)?;
    let unscaled = a_unscaled
        .checked_rem(b_unscaled)
        .ok_or_else(|| RookError::Evaluation("Numeric modulo overflow".to_string()))?;
    Ok(crate::types::value::NumericValue {
        unscaled,
        scale: target_scale,
    })
}

/// Helper: apply a binary arithmetic operation to two nullable values.
/// Supports INT, BIGINT, SMALLINT, DOUBLE PRECISION, NUMERIC, and cross-type promotion.
#[allow(clippy::too_many_arguments)]
fn arithmetic_op<FI, FB, FD, FF, FDbl, FN>(
    left: Option<DataValue>,
    right: Option<DataValue>,
    int_op: FI,
    bigint_op: FB,
    smallint_op: FD,
    float_op: FF,
    double_op: FDbl,
    numeric_op: FN,
) -> RookResult<Option<DataValue>>
where
    FI: FnOnce(i32, i32) -> RookResult<i32>,
    FB: FnOnce(i64, i64) -> RookResult<i64>,
    FD: FnOnce(i16, i16) -> RookResult<i16>,
    FF: FnOnce(f64, f64) -> RookResult<f64>,
    FDbl: FnOnce(f64, f64) -> RookResult<f64>,
    FN: FnOnce(
        crate::types::value::NumericValue,
        crate::types::value::NumericValue,
    ) -> RookResult<crate::types::value::NumericValue>,
{
    match (left, right) {
        (None, _) | (_, None) => Ok(None),
        (Some(DataValue::Int(a)), Some(DataValue::Int(b))) => {
            int_op(a, b).map(|v| Some(DataValue::Int(v)))
        }
        (Some(DataValue::BigInt(a)), Some(DataValue::BigInt(b))) => {
            bigint_op(a, b).map(|v| Some(DataValue::BigInt(v)))
        }
        (Some(DataValue::SmallInt(a)), Some(DataValue::SmallInt(b))) => {
            smallint_op(a, b).map(|v| Some(DataValue::SmallInt(v)))
        }
        (Some(DataValue::Real(a)), Some(DataValue::Real(b))) => float_op(a.0 as f64, b.0 as f64)
            .map(|v| Some(DataValue::Real(crate::types::value::OrderedF32(v as f32)))),
        (Some(DataValue::DoublePrecision(a)), Some(DataValue::DoublePrecision(b))) => {
            double_op(a.0, b.0).map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            })
        }
        // Cross-type promotion
        (Some(DataValue::Real(a)), Some(DataValue::DoublePrecision(b))) => {
            double_op(a.0 as f64, b.0).map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            })
        }
        (Some(DataValue::DoublePrecision(a)), Some(DataValue::Real(b))) => {
            double_op(a.0, b.0 as f64).map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            })
        }
        (Some(DataValue::Int(a)), Some(DataValue::DoublePrecision(b))) => double_op(a as f64, b.0)
            .map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            }),
        (Some(DataValue::DoublePrecision(a)), Some(DataValue::Int(b))) => double_op(a.0, b as f64)
            .map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            }),
        (Some(DataValue::SmallInt(a)), Some(DataValue::Int(b))) => {
            int_op(a as i32, b).map(|v| Some(DataValue::Int(v)))
        }
        (Some(DataValue::Int(a)), Some(DataValue::SmallInt(b))) => {
            int_op(a, b as i32).map(|v| Some(DataValue::Int(v)))
        }
        (Some(DataValue::SmallInt(a)), Some(DataValue::BigInt(b))) => {
            bigint_op(a as i64, b).map(|v| Some(DataValue::BigInt(v)))
        }
        (Some(DataValue::BigInt(a)), Some(DataValue::SmallInt(b))) => {
            bigint_op(a, b as i64).map(|v| Some(DataValue::BigInt(v)))
        }
        (Some(DataValue::Int(a)), Some(DataValue::BigInt(b))) => {
            bigint_op(a as i64, b).map(|v| Some(DataValue::BigInt(v)))
        }
        (Some(DataValue::BigInt(a)), Some(DataValue::Int(b))) => {
            bigint_op(a, b as i64).map(|v| Some(DataValue::BigInt(v)))
        }
        (Some(DataValue::SmallInt(a)), Some(DataValue::DoublePrecision(b))) => {
            double_op(a as f64, b.0).map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            })
        }
        (Some(DataValue::DoublePrecision(a)), Some(DataValue::SmallInt(b))) => {
            double_op(a.0, b as f64).map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            })
        }
        (Some(DataValue::BigInt(a)), Some(DataValue::DoublePrecision(b))) => {
            double_op(a as f64, b.0).map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            })
        }
        (Some(DataValue::DoublePrecision(a)), Some(DataValue::BigInt(b))) => {
            double_op(a.0, b as f64).map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            })
        }
        // Cross-type REAL ↔ INTEGER arithmetic (widen both to DoublePrecision)
        (Some(DataValue::Real(a)), Some(DataValue::Int(b))) => {
            double_op(a.0 as f64, b as f64).map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            })
        }
        (Some(DataValue::Int(a)), Some(DataValue::Real(b))) => {
            double_op(a as f64, b.0 as f64).map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            })
        }
        (Some(DataValue::Real(a)), Some(DataValue::SmallInt(b))) => double_op(a.0 as f64, b as f64)
            .map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            }),
        (Some(DataValue::SmallInt(a)), Some(DataValue::Real(b))) => double_op(a as f64, b.0 as f64)
            .map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            }),
        (Some(DataValue::Real(a)), Some(DataValue::BigInt(b))) => double_op(a.0 as f64, b as f64)
            .map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            }),
        (Some(DataValue::BigInt(a)), Some(DataValue::Real(b))) => double_op(a as f64, b.0 as f64)
            .map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            }),
        // Exact NUMERIC ↔ NUMERIC arithmetic
        (Some(DataValue::Numeric(a)), Some(DataValue::Numeric(b))) => {
            numeric_op(a, b).map(|v| Some(DataValue::Numeric(v)))
        }
        // Cross-type NUMERIC ↔ INTEGER: exact promotion to NumericValue (scale 0)
        (Some(DataValue::Numeric(a)), Some(DataValue::SmallInt(b))) => numeric_op(
            a,
            crate::types::value::NumericValue {
                unscaled: b as i128,
                scale: 0,
            },
        )
        .map(|v| Some(DataValue::Numeric(v))),
        (Some(DataValue::SmallInt(a)), Some(DataValue::Numeric(b))) => numeric_op(
            crate::types::value::NumericValue {
                unscaled: a as i128,
                scale: 0,
            },
            b,
        )
        .map(|v| Some(DataValue::Numeric(v))),
        (Some(DataValue::Numeric(a)), Some(DataValue::Int(b))) => numeric_op(
            a,
            crate::types::value::NumericValue {
                unscaled: b as i128,
                scale: 0,
            },
        )
        .map(|v| Some(DataValue::Numeric(v))),
        (Some(DataValue::Int(a)), Some(DataValue::Numeric(b))) => numeric_op(
            crate::types::value::NumericValue {
                unscaled: a as i128,
                scale: 0,
            },
            b,
        )
        .map(|v| Some(DataValue::Numeric(v))),
        (Some(DataValue::Numeric(a)), Some(DataValue::BigInt(b))) => numeric_op(
            a,
            crate::types::value::NumericValue {
                unscaled: b as i128,
                scale: 0,
            },
        )
        .map(|v| Some(DataValue::Numeric(v))),
        (Some(DataValue::BigInt(a)), Some(DataValue::Numeric(b))) => numeric_op(
            crate::types::value::NumericValue {
                unscaled: a as i128,
                scale: 0,
            },
            b,
        )
        .map(|v| Some(DataValue::Numeric(v))),
        // Cross-type NUMERIC ↔ FLOAT: convert both to f64, use double_op
        (Some(DataValue::Numeric(a)), Some(DataValue::Real(b))) => {
            double_op(a.unscaled as f64 / 10_f64.powi(a.scale as i32), b.0 as f64).map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            })
        }
        (Some(DataValue::Real(a)), Some(DataValue::Numeric(b))) => {
            double_op(a.0 as f64, b.unscaled as f64 / 10_f64.powi(b.scale as i32)).map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            })
        }
        (Some(DataValue::Numeric(a)), Some(DataValue::DoublePrecision(b))) => {
            double_op(a.unscaled as f64 / 10_f64.powi(a.scale as i32), b.0).map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            })
        }
        (Some(DataValue::DoublePrecision(a)), Some(DataValue::Numeric(b))) => {
            double_op(a.0, b.unscaled as f64 / 10_f64.powi(b.scale as i32)).map(|v| {
                Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(
                    v,
                )))
            })
        }
        (Some(l), Some(r)) => Err(RookError::Evaluation(format!(
            "Arithmetic not supported between {:?} and {:?}",
            l, r
        ))),
    }
}

/// Position in `schema` of the column `column`, qualified by `table` when
/// given; `None` when there is none.
///
/// A qualified name matches a column of that table, or else a column whose
/// table an operator dropped (but never a column of another table). An
/// unqualified name matches the first column with that name that a
/// `USING` join has not merged away. The planner has already rejected
/// ambiguous names (see [`resolve_column`]), so this is the cheap lookup
/// for rows.
pub(crate) fn column_position(
    schema: &[ColumnInfo],
    table: Option<&str>,
    column: &str,
) -> Option<usize> {
    match table {
        Some(t) => schema
            .iter()
            .position(|ci| {
                ci.name.eq_ignore_ascii_case(column)
                    && ci
                        .table
                        .as_ref()
                        .is_some_and(|ct| ct.eq_ignore_ascii_case(t))
            })
            .or_else(|| {
                schema
                    .iter()
                    .position(|ci| ci.name.eq_ignore_ascii_case(column) && ci.table.is_none())
            }),
        None => schema
            .iter()
            .position(|ci| ci.name.eq_ignore_ascii_case(column) && !ci.merged)
            .or_else(|| {
                schema
                    .iter()
                    .position(|ci| ci.name.eq_ignore_ascii_case(column))
            }),
    }
}

/// Bind a column reference to its position in `schema`, once, at plan time.
///
/// Like [`column_position`], but a bare name that matches more than one
/// column is an error (`Column reference 'id' is ambiguous`), as in SQL.
/// `Ok(None)` means there is no such column. Columns a `USING` / `NATURAL`
/// join merged do not count as a second match.
pub(crate) fn resolve_column(
    schema: &[ColumnInfo],
    table: Option<&str>,
    column: &str,
) -> RookResult<Option<usize>> {
    if table.is_none() {
        let mut named = schema
            .iter()
            .enumerate()
            .filter(|(_, ci)| ci.name.eq_ignore_ascii_case(column) && !ci.merged);
        if named.next().is_some() && named.next().is_some() {
            return Err(RookError::AmbiguousColumn(column.to_string()));
        }
    }
    Ok(column_position(schema, table, column))
}

/// The names of `schema`'s columns for checking bare references: a column
/// a join merged away has none, so its twin is the only match.
pub(crate) fn binding_names(schema: &[ColumnInfo]) -> Vec<String> {
    schema
        .iter()
        .map(|c| {
            if c.merged {
                String::new()
            } else {
                c.name.clone()
            }
        })
        .collect()
}
