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
pub use predicate::{BooleanTest, ComparisonOp, InList, Predicate, evaluate_predicate};

use std::cell::RefCell;
use std::rc::Rc;

use crate::backend::error::{ResultContext, RookError, RookResult};
use crate::types::value::DataValue;

use super::operators::PhysicalOperator;
use super::tuple::{ColumnInfo, Tuple};

// ── Expressions ───────────────────────────────────────────────────────────────

/// An expression that can be evaluated against a deserialised tuple.
#[derive(Clone, Debug)]
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
    /// A column bound to its position in the rows the expression is
    /// evaluated against, by [`Expr::bind`]: read without a name lookup.
    Slot(usize),
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
    /// A comparison, AND / OR / NOT or IS [NOT] NULL used as a value
    /// (`SELECT x > 1`): evaluated by [`evaluate_predicate`], the one
    /// implementation of SQL's three-valued logic, as TRUE, FALSE or NULL.
    Predicate(Box<Predicate>),
}

impl Expr {
    /// This expression with every column it names bound to its position in
    /// `schema`, so evaluating it against rows of `schema` skips the
    /// per-row name lookup. Names `schema` lacks are left for
    /// [`Expr::evaluate`] to report; subqueries bind their own columns.
    pub fn bind(&self, schema: &[ColumnInfo]) -> Expr {
        let b = |e: &Expr| Box::new(e.bind(schema));
        match self {
            Expr::Column { table, column } => {
                match column_position(schema, table.as_deref(), column) {
                    Some(i) => Expr::Slot(i),
                    None => self.clone(),
                }
            }
            Expr::Slot(_)
            | Expr::Constant(_)
            | Expr::Null
            | Expr::CorrelatedParam(_)
            | Expr::CorrelatedScalarSubquery { .. }
            | Expr::SubqueryScalar(_) => self.clone(),
            Expr::Add(l, r) => Expr::Add(b(l), b(r)),
            Expr::Sub(l, r) => Expr::Sub(b(l), b(r)),
            Expr::Mul(l, r) => Expr::Mul(b(l), b(r)),
            Expr::Div(l, r) => Expr::Div(b(l), b(r)),
            Expr::Predicate(p) => Expr::Predicate(Box::new(p.bind(schema))),
            Expr::Cast(e, ty) => Expr::Cast(b(e), ty.clone()),
            Expr::Case {
                when_then_pairs,
                else_result,
            } => Expr::Case {
                when_then_pairs: when_then_pairs
                    .iter()
                    .map(|(w, t)| (w.bind(schema), t.bind(schema)))
                    .collect(),
                else_result: else_result.as_deref().map(b),
            },
            Expr::Function { name, args } => Expr::Function {
                name: name.clone(),
                args: args.iter().map(|a| a.bind(schema)).collect(),
            },
        }
    }

    /// [`Expr::evaluate`], borrowing the value from the row or the
    /// expression when it is a column or a constant instead of copying it:
    /// comparing a text column allocates nothing.
    pub fn evaluate_ref<'a>(
        &'a self,
        tuple: &'a Tuple,
        schema: &[ColumnInfo],
    ) -> RookResult<Option<std::borrow::Cow<'a, DataValue>>> {
        use std::borrow::Cow;
        Ok(match self {
            Expr::Slot(i) => slot_value(tuple, *i)?.map(Cow::Borrowed),
            Expr::Constant(dv) => Some(Cow::Borrowed(dv)),
            Expr::Null => None,
            Expr::Column { table, column } => {
                match column_position(schema, table.as_deref(), column) {
                    Some(i) => slot_value(tuple, i)?.map(Cow::Borrowed),
                    None => evaluate_column(table, column, tuple, schema)?.map(Cow::Owned),
                }
            }
            other => other.evaluate(tuple, schema)?.map(Cow::Owned),
        })
    }

    /// Evaluate this expression against a tuple and schema, returning the resulting value
    /// (or `None` for NULL).
    pub fn evaluate(&self, tuple: &Tuple, schema: &[ColumnInfo]) -> RookResult<Option<DataValue>> {
        match self {
            Expr::CorrelatedScalarSubquery {
                inner_plan,
                params,
                outer_col_indices,
            } => evaluate_correlated_scalar_subquery(inner_plan, params, outer_col_indices, tuple),
            Expr::SubqueryScalar(sub) => evaluate_subquery_scalar(sub, tuple),
            Expr::Column { table, column } => evaluate_column(table, column, tuple, schema),
            Expr::Slot(i) => Ok(slot_value(tuple, *i)?.cloned()),
            Expr::Constant(dv) => Ok(Some(dv.clone())),
            Expr::Null => Ok(None),
            Expr::Add(l, r) => add_values(l.evaluate(tuple, schema)?, r.evaluate(tuple, schema)?),
            Expr::Sub(l, r) => sub_values(l.evaluate(tuple, schema)?, r.evaluate(tuple, schema)?),
            Expr::Mul(l, r) => mul_values(l.evaluate(tuple, schema)?, r.evaluate(tuple, schema)?),
            Expr::Div(l, r) => div_values(l.evaluate(tuple, schema)?, r.evaluate(tuple, schema)?),
            Expr::Cast(inner, target_type) => {
                evaluate_cast(inner.evaluate(tuple, schema)?, target_type)
            }
            Expr::CorrelatedParam(param_cell) => Ok(param_cell.borrow().clone()),
            Expr::Case {
                when_then_pairs,
                else_result,
            } => evaluate_case(when_then_pairs, else_result, tuple, schema),
            Expr::Function { name, args } => evaluate_scalar_function(name, args, tuple, schema),
            Expr::Predicate(p) => Ok(evaluate_predicate(p, tuple, schema)?.map(DataValue::Bool)),
        }
    }
}

/// Run the inner plan of a correlated scalar subquery with the outer row's
/// values bound to its parameters; at most one row may come back.
fn evaluate_correlated_scalar_subquery(
    inner_plan: &Rc<RefCell<Box<dyn PhysicalOperator>>>,
    params: &[Rc<RefCell<Option<DataValue>>>],
    outer_col_indices: &[usize],
    tuple: &Tuple,
) -> RookResult<Option<DataValue>> {
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

/// A correlated scalar subquery run as a full plan: no rows is NULL, more than one is an error.
fn evaluate_subquery_scalar(
    sub: &correlated::CorrelatedSubquery,
    tuple: &Tuple,
) -> RookResult<Option<DataValue>> {
    let rows = sub.rows(tuple)?;
    match rows.len() {
        0 => Ok(None),
        1 => Ok(rows[0].values.first().cloned().flatten()),
        _ => Err(RookError::Evaluation(
            "Scalar subquery returned more than one row".to_string(),
        )),
    }
}

/// The value at position `i` of `tuple`, for a bound column.
fn slot_value(tuple: &Tuple, i: usize) -> RookResult<Option<&DataValue>> {
    tuple.values.get(i).map(Option::as_ref).ok_or_else(|| {
        RookError::Internal(format!(
            "Column position {} out of bounds (arity {})",
            i,
            tuple.arity()
        ))
    })
}

/// Look up a column value in the tuple by name (and optional table qualifier).
fn evaluate_column(
    table: &Option<String>,
    column: &str,
    tuple: &Tuple,
    schema: &[ColumnInfo],
) -> RookResult<Option<DataValue>> {
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
            None => column.to_string(),
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

/// SQL addition of two nullable values, promoting across numeric types.
fn add_values(lv: Option<DataValue>, rv: Option<DataValue>) -> RookResult<Option<DataValue>> {
    match (&lv, &rv) {
        (Some(DataValue::Date(d)), Some(n)) | (Some(n), Some(DataValue::Date(d))) => {
            if let Some(days) = integer_days(n) {
                return shift_date(*d, days);
            }
        }
        _ => {}
    }
    arithmetic_op(
        lv,
        rv,
        |a, b| {
            a.checked_add(b)
                .ok_or_else(|| RookError::Evaluation("Integer addition overflow".to_string()))
        },
        |a, b| {
            a.checked_add(b)
                .ok_or_else(|| RookError::Evaluation("BigInt addition overflow".to_string()))
        },
        |a, b| {
            a.checked_add(b)
                .ok_or_else(|| RookError::Evaluation("SmallInt addition overflow".to_string()))
        },
        |a, b| Ok(a + b),
        |a, b| Ok(a + b),
        numeric_add,
    )
}

/// An integer value as a count of days, for `date ± n`.
fn integer_days(v: &DataValue) -> Option<i64> {
    match v {
        DataValue::SmallInt(n) => Some(*n as i64),
        DataValue::Int(n) => Some(*n as i64),
        DataValue::BigInt(n) => Some(*n),
        _ => None,
    }
}

/// `date + days`, as in PostgreSQL: a DATE plus an integer is a DATE.
fn shift_date(date: chrono::NaiveDate, days: i64) -> RookResult<Option<DataValue>> {
    let n = chrono::Days::new(days.unsigned_abs());
    if days < 0 {
        date.checked_sub_days(n)
    } else {
        date.checked_add_days(n)
    }
    .map(|d| Some(DataValue::Date(d)))
    .ok_or_else(|| RookError::Evaluation("date out of range".to_string()))
}

/// SQL subtraction of two nullable values, promoting across numeric types.
fn sub_values(lv: Option<DataValue>, rv: Option<DataValue>) -> RookResult<Option<DataValue>> {
    if let (Some(DataValue::Date(d)), Some(n)) = (&lv, &rv)
        && let Some(days) = integer_days(n)
    {
        return shift_date(*d, days.checked_neg().unwrap_or(i64::MAX));
    }
    arithmetic_op(
        lv,
        rv,
        |a, b| {
            a.checked_sub(b)
                .ok_or_else(|| RookError::Evaluation("Integer subtraction overflow".to_string()))
        },
        |a, b| {
            a.checked_sub(b)
                .ok_or_else(|| RookError::Evaluation("BigInt subtraction overflow".to_string()))
        },
        |a, b| {
            a.checked_sub(b)
                .ok_or_else(|| RookError::Evaluation("SmallInt subtraction overflow".to_string()))
        },
        |a, b| Ok(a - b),
        |a, b| Ok(a - b),
        numeric_sub,
    )
}

/// SQL multiplication of two nullable values, promoting across numeric types.
fn mul_values(lv: Option<DataValue>, rv: Option<DataValue>) -> RookResult<Option<DataValue>> {
    arithmetic_op(
        lv,
        rv,
        |a, b| {
            a.checked_mul(b)
                .ok_or_else(|| RookError::Evaluation("Integer multiplication overflow".to_string()))
        },
        |a, b| {
            a.checked_mul(b)
                .ok_or_else(|| RookError::Evaluation("BigInt multiplication overflow".to_string()))
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

/// SQL division of two nullable values, promoting across numeric types.
fn div_values(lv: Option<DataValue>, rv: Option<DataValue>) -> RookResult<Option<DataValue>> {
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

/// `CAST(expr AS type)` of an already-evaluated operand.
fn evaluate_cast(
    val: Option<DataValue>,
    target_type: &crate::types::datatype::DataType,
) -> RookResult<Option<DataValue>> {
    match val {
        None => Ok(None), // CAST(NULL AS type) → NULL
        Some(dv) => {
            let result = crate::types::functions::cast(&dv, target_type).context("CAST error")?;
            Ok(Some(result))
        }
    }
}

/// `CASE WHEN ... THEN ... [ELSE ...] END`: the first WHEN that is true wins.
fn evaluate_case(
    when_then_pairs: &[(Expr, Expr)],
    else_result: &Option<Box<Expr>>,
    tuple: &Tuple,
    schema: &[ColumnInfo],
) -> RookResult<Option<DataValue>> {
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
            numeric_op(*a, *b).map(|v| Some(DataValue::Numeric(Box::new(v))))
        }
        // Cross-type NUMERIC ↔ INTEGER: exact promotion to NumericValue (scale 0)
        (Some(DataValue::Numeric(a)), Some(DataValue::SmallInt(b))) => numeric_op(
            *a,
            crate::types::value::NumericValue {
                unscaled: b as i128,
                scale: 0,
            },
        )
        .map(|v| Some(DataValue::Numeric(Box::new(v)))),
        (Some(DataValue::SmallInt(a)), Some(DataValue::Numeric(b))) => numeric_op(
            crate::types::value::NumericValue {
                unscaled: a as i128,
                scale: 0,
            },
            *b,
        )
        .map(|v| Some(DataValue::Numeric(Box::new(v)))),
        (Some(DataValue::Numeric(a)), Some(DataValue::Int(b))) => numeric_op(
            *a,
            crate::types::value::NumericValue {
                unscaled: b as i128,
                scale: 0,
            },
        )
        .map(|v| Some(DataValue::Numeric(Box::new(v)))),
        (Some(DataValue::Int(a)), Some(DataValue::Numeric(b))) => numeric_op(
            crate::types::value::NumericValue {
                unscaled: a as i128,
                scale: 0,
            },
            *b,
        )
        .map(|v| Some(DataValue::Numeric(Box::new(v)))),
        (Some(DataValue::Numeric(a)), Some(DataValue::BigInt(b))) => numeric_op(
            *a,
            crate::types::value::NumericValue {
                unscaled: b as i128,
                scale: 0,
            },
        )
        .map(|v| Some(DataValue::Numeric(Box::new(v)))),
        (Some(DataValue::BigInt(a)), Some(DataValue::Numeric(b))) => numeric_op(
            crate::types::value::NumericValue {
                unscaled: a as i128,
                scale: 0,
            },
            *b,
        )
        .map(|v| Some(DataValue::Numeric(Box::new(v)))),
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
