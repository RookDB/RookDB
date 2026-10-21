//! Free helper functions for the physical planner.
//!
//! These are standalone utility functions used across multiple planner modules.

use crate::backend::error::{RookError, RookResult};
use crate::types::DataValue;
use crate::types::datatype::DataType;

use super::super::operators::{AggregateFunction, infer_aggregate_output_type};

/// Promote two types to a common type using SQL-99 implicit promotion rules.
///
/// Rules:
///   - If either is DOUBLE → DOUBLE
///   - If either is REAL → DOUBLE
///   - If either is NUMERIC → DOUBLE
///   - If either is BIGINT → BIGINT
///   - If either is INT → INT
///   - SMALLINT + SMALLINT → SMALLINT
///   - Otherwise → INT (non-numeric fallback)
pub fn promote_numeric_type(a: &DataType, b: &DataType) -> DataType {
    use DataType as DT;

    let is_numeric = |t: &DataType| -> bool {
        matches!(
            t,
            DT::SmallInt
                | DT::Int
                | DT::BigInt
                | DT::Real
                | DT::DoublePrecision
                | DT::Numeric { .. }
                | DT::Decimal { .. }
        )
    };

    if !is_numeric(a) || !is_numeric(b) {
        return DT::Int;
    }

    let rank = |t: &DataType| -> u8 {
        match t {
            DT::SmallInt => 1,
            DT::Int => 2,
            DT::BigInt => 3,
            DT::Real => 4,
            DT::Numeric { .. } | DT::Decimal { .. } => 4,
            DT::DoublePrecision => 5,
            _ => 0,
        }
    };

    match rank(a).max(rank(b)) {
        1 => DT::SmallInt,
        2 => DT::Int,
        3 => DT::BigInt,
        _ => DT::DoublePrecision,
    }
}

/// Result type of arithmetic between a NUMERIC and another exact number
/// (NUMERIC or an integer), which the executor computes exactly as a NUMERIC:
/// `SUM(bigint_col) + 1` is a NUMERIC, not a DOUBLE PRECISION. `None` when
/// neither side is a NUMERIC or one side is not exact.
fn exact_numeric_result_type(
    a: &DataType,
    op: rook_ast::ArithOp,
    b: &DataType,
) -> Option<DataType> {
    use DataType as DT;

    // Scale of an exact operand; integers have none.
    let scale = |t: &DataType| -> Option<u8> {
        match t {
            DT::SmallInt | DT::Int | DT::BigInt => Some(0),
            DT::Numeric { scale, .. } | DT::Decimal { scale, .. } => Some(*scale),
            _ => None,
        }
    };
    let is_decimal = |t: &DataType| matches!(t, DT::Numeric { .. } | DT::Decimal { .. });
    if !is_decimal(a) && !is_decimal(b) {
        return None;
    }
    let (sa, sb) = (scale(a)?, scale(b)?);
    // The scales the executor's numeric_add/sub/mul/div produce.
    let scale = match op {
        rook_ast::ArithOp::Mul => sa.saturating_add(sb).min(MAX_NUMERIC_PRECISION),
        rook_ast::ArithOp::Div => {
            crate::backend::executor::physical::expr::numeric_division_scale(sa, sb)
        }
        _ => sa.max(sb),
    };
    Some(DT::Numeric {
        precision: MAX_NUMERIC_PRECISION,
        scale,
    })
}

/// Widest NUMERIC precision the engine has.
const MAX_NUMERIC_PRECISION: u8 = 38;

/// Convert a `DataValue` (from the physical engine) to a `ConstantValue` (AST node).
/// Used only for nested scalar subqueries inside Binary/Cast expressions.
pub fn data_value_to_constant_value(dv: &DataValue) -> rook_ast::ConstantValue {
    use crate::types::DataValue as DV;
    match dv {
        DV::Int(v) => rook_ast::ConstantValue::Int(*v as i64),
        DV::BigInt(v) => rook_ast::ConstantValue::Int(*v),
        DV::SmallInt(v) => rook_ast::ConstantValue::Int(*v as i64),
        DV::DoublePrecision(v) => rook_ast::ConstantValue::Float(v.0),
        DV::Real(v) => rook_ast::ConstantValue::Float(v.0 as f64),
        DV::Bool(v) => rook_ast::ConstantValue::Boolean(*v),
        DV::Varchar(s) | DV::Char(s) => rook_ast::ConstantValue::Text(s.clone()),
        DV::Numeric(n) => {
            rook_ast::ConstantValue::Float(n.unscaled as f64 / 10f64.powi(n.scale as i32))
        }
        DV::Date(_) | DV::Time(_) | DV::Timestamp(_) => {
            rook_ast::ConstantValue::Text(format!("{:?}", dv))
        }
        DV::Bit(_) => rook_ast::ConstantValue::Int(0),
    }
}

/// Derive a display name from an AST expression node.
pub fn ast_expr_to_output_name(expr: &rook_ast::ExprNode) -> String {
    match expr {
        rook_ast::ExprNode::Column(name) => name.clone(),
        rook_ast::ExprNode::Compound(parts) => parts.last().cloned().unwrap_or_default(),
        rook_ast::ExprNode::Constant(cv) => cv.to_string(),
        rook_ast::ExprNode::Binary { .. } => "expr".to_string(),
        rook_ast::ExprNode::Cast { data_type, .. } => {
            format!("CAST({})", data_type)
        }
        rook_ast::ExprNode::ScalarSubquery(_) => "(scalar subquery)".to_string(),
        rook_ast::ExprNode::Function { name, .. } => name.clone(),
        rook_ast::ExprNode::Case { .. } => "CASE".to_string(),
        rook_ast::ExprNode::Compare { .. } => "compare".to_string(),
        rook_ast::ExprNode::Logical { .. } => "logical".to_string(),
        rook_ast::ExprNode::Not(_) => "not".to_string(),
        rook_ast::ExprNode::IsNull(_) => "is_null".to_string(),
        rook_ast::ExprNode::IsNotNull(_) => "is_not_null".to_string(),
    }
}

/// Infer the type of an AST expression evaluated against rows of `schema`.
///
/// Columns resolve as the executor resolves them, so a qualified `b.id`
/// takes `b`'s type even when an earlier `a.id` has the same name. A NULL
/// literal has no type of its own: it takes the type of the other operand,
/// branch or argument it stands beside, and a value that is NULL throughout
/// (`SELECT NULL`) is typed INT.
pub fn infer_expr_type_in_schema(
    expr: &rook_ast::ExprNode,
    schema: &[super::super::tuple::ColumnInfo],
) -> RookResult<DataType> {
    Ok(infer_type(expr, schema)?.unwrap_or(DataType::Int))
}

/// [`infer_expr_type_in_schema`]; `None` is the unknown type of NULL.
fn infer_type(
    expr: &rook_ast::ExprNode,
    schema: &[super::super::tuple::ColumnInfo],
) -> RookResult<Option<DataType>> {
    use rook_ast::{ConstantValue as CV, ExprNode as E, FunctionArg};
    let column = |table: Option<&str>, name: &str| {
        super::super::expr::column_position(schema, table, name)
            .map(|i| Some(schema[i].data_type.clone()))
            .ok_or_else(|| RookError::NotFound {
                entity: "Column",
                name: match table {
                    Some(t) => format!("{}.{}", t, name),
                    None => name.to_string(),
                },
            })
    };
    // The first argument or branch whose type is known.
    let first_known = |exprs: &mut dyn Iterator<Item = &E>| -> RookResult<Option<DataType>> {
        for e in exprs {
            if let Some(t) = infer_type(e, schema)? {
                return Ok(Some(t));
            }
        }
        Ok(None)
    };
    Ok(match expr {
        E::Column(name) => column(None, name)?,
        E::Compound(parts) => {
            let name = parts
                .last()
                .ok_or_else(|| RookError::Internal("Empty compound identifier".to_string()))?;
            let table = parts.len().checked_sub(2).map(|i| parts[i].as_str());
            column(table, name)?
        }
        E::Constant(cv) => match cv {
            CV::Null => None,
            CV::Int(i) if i32::try_from(*i).is_ok() => Some(DataType::Int),
            CV::Int(_) => Some(DataType::BigInt),
            CV::Float(_) => Some(DataType::DoublePrecision),
            CV::Text(_) => Some(DataType::Varchar(u16::MAX)),
            CV::Boolean(_) => Some(DataType::Bool),
        },
        E::Binary { left, op, right } => {
            match (infer_type(left, schema)?, infer_type(right, schema)?) {
                // DATE ± integer days is a DATE.
                (Some(DataType::Date), Some(_)) | (Some(_), Some(DataType::Date))
                    if matches!(op, rook_ast::ArithOp::Add | rook_ast::ArithOp::Sub) =>
                {
                    Some(DataType::Date)
                }
                (Some(l), Some(r)) => Some(
                    exact_numeric_result_type(&l, *op, &r)
                        .unwrap_or_else(|| promote_numeric_type(&l, &r)),
                ),
                (known, None) | (None, known) => known,
            }
        }
        E::Cast { data_type, .. } => Some(data_type.parse::<DataType>().map_err(|e| {
            RookError::TypeMismatch(format!("Invalid CAST target type '{}': {}", data_type, e))
        })?),
        E::ScalarSubquery(_) => {
            return Err(RookError::Internal(
                "Scalar subqueries must be materialized before type inference".to_string(),
            ));
        }
        E::Function { name, args, .. } => {
            let mut arg_exprs = args.iter().filter_map(|a| match a {
                FunctionArg::Expr(e) => Some(e.as_ref()),
                FunctionArg::Star => None,
            });
            let upper = name.to_ascii_uppercase();
            let aggregate = match upper.as_str() {
                "COUNT" => Some(AggregateFunction::Count),
                "SUM" => Some(AggregateFunction::Sum),
                "AVG" => Some(AggregateFunction::Avg),
                "MIN" => Some(AggregateFunction::Min),
                "MAX" => Some(AggregateFunction::Max),
                _ => None,
            };
            if let Some(function) = aggregate {
                let input = match arg_exprs.next() {
                    Some(e) => infer_type(e, schema)?,
                    None => None,
                };
                return Ok(Some(infer_aggregate_output_type(function, input.as_ref())));
            }
            match upper.as_str() {
                "UPPER" | "UCASE" | "LOWER" | "LCASE" | "TRIM" | "LTRIM" | "RTRIM"
                | "SUBSTRING" | "SUBSTR" | "||" | "CONCAT" => Some(DataType::Varchar(u16::MAX)),
                "LENGTH" | "LEN" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "POSITION"
                | "CHARINDEX" | "EXTRACT" | "DATE_PART" => Some(DataType::Int),
                "POWER" | "POW" | "SQRT" => Some(DataType::DoublePrecision),
                "CURRENT_DATE" => Some(DataType::Date),
                "CURRENT_TIME" => Some(DataType::Time),
                "CURRENT_TIMESTAMP" | "NOW" => Some(DataType::Timestamp),
                // The result has the type of the (first) argument.
                "ABS" | "FLOOR" | "CEIL" | "CEILING" | "ROUND" | "NULLIF" => match arg_exprs.next()
                {
                    Some(e) => infer_type(e, schema)?,
                    None => None,
                },
                "MOD" | "COALESCE" => first_known(&mut arg_exprs)?,
                _ => {
                    return Err(RookError::Evaluation(format!(
                        "Unknown scalar function: '{}'",
                        name
                    )));
                }
            }
        }
        E::Case {
            when_then_pairs,
            else_result,
        } => first_known(
            &mut when_then_pairs
                .iter()
                .map(|(_, then)| then.as_ref())
                .chain(else_result.as_deref()),
        )?,
        E::Compare { .. } | E::Logical { .. } | E::Not(_) | E::IsNull(_) | E::IsNotNull(_) => {
            Some(DataType::Bool)
        }
    })
}
