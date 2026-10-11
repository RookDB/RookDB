//! Free helper functions for the physical planner.
//!
//! These are standalone utility functions used across multiple planner modules.

use crate::backend::error::{RookError, RookResult};
use crate::types::DataValue;
use crate::types::datatype::DataType;

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
        rook_ast::ExprNode::Constant(cv) => format!("{:?}", cv),
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

/// Infer the DataType of an AST expression given child schema types and column names.
pub fn infer_expr_type_from_ast(
    expr: &rook_ast::ExprNode,
    child_types: &[crate::types::datatype::DataType],
    column_names: &[String],
) -> RookResult<crate::types::datatype::DataType> {
    match expr {
        rook_ast::ExprNode::Column(name) => {
            let idx = column_names
                .iter()
                .position(|c| c.eq_ignore_ascii_case(name))
                .ok_or_else(|| RookError::NotFound {
                    entity: "Column",
                    name: name.clone(),
                })?;
            Ok(child_types[idx].clone())
        }
        rook_ast::ExprNode::Compound(parts) => {
            let name = parts
                .last()
                .ok_or_else(|| RookError::Internal("Empty compound identifier".to_string()))?;
            let idx = column_names
                .iter()
                .position(|c| c.eq_ignore_ascii_case(name))
                .ok_or_else(|| RookError::NotFound {
                    entity: "Column",
                    name: name.clone(),
                })?;
            Ok(child_types[idx].clone())
        }
        rook_ast::ExprNode::Constant(cv) => match cv {
            rook_ast::ConstantValue::Null => Err(RookError::Internal(
                "Cannot infer type for NULL literal".to_string(),
            )),
            rook_ast::ConstantValue::Int(_) => Ok(crate::types::datatype::DataType::Int),
            rook_ast::ConstantValue::Float(_) => {
                Ok(crate::types::datatype::DataType::DoublePrecision)
            }
            rook_ast::ConstantValue::Text(_) => {
                Ok(crate::types::datatype::DataType::Varchar(u16::MAX))
            }
            rook_ast::ConstantValue::Boolean(_) => Ok(crate::types::datatype::DataType::Bool),
        },
        rook_ast::ExprNode::Binary { left, right, .. } => {
            // Infer the result type by looking at child expression types and
            // applying SQL type promotion (e.g., INT + DOUBLE → DOUBLE).
            let left_type = infer_expr_type_from_ast(left, child_types, column_names)?;
            let right_type = infer_expr_type_from_ast(right, child_types, column_names)?;
            Ok(promote_numeric_type(&left_type, &right_type))
        }
        rook_ast::ExprNode::Cast { data_type, .. } => data_type
            .parse::<crate::types::datatype::DataType>()
            .map_err(|e| {
                RookError::TypeMismatch(format!("Invalid CAST target type '{}': {}", data_type, e))
            }),
        // Scalar subqueries are materialized before this function is called,
        // so any remaining ScalarSubquery node would have been replaced with
        // a Constant. This is a fallback for the unimplemented case.
        rook_ast::ExprNode::ScalarSubquery(_) => Err(RookError::Internal(
            "Scalar subqueries must be materialized before type inference".to_string(),
        )),
        // Scalar / aggregate function — infer result type from the function name
        rook_ast::ExprNode::Function { name, args, .. } => {
            let upper = name.to_ascii_uppercase();
            match upper.as_str() {
                // Aggregate functions
                "COUNT" => Ok(crate::types::datatype::DataType::BigInt),
                "SUM" | "AVG" => Ok(crate::types::datatype::DataType::DoublePrecision),
                "MIN" | "MAX" => {
                    // Infer from argument type if possible
                    if let Some(rook_ast::FunctionArg::Expr(expr)) = args.first() {
                        infer_expr_type_from_ast(expr, child_types, column_names)
                    } else {
                        Ok(crate::types::datatype::DataType::Int)
                    }
                }
                // String functions return VARCHAR
                "UPPER" | "UCASE" | "LOWER" | "LCASE" | "TRIM" | "LTRIM" | "RTRIM"
                | "SUBSTRING" | "SUBSTR" => Ok(crate::types::datatype::DataType::Varchar(u16::MAX)),
                // String length returns INT
                "LENGTH" | "LEN" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "POSITION"
                | "CHARINDEX" => Ok(crate::types::datatype::DataType::Int),
                // Numeric functions
                "ABS" | "FLOOR" | "CEIL" | "CEILING" | "ROUND" => {
                    if let Some(rook_ast::FunctionArg::Expr(expr)) = args.first() {
                        infer_expr_type_from_ast(expr, child_types, column_names)
                    } else {
                        Ok(crate::types::datatype::DataType::Int)
                    }
                }
                // EXTRACT returns INT
                "EXTRACT" | "DATE_PART" => Ok(crate::types::datatype::DataType::Int),
                // COALESCE — use type of first non-NULL argument
                "COALESCE" => {
                    if let Some(rook_ast::FunctionArg::Expr(expr)) = args.first() {
                        infer_expr_type_from_ast(expr, child_types, column_names)
                    } else {
                        Ok(crate::types::datatype::DataType::Int)
                    }
                }
                // NULLIF — use type of first argument
                "NULLIF" => {
                    if let Some(rook_ast::FunctionArg::Expr(expr)) = args.first() {
                        infer_expr_type_from_ast(expr, child_types, column_names)
                    } else {
                        Ok(crate::types::datatype::DataType::Int)
                    }
                }
                // CURRENT_* return VARCHAR or TIMESTAMP
                "CURRENT_DATE" | "CURRENT_TIME" | "CURRENT_TIMESTAMP" | "NOW" => {
                    Ok(crate::types::datatype::DataType::Varchar(u16::MAX))
                }
                _ => Ok(crate::types::datatype::DataType::Int),
            }
        }
        rook_ast::ExprNode::Case {
            when_then_pairs,
            else_result,
        } => {
            // For CASE expressions, infer from first THEN branch (or ELSE branch)
            if let Some((_, then_expr)) = when_then_pairs.first() {
                infer_expr_type_from_ast(then_expr, child_types, column_names)
            } else if let Some(else_expr) = else_result {
                infer_expr_type_from_ast(else_expr, child_types, column_names)
            } else {
                Ok(crate::types::datatype::DataType::Int)
            }
        }
        rook_ast::ExprNode::Compare { .. }
        | rook_ast::ExprNode::Logical { .. }
        | rook_ast::ExprNode::Not(_)
        | rook_ast::ExprNode::IsNull(_)
        | rook_ast::ExprNode::IsNotNull(_) => Ok(crate::types::datatype::DataType::Bool),
    }
}
