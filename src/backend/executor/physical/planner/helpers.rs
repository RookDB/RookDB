//! Free helper functions for the physical planner.
//!
//! These are standalone utility functions used across multiple planner modules.

use crate::backend::error::{RookError, RookResult};
use crate::types::DataValue;
use crate::types::datatype::DataType;

use crate::planner::types::{TypeEnv, expr_type};

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
        rook_ast::ExprNode::OuterColumn(r) => r.column.clone(),
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
        rook_ast::ExprNode::Predicate(_) => "predicate".to_string(),
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
    infer_expr_type_with(expr, schema, &|_| None)
}

/// [`infer_expr_type_in_schema`] for an expression whose scalar subqueries
/// are already planned: `subquery_type` gives each one's result type.
pub fn infer_expr_type_with(
    expr: &rook_ast::ExprNode,
    schema: &[super::super::tuple::ColumnInfo],
    subquery_type: &dyn Fn(&rook_ast::SubqueryInfo) -> Option<DataType>,
) -> RookResult<DataType> {
    let env = SchemaTypes {
        schema,
        subquery_type,
    };
    Ok(expr_type(expr, &env)?.unwrap_or(DataType::Int))
}

/// Types of an expression's columns read from rows of `schema`.
struct SchemaTypes<'a> {
    schema: &'a [super::super::tuple::ColumnInfo],
    subquery_type: &'a dyn Fn(&rook_ast::SubqueryInfo) -> Option<DataType>,
}

impl TypeEnv for SchemaTypes<'_> {
    fn column(&self, expr: &rook_ast::ExprNode) -> RookResult<Option<DataType>> {
        let (table, name) = match expr {
            rook_ast::ExprNode::Column(name) => (None, name.as_str()),
            rook_ast::ExprNode::Compound(parts) => (
                parts.len().checked_sub(2).map(|i| parts[i].as_str()),
                parts
                    .last()
                    .ok_or_else(|| RookError::Internal("Empty compound identifier".to_string()))?
                    .as_str(),
            ),
            _ => return Ok(None),
        };
        super::super::expr::column_position(self.schema, table, name)
            .map(|i| Some(self.schema[i].data_type.clone()))
            .ok_or_else(|| RookError::NotFound {
                entity: "Column",
                name: match table {
                    Some(t) => format!("{}.{}", t, name),
                    None => name.to_string(),
                },
            })
    }

    fn subquery(&self, info: &rook_ast::SubqueryInfo) -> RookResult<Option<DataType>> {
        Ok((self.subquery_type)(info))
    }

    fn strict(&self) -> bool {
        false
    }
}
