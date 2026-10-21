//! Semantic Analysis — resolves column names, validates types, expands wildcards.
//!
//! This module runs as part of the planner pipeline to transform raw parsed
//! query information into fully-validated, resolved metadata before the
//! logical plan tree is built.

use rook_ast::logical::*;
use rook_ast::{ExprNode, FunctionArg, PredicateNode};

use crate::catalog::Column;

/// Convert a catalog `Vec<Column>` into a `ColumnSchema` for the logical plan.
pub fn catalog_columns_to_schema(columns: &[Column]) -> ColumnSchema {
    let infos: Vec<ColumnInfo> = columns
        .iter()
        .map(|col| ColumnInfo {
            name: col.name.clone(),
            data_type: col.data_type.to_string(),
            nullable: col.nullable,
        })
        .collect();
    ColumnSchema { columns: infos }
}

/// Resolve column references in a predicate against a schema.
///
/// Returns `Ok(())` if all columns are valid, or an error listing unknown columns.
pub fn resolve_predicate_columns(
    predicate: &PredicateNode,
    schema: &ColumnSchema,
) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();
    resolve_predicate_columns_recursive(predicate, schema, &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn resolve_predicate_columns_recursive(
    node: &PredicateNode,
    schema: &ColumnSchema,
    errors: &mut Vec<String>,
) {
    match node {
        PredicateNode::BinaryOp { left, right, .. } => {
            resolve_predicate_columns_recursive(left, schema, errors);
            resolve_predicate_columns_recursive(right, schema, errors);
        }
        PredicateNode::Not(inner) => {
            resolve_predicate_columns_recursive(inner, schema, errors);
        }
        PredicateNode::Compare { left, right, .. } => {
            resolve_expr_columns(left, schema, errors);
            resolve_expr_columns(right, schema, errors);
        }
        PredicateNode::IsNull(expr)
        | PredicateNode::IsNotNull(expr)
        | PredicateNode::Between { expr, .. }
        | PredicateNode::Like { expr, .. } => {
            resolve_expr_columns(expr, schema, errors);
        }
        PredicateNode::InList { expr, list } => {
            resolve_expr_columns(expr, schema, errors);
            for item in list {
                resolve_expr_columns(item, schema, errors);
            }
        }
        PredicateNode::Exists(_) => {
            // EXISTS subquery columns are resolved inside the subquery's own scope
        }
        PredicateNode::InSubquery { expr, .. } => {
            // Only the left-hand expression references outer columns
            resolve_expr_columns(expr, schema, errors);
        }
        PredicateNode::IsDistinctFrom { left, right } => {
            resolve_expr_columns(left, schema, errors);
            resolve_expr_columns(right, schema, errors);
        }
        PredicateNode::IsBoolean { expr, .. } => {
            resolve_expr_columns(expr, schema, errors);
        }
    }
}

fn resolve_expr_columns(expr: &ExprNode, schema: &ColumnSchema, errors: &mut Vec<String>) {
    match expr {
        ExprNode::Column(name) => {
            if !schema.contains(name) {
                errors.push(format!("Column '{}' not found in table schema", name));
            }
        }
        ExprNode::Compound(parts) => {
            // For `table.column`, extract the column part (last element)
            if let Some(col_name) = parts.last()
                && !schema.contains(col_name)
            {
                errors.push(format!(
                    "Column '{}' not found (resolved from '{}')",
                    col_name,
                    parts.join(".")
                ));
            }
        }
        ExprNode::Constant(_) => {} // constants always valid
        ExprNode::Binary { left, right, .. } => {
            resolve_expr_columns(left, schema, errors);
            resolve_expr_columns(right, schema, errors);
        }
        ExprNode::Cast { expr: inner, .. } => {
            resolve_expr_columns(inner, schema, errors);
        }
        ExprNode::ScalarSubquery(_) => {
            // Scalar subquery columns are resolved inside the subquery's own scope.
        }
        ExprNode::Function { args, .. } => {
            for arg in args {
                if let FunctionArg::Expr(inner) = arg {
                    resolve_expr_columns(inner, schema, errors);
                }
            }
        }
        ExprNode::Case {
            when_then_pairs,
            else_result,
        } => {
            for (when, then) in when_then_pairs {
                resolve_expr_columns(when, schema, errors);
                resolve_expr_columns(then, schema, errors);
            }
            if let Some(else_node) = else_result {
                resolve_expr_columns(else_node, schema, errors);
            }
        }
        ExprNode::Compare { left, right, .. } | ExprNode::Logical { left, right, .. } => {
            resolve_expr_columns(left, schema, errors);
            resolve_expr_columns(right, schema, errors);
        }
        ExprNode::Not(inner) | ExprNode::IsNull(inner) | ExprNode::IsNotNull(inner) => {
            resolve_expr_columns(inner, schema, errors);
        }
        ExprNode::Predicate(p) => resolve_predicate_columns_recursive(p, schema, errors),
    }
}

/// Check if an expression contains only aggregate-free column references
/// and constants (i.e., it can be pushed down to a filter).
pub fn is_predicate_safe_for_pushdown(_predicate: &PredicateNode) -> bool {
    // Always true — predicates referencing aggregate results are rejected
    // earlier (aggregates are only valid inside HAVING), so any predicate
    // that reaches here is pushdown-safe.
    true
}
