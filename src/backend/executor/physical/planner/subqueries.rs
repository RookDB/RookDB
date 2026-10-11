//! Subquery planning for the PhysicalPlanner.
//!
//! Handles correlated EXISTS/IN subqueries, scalar subquery materialization,
//! and predicate building with subquery support.

use std::cell::RefCell;
use std::rc::Rc;

use super::super::engine::execute_shared;
use super::super::expr::{ComparisonOp, Expr, Predicate, expr_from_ast, predicate_from_ast};
use super::super::operators::{
    FilterOperator, LimitOperator, PhysicalOperator, ProjectionOperator,
};
use super::super::tuple::ColumnInfo;
use super::PhysicalPlanner;
use crate::backend::error::{RookError, RookResult};
use crate::types::DataValue;
use crate::types::datatype::DataType;

impl PhysicalPlanner {
    /// Recursively convert a `PredicateNode` to a physical `Predicate`,
    /// materializing any subqueries (EXISTS, IN subquery) at plan time.
    pub(crate) fn build_predicate_with_subqueries(
        &self,
        node: &rook_ast::PredicateNode,
        column_names: &[String],
        schema: &[ColumnInfo],
    ) -> RookResult<Predicate> {
        let materialized_node = self.materialize_subqueries_in_predicate(node)?;
        self.build_predicate_with_subqueries_inner(&materialized_node, column_names, schema)
    }

    fn build_predicate_with_subqueries_inner(
        &self,
        node: &rook_ast::PredicateNode,
        column_names: &[String],
        schema: &[ColumnInfo],
    ) -> RookResult<Predicate> {
        match node {
            rook_ast::PredicateNode::BinaryOp { left, op, right } => {
                let l = self.build_predicate_with_subqueries_inner(left, column_names, schema)?;
                let r = self.build_predicate_with_subqueries_inner(right, column_names, schema)?;
                match op {
                    rook_ast::BinaryOp::And => Ok(Predicate::and(l, r)),
                    rook_ast::BinaryOp::Or => Ok(Predicate::or(l, r)),
                }
            }
            rook_ast::PredicateNode::Not(inner) => {
                let inner =
                    self.build_predicate_with_subqueries_inner(inner, column_names, schema)?;
                Ok(Predicate::not(inner))
            }
            rook_ast::PredicateNode::Exists(subquery_info) => {
                if self.is_subquery_correlated(&subquery_info.select) {
                    self.note(|| "Correlated EXISTS subquery: evaluated per outer row".to_string());
                    self.build_correlated_exists(subquery_info, schema)
                } else {
                    let exists = self.materialize_exists_subquery(subquery_info.select.as_ref())?;
                    self.note(|| {
                        format!("EXISTS subquery evaluated once while planning: {}", exists)
                    });
                    Ok(Predicate::ExistsResult(exists))
                }
            }
            rook_ast::PredicateNode::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                if self.is_subquery_correlated(&subquery.select) {
                    self.note(|| "Correlated IN subquery: evaluated per outer row".to_string());
                    let lhs_expr = expr_from_ast(expr, column_names)?;
                    self.build_correlated_in_subquery(subquery, lhs_expr, *negated, schema)
                } else {
                    let lhs_expr = expr_from_ast(expr, column_names)?;
                    let values = self.materialize_in_subquery(subquery.select.as_ref())?;
                    Ok(Predicate::InSubqueryResult(lhs_expr, values, *negated))
                }
            }
            // Compare nodes — handle correlated scalar subqueries if present
            rook_ast::PredicateNode::Compare { left, op, right } => {
                let l = self.build_expr_with_correlated_subquery(left, column_names, schema)?;
                let r = self.build_expr_with_correlated_subquery(right, column_names, schema)?;
                let phys_op = match op {
                    rook_ast::ComparisonOp::Eq => ComparisonOp::Equals,
                    rook_ast::ComparisonOp::Ne => ComparisonOp::NotEquals,
                    rook_ast::ComparisonOp::Lt => ComparisonOp::LessThan,
                    rook_ast::ComparisonOp::Le => ComparisonOp::LessOrEqual,
                    rook_ast::ComparisonOp::Gt => ComparisonOp::GreaterThan,
                    rook_ast::ComparisonOp::Ge => ComparisonOp::GreaterOrEqual,
                };
                Ok(Predicate::Compare(l, phys_op, r))
            }
            // IS DISTINCT FROM and IS Boolean — handle directly
            rook_ast::PredicateNode::IsDistinctFrom { left, right } => {
                let l = expr_from_ast(left, column_names)?;
                let r = expr_from_ast(right, column_names)?;
                Ok(Predicate::IsDistinctFrom(l, r))
            }
            rook_ast::PredicateNode::IsBoolean {
                expr,
                test,
                negated,
            } => {
                let e = expr_from_ast(expr, column_names)?;
                let bt = match test {
                    rook_ast::BooleanTest::True => super::super::expr::BooleanTest::True,
                    rook_ast::BooleanTest::False => super::super::expr::BooleanTest::False,
                    rook_ast::BooleanTest::Unknown => super::super::expr::BooleanTest::Unknown,
                };
                Ok(Predicate::IsBoolean {
                    expr: e,
                    test: bt,
                    negated: *negated,
                })
            }
            // All standard predicate nodes delegate to predicate_from_ast
            _ => {
                // Use the standard converter
                predicate_from_ast(node, column_names)
            }
        }
    }

    pub(crate) fn build_expr_with_correlated_subquery(
        &self,
        expr: &rook_ast::ExprNode,
        column_names: &[String],
        schema: &[ColumnInfo],
    ) -> RookResult<Expr> {
        match expr {
            rook_ast::ExprNode::ScalarSubquery(info) => {
                if self.is_subquery_correlated(&info.select) {
                    let (phys_expr, _) = self.build_correlated_scalar_subquery(info, schema)?;
                    Ok(phys_expr)
                } else {
                    let (value, _) = self.materialize_scalar_subquery(&info.select)?;
                    let constant = match value {
                        Some(dv) => Expr::Constant(dv),
                        None => Expr::Null,
                    };
                    Ok(constant)
                }
            }
            rook_ast::ExprNode::Binary { left, op, right } => {
                let l = self.build_expr_with_correlated_subquery(left, column_names, schema)?;
                let r = self.build_expr_with_correlated_subquery(right, column_names, schema)?;
                let phys_op = match op {
                    rook_ast::ArithOp::Add => Expr::Add(Box::new(l), Box::new(r)),
                    rook_ast::ArithOp::Sub => Expr::Sub(Box::new(l), Box::new(r)),
                    rook_ast::ArithOp::Mul => Expr::Mul(Box::new(l), Box::new(r)),
                    rook_ast::ArithOp::Div => Expr::Div(Box::new(l), Box::new(r)),
                };
                Ok(phys_op)
            }
            rook_ast::ExprNode::Cast {
                expr: inner,
                data_type,
            } => {
                let inner_expr =
                    self.build_expr_with_correlated_subquery(inner, column_names, schema)?;
                let dt: DataType = data_type
                    .parse()
                    .map_err(|e: String| RookError::TypeMismatch(e))?;
                Ok(Expr::Cast(Box::new(inner_expr), dt))
            }
            _ => expr_from_ast(expr, column_names),
        }
    }

    /// Plan and execute an EXISTS subquery, returning whether any rows exist.
    pub(crate) fn materialize_exists_subquery(
        &self,
        select: &rook_ast::SelectPlan,
    ) -> RookResult<bool> {
        // Build QueryPlan around the SelectPlan
        let query_plan = rook_ast::QueryPlan::Select(select.clone());

        // Plan the inner query into a LogicalPlan
        let logical_plan = crate::planner::plan_query(&query_plan, &self.catalog, &self.db_name)
            .map_err(|e| {
                RookError::Internal(e.message).with_context("Failed to plan EXISTS subquery")
            })?;

        // Execute the plan and check if any tuples are produced
        let tuples = execute_shared(
            &logical_plan,
            std::sync::Arc::clone(&self.catalog),
            &self.db_name,
        )?
        .0;
        Ok(!tuples.is_empty())
    }

    /// Plan and execute an IN subquery, returning the list of values from the
    /// first column of the subquery result.
    pub(crate) fn materialize_in_subquery(
        &self,
        select: &rook_ast::SelectPlan,
    ) -> RookResult<Vec<Option<DataValue>>> {
        let query_plan = rook_ast::QueryPlan::Select(select.clone());

        let logical_plan = crate::planner::plan_query(&query_plan, &self.catalog, &self.db_name)
            .map_err(|e| {
                RookError::Internal(e.message).with_context("Failed to plan IN subquery")
            })?;

        // Execute and collect tuples
        let tuples = execute_shared(
            &logical_plan,
            std::sync::Arc::clone(&self.catalog),
            &self.db_name,
        )?
        .0;

        // Extract the first column value from each tuple
        let values: Vec<Option<DataValue>> = tuples
            .into_iter()
            .map(|t| t.values.into_iter().next().unwrap_or(None))
            .collect();

        self.note(|| {
            format!(
                "IN subquery evaluated once while planning: {} value(s)",
                values.len()
            )
        });

        Ok(values)
    }

    /// Materialize a single scalar subquery and return (value, data_type).
    pub(crate) fn materialize_scalar_subquery(
        &self,
        select: &rook_ast::SelectPlan,
    ) -> RookResult<(Option<DataValue>, DataType)> {
        self.note(|| "Scalar subquery evaluated once while planning".to_string());
        let query_plan = rook_ast::QueryPlan::Select(select.clone());

        let logical_plan = crate::planner::plan_query(&query_plan, &self.catalog, &self.db_name)
            .map_err(|e| {
                RookError::Internal(e.message).with_context("Failed to plan scalar subquery")
            })?;

        let tuples = execute_shared(
            &logical_plan,
            std::sync::Arc::clone(&self.catalog),
            &self.db_name,
        )?
        .0;

        match tuples.len() {
            0 => Ok((None, DataType::Int)), // NULL → default to Int
            1 => {
                let mut tuple = tuples.into_iter().next().unwrap();
                let value = tuple.values.drain(..).next().flatten();
                // Infer the data type from the value (or default to Int)
                let data_type = value
                    .as_ref()
                    .map(|v| v.data_type())
                    .unwrap_or(DataType::Int);
                Ok((value, data_type))
            }
            n => Err(RookError::Internal(format!(
                "Scalar subquery returned more than one row (got {})",
                n
            ))),
        }
    }

    /// Recursively walk an `ExprNode` tree and replace any `ScalarSubquery`
    /// nodes with `Constant` nodes. Used for nested subqueries inside
    /// `Binary`, `Cast`, etc.
    pub(crate) fn materialize_nested_subqueries(
        &self,
        expr: &rook_ast::ExprNode,
    ) -> RookResult<rook_ast::ExprNode> {
        match expr {
            rook_ast::ExprNode::ScalarSubquery(info) => {
                if self.is_subquery_correlated(&info.select) {
                    Ok(expr.clone())
                } else {
                    let (value, _) = self.materialize_scalar_subquery(&info.select)?;
                    let constant = match value {
                        Some(dv) => super::helpers::data_value_to_constant_value(&dv),
                        None => rook_ast::ConstantValue::Null,
                    };
                    Ok(rook_ast::ExprNode::Constant(constant))
                }
            }
            rook_ast::ExprNode::Binary { left, op, right } => {
                let left = self.materialize_nested_subqueries(left)?;
                let right = self.materialize_nested_subqueries(right)?;
                Ok(rook_ast::ExprNode::Binary {
                    left: Box::new(left),
                    op: *op,
                    right: Box::new(right),
                })
            }
            rook_ast::ExprNode::Cast {
                expr: inner,
                data_type,
            } => {
                let inner = self.materialize_nested_subqueries(inner)?;
                Ok(rook_ast::ExprNode::Cast {
                    expr: Box::new(inner),
                    data_type: data_type.clone(),
                })
            }
            rook_ast::ExprNode::Function {
                name,
                args,
                distinct,
            } => {
                let mut new_args = Vec::new();
                for arg in args {
                    match arg {
                        rook_ast::FunctionArg::Expr(e) => {
                            let materialized = self.materialize_nested_subqueries(e)?;
                            new_args.push(rook_ast::FunctionArg::Expr(Box::new(materialized)));
                        }
                        rook_ast::FunctionArg::Star => new_args.push(rook_ast::FunctionArg::Star),
                    }
                }
                Ok(rook_ast::ExprNode::Function {
                    name: name.clone(),
                    args: new_args,
                    distinct: *distinct,
                })
            }
            rook_ast::ExprNode::Case {
                when_then_pairs,
                else_result,
            } => {
                let mut new_pairs = Vec::new();
                for (when, then) in when_then_pairs {
                    new_pairs.push((
                        Box::new(self.materialize_nested_subqueries(when)?),
                        Box::new(self.materialize_nested_subqueries(then)?),
                    ));
                }
                let new_else = match else_result {
                    Some(e) => Some(Box::new(self.materialize_nested_subqueries(e)?)),
                    None => None,
                };
                Ok(rook_ast::ExprNode::Case {
                    when_then_pairs: new_pairs,
                    else_result: new_else,
                })
            }
            rook_ast::ExprNode::Compare { left, op, right } => {
                let left = self.materialize_nested_subqueries(left)?;
                let right = self.materialize_nested_subqueries(right)?;
                Ok(rook_ast::ExprNode::Compare {
                    left: Box::new(left),
                    op: *op,
                    right: Box::new(right),
                })
            }
            rook_ast::ExprNode::Logical { left, op, right } => {
                let left = self.materialize_nested_subqueries(left)?;
                let right = self.materialize_nested_subqueries(right)?;
                Ok(rook_ast::ExprNode::Logical {
                    left: Box::new(left),
                    op: *op,
                    right: Box::new(right),
                })
            }
            rook_ast::ExprNode::Not(inner) => {
                let inner = self.materialize_nested_subqueries(inner)?;
                Ok(rook_ast::ExprNode::Not(Box::new(inner)))
            }
            rook_ast::ExprNode::IsNull(inner) => {
                let inner = self.materialize_nested_subqueries(inner)?;
                Ok(rook_ast::ExprNode::IsNull(Box::new(inner)))
            }
            rook_ast::ExprNode::IsNotNull(inner) => {
                let inner = self.materialize_nested_subqueries(inner)?;
                Ok(rook_ast::ExprNode::IsNotNull(Box::new(inner)))
            }
            // Leaf nodes: Column, Compound, Constant — no subqueries inside
            _ => Ok(expr.clone()),
        }
    }

    /// Recursively walk a `PredicateNode` tree and replace any `ScalarSubquery`
    /// nodes within its expressions with `Constant` nodes.
    pub(crate) fn materialize_subqueries_in_predicate(
        &self,
        node: &rook_ast::PredicateNode,
    ) -> RookResult<rook_ast::PredicateNode> {
        match node {
            rook_ast::PredicateNode::BinaryOp { left, op, right } => {
                let left = self.materialize_subqueries_in_predicate(left)?;
                let right = self.materialize_subqueries_in_predicate(right)?;
                Ok(rook_ast::PredicateNode::BinaryOp {
                    left: Box::new(left),
                    op: *op,
                    right: Box::new(right),
                })
            }
            rook_ast::PredicateNode::Not(inner) => {
                let inner = self.materialize_subqueries_in_predicate(inner)?;
                Ok(rook_ast::PredicateNode::Not(Box::new(inner)))
            }
            rook_ast::PredicateNode::Compare { left, op, right } => {
                let left = self.materialize_nested_subqueries(left)?;
                let right = self.materialize_nested_subqueries(right)?;
                Ok(rook_ast::PredicateNode::Compare {
                    left: Box::new(left),
                    op: *op,
                    right: Box::new(right),
                })
            }
            rook_ast::PredicateNode::IsNull(expr) => {
                let expr = self.materialize_nested_subqueries(expr)?;
                Ok(rook_ast::PredicateNode::IsNull(Box::new(expr)))
            }
            rook_ast::PredicateNode::IsNotNull(expr) => {
                let expr = self.materialize_nested_subqueries(expr)?;
                Ok(rook_ast::PredicateNode::IsNotNull(Box::new(expr)))
            }
            rook_ast::PredicateNode::Between { expr, low, high } => {
                let expr = self.materialize_nested_subqueries(expr)?;
                let low = self.materialize_nested_subqueries(low)?;
                let high = self.materialize_nested_subqueries(high)?;
                Ok(rook_ast::PredicateNode::Between {
                    expr: Box::new(expr),
                    low: Box::new(low),
                    high: Box::new(high),
                })
            }
            rook_ast::PredicateNode::InList { expr, list } => {
                let expr = self.materialize_nested_subqueries(expr)?;
                let mut new_list = Vec::new();
                for item in list {
                    new_list.push(self.materialize_nested_subqueries(item)?);
                }
                Ok(rook_ast::PredicateNode::InList {
                    expr: Box::new(expr),
                    list: new_list,
                })
            }
            rook_ast::PredicateNode::Like {
                expr,
                pattern,
                escape_char,
            } => {
                let expr = self.materialize_nested_subqueries(expr)?;
                Ok(rook_ast::PredicateNode::Like {
                    expr: Box::new(expr),
                    pattern: pattern.clone(),
                    escape_char: *escape_char,
                })
            }
            rook_ast::PredicateNode::Exists(_) => Ok(node.clone()),
            rook_ast::PredicateNode::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let expr = self.materialize_nested_subqueries(expr)?;
                Ok(rook_ast::PredicateNode::InSubquery {
                    expr: Box::new(expr),
                    subquery: subquery.clone(),
                    negated: *negated,
                })
            }
            rook_ast::PredicateNode::IsDistinctFrom { left, right } => {
                let left = self.materialize_nested_subqueries(left)?;
                let right = self.materialize_nested_subqueries(right)?;
                Ok(rook_ast::PredicateNode::IsDistinctFrom {
                    left: Box::new(left),
                    right: Box::new(right),
                })
            }
            rook_ast::PredicateNode::IsBoolean {
                expr,
                test,
                negated,
            } => {
                let expr = self.materialize_nested_subqueries(expr)?;
                Ok(rook_ast::PredicateNode::IsBoolean {
                    expr: Box::new(expr),
                    test: *test,
                    negated: *negated,
                })
            }
        }
    }
}

// ── Correlated subquery detection and building ────────────────────────────────

impl PhysicalPlanner {
    /// Check if a predicate (from an inner query's SELECT) references columns
    /// belonging to an **outer** table (i.e., columns that are NOT in any of
    /// the inner FROM tables). If so, the subquery is correlated.
    pub(crate) fn is_predicate_correlated(
        &self,
        pred_node: Option<&rook_ast::PredicateNode>,
        inner_table_names: &[String],
    ) -> bool {
        let Some(node) = pred_node else {
            return false;
        };
        self.pred_contains_outer_ref(node, inner_table_names)
    }

    /// Recursively walk a predicate tree looking for any column reference that
    /// does not belong to the inner query's tables.
    fn pred_contains_outer_ref(
        &self,
        node: &rook_ast::PredicateNode,
        inner_tables: &[String],
    ) -> bool {
        match node {
            rook_ast::PredicateNode::BinaryOp { left, right, .. } => {
                self.pred_contains_outer_ref(left, inner_tables)
                    || self.pred_contains_outer_ref(right, inner_tables)
            }
            rook_ast::PredicateNode::Not(inner) => {
                self.pred_contains_outer_ref(inner, inner_tables)
            }
            rook_ast::PredicateNode::Compare { left, right, .. } => {
                self.expr_is_outer_ref(left, inner_tables)
                    || self.expr_is_outer_ref(right, inner_tables)
            }
            rook_ast::PredicateNode::IsNull(expr) | rook_ast::PredicateNode::IsNotNull(expr) => {
                self.expr_is_outer_ref(expr, inner_tables)
            }
            rook_ast::PredicateNode::Between { expr, low, high } => {
                self.expr_is_outer_ref(expr, inner_tables)
                    || self.expr_is_outer_ref(low, inner_tables)
                    || self.expr_is_outer_ref(high, inner_tables)
            }
            rook_ast::PredicateNode::InList { expr, list } => {
                self.expr_is_outer_ref(expr, inner_tables)
                    || list.iter().any(|e| self.expr_is_outer_ref(e, inner_tables))
            }
            rook_ast::PredicateNode::Like { expr, .. } => {
                self.expr_is_outer_ref(expr, inner_tables)
            }
            rook_ast::PredicateNode::IsDistinctFrom { left, right } => {
                self.expr_is_outer_ref(left, inner_tables)
                    || self.expr_is_outer_ref(right, inner_tables)
            }
            rook_ast::PredicateNode::IsBoolean { expr, .. } => {
                self.expr_is_outer_ref(expr, inner_tables)
            }
            // EXISTS and IN-subquery inside a predicate are not expected here
            rook_ast::PredicateNode::Exists(_) | rook_ast::PredicateNode::InSubquery { .. } => {
                false
            }
        }
    }

    /// Check whether an expression node references an outer table column.
    ///
    /// A `Compound([table, col])` is an outer ref if `table` does not match any
    /// of the inner table names. A simple `Column(name)` is assumed to be an
    /// inner ref unless proven otherwise (conservative heuristic).
    fn expr_is_outer_ref(&self, expr: &rook_ast::ExprNode, inner_tables: &[String]) -> bool {
        match expr {
            rook_ast::ExprNode::Compound(parts) => {
                if let Some(table_part) = parts.first() {
                    !inner_tables
                        .iter()
                        .any(|t| t.eq_ignore_ascii_case(table_part))
                } else {
                    false
                }
            }
            // Simple Column refs: if the column name exists in an inner table
            // schema, it's an inner ref. Otherwise conservatively assume it's
            // an outer ref — this handles cases like `x = outer.col` where `x`
            // is an unqualified inner column and `outer.col` is compound.
            rook_ast::ExprNode::Column(name) => {
                // Check if it's a column in any inner table
                !self.column_exists_in_any_inner_table(name, inner_tables)
            }
            rook_ast::ExprNode::Constant(_) | rook_ast::ExprNode::ScalarSubquery(_) => false,
            rook_ast::ExprNode::Function { .. } => false,
            rook_ast::ExprNode::Binary { left, right, .. } => {
                self.expr_is_outer_ref(left, inner_tables)
                    || self.expr_is_outer_ref(right, inner_tables)
            }
            rook_ast::ExprNode::Cast { expr: inner, .. } => {
                self.expr_is_outer_ref(inner, inner_tables)
            }
            rook_ast::ExprNode::Compare { left, right, .. }
            | rook_ast::ExprNode::Logical { left, right, .. } => {
                self.expr_is_outer_ref(left, inner_tables)
                    || self.expr_is_outer_ref(right, inner_tables)
            }
            rook_ast::ExprNode::Not(inner)
            | rook_ast::ExprNode::IsNull(inner)
            | rook_ast::ExprNode::IsNotNull(inner) => self.expr_is_outer_ref(inner, inner_tables),
            rook_ast::ExprNode::Case {
                when_then_pairs,
                else_result,
            } => {
                for (when, then) in when_then_pairs {
                    if self.expr_is_outer_ref(when, inner_tables)
                        || self.expr_is_outer_ref(then, inner_tables)
                    {
                        return true;
                    }
                }
                if let Some(else_node) = else_result {
                    self.expr_is_outer_ref(else_node, inner_tables)
                } else {
                    false
                }
            }
        }
    }

    /// Check whether a bare column name exists in any of the inner tables'
    /// catalog schemas.
    fn column_exists_in_any_inner_table(&self, col_name: &str, inner_tables: &[String]) -> bool {
        let db = match self.catalog.databases.get(&self.db_name) {
            Some(db) => db,
            None => return false,
        };
        for table_name in inner_tables {
            if let Some(table) = db.tables.get(table_name)
                && table
                    .columns
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(col_name))
            {
                return true;
            }
        }
        false
    }

    pub(crate) fn is_subquery_correlated(&self, select: &rook_ast::SelectPlan) -> bool {
        let mut inner_tables: Vec<String> = Vec::new();
        for t in &select.from {
            if let Some(ref alias) = t.alias {
                inner_tables.push(alias.clone());
            } else {
                let raw_name = t
                    .name
                    .strip_prefix("__cte__:")
                    .unwrap_or(&t.name)
                    .to_string();
                inner_tables.push(raw_name);
                inner_tables.push(t.name.clone());
            }
        }
        self.is_predicate_correlated(select.selection.as_ref(), &inner_tables)
    }

    fn resolve_inner_scan_and_columns(
        &self,
        select: &rook_ast::SelectPlan,
    ) -> RookResult<(Box<dyn PhysicalOperator>, String, Vec<String>)> {
        let inner_tref = select
            .from
            .first()
            .ok_or_else(|| RookError::Internal("Subquery has no FROM table".to_string()))?;

        let raw_name = inner_tref
            .name
            .strip_prefix("__cte__:")
            .unwrap_or(&inner_tref.name);
        let inner_table_alias = inner_tref
            .alias
            .clone()
            .unwrap_or_else(|| raw_name.to_string());

        if let Some(cte_def) = select
            .ctes
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(raw_name))
        {
            let mut query = (*cte_def.query).clone();
            if query.ctes.is_empty() {
                query.ctes = select.ctes.clone();
            }
            let query_plan = rook_ast::QueryPlan::Select(query);
            let logical_plan =
                crate::planner::plan_query(&query_plan, &self.catalog, &self.db_name).map_err(
                    |e| {
                        RookError::Internal(e.message)
                            .with_context("Failed to plan CTE inner query in correlated subquery")
                    },
                )?;
            let inner_planner =
                PhysicalPlanner::new(std::sync::Arc::clone(&self.catalog), self.db_name.clone());
            let inner_base = inner_planner.plan(&logical_plan)?;
            let inner_col_names: Vec<String> =
                inner_base.schema().iter().map(|c| c.name.clone()).collect();
            Ok((inner_base, inner_table_alias, inner_col_names))
        } else {
            let inner_table_name = inner_tref.name.clone();
            let inner_table_schema = self.resolve_table_schema(&inner_table_name)?;
            let inner_col_names: Vec<String> =
                inner_table_schema.iter().map(|c| c.name.clone()).collect();
            let inner_table_scan = rook_ast::logical::LogicalTableScan {
                table: inner_table_name,
                alias: inner_tref.alias.clone(),
                schema: rook_ast::logical::ColumnSchema {
                    columns: inner_col_names
                        .iter()
                        .map(|name| rook_ast::logical::ColumnInfo {
                            name: name.clone(),
                            data_type: "UNKNOWN".to_string(),
                            nullable: true,
                        })
                        .collect(),
                },
                system_table_name: None,
            };
            let inner_base = self.plan_table_scan(&inner_table_scan)?;
            Ok((inner_base, inner_table_alias, inner_col_names))
        }
    }

    /// Build a correlated EXISTS subquery with per-row execution.
    fn build_correlated_exists(
        &self,
        subquery_info: &rook_ast::SubqueryInfo,
        outer_schema: &[ColumnInfo],
    ) -> RookResult<Predicate> {
        let select = &subquery_info.select;

        let (inner_base, inner_table_alias, inner_col_names) =
            self.resolve_inner_scan_and_columns(select)?;

        let correlation_pairs = if let Some(ref selection) = select.selection {
            self.extract_all_correlations(selection, &inner_table_alias, &inner_col_names)?
        } else {
            return Err(RookError::Internal(
                "Correlated EXISTS subquery has no WHERE clause".to_string(),
            ));
        };

        if correlation_pairs.is_empty() {
            return Err(RookError::Internal(
                "No correlation found in EXISTS subquery".to_string(),
            ));
        }

        let mut params = Vec::new();
        let mut outer_indices = Vec::new();
        let mut filter_predicate: Option<Predicate> = None;

        for (inner_col_name, outer_col_name, comp_op) in &correlation_pairs {
            let outer_col_idx = outer_schema
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(outer_col_name))
                .ok_or_else(|| RookError::NotFound {
                    entity: "Column",
                    name: outer_col_name.clone(),
                })?;

            let param = Rc::new(RefCell::new(None));
            let phys_comparison = match comp_op {
                rook_ast::ComparisonOp::Eq => ComparisonOp::Equals,
                rook_ast::ComparisonOp::Ne => ComparisonOp::NotEquals,
                rook_ast::ComparisonOp::Lt => ComparisonOp::LessThan,
                rook_ast::ComparisonOp::Le => ComparisonOp::LessOrEqual,
                rook_ast::ComparisonOp::Gt => ComparisonOp::GreaterThan,
                rook_ast::ComparisonOp::Ge => ComparisonOp::GreaterOrEqual,
            };

            let col_pred = Predicate::Compare(
                Expr::Column {
                    table: None,
                    column: inner_col_name.clone(),
                },
                phys_comparison,
                Expr::CorrelatedParam(param.clone()),
            );

            filter_predicate = Some(match filter_predicate {
                None => col_pred,
                Some(acc) => Predicate::and(acc, col_pred),
            });

            params.push(param);
            outer_indices.push(outer_col_idx);
        }

        let filter_op: Box<dyn PhysicalOperator> =
            Box::new(FilterOperator::new(inner_base, filter_predicate.unwrap()));
        let inner_plan_rc = Rc::new(RefCell::new(filter_op));

        Ok(Predicate::CorrelatedExists {
            inner_plan: inner_plan_rc,
            params,
            outer_col_indices: outer_indices,
        })
    }

    /// Build a correlated IN (subquery) with per-row execution.
    fn build_correlated_in_subquery(
        &self,
        subquery: &rook_ast::SubqueryInfo,
        lhs_expr: Expr,
        negated: bool,
        outer_schema: &[ColumnInfo],
    ) -> RookResult<Predicate> {
        let select = &subquery.select;

        let (inner_base, inner_table_alias, inner_col_names) =
            self.resolve_inner_scan_and_columns(select)?;

        let correlation_pairs = if let Some(ref selection) = select.selection {
            self.extract_all_correlations(selection, &inner_table_alias, &inner_col_names)?
        } else {
            return Err(RookError::Internal(
                "Correlated IN subquery has no WHERE clause".to_string(),
            ));
        };

        if correlation_pairs.is_empty() {
            return Err(RookError::Internal(
                "No correlation found in IN subquery".to_string(),
            ));
        }

        let mut params = Vec::new();
        let mut outer_indices = Vec::new();
        let mut filter_predicate: Option<Predicate> = None;

        for (inner_col_name, outer_col_name, comp_op) in &correlation_pairs {
            let outer_col_idx = outer_schema
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(outer_col_name))
                .ok_or_else(|| RookError::NotFound {
                    entity: "Column",
                    name: outer_col_name.clone(),
                })?;

            let param = Rc::new(RefCell::new(None));
            let phys_comparison = match comp_op {
                rook_ast::ComparisonOp::Eq => ComparisonOp::Equals,
                rook_ast::ComparisonOp::Ne => ComparisonOp::NotEquals,
                rook_ast::ComparisonOp::Lt => ComparisonOp::LessThan,
                rook_ast::ComparisonOp::Le => ComparisonOp::LessOrEqual,
                rook_ast::ComparisonOp::Gt => ComparisonOp::GreaterThan,
                rook_ast::ComparisonOp::Ge => ComparisonOp::GreaterOrEqual,
            };

            let col_pred = Predicate::Compare(
                Expr::Column {
                    table: None,
                    column: inner_col_name.clone(),
                },
                phys_comparison,
                Expr::CorrelatedParam(param.clone()),
            );

            filter_predicate = Some(match filter_predicate {
                None => col_pred,
                Some(acc) => Predicate::and(acc, col_pred),
            });

            params.push(param);
            outer_indices.push(outer_col_idx);
        }

        let filter_op: Box<dyn PhysicalOperator> =
            Box::new(FilterOperator::new(inner_base, filter_predicate.unwrap()));
        let inner_plan_rc = Rc::new(RefCell::new(filter_op));

        Ok(Predicate::CorrelatedInSubquery {
            inner_plan: inner_plan_rc,
            params,
            outer_col_indices: outer_indices,
            lhs_expr,
            negated,
        })
    }

    /// Build a correlated scalar subquery with per-row execution.
    pub(crate) fn build_correlated_scalar_subquery(
        &self,
        subquery_info: &rook_ast::SubqueryInfo,
        outer_schema: &[ColumnInfo],
    ) -> RookResult<(Expr, DataType)> {
        let select = &subquery_info.select;

        let (inner_base, inner_table_alias, inner_col_names) =
            self.resolve_inner_scan_and_columns(select)?;

        let correlation_pairs = if let Some(ref selection) = select.selection {
            self.extract_all_correlations(selection, &inner_table_alias, &inner_col_names)?
        } else {
            return Err(RookError::Internal(
                "Correlated scalar subquery has no WHERE clause".to_string(),
            ));
        };

        let mut params = Vec::new();
        let mut outer_indices = Vec::new();
        let mut filter_predicate: Option<Predicate> = None;

        for (inner_col_name, outer_col_name, comp_op) in &correlation_pairs {
            let outer_col_idx = outer_schema
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(outer_col_name))
                .ok_or_else(|| RookError::NotFound {
                    entity: "Column",
                    name: outer_col_name.clone(),
                })?;

            let param = Rc::new(RefCell::new(None));
            let phys_comparison = match comp_op {
                rook_ast::ComparisonOp::Eq => ComparisonOp::Equals,
                rook_ast::ComparisonOp::Ne => ComparisonOp::NotEquals,
                rook_ast::ComparisonOp::Lt => ComparisonOp::LessThan,
                rook_ast::ComparisonOp::Le => ComparisonOp::LessOrEqual,
                rook_ast::ComparisonOp::Gt => ComparisonOp::GreaterThan,
                rook_ast::ComparisonOp::Ge => ComparisonOp::GreaterOrEqual,
            };

            let col_pred = Predicate::Compare(
                Expr::Column {
                    table: None,
                    column: inner_col_name.clone(),
                },
                phys_comparison,
                Expr::CorrelatedParam(param.clone()),
            );

            filter_predicate = Some(match filter_predicate {
                None => col_pred,
                Some(acc) => Predicate::and(acc, col_pred),
            });

            params.push(param);
            outer_indices.push(outer_col_idx);
        }

        let filtered_op: Box<dyn PhysicalOperator> =
            Box::new(FilterOperator::new(inner_base, filter_predicate.unwrap()));

        let has_aggregates = !select.group_by.is_empty()
            || select.having.is_some()
            || select
                .projections
                .iter()
                .any(crate::planner::helpers::contains_aggregate);

        let (mut op, data_type) = if has_aggregates {
            let aggregates = crate::planner::helpers::extract_aggregates(&select.projections);
            let log_agg = rook_ast::logical::LogicalAggregate {
                child: Box::new(rook_ast::logical::LogicalPlan::TableScan(
                    rook_ast::logical::LogicalTableScan {
                        table: "".to_string(),
                        alias: None,
                        schema: rook_ast::logical::ColumnSchema::empty(),
                        system_table_name: None,
                    },
                )),
                group_by: select.group_by.clone(),
                aggregates,
                having: select.having.clone(),
            };
            let agg_op = self.plan_aggregate_on_child(filtered_op, &log_agg)?;
            let dt = agg_op
                .schema()
                .first()
                .map(|c| c.data_type.clone())
                .unwrap_or(DataType::Int);
            (agg_op, dt)
        } else {
            let child_schema = filtered_op.schema();
            let col_names: Vec<String> = child_schema.iter().map(|c| c.name.clone()).collect();
            let col_types: Vec<_> = child_schema.iter().map(|c| c.data_type.clone()).collect();
            let mut projections = Vec::new();
            for p in &select.projections {
                match p {
                    rook_ast::SelectExpr::UnnamedExpr(e) => {
                        let (expr, dt) = self.plan_projection_expr(e, &col_names, &col_types)?;
                        projections.push((expr, "".to_string(), dt));
                    }
                    rook_ast::SelectExpr::ExprWithAlias { expr: e, alias } => {
                        let (expr, dt) = self.plan_projection_expr(e, &col_names, &col_types)?;
                        projections.push((expr, alias.clone(), dt));
                    }
                    _ => {}
                }
            }
            let dt = projections
                .first()
                .map(|(_, _, dt)| dt.clone())
                .unwrap_or(DataType::Int);
            let proj_op: Box<dyn PhysicalOperator> =
                Box::new(ProjectionOperator::new(filtered_op, projections));
            (proj_op, dt)
        };

        if let Some(ref l) = select.limit {
            op = Box::new(LimitOperator::new(
                op,
                l.limit as usize,
                l.offset.unwrap_or(0) as usize,
            ));
        }

        let inner_plan_rc = Rc::new(RefCell::new(op));
        Ok((
            Expr::CorrelatedScalarSubquery {
                inner_plan: inner_plan_rc,
                params,
                outer_col_indices: outer_indices,
            },
            data_type,
        ))
    }

    /// Extract all (inner_column_name, outer_column_name, comparison_op) pairs
    /// from a predicate that compares inner-table columns to outer-table columns.
    ///
    /// Walks the predicate tree, collecting ALL Compare nodes that reference
    /// both an inner and an outer column. For AND-connected comparisons, all
    /// pairs are returned. For OR-connected comparisons, returns the first found
    /// (simpler single-column EXISTS fallback).
    fn extract_all_correlations(
        &self,
        node: &rook_ast::PredicateNode,
        inner_table_name: &str,
        inner_col_names: &[String],
    ) -> RookResult<Vec<(String, String, rook_ast::ComparisonOp)>> {
        let mut pairs = Vec::new();
        self.collect_correlations(node, inner_table_name, inner_col_names, &mut pairs)?;
        if pairs.is_empty() {
            return Err(RookError::Internal(
                "No correlation found in subquery predicate".to_string(),
            ));
        }
        Ok(pairs)
    }

    /// Recursively collect correlation pairs from a predicate tree.
    fn collect_correlations(
        &self,
        node: &rook_ast::PredicateNode,
        inner_table_name: &str,
        inner_col_names: &[String],
        pairs: &mut Vec<(String, String, rook_ast::ComparisonOp)>,
    ) -> RookResult<()> {
        match node {
            rook_ast::PredicateNode::Compare { left, op, right } => {
                if let Ok((inner_name, outer_name)) =
                    self.resolve_inner_outer_columns(left, right, inner_table_name, inner_col_names)
                {
                    pairs.push((inner_name, outer_name, *op));
                }
                // If not a valid correlation, just skip it (not an error)
                Ok(())
            }
            rook_ast::PredicateNode::BinaryOp { left, op, right } => {
                match op {
                    rook_ast::BinaryOp::And => {
                        // AND: collect correlations from both sides
                        self.collect_correlations(left, inner_table_name, inner_col_names, pairs)?;
                        self.collect_correlations(right, inner_table_name, inner_col_names, pairs)?;
                        Ok(())
                    }
                    rook_ast::BinaryOp::Or => {
                        // OR: complex — just try to extract single from either side
                        if let Ok(mut single_pairs) =
                            self.extract_all_correlations(left, inner_table_name, inner_col_names)
                            && !single_pairs.is_empty()
                        {
                            pairs.append(&mut single_pairs);
                            return Ok(());
                        }
                        if let Ok(mut single_pairs) =
                            self.extract_all_correlations(right, inner_table_name, inner_col_names)
                        {
                            pairs.append(&mut single_pairs);
                        }
                        Ok(())
                    }
                }
            }
            _ => Ok(()), // Skip non-comparison predicates
        }
    }

    /// Legacy wrapper: extract a single (inner_column_name, outer_column_name, comparison_op)
    /// from the first valid correlation found.
    #[allow(dead_code)]
    fn extract_correlation_from_predicate(
        &self,
        node: &rook_ast::PredicateNode,
        inner_table_name: &str,
        inner_col_names: &[String],
    ) -> RookResult<(String, String, rook_ast::ComparisonOp)> {
        let pairs = self.extract_all_correlations(node, inner_table_name, inner_col_names)?;
        if pairs.is_empty() {
            return Err(RookError::Internal(
                "Correlated subquery predicate must contain a column comparison".to_string(),
            ));
        }
        Ok(pairs[0].clone())
    }

    /// Figure out which of `left` / `right` is the inner column and which is
    /// the outer column reference.
    fn resolve_inner_outer_columns(
        &self,
        left: &rook_ast::ExprNode,
        right: &rook_ast::ExprNode,
        inner_table_name: &str,
        inner_col_names: &[String],
    ) -> RookResult<(String, String)> {
        // Check (left = inner, right = outer)
        if let Some(inner_name) =
            self.extract_column_name_if_inner(left, inner_table_name, inner_col_names)
            && let Some(outer_name) =
                self.extract_column_name_if_outer(right, inner_table_name, inner_col_names)
        {
            return Ok((inner_name, outer_name));
        }

        // Check (left = outer, right = inner)
        if let Some(inner_name) =
            self.extract_column_name_if_inner(right, inner_table_name, inner_col_names)
            && let Some(outer_name) =
                self.extract_column_name_if_outer(left, inner_table_name, inner_col_names)
        {
            return Ok((inner_name, outer_name));
        }

        Err(RookError::Internal(
            "Could not resolve inner/outer columns in correlated subquery predicate".to_string(),
        ))
    }

    /// If `expr` references an inner table column, return the column name.
    fn extract_column_name_if_inner(
        &self,
        expr: &rook_ast::ExprNode,
        inner_table_name: &str,
        inner_col_names: &[String],
    ) -> Option<String> {
        let stripped = inner_table_name
            .strip_prefix("__cte__:")
            .unwrap_or(inner_table_name);
        match expr {
            rook_ast::ExprNode::Compound(parts) if parts.len() >= 2 => {
                if parts[0].eq_ignore_ascii_case(inner_table_name)
                    || parts[0].eq_ignore_ascii_case(stripped)
                {
                    return Some(parts[1].clone());
                }
                None
            }
            rook_ast::ExprNode::Column(name) => {
                // Unqualified column — check if it exists in inner table
                if inner_col_names.iter().any(|c| c.eq_ignore_ascii_case(name)) {
                    Some(name.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// If `expr` references an outer table column, return the column name
    /// (the last component of a Compound, or the Column name itself).
    fn extract_column_name_if_outer(
        &self,
        expr: &rook_ast::ExprNode,
        inner_table_name: &str,
        inner_col_names: &[String],
    ) -> Option<String> {
        let stripped = inner_table_name
            .strip_prefix("__cte__:")
            .unwrap_or(inner_table_name);
        match expr {
            rook_ast::ExprNode::Compound(parts) if parts.len() >= 2 => {
                // Outer reference: table prefix does NOT match inner table
                if !parts[0].eq_ignore_ascii_case(inner_table_name)
                    && !parts[0].eq_ignore_ascii_case(stripped)
                {
                    Some(parts[parts.len() - 1].clone())
                } else {
                    None
                }
            }
            rook_ast::ExprNode::Column(name) => {
                // Unqualified column — outer if it's NOT in the inner table
                if !inner_col_names.iter().any(|c| c.eq_ignore_ascii_case(name)) {
                    Some(name.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}
