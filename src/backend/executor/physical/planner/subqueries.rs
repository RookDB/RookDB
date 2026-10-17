//! Subquery planning for the PhysicalPlanner.
//!
//! Handles correlated EXISTS/IN subqueries, scalar subquery materialization,
//! and predicate building with subquery support.

use std::cell::RefCell;
use std::rc::Rc;

use super::super::engine::execute_shared;
use super::super::expr::correlated::{
    CorrelatedSubquery, OuterBinding, SubqueryUse, Visitor, outer_placeholder, ref_key,
    rewrite_pred, rewrite_select,
};
use super::super::expr::predicate::{InList, SubqueryKind};
use super::super::expr::{
    ComparisonOp, Expr, Predicate, expr_from_ast, predicate_from_ast, resolve_column,
};

/// An outer column a correlated subquery names: its qualifier, if written, and name.
type OuterRef = (Option<String>, String);

/// What a subquery's own FROM clause makes visible: the names a qualifier may
/// use (alias, or table name) and the columns of its relations. A reference
/// that matches neither belongs to the enclosing query (innermost scope wins).
struct InnerScope {
    qualifiers: Vec<String>,
    columns: Vec<String>,
}
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
                    Ok(Predicate::InSubqueryResult(
                        lhs_expr,
                        Rc::new(InList::new(values)),
                        *negated,
                    ))
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
                let dt: DataType = data_type.parse()?;
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

/// Finds a subquery's outer references (see `PhysicalPlanner::prepare_template`).
struct Marker<'a> {
    planner: &'a PhysicalPlanner,
    /// Scope of the select being walked, with and without output aliases.
    cur: InnerScope,
    cur_having: InnerScope,
    /// Scopes of the selects between the template root and the one walked,
    /// the root included.
    enclosing: Vec<InnerScope>,
    refs: &'a mut Vec<OuterRef>,
    /// Whether the walk is inside a nested subquery.
    nested: bool,
}

impl Visitor for Marker<'_> {
    fn expr(&mut self, node: &rook_ast::ExprNode, having: bool) -> Option<rook_ast::ExprNode> {
        let scope = if having { &self.cur_having } else { &self.cur };
        if !self.planner.expr_is_outer_ref(node, scope) {
            return None;
        }
        let key = ref_key(node)?;
        if self.nested {
            // Resolved by an enclosing scope up to the template root?
            if self
                .enclosing
                .iter()
                .any(|s| !self.planner.expr_is_outer_ref(node, s))
            {
                return None;
            }
            let placeholder = outer_placeholder(key.0.as_deref(), &key.1);
            if !self.refs.contains(&key) {
                self.refs.push(key);
            }
            return Some(placeholder);
        }
        if !self.refs.contains(&key) {
            self.refs.push(key);
        }
        None
    }

    fn subquery(&mut self, select: &mut rook_ast::SelectPlan) {
        let scope = self.planner.inner_scope(select);
        let having_scope = PhysicalPlanner::alias_scope(select, &scope);
        let outer_cur = std::mem::replace(&mut self.cur, scope);
        let outer_having = std::mem::replace(&mut self.cur_having, having_scope);
        let was_nested = std::mem::replace(&mut self.nested, true);
        self.enclosing.push(InnerScope {
            qualifiers: outer_cur.qualifiers.clone(),
            columns: outer_cur.columns.clone(),
        });
        rewrite_select(select, self);
        self.enclosing.pop();
        self.nested = was_nested;
        self.cur_having = outer_having;
        self.cur = outer_cur;
    }
}

// ── Correlated subquery detection ─────────────────────────────────────────────

impl PhysicalPlanner {
    /// Whether a column reference names a column of the enclosing query.
    ///
    /// A qualified name is outer when its qualifier is not one of the
    /// subquery's relations; an unqualified name is outer when no relation of
    /// the subquery has such a column (innermost scope wins).
    fn expr_is_outer_ref(&self, expr: &rook_ast::ExprNode, scope: &InnerScope) -> bool {
        match expr {
            rook_ast::ExprNode::Compound(parts) if parts.len() >= 2 => !scope
                .qualifiers
                .iter()
                .any(|t| t.eq_ignore_ascii_case(&parts[parts.len() - 2])),
            rook_ast::ExprNode::Column(name) => {
                !scope.columns.iter().any(|c| c.eq_ignore_ascii_case(name))
            }
            _ => false,
        }
    }

    /// The scope a clause sees where output aliases are also in scope
    /// (HAVING, ORDER BY).
    fn alias_scope(select: &rook_ast::SelectPlan, scope: &InnerScope) -> InnerScope {
        let mut with_aliases = InnerScope {
            qualifiers: scope.qualifiers.clone(),
            columns: scope.columns.clone(),
        };
        for p in &select.projections {
            if let rook_ast::SelectExpr::ExprWithAlias { alias, .. } = p {
                with_aliases.columns.push(alias.clone());
            }
        }
        with_aliases
    }

    /// The outer columns a subquery names, as lower-cased (qualifier, name),
    /// in first-use order. Looks through the SELECT list, WHERE, JOIN ... ON,
    /// GROUP BY, HAVING and ORDER BY (where output aliases are also in
    /// scope), and into nested subqueries for references that neither the
    /// nested subquery nor this one resolves.
    fn outer_refs(&self, select: &rook_ast::SelectPlan) -> Vec<OuterRef> {
        self.prepare_template(select).1
    }

    /// The template a correlated subquery is bound from, and its outer
    /// references. A reference inside a nested subquery that no scope up to
    /// and including `select` resolves reaches past `select` to the enclosing
    /// query; it is rewritten to a placeholder so it can be bound wherever it
    /// sits. References that `select` itself resolves are left for the nested
    /// subquery's own planning.
    fn prepare_template(
        &self,
        select: &rook_ast::SelectPlan,
    ) -> (rook_ast::SelectPlan, Vec<OuterRef>) {
        let scope = self.inner_scope(select);
        let having_scope = Self::alias_scope(select, &scope);
        let mut refs: Vec<OuterRef> = Vec::new();
        let mut template = select.clone();
        let mut marker = Marker {
            planner: self,
            cur: scope,
            cur_having: having_scope,
            enclosing: Vec::new(),
            refs: &mut refs,
            nested: false,
        };
        rewrite_select(&mut template, &mut marker);
        (template, refs)
    }

    /// Whether `pred` names an outer column, given the subquery's `scope`.
    fn pred_has_outer_ref(&self, pred: &rook_ast::PredicateNode, scope: &InnerScope) -> bool {
        let mut found = false;
        let mut probe = pred.clone();
        rewrite_pred(
            &mut probe,
            false,
            &mut |node: &rook_ast::ExprNode, _: bool| {
                found |= self.expr_is_outer_ref(node, scope);
                None
            },
        );
        found
    }

    pub(crate) fn is_subquery_correlated(&self, select: &rook_ast::SelectPlan) -> bool {
        !self.outer_refs(select).is_empty()
    }

    /// The scope a subquery's FROM clause (including JOINed relations) opens.
    fn inner_scope(&self, select: &rook_ast::SelectPlan) -> InnerScope {
        let mut qualifiers: Vec<String> = Vec::new();
        let mut columns: Vec<String> = Vec::new();
        let relations = select
            .from
            .iter()
            .chain(select.joins.iter().map(|j| &j.relation));
        for t in relations {
            if let Some(ref alias) = t.alias {
                qualifiers.push(alias.clone());
            } else {
                qualifiers.push(
                    t.name
                        .strip_prefix("__cte__:")
                        .unwrap_or(&t.name)
                        .to_string(),
                );
                qualifiers.push(t.name.clone());
            }
            // Columns come from the relation itself, never from its alias.
            columns.extend(self.relation_column_names(select, t));
        }
        InnerScope {
            qualifiers,
            columns,
        }
    }

    /// Column names of one FROM relation: a CTE's output or a table's schema.
    fn relation_column_names(
        &self,
        select: &rook_ast::SelectPlan,
        t: &rook_ast::TableRef,
    ) -> Vec<String> {
        let raw_name = t.name.strip_prefix("__cte__:").unwrap_or(&t.name);
        if let Some(cte_def) = select
            .ctes
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(raw_name))
        {
            return self.cte_output_columns(select, cte_def).unwrap_or_default();
        }
        self.resolve_table_schema(&t.name)
            .map(|cols| cols.into_iter().map(|c| c.name).collect())
            .unwrap_or_default()
    }

    /// Plan a CTE body and return its output column names.
    fn cte_output_columns(
        &self,
        select: &rook_ast::SelectPlan,
        cte_def: &rook_ast::CteDef,
    ) -> RookResult<Vec<String>> {
        let base = self.plan_cte_body(select, cte_def)?;
        Ok(base.schema().iter().map(|c| c.name.clone()).collect())
    }

    /// Plan a CTE body into a physical operator.
    fn plan_cte_body(
        &self,
        select: &rook_ast::SelectPlan,
        cte_def: &rook_ast::CteDef,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        let mut query = (*cte_def.query).clone();
        if query.ctes.is_empty() {
            query.ctes = select.ctes.clone();
        }
        let query_plan = rook_ast::QueryPlan::Select(query);
        let logical_plan = crate::planner::plan_query(&query_plan, &self.catalog, &self.db_name)
            .map_err(|e| {
                RookError::Internal(e.message)
                    .with_context("Failed to plan CTE inner query in correlated subquery")
            })?;
        let inner_planner =
            PhysicalPlanner::new(std::sync::Arc::clone(&self.catalog), self.db_name.clone());
        inner_planner.plan(&logical_plan)
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
            let inner_base = self.plan_cte_body(select, cte_def)?;
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
}

// ── Correlated subquery building ──────────────────────────────────────────────

/// What the caller does with the subquery's result.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    Exists,
    In,
    Scalar,
}

/// A correlated subquery in the simple shape: one table, WHERE a conjunction
/// of `inner_col <op> outer_col` terms and outer-free filters. Its inner plan
/// is built once and re-run per outer row with the parameters rebound.
struct SimpleCorrelation {
    /// The table scan with the WHERE filter applied.
    filtered: Box<dyn PhysicalOperator>,
    params: Vec<Rc<RefCell<Option<DataValue>>>>,
    outer_indices: Vec<usize>,
}

/// Whether `pred` contains a subquery.
fn has_nested_subquery(pred: &rook_ast::PredicateNode) -> bool {
    struct Finder(bool);
    impl Visitor for Finder {
        fn expr(&mut self, _: &rook_ast::ExprNode, _: bool) -> Option<rook_ast::ExprNode> {
            None
        }
        fn subquery(&mut self, _: &mut rook_ast::SelectPlan) {
            self.0 = true;
        }
    }
    let mut finder = Finder(false);
    rewrite_pred(&mut pred.clone(), false, &mut finder);
    finder.0
}

fn flatten_and<'a>(node: &'a rook_ast::PredicateNode, out: &mut Vec<&'a rook_ast::PredicateNode>) {
    match node {
        rook_ast::PredicateNode::BinaryOp {
            left,
            op: rook_ast::BinaryOp::And,
            right,
        } => {
            flatten_and(left, out);
            flatten_and(right, out);
        }
        other => out.push(other),
    }
}

/// `outer <op> inner` read as `inner <flipped op> outer`.
fn flip(op: rook_ast::ComparisonOp) -> rook_ast::ComparisonOp {
    use rook_ast::ComparisonOp as C;
    match op {
        C::Lt => C::Gt,
        C::Le => C::Ge,
        C::Gt => C::Lt,
        C::Ge => C::Le,
        other => other,
    }
}

fn physical_op(op: rook_ast::ComparisonOp) -> ComparisonOp {
    match op {
        rook_ast::ComparisonOp::Eq => ComparisonOp::Equals,
        rook_ast::ComparisonOp::Ne => ComparisonOp::NotEquals,
        rook_ast::ComparisonOp::Lt => ComparisonOp::LessThan,
        rook_ast::ComparisonOp::Le => ComparisonOp::LessOrEqual,
        rook_ast::ComparisonOp::Gt => ComparisonOp::GreaterThan,
        rook_ast::ComparisonOp::Ge => ComparisonOp::GreaterOrEqual,
    }
}

impl PhysicalPlanner {
    /// Plan the subquery's own table scan filtered by its WHERE clause, if the
    /// subquery has the simple shape for `shape`; `None` sends it to the
    /// general path, which runs it as a full plan per outer key.
    fn try_simple_correlation(
        &self,
        select: &rook_ast::SelectPlan,
        outer_schema: &[ColumnInfo],
        shape: Shape,
    ) -> RookResult<Option<SimpleCorrelation>> {
        if select.from.len() != 1
            || !select.joins.is_empty()
            || select.distinct
            || !select.order_by.is_empty()
        {
            return Ok(None);
        }
        let has_aggregates = !select.group_by.is_empty()
            || select.having.is_some()
            || select
                .projections
                .iter()
                .any(crate::planner::helpers::contains_aggregate);
        if shape != Shape::Scalar && (has_aggregates || select.limit.is_some()) {
            return Ok(None);
        }
        if shape == Shape::In
            && !matches!(
                select.projections.as_slice(),
                [rook_ast::SelectExpr::UnnamedExpr(_)]
                    | [rook_ast::SelectExpr::ExprWithAlias { .. }]
            )
        {
            return Ok(None);
        }
        // Outer references anywhere but WHERE are not bound by this path.
        let mut rest = select.clone();
        rest.selection = None;
        if !self.outer_refs(&rest).is_empty() {
            return Ok(None);
        }
        let Some(selection) = &select.selection else {
            return Ok(None);
        };
        // A nested subquery may reach the outer query; the general path binds it.
        if has_nested_subquery(selection) {
            return Ok(None);
        }

        let scope = self.inner_scope(select);
        let mut conjuncts = Vec::new();
        flatten_and(selection, &mut conjuncts);
        // (inner column, outer reference, comparison read as inner <op> outer)
        let mut correlations: Vec<(String, OuterRef, rook_ast::ComparisonOp)> = Vec::new();
        let mut locals: Vec<&rook_ast::PredicateNode> = Vec::new();
        for c in conjuncts {
            if !self.pred_has_outer_ref(c, &scope) {
                locals.push(c);
                continue;
            }
            let rook_ast::PredicateNode::Compare { left, op, right } = c else {
                return Ok(None);
            };
            let side = |e: &rook_ast::ExprNode| match e {
                rook_ast::ExprNode::Column(_) | rook_ast::ExprNode::Compound(_) => {
                    ref_key(e).map(|k| (self.expr_is_outer_ref(e, &scope), k))
                }
                _ => None,
            };
            match (side(left), side(right)) {
                (Some((false, (_, inner))), Some((true, outer))) => {
                    correlations.push((inner, outer, *op))
                }
                (Some((true, outer)), Some((false, (_, inner)))) => {
                    correlations.push((inner, outer, flip(*op)))
                }
                _ => return Ok(None),
            }
        }
        if correlations.is_empty() {
            return Ok(None);
        }

        let (inner_base, _, _) = self.resolve_inner_scan_and_columns(select)?;
        let names = super::super::expr::binding_names(inner_base.schema());
        let mut params = Vec::new();
        let mut outer_indices = Vec::new();
        let mut filter: Option<Predicate> = None;
        let mut and = |p: Predicate| {
            filter = Some(match filter.take() {
                None => p,
                Some(acc) => Predicate::and(acc, p),
            })
        };
        for (inner_col, (outer_qual, outer_name), op) in correlations {
            let idx = resolve_column(outer_schema, outer_qual.as_deref(), &outer_name)?
                .ok_or_else(|| RookError::NotFound {
                    entity: "Column",
                    name: outer_name.clone(),
                })?;
            let param = Rc::new(RefCell::new(None));
            and(Predicate::Compare(
                Expr::Column {
                    table: None,
                    column: inner_col,
                },
                physical_op(op),
                Expr::CorrelatedParam(param.clone()),
            ));
            params.push(param);
            outer_indices.push(idx);
        }
        for local in locals {
            match predicate_from_ast(local, &names) {
                Ok(p) => and(p),
                Err(_) => return Ok(None),
            }
        }
        let filter = filter.expect("at least one correlation");
        Ok(Some(SimpleCorrelation {
            filtered: Box::new(FilterOperator::new(inner_base, filter)),
            params,
            outer_indices,
        }))
    }

    /// Bind a subquery for the general path: resolve each outer column it
    /// names against `outer_schema`, and check it plans (so mistakes surface
    /// while planning, not on the first outer row). Returns the subquery and
    /// the schema of its result.
    fn build_general_correlated(
        &self,
        select: &rook_ast::SelectPlan,
        outer_schema: &[ColumnInfo],
        usage: SubqueryUse,
    ) -> RookResult<(Rc<CorrelatedSubquery>, Vec<ColumnInfo>)> {
        let (template, refs) = self.prepare_template(select);
        let mut bindings = Vec::new();
        for (qualifier, name) in refs {
            let index =
                resolve_column(outer_schema, qualifier.as_deref(), &name)?.ok_or_else(|| {
                    RookError::NotFound {
                        entity: "Column",
                        name: name.clone(),
                    }
                })?;
            bindings.push(OuterBinding {
                qualifier,
                name,
                index,
            });
        }
        let types: Vec<String> = bindings
            .iter()
            .map(|b| outer_schema[b.index].data_type.to_string())
            .collect();
        let sub = CorrelatedSubquery::new(
            template,
            bindings,
            std::sync::Arc::clone(&self.catalog),
            self.db_name.clone(),
            usage,
        );
        let query_plan = rook_ast::QueryPlan::Select(sub.probe_select(&types));
        let logical = crate::planner::plan_query(&query_plan, &self.catalog, &self.db_name)
            .map_err(|e| {
                RookError::Internal(e.message).with_context("Failed to plan correlated subquery")
            })?;
        let probe =
            PhysicalPlanner::new(std::sync::Arc::clone(&self.catalog), self.db_name.clone())
                .plan(&logical)?;
        let schema = probe.schema().to_vec();
        Ok((Rc::new(sub), schema))
    }

    /// Project a non-aggregate subquery's SELECT list over `child`.
    fn project_select_list(
        &self,
        select: &rook_ast::SelectPlan,
        child: Box<dyn PhysicalOperator>,
    ) -> RookResult<(Box<dyn PhysicalOperator>, DataType)> {
        let child_schema = child.schema();
        let col_names = super::super::expr::binding_names(child_schema);
        let mut projections = Vec::new();
        for p in &select.projections {
            match p {
                rook_ast::SelectExpr::UnnamedExpr(e) => {
                    let (expr, dt) = self.plan_projection_expr(e, &col_names, child_schema)?;
                    projections.push((expr, "".to_string(), dt));
                }
                rook_ast::SelectExpr::ExprWithAlias { expr: e, alias } => {
                    let (expr, dt) = self.plan_projection_expr(e, &col_names, child_schema)?;
                    projections.push((expr, alias.clone(), dt));
                }
                _ => {}
            }
        }
        let dt = projections
            .first()
            .map(|(_, _, dt)| dt.clone())
            .unwrap_or(DataType::Int);
        Ok((Box::new(ProjectionOperator::new(child, projections)), dt))
    }

    /// Build a correlated EXISTS subquery.
    fn build_correlated_exists(
        &self,
        subquery_info: &rook_ast::SubqueryInfo,
        outer_schema: &[ColumnInfo],
    ) -> RookResult<Predicate> {
        let select = &subquery_info.select;
        if let Some(simple) = self.try_simple_correlation(select, outer_schema, Shape::Exists)? {
            self.note(|| "Correlated EXISTS subquery: evaluated per outer row".to_string());
            return Ok(Predicate::CorrelatedExists {
                inner_plan: Rc::new(RefCell::new(simple.filtered)),
                params: simple.params,
                outer_col_indices: simple.outer_indices,
            });
        }
        let (sub, _) = self.build_general_correlated(select, outer_schema, SubqueryUse::Exists)?;
        self.note(|| "Correlated EXISTS subquery: full plan per distinct outer key".to_string());
        Ok(Predicate::CorrelatedSubquery {
            sub,
            kind: SubqueryKind::Exists,
        })
    }

    /// Build a correlated IN (subquery).
    fn build_correlated_in_subquery(
        &self,
        subquery: &rook_ast::SubqueryInfo,
        lhs_expr: Expr,
        negated: bool,
        outer_schema: &[ColumnInfo],
    ) -> RookResult<Predicate> {
        let select = &subquery.select;
        if let Some(simple) = self.try_simple_correlation(select, outer_schema, Shape::In)? {
            let (projected, _) = self.project_select_list(select, simple.filtered)?;
            self.note(|| "Correlated IN subquery: evaluated per outer row".to_string());
            return Ok(Predicate::CorrelatedInSubquery {
                inner_plan: Rc::new(RefCell::new(projected)),
                params: simple.params,
                outer_col_indices: simple.outer_indices,
                lhs_expr,
                negated,
            });
        }
        let (sub, _) = self.build_general_correlated(select, outer_schema, SubqueryUse::Rows)?;
        self.note(|| "Correlated IN subquery: full plan per distinct outer key".to_string());
        Ok(Predicate::CorrelatedSubquery {
            sub,
            kind: SubqueryKind::In {
                lhs: lhs_expr,
                negated,
            },
        })
    }

    /// Build a correlated scalar subquery.
    pub(crate) fn build_correlated_scalar_subquery(
        &self,
        subquery_info: &rook_ast::SubqueryInfo,
        outer_schema: &[ColumnInfo],
    ) -> RookResult<(Expr, DataType)> {
        let select = &subquery_info.select;
        let Some(simple) = self.try_simple_correlation(select, outer_schema, Shape::Scalar)? else {
            let (sub, schema) =
                self.build_general_correlated(select, outer_schema, SubqueryUse::Rows)?;
            let dt = schema
                .first()
                .map(|c| c.data_type.clone())
                .unwrap_or(DataType::Int);
            return Ok((Expr::SubqueryScalar(sub), dt));
        };
        let SimpleCorrelation {
            filtered: filtered_op,
            params,
            outer_indices,
        } = simple;

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
            self.project_select_list(select, filtered_op)?
        };

        if let Some(ref l) = select.limit {
            op = Box::new(LimitOperator::new(
                op,
                l.limit as usize,
                l.offset.unwrap_or(0) as usize,
            ));
        }

        Ok((
            Expr::CorrelatedScalarSubquery {
                inner_plan: Rc::new(RefCell::new(op)),
                params,
                outer_col_indices: outer_indices,
            },
            data_type,
        ))
    }
}
