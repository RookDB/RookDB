//! Subquery planning for the PhysicalPlanner.
//!
//! Handles correlated EXISTS/IN subqueries, scalar subquery materialization,
//! and predicate building with subquery support.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use super::super::engine::execute_shared;
use super::super::expr::convert::{Subqueries, predicate_with};
use super::super::expr::correlated::{
    CorrelatedSubquery, OuterBinding, SubqueryUse, outer_columns,
};
use super::super::expr::predicate::{InList, SubqueryKind};
use super::super::expr::{ComparisonOp, Expr, Predicate, predicate_from_ast, resolve_column};

/// An outer column a correlated subquery reads: its qualifier and name.
type OuterRef = (Option<String>, String);

use super::super::operators::{
    CorrelatedLookupOperator, FilterOperator, IndexScanOperator, LimitOperator, PhysicalOperator,
    ProjectionOperator,
};
use super::super::tuple::ColumnInfo;
use super::PhysicalPlanner;
use crate::backend::error::{ResultContext, RookError, RookResult};
use crate::planner::optimizer::helpers::contains_subquery;
use crate::types::DataValue;
use crate::types::datatype::DataType;

/// The physical planner's [`Subqueries`]: an uncorrelated subquery runs once
/// while planning and becomes its result; a correlated one is planned to run
/// per outer row, binding outer columns against `schema`.
pub(crate) struct PlanSubqueries<'a> {
    planner: &'a PhysicalPlanner,
    schema: &'a [ColumnInfo],
    /// The result type of each scalar subquery planned, by its address in
    /// the AST, for [`PlanSubqueries::scalar_type`].
    scalar_types: RefCell<HashMap<*const rook_ast::SubqueryInfo, DataType>>,
}

impl<'a> PlanSubqueries<'a> {
    pub(crate) fn new(planner: &'a PhysicalPlanner, schema: &'a [ColumnInfo]) -> Self {
        PlanSubqueries {
            planner,
            schema,
            scalar_types: RefCell::new(HashMap::new()),
        }
    }

    /// The result type of a scalar subquery this has already planned.
    pub(crate) fn scalar_type(&self, info: &rook_ast::SubqueryInfo) -> Option<DataType> {
        self.scalar_types.borrow().get(&(info as *const _)).cloned()
    }
}

impl Subqueries for PlanSubqueries<'_> {
    fn scalar(&self, info: &rook_ast::SubqueryInfo) -> RookResult<Expr> {
        let p = self.planner;
        let (expr, data_type) = if p.is_subquery_correlated(&info.select) {
            p.build_correlated_scalar_subquery(info, self.schema)?
        } else {
            let (value, data_type) = p.materialize_scalar_subquery(&info.select)?;
            (value.map_or(Expr::Null, Expr::Constant), data_type)
        };
        self.scalar_types
            .borrow_mut()
            .insert(info as *const _, data_type);
        Ok(expr)
    }

    fn exists(&self, info: &rook_ast::SubqueryInfo) -> RookResult<Predicate> {
        let p = self.planner;
        if p.is_subquery_correlated(&info.select) {
            p.note(|| "Correlated EXISTS subquery: evaluated per outer row".to_string());
            p.build_correlated_exists(info, self.schema)
        } else {
            let exists = p.materialize_exists_subquery(info.select.as_ref())?;
            p.note(|| format!("EXISTS subquery evaluated once while planning: {}", exists));
            Ok(Predicate::ExistsResult(exists))
        }
    }

    fn in_subquery(
        &self,
        lhs: Expr,
        info: &rook_ast::SubqueryInfo,
        negated: bool,
    ) -> RookResult<Predicate> {
        let p = self.planner;
        if p.is_subquery_correlated(&info.select) {
            p.note(|| "Correlated IN subquery: evaluated per outer row".to_string());
            p.build_correlated_in_subquery(info, lhs, negated, self.schema)
        } else {
            let values = p.materialize_in_subquery(info.select.as_ref())?;
            Ok(Predicate::InSubqueryResult(
                lhs,
                Rc::new(InList::new(values)),
                negated,
            ))
        }
    }
}

impl PhysicalPlanner {
    /// Convert a `PredicateNode` to a physical `Predicate`, planning any
    /// subqueries in it wherever they appear (see [`PlanSubqueries`]).
    pub(crate) fn build_predicate_with_subqueries(
        &self,
        node: &rook_ast::PredicateNode,
        column_names: &[String],
        schema: &[ColumnInfo],
    ) -> RookResult<Predicate> {
        predicate_with(node, column_names, &PlanSubqueries::new(self, schema))
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
            .context("Failed to plan EXISTS subquery")?;

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
            .context("Failed to plan IN subquery")?;

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
            .context("Failed to plan scalar subquery")?;

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
            n => Err(RookError::Evaluation(format!(
                "Scalar subquery returned more than one row (got {})",
                n
            ))),
        }
    }
}

// ── Correlated subquery detection ─────────────────────────────────────────────

impl PhysicalPlanner {
    /// The columns of the enclosing query a subquery reads, which the binder
    /// marked as outer columns.
    fn outer_refs(&self, select: &rook_ast::SelectPlan) -> Vec<OuterRef> {
        outer_columns(select)
    }

    pub(crate) fn is_subquery_correlated(&self, select: &rook_ast::SelectPlan) -> bool {
        !self.outer_refs(select).is_empty()
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
            .context("Failed to plan CTE inner query in correlated subquery")?;
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

/// Whether `expr` reads a column of the enclosing query.
fn reads_outer(expr: &rook_ast::ExprNode) -> bool {
    matches!(expr, rook_ast::ExprNode::OuterColumn(_))
        || expr.children().into_iter().any(reads_outer)
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
        let has_aggregates = crate::planner::grouping::is_grouped(select);
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
        if contains_subquery(selection) {
            return Ok(None);
        }

        let mut conjuncts = Vec::new();
        flatten_and(selection, &mut conjuncts);
        // (inner column, outer reference, comparison read as inner <op> outer)
        let mut correlations: Vec<(String, OuterRef, rook_ast::ComparisonOp)> = Vec::new();
        let mut locals: Vec<&rook_ast::PredicateNode> = Vec::new();
        for c in conjuncts {
            if !c.exprs().into_iter().any(reads_outer) {
                locals.push(c);
                continue;
            }
            let rook_ast::PredicateNode::Compare { left, op, right } = c else {
                return Ok(None);
            };
            use rook_ast::ExprNode as E;
            let inner = |e: &E| match e {
                E::Column(name) => Some(name.clone()),
                E::Compound(parts) => parts.last().cloned(),
                _ => None,
            };
            let outer = |e: &E| match e {
                E::OuterColumn(r) => Some((r.table.clone(), r.column.clone())),
                _ => None,
            };
            correlations.push(
                match (inner(left), outer(right), outer(left), inner(right)) {
                    (Some(i), Some(o), _, _) => (i, o, *op),
                    (_, _, Some(o), Some(i)) => (i, o, op.flipped()),
                    _ => return Ok(None),
                },
            );
        }
        if correlations.is_empty() {
            return Ok(None);
        }

        let (inner_base, _, _) = self.resolve_inner_scan_and_columns(select)?;
        let names = super::super::expr::binding_names(inner_base.schema());
        let mut params = Vec::new();
        let mut outer_indices = Vec::new();
        // Inner columns compared for equality, with their parameters: the
        // lookup keys that let the inner rows be hashed once.
        let mut lookup_columns = Vec::new();
        let mut lookup_params = Vec::new();
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
            if op == rook_ast::ComparisonOp::Eq
                && let Ok(Some(col)) = resolve_column(inner_base.schema(), None, &inner_col)
            {
                lookup_columns.push(col);
                lookup_params.push(param.clone());
            }
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
        let inner_base: Box<dyn PhysicalOperator> = if lookup_columns.is_empty() {
            inner_base
        } else {
            let probe = match lookup_columns.as_slice() {
                [column] => self.inner_index_probe(select, inner_base.schema(), *column),
                _ => None,
            };
            let lookup = CorrelatedLookupOperator::new(inner_base, lookup_columns, lookup_params);
            Box::new(match probe {
                Some((scan, key_type)) => lookup.with_index_probe(scan, key_type),
                None => lookup,
            })
        };
        Ok(Some(SimpleCorrelation {
            filtered: Box::new(FilterOperator::new(inner_base, filter)),
            params,
            outer_indices,
        }))
    }

    /// An index probe on `column` of the subquery's table, when it reads a
    /// table (not a CTE) with a single-column index there.
    fn inner_index_probe(
        &self,
        select: &rook_ast::SelectPlan,
        schema: &[ColumnInfo],
        column: usize,
    ) -> Option<(IndexScanOperator, DataType)> {
        let table = &select.from.first()?.name;
        if table.starts_with("__cte__:")
            || select
                .ctes
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(table))
        {
            return None;
        }
        self.plan_index_probe(table, schema.to_vec(), column)
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
        let template = select.clone();
        let refs = self.outer_refs(select);
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
            .context("Failed to plan correlated subquery")?;
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
        let items = select
            .projections
            .iter()
            .filter_map(|p| match p {
                rook_ast::SelectExpr::UnnamedExpr(e) => Some((e.clone(), String::new())),
                rook_ast::SelectExpr::ExprWithAlias { expr, alias } => {
                    Some((expr.clone(), alias.clone()))
                }
                _ => None,
            })
            .collect();
        self.project(items, child)
    }

    /// Compute `items` (expression, output name) over `child`; also the
    /// type of the first, the subquery's value.
    fn project(
        &self,
        items: Vec<(rook_ast::ExprNode, String)>,
        child: Box<dyn PhysicalOperator>,
    ) -> RookResult<(Box<dyn PhysicalOperator>, DataType)> {
        let child_schema = child.schema();
        let col_names = super::super::expr::binding_names(child_schema);
        let mut projections = Vec::new();
        for (e, name) in items {
            let (expr, dt) = self.plan_projection_expr(&e, &col_names, child_schema)?;
            projections.push((expr, name, dt));
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

        let (mut op, data_type) = if crate::planner::grouping::is_grouped(select) {
            // Aggregate the filtered rows, then compute the SELECT list from
            // the aggregation's columns, as the logical planner does.
            let grouping = crate::planner::grouping::Grouping::new(select)?;
            let rook_ast::logical::LogicalPlan::Aggregate(aggregate) = grouping.aggregate(
                select.having.as_ref(),
                rook_ast::logical::LogicalPlan::TableScan(rook_ast::logical::LogicalTableScan {
                    table: String::new(),
                    alias: None,
                    schema: rook_ast::logical::ColumnSchema::empty(),
                    system_table_name: None,
                }),
            )?
            else {
                unreachable!("Grouping::aggregate plans an aggregation");
            };
            let aggregated = self.plan_aggregate_on_child(filtered_op, &aggregate)?;
            let outputs = grouping.outputs(&select.projections)?;
            self.project(
                outputs.into_iter().map(|o| (o.expr, o.name)).collect(),
                aggregated,
            )?
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
