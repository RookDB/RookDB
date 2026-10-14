//! Physical Planner — Converts a `LogicalPlan` into a tree of `PhysicalOperator` nodes.
//!
//! The planner walks the logical plan tree bottom-up and builds the corresponding
//! physical operator tree. It uses the catalog to resolve table schemas and file
//! paths needed for operators like `SeqScan`.

use rook_ast::logical::*;

use super::explain::{PlanNode, PlanTrace};
use super::expr::{Expr, Predicate, binding_names, expr_from_ast, resolve_column};
use super::operators::{
    CteScanOperator, DistinctOperator, Estimated, FilterOperator, IndexScanMode, IndexScanOperator,
    InsertOperator, LimitOperator, PhysicalOperator, ProjectionOperator, SeqScanOperator,
    SetOpOperator, SetOpType as PhysicalSetOpType, SingleRowOperator, SortOperator,
    SubqueryExecOperator, SubqueryType as PhysicalSubqueryType, ValuesOperator,
};
use super::tuple::ColumnInfo;
use crate::types::datatype::DataType;

use self::helpers::infer_expr_type_from_ast;
use crate::backend::catalog::types::Catalog;
use crate::backend::error::{RookError, RookResult};
use crate::backend::heap::heap_manager::HeapManager;
use crate::backend::index::btree::BTree;
use crate::statistics::estimator::{Estimator, StatsSource};

pub mod helpers;
pub mod index_scan;
pub mod joins;
mod read_columns;
pub mod subqueries;

/// Converts a `LogicalPlan` into an executable `PhysicalOperator` tree.
pub struct PhysicalPlanner {
    /// The catalog for schema lookups, shared with the caller and with the
    /// operators that need it.
    pub(crate) catalog: std::sync::Arc<Catalog>,
    /// Current database name.
    pub(crate) db_name: String,
    /// Lower-case names of the columns the query being planned reads, or
    /// `None` to decode every column (see `read_columns`).
    read_columns: std::cell::RefCell<Option<std::collections::HashSet<String>>>,
    /// Column types of the non-recursive CTEs in scope (derived tables
    /// included), by lower-case name, so a scan of one carries the types its
    /// body produced.
    cte_types: std::cell::RefCell<std::collections::HashMap<String, Vec<DataType>>>,
    /// Decisions that leave no operator in the tree, collected while a plan
    /// is being explained (`plan_traced`); `None` otherwise.
    trace: std::cell::RefCell<Option<PlanTrace>>,
    /// What the estimator learned of each table while this plan was made,
    /// so a table's statistics are looked up once per plan.
    table_info: std::cell::RefCell<std::collections::HashMap<String, TableInfo>>,
}

/// What `plan_sort_keys` works out: the (column index, descending) sort keys,
/// the hidden columns complex sort expressions need, and whether there are any.
type SortKeys = (Vec<(usize, bool)>, Vec<(Expr, String, DataType)>, bool);

/// A table's statistics (if current ones are at hand) and live row count.
#[derive(Clone)]
struct TableInfo {
    stats: Option<std::sync::Arc<crate::statistics::TableStatistics>>,
    rows: Option<f64>,
}

/// The statistics the estimator may read while a plan is made: only what is
/// already at hand, so planning a query never reads a table to gather them.
struct PlannerStats<'p>(&'p PhysicalPlanner);

impl PlannerStats<'_> {
    fn info(&self, table: &str) -> TableInfo {
        let planner = self.0;
        if let Some(info) = planner.table_info.borrow().get(table) {
            return info.clone();
        }
        let engine = planner.catalog.engine();
        let stats =
            crate::backend::cache::known_statistics(&planner.catalog, &planner.db_name, table);
        let rows = if stats.is_some() {
            None
        } else {
            crate::backend::heap::HeapFile::open(
                engine,
                &engine.paths().heap(&planner.db_name, table),
            )
            .and_then(|f| f.header())
            .ok()
            .map(|h| h.total_tuples as f64)
        };
        let info = TableInfo { stats, rows };
        planner
            .table_info
            .borrow_mut()
            .insert(table.to_string(), info.clone());
        info
    }
}

impl StatsSource for PlannerStats<'_> {
    fn table(&self, table: &str) -> Option<std::sync::Arc<crate::statistics::TableStatistics>> {
        self.info(table).stats
    }

    fn row_hint(&self, table: &str) -> Option<f64> {
        self.info(table).rows
    }
}

impl PhysicalPlanner {
    pub fn new(catalog: impl Into<std::sync::Arc<Catalog>>, db_name: String) -> Self {
        Self {
            catalog: catalog.into(),
            db_name,
            read_columns: std::cell::RefCell::new(None),
            cte_types: std::cell::RefCell::new(std::collections::HashMap::new()),
            trace: std::cell::RefCell::new(None),
            table_info: std::cell::RefCell::new(std::collections::HashMap::new()),
        }
    }

    /// [`plan`](Self::plan), also returning the decisions that left no
    /// operator in the tree. With `root.explain()` they describe the plan.
    pub fn plan_traced(
        &self,
        logical_plan: &LogicalPlan,
    ) -> RookResult<(Box<dyn PhysicalOperator>, PlanTrace)> {
        *self.trace.borrow_mut() = Some(PlanTrace::default());
        let root = self.plan(logical_plan);
        let trace = self.trace.borrow_mut().take().unwrap_or_default();
        Ok((root?, trace))
    }

    /// Record a decision that leaves no operator in the tree. The text is
    /// only built when the plan is being explained or debug logging is on.
    pub(crate) fn note(&self, text: impl FnOnce() -> String) {
        let tracing = self.trace.borrow().is_some();
        if !tracing && !log::log_enabled!(log::Level::Debug) {
            return;
        }
        let text = text();
        log::debug!("[Planner] {}", text);
        if let Some(trace) = self.trace.borrow_mut().as_mut() {
            trace.notes.push(text);
        }
    }

    /// Record the plan of a CTE evaluated while planning.
    pub(crate) fn note_cte(&self, name: &str, plan: impl FnOnce() -> PlanNode) {
        if let Some(trace) = self.trace.borrow_mut().as_mut() {
            trace.ctes.push((name.to_string(), plan()));
        }
    }

    /// Plan a logical query plan into a physical operator tree.
    ///
    /// Returns the root of the operator tree.
    pub fn plan(&self, logical_plan: &LogicalPlan) -> RookResult<Box<dyn PhysicalOperator>> {
        // Start with an empty CTE registry — CTEs are materialised on demand
        // as the planner descends into LogicalCte nodes.
        let mut cte_registry: std::collections::HashMap<String, Vec<super::tuple::Tuple>> =
            std::collections::HashMap::new();
        *self.read_columns.borrow_mut() = if self.catalog.engine().decodes_all_columns() {
            None
        } else {
            read_columns::read_columns(logical_plan)
        };
        self.cte_types.borrow_mut().clear();
        self.table_info.borrow_mut().clear();
        self.plan_internal(logical_plan, &mut cte_registry)
    }

    /// The operator for `plan`, carrying the estimator's row count for it.
    pub(crate) fn plan_internal(
        &self,
        plan: &LogicalPlan,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::tuple::Tuple>>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        let op = self.plan_node(plan, cte_registry)?;
        if op.is_estimated() {
            return Ok(op);
        }
        let rows = self.estimate_rows(plan, cte_registry);
        Ok(Box::new(Estimated::new(op, rows)))
    }

    /// The shared estimator's row count for `plan`.
    pub(crate) fn estimate_rows(
        &self,
        plan: &LogicalPlan,
        cte_registry: &std::collections::HashMap<String, Vec<super::tuple::Tuple>>,
    ) -> f64 {
        let stats = PlannerStats(self);
        let ctes = cte_registry
            .iter()
            .map(|(name, rows)| (name.clone(), rows.len() as f64))
            .collect();
        Estimator::new(&stats).with_cte_rows(ctes).rows(plan)
    }

    fn plan_node(
        &self,
        plan: &LogicalPlan,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::tuple::Tuple>>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        match plan {
            LogicalPlan::TableScan(ts) => self.plan_table_scan(ts),

            LogicalPlan::Filter(f) => self.plan_filter(f, cte_registry),

            LogicalPlan::Project(p) => self.plan_project(p, cte_registry),

            LogicalPlan::Distinct(d) => {
                let child = self.plan_internal(&d.child, cte_registry)?;
                Ok(Box::new(DistinctOperator::new(child)))
            }

            LogicalPlan::Sort(s) => self.plan_sort(s, cte_registry),

            LogicalPlan::Limit(l) => {
                let child = self.plan_internal(&l.child, cte_registry)?;
                Ok(Box::new(LimitOperator::new(
                    child,
                    l.limit as usize,
                    l.offset as usize,
                )))
            }

            LogicalPlan::Aggregate(a) => self.plan_aggregate_with_ctes(a, cte_registry),

            LogicalPlan::Join(j) => self.plan_join_with_ctes(j, cte_registry),

            LogicalPlan::SetOp(s) => self.plan_set_op(s, cte_registry),

            LogicalPlan::Subquery(sq) => self.plan_subquery(sq, cte_registry),

            // ── Recursive CTE node ──────────────────────────────────────────
            LogicalPlan::RecursiveCte(rc) => self.plan_recursive_cte(rc, cte_registry),

            // ── CTE nodes ────────────────────────────────────────────────────
            LogicalPlan::Cte(c) => self.plan_cte(c, cte_registry),

            LogicalPlan::Insert(inp) => self.plan_insert(inp, cte_registry),

            LogicalPlan::CteScan(cs) => self.plan_cte_scan(cs, cte_registry),
        }
    }

    /// A filter, index-accelerated when it sits directly on a table scan.
    fn plan_filter(
        &self,
        f: &LogicalFilter,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::tuple::Tuple>>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        // Check if the filter's child is a TableScan that can use an
        // index-accelerated scan (PointLookup or RangeLookup) instead
        // of a SeqScan + FilterOperator.
        if let LogicalPlan::TableScan(ref ts) = *f.child
            && let Some(scan_op) = self.try_plan_index_scan_with_predicate(ts, &f.predicate)?
        {
            // IMPORTANT: Always apply the full predicate as a FilterOperator
            // on top of the index-accelerated scan. This guarantees correctness
            // for compound AND predicates like `indexed_col = 5 AND other > 10`
            // where only the indexed portion is used by the index scan and the
            // non-indexed portion must still be applied as a filter.
            let predicate = self.make_predicate(&f.predicate, scan_op.schema())?;
            return Ok(Box::new(FilterOperator::new(scan_op, predicate)));
        }

        // Fall through: create a SeqScan + FilterOperator
        let child = self.plan_internal(&f.child, cte_registry)?;
        let predicate = self.make_predicate(&f.predicate, child.schema())?;
        Ok(Box::new(FilterOperator::new(child, predicate)))
    }

    /// A projection (`SELECT *` when it lists no expressions).
    fn plan_project(
        &self,
        p: &LogicalProject,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::tuple::Tuple>>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        let child = self.plan_internal(&p.child, cte_registry)?;
        let child_schema = child.schema();
        let column_names = binding_names(child_schema);
        let child_types: Vec<_> = child_schema.iter().map(|c| c.data_type.clone()).collect();

        // Convert project expressions to physical expressions
        if p.expressions.is_empty() {
            // No explicit expressions — treat as SELECT *
            return Ok(Box::new(ProjectionOperator::star(child)));
        }

        let mut projections = Vec::new();
        for ne in &p.expressions {
            let (e, data_type) =
                self.plan_projection_expr(&ne.expr, &column_names, &child_types, child_schema)?;
            projections.push((e, ne.name.clone(), data_type));
        }

        Ok(Box::new(ProjectionOperator::new(child, projections)))
    }

    /// ORDER BY, with an optional top-k bound.
    fn plan_sort(
        &self,
        s: &LogicalSort,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::tuple::Tuple>>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        let child = self.plan_internal(&s.child, cte_registry)?;
        // Clone the schema immediately to avoid borrowing `child` (which will be moved later)
        let child_schema: Vec<ColumnInfo> = child.schema().to_vec();
        let column_names: Vec<String> = child_schema.iter().map(|c| c.name.clone()).collect();
        let bind_names = binding_names(&child_schema);
        let child_types: Vec<_> = child_schema.iter().map(|c| c.data_type.clone()).collect();

        // ── Build sort keys, handling complex expressions ──────────────
        //
        // For simple column references, we look up the column index directly.
        // For complex expressions (e.g. `ORDER BY a+b`), we:
        //   1. Plan the expression as an extra physical expression
        //   2. Project it as an additional hidden column
        //   3. Sort by the hidden column's index
        //   4. Strip the hidden column after sorting
        let (sort_keys, extra_projections, has_complex) = self.plan_sort_keys(
            &s.order_by,
            &child_schema,
            &column_names,
            &bind_names,
            &child_types,
        )?;

        // ── Build the operator chain ──────────────────────────────────

        if has_complex {
            self.plan_sort_with_hidden_columns(
                s,
                child,
                &child_schema,
                column_names.len(),
                extra_projections,
                sort_keys,
            )
        } else {
            self.plan_sort_on_columns(s, child, &column_names, sort_keys)
        }
    }

    /// The sort keys as (column index, descending) pairs, the hidden columns that complex
    /// sort expressions need, and whether there are any.
    fn plan_sort_keys(
        &self,
        order_by: &[rook_ast::OrderByExpr],
        child_schema: &[ColumnInfo],
        column_names: &[String],
        bind_names: &[String],
        child_types: &[DataType],
    ) -> RookResult<SortKeys> {
        let mut sort_keys: Vec<(usize, bool)> = Vec::new();
        let mut extra_projections: Vec<(Expr, String, DataType)> = Vec::new();
        let mut has_complex = false;

        for ob in order_by {
            match &ob.expr {
                rook_ast::ExprNode::Column(name) => {
                    let idx = resolve_column(child_schema, None, name)?.ok_or_else(|| {
                        RookError::NotFound {
                            entity: "Column",
                            name: name.clone(),
                        }
                    })?;
                    sort_keys.push((idx, !ob.ascending));
                }
                rook_ast::ExprNode::Compound(parts) => {
                    let name = parts.last().cloned().unwrap_or_default();
                    // Under a join, `b.id` is b's `id`, not the first `id`.
                    let table = parts.len().checked_sub(2).map(|i| parts[i].as_str());
                    let idx = resolve_column(child_schema, table, &name)?
                        .or_else(|| {
                            column_names
                                .iter()
                                .position(|n| n.eq_ignore_ascii_case(&name))
                        })
                        .ok_or(RookError::NotFound {
                            entity: "Column",
                            name,
                        })?;
                    sort_keys.push((idx, !ob.ascending));
                }
                rook_ast::ExprNode::Constant(rook_ast::ConstantValue::Int(pos)) => {
                    let pos_usize = *pos as usize;
                    if pos_usize >= 1 && pos_usize <= column_names.len() {
                        sort_keys.push((pos_usize - 1, !ob.ascending));
                    } else {
                        return Err(RookError::Internal(format!(
                            "ORDER BY position {} is out of range (1..={})",
                            pos,
                            column_names.len()
                        )));
                    }
                }
                complex_expr => {
                    // Complex sort expression: project as a hidden column
                    let (phys_expr, data_type) = self.plan_projection_expr(
                        complex_expr,
                        bind_names,
                        child_types,
                        child_schema,
                    )?;
                    let col_name = format!("__sort_col_{}", extra_projections.len());
                    let sort_idx = column_names.len() + extra_projections.len();
                    sort_keys.push((sort_idx, !ob.ascending));
                    extra_projections.push((phys_expr, col_name.clone(), data_type));
                    has_complex = true;
                }
            }
        }

        Ok((sort_keys, extra_projections, has_complex))
    }

    /// Sort by hidden columns that hold complex expressions, then strip them.
    fn plan_sort_with_hidden_columns(
        &self,
        s: &LogicalSort,
        child: Box<dyn PhysicalOperator>,
        child_schema: &[ColumnInfo],
        column_count: usize,
        extra_projections: Vec<(Expr, String, DataType)>,
        sort_keys: Vec<(usize, bool)>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        // Option 1: Project all existing columns + extra sort columns
        let mut all_projections: Vec<(Expr, String, DataType)> = child_schema
            .iter()
            .map(|ci| {
                let expr = Expr::Column {
                    table: None,
                    column: ci.name.clone(),
                };
                (expr, ci.name.clone(), ci.data_type.clone())
            })
            .collect();
        all_projections.extend(extra_projections);
        let child_with_extra = Box::new(ProjectionOperator::new(child, all_projections));

        let mut sort_op = SortOperator::new(child_with_extra, sort_keys)
            .with_spill(self.catalog.engine().sort_spill());
        if let Some(limit) = s.limit {
            sort_op = sort_op.with_limit(limit as usize);
        }

        // Strip the extra sort columns
        let result_schema: Vec<String> = child_schema.iter().map(|c| c.name.clone()).collect();
        let indices: Vec<usize> = (0..column_count).collect();

        if let Some(limit) = s.limit {
            let limited = Box::new(LimitOperator::new(Box::new(sort_op), limit as usize, 0));
            Ok(Box::new(ProjectionOperator::from_indices(
                limited,
                &indices,
                &result_schema,
            )?))
        } else {
            Ok(Box::new(ProjectionOperator::from_indices(
                Box::new(sort_op),
                &indices,
                &result_schema,
            )?))
        }
    }

    /// Sort by plain columns, or skip the sort when the child already yields that order.
    fn plan_sort_on_columns(
        &self,
        s: &LogicalSort,
        child: Box<dyn PhysicalOperator>,
        column_names: &[String],
        sort_keys: Vec<(usize, bool)>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        // Simple case: all sort keys are column references
        let already_sorted = if let Some(child_order) = child.ordering() {
            sort_keys.len() <= child_order.len()
                && sort_keys
                    .iter()
                    .zip(&child_order)
                    .all(|(req, actual)| req == actual)
        } else {
            false
        };

        if already_sorted {
            self.note(|| {
                let keys: Vec<String> = sort_keys
                    .iter()
                    .map(|&(idx, descending)| {
                        let dir = if descending { "DESC" } else { "ASC" };
                        format!("{} {}", column_names[idx], dir)
                    })
                    .collect();
                format!(
                    "Sort eliminated: {} already yields rows ordered by {}",
                    child.name(),
                    keys.join(", ")
                )
            });
            if let Some(limit) = s.limit {
                Ok(Box::new(LimitOperator::new(child, limit as usize, 0)))
            } else {
                Ok(child)
            }
        } else {
            let mut sort_op =
                SortOperator::new(child, sort_keys).with_spill(self.catalog.engine().sort_spill());
            if let Some(limit) = s.limit {
                sort_op = sort_op.with_limit(limit as usize);
            }

            if let Some(limit) = s.limit {
                Ok(Box::new(LimitOperator::new(
                    Box::new(sort_op),
                    limit as usize,
                    0,
                )))
            } else {
                Ok(Box::new(sort_op))
            }
        }
    }

    /// UNION / INTERSECT / EXCEPT of two plans.
    fn plan_set_op(
        &self,
        s: &LogicalSetOp,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::tuple::Tuple>>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        let left = self.plan_internal(&s.left, cte_registry)?;
        let right = self.plan_internal(&s.right, cte_registry)?;

        if left.schema().len() != right.schema().len() {
            return Err(RookError::Internal(format!(
                "Set operation queries must have the same number of columns: {} vs {}",
                left.schema().len(),
                right.schema().len()
            )));
        }

        let op_type = match s.op {
            rook_ast::logical::SetOpType::Union => PhysicalSetOpType::Union,
            rook_ast::logical::SetOpType::Intersect => PhysicalSetOpType::Intersect,
            rook_ast::logical::SetOpType::Except => PhysicalSetOpType::Except,
        };

        Ok(Box::new(SetOpOperator::new(left, right, op_type, s.all)))
    }

    /// A subquery: a derived table in FROM, or a scalar subquery.
    fn plan_subquery(
        &self,
        sq: &LogicalSubquery,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::tuple::Tuple>>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        // For FROM-clause subqueries (derived tables), plan the inner query
        // and optionally wrap in SubqueryExecOperator in table (passthrough) mode.
        let inner = self.plan_internal(&sq.subquery, cte_registry)?;

        // If the subquery has an alias, it's a derived table in FROM clause.
        // Just return the inner plan directly — it acts as a scan of the subquery result.
        if sq.alias.is_some() {
            return Ok(inner);
        }

        // No alias means this is likely a scalar subquery used in an expression.
        // Wrap it in SubqueryExecOperator in Scalar mode.
        Ok(Box::new(SubqueryExecOperator::new(
            inner,
            PhysicalSubqueryType::Scalar,
            None,
        )))
    }

    /// A WITH clause: materialise the CTE body, then plan the outer query.
    fn plan_cte(
        &self,
        c: &LogicalCte,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::tuple::Tuple>>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        // 1. Materialise the CTE inner plan into an in-memory Vec<Tuple>.
        //
        // We fully execute the inner plan now (before planning the outer
        // query) so that any CteScan leaves in the outer query can look
        // up the cached tuples in O(1) without re-executing the CTE body.
        let mut inner_op = self.plan_internal(&c.inner, cte_registry)?;
        let mut tuples = Vec::new();
        while let Some(tuple) = inner_op.next()? {
            tuples.push(tuple);
        }
        self.note_cte(&c.name, || inner_op.explain());
        self.note(|| format!("CTE '{}' materialised: {} row(s)", c.name, tuples.len()));

        // 2. Register the materialised tuples so CteScan leaves can find
        //    them, with the column types the body declared.
        let key = c.name.to_ascii_lowercase();
        let types = inner_op
            .schema()
            .iter()
            .map(|ci| ci.data_type.clone())
            .collect();
        cte_registry.insert(key.clone(), tuples);
        let shadowed = self.cte_types.borrow_mut().insert(key.clone(), types);

        // 3. Plan the outer query (which may contain CteScan leaves that
        //    reference the CTE we just registered).
        let outer = self.plan_internal(&c.outer, cte_registry);
        let mut cte_types = self.cte_types.borrow_mut();
        match shadowed {
            Some(types) => cte_types.insert(key, types),
            None => cte_types.remove(&key),
        };
        outer
    }

    /// INSERT, from VALUES rows or from a SELECT.
    fn plan_insert(
        &self,
        inp: &LogicalInsert,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::tuple::Tuple>>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        // VALUES form: the logical planner already expanded rows to
        // full table arity; feed them via a constant-producing child.
        let child: Box<dyn PhysicalOperator> = if !inp.values_rows.is_empty() {
            let table_schema = self.resolve_table_schema(&inp.table)?;
            let schema: Vec<ColumnInfo> = table_schema
                .iter()
                .map(|c| ColumnInfo {
                    merged: false,
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    table: Some(inp.table.clone()),
                })
                .collect();
            let compiled = inp
                .values_rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|node| expr_from_ast(node, &[]))
                        .collect::<RookResult<Vec<_>>>()
                })
                .collect::<RookResult<Vec<_>>>()?;
            Box::new(ValuesOperator::new(compiled, schema))
        } else {
            self.plan_internal(&inp.child, cte_registry)?
        };

        // INSERT INTO t (cols) SELECT ...: place each selected value
        // in its listed column; the rest take their DEFAULT or NULL.
        // (The VALUES form arrives already expanded.)
        let target = if inp.values_rows.is_empty() && !inp.columns.is_empty() {
            let table_schema = self.resolve_table_schema(&inp.table)?;
            if child.schema().len() != inp.columns.len() {
                return Err(RookError::TypeMismatch(format!(
                    "INSERT lists {} column(s) but the SELECT returns {}",
                    inp.columns.len(),
                    child.schema().len()
                )));
            }
            let positions = inp
                .columns
                .iter()
                .map(|name| {
                    table_schema
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(name))
                        .ok_or_else(|| RookError::NotFound {
                            entity: "Column",
                            name: format!("{}.{}", inp.table, name),
                        })
                })
                .collect::<RookResult<Vec<usize>>>()?;
            let defaults = table_schema
                .iter()
                .map(|c| c.constraints.default.clone())
                .collect();
            Some((positions, defaults))
        } else {
            None
        };
        let insert = InsertOperator::new(
            child,
            inp.table.clone(),
            self.db_name.clone(),
            std::sync::Arc::clone(&self.catalog),
        );
        Ok(Box::new(match target {
            Some((positions, defaults)) => insert.with_target_columns(positions, defaults),
            None => insert,
        }))
    }

    /// A scan over the tuples a CTE materialised.
    fn plan_cte_scan(
        &self,
        cs: &LogicalCteScan,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::tuple::Tuple>>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        // Retrieve the pre-materialised tuples from the CTE registry.
        // For recursive CTEs, `plan_recursive_cte` updates the registry before
        // each iteration, so the operator always sees the latest working table.
        let tuples = cte_registry
            .get(&cs.name.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| {
                RookError::Internal(format!(
                    "CTE '{}' not found in registry during physical planning",
                    cs.name
                ))
            })?;

        // Build the schema for CteScanOperator from the CteScan node.
        // The CTE name serves as the table qualifier so that table-qualified
        // references (e.g. `my_cte.col`) and NATURAL JOIN disambiguation work
        // correctly when a CTE is on the right side of a join.
        // An alias (`FROM c x`) replaces the name, as it does for tables.
        // Columns take the types the CTE body declared; a recursive CTE,
        // whose rows come from more than one plan, keeps the untyped
        // VARCHAR placeholder.
        let cte_table_name = cs.alias.clone().unwrap_or_else(|| cs.name.clone());
        let cte_types = self.cte_types.borrow();
        let types = cte_types
            .get(&cs.name.to_ascii_lowercase())
            .filter(|types| types.len() == cs.schema.columns.len());
        let schema: Vec<super::tuple::ColumnInfo> = cs
            .schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| super::tuple::ColumnInfo {
                merged: false,
                name: c.name.clone(),
                data_type: types
                    .map(|types| types[i].clone())
                    .unwrap_or(DataType::Varchar(u16::MAX)),
                table: Some(cte_table_name.clone()),
            })
            .collect();

        Ok(Box::new(CteScanOperator::new(tuples, schema)))
    }
}

// ── Table scan planning ───────────────────────────────────────────────────────

impl PhysicalPlanner {
    /// Create a table scan operator for the given LogicalTableScan.
    ///
    /// If the scan targets a system table (`ts.system_table_name.is_some()`),
    /// the heap path is resolved to `database/system/{name}.dat` and the
    /// column schema for the output tuples is drawn from the system table's
    /// definition rather than the user catalog.
    ///
    /// Otherwise, if a `.idx` file exists alongside the `.dat` heap file,
    /// uses an `IndexScanOperator` with `FullScan` mode instead of a
    /// sequential scan.  This allows the B+ Tree index to drive tuple
    /// retrieval in key order.
    pub(crate) fn plan_table_scan(
        &self,
        ts: &LogicalTableScan,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        // ── System table path ─────────────────────────────────────────────
        if let Some(sys_name) = &ts.system_table_name {
            // Virtual single-row table: no heap file needed
            if sys_name == "__singlerow__" {
                return Ok(Box::new(SingleRowOperator::new()));
            }
            let heap_path = self.catalog.engine().paths().system_table(sys_name);
            if !heap_path.exists() {
                return Err(RookError::NotFound {
                    entity: "System table",
                    name: sys_name.clone(),
                });
            }
            // Use the system table's actual physical schema for deserialisation
            // (so INT columns are decoded as Int, BOOL as Bool, etc.) but keep
            // the INFORMATION_SCHEMA view column names for display.
            //
            // IMPORTANT: Some INFORMATION_SCHEMA views (especially COLUMNS) have
            // column names that do NOT positionally align with the physical system
            // table columns.  For example, sys_columns has physical columns
            // [col_id, table_id, name, data_type, ordinal, nullable, has_default,
            //  default_value] but the COLUMNS view exposes [table_catalog,
            //  table_schema, table_name, column_name, ordinal_position, data_type,
            //  is_nullable, column_default].  We must map each info_schema column
            //  to the correct physical column index and type.
            //
            // Use ts.schema (view column names, set by the logical planner) instead
            // of info_schema_column_schema(sys_name) because sys_name is the system
            // table name (e.g. "databases") while ts.schema holds the correct view
            // column names (e.g. "catalog_name", "schema_name" for SCHEMATA).
            let info_schema = &ts.schema;
            let sys_schema = crate::backend::system_table::system_table_schema(sys_name);
            // Map info_schema column names to physical column types + positions
            let (mut column_info, column_mapping) =
                self.map_info_schema_columns(sys_name, info_schema, sys_schema);
            // Set the table name for each column to allow table-qualified resolution
            // (alias takes precedence per SQL semantics).
            let sys_table_name = ts.alias.clone().unwrap_or_else(|| sys_name.to_string());
            for ci in column_info.iter_mut() {
                ci.table = Some(sys_table_name.clone());
            }
            let schema_types: Vec<DataType> = sys_schema.to_vec();
            // Owned handle: the operator keeps it for the whole query, so it cannot be leased from the engine cache.
            let heap_manager = HeapManager::open(heap_path).map_err(|e| {
                RookError::Io(e).with_context(format!("Failed to open system table '{}'", sys_name))
            })?;
            return Ok(Box::new(
                SeqScanOperator::new_with_mapping(
                    heap_manager,
                    schema_types,
                    column_info,
                    column_mapping,
                )
                .described(sys_name),
            ));
        }

        // ── Regular user table path ───────────────────────────────────────
        let table_schema = self.resolve_table_schema(&ts.table)?;
        // Stamp the ALIAS (when present) as the tuple qualifier — SQL
        // semantics: once a table is aliased, references must use the alias.
        // This also lets self-joins disambiguate two scans of one table.
        let table_name = ts.alias.clone().unwrap_or_else(|| ts.table.clone());
        let column_info: Vec<ColumnInfo> = table_schema
            .iter()
            .map(|c| ColumnInfo {
                merged: false,
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                table: Some(table_name.clone()),
            })
            .collect();

        let heap_path = self.catalog.engine().paths().heap(&self.db_name, &ts.table);

        if !heap_path.exists() {
            return Err(RookError::NotFound {
                entity: "Table",
                name: ts.table.clone(),
            });
        }

        // Try loading named indexes from sys_indexes
        let named_indexes = crate::backend::executor::create_index::load_table_indexes(
            self.catalog.engine(),
            &self.db_name,
            &ts.table,
        )
        .unwrap_or_default();

        if !named_indexes.is_empty() {
            // Use the first named index for the full scan
            let first_idx_name = &named_indexes[0].0;
            let index_path =
                self.catalog
                    .engine()
                    .paths()
                    .index(&self.db_name, &ts.table, first_idx_name);

            if index_path.exists() {
                crate::backend::cache::checkpoint();
                let mut btree = BTree::open(index_path).map_err(|e| {
                    RookError::Io(e)
                        .with_context(format!("Failed to open index for table '{}'", ts.table))
                })?;

                // Set the key type from the indexed column (NOT the first table column)
                let idx_col_name = &named_indexes[0].1;
                if let Some(idx_col) = table_schema
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(idx_col_name))
                {
                    btree.set_key_type(idx_col.data_type.clone());
                } else if let Some(first_col) = table_schema.first() {
                    btree.set_key_type(first_col.data_type.clone());
                }

                // Owned handle: the operator keeps it for the whole query, so it cannot be leased from the engine cache.
                let heap_manager = HeapManager::open(heap_path).map_err(|e| {
                    RookError::Io(e)
                        .with_context(format!("Failed to open heap for table '{}'", ts.table))
                })?;

                let indexed_cols: Vec<usize> = column_info
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(idx_col_name))
                    .into_iter()
                    .collect();

                return Ok(Box::new(
                    IndexScanOperator::new(
                        btree,
                        heap_manager,
                        IndexScanMode::FullScan,
                        column_info,
                        indexed_cols,
                    )
                    .described(&ts.table, first_idx_name),
                ));
            }
        }

        // Owned handle: the operator keeps it for the whole query, so it cannot be leased from the engine cache.
        let heap_manager = HeapManager::open(heap_path).map_err(|e| {
            RookError::Io(e).with_context(format!("Failed to open heap for table '{}'", ts.table))
        })?;

        // Decode only the columns the query reads.
        let wanted: Option<Vec<usize>> = self.read_columns.borrow().as_ref().map(|names| {
            column_info
                .iter()
                .enumerate()
                .filter(|(_, c)| names.contains(&c.name.to_ascii_lowercase()))
                .map(|(i, _)| i)
                .collect()
        });
        let scan = SeqScanOperator::new(heap_manager, column_info).described(&ts.table);
        Ok(match wanted {
            Some(columns) => Box::new(scan.read_only(columns)),
            None => Box::new(scan),
        })
    }
}

// ── Schema helpers ────────────────────────────────────────────────────────────

impl PhysicalPlanner {
    /// Return the column-to-physical-index mapping for a given info_schema view.
    ///
    /// Many INFORMATION_SCHEMA views expose column names that do NOT positionally
    /// align with the physical system table columns (e.g. "columns" view's
    /// `column_name` maps to physical index 2, not 3).  This function returns
    /// the correct `(view_col_idx -> phys_col_idx)` mapping so the SeqScan
    /// operator can reorder deserialised values to match view column order.
    fn info_schema_column_mapping(sys_name: &str) -> Vec<usize> {
        match sys_name.to_ascii_lowercase().as_str() {
            // sys_columns: [col_id(0), table_id(1), name(2), data_type(3),
            //               ordinal(4), nullable(5), has_default(6), default_value(7)]
            // COLUMNS view: [table_catalog, table_schema, table_name, column_name,
            //                ordinal_position, data_type, is_nullable, column_default]
            "columns" => vec![0, 1, 2, 2, 4, 3, 5, 7],

            // sys_constraints: [constraint_id(0), table_id(1), type(2),
            //                   columns(3), ref_table(4), ref_columns(5)]
            // TABLE_CONSTRAINTS view: [constraint_catalog, constraint_schema,
            //                          constraint_name, table_name,
            //                          constraint_type, constraint_columns]
            "constraints" | "table_constraints" => vec![0, 1, 2, 3, 2, 3],

            // sys_indexes: [index_id(0), table_id(1), name(2), is_unique(3),
            //               is_primary(4), columns(5)]
            // INDEXES/STATISTICS view: [table_catalog, table_schema, table_name,
            //                          index_name, unique, primary]
            "indexes" | "statistics" => vec![0, 1, 2, 2, 3, 4],

            // Default: empty = identity mapping (positions align).
            // Caller will generate identity based on sys_schema length.
            _ => Vec::new(),
        }
    }

    /// Map INFORMATION_SCHEMA view column names to the correct physical column types.
    ///
    /// Returns `(Vec<ColumnInfo>, Vec<usize>)` where:
    /// - `ColumnInfo` contains view column names with physical DataTypes
    /// - `Vec<usize>` maps each view column position to its physical column index
    ///
    /// The `SeqScanOperator` uses the physical DataTypes for correct deserialisation
    /// and applies the mapping to reorder values into view column order.
    pub(crate) fn map_info_schema_columns(
        &self,
        sys_name: &str,
        info_schema: &rook_ast::logical::ColumnSchema,
        sys_schema: &[DataType],
    ) -> (Vec<ColumnInfo>, Vec<usize>) {
        let mut mapping = Self::info_schema_column_mapping(sys_name);

        // If no explicit mapping (empty vec), use identity mapping: each view
        // column maps to the same physical position.  This is correct for views
        // like "tables", "schemata", "views" where columns align positionally.
        if mapping.is_empty() {
            mapping = (0..sys_schema.len()).collect();
        }

        let column_info: Vec<ColumnInfo> = info_schema
            .columns
            .iter()
            .zip(mapping.iter())
            .map(|(c, &phys_idx)| {
                let dt = sys_schema
                    .get(phys_idx)
                    .cloned()
                    .unwrap_or(DataType::Varchar(255));
                ColumnInfo {
                    merged: false,
                    name: c.name.clone(),
                    data_type: dt,
                    table: None,
                }
            })
            .collect();

        (column_info, mapping)
    }

    /// Resolve the table's schema from the catalog.
    pub(crate) fn resolve_table_schema(
        &self,
        table_name: &str,
    ) -> RookResult<Vec<crate::backend::catalog::types::Column>> {
        let db = self
            .catalog
            .databases
            .get(&self.db_name)
            .ok_or_else(|| RookError::NotFound {
                entity: "Database",
                name: self.db_name.clone(),
            })?;
        let table = db
            .tables
            .get(table_name)
            .ok_or_else(|| RookError::NotFound {
                entity: "Table",
                name: table_name.to_string(),
            })?;
        Ok(table.columns.clone())
    }

    /// Build a predicate from the AST predicate node, resolving column names
    /// against the child operator's schema. Handles subqueries (EXISTS, IN)
    /// by materializing them during physical plan construction.
    pub(crate) fn make_predicate(
        &self,
        pred_node: &rook_ast::PredicateNode,
        schema: &[ColumnInfo],
    ) -> RookResult<Predicate> {
        let column_names = binding_names(schema);
        self.build_predicate_with_subqueries(pred_node, &column_names, schema)
    }
}

// ── Projection expression planning ────────────────────────────────────────────

impl PhysicalPlanner {
    /// Plan a single projection expression, handling `ScalarSubquery` nodes
    /// directly without going through the ConstantValue round-trip.
    pub(crate) fn plan_projection_expr(
        &self,
        expr: &rook_ast::ExprNode,
        column_names: &[String],
        child_types: &[DataType],
        child_schema: &[ColumnInfo],
    ) -> RookResult<(Expr, DataType)> {
        match expr {
            // Top-level scalar subquery: materialize or build correlated.
            // Outer references bind against `child_schema`, so a qualifier
            // (`r.id` vs `l.id`) picks the right column.
            rook_ast::ExprNode::ScalarSubquery(info) => {
                if self.is_subquery_correlated(&info.select) {
                    let (phys_expr, dt) =
                        self.build_correlated_scalar_subquery(info, child_schema)?;
                    Ok((phys_expr, dt))
                } else {
                    let (value, data_type) = self.materialize_scalar_subquery(&info.select)?;
                    let phys_expr = match value {
                        Some(dv) => Expr::Constant(dv),
                        None => Expr::Null,
                    };
                    Ok((phys_expr, data_type))
                }
            }
            // Nested subqueries inside Binary or Cast
            rook_ast::ExprNode::Binary { left, op, right } => {
                let left_expr =
                    self.build_expr_with_correlated_subquery(left, column_names, child_schema)?;
                let right_expr =
                    self.build_expr_with_correlated_subquery(right, column_names, child_schema)?;
                let phys_op = match op {
                    rook_ast::ArithOp::Add => Expr::Add(Box::new(left_expr), Box::new(right_expr)),
                    rook_ast::ArithOp::Sub => Expr::Sub(Box::new(left_expr), Box::new(right_expr)),
                    rook_ast::ArithOp::Mul => Expr::Mul(Box::new(left_expr), Box::new(right_expr)),
                    rook_ast::ArithOp::Div => Expr::Div(Box::new(left_expr), Box::new(right_expr)),
                };
                let data_type = infer_expr_type_from_ast(expr, child_types, column_names)?;
                Ok((phys_op, data_type))
            }
            rook_ast::ExprNode::Cast {
                expr: inner,
                data_type,
            } => {
                let inner_expr =
                    self.build_expr_with_correlated_subquery(inner, column_names, child_schema)?;
                let target_dt: DataType = data_type.parse().map_err(|e: String| {
                    RookError::TypeMismatch(format!(
                        "Invalid CAST target type '{}': {}",
                        data_type, e
                    ))
                })?;
                Ok((
                    Expr::Cast(Box::new(inner_expr), target_dt.clone()),
                    target_dt,
                ))
            }
            // All other expression types: use the standard converter
            _ => {
                let e = expr_from_ast(expr, column_names)?;
                let data_type = infer_expr_type_from_ast(expr, child_types, column_names)?;
                Ok((e, data_type))
            }
        }
    }
}

// ── Index-accelerated scan planning (M1 + M2) ────────────────────────────────

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rook_ast::{BinaryOp, ComparisonOp, ConstantValue, ExprNode, PredicateNode};

    fn col(name: &str) -> ExprNode {
        ExprNode::Column(name.to_string())
    }
    fn compound(parts: &[&str]) -> ExprNode {
        ExprNode::Compound(parts.iter().map(|s| s.to_string()).collect())
    }
    fn constant_int(v: i64) -> ExprNode {
        ExprNode::Constant(ConstantValue::Int(v))
    }
    fn constant_float(v: f64) -> ExprNode {
        ExprNode::Constant(ConstantValue::Float(v))
    }
    fn constant_text(s: &str) -> ExprNode {
        ExprNode::Constant(ConstantValue::Text(s.to_string()))
    }
    #[allow(dead_code)]
    fn constant_bool(b: bool) -> ExprNode {
        ExprNode::Constant(ConstantValue::Boolean(b))
    }

    fn eq_pred(l: ExprNode, r: ExprNode) -> PredicateNode {
        PredicateNode::Compare {
            left: Box::new(l),
            op: ComparisonOp::Eq,
            right: Box::new(r),
        }
    }
    fn gt_pred(l: ExprNode, r: ExprNode) -> PredicateNode {
        PredicateNode::Compare {
            left: Box::new(l),
            op: ComparisonOp::Gt,
            right: Box::new(r),
        }
    }
    fn and_pred(l: PredicateNode, r: PredicateNode) -> PredicateNode {
        PredicateNode::BinaryOp {
            left: Box::new(l),
            op: BinaryOp::And,
            right: Box::new(r),
        }
    }
    fn not_pred(inner: PredicateNode) -> PredicateNode {
        PredicateNode::Not(Box::new(inner))
    }

    /// Create a dummy PhysicalPlanner for testing private methods.
    fn make_planner() -> PhysicalPlanner {
        PhysicalPlanner {
            catalog: std::sync::Arc::new(Catalog::new(
                crate::backend::engine::Engine::current_dir(),
            )),
            db_name: "test".to_string(),
            read_columns: std::cell::RefCell::new(None),
            cte_types: std::cell::RefCell::new(std::collections::HashMap::new()),
            trace: std::cell::RefCell::new(None),
            table_info: std::cell::RefCell::new(std::collections::HashMap::new()),
        }
    }

    // ── extract_index_mode_from_predicate ─────────────────────────────────

    #[test]
    fn test_extract_point_lookup_simple_eq() {
        let planner = make_planner();
        let pred = eq_pred(col("id"), constant_int(42));
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_some(), "Equality on indexed column should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(42));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_point_lookup_constant_on_left() {
        let planner = make_planner();
        let pred = eq_pred(constant_int(99), col("age"));
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "constant = col should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(99));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_non_indexed_column_returns_none() {
        let planner = make_planner();
        let pred = eq_pred(col("name"), constant_int(1));
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_none(), "Non-indexed column should not match");
    }

    #[test]
    fn test_extract_and_predicate_first_side_matches() {
        let planner = make_planner();
        let pred = and_pred(
            eq_pred(col("id"), constant_int(5)),
            eq_pred(col("name"), constant_text("hello")),
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_some(), "First side of AND should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(5));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_and_predicate_second_side_matches() {
        let planner = make_planner();
        let pred = and_pred(
            eq_pred(col("name"), constant_text("hello")),
            eq_pred(col("id"), constant_int(10)),
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_some(), "Second side of AND should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(10));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_and_predicate_neither_side_matches() {
        let planner = make_planner();
        let pred = and_pred(
            eq_pred(col("name"), constant_text("hello")),
            eq_pred(col("age"), constant_int(30)),
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_none(), "Neither side matches indexed column 'id'");
    }

    #[test]
    fn test_extract_not_predicate_returns_none() {
        let planner = make_planner();
        let pred = not_pred(eq_pred(col("id"), constant_int(5)));
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_none(), "NOT predicate should not match");
    }

    #[test]
    fn test_extract_gt_returns_range_lookup() {
        let planner = make_planner();
        // col > 18 → RangeLookup(19, MAX) for INT
        let pred = gt_pred(col("age"), constant_int(18));
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "Range operator should now match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(
                    low,
                    crate::types::value::DataValue::Int(19),
                    "Gt should increment the bound"
                );
                assert_eq!(
                    high,
                    crate::types::value::DataValue::Int(i32::MAX),
                    "Gt should use MAX sentinel"
                );
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_ge_uses_value_as_low() {
        let planner = make_planner();
        // col >= 18 → RangeLookup(18, MAX) for INT
        let pred = PredicateNode::Compare {
            left: Box::new(col("age")),
            op: ComparisonOp::Ge,
            right: Box::new(constant_int(18)),
        };
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "Ge should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(
                    low,
                    crate::types::value::DataValue::Int(18),
                    "Ge should NOT increment the bound"
                );
                assert_eq!(high, crate::types::value::DataValue::Int(i32::MAX));
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_lt_returns_range_lookup() {
        let planner = make_planner();
        // col < 10 → RangeLookup(MIN, 9) for INT
        let pred = PredicateNode::Compare {
            left: Box::new(col("age")),
            op: ComparisonOp::Lt,
            right: Box::new(constant_int(10)),
        };
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "Lt should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(i32::MIN));
                assert_eq!(
                    high,
                    crate::types::value::DataValue::Int(9),
                    "Lt should decrement the bound"
                );
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_le_uses_value_as_high() {
        let planner = make_planner();
        // col <= 10 → RangeLookup(MIN, 10) for INT
        let pred = PredicateNode::Compare {
            left: Box::new(col("age")),
            op: ComparisonOp::Le,
            right: Box::new(constant_int(10)),
        };
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "Le should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(i32::MIN));
                assert_eq!(
                    high,
                    crate::types::value::DataValue::Int(10),
                    "Le should NOT decrement the bound"
                );
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_gt_float_range() {
        let planner = make_planner();
        // salary > 50000.0 → RangeLookup(nextafter(50000.0), MAX) for DOUBLE
        let pred = gt_pred(col("salary"), constant_float(50000.0));
        let mode =
            planner.extract_index_mode_from_predicate(&pred, "salary", &DataType::DoublePrecision);
        assert!(mode.is_some(), "Gt on float should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                match low {
                    crate::types::value::DataValue::DoublePrecision(v) => {
                        assert!(v.0 > 50000.0, "Gt on float should use next-up value");
                    }
                    _ => panic!("Expected DoublePrecision"),
                }
                match high {
                    crate::types::value::DataValue::DoublePrecision(v) => {
                        assert_eq!(v.0, f64::MAX);
                    }
                    _ => panic!("Expected DoublePrecision"),
                }
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_gt_bigint_range() {
        let planner = make_planner();
        // big_col > 1_000_000_000_000 → RangeLookup(1_000_000_000_001, MAX) for BIGINT
        let pred = gt_pred(col("big_col"), constant_int(1_000_000_000_000));
        let mode = planner.extract_index_mode_from_predicate(&pred, "big_col", &DataType::BigInt);
        assert!(mode.is_some(), "Gt on BigInt should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(
                    low,
                    crate::types::value::DataValue::BigInt(1_000_000_000_001)
                );
                assert_eq!(high, crate::types::value::DataValue::BigInt(i64::MAX));
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_and_combines_two_range_lookups() {
        let planner = make_planner();
        // AND(col > 5, col < 10) on same column → RangeLookup(6, 9)
        let pred = and_pred(
            gt_pred(col("age"), constant_int(5)),
            PredicateNode::Compare {
                left: Box::new(col("age")),
                op: ComparisonOp::Lt,
                right: Box::new(constant_int(10)),
            },
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "AND of two ranges should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(
                    low,
                    crate::types::value::DataValue::Int(6),
                    "Combined low should be max(6, MIN) = 6"
                );
                assert_eq!(
                    high,
                    crate::types::value::DataValue::Int(9),
                    "Combined high should be min(MAX, 9) = 9"
                );
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_and_combines_ge_and_le() {
        let planner = make_planner();
        // AND(col >= 5, col <= 10) → RangeLookup(5, 10)
        let pred = and_pred(
            PredicateNode::Compare {
                left: Box::new(col("age")),
                op: ComparisonOp::Ge,
                right: Box::new(constant_int(5)),
            },
            PredicateNode::Compare {
                left: Box::new(col("age")),
                op: ComparisonOp::Le,
                right: Box::new(constant_int(10)),
            },
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "AND of Ge+Le should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(5));
                assert_eq!(high, crate::types::value::DataValue::Int(10));
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_and_combines_eq_and_gt() {
        let planner = make_planner();
        // AND(age = 7, age > 5) → PointLookup(7) (more selective)
        let pred = and_pred(
            eq_pred(col("age"), constant_int(7)),
            gt_pred(col("age"), constant_int(5)),
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "AND of Eq+Gt should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(
                    dv,
                    crate::types::value::DataValue::Int(7),
                    "PointLookup should be preferred over RangeLookup"
                );
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_compound_column() {
        let planner = make_planner();
        let pred = eq_pred(compound(&["users", "id"]), constant_int(42));
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_some(), "Compound column should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(42));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_varchar_equality() {
        let planner = make_planner();
        let pred = eq_pred(col("name"), constant_text("Alice"));
        let mode =
            planner.extract_index_mode_from_predicate(&pred, "name", &DataType::Varchar(255));
        assert!(mode.is_some(), "Varchar equality should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(
                    dv,
                    crate::types::value::DataValue::Varchar("Alice".to_string())
                );
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_float_equality() {
        let planner = make_planner();
        let pred = eq_pred(col("salary"), constant_float(75000.5));
        let mode =
            planner.extract_index_mode_from_predicate(&pred, "salary", &DataType::DoublePrecision);
        assert!(mode.is_some(), "Float equality should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => match dv {
                crate::types::value::DataValue::DoublePrecision(v) => {
                    assert!((v.0 - 75000.5).abs() < 0.001, "Unexpected value: {}", v.0);
                }
                _ => panic!("Expected DoublePrecision"),
            },
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_case_insensitive_column_match() {
        let planner = make_planner();
        // Indexed column is "ID" but predicate uses "id"
        let pred = eq_pred(col("id"), constant_int(1));
        let mode = planner.extract_index_mode_from_predicate(&pred, "ID", &DataType::Int);
        assert!(mode.is_some(), "Column matching should be case-insensitive");
    }

    // ── scan_mode_priority ────────────────────────────────────────────────

    #[test]
    fn test_scan_mode_priority_ordering() {
        let point = IndexScanMode::PointLookup(crate::types::value::DataValue::Int(0));
        let full = IndexScanMode::FullScan;

        assert_eq!(
            PhysicalPlanner::scan_mode_priority(&point),
            0,
            "PointLookup should have highest priority (0)"
        );
        assert_eq!(
            PhysicalPlanner::scan_mode_priority(&full),
            2,
            "FullScan should have lowest priority (2)"
        );
        assert!(
            PhysicalPlanner::scan_mode_priority(&point)
                < PhysicalPlanner::scan_mode_priority(&full),
            "PointLookup priority should be higher (lower number) than FullScan"
        );
    }

    // ── ast_constant_to_data_value ────────────────────────────────────────

    #[test]
    fn test_ast_constant_int_to_data_value() {
        let dv = PhysicalPlanner::ast_constant_to_data_value(&ConstantValue::Int(42));
        assert_eq!(dv, Some(crate::types::value::DataValue::Int(42)));
    }

    #[test]
    fn test_ast_constant_bigint_to_data_value() {
        // Larger than i32::MAX should become BigInt
        let dv = PhysicalPlanner::ast_constant_to_data_value(&ConstantValue::Int(3_000_000_000));
        assert_eq!(
            dv,
            Some(crate::types::value::DataValue::BigInt(3_000_000_000))
        );
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_ast_constant_float_to_data_value() {
        let dv = PhysicalPlanner::ast_constant_to_data_value(&ConstantValue::Float(3.14));
        match dv {
            Some(crate::types::value::DataValue::DoublePrecision(v)) => {
                assert!((v.0 - 3.14).abs() < 0.001);
            }
            _ => panic!("Expected DoublePrecision"),
        }
    }

    #[test]
    fn test_ast_constant_text_to_data_value() {
        let dv =
            PhysicalPlanner::ast_constant_to_data_value(&ConstantValue::Text("hello".to_string()));
        assert_eq!(
            dv,
            Some(crate::types::value::DataValue::Varchar("hello".to_string()))
        );
    }

    #[test]
    fn test_ast_constant_bool_to_data_value() {
        let dv = PhysicalPlanner::ast_constant_to_data_value(&ConstantValue::Boolean(true));
        assert_eq!(dv, Some(crate::types::value::DataValue::Bool(true)));
    }

    #[test]
    fn test_ast_constant_null_to_data_value() {
        let dv = PhysicalPlanner::ast_constant_to_data_value(&ConstantValue::Null);
        assert_eq!(dv, None, "Null maps to None");
    }

    // ── try_plan_index_scan_with_predicate edge cases ─────────────────────

    #[test]
    fn test_try_index_scan_system_table_returns_none() {
        // System tables are skipped (e.g., information_schema)
        let planner = make_planner();
        let ts = LogicalTableScan {
            table: "tables".to_string(),
            alias: None,
            schema: ColumnSchema::empty(),
            system_table_name: Some("tables".to_string()),
        };
        let pred = eq_pred(col("table_name"), constant_text("users"));
        let result = planner
            .try_plan_index_scan_with_predicate(&ts, &pred)
            .expect("Should not error");
        assert!(
            result.is_none(),
            "System tables should not use index-accelerated scans"
        );
    }

    #[test]
    fn test_try_index_scan_nonexistent_table_returns_none() {
        // Table not found in catalog → graceful None
        let planner = make_planner();
        let ts = LogicalTableScan {
            table: "ghost_table".to_string(),
            alias: None,
            schema: ColumnSchema::empty(),
            system_table_name: None,
        };
        let pred = eq_pred(col("id"), constant_int(1));
        let result = planner
            .try_plan_index_scan_with_predicate(&ts, &pred)
            .expect("Should not error");
        assert!(
            result.is_none(),
            "Non-existent table should return None gracefully"
        );
    }

    #[test]
    fn test_try_index_scan_nonexistent_table_is_graceful() {
        // Table doesn't exist in catalog → graceful None (not panic)
        let planner = make_planner();
        let ts = LogicalTableScan {
            table: "ghost".to_string(),
            alias: None,
            schema: ColumnSchema::empty(),
            system_table_name: None,
        };
        let pred = eq_pred(col("id"), constant_int(1));
        let result = planner.try_plan_index_scan_with_predicate(&ts, &pred);
        assert!(result.is_ok(), "Should not error on non-existent table");
        assert!(result.unwrap().is_none(), "Should return None gracefully");
    }

    // ── Compound AND predicate correctness ─────────────────────────────────

    #[test]
    fn test_extract_and_predicate_preserves_non_indexed_condition() {
        // This validates that extract_index_mode_from_predicate returns the
        // indexed portion of an AND predicate. The full predicate is ALWAYS
        // applied as a FilterOperator on top of the index scan, so the
        // non-indexed portion is preserved. This test verifies we correctly
        // extract the indexed-match portion.
        let planner = make_planner();

        // AND: indexed_col = 5 AND non_indexed_col > 10
        // Only the indexed part should be returned by extract
        let pred = and_pred(
            eq_pred(col("salary"), constant_float(50000.0)),
            gt_pred(col("age"), constant_int(30)),
        );

        let mode =
            planner.extract_index_mode_from_predicate(&pred, "salary", &DataType::DoublePrecision);
        assert!(
            mode.is_some(),
            "Should match the indexed column part of AND"
        );
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(
                    dv,
                    crate::types::value::DataValue::DoublePrecision(
                        crate::types::value::OrderedF64(50000.0)
                    ),
                    "Should extract the indexed side of the AND"
                );
            }
            other => panic!("Expected PointLookup for indexed column, got {:?}", other),
        }

        // NOTE: The non-indexed part (age > 30) is preserved by the FilterOperator
        // that wraps the index scan in plan_internal. This test just verifies
        // that extract_index_mode_from_predicate returns the correct indexed portion.
    }

    // ── BETWEEN index range pushdown (L1) ─────────────────────────────────

    fn between_pred(expr: ExprNode, low: ExprNode, high: ExprNode) -> PredicateNode {
        PredicateNode::Between {
            expr: Box::new(expr),
            low: Box::new(low),
            high: Box::new(high),
        }
    }

    fn inlist_pred(expr: ExprNode, items: Vec<ExprNode>) -> PredicateNode {
        PredicateNode::InList {
            expr: Box::new(expr),
            list: items,
        }
    }

    #[test]
    fn test_extract_between_int_range() {
        let planner = make_planner();
        // age BETWEEN 18 AND 65 → RangeLookup(18, 65)
        let pred = between_pred(col("age"), constant_int(18), constant_int(65));
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "BETWEEN on indexed column should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(18));
                assert_eq!(high, crate::types::value::DataValue::Int(65));
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_between_float_range() {
        let planner = make_planner();
        // salary BETWEEN 30000.0 AND 80000.0 → RangeLookup(30000.0, 80000.0)
        let pred = between_pred(
            col("salary"),
            constant_float(30000.0),
            constant_float(80000.0),
        );
        let mode =
            planner.extract_index_mode_from_predicate(&pred, "salary", &DataType::DoublePrecision);
        assert!(mode.is_some(), "BETWEEN on indexed column should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => match (low, high) {
                (
                    crate::types::value::DataValue::DoublePrecision(l),
                    crate::types::value::DataValue::DoublePrecision(h),
                ) => {
                    assert!((l.0 - 30000.0).abs() < 0.001);
                    assert!((h.0 - 80000.0).abs() < 0.001);
                }
                _ => panic!("Expected DoublePrecision values"),
            },
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_between_non_indexed_column_returns_none() {
        let planner = make_planner();
        // BETWEEN on 'name' but index is on 'id'
        let pred = between_pred(col("name"), constant_text("A"), constant_text("Z"));
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(
            mode.is_none(),
            "BETWEEN on non-indexed column should not match"
        );
    }

    #[test]
    fn test_extract_between_varchar_range() {
        let planner = make_planner();
        // name BETWEEN 'Alice' AND 'Charlie' → RangeLookup('Alice', 'Charlie')
        let pred = between_pred(
            col("name"),
            constant_text("Alice"),
            constant_text("Charlie"),
        );
        let mode =
            planner.extract_index_mode_from_predicate(&pred, "name", &DataType::Varchar(255));
        assert!(
            mode.is_some(),
            "BETWEEN on indexed VARCHAR column should match"
        );
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(
                    low,
                    crate::types::value::DataValue::Varchar("Alice".to_string())
                );
                assert_eq!(
                    high,
                    crate::types::value::DataValue::Varchar("Charlie".to_string())
                );
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_between_compound_column() {
        let planner = make_planner();
        // users.id BETWEEN 10 AND 20 → RangeLookup(10, 20)
        let pred = between_pred(
            compound(&["users", "id"]),
            constant_int(10),
            constant_int(20),
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_some(), "BETWEEN with compound column should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(10));
                assert_eq!(high, crate::types::value::DataValue::Int(20));
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_between_expression_not_constant_returns_none() {
        let planner = make_planner();
        // age BETWEEN col+1 AND 100  (dynamic low bound) → should not match
        let dynamic_low = ExprNode::Binary {
            left: Box::new(col("min_age")),
            op: rook_ast::ArithOp::Add,
            right: Box::new(constant_int(1)),
        };
        let pred = between_pred(col("age"), dynamic_low, constant_int(100));
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(
            mode.is_none(),
            "BETWEEN with dynamic low bound should not match"
        );
    }

    #[test]
    fn test_extract_between_and_eq_compound_uses_between() {
        let planner = make_planner();
        // AND(age BETWEEN 18 AND 65, name = 'Alice') on indexed 'age'
        // Should pick BETWEEN (RangeLookup) since it matches 'age'
        let pred = and_pred(
            between_pred(col("age"), constant_int(18), constant_int(65)),
            eq_pred(col("name"), constant_text("Alice")),
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "BETWEEN side of AND should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(18));
                assert_eq!(high, crate::types::value::DataValue::Int(65));
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    // ── IN list index pushdown (L1) ───────────────────────────────────────

    #[test]
    fn test_extract_inlist_single_element_creates_point_lookup() {
        let planner = make_planner();
        // id IN (42) → PointLookup(42)
        let pred = inlist_pred(col("id"), vec![constant_int(42)]);
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(
            mode.is_some(),
            "Single-element IN list on indexed column should match"
        );
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(42));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_inlist_multi_element_returns_none() {
        let planner = make_planner();
        // id IN (1, 2, 3) → not accelerated, falls through to SeqScan+Filter
        let pred = inlist_pred(
            col("id"),
            vec![constant_int(1), constant_int(2), constant_int(3)],
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(
            mode.is_none(),
            "Multi-element IN list should not be directly accelerated"
        );
    }

    #[test]
    fn test_extract_inlist_non_indexed_column_returns_none() {
        let planner = make_planner();
        // name IN ('Alice') on indexed 'id' → no match
        let pred = inlist_pred(col("name"), vec![constant_text("Alice")]);
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_none(), "IN on non-indexed column should not match");
    }

    #[test]
    fn test_extract_inlist_varchar_single() {
        let planner = make_planner();
        // name IN ('Bob') → PointLookup('Bob')
        let pred = inlist_pred(col("name"), vec![constant_text("Bob")]);
        let mode =
            planner.extract_index_mode_from_predicate(&pred, "name", &DataType::Varchar(255));
        assert!(mode.is_some(), "Single-element VARCHAR IN should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(
                    dv,
                    crate::types::value::DataValue::Varchar("Bob".to_string())
                );
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_inlist_compound_column_single() {
        let planner = make_planner();
        // users.id IN (100) → PointLookup(100)
        let pred = inlist_pred(compound(&["users", "id"]), vec![constant_int(100)]);
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(
            mode.is_some(),
            "Single-element IN with compound column should match"
        );
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(100));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_inlist_single_non_constant_returns_none() {
        let planner = make_planner();
        // id IN (some_column) → single element but not constant → no match
        let pred = inlist_pred(col("id"), vec![col("other")]);
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(
            mode.is_none(),
            "IN with non-constant list element should not match"
        );
    }
}
