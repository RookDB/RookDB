//! Rule-Based Optimizer — rewrites a `LogicalPlan` tree to improve execution
//! efficiency before physical planning.
//!
//! # Optimisation passes (applied in order):
//!
//! 1. **Constant folding** — Evaluate constant sub-expressions at plan time.
//! 2. **Predicate pushdown** — Push `LogicalFilter` below `Project`, `Sort`,
//!    `Distinct`, into join children (never into the null-supplying side of an
//!    outer join), and merge consecutive filters.
//! 3. **Projection pruning** — Remove columns from `LogicalTableScan` that are
//!    not referenced by any ancestor node.
//! 4. **Sort hoisting** — Move `Sort` below `Project` when the sort keys
//!    survive the projection.
//! 5. **Limit pushdown** — Push `LogicalLimit` through `LogicalSort` so the
//!    sort operator can use a bounded heap.
//! 6. **Join order heuristics** — Reorder joins so the smallest estimated table
//!    is on the left (build side of hash join). Requires statistics; skipped
//!    otherwise.

pub mod helpers;
#[cfg(test)]
pub mod tests;

use rook_ast::logical::*;
use rook_ast::{ExprNode, JoinType, PredicateNode};

use std::collections::{HashMap, HashSet};

use crate::planner::helpers::derive_qualified_schema;
use crate::statistics::TableStatistics;

use self::helpers::*;

/// The rule-based query optimizer.
///
/// Provide optional `TableStatistics` to enable join-order heuristics.
/// Without statistics, join ordering is skipped and all other rules still apply.
#[derive(Debug, Default)]
pub struct Optimizer {
    /// Map of table name → statistics, used by join-ordering heuristics.
    pub table_stats: Option<HashMap<String, TableStatistics>>,
}

impl Optimizer {
    /// Create a new optimizer with no statistics.
    pub fn new() -> Self {
        Self { table_stats: None }
    }

    /// Create an optimizer with table statistics (needed for join ordering).
    pub fn with_statistics(stats: HashMap<String, TableStatistics>) -> Self {
        Self {
            table_stats: Some(stats),
        }
    }

    /// Run all optimisation passes on the given logical plan.
    ///
    /// Passes are applied in this order:
    /// 1. Constant folding (pre-pass to simplify expressions)
    /// 2. Predicate pushdown
    /// 3. Projection pruning
    /// 4. Sort hoisting
    /// 5. Limit pushdown
    /// 6. Join ordering (only when statistics are available)
    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let plan = self.constant_folding(plan);
        let plan = self.predicate_pushdown(plan);
        let plan = self.projection_pruning(plan);
        let plan = self.hoist_sort_below_project(plan);
        let plan = self.limit_pushdown(plan);
        if self.table_stats.is_some() {
            self.join_ordering(plan)
        } else {
            plan
        }
    }

    // ─── Pass 1: Constant Folding ─────────────────────────────────────────

    /// Evaluate constant sub-expressions at plan time.
    fn constant_folding(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::TableScan(t) => LogicalPlan::TableScan(t),
            LogicalPlan::Filter(f) => {
                let child = self.constant_folding(*f.child);
                let pred = fold_predicate(&f.predicate);
                LogicalPlan::Filter(LogicalFilter {
                    predicate: pred,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Project(p) => {
                let child = self.constant_folding(*p.child);
                let expressions = p
                    .expressions
                    .into_iter()
                    .map(|ne| NamedExpr {
                        name: ne.name,
                        expr: fold_expr(&ne.expr),
                    })
                    .collect();
                LogicalPlan::Project(LogicalProject {
                    expressions,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Distinct(d) => {
                let child = self.constant_folding(*d.child);
                LogicalPlan::Distinct(LogicalDistinct {
                    child: Box::new(child),
                })
            }
            LogicalPlan::Sort(s) => {
                let child = self.constant_folding(*s.child);
                let order_by = s
                    .order_by
                    .into_iter()
                    .map(|ob| rook_ast::OrderByExpr {
                        expr: fold_expr(&ob.expr),
                        ascending: ob.ascending,
                    })
                    .collect();
                LogicalPlan::Sort(LogicalSort {
                    order_by,
                    child: Box::new(child),
                    limit: s.limit,
                })
            }
            LogicalPlan::Limit(l) => {
                let child = self.constant_folding(*l.child);
                LogicalPlan::Limit(LogicalLimit {
                    limit: l.limit,
                    offset: l.offset,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Aggregate(a) => {
                let child = self.constant_folding(*a.child);
                let group_by = a.group_by.into_iter().map(|e| fold_expr(&e)).collect();
                let aggregates = a
                    .aggregates
                    .into_iter()
                    .map(|ag| AggregateExpr {
                        function: ag.function,
                        args: ag.args.into_iter().map(|e| fold_expr(&e)).collect(),
                        alias: ag.alias,
                        distinct: ag.distinct,
                    })
                    .collect();
                LogicalPlan::Aggregate(LogicalAggregate {
                    group_by,
                    aggregates,
                    having: a.having.map(|h| fold_predicate(&h)),
                    child: Box::new(child),
                })
            }
            LogicalPlan::Join(j) => {
                let left = self.constant_folding(*j.left);
                let right = self.constant_folding(*j.right);
                let condition = j.condition.map(|c| fold_predicate(&c));
                LogicalPlan::Join(LogicalJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type: j.join_type,
                    condition,
                })
            }
            LogicalPlan::SetOp(s) => {
                let left = self.constant_folding(*s.left);
                let right = self.constant_folding(*s.right);
                LogicalPlan::SetOp(LogicalSetOp {
                    op: s.op,
                    left: Box::new(left),
                    right: Box::new(right),
                    all: s.all,
                })
            }
            LogicalPlan::Subquery(sq) => {
                let subquery = self.constant_folding(*sq.subquery);
                LogicalPlan::Subquery(LogicalSubquery {
                    subquery: Box::new(subquery),
                    alias: sq.alias,
                })
            }
            LogicalPlan::Cte(c) => {
                let inner = self.constant_folding(*c.inner);
                let outer = self.constant_folding(*c.outer);
                LogicalPlan::Cte(LogicalCte {
                    name: c.name,
                    inner: Box::new(inner),
                    outer: Box::new(outer),
                })
            }
            LogicalPlan::RecursiveCte(rc) => {
                let non_recursive = self.constant_folding(*rc.non_recursive);
                let recursive = self.constant_folding(*rc.recursive);
                let outer = self.constant_folding(*rc.outer);
                LogicalPlan::RecursiveCte(LogicalRecursiveCte {
                    name: rc.name,
                    non_recursive: Box::new(non_recursive),
                    recursive: Box::new(recursive),
                    union_all: rc.union_all,
                    schema: rc.schema,
                    outer: Box::new(outer),
                })
            }
            LogicalPlan::CteScan(_) => plan,
            LogicalPlan::Insert(inp) => {
                let child = self.constant_folding(*inp.child);
                LogicalPlan::Insert(LogicalInsert {
                    table: inp.table,
                    columns: inp.columns,
                    child: Box::new(child),
                    values_rows: Vec::new(),
                })
            }
        }
    }

    // ─── Pass 2: Predicate Pushdown ───────────────────────────────────────

    /// Move `LogicalFilter` nodes as close to `LogicalTableScan` as possible.
    fn predicate_pushdown(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::TableScan(_) => plan,
            LogicalPlan::Filter(f) => {
                let child = self.predicate_pushdown(*f.child);
                self.push_filter_down(f.predicate, child)
            }
            LogicalPlan::Project(p) => {
                let child = self.predicate_pushdown(*p.child);
                LogicalPlan::Project(LogicalProject {
                    expressions: p.expressions,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Sort(s) => {
                let child = self.predicate_pushdown(*s.child);
                LogicalPlan::Sort(LogicalSort {
                    order_by: s.order_by,
                    child: Box::new(child),
                    limit: s.limit,
                })
            }
            LogicalPlan::Limit(l) => {
                let child = self.predicate_pushdown(*l.child);
                LogicalPlan::Limit(LogicalLimit {
                    limit: l.limit,
                    offset: l.offset,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Distinct(d) => {
                let child = self.predicate_pushdown(*d.child);
                LogicalPlan::Distinct(LogicalDistinct {
                    child: Box::new(child),
                })
            }
            LogicalPlan::Aggregate(a) => {
                let child = self.predicate_pushdown(*a.child);
                LogicalPlan::Aggregate(LogicalAggregate {
                    group_by: a.group_by,
                    aggregates: a.aggregates,
                    having: a.having,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Join(j) => {
                let left = self.predicate_pushdown(*j.left);
                let right = self.predicate_pushdown(*j.right);
                LogicalPlan::Join(LogicalJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type: j.join_type,
                    condition: j.condition,
                })
            }
            LogicalPlan::SetOp(s) => {
                let left = self.predicate_pushdown(*s.left);
                let right = self.predicate_pushdown(*s.right);
                LogicalPlan::SetOp(LogicalSetOp {
                    op: s.op,
                    left: Box::new(left),
                    right: Box::new(right),
                    all: s.all,
                })
            }
            LogicalPlan::Subquery(sq) => {
                let subquery = self.predicate_pushdown(*sq.subquery);
                LogicalPlan::Subquery(LogicalSubquery {
                    subquery: Box::new(subquery),
                    alias: sq.alias,
                })
            }
            LogicalPlan::Cte(c) => {
                let inner = self.predicate_pushdown(*c.inner);
                let outer = self.predicate_pushdown(*c.outer);
                LogicalPlan::Cte(LogicalCte {
                    name: c.name,
                    inner: Box::new(inner),
                    outer: Box::new(outer),
                })
            }
            LogicalPlan::RecursiveCte(rc) => {
                let non_recursive = self.predicate_pushdown(*rc.non_recursive);
                let recursive = self.predicate_pushdown(*rc.recursive);
                let outer = self.predicate_pushdown(*rc.outer);
                LogicalPlan::RecursiveCte(LogicalRecursiveCte {
                    name: rc.name,
                    non_recursive: Box::new(non_recursive),
                    recursive: Box::new(recursive),
                    union_all: rc.union_all,
                    schema: rc.schema,
                    outer: Box::new(outer),
                })
            }
            LogicalPlan::CteScan(_) => plan,
            LogicalPlan::Insert(inp) => {
                let child = self.predicate_pushdown(*inp.child);
                LogicalPlan::Insert(LogicalInsert {
                    table: inp.table,
                    columns: inp.columns,
                    child: Box::new(child),
                    values_rows: Vec::new(),
                })
            }
        }
    }

    /// Try to push a predicate `pred` through (or into) `child`.
    fn push_filter_down(&self, pred: PredicateNode, child: LogicalPlan) -> LogicalPlan {
        match child {
            LogicalPlan::TableScan(_) => LogicalPlan::Filter(LogicalFilter {
                predicate: pred,
                child: Box::new(child),
            }),
            LogicalPlan::Filter(f) => {
                let merged = PredicateNode::BinaryOp {
                    left: Box::new(f.predicate),
                    op: rook_ast::BinaryOp::And,
                    right: Box::new(pred),
                };
                LogicalPlan::Filter(LogicalFilter {
                    predicate: merged,
                    child: f.child,
                })
            }
            LogicalPlan::Project(p) => {
                let passthrough_cols: HashSet<&str> = p
                    .expressions
                    .iter()
                    .filter_map(|ne| match &ne.expr {
                        ExprNode::Column(c) => Some(c.as_str()),
                        _ => None,
                    })
                    .collect();
                let pred_cols = extract_column_names(&pred);
                if pred_cols
                    .iter()
                    .all(|c| passthrough_cols.contains(c.as_str()))
                {
                    LogicalPlan::Project(LogicalProject {
                        expressions: p.expressions,
                        child: Box::new(self.push_filter_down(pred, *p.child)),
                    })
                } else {
                    LogicalPlan::Filter(LogicalFilter {
                        predicate: pred,
                        child: Box::new(LogicalPlan::Project(p)),
                    })
                }
            }
            LogicalPlan::Sort(s) => LogicalPlan::Sort(LogicalSort {
                order_by: s.order_by,
                child: Box::new(self.push_filter_down(pred, *s.child)),
                limit: s.limit,
            }),
            LogicalPlan::Distinct(d) => LogicalPlan::Distinct(LogicalDistinct {
                child: Box::new(self.push_filter_down(pred, *d.child)),
            }),
            LogicalPlan::Join(j) => {
                // Push filter predicates through joins when the predicate
                // references columns from only one side. This reduces the
                // number of rows that need to be joined. Each AND-ed conjunct
                // is placed on its own, and sides are decided with table
                // qualifiers: `a.k = r.k` touches both sides even though the
                // bare name `k` exists on the left alone.
                let left_schema = derive_qualified_schema(&j.left);
                let right_schema = derive_qualified_schema(&j.right);
                let side_has =
                    |side: &[(Option<String>, rook_ast::logical::ColumnInfo)],
                     (table, column): &(Option<String>, String)| {
                        side.iter().any(|(side_table, c)| {
                            c.name.eq_ignore_ascii_case(column)
                                && match (table, side_table) {
                                    (Some(t), Some(st)) => st.eq_ignore_ascii_case(t),
                                    // Unqualified reference, or a side whose columns
                                    // carry no table: match by name.
                                    _ => true,
                                }
                        })
                    };

                let can_push_left = matches!(
                    j.join_type,
                    JoinType::Inner | JoinType::Cross | JoinType::Natural | JoinType::Left
                );
                let can_push_right = matches!(
                    j.join_type,
                    JoinType::Inner | JoinType::Cross | JoinType::Natural | JoinType::Right
                );
                let can_merge = matches!(
                    j.join_type,
                    JoinType::Inner | JoinType::Cross | JoinType::Natural
                );

                let mut conjuncts = Vec::new();
                split_conjuncts(pred, &mut conjuncts);

                let mut left = *j.left;
                let mut right = *j.right;
                let mut condition = j.condition;
                let mut above: Option<PredicateNode> = None;
                let and = |acc: Option<PredicateNode>, p: PredicateNode| match acc {
                    Some(existing) => Some(PredicateNode::BinaryOp {
                        left: Box::new(existing),
                        op: rook_ast::BinaryOp::And,
                        right: Box::new(p),
                    }),
                    None => Some(p),
                };

                for conjunct in conjuncts {
                    let refs = extract_column_refs(&conjunct);
                    let on_left = refs.iter().all(|r| side_has(&left_schema, r));
                    let on_right = refs.iter().all(|r| side_has(&right_schema, r));

                    if on_left && !on_right && can_push_left {
                        // Only left-side columns, and the left child is preserved
                        left = self.push_filter_down(conjunct, left);
                    } else if on_right && !on_left && can_push_right {
                        // Only right-side columns, and the right child is preserved
                        right = self.push_filter_down(conjunct, right);
                    } else if !refs.is_empty() && can_merge {
                        // Columns from both sides of an INNER/CROSS join: merge
                        // into the join condition so the join filters as it runs.
                        condition = and(condition, conjunct);
                    } else {
                        // Outer join (or no columns at all): keep it above the
                        // join to preserve correct semantics.
                        above = and(above, conjunct);
                    }
                }

                let join = LogicalPlan::Join(LogicalJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type: j.join_type,
                    condition,
                });
                match above {
                    Some(predicate) => LogicalPlan::Filter(LogicalFilter {
                        predicate,
                        child: Box::new(join),
                    }),
                    None => join,
                }
            }
            LogicalPlan::Limit(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::SetOp(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::Cte(_)
            | LogicalPlan::CteScan(_)
            | LogicalPlan::RecursiveCte(_)
            | LogicalPlan::Insert(_) => LogicalPlan::Filter(LogicalFilter {
                predicate: pred,
                child: Box::new(child),
            }),
        }
    }

    // ─── Pass 3: Projection Pruning ───────────────────────────────────────

    /// Strip columns from `LogicalTableScan` that are not referenced by any ancestor.
    fn projection_pruning(&self, plan: LogicalPlan) -> LogicalPlan {
        let required = collect_required_columns(&plan);
        self.prune_columns(plan, &required)
    }

    /// Given a plan and required columns, prune column lists at TableScan/Project.
    fn prune_columns(&self, plan: LogicalPlan, required: &HashSet<String>) -> LogicalPlan {
        match plan {
            LogicalPlan::TableScan(t) => {
                // Never prune columns from system table scans: the physical
                // planner's map_info_schema_columns zips the logical schema
                // with the physical types, and a pruned schema would produce
                // too few types for correct heap tuple deserialisation.
                if t.system_table_name.is_some() {
                    return LogicalPlan::TableScan(t);
                }
                let kept_columns: Vec<ColumnInfo> = t
                    .schema
                    .columns
                    .into_iter()
                    .filter(|col| required.contains(&col.name) || required.is_empty())
                    .collect();
                LogicalPlan::TableScan(LogicalTableScan {
                    table: t.table,
                    alias: t.alias,
                    schema: ColumnSchema {
                        columns: kept_columns,
                    },
                    system_table_name: t.system_table_name,
                })
            }
            LogicalPlan::Filter(f) => {
                let child_needed = columns_in_predicate(&f.predicate)
                    .into_iter()
                    .chain(required.iter().cloned())
                    .collect();
                let child = self.prune_columns(*f.child, &child_needed);
                LogicalPlan::Filter(LogicalFilter {
                    predicate: f.predicate,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Project(p) => {
                let LogicalProject {
                    expressions: p_exprs,
                    child: p_child,
                } = p;
                let expr_cols: HashSet<String> = p_exprs
                    .iter()
                    .flat_map(|ne| extract_expr_columns(&ne.expr))
                    .collect();
                let child_needed: HashSet<String> = expr_cols
                    .into_iter()
                    .chain(required.iter().cloned())
                    .collect();
                let child = self.prune_columns(*p_child, &child_needed);
                LogicalPlan::Project(LogicalProject {
                    expressions: p_exprs,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Sort(s) => {
                let sort_cols: HashSet<String> = s
                    .order_by
                    .iter()
                    .flat_map(|ob| extract_expr_columns(&ob.expr))
                    .collect();
                let child_needed: HashSet<String> = sort_cols
                    .into_iter()
                    .chain(required.iter().cloned())
                    .collect();
                let child = self.prune_columns(*s.child, &child_needed);
                LogicalPlan::Sort(LogicalSort {
                    order_by: s.order_by,
                    child: Box::new(child),
                    limit: s.limit,
                })
            }
            LogicalPlan::Distinct(d) => {
                let child = self.prune_columns(*d.child, required);
                LogicalPlan::Distinct(LogicalDistinct {
                    child: Box::new(child),
                })
            }
            LogicalPlan::Limit(l) => {
                let child = self.prune_columns(*l.child, required);
                LogicalPlan::Limit(LogicalLimit {
                    limit: l.limit,
                    offset: l.offset,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Aggregate(a) => {
                let agg_cols: HashSet<String> = a
                    .aggregates
                    .iter()
                    .flat_map(|ag| ag.args.iter().flat_map(extract_expr_columns))
                    .chain(
                        a.having
                            .as_ref()
                            .map(columns_in_predicate)
                            .unwrap_or_default(),
                    )
                    .collect();
                let gb_cols: HashSet<String> =
                    a.group_by.iter().flat_map(extract_expr_columns).collect();
                let child_needed: HashSet<String> = agg_cols
                    .into_iter()
                    .chain(gb_cols)
                    .chain(required.iter().cloned())
                    .collect();
                let child = self.prune_columns(*a.child, &child_needed);
                LogicalPlan::Aggregate(LogicalAggregate {
                    group_by: a.group_by,
                    aggregates: a.aggregates,
                    having: a.having,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Join(j) => {
                let cond_cols = j
                    .condition
                    .as_ref()
                    .map(columns_in_predicate)
                    .unwrap_or_default();
                let all_needed: HashSet<String> = cond_cols
                    .into_iter()
                    .chain(required.iter().cloned())
                    .collect();
                let left = self.prune_columns(*j.left, &all_needed);
                let right = self.prune_columns(*j.right, &all_needed);
                LogicalPlan::Join(LogicalJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type: j.join_type,
                    condition: j.condition,
                })
            }
            LogicalPlan::SetOp(s) => {
                let left = self.prune_columns(*s.left, required);
                let right = self.prune_columns(*s.right, required);
                LogicalPlan::SetOp(LogicalSetOp {
                    op: s.op,
                    left: Box::new(left),
                    right: Box::new(right),
                    all: s.all,
                })
            }
            LogicalPlan::Subquery(sq) => {
                let subquery = self.prune_columns(*sq.subquery, required);
                LogicalPlan::Subquery(LogicalSubquery {
                    subquery: Box::new(subquery),
                    alias: sq.alias,
                })
            }
            LogicalPlan::Cte(c) => {
                let inner = self.prune_columns(*c.inner, required);
                let outer = self.prune_columns(*c.outer, required);
                LogicalPlan::Cte(LogicalCte {
                    name: c.name,
                    inner: Box::new(inner),
                    outer: Box::new(outer),
                })
            }
            LogicalPlan::RecursiveCte(rc) => {
                let non_recursive = self.prune_columns(*rc.non_recursive, required);
                let recursive = self.prune_columns(*rc.recursive, required);
                let outer = self.prune_columns(*rc.outer, required);
                LogicalPlan::RecursiveCte(LogicalRecursiveCte {
                    name: rc.name,
                    non_recursive: Box::new(non_recursive),
                    recursive: Box::new(recursive),
                    union_all: rc.union_all,
                    schema: rc.schema,
                    outer: Box::new(outer),
                })
            }
            LogicalPlan::CteScan(_) => plan,
            LogicalPlan::Insert(inp) => {
                let child = self.prune_columns(*inp.child, required);
                LogicalPlan::Insert(LogicalInsert {
                    table: inp.table,
                    columns: inp.columns,
                    child: Box::new(child),
                    values_rows: Vec::new(),
                })
            }
        }
    }

    // ─── Pass 4: Join Order Heuristics ────────────────────────────────────

    /// Reorder joins so the smallest estimated table is on the left.
    ///
    /// Only commutative join types (Inner, Cross, Natural) are reordered.
    /// Outer joins (Left, Right, Full) are preserved because they are not
    /// commutative — swapping them would change SQL semantics.
    fn join_ordering(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Join(j) => {
                let left = self.join_ordering(*j.left);
                let right = self.join_ordering(*j.right);

                // Only reorder commutative join types (Inner, Cross, Natural).
                // Left, Right, and Full outer joins are NOT commutative.
                let can_reorder = matches!(
                    j.join_type,
                    JoinType::Inner | JoinType::Cross | JoinType::Natural
                );

                if can_reorder {
                    let (left_size, right_size) = (
                        self.estimate_cardinality(&left),
                        self.estimate_cardinality(&right),
                    );
                    if right_size < left_size {
                        return LogicalPlan::Join(LogicalJoin {
                            left: Box::new(right),
                            right: Box::new(left),
                            join_type: j.join_type,
                            condition: j.condition,
                        });
                    }
                }

                LogicalPlan::Join(LogicalJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type: j.join_type,
                    condition: j.condition,
                })
            }
            LogicalPlan::Filter(f) => {
                let child = self.join_ordering(*f.child);
                LogicalPlan::Filter(LogicalFilter {
                    predicate: f.predicate,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Project(p) => {
                let child = self.join_ordering(*p.child);
                LogicalPlan::Project(LogicalProject {
                    expressions: p.expressions,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Sort(s) => {
                let child = self.join_ordering(*s.child);
                LogicalPlan::Sort(LogicalSort {
                    order_by: s.order_by,
                    child: Box::new(child),
                    limit: s.limit,
                })
            }
            LogicalPlan::Distinct(d) => {
                let child = self.join_ordering(*d.child);
                LogicalPlan::Distinct(LogicalDistinct {
                    child: Box::new(child),
                })
            }
            LogicalPlan::Limit(l) => {
                let child = self.join_ordering(*l.child);
                LogicalPlan::Limit(LogicalLimit {
                    limit: l.limit,
                    offset: l.offset,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Aggregate(a) => {
                let child = self.join_ordering(*a.child);
                LogicalPlan::Aggregate(LogicalAggregate {
                    group_by: a.group_by,
                    aggregates: a.aggregates,
                    having: a.having,
                    child: Box::new(child),
                })
            }
            LogicalPlan::SetOp(s) => {
                let left = self.join_ordering(*s.left);
                let right = self.join_ordering(*s.right);
                LogicalPlan::SetOp(LogicalSetOp {
                    op: s.op,
                    left: Box::new(left),
                    right: Box::new(right),
                    all: s.all,
                })
            }
            LogicalPlan::Subquery(sq) => {
                let subquery = self.join_ordering(*sq.subquery);
                LogicalPlan::Subquery(LogicalSubquery {
                    subquery: Box::new(subquery),
                    alias: sq.alias,
                })
            }
            LogicalPlan::Cte(c) => {
                let inner = self.join_ordering(*c.inner);
                let outer = self.join_ordering(*c.outer);
                LogicalPlan::Cte(LogicalCte {
                    name: c.name,
                    inner: Box::new(inner),
                    outer: Box::new(outer),
                })
            }
            LogicalPlan::RecursiveCte(rc) => {
                let non_recursive = self.join_ordering(*rc.non_recursive);
                let recursive = self.join_ordering(*rc.recursive);
                let outer = self.join_ordering(*rc.outer);
                LogicalPlan::RecursiveCte(LogicalRecursiveCte {
                    name: rc.name,
                    non_recursive: Box::new(non_recursive),
                    recursive: Box::new(recursive),
                    union_all: rc.union_all,
                    schema: rc.schema,
                    outer: Box::new(outer),
                })
            }
            LogicalPlan::CteScan(_) => plan,
            other => other,
        }
    }

    /// Estimate the number of rows a plan node will produce.
    fn estimate_cardinality(&self, plan: &LogicalPlan) -> f64 {
        match plan {
            LogicalPlan::TableScan(t) => {
                if let Some(ref stats) = self.table_stats {
                    stats
                        .get(&t.table)
                        .map(|s| s.total_tuple_count as f64)
                        .unwrap_or(1000.0)
                } else {
                    1000.0
                }
            }
            LogicalPlan::Filter(_) => self.estimate_child_cardinality(plan) * 0.1,
            LogicalPlan::Project(p) => self.estimate_cardinality(&p.child),
            LogicalPlan::Join(_) => self.estimate_child_cardinality(plan) * 0.2,
            LogicalPlan::Aggregate(_) => self.estimate_child_cardinality(plan) * 0.5,
            LogicalPlan::Distinct(d) => self.estimate_cardinality(&d.child) * 0.8,
            LogicalPlan::Sort(s) => self.estimate_cardinality(&s.child),
            LogicalPlan::Limit(l) => l.limit as f64,
            LogicalPlan::SetOp(s) => match s.op {
                SetOpType::Union => {
                    let left = self.estimate_cardinality(&s.left);
                    let right = self.estimate_cardinality(&s.right);
                    if s.all {
                        left + right
                    } else {
                        (left + right) * 0.7
                    }
                }
                _ => {
                    let left = self.estimate_cardinality(&s.left);
                    let right = self.estimate_cardinality(&s.right);
                    (left.min(right)) * 0.5
                }
            },
            LogicalPlan::Subquery(sq) => self.estimate_cardinality(&sq.subquery),
            LogicalPlan::Cte(c) => self.estimate_cardinality(&c.outer),
            LogicalPlan::RecursiveCte(rc) => self.estimate_cardinality(&rc.outer),
            LogicalPlan::CteScan(_) => 1000.0,
            LogicalPlan::Insert(inp) => self.estimate_cardinality(&inp.child),
        }
    }

    fn estimate_child_cardinality(&self, plan: &LogicalPlan) -> f64 {
        match plan {
            LogicalPlan::Filter(f) => self.estimate_cardinality(&f.child),
            LogicalPlan::Join(j) => {
                let left = self.estimate_cardinality(&j.left);
                let right = self.estimate_cardinality(&j.right);
                left * right
            }
            LogicalPlan::Aggregate(a) => self.estimate_cardinality(&a.child),
            LogicalPlan::Cte(c) => self.estimate_cardinality(&c.outer),
            LogicalPlan::RecursiveCte(rc) => self.estimate_cardinality(&rc.outer),
            LogicalPlan::CteScan(_) => 1000.0,
            _ => 1000.0,
        }
    }

    // ─── Pass: Sort hoisting ─────────────────────────────────────────────

    /// Rewrite `Sort(Project(X))` into `Project(Sort(X))` when every ORDER BY
    /// column is available in the projection's input.
    ///
    /// The logical planner places Sort above Project (SQL evaluation order),
    /// but sorting needs the pre-projection tuple when an ORDER BY column is
    /// not part of the SELECT list (`SELECT name FROM t ORDER BY salary`).
    fn hoist_sort_below_project(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Sort(s) => match *s.child {
                LogicalPlan::Project(p) => {
                    let input_schema = derive_qualified_schema(&p.child);
                    let all_keys_resolve = s
                        .order_by
                        .iter()
                        .all(|ob| sort_key_reads_input(&ob.expr, &p.expressions, &input_schema));
                    if all_keys_resolve {
                        let inner_sort = LogicalPlan::Sort(LogicalSort {
                            order_by: s.order_by,
                            child: p.child,
                            limit: s.limit,
                        });
                        LogicalPlan::Project(LogicalProject {
                            expressions: p.expressions,
                            child: Box::new(inner_sort),
                        })
                    } else {
                        LogicalPlan::Sort(LogicalSort {
                            order_by: s.order_by,
                            child: Box::new(LogicalPlan::Project(p)),
                            limit: s.limit,
                        })
                    }
                }
                other => LogicalPlan::Sort(LogicalSort {
                    order_by: s.order_by,
                    child: Box::new(other),
                    limit: s.limit,
                }),
            },
            // Recurse through result-shaping wrappers so a buried
            // `Sort(Project(..))` pair is still rewritten.
            LogicalPlan::Limit(l) => LogicalPlan::Limit(LogicalLimit {
                limit: l.limit,
                offset: l.offset,
                child: Box::new(self.hoist_sort_below_project(*l.child)),
            }),
            LogicalPlan::Distinct(d) => LogicalPlan::Distinct(LogicalDistinct {
                child: Box::new(self.hoist_sort_below_project(*d.child)),
            }),
            LogicalPlan::Project(p) => LogicalPlan::Project(LogicalProject {
                expressions: p.expressions,
                child: Box::new(self.hoist_sort_below_project(*p.child)),
            }),
            other => other,
        }
    }

    fn limit_pushdown(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Limit(l) if l.offset == 0 => match *l.child {
                LogicalPlan::Sort(s) => {
                    let existing_limit = s.limit.unwrap_or(l.limit);
                    let new_limit = existing_limit.min(l.limit);
                    let child = self.limit_pushdown(*s.child);
                    LogicalPlan::Sort(LogicalSort {
                        order_by: s.order_by,
                        child: Box::new(child),
                        limit: Some(new_limit),
                    })
                }
                LogicalPlan::Project(p) => {
                    let LogicalProject {
                        expressions: p_exprs,
                        child: p_child,
                    } = p;
                    let inner = self.limit_pushdown(*p_child);
                    match inner {
                        LogicalPlan::Sort(s) => {
                            let existing_limit = s.limit.unwrap_or(l.limit);
                            let new_limit = existing_limit.min(l.limit);
                            LogicalPlan::Project(LogicalProject {
                                expressions: p_exprs,
                                child: Box::new(LogicalPlan::Sort(LogicalSort {
                                    order_by: s.order_by,
                                    child: s.child,
                                    limit: Some(new_limit),
                                })),
                            })
                        }
                        _ => LogicalPlan::Limit(LogicalLimit {
                            limit: l.limit,
                            offset: l.offset,
                            child: Box::new(LogicalPlan::Project(LogicalProject {
                                expressions: p_exprs,
                                child: Box::new(inner),
                            })),
                        }),
                    }
                }
                other => {
                    let child = self.limit_pushdown(other);
                    LogicalPlan::Limit(LogicalLimit {
                        limit: l.limit,
                        offset: l.offset,
                        child: Box::new(child),
                    })
                }
            },
            LogicalPlan::Limit(l) => {
                let child = self.limit_pushdown(*l.child);
                // With an OFFSET the sort cannot absorb this node (it has no
                // skip concept), but its top-k hint must cover every row the
                // LIMIT..OFFSET window needs: `limit + offset` rows. The sort
                // may sit beneath result-shaping wrappers (Project/Distinct),
                // so the widening descends through those.
                let needed = l.limit.saturating_add(l.offset);
                let child = widen_sort_hint(child, needed);
                LogicalPlan::Limit(LogicalLimit {
                    limit: l.limit,
                    offset: l.offset,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Filter(f) => {
                let child = self.limit_pushdown(*f.child);
                LogicalPlan::Filter(LogicalFilter {
                    predicate: f.predicate,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Project(p) => {
                let child = self.limit_pushdown(*p.child);
                LogicalPlan::Project(LogicalProject {
                    expressions: p.expressions,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Sort(s) => {
                let child = self.limit_pushdown(*s.child);
                LogicalPlan::Sort(LogicalSort {
                    order_by: s.order_by,
                    child: Box::new(child),
                    limit: s.limit,
                })
            }
            LogicalPlan::Distinct(d) => {
                let child = self.limit_pushdown(*d.child);
                LogicalPlan::Distinct(LogicalDistinct {
                    child: Box::new(child),
                })
            }
            LogicalPlan::Aggregate(a) => {
                let child = self.limit_pushdown(*a.child);
                LogicalPlan::Aggregate(LogicalAggregate {
                    group_by: a.group_by,
                    aggregates: a.aggregates,
                    having: a.having,
                    child: Box::new(child),
                })
            }
            LogicalPlan::Join(j) => {
                let left = self.limit_pushdown(*j.left);
                let right = self.limit_pushdown(*j.right);
                LogicalPlan::Join(LogicalJoin {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type: j.join_type,
                    condition: j.condition,
                })
            }
            LogicalPlan::SetOp(s) => {
                let left = self.limit_pushdown(*s.left);
                let right = self.limit_pushdown(*s.right);
                LogicalPlan::SetOp(LogicalSetOp {
                    op: s.op,
                    left: Box::new(left),
                    right: Box::new(right),
                    all: s.all,
                })
            }
            LogicalPlan::Subquery(sq) => {
                let subquery = self.limit_pushdown(*sq.subquery);
                LogicalPlan::Subquery(LogicalSubquery {
                    subquery: Box::new(subquery),
                    alias: sq.alias,
                })
            }
            LogicalPlan::TableScan(_) => plan,
            LogicalPlan::Cte(c) => {
                let inner = self.limit_pushdown(*c.inner);
                let outer = self.limit_pushdown(*c.outer);
                LogicalPlan::Cte(LogicalCte {
                    name: c.name,
                    inner: Box::new(inner),
                    outer: Box::new(outer),
                })
            }
            LogicalPlan::RecursiveCte(rc) => {
                let non_recursive = self.limit_pushdown(*rc.non_recursive);
                let recursive = self.limit_pushdown(*rc.recursive);
                let outer = self.limit_pushdown(*rc.outer);
                LogicalPlan::RecursiveCte(LogicalRecursiveCte {
                    name: rc.name,
                    non_recursive: Box::new(non_recursive),
                    recursive: Box::new(recursive),
                    union_all: rc.union_all,
                    schema: rc.schema,
                    outer: Box::new(outer),
                })
            }
            LogicalPlan::CteScan(_) => plan,
            LogicalPlan::Insert(inp) => {
                let child = self.limit_pushdown(*inp.child);
                LogicalPlan::Insert(LogicalInsert {
                    table: inp.table,
                    columns: inp.columns,
                    child: Box::new(child),
                    values_rows: Vec::new(),
                })
            }
        }
    }
}

/// Whether an ORDER BY key names an input column of the projection, so the
/// sort can run beneath it. A bare name that the projection redefines
/// (`SELECT -id AS id ... ORDER BY id`) names the output instead, and a
/// qualified name must match its table in the input.
fn sort_key_reads_input(
    expr: &ExprNode,
    outputs: &[NamedExpr],
    input: &[(Option<String>, rook_ast::logical::ColumnInfo)],
) -> bool {
    match expr {
        ExprNode::Column(name) => {
            let passthrough = |e: &ExprNode| match e {
                ExprNode::Column(c) => c.eq_ignore_ascii_case(name),
                ExprNode::Compound(parts) => {
                    parts.last().is_some_and(|c| c.eq_ignore_ascii_case(name))
                }
                _ => false,
            };
            let redefined = outputs
                .iter()
                .any(|o| o.name.eq_ignore_ascii_case(name) && !passthrough(&o.expr));
            !redefined && input.iter().any(|(_, c)| c.name.eq_ignore_ascii_case(name))
        }
        ExprNode::Compound(parts) if parts.len() >= 2 => {
            let (table, name) = (&parts[parts.len() - 2], &parts[parts.len() - 1]);
            input.iter().any(|(t, c)| {
                c.name.eq_ignore_ascii_case(name)
                    && t.as_ref().is_none_or(|t| t.eq_ignore_ascii_case(table))
            })
        }
        _ => false,
    }
}

/// Widen the top-k hint of the topmost sort beneath result-shaping wrappers
/// so a `LIMIT .. OFFSET` window always has enough sorted rows to draw from.
fn widen_sort_hint(plan: LogicalPlan, needed: u64) -> LogicalPlan {
    match plan {
        LogicalPlan::Sort(s) => LogicalPlan::Sort(LogicalSort {
            order_by: s.order_by,
            child: s.child,
            limit: Some(needed),
        }),
        LogicalPlan::Project(p) => LogicalPlan::Project(LogicalProject {
            expressions: p.expressions,
            child: Box::new(widen_sort_hint(*p.child, needed)),
        }),
        // Do NOT push through Distinct: duplicate elimination reduces cardinality,
        // so pushing a top-k limit below Distinct prematurely drops distinct keys.
        LogicalPlan::Distinct(d) => LogicalPlan::Distinct(d),
        other => other,
    }
}

/// Flatten a predicate into its top-level AND-ed conjuncts.
fn split_conjuncts(pred: PredicateNode, out: &mut Vec<PredicateNode>) {
    match pred {
        PredicateNode::BinaryOp {
            left,
            op: rook_ast::BinaryOp::And,
            right,
        } => {
            split_conjuncts(*left, out);
            split_conjuncts(*right, out);
        }
        other => out.push(other),
    }
}
