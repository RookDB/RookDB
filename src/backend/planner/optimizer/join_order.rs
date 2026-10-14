//! Pass 6: join ordering.
//!
//! A run of INNER and CROSS joins is a *chain*: its inputs (the relations)
//! may be joined in any order, provided every condition is applied once
//! all of its columns are present. The pass flattens each maximal chain
//! into its relations and the AND-ed conjuncts of its conditions, and
//! rebuilds it greedily:
//!
//! 1. A conjunct that reads one relation becomes a filter on it.
//! 2. Start from the relation with the smallest estimated size.
//! 3. Repeatedly join the relation that gives the smallest estimated result
//!    among those an equality connects to what is joined so far (failing
//!    that, any condition; failing that, a cross product).
//! 4. Each conjunct goes on the lowest join that has all its relations.
//!
//! Within each join the smaller estimated input goes on the left, the build
//! side of a hash join. Estimates come from the shared [`Estimator`](crate::statistics::estimator::Estimator).
//!
//! Outer and NATURAL joins end a chain and keep their inputs in place; a
//! chain inside one of their inputs is reordered on its own.
//!
//! Reordering changes the order of the columns a join outputs, never which
//! columns. Rows leave a SELECT through a projection that names each column
//! (`SELECT *` is expanded into qualified names), so the order the client
//! sees is the written one, and a qualified name finds the same column in
//! any order. An unqualified name finds the *first* column of that name, so
//! a chain stays as written when the query reads, unqualified, a name that
//! two of its relations have; or, when the query also resolves names in
//! ways not visible in the plan (a NATURAL join, a subquery expression),
//! when two of its relations share any name at all. The pass also leaves a
//! chain alone when a relation is anything but base tables, filters and
//! joins (a derived table, a view, a CTE), as their columns' qualifiers are
//! not known here.

use std::collections::HashSet;

use rook_ast::logical::*;
use rook_ast::{ComparisonOp, ExprNode, FunctionArg, JoinType, PredicateNode};

use crate::planner::helpers::derive_qualified_schema;

use super::helpers::{extract_column_refs, extract_expr_column_refs};
use super::{Optimizer, split_conjuncts};

/// What the query around the joins lets the pass change: the names it reads
/// unqualified, where the order of a join's columns could matter.
#[derive(Default)]
struct OrderGuard {
    /// Lower-case names read without a table qualifier, against join rows.
    unqualified: HashSet<String>,
    /// The query resolves names the plan does not show (NATURAL join,
    /// subquery expression): no reordered relations may share a name.
    strict: bool,
    /// A projection passes whole rows: the column order would show.
    frozen: bool,
}

impl OrderGuard {
    fn of(plan: &LogicalPlan) -> Self {
        let mut guard = OrderGuard::default();
        guard.walk_plan(plan);
        guard
    }

    fn walk_plan(&mut self, plan: &LogicalPlan) {
        // Expressions over a projection or an aggregate read its output,
        // whose column order the joins below do not decide.
        let reads_join_rows = |child: &LogicalPlan| {
            !matches!(child, LogicalPlan::Project(_) | LogicalPlan::Aggregate(_))
        };
        match plan {
            LogicalPlan::TableScan(_) | LogicalPlan::CteScan(_) => {}
            LogicalPlan::Filter(f) => {
                // A filter on one table sees that table's columns alone.
                if !matches!(*f.child, LogicalPlan::TableScan(_)) && reads_join_rows(&f.child) {
                    self.walk_pred(&f.predicate);
                }
                self.walk_plan(&f.child);
            }
            LogicalPlan::Project(p) => {
                if p.expressions.is_empty() {
                    self.frozen = true;
                }
                if reads_join_rows(&p.child) {
                    for e in &p.expressions {
                        self.walk_expr(&e.expr);
                    }
                }
                self.walk_plan(&p.child);
            }
            LogicalPlan::Sort(s) => {
                if reads_join_rows(&s.child) {
                    for o in &s.order_by {
                        self.walk_expr(&o.expr);
                    }
                }
                self.walk_plan(&s.child);
            }
            LogicalPlan::Distinct(d) => self.walk_plan(&d.child),
            LogicalPlan::Limit(l) => self.walk_plan(&l.child),
            LogicalPlan::Aggregate(a) => {
                for e in &a.group_by {
                    self.walk_expr(e);
                }
                for e in a.aggregates.iter().flat_map(|agg| &agg.args) {
                    self.walk_expr(e);
                }
                if let Some(h) = &a.having {
                    self.walk_pred(h);
                }
                self.walk_plan(&a.child);
            }
            LogicalPlan::Join(j) => {
                if j.join_type == JoinType::Natural {
                    self.strict = true;
                }
                if let Some(c) = &j.condition {
                    self.walk_pred(c);
                }
                self.walk_plan(&j.left);
                self.walk_plan(&j.right);
            }
            LogicalPlan::SetOp(s) => {
                self.walk_plan(&s.left);
                self.walk_plan(&s.right);
            }
            LogicalPlan::Subquery(sq) => self.walk_plan(&sq.subquery),
            LogicalPlan::Cte(c) => {
                self.walk_plan(&c.inner);
                self.walk_plan(&c.outer);
            }
            LogicalPlan::RecursiveCte(rc) => {
                self.walk_plan(&rc.non_recursive);
                self.walk_plan(&rc.recursive);
                self.walk_plan(&rc.outer);
            }
            LogicalPlan::Insert(i) => self.walk_plan(&i.child),
        }
    }

    fn walk_expr(&mut self, expr: &ExprNode) {
        match expr {
            ExprNode::Column(name) => {
                self.unqualified.insert(name.to_ascii_lowercase());
            }
            ExprNode::Compound(parts) => {
                if let [name] = parts.as_slice() {
                    self.unqualified.insert(name.to_ascii_lowercase());
                }
            }
            ExprNode::Constant(_) => {}
            ExprNode::ScalarSubquery(_) => self.strict = true,
            ExprNode::Binary { left, right, .. }
            | ExprNode::Compare { left, right, .. }
            | ExprNode::Logical { left, right, .. } => {
                self.walk_expr(left);
                self.walk_expr(right);
            }
            ExprNode::Cast { expr, .. }
            | ExprNode::Not(expr)
            | ExprNode::IsNull(expr)
            | ExprNode::IsNotNull(expr) => self.walk_expr(expr),
            ExprNode::Function { args, .. } => {
                for arg in args {
                    if let FunctionArg::Expr(e) = arg {
                        self.walk_expr(e);
                    }
                }
            }
            ExprNode::Case {
                when_then_pairs,
                else_result,
            } => {
                for (when, then) in when_then_pairs {
                    self.walk_expr(when);
                    self.walk_expr(then);
                }
                if let Some(e) = else_result {
                    self.walk_expr(e);
                }
            }
        }
    }

    fn walk_pred(&mut self, pred: &PredicateNode) {
        match pred {
            PredicateNode::BinaryOp { left, right, .. } => {
                self.walk_pred(left);
                self.walk_pred(right);
            }
            PredicateNode::Not(inner) => self.walk_pred(inner),
            PredicateNode::Compare { left, right, .. }
            | PredicateNode::IsDistinctFrom { left, right } => {
                self.walk_expr(left);
                self.walk_expr(right);
            }
            PredicateNode::IsNull(e)
            | PredicateNode::IsNotNull(e)
            | PredicateNode::Like { expr: e, .. }
            | PredicateNode::IsBoolean { expr: e, .. } => self.walk_expr(e),
            PredicateNode::Between { expr, low, high } => {
                self.walk_expr(expr);
                self.walk_expr(low);
                self.walk_expr(high);
            }
            PredicateNode::InList { expr, list } => {
                self.walk_expr(expr);
                for e in list {
                    self.walk_expr(e);
                }
            }
            PredicateNode::Exists(_) => self.strict = true,
            PredicateNode::InSubquery { expr, .. } => {
                self.strict = true;
                self.walk_expr(expr);
            }
        }
    }
}

/// One AND-ed conjunct of a chain's conditions.
struct Conjunct {
    pred: PredicateNode,
    /// Bit `i` set: the conjunct reads relation `i` of the chain.
    rels: u64,
    /// An equality between columns of two disjoint sets of relations.
    equi: bool,
    /// The fraction of the cross product of its relations it keeps, once
    /// the relations are known.
    selectivity: f64,
}

impl Optimizer {
    /// Reorder each chain of inner joins in `plan`. See the module
    /// documentation.
    pub(super) fn join_ordering(&self, plan: LogicalPlan) -> LogicalPlan {
        let guard = OrderGuard::of(&plan);
        if guard.frozen {
            return plan;
        }
        self.order_joins(plan, &guard)
    }

    fn order_joins(&self, plan: LogicalPlan, guard: &OrderGuard) -> LogicalPlan {
        match plan {
            LogicalPlan::Join(j) if in_chain(j.join_type) => {
                self.order_chain(LogicalPlan::Join(j), guard)
            }
            LogicalPlan::Join(j) => LogicalPlan::Join(LogicalJoin {
                left: Box::new(self.order_joins(*j.left, guard)),
                right: Box::new(self.order_joins(*j.right, guard)),
                join_type: j.join_type,
                condition: j.condition,
                using: j.using,
            }),
            LogicalPlan::Filter(f) => LogicalPlan::Filter(LogicalFilter {
                predicate: f.predicate,
                child: Box::new(self.order_joins(*f.child, guard)),
            }),
            LogicalPlan::Project(p) => LogicalPlan::Project(LogicalProject {
                expressions: p.expressions,
                child: Box::new(self.order_joins(*p.child, guard)),
            }),
            LogicalPlan::Sort(s) => LogicalPlan::Sort(LogicalSort {
                order_by: s.order_by,
                child: Box::new(self.order_joins(*s.child, guard)),
                limit: s.limit,
            }),
            LogicalPlan::Distinct(d) => LogicalPlan::Distinct(LogicalDistinct {
                child: Box::new(self.order_joins(*d.child, guard)),
            }),
            LogicalPlan::Limit(l) => LogicalPlan::Limit(LogicalLimit {
                limit: l.limit,
                offset: l.offset,
                child: Box::new(self.order_joins(*l.child, guard)),
            }),
            LogicalPlan::Aggregate(a) => LogicalPlan::Aggregate(LogicalAggregate {
                group_by: a.group_by,
                aggregates: a.aggregates,
                having: a.having,
                child: Box::new(self.order_joins(*a.child, guard)),
            }),
            LogicalPlan::SetOp(s) => LogicalPlan::SetOp(LogicalSetOp {
                op: s.op,
                left: Box::new(self.order_joins(*s.left, guard)),
                right: Box::new(self.order_joins(*s.right, guard)),
                all: s.all,
            }),
            LogicalPlan::Subquery(sq) => LogicalPlan::Subquery(LogicalSubquery {
                subquery: Box::new(self.order_joins(*sq.subquery, guard)),
                alias: sq.alias,
            }),
            LogicalPlan::Cte(c) => LogicalPlan::Cte(LogicalCte {
                name: c.name,
                inner: Box::new(self.order_joins(*c.inner, guard)),
                outer: Box::new(self.order_joins(*c.outer, guard)),
            }),
            LogicalPlan::RecursiveCte(rc) => LogicalPlan::RecursiveCte(LogicalRecursiveCte {
                name: rc.name,
                non_recursive: Box::new(self.order_joins(*rc.non_recursive, guard)),
                recursive: Box::new(self.order_joins(*rc.recursive, guard)),
                union_all: rc.union_all,
                schema: rc.schema,
                outer: Box::new(self.order_joins(*rc.outer, guard)),
            }),
            other => other,
        }
    }

    /// Rebuild the chain rooted at `chain`, or keep it as written when the
    /// guard or its relations forbid reordering.
    fn order_chain(&self, chain: LogicalPlan, guard: &OrderGuard) -> LogicalPlan {
        let mut leaf_refs = Vec::new();
        let mut preds = Vec::new();
        chain_parts(&chain, &mut leaf_refs, &mut preds);
        let Some(conjuncts) = analyse_chain(&leaf_refs, preds, guard) else {
            return self.keep_chain(chain, guard);
        };

        let mut leaves = Vec::new();
        take_leaves(chain, &mut leaves);
        let mut leaves: Vec<Option<LogicalPlan>> = leaves
            .into_iter()
            .map(|leaf| Some(self.order_joins(leaf, guard)))
            .collect();

        // A conjunct on one relation filters it before any join.
        let mut joining = Vec::new();
        for c in conjuncts {
            if c.rels.count_ones() == 1 {
                let i = c.rels.trailing_zeros() as usize;
                let leaf = leaves[i].take().expect("each leaf is taken once");
                leaves[i] = Some(self.push_filter_down(c.pred, leaf));
            } else {
                joining.push(Some(c));
            }
        }

        let estimator = self.estimator();
        let sizes: Vec<f64> = leaves
            .iter()
            .map(|leaf| estimator.rows(leaf.as_ref().expect("leaf present")))
            .collect();
        let inputs: Vec<&LogicalPlan> = leaves
            .iter()
            .map(|leaf| leaf.as_ref().expect("leaf present"))
            .collect();
        for c in joining.iter_mut().flatten() {
            c.selectivity = estimator.conjunct_selectivity(&c.pred, &inputs, &sizes);
        }
        let order = greedy_order(&sizes, &joining);

        let mut joined = 0u64;
        let mut tree: Option<(LogicalPlan, f64)> = None;
        for i in order {
            let before = joined;
            joined |= 1 << i;
            let leaf = leaves[i].take().expect("each leaf is joined once");
            let Some((acc, acc_size)) = tree.take() else {
                tree = Some((leaf, sizes[i]));
                continue;
            };
            let mut condition: Option<PredicateNode> = None;
            for slot in &mut joining {
                if slot.as_ref().is_some_and(|c| c.rels & !joined == 0) {
                    let pred = slot.take().expect("checked above").pred;
                    condition = Some(match condition {
                        Some(existing) => PredicateNode::BinaryOp {
                            left: Box::new(existing),
                            op: rook_ast::BinaryOp::And,
                            right: Box::new(pred),
                        },
                        None => pred,
                    });
                }
            }
            let (left, right) = if sizes[i] < acc_size {
                (leaf, acc)
            } else {
                (acc, leaf)
            };
            let join = LogicalPlan::Join(LogicalJoin {
                left: Box::new(left),
                right: Box::new(right),
                join_type: if condition.is_some() {
                    JoinType::Inner
                } else {
                    JoinType::Cross
                },
                condition,
                using: vec![],
            });
            tree = Some((join, joined_size(acc_size, before, i, &sizes, &joining)));
        }
        tree.expect("a chain has two or more relations").0
    }

    /// The chain as written, with chains inside its relations reordered.
    fn keep_chain(&self, plan: LogicalPlan, guard: &OrderGuard) -> LogicalPlan {
        match plan {
            LogicalPlan::Join(j) if in_chain(j.join_type) => LogicalPlan::Join(LogicalJoin {
                left: Box::new(self.keep_chain(*j.left, guard)),
                right: Box::new(self.keep_chain(*j.right, guard)),
                join_type: j.join_type,
                condition: j.condition,
                using: j.using,
            }),
            other => self.order_joins(other, guard),
        }
    }
}

/// Join types whose inputs may be joined in any order with their
/// neighbours'.
fn in_chain(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::Inner | JoinType::Cross)
}

/// The relations of the chain rooted at `plan`, in written order, and the
/// conjuncts of its conditions.
fn chain_parts<'a>(
    plan: &'a LogicalPlan,
    leaves: &mut Vec<&'a LogicalPlan>,
    conjuncts: &mut Vec<PredicateNode>,
) {
    match plan {
        LogicalPlan::Join(j) if in_chain(j.join_type) => {
            chain_parts(&j.left, leaves, conjuncts);
            chain_parts(&j.right, leaves, conjuncts);
            if let Some(c) = &j.condition {
                split_conjuncts(c.clone(), conjuncts);
            }
        }
        other => leaves.push(other),
    }
}

/// The relations of the chain rooted at `plan`, in the order of
/// [`chain_parts`].
fn take_leaves(plan: LogicalPlan, leaves: &mut Vec<LogicalPlan>) {
    match plan {
        LogicalPlan::Join(j) if in_chain(j.join_type) => {
            take_leaves(*j.left, leaves);
            take_leaves(*j.right, leaves);
        }
        other => leaves.push(other),
    }
}

/// Base tables under filters and joins: the columns' qualifiers are the
/// tables' names or aliases, as the scans stamp them.
fn is_plain(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::TableScan(t) => t.system_table_name.is_none(),
        LogicalPlan::Filter(f) => is_plain(&f.child),
        LogicalPlan::Join(j) => is_plain(&j.left) && is_plain(&j.right),
        _ => false,
    }
}

/// Which relations each conjunct reads, or `None` when the chain must stay
/// as written (see the module documentation).
fn analyse_chain(
    leaves: &[&LogicalPlan],
    preds: Vec<PredicateNode>,
    guard: &OrderGuard,
) -> Option<Vec<Conjunct>> {
    if leaves.len() > 64 || !leaves.iter().all(|l| is_plain(l)) {
        return None;
    }
    // (qualifier, name) of each relation's columns, lower-case.
    let mut columns: Vec<Vec<(String, String)>> = Vec::with_capacity(leaves.len());
    for leaf in leaves {
        let mut cols = Vec::new();
        for (table, col) in derive_qualified_schema(leaf) {
            cols.push((table?.to_ascii_lowercase(), col.name.to_ascii_lowercase()));
        }
        columns.push(cols);
    }

    // One qualifier on two relations would make qualified names ambiguous.
    for (i, a) in columns.iter().enumerate() {
        for b in &columns[i + 1..] {
            if a.iter().any(|(t, _)| b.iter().any(|(u, _)| t == u)) {
                return None;
            }
        }
    }
    // Names two relations have: unqualified, they find whichever comes first.
    for (i, a) in columns.iter().enumerate() {
        for (_, name) in a {
            let shared = columns[i + 1..]
                .iter()
                .any(|b| b.iter().any(|(_, c)| c == name));
            if shared && (guard.strict || guard.unqualified.contains(name)) {
                return None;
            }
        }
    }

    let rels_of = |refs: Vec<(Option<String>, String)>| -> Option<u64> {
        let mut rels = 0u64;
        for (table, name) in refs {
            let table = table.map(|t| t.to_ascii_lowercase());
            let name = name.to_ascii_lowercase();
            let mut found = columns.iter().enumerate().filter(|(_, cols)| {
                cols.iter()
                    .any(|(t, c)| *c == name && table.as_ref().is_none_or(|q| q == t))
            });
            let (i, _) = found.next()?;
            if found.next().is_some() {
                return None;
            }
            rels |= 1 << i;
        }
        Some(rels)
    };

    preds
        .into_iter()
        .map(|pred| {
            let mut probe = OrderGuard::default();
            probe.walk_pred(&pred);
            if probe.strict {
                return None;
            }
            let rels = rels_of(extract_column_refs(&pred))?;
            let equi = match &pred {
                PredicateNode::Compare {
                    left,
                    op: ComparisonOp::Eq,
                    right,
                } => {
                    let l = rels_of(extract_expr_column_refs(left))?;
                    let r = rels_of(extract_expr_column_refs(right))?;
                    l != 0 && r != 0 && l & r == 0
                }
                _ => false,
            };
            Some(Conjunct {
                pred,
                rels,
                equi,
                selectivity: 1.0,
            })
        })
        .collect()
}

/// Estimated rows of `size` rows (the relations in `joined`) joined to
/// relation `next`: the cross product, less what each condition that
/// reads `next` and what is joined already keeps.
fn joined_size(
    size: f64,
    joined: u64,
    next: usize,
    sizes: &[f64],
    conjuncts: &[Option<Conjunct>],
) -> f64 {
    let bit = 1u64 << next;
    let kept: f64 = conjuncts
        .iter()
        .flatten()
        .filter(|c| c.rels & bit != 0 && c.rels & !(joined | bit) == 0)
        .map(|c| c.selectivity)
        .product();
    size * sizes[next] * kept
}

/// The order to join relations of estimated `sizes` in, given the
/// conjuncts that join them. Ties go to the relation written first.
fn greedy_order(sizes: &[f64], conjuncts: &[Option<Conjunct>]) -> Vec<usize> {
    let n = sizes.len();
    let Some(first) = (0..n).min_by(|&a, &b| sizes[a].total_cmp(&sizes[b])) else {
        return Vec::new();
    };
    let mut order = vec![first];
    let mut joined = 1u64 << first;
    let mut size = sizes[first];
    while order.len() < n {
        // 0: an equality joins it to what is joined; 1: another condition;
        // 2: nothing (a cross product).
        let link = |r: usize| {
            let bit = 1u64 << r;
            let mut links = conjuncts.iter().flatten().filter(|c| {
                c.rels & bit != 0 && c.rels & joined != 0 && c.rels & !(joined | bit) == 0
            });
            match links.next() {
                Some(c) if c.equi || links.any(|c| c.equi) => 0,
                Some(_) => 1,
                None => 2,
            }
        };
        let next = (0..n)
            .filter(|r| joined & (1 << r) == 0)
            .min_by(|&a, &b| {
                link(a).cmp(&link(b)).then(
                    joined_size(size, joined, a, sizes, conjuncts)
                        .total_cmp(&joined_size(size, joined, b, sizes, conjuncts)),
                )
            })
            .expect("a relation is left to join");
        size = joined_size(size, joined, next, sizes, conjuncts);
        joined |= 1 << next;
        order.push(next);
    }
    order
}
