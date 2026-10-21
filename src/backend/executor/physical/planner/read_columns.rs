//! The columns a query reads, so a table scan can decode only those.
//!
//! The answer is either the set of column names the plan mentions anywhere,
//! or `None`: "may read every column". `None` is the answer whenever the
//! plan can see a column without naming it, namely when
//!
//! * a table's rows leave the plan, or reach DISTINCT, a set operation or
//!   an INSERT, without going through a projection or an aggregate (these
//!   pass or compare whole rows);
//! * it contains a CTE, a subquery in an expression, or a NATURAL join
//!   (whose condition is derived from the columns the tables share);
//! * an expression names a wildcard (`t.*`).
//!
//! For a plan with an answer, [`join_outputs`] narrows it per join: the
//! columns read above each join, which are all the join needs to output.
//!
//! The expression walkers match every node type with no catch-all arm, so
//! a new kind of node must be handled here before the planner compiles.

use std::collections::{HashMap, HashSet};

use rook_ast::logical::LogicalPlan;
use rook_ast::{ExprNode, FunctionArg, PredicateNode};

/// Lower-case names of the columns `plan` reads, or `None` when it may read
/// columns it does not name. See the module documentation.
pub(super) fn read_columns(plan: &LogicalPlan) -> Option<HashSet<String>> {
    whole_rows_are_projected(plan)?;
    let mut names = HashSet::new();
    walk_plan(plan, &mut names)?;
    Some(names)
}

/// For each join in `plan` (by address), the lower-case names of the
/// columns the operators above it read. Its own condition is not among
/// them: the join evaluates that on its full rows. Only meaningful when
/// [`read_columns`] has an answer for `plan`; `None` when a name cannot be
/// placed.
pub(super) fn join_outputs(plan: &LogicalPlan) -> Option<HashMap<usize, HashSet<String>>> {
    let mut out = HashMap::new();
    walk_above(plan, &HashSet::new(), &mut out)?;
    Some(out)
}

/// Record, for each join under `plan`, `above` plus the names read between
/// it and `plan`.
fn walk_above(
    plan: &LogicalPlan,
    above: &HashSet<String>,
    out: &mut HashMap<usize, HashSet<String>>,
) -> Option<()> {
    let mut names = above.clone();
    match plan {
        LogicalPlan::TableScan(_) => Some(()),
        LogicalPlan::Filter(f) => {
            walk_predicate(&f.predicate, &mut names)?;
            walk_above(&f.child, &names, out)
        }
        LogicalPlan::Project(p) => {
            for e in &p.expressions {
                walk_expr(&e.expr, &mut names)?;
            }
            walk_above(&p.child, &names, out)
        }
        LogicalPlan::Distinct(d) => walk_above(&d.child, &names, out),
        LogicalPlan::Sort(s) => {
            for o in &s.order_by {
                walk_expr(&o.expr, &mut names)?;
            }
            walk_above(&s.child, &names, out)
        }
        LogicalPlan::Limit(l) => walk_above(&l.child, &names, out),
        LogicalPlan::Aggregate(a) => {
            for e in &a.group_by {
                walk_expr(e, &mut names)?;
            }
            for agg in &a.aggregates {
                for e in &agg.args {
                    walk_expr(e, &mut names)?;
                }
            }
            if let Some(h) = &a.having {
                walk_predicate(h, &mut names)?;
            }
            walk_above(&a.child, &names, out)
        }
        LogicalPlan::Join(j) => {
            // The node's own address, as the planner sees it (not a box's).
            let node: &rook_ast::logical::LogicalJoin = j;
            out.insert(node as *const _ as usize, above.clone());
            if let Some(c) = &j.condition {
                walk_predicate(c, &mut names)?;
            }
            walk_above(&j.left, &names, out)?;
            walk_above(&j.right, &names, out)
        }
        LogicalPlan::SetOp(s) => {
            walk_above(&s.left, &names, out)?;
            walk_above(&s.right, &names, out)
        }
        LogicalPlan::Subquery(s) => walk_above(&s.subquery, &names, out),
        LogicalPlan::Insert(i) => {
            for row in &i.values_rows {
                for e in row {
                    walk_expr(e, &mut names)?;
                }
            }
            walk_above(&i.child, &names, out)
        }
        LogicalPlan::Cte(_) | LogicalPlan::RecursiveCte(_) | LogicalPlan::CteScan(_) => None,
    }
}

/// `Some` when every row `plan` outputs was built by a projection or an
/// aggregate, never a table row passed through whole.
fn whole_rows_are_projected(plan: &LogicalPlan) -> Option<()> {
    match plan {
        LogicalPlan::Project(_) | LogicalPlan::Aggregate(_) => Some(()),
        LogicalPlan::Filter(f) => whole_rows_are_projected(&f.child),
        LogicalPlan::Sort(s) => whole_rows_are_projected(&s.child),
        LogicalPlan::Limit(l) => whole_rows_are_projected(&l.child),
        LogicalPlan::Distinct(d) => whole_rows_are_projected(&d.child),
        LogicalPlan::Subquery(s) => whole_rows_are_projected(&s.subquery),
        LogicalPlan::Join(j) => {
            whole_rows_are_projected(&j.left)?;
            whole_rows_are_projected(&j.right)
        }
        LogicalPlan::SetOp(s) => {
            whole_rows_are_projected(&s.left)?;
            whole_rows_are_projected(&s.right)
        }
        LogicalPlan::Insert(i) => whole_rows_are_projected(&i.child),
        LogicalPlan::TableScan(_)
        | LogicalPlan::Cte(_)
        | LogicalPlan::RecursiveCte(_)
        | LogicalPlan::CteScan(_) => None,
    }
}

fn walk_plan(plan: &LogicalPlan, names: &mut HashSet<String>) -> Option<()> {
    match plan {
        LogicalPlan::TableScan(_) => Some(()),
        LogicalPlan::Filter(f) => {
            walk_predicate(&f.predicate, names)?;
            walk_plan(&f.child, names)
        }
        LogicalPlan::Project(p) => {
            for e in &p.expressions {
                walk_expr(&e.expr, names)?;
            }
            walk_plan(&p.child, names)
        }
        LogicalPlan::Distinct(d) => {
            whole_rows_are_projected(&d.child)?;
            walk_plan(&d.child, names)
        }
        LogicalPlan::Sort(s) => {
            for o in &s.order_by {
                walk_expr(&o.expr, names)?;
            }
            walk_plan(&s.child, names)
        }
        LogicalPlan::Limit(l) => walk_plan(&l.child, names),
        LogicalPlan::Aggregate(a) => {
            for e in &a.group_by {
                walk_expr(e, names)?;
            }
            for agg in &a.aggregates {
                for e in &agg.args {
                    walk_expr(e, names)?;
                }
            }
            if let Some(h) = &a.having {
                walk_predicate(h, names)?;
            }
            walk_plan(&a.child, names)
        }
        LogicalPlan::Join(j) => {
            if j.join_type == rook_ast::JoinType::Natural {
                return None;
            }
            if let Some(c) = &j.condition {
                walk_predicate(c, names)?;
            }
            walk_plan(&j.left, names)?;
            walk_plan(&j.right, names)
        }
        LogicalPlan::SetOp(s) => {
            whole_rows_are_projected(&s.left)?;
            whole_rows_are_projected(&s.right)?;
            walk_plan(&s.left, names)?;
            walk_plan(&s.right, names)
        }
        LogicalPlan::Subquery(s) => walk_plan(&s.subquery, names),
        LogicalPlan::Insert(i) => {
            whole_rows_are_projected(&i.child)?;
            for row in &i.values_rows {
                for e in row {
                    walk_expr(e, names)?;
                }
            }
            walk_plan(&i.child, names)
        }
        LogicalPlan::Cte(_) | LogicalPlan::RecursiveCte(_) | LogicalPlan::CteScan(_) => None,
    }
}

fn walk_expr(expr: &ExprNode, names: &mut HashSet<String>) -> Option<()> {
    match expr {
        ExprNode::Column(name) => add(name, names),
        ExprNode::Compound(parts) => add(parts.last()?, names),
        ExprNode::Constant(_) => Some(()),
        ExprNode::Binary { left, right, .. }
        | ExprNode::Compare { left, right, .. }
        | ExprNode::Logical { left, right, .. } => {
            walk_expr(left, names)?;
            walk_expr(right, names)
        }
        ExprNode::Cast { expr, .. }
        | ExprNode::Not(expr)
        | ExprNode::IsNull(expr)
        | ExprNode::IsNotNull(expr) => walk_expr(expr, names),
        ExprNode::Function { args, .. } => {
            for arg in args {
                match arg {
                    FunctionArg::Star => {}
                    FunctionArg::Expr(e) => walk_expr(e, names)?,
                }
            }
            Some(())
        }
        ExprNode::Case {
            when_then_pairs,
            else_result,
        } => {
            for (when, then) in when_then_pairs {
                walk_expr(when, names)?;
                walk_expr(then, names)?;
            }
            match else_result {
                Some(e) => walk_expr(e, names),
                None => Some(()),
            }
        }
        ExprNode::ScalarSubquery(_) => None,
        ExprNode::Predicate(p) => walk_predicate(p, names),
    }
}

fn walk_predicate(pred: &PredicateNode, names: &mut HashSet<String>) -> Option<()> {
    match pred {
        PredicateNode::BinaryOp { left, right, .. } => {
            walk_predicate(left, names)?;
            walk_predicate(right, names)
        }
        PredicateNode::Not(inner) => walk_predicate(inner, names),
        PredicateNode::Compare { left, right, .. }
        | PredicateNode::IsDistinctFrom { left, right } => {
            walk_expr(left, names)?;
            walk_expr(right, names)
        }
        PredicateNode::IsNull(e)
        | PredicateNode::IsNotNull(e)
        | PredicateNode::Like { expr: e, .. }
        | PredicateNode::IsBoolean { expr: e, .. } => walk_expr(e, names),
        PredicateNode::Between { expr, low, high } => {
            walk_expr(expr, names)?;
            walk_expr(low, names)?;
            walk_expr(high, names)
        }
        PredicateNode::InList { expr, list } => {
            walk_expr(expr, names)?;
            for e in list {
                walk_expr(e, names)?;
            }
            Some(())
        }
        PredicateNode::Exists(_) | PredicateNode::InSubquery { .. } => None,
    }
}

/// Record a column name; a wildcard means every column.
fn add(name: &str, names: &mut HashSet<String>) -> Option<()> {
    if name == "*" {
        return None;
    }
    names.insert(name.to_ascii_lowercase());
    Some(())
}
