//! Execution Engine — Drives a `PhysicalOperator` tree and collects / displays results.
//!
//! The engine handles the top-level orchestration: creating the planner,
//! building the operator tree, pulling all tuples from the root operator,
//! and formatting the output for the CLI.

use rook_ast::logical::LogicalPlan;

use super::explain::PlanDescription;
use super::planner::PhysicalPlanner;
use super::tuple::{Tuple, display_tuples};

use crate::backend::catalog::types::Catalog;
use crate::backend::error::RookResult;

/// Execute a logical plan using the Volcano engine and display the results.
///
/// Returns the number of tuples produced.
pub fn execute_plan(plan: &LogicalPlan, catalog: &Catalog, db_name: &str) -> RookResult<usize> {
    log::debug!("[Volcano] Plan: {:?}", plan);

    // 1. Create physical planner
    let planner = PhysicalPlanner::new(catalog.clone(), db_name.to_string());

    // 2. Convert logical plan → physical operator tree
    let mut root = planner.plan(plan)?;

    // 3. Pull all tuples from the root operator
    let mut tuples: Vec<Tuple> = Vec::new();
    let mut batch: Vec<Tuple> = Vec::with_capacity(super::operators::DEFAULT_BATCH_SIZE);
    while root.next_batch(&mut batch)? > 0 {
        tuples.append(&mut batch);
    }

    // 4. Display results
    let schema = root.schema().to_vec();
    display_tuples(&tuples, &schema);

    Ok(tuples.len())
}

/// Execute a plan and collect tuples along with their output schema.
pub fn execute_plan_collect_with_schema(
    plan: &LogicalPlan,
    catalog: &Catalog,
    db_name: &str,
) -> RookResult<(Vec<Tuple>, Vec<super::tuple::ColumnInfo>)> {
    execute_shared(plan, std::sync::Arc::new(catalog.clone()), db_name)
}

/// [`execute_plan_collect_with_schema`] for a caller that already holds the
/// catalog in an `Arc`: the plan shares it instead of copying it.
pub fn execute_shared(
    plan: &LogicalPlan,
    catalog: std::sync::Arc<Catalog>,
    db_name: &str,
) -> RookResult<(Vec<Tuple>, Vec<super::tuple::ColumnInfo>)> {
    let planner = PhysicalPlanner::new(catalog, db_name.to_string());
    let mut root = planner.plan(plan)?;
    let schema = root.schema().to_vec();

    let mut tuples: Vec<Tuple> = Vec::new();
    let mut batch: Vec<Tuple> = Vec::with_capacity(super::operators::DEFAULT_BATCH_SIZE);
    while root.next_batch(&mut batch)? > 0 {
        tuples.append(&mut batch);
    }

    Ok((tuples, schema))
}

/// Plan `plan` without running it and describe the physical plan: the
/// operator tree and the decisions made for it (what `EXPLAIN` shows).
///
/// CTEs and uncorrelated subqueries are evaluated while planning, as they
/// are for execution; the statement's own operators do not run.
pub fn explain_shared(
    plan: &LogicalPlan,
    catalog: std::sync::Arc<Catalog>,
    db_name: &str,
) -> RookResult<PlanDescription> {
    let planner = PhysicalPlanner::new(catalog, db_name.to_string());
    let (root, trace) = planner.plan_traced(plan)?;
    Ok(PlanDescription {
        root: root.explain(),
        trace,
    })
}

/// Run `plan` to the end, then describe the physical plan, including what
/// only running it tells (`EXPLAIN ANALYZE`): whether a sort or a hash join
/// went to disk. Also returns the number of rows produced.
pub fn explain_analyze_shared(
    plan: &LogicalPlan,
    catalog: std::sync::Arc<Catalog>,
    db_name: &str,
) -> RookResult<(PlanDescription, usize)> {
    let planner = PhysicalPlanner::new(catalog, db_name.to_string());
    let (mut root, trace) = planner.plan_traced(plan)?;
    let mut rows = 0;
    let mut batch: Vec<Tuple> = Vec::with_capacity(super::operators::DEFAULT_BATCH_SIZE);
    loop {
        let n = root.next_batch(&mut batch)?;
        if n == 0 {
            break;
        }
        rows += n;
    }
    let description = PlanDescription {
        root: root.explain(),
        trace,
    };
    Ok((description, rows))
}

/// Execute a plan and collect tuples without displaying (useful for testing).
pub fn execute_plan_collect(
    plan: &LogicalPlan,
    catalog: &Catalog,
    db_name: &str,
) -> RookResult<Vec<Tuple>> {
    execute_plan_collect_with_schema(plan, catalog, db_name).map(|(tuples, _)| tuples)
}

/// Pretty-print the operator plan tree (for debugging).
pub fn print_plan_tree(plan: &LogicalPlan) {
    println!("=== Physical Plan Tree ===");
    print_plan_recursive(plan, 0);
    println!("==========================");
}

fn print_plan_recursive(plan: &LogicalPlan, depth: usize) {
    let indent = "  ".repeat(depth);
    match plan {
        LogicalPlan::TableScan(ts) => {
            println!("{}└─ SeqScan on {}", indent, ts.table);
        }
        LogicalPlan::Filter(f) => {
            println!("{}├─ Filter", indent);
            print_plan_recursive(&f.child, depth + 1);
        }
        LogicalPlan::Project(p) => {
            let names: Vec<&str> = p.expressions.iter().map(|ne| ne.name.as_str()).collect();
            println!("{}├─ Project ({})", indent, names.join(", "));
            print_plan_recursive(&p.child, depth + 1);
        }
        LogicalPlan::Distinct(d) => {
            println!("{}├─ Distinct", indent);
            print_plan_recursive(&d.child, depth + 1);
        }
        LogicalPlan::Sort(s) => {
            let keys: Vec<String> = s
                .order_by
                .iter()
                .map(|ob| {
                    let col = format!("{:?}", ob.expr);
                    let dir = if ob.ascending { " ASC" } else { " DESC" };
                    format!("{}{}", col, dir)
                })
                .collect();
            println!("{}├─ Sort ({})", indent, keys.join(", "));
            print_plan_recursive(&s.child, depth + 1);
        }
        LogicalPlan::Limit(l) => {
            println!("{}├─ Limit {} OFFSET {}", indent, l.limit, l.offset);
            print_plan_recursive(&l.child, depth + 1);
        }
        LogicalPlan::Aggregate(a) => {
            let gb_cols = a.group_by.len();
            let agg_count = a.aggregates.len();
            println!(
                "{}├─ Aggregate (group_by={}, aggregates={})",
                indent, gb_cols, agg_count
            );
            print_plan_recursive(&a.child, depth + 1);
        }
        LogicalPlan::Join(j) => {
            let join_type = format!("{:?}", j.join_type);
            let has_cond = if j.condition.is_some() { "ON" } else { "none" };
            println!("{}├─ {} Join ({})", indent, join_type, has_cond);
            print_plan_recursive(&j.left, depth + 1);
            print_plan_recursive(&j.right, depth + 1);
        }
        LogicalPlan::SetOp(s) => {
            println!("{}├─ SetOp {:?}", indent, s.op);
            print_plan_recursive(&s.left, depth + 1);
            print_plan_recursive(&s.right, depth + 1);
        }
        LogicalPlan::Subquery(sq) => {
            println!("{}├─ Subquery", indent);
            print_plan_recursive(&sq.subquery, depth + 1);
        }
        LogicalPlan::Cte(c) => {
            println!("{}├─ CTE '{}'", indent, c.name);
            print_plan_recursive(&c.inner, depth + 1);
            print_plan_recursive(&c.outer, depth + 1);
        }
        LogicalPlan::RecursiveCte(rc) => {
            let op = if rc.union_all { "UNION ALL" } else { "UNION" };
            println!("{}├─ RecursiveCTE '{}' ({})", indent, rc.name, op);
            println!("{}    non_recursive:", indent);
            print_plan_recursive(&rc.non_recursive, depth + 2);
            println!("{}    recursive:", indent);
            print_plan_recursive(&rc.recursive, depth + 2);
            println!("{}    outer:", indent);
            print_plan_recursive(&rc.outer, depth + 2);
        }
        LogicalPlan::CteScan(cs) => {
            println!("{}└─ CteScan on '{}'", indent, cs.name);
        }
        LogicalPlan::Insert(inp) => {
            println!("{}├─ Insert into '{}'", indent, inp.table);
            print_plan_recursive(&inp.child, depth + 1);
        }
    }
}
