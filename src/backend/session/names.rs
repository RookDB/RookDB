//! Name handling for statements: table-name resolution, table references in
//! stored views, and table-level constraints.

use rook_ast::{QueryPlan, ReferentialAction, TableConstraint};

use crate::catalog::Catalog;

/// Identifiers are case-insensitive: point each statement's target table at
/// the spelling it was created with (`DELETE FROM T` reaches table `t`).
/// SELECT sources are resolved the same way by the planner.
pub(super) fn resolve_table_names(
    mut plan: QueryPlan,
    catalog: &Catalog,
    current_db: &Option<String>,
) -> QueryPlan {
    let Some(db) = current_db.as_ref().and_then(|d| catalog.databases.get(d)) else {
        return plan;
    };
    let resolve = |name: &mut String| {
        if !db.tables.contains_key(name.as_str()) {
            let mut matches = db.tables.keys().filter(|t| t.eq_ignore_ascii_case(name));
            if let (Some(stored), None) = (matches.next(), matches.next()) {
                *name = stored.clone();
            }
        }
    };
    match &mut plan {
        QueryPlan::Insert(p) => resolve(&mut p.table),
        QueryPlan::Update(p) => resolve(&mut p.table),
        QueryPlan::Delete(p) => resolve(&mut p.table),
        QueryPlan::DropTable(p) => resolve(&mut p.table),
        QueryPlan::Truncate(p) => resolve(&mut p.table),
        QueryPlan::AlterTable(p) => resolve(&mut p.table),
        QueryPlan::Vacuum(p) => resolve(&mut p.table),
        QueryPlan::Analyze(p) => {
            if let Some(table) = p.table.as_mut() {
                resolve(table)
            }
        }
        QueryPlan::CreateIndex(p) => resolve(&mut p.table_name),
        QueryPlan::DropIndex(p) => resolve(&mut p.table_name),
        QueryPlan::Explain(p) => {
            let inner = std::mem::replace(&mut *p.statement, QueryPlan::ShowTables);
            *p.statement = resolve_table_names(inner, catalog, current_db);
        }
        _ => {}
    }
    plan
}

/// Whether a stored view's query reads `table`.
pub(super) fn select_plan_references_table(plan: &rook_ast::SelectPlan, table: &str) -> bool {
    // Check FROM
    for tr in &plan.from {
        if tr.name.eq_ignore_ascii_case(table) {
            return true;
        }
    }
    // Check JOINs
    for jc in &plan.joins {
        if jc.relation.name.eq_ignore_ascii_case(table) {
            return true;
        }
    }
    // Check CTEs
    for cte in &plan.ctes {
        if select_plan_references_table(&cte.query, table) {
            return true;
        }
        if let Some(rt) = &cte.recursive_term
            && select_plan_references_table(rt, table)
        {
            return true;
        }
    }
    // Check projections (ScalarSubquery)
    for proj in &plan.projections {
        match proj {
            rook_ast::SelectExpr::UnnamedExpr(expr)
            | rook_ast::SelectExpr::ExprWithAlias { expr, .. } => {
                if expr_references_table(expr, table) {
                    return true;
                }
            }
            _ => {}
        }
    }
    // Check selection (WHERE)
    if let Some(sel) = &plan.selection
        && predicate_node_references_table(sel, table)
    {
        return true;
    }
    // Check HAVING
    if let Some(hav) = &plan.having
        && predicate_node_references_table(hav, table)
    {
        return true;
    }
    false
}

fn expr_references_table(expr: &rook_ast::ExprNode, table: &str) -> bool {
    match expr {
        // An unqualified column name does not name a table.
        rook_ast::ExprNode::Column(_) => false,
        rook_ast::ExprNode::Compound(parts) => {
            parts.len() >= 2 && parts[parts.len() - 2].eq_ignore_ascii_case(table)
        }
        rook_ast::ExprNode::Binary { left, right, .. } => {
            expr_references_table(left, table) || expr_references_table(right, table)
        }
        rook_ast::ExprNode::Cast { expr: inner, .. } => expr_references_table(inner, table),
        rook_ast::ExprNode::Case {
            when_then_pairs,
            else_result,
        } => {
            when_then_pairs
                .iter()
                .any(|(w, t)| expr_references_table(w, table) || expr_references_table(t, table))
                || else_result
                    .as_ref()
                    .is_some_and(|el| expr_references_table(el, table))
        }
        rook_ast::ExprNode::Function { args, .. } => args.iter().any(|arg| {
            matches!(arg, rook_ast::FunctionArg::Expr(inner) if expr_references_table(inner, table))
        }),
        rook_ast::ExprNode::ScalarSubquery(info) => {
            select_plan_references_table(&info.select, table)
        }
        _ => false,
    }
}

fn predicate_node_references_table(pred: &rook_ast::PredicateNode, table: &str) -> bool {
    use rook_ast::PredicateNode as P;
    match pred {
        P::BinaryOp { left, right, .. } => {
            predicate_node_references_table(left, table)
                || predicate_node_references_table(right, table)
        }
        P::Not(inner) => predicate_node_references_table(inner, table),
        P::Compare { left, right, .. } | P::IsDistinctFrom { left, right } => {
            expr_references_table(left, table) || expr_references_table(right, table)
        }
        P::IsNull(expr) | P::IsNotNull(expr) => expr_references_table(expr, table),
        P::Between { expr, low, high } => {
            expr_references_table(expr, table)
                || expr_references_table(low, table)
                || expr_references_table(high, table)
        }
        P::InList { expr, list } => {
            expr_references_table(expr, table)
                || list.iter().any(|item| expr_references_table(item, table))
        }
        P::Like { expr, .. } | P::IsBoolean { expr, .. } => expr_references_table(expr, table),
        P::Exists(info) => select_plan_references_table(&info.select, table),
        P::InSubquery { expr, subquery, .. } => {
            expr_references_table(expr, table)
                || select_plan_references_table(&subquery.select, table)
        }
    }
}

/// Save a table-level constraint to `sys_constraints`.
pub(super) fn save_table_constraint(
    engine: &crate::backend::engine::Engine,
    db: &str,
    table: &str,
    constraint: &TableConstraint,
) {
    use crate::backend::system_table::insert_constraint_metadata;

    let join = crate::backend::system_table::join_column_list;
    let action = |keyword: &str, a: ReferentialAction| match a {
        ReferentialAction::Cascade => format!(" {} CASCADE", keyword),
        ReferentialAction::SetNull => format!(" {} SET NULL", keyword),
        ReferentialAction::NoAction | ReferentialAction::Restrict => String::new(),
    };
    let _ = match constraint {
        TableConstraint::PrimaryKey { columns } => {
            insert_constraint_metadata(engine, db, table, "PRIMARY KEY", &join(columns), None, None)
        }
        TableConstraint::Unique { columns } => {
            insert_constraint_metadata(engine, db, table, "UNIQUE", &join(columns), None, None)
        }
        TableConstraint::ForeignKey {
            columns,
            ref_table,
            ref_columns,
            on_delete,
            on_update,
        } => {
            let kind = format!(
                "FOREIGN KEY{}{}",
                action("ON DELETE", *on_delete),
                action("ON UPDATE", *on_update)
            );
            insert_constraint_metadata(
                engine,
                db,
                table,
                &kind,
                &join(columns),
                Some(ref_table),
                Some(&join(ref_columns)),
            )
        }
        TableConstraint::Check(expr) => {
            insert_constraint_metadata(engine, db, table, "CHECK", expr, None, None)
        }
    };
}
