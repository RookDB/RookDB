//! Name handling for statements: table-name resolution, table references in
//! stored views, and table-level constraint definitions.

use rook_ast::QueryPlan;

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
        QueryPlan::CreateIndex(p) => resolve(&mut p.table_name),
        QueryPlan::DropIndex(p) => resolve(&mut p.table_name),
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

/// Parse a table-level constraint definition string and save it to `sys_constraints`.
///
/// Handles definitions produced by sqlparser such as:
///   "PRIMARY KEY (id)", "UNIQUE (name)", "UNIQUE",
///   "FOREIGN KEY (user_id) REFERENCES users(id)",
///   "CHECK (age > 0)"
pub(super) fn save_table_constraint(
    engine: &crate::backend::engine::Engine,
    db: &str,
    table: &str,
    definition: &str,
) {
    use crate::backend::system_table::insert_constraint_metadata;

    // ASCII-only folding: keyword matching needs nothing beyond A-Z, and it
    // guarantees `upper.len() == definition.len()` so offsets into the
    // original string stay valid (see the FOREIGN KEY arm below).
    let upper = definition.to_ascii_uppercase();

    // PRIMARY KEY (cols...)
    if let Some(rest) = upper.strip_prefix("PRIMARY KEY (") {
        if let Some(end) = rest.rfind(')') {
            let cols = rest[..end].to_string();
            let _ = insert_constraint_metadata(engine, db, table, "PRIMARY KEY", &cols, None, None);
        }
    }
    // UNIQUE (cols...)
    else if let Some(rest) = upper.strip_prefix("UNIQUE (") {
        if let Some(end) = rest.rfind(')') {
            let cols = rest[..end].to_string();
            let _ = insert_constraint_metadata(engine, db, table, "UNIQUE", &cols, None, None);
        }
    }
    // UNIQUE without columns (table-level shorthand)
    else if upper == "UNIQUE" {
        let _ = insert_constraint_metadata(engine, db, table, "UNIQUE", "", None, None);
    }
    // FOREIGN KEY (cols) REFERENCES ref_table(ref_cols) [ON DELETE CASCADE|RESTRICT]
    //
    // Keywords are matched on an ASCII-uppercased copy, but identifiers are
    // sliced from the ORIGINAL definition: ref_table/ref_cols feed
    // case-sensitive file and catalog lookups during enforcement, so their
    // letter-case must survive verbatim.
    else if upper.starts_with("FOREIGN KEY (") {
        // to_ascii_uppercase is length-preserving by construction, so
        // offsets from `upper` always address the same characters in
        // `definition` (plain to_uppercase is NOT: 'ß' → "SS" would shift
        // every later offset).
        let off = "FOREIGN KEY (".len();
        let rest_u = &upper[off..];
        if let Some(end_paren) = rest_u.find(')') {
            let cols = definition[off..off + end_paren].to_string();
            let after_paren = definition[off + end_paren + 1..].trim_start();
            if let Some(ref_rest) = after_paren.strip_prefix("REFERENCES ") {
                if let Some(ref_start) = ref_rest.find('(') {
                    let ref_table_name = ref_rest[..ref_start].trim().to_string();
                    let ref_cols_rest = &ref_rest[ref_start + 1..];
                    if let Some(ref_end) = ref_cols_rest.rfind(')') {
                        let ref_cols = ref_cols_rest[..ref_end].to_string();
                        let remaining_after_ref = ref_cols_rest[ref_end + 1..].trim();
                        let mut delete_action = "";
                        let mut update_action = "";
                        let mut rem = remaining_after_ref.to_uppercase();
                        if let Some(after) = rem.strip_prefix("ON DELETE ") {
                            if after.starts_with("CASCADE") {
                                delete_action = " ON DELETE CASCADE";
                            } else if after.starts_with("SET NULL") {
                                delete_action = " ON DELETE SET NULL";
                            }
                            let skip_to = after
                                .find(|c: char| c.is_whitespace())
                                .unwrap_or(after.len());
                            rem = after[skip_to..].trim().to_string();
                        }
                        if let Some(after) = rem.strip_prefix("ON UPDATE ") {
                            if after.starts_with("CASCADE") {
                                update_action = " ON UPDATE CASCADE";
                            } else if after.starts_with("SET NULL") {
                                update_action = " ON UPDATE SET NULL";
                            }
                        }
                        let constr_type = format!("FOREIGN KEY{}{}", delete_action, update_action);
                        let _ = insert_constraint_metadata(
                            engine,
                            db,
                            table,
                            &constr_type,
                            &cols,
                            Some(&ref_table_name),
                            Some(&ref_cols),
                        );
                    }
                } else {
                    let ref_table_name = ref_rest
                        .split_whitespace()
                        .next()
                        .unwrap_or(ref_rest)
                        .to_string();
                    let _ = insert_constraint_metadata(
                        engine,
                        db,
                        table,
                        "FOREIGN KEY",
                        &cols,
                        Some(&ref_table_name),
                        Some(""),
                    );
                }
            }
        }
    }
    // CHECK (expr)
    else if upper.starts_with("CHECK (") || upper.starts_with("CHECK(") {
        // Offset of the opening paren: `find_matching_paren` needs it.
        let check_start = if upper.starts_with("CHECK(") { 5 } else { 6 };
        let remaining = &definition[check_start..];
        if let Some(end) = find_matching_paren(remaining) {
            let expr = remaining[1..end].trim();
            if !expr.is_empty() {
                let _ = insert_constraint_metadata(engine, db, table, "CHECK", expr, None, None);
            }
        }
    }
}

/// Find the index of the matching close-paren for a string that starts with `(...)`.
/// Properly handles nested parentheses by counting depth.
fn find_matching_paren(s: &str) -> Option<usize> {
    if !s.starts_with('(') {
        return None;
    }
    let mut depth: i32 = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Strip redundant parentheses wrapping a whole expression: `('abc')` -> `'abc'`.
pub(super) fn strip_outer_parens(expr: &str) -> &str {
    let mut expr = expr.trim();
    while expr.starts_with('(') && find_matching_paren(expr) == Some(expr.len() - 1) {
        expr = expr[1..expr.len() - 1].trim();
    }
    expr
}
