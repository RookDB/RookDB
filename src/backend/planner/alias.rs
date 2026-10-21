//! Table-alias rewriting for SELECT planning.
//!
//! SQL allows qualifying columns with FROM/JOIN aliases:
//!
//! ```sql
//! SELECT e.name FROM employees AS e JOIN departments AS d ON e.dept_id = d.id
//! ```
//!
//! Scan operators stamp tuple columns with the *real* table name, so a
//! qualified reference carrying an alias fails at evaluation time
//! ("Column 'name' not found in tuple schema").  The fix per the roadmap:
//! build an alias→table mapping during logical planning and rewrite every
//! compound identifier's qualifier before plan construction, so downstream
//! stages only ever see resolvable table names.

use std::collections::HashMap;

use rook_ast::{ExprNode, FunctionArg, JoinClause, PredicateNode, SelectExpr, SelectPlan};

use crate::catalog::Database;

/// Build an alias→canonical-name map from a SELECT's FROM/JOIN clauses.
///
/// Keys are lowercase qualifiers; values are the table names that scan
/// operators stamp into `ColumnInfo::table`.  Includes identity entries for
/// real table names so explicit qualification keeps working alongside aliases.
pub fn build_alias_map(select: &SelectPlan, db: &Database) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let mut trefs: Vec<&rook_ast::TableRef> = select.from.iter().collect();
    trefs.extend(select.joins.iter().map(|j| &j.relation));

    for tref in trefs {
        // CTE scans stamp no table qualifier — nothing useful to map.
        if tref.name.starts_with("__cte__:") {
            continue;
        }
        // Views are expanded recursively; their columns keep the view's own
        // base-table qualifiers. Mapping the view alias would be wrong.
        if db.views.contains_key(&tref.name) {
            continue;
        }
        // INFORMATION_SCHEMA views surface system heap tables; the physical
        // scan stamps the *system* table name (e.g. "tables") — or the alias
        // when one is declared — as the tuple qualifier.
        if let Some(sys_name) = tref.name.strip_prefix("information_schema.") {
            let sys_lower = sys_name.to_ascii_lowercase();
            let stamped = match sys_lower.as_str() {
                "schemata" => "databases",
                "tables" => "tables",
                "columns" => "columns",
                "table_constraints" => "constraints",
                "statistics" | "indexes" => "indexes",
                "key_column_usage" => "columns",
                other => other,
            };
            let target = tref.alias.clone().unwrap_or_else(|| stamped.to_string());
            if let Some(alias) = &tref.alias {
                map.insert(alias.to_ascii_lowercase(), target.clone());
            }
            map.entry(stamped.to_ascii_lowercase()).or_insert(target);
            continue;
        }
        if db.tables.contains_key(&tref.name) {
            // The scan stamps the alias when present (SQL semantics), so all
            // qualifiers — alias AND raw table name — rewrite to it.
            let target = tref.alias.clone().unwrap_or_else(|| tref.name.clone());
            if let Some(alias) = &tref.alias {
                map.insert(alias.to_ascii_lowercase(), target.clone());
            }
            map.entry(tref.name.to_ascii_lowercase()).or_insert(target);
        }
    }
    map
}

/// Rewrite all alias-qualified identifiers in a mutable `SelectPlan`.
///
/// Only the statement's own clauses are rewritten — nested subqueries carry
/// their own scope and are planned separately.
pub fn rewrite_select_aliases(select: &mut SelectPlan, map: &HashMap<String, String>) {
    if map.is_empty() {
        return;
    }
    for proj in &mut select.projections {
        match proj {
            SelectExpr::UnnamedExpr(e) => rewrite_expr(e, map),
            SelectExpr::ExprWithAlias { expr, .. } => rewrite_expr(expr, map),
            SelectExpr::Wildcard | SelectExpr::QualifiedWildcard(_) => {}
        }
    }
    if let Some(pred) = &mut select.selection {
        rewrite_predicate(pred, map);
    }
    for join in &mut select.joins {
        rewrite_join_clause(join, map);
    }
    for expr in &mut select.group_by {
        rewrite_expr(expr, map);
    }
    if let Some(pred) = &mut select.having {
        rewrite_predicate(pred, map);
    }
    for ob in &mut select.order_by {
        rewrite_expr(&mut ob.expr, map);
    }
}

fn rewrite_join_clause(join: &mut JoinClause, map: &HashMap<String, String>) {
    if let Some(cond) = &mut join.condition {
        rewrite_predicate(cond, map);
    }
}

/// Rewrite qualifiers in an expression tree.
///
/// Does NOT descend into scalar subqueries: they establish their own scope
/// with their own FROM clause and alias mapping.
pub fn rewrite_expr(expr: &mut ExprNode, map: &HashMap<String, String>) {
    match expr {
        ExprNode::Compound(parts) => rewrite_compound(parts, map),
        ExprNode::Binary { left, right, .. } => {
            rewrite_expr(left, map);
            rewrite_expr(right, map);
        }
        ExprNode::Cast { expr, .. } => rewrite_expr(expr, map),
        ExprNode::Function { args, .. } => {
            for arg in args.iter_mut() {
                match arg {
                    FunctionArg::Star => {}
                    FunctionArg::Expr(inner) => rewrite_expr(inner, map),
                }
            }
        }
        ExprNode::Case {
            when_then_pairs,
            else_result,
        } => {
            for (cond, res) in when_then_pairs.iter_mut() {
                rewrite_expr(cond, map);
                rewrite_expr(res, map);
            }
            if let Some(else_e) = else_result {
                rewrite_expr(else_e, map);
            }
        }
        ExprNode::Compare { left, right, .. } | ExprNode::Logical { left, right, .. } => {
            rewrite_expr(left, map);
            rewrite_expr(right, map);
        }
        ExprNode::Not(inner) | ExprNode::IsNull(inner) | ExprNode::IsNotNull(inner) => {
            rewrite_expr(inner, map);
        }
        ExprNode::Predicate(p) => rewrite_predicate(p, map),
        // Columns, constants, scalar subqueries: nothing to rewrite.
        _ => {}
    }
}

fn rewrite_compound(parts: &mut [String], map: &HashMap<String, String>) {
    if parts.len() < 2 {
        return;
    }
    let qualifier_idx = parts.len() - 2;
    if let Some(canonical) = map.get(&parts[qualifier_idx].to_ascii_lowercase()) {
        parts[qualifier_idx] = canonical.clone();
    }
}

/// Rewrite qualifiers in a predicate tree.
///
/// Subquery predicates (EXISTS / IN-subquery) are skipped: they plan their
/// inner SELECT independently and correlate through explicit parameter
/// bindings, not through this query's qualifiers.
pub fn rewrite_predicate(pred: &mut PredicateNode, map: &HashMap<String, String>) {
    use PredicateNode as P;
    match pred {
        P::BinaryOp { left, right, .. } => {
            rewrite_predicate(left, map);
            rewrite_predicate(right, map);
        }
        P::Not(inner) => rewrite_predicate(inner, map),
        P::Compare { left, right, .. } => {
            rewrite_expr(left, map);
            rewrite_expr(right, map);
        }
        P::IsNull(e) | P::IsNotNull(e) => rewrite_expr(e, map),
        P::Between { expr, low, high } => {
            rewrite_expr(expr, map);
            rewrite_expr(low, map);
            rewrite_expr(high, map);
        }
        P::InList { expr, list } => {
            rewrite_expr(expr, map);
            for item in list.iter_mut() {
                rewrite_expr(item, map);
            }
        }
        P::Like { expr, .. } => rewrite_expr(expr, map),
        P::IsDistinctFrom { left, right } => {
            rewrite_expr(left, map);
            rewrite_expr(right, map);
        }
        P::IsBoolean { expr, .. } => rewrite_expr(expr, map),
        P::Exists(_) | P::InSubquery { .. } => {}
    }
}
