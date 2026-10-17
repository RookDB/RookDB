//! Helper functions for the logical planner.
//!
//! Provides table/CTE resolution, projection expansion, schema derivation,
//! aggregate extraction, and plan tree label collection.

use rook_ast::logical::*;
use rook_ast::*;

use crate::catalog::Catalog;

use super::PlanError;

// ── Table Resolution ──────────────────────────────────────────────────────────

/// Resolve a `TableRef` to its canonical table name and optional alias.
pub fn resolve_table_ref(
    tref: &TableRef,
    db: &crate::catalog::Database,
) -> Result<(String, Option<String>), PlanError> {
    if db.tables.contains_key(&tref.name) {
        Ok((tref.name.clone(), tref.alias.clone()))
    } else if let Some(stored) = unique_case_insensitive(db.tables.keys(), &tref.name) {
        // Identifiers are case-insensitive: `FROM T` finds a table created as
        // `t`. The stored spelling is what names the heap file.
        Ok((stored, tref.alias.clone()))
    } else {
        Err(PlanError {
            message: format!("Table '{}' not found", tref.name),
        })
    }
}

/// The one name in `names` equal to `wanted` ignoring ASCII case, if exactly
/// one exists.
pub fn unique_case_insensitive<'a>(
    names: impl Iterator<Item = &'a String>,
    wanted: &str,
) -> Option<String> {
    let mut matches = names.filter(|n| n.eq_ignore_ascii_case(wanted));
    let first = matches.next()?;
    matches.next().is_none().then(|| first.clone())
}

/// Resolve a FROM-clause item, expanding views recursively.
pub fn resolve_from_item(
    tref: &TableRef,
    db: &crate::catalog::Database,
    catalog: &Catalog,
    db_name: &str,
    cte_registry: &std::collections::HashMap<String, (LogicalPlan, ColumnSchema)>,
) -> Result<LogicalPlan, PlanError> {
    if let Some(view_def) = db.views.get(&tref.name) {
        let select_plan: rook_ast::SelectPlan = serde_json::from_str(&view_def.query_json)
            .map_err(|e| PlanError {
                message: format!("Failed to deserialise view '{}': {}", tref.name, e),
            })?;
        return super::plan_select(&select_plan, catalog, db_name);
    }
    build_table_or_cte_scan(tref, db, cte_registry)
}

/// Build a plan leaf for a FROM-clause table reference (CTE, info_schema, or catalog table).
pub fn build_table_or_cte_scan(
    tref: &TableRef,
    db: &crate::catalog::Database,
    cte_registry: &std::collections::HashMap<String, (LogicalPlan, ColumnSchema)>,
) -> Result<LogicalPlan, PlanError> {
    const CTE_PREFIX: &str = "__cte__:";
    if let Some(cte_name) = tref.name.strip_prefix(CTE_PREFIX) {
        let key = cte_name.to_ascii_lowercase();
        let (_inner, schema) = cte_registry.get(&key).ok_or_else(|| PlanError {
            message: format!(
                "CTE '{}' is referenced but not defined in WITH clause",
                cte_name
            ),
        })?;
        Ok(LogicalPlan::CteScan(LogicalCteScan {
            name: cte_name.to_string(),
            alias: tref.alias.clone(),
            schema: schema.clone(),
        }))
    } else if let Some((_inner, schema)) = cte_registry.get(&tref.name.to_ascii_lowercase()) {
        Ok(LogicalPlan::CteScan(LogicalCteScan {
            name: tref.name.clone(),
            alias: tref.alias.clone(),
            schema: schema.clone(),
        }))
    } else if let Some(sys_name) = tref.name.strip_prefix("information_schema.") {
        let sys_lower = sys_name.to_ascii_lowercase();
        let sys_table_name = match sys_lower.as_str() {
            "schemata" => "databases",
            "tables" => "tables",
            "columns" => "columns",
            "table_constraints" => "constraints",
            "statistics" | "indexes" => "indexes",
            "key_column_usage" => "columns",
            other => other,
        };
        let column_schema = crate::backend::system_table::info_schema_column_schema(&sys_lower);
        Ok(LogicalPlan::TableScan(LogicalTableScan {
            table: sys_name.to_string(),
            alias: tref.alias.clone(),
            schema: column_schema,
            system_table_name: Some(sys_table_name.to_string()),
        }))
    } else {
        let (resolved_name, alias) = resolve_table_ref(tref, db)?;
        let table_schema = db.tables.get(&resolved_name).ok_or_else(|| PlanError {
            message: format!("Table '{}' not found in database", resolved_name),
        })?;
        let column_schema =
            crate::planner::semantic::catalog_columns_to_schema(&table_schema.columns);
        Ok(LogicalPlan::TableScan(LogicalTableScan {
            table: resolved_name,
            alias,
            schema: column_schema,
            system_table_name: None,
        }))
    }
}

// ── Projection Expansion ──────────────────────────────────────────────────────

/// Expand SELECT items into named expressions, resolving `SELECT *`.
pub fn expand_projections(
    projections: &[SelectExpr],
    current_plan: &LogicalPlan,
) -> Result<Vec<NamedExpr>, PlanError> {
    let qualified_schema = derive_qualified_schema(current_plan);

    // Above an Aggregate, a SELECT expression that repeats a GROUP BY
    // expression (`SELECT x + 1 ... GROUP BY x + 1`) reads that group key's
    // output column; its inputs no longer exist at this level.
    let group_by: &[ExprNode] = match current_plan {
        LogicalPlan::Aggregate(a) => &a.group_by,
        _ => &[],
    };
    let rewritten: Vec<SelectExpr>;
    let projections = if group_by.iter().any(is_computed_group_key) {
        rewritten = projections
            .iter()
            .map(|item| match item {
                SelectExpr::UnnamedExpr(e) if contains_group_expr(e, group_by) => {
                    // Keep the display name of the expression as written.
                    SelectExpr::ExprWithAlias {
                        expr: replace_group_exprs(e, group_by),
                        alias: expr_to_name(e),
                    }
                }
                SelectExpr::ExprWithAlias { expr, alias } => SelectExpr::ExprWithAlias {
                    expr: replace_group_exprs(expr, group_by),
                    alias: alias.clone(),
                },
                other => other.clone(),
            })
            .collect();
        &rewritten[..]
    } else {
        projections
    };

    let mut result = Vec::new();
    for item in projections {
        match item {
            SelectExpr::Wildcard => {
                for (expr, name) in star_items(current_plan) {
                    result.push(NamedExpr { name, expr });
                }
            }
            SelectExpr::QualifiedWildcard(prefix) => {
                let mut matched = false;
                for (table, col) in &qualified_schema {
                    let is_match = match table {
                        Some(tbl) => tbl.eq_ignore_ascii_case(prefix),
                        None => col
                            .name
                            .split('.')
                            .next()
                            .map(|p| p.eq_ignore_ascii_case(prefix))
                            .unwrap_or(false),
                    };
                    if is_match {
                        matched = true;
                        result.push(NamedExpr {
                            name: col.name.clone(),
                            expr: ExprNode::Compound(vec![prefix.clone(), col.name.clone()]),
                        });
                    }
                }
                if !matched {
                    for (table, col) in &qualified_schema {
                        let expr = match table {
                            Some(tbl) => ExprNode::Compound(vec![tbl.clone(), col.name.clone()]),
                            None => ExprNode::Column(col.name.clone()),
                        };
                        result.push(NamedExpr {
                            name: col.name.clone(),
                            expr,
                        });
                    }
                }
            }
            SelectExpr::UnnamedExpr(expr) => {
                let name = expr_to_name(expr);
                // For expressions containing nested aggregates (e.g. `SUM(a)+1`),
                // recursively replace aggregate function calls with column references
                // so the projection above the AggregateOperator resolves them correctly.
                let resolved_expr = if contains_aggregate_expr(expr) {
                    replace_aggregates_in_expr(expr)
                } else {
                    match expr {
                        ExprNode::Function {
                            name: fn_name,
                            args,
                            distinct,
                        } => {
                            let upper = fn_name.to_ascii_uppercase();
                            if is_aggregate_function(&upper) {
                                ExprNode::Column(aggregate_ref_name(fn_name, args, *distinct))
                            } else {
                                expr.clone()
                            }
                        }
                        _ => expr.clone(),
                    }
                };
                result.push(NamedExpr {
                    name,
                    expr: resolved_expr,
                });
            }
            SelectExpr::ExprWithAlias { expr, alias } => {
                // A top-level aliased aggregate (`COUNT(*) AS n`) computes a
                // column named after the ALIAS — reference that. Otherwise
                // rewrite nested aggregate calls (`SUM(a)+1`) recursively.
                let resolved_expr = match expr {
                    ExprNode::Function { name, .. }
                        if is_aggregate_function(&name.to_ascii_uppercase()) =>
                    {
                        ExprNode::Column(alias.clone())
                    }
                    _ if contains_aggregate_expr(expr) => replace_aggregates_in_expr(expr),
                    _ => expr.clone(),
                };
                result.push(NamedExpr {
                    name: alias.clone(),
                    expr: resolved_expr,
                });
            }
        }
    }
    Ok(result)
}

/// The columns `SELECT *` shows, each as the expression that reads it.
///
/// A `USING` / `NATURAL` join shows each shared column once, first: the left
/// copy, the right copy for a RIGHT join, or `COALESCE(left, right)` for a
/// FULL join. The rest follow in table order.
fn star_items(plan: &LogicalPlan) -> Vec<(ExprNode, String)> {
    match plan {
        LogicalPlan::Filter(f) => star_items(&f.child),
        LogicalPlan::Sort(s) => star_items(&s.child),
        LogicalPlan::Limit(l) => star_items(&l.child),
        LogicalPlan::Distinct(d) => star_items(&d.child),
        LogicalPlan::Join(j) => {
            let left = star_items(&j.left);
            let right = star_items(&j.right);
            let has = |items: &[(ExprNode, String)], n: &str| {
                items.iter().any(|(_, name)| name.eq_ignore_ascii_case(n))
            };
            let names: Vec<String> = match j.join_type {
                JoinType::Natural => left
                    .iter()
                    .filter(|(_, n)| has(&right, n))
                    .map(|(_, n)| n.clone())
                    .collect(),
                _ => j
                    .using
                    .iter()
                    .filter(|n| has(&left, n) && has(&right, n))
                    .cloned()
                    .collect(),
            };
            let find = |items: &[(ExprNode, String)], n: &str| {
                items
                    .iter()
                    .find(|(_, name)| name.eq_ignore_ascii_case(n))
                    .map(|(e, _)| e.clone())
            };
            let mut out = Vec::new();
            for n in &names {
                let (Some(l), Some(r)) = (find(&left, n), find(&right, n)) else {
                    continue;
                };
                let expr = match j.join_type {
                    JoinType::Right => r,
                    JoinType::Full => ExprNode::Function {
                        name: "COALESCE".to_string(),
                        args: vec![
                            FunctionArg::Expr(Box::new(l)),
                            FunctionArg::Expr(Box::new(r)),
                        ],
                        distinct: false,
                    },
                    _ => l,
                };
                out.push((expr, n.clone()));
            }
            let shared = |name: &str| names.iter().any(|n| n.eq_ignore_ascii_case(name));
            out.extend(left.into_iter().filter(|(_, n)| !shared(n)));
            out.extend(right.into_iter().filter(|(_, n)| !shared(n)));
            out
        }
        other => derive_qualified_schema(other)
            .into_iter()
            .map(|(table, col)| {
                let expr = match table {
                    Some(tbl) => ExprNode::Compound(vec![tbl, col.name.clone()]),
                    None => ExprNode::Column(col.name.clone()),
                };
                (expr, col.name)
            })
            .collect(),
    }
}

// ── Schema Derivation ─────────────────────────────────────────────────────────

/// Derive a column schema from a logical plan (for semantic validation).
pub fn derive_schema(plan: &LogicalPlan) -> ColumnSchema {
    match plan {
        LogicalPlan::TableScan(t) => t.schema.clone(),
        LogicalPlan::Filter(f) => derive_schema(&f.child),
        LogicalPlan::Project(p) => {
            let mut cols = Vec::new();
            for expr in &p.expressions {
                cols.push(ColumnInfo {
                    name: expr.name.clone(),
                    data_type: "UNKNOWN".to_string(),
                    nullable: true,
                });
            }
            ColumnSchema { columns: cols }
        }
        LogicalPlan::Join(j) => {
            let left_schema = derive_schema(&j.left);
            let right_schema = derive_schema(&j.right);
            let mut cols = left_schema.columns.clone();
            cols.extend(right_schema.columns);
            ColumnSchema { columns: cols }
        }
        LogicalPlan::Distinct(d) => derive_schema(&d.child),
        LogicalPlan::Sort(s) => derive_schema(&s.child),
        LogicalPlan::Limit(l) => derive_schema(&l.child),
        LogicalPlan::Aggregate(a) => {
            let child_schema = derive_schema(&a.child);
            let mut cols = Vec::new();
            for expr in &a.group_by {
                let name = group_key_name(expr);
                cols.push(ColumnInfo {
                    name,
                    data_type: "UNKNOWN".to_string(),
                    nullable: true,
                });
            }
            for agg in &a.aggregates {
                let name = agg
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("{:?}({})", agg.function, agg.args.len()));
                cols.push(ColumnInfo {
                    name,
                    data_type: "UNKNOWN".to_string(),
                    nullable: true,
                });
            }
            if cols.is_empty() {
                child_schema
            } else {
                ColumnSchema { columns: cols }
            }
        }
        LogicalPlan::SetOp(s) => derive_schema(&s.left),
        LogicalPlan::Subquery(sq) => derive_schema(&sq.subquery),
        LogicalPlan::Cte(c) => derive_schema(&c.outer),
        LogicalPlan::RecursiveCte(rc) => derive_schema(&rc.outer),
        LogicalPlan::CteScan(cs) => cs.schema.clone(),
        LogicalPlan::Insert(inp) => derive_schema(&inp.child),
    }
}

/// Derive a schema paired with optional source table qualifiers from a logical plan.
pub fn derive_qualified_schema(plan: &LogicalPlan) -> Vec<(Option<String>, ColumnInfo)> {
    match plan {
        LogicalPlan::TableScan(t) => {
            let tbl = t.alias.clone().unwrap_or_else(|| t.table.clone());
            t.schema
                .columns
                .iter()
                .map(|c| (Some(tbl.clone()), c.clone()))
                .collect()
        }
        LogicalPlan::Filter(f) => derive_qualified_schema(&f.child),
        LogicalPlan::Project(p) => p
            .expressions
            .iter()
            .map(|expr| {
                let table = match &expr.expr {
                    ExprNode::Compound(parts) if parts.len() >= 2 => {
                        Some(parts[parts.len() - 2].clone())
                    }
                    _ => None,
                };
                (
                    table,
                    ColumnInfo {
                        name: expr.name.clone(),
                        data_type: "UNKNOWN".to_string(),
                        nullable: true,
                    },
                )
            })
            .collect(),
        LogicalPlan::Join(j) => {
            let mut cols = derive_qualified_schema(&j.left);
            cols.extend(derive_qualified_schema(&j.right));
            cols
        }
        LogicalPlan::Distinct(d) => derive_qualified_schema(&d.child),
        LogicalPlan::Sort(s) => derive_qualified_schema(&s.child),
        LogicalPlan::Limit(l) => derive_qualified_schema(&l.child),
        LogicalPlan::Aggregate(a) => {
            let mut cols = Vec::new();
            for expr in &a.group_by {
                let name = group_key_name(expr);
                cols.push((
                    None,
                    ColumnInfo {
                        name,
                        data_type: "UNKNOWN".to_string(),
                        nullable: true,
                    },
                ));
            }
            for agg in &a.aggregates {
                let name = agg
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("{:?}({})", agg.function, agg.args.len()));
                cols.push((
                    None,
                    ColumnInfo {
                        name,
                        data_type: "UNKNOWN".to_string(),
                        nullable: true,
                    },
                ));
            }
            if cols.is_empty() {
                derive_qualified_schema(&a.child)
            } else {
                cols
            }
        }
        LogicalPlan::SetOp(s) => derive_qualified_schema(&s.left),
        LogicalPlan::Subquery(sq) => derive_qualified_schema(&sq.subquery),
        LogicalPlan::Cte(c) => derive_qualified_schema(&c.outer),
        LogicalPlan::RecursiveCte(rc) => derive_qualified_schema(&rc.outer),
        LogicalPlan::CteScan(cs) => cs
            .schema
            .columns
            .iter()
            .map(|c| (cs.alias.clone(), c.clone()))
            .collect(),
        LogicalPlan::Insert(inp) => derive_qualified_schema(&inp.child),
    }
}

// ── Expression Naming ─────────────────────────────────────────────────────────

/// Extract a display name from an expression.
pub fn expr_to_name(expr: &ExprNode) -> String {
    match expr {
        ExprNode::Column(name) => name.clone(),
        ExprNode::Compound(parts) => parts.last().cloned().unwrap_or_default(),
        ExprNode::Constant(cv) => format!("{:?}", cv),
        ExprNode::Binary { .. } => "expr".to_string(),
        ExprNode::Cast { data_type, .. } => format!("CAST({})", data_type),
        ExprNode::ScalarSubquery(_) => "(scalar subquery)".to_string(),
        ExprNode::Function { name, .. } => name.clone(),
        ExprNode::Case { .. } => "CASE".to_string(),
        ExprNode::Compare { .. } => "expr".to_string(),
        ExprNode::Logical { .. } => "expr".to_string(),
        ExprNode::Not(..) => "expr".to_string(),
        ExprNode::IsNull(..) => "expr".to_string(),
        ExprNode::IsNotNull(..) => "expr".to_string(),
    }
}

/// A GROUP BY key that is computed rather than a plain column.
fn is_computed_group_key(expr: &ExprNode) -> bool {
    !matches!(
        expr,
        ExprNode::Column(_) | ExprNode::Compound(_) | ExprNode::Constant(_)
    )
}

/// Output-column name of a GROUP BY key. Computed keys get a name unique to
/// the expression so two of them never share a column.
pub fn group_key_name(expr: &ExprNode) -> String {
    if is_computed_group_key(expr) {
        format!("{:?}", expr)
    } else {
        expr_to_name(expr)
    }
}

fn contains_group_expr(expr: &ExprNode, group_by: &[ExprNode]) -> bool {
    replace_group_exprs(expr, group_by) != *expr
}

/// Replace every occurrence of a computed GROUP BY expression inside `expr`
/// with a reference to that key's output column.
fn replace_group_exprs(expr: &ExprNode, group_by: &[ExprNode]) -> ExprNode {
    if is_computed_group_key(expr) && group_by.contains(expr) {
        return ExprNode::Column(group_key_name(expr));
    }
    match expr {
        // An aggregate's argument is read from the rows of the group.
        ExprNode::Function { name, .. } if is_aggregate_function(&name.to_ascii_uppercase()) => {
            expr.clone()
        }
        other => other.map_children(&mut |e| replace_group_exprs(e, group_by)),
    }
}

/// The first column `expr` reads outside an aggregate call that is not a
/// GROUP BY key, as written; such a column has no single value per group.
pub fn ungrouped_column(expr: &ExprNode, group_by: &[ExprNode]) -> Option<String> {
    if group_by.contains(expr) {
        return None;
    }
    let last = |parts: &[String]| parts.last().cloned().unwrap_or_default();
    let grouped = |table: Option<&str>, name: &str| {
        group_by.iter().any(|g| match g {
            ExprNode::Column(c) => c.eq_ignore_ascii_case(name),
            ExprNode::Compound(parts) => {
                last(parts).eq_ignore_ascii_case(name)
                    && table.is_none_or(|t| {
                        parts.len() < 2 || parts[parts.len() - 2].eq_ignore_ascii_case(t)
                    })
            }
            _ => false,
        })
    };
    match expr {
        ExprNode::Column(name) => (!grouped(None, name)).then(|| name.clone()),
        ExprNode::Compound(parts) => {
            let table = parts.len().checked_sub(2).map(|i| parts[i].as_str());
            (!grouped(table, &last(parts))).then(|| parts.join("."))
        }
        ExprNode::Function { name, .. } if is_aggregate_function(&name.to_ascii_uppercase()) => {
            None
        }
        other => other
            .children()
            .into_iter()
            .find_map(|e| ungrouped_column(e, group_by)),
    }
}

// ── Aggregate Detection ───────────────────────────────────────────────────────

/// Check if a function name is an aggregate function.
pub fn is_aggregate_function(name: &str) -> bool {
    matches!(name, "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
}

/// Check if a SELECT expression contains an aggregate function.
pub fn contains_aggregate(expr: &SelectExpr) -> bool {
    match expr {
        SelectExpr::UnnamedExpr(e) | SelectExpr::ExprWithAlias { expr: e, .. } => {
            contains_aggregate_expr(e)
        }
        _ => false,
    }
}

fn contains_aggregate_expr(expr: &ExprNode) -> bool {
    match expr {
        ExprNode::Function { name, .. } if is_aggregate_function(&name.to_ascii_uppercase()) => {
            true
        }
        other => other.children().into_iter().any(contains_aggregate_expr),
    }
}

/// Recursively replace aggregate function calls in an expression tree with column
/// references.  This allows `SUM(a)+1` to become `Column("SUM")+1`, where `Column("SUM")`
/// refers to the output column produced by the AggregateOperator.
///
/// Non-aggregate function calls (e.g. UPPER, LENGTH) are left untouched.
pub fn replace_aggregates_in_expr(expr: &ExprNode) -> ExprNode {
    match expr {
        ExprNode::Function {
            name,
            args,
            distinct,
        } if is_aggregate_function(&name.to_ascii_uppercase()) => {
            // A reference to the column extract_aggregates names for it.
            ExprNode::Column(aggregate_ref_name(name, args, *distinct))
        }
        other => other.map_children(&mut replace_aggregates_in_expr),
    }
}

/// Recursively extract aggregate function calls from an expression tree.
///
/// Walks the expression tree depth-first and collects all aggregate function
/// nodes (COUNT, SUM, AVG, MIN, MAX) at any nesting depth. This handles
/// expressions like `SUM(price)+1` where the aggregate is nested inside
/// a binary arithmetic expression.
fn extract_aggregates_from_expr(expr: &ExprNode) -> Vec<AggregateExpr> {
    if let ExprNode::Function {
        name,
        args,
        distinct,
    } = expr
    {
        let function = match name.to_ascii_uppercase().as_str() {
            "COUNT" => Some(AggregateFunction::Count),
            "SUM" => Some(AggregateFunction::Sum),
            "AVG" => Some(AggregateFunction::Avg),
            "MIN" => Some(AggregateFunction::Min),
            "MAX" => Some(AggregateFunction::Max),
            _ => None,
        };
        if let Some(function) = function {
            return vec![AggregateExpr {
                function,
                args: expr.children().into_iter().cloned().collect(),
                alias: Some(aggregate_ref_name(name, args, *distinct)),
                distinct: *distinct,
            }];
        }
    }
    expr.children()
        .into_iter()
        .flat_map(extract_aggregates_from_expr)
        .collect()
}

/// Extract aggregate expressions from projection list.
///
/// Recursively searches each projection expression for aggregate function
/// calls at any nesting depth. Handles `SUM(price)+1`, `SUM(price)*2`,
/// `AVG(qty) - MIN(price)`, and other compound expressions.
pub fn extract_aggregates(projections: &[SelectExpr]) -> Vec<AggregateExpr> {
    let mut aggregates = Vec::new();
    for item in projections {
        let expr = match item {
            SelectExpr::UnnamedExpr(e) | SelectExpr::ExprWithAlias { expr: e, .. } => e,
            _ => continue,
        };
        // Recursively search for aggregates at any nesting depth
        let mut found = extract_aggregates_from_expr(expr);

        // For top-level aggregates with an explicit alias (e.g. `SUM(price) AS total`),
        // apply the alias to the aggregate. For nested aggregates (e.g. inside Binary),
        // the function name remains as the alias.
        if found.len() == 1
            && let SelectExpr::ExprWithAlias { alias, expr } = item
            && matches!(expr, ExprNode::Function { name, .. }
                if is_aggregate_function(&name.to_ascii_uppercase()))
        {
            found[0].alias = Some(alias.clone());
        }

        // A call repeated in the list (`COALESCE(MAX(x), 0), MAX(x) > 0`)
        // is computed once: its output column has one name.
        for agg in found {
            if !aggregates
                .iter()
                .any(|a: &AggregateExpr| a.alias == agg.alias)
            {
                aggregates.push(agg);
            }
        }
    }
    aggregates
}

/// Extract aggregate calls from a HAVING (or WHERE-style) predicate tree.
///
/// `HAVING COUNT(*) >= 2` must compute `COUNT(*)` even when it does not
/// appear in the SELECT list, so the logical planner merges these into the
/// aggregate list as well.
pub fn extract_aggregates_from_predicate(pred: &PredicateNode) -> Vec<AggregateExpr> {
    use PredicateNode as P;
    match pred {
        P::BinaryOp { left, right, .. } => {
            let mut out = self::extract_aggregates_from_predicate(left);
            out.extend(extract_aggregates_from_predicate(right));
            out
        }
        P::Not(inner) => extract_aggregates_from_predicate(inner),
        P::Compare { left, right, .. } => {
            let mut out = extract_aggregates_from_expr(left);
            out.extend(extract_aggregates_from_expr(right));
            out
        }
        P::IsNull(e) | P::IsNotNull(e) => extract_aggregates_from_expr(e),
        P::Between { expr, low, high } => {
            let mut out = extract_aggregates_from_expr(expr);
            out.extend(extract_aggregates_from_expr(low));
            out.extend(extract_aggregates_from_expr(high));
            out
        }
        P::InList { expr, list } => {
            let mut out = extract_aggregates_from_expr(expr);
            for e in list {
                out.extend(extract_aggregates_from_expr(e));
            }
            out
        }
        P::Like { expr, .. } => extract_aggregates_from_expr(expr),
        P::IsDistinctFrom { left, right } => {
            let mut out = extract_aggregates_from_expr(left);
            out.extend(extract_aggregates_from_expr(right));
            out
        }
        P::IsBoolean { expr, .. } => extract_aggregates_from_expr(expr),
        // Subquery predicates carry their own scope; their aggregates are
        // planned inside the subquery.
        P::Exists(_) | P::InSubquery { .. } => Vec::new(),
    }
}

/// Stable identity of an aggregate call, used to deduplicate repeated
/// occurrences of the same expression across SELECT / HAVING.
pub fn aggregate_identity(agg: &AggregateExpr) -> String {
    format!(
        "{:?}({}{:?})",
        agg.function,
        if agg.distinct { "DISTINCT " } else { "" },
        agg.args
    )
}

/// Internal output-column name of an aggregate call. It is unique per
/// distinct call, so `SUM(a)` and `SUM(b)` in one SELECT never share a
/// column; the display name of the result comes from the projection above.
pub fn aggregate_ref_name(name: &str, args: &[rook_ast::FunctionArg], distinct: bool) -> String {
    let rendered: Vec<String> = args
        .iter()
        .map(|a| match a {
            rook_ast::FunctionArg::Star => "*".to_string(),
            rook_ast::FunctionArg::Expr(e) => format!("{:?}", e),
        })
        .collect();
    format!(
        "{}({}{})",
        name.to_ascii_uppercase(),
        if distinct { "DISTINCT " } else { "" },
        rendered.join(",")
    )
}

// ── Plan Tree Labels ──────────────────────────────────────────────────────────

/// Collect all node labels from the plan tree in depth-first order.
pub fn collect_labels(plan: &LogicalPlan) -> Vec<String> {
    let mut labels = Vec::new();
    collect_labels_recursive(plan, &mut labels);
    labels
}

pub fn collect_labels_recursive(plan: &LogicalPlan, labels: &mut Vec<String>) {
    labels.push(plan.label().to_string());
    match plan {
        LogicalPlan::TableScan(_) => {}
        LogicalPlan::Filter(f) => collect_labels_recursive(&f.child, labels),
        LogicalPlan::Project(p) => collect_labels_recursive(&p.child, labels),
        LogicalPlan::Distinct(d) => collect_labels_recursive(&d.child, labels),
        LogicalPlan::Sort(s) => collect_labels_recursive(&s.child, labels),
        LogicalPlan::Limit(l) => collect_labels_recursive(&l.child, labels),
        LogicalPlan::Aggregate(a) => collect_labels_recursive(&a.child, labels),
        LogicalPlan::Join(j) => {
            collect_labels_recursive(&j.left, labels);
            collect_labels_recursive(&j.right, labels);
        }
        LogicalPlan::SetOp(s) => {
            collect_labels_recursive(&s.left, labels);
            collect_labels_recursive(&s.right, labels);
        }
        LogicalPlan::Subquery(sq) => collect_labels_recursive(&sq.subquery, labels),
        LogicalPlan::Cte(c) => {
            collect_labels_recursive(&c.inner, labels);
            collect_labels_recursive(&c.outer, labels);
        }
        LogicalPlan::RecursiveCte(rc) => {
            collect_labels_recursive(&rc.non_recursive, labels);
            collect_labels_recursive(&rc.recursive, labels);
            collect_labels_recursive(&rc.outer, labels);
        }
        LogicalPlan::CteScan(_) => {}
        LogicalPlan::Insert(inp) => collect_labels_recursive(&inp.child, labels),
    }
}
