//! Helper functions for the logical planner.
//!
//! Provides table/CTE resolution, projection expansion, schema derivation,
//! aggregate extraction, and plan tree label collection.

use rook_ast::logical::*;
use rook_ast::*;

use crate::catalog::Catalog;

use crate::backend::error::{RookError, RookResult};

// ── Table Resolution ──────────────────────────────────────────────────────────

/// Resolve a `TableRef` to its canonical table name and optional alias.
pub fn resolve_table_ref(
    tref: &TableRef,
    db: &crate::catalog::Database,
) -> RookResult<(String, Option<String>)> {
    if db.tables.contains_key(&tref.name) {
        Ok((tref.name.clone(), tref.alias.clone()))
    } else if let Some(stored) = unique_case_insensitive(db.tables.keys(), &tref.name) {
        // Identifiers are case-insensitive: `FROM T` finds a table created as
        // `t`. The stored spelling is what names the heap file.
        Ok((stored, tref.alias.clone()))
    } else {
        Err(not_found("Table", &tref.name))
    }
}

/// `"{entity} '{name}' not found"`.
pub fn not_found(entity: &'static str, name: &str) -> RookError {
    RookError::NotFound {
        entity,
        name: name.to_string(),
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
) -> RookResult<LogicalPlan> {
    if let Some(view_def) = db.views.get(&tref.name) {
        let select_plan: rook_ast::SelectPlan = serde_json::from_str(&view_def.query_json)
            .map_err(|e| {
                RookError::Internal(format!("Failed to deserialise view '{}': {}", tref.name, e))
            })?;
        // The view's rows carry the name it is read by, as a table's do.
        return Ok(LogicalPlan::Subquery(LogicalSubquery {
            subquery: Box::new(super::plan_select(&select_plan, catalog, db_name)?),
            alias: Some(tref.alias.clone().unwrap_or_else(|| tref.name.clone())),
        }));
    }
    build_table_or_cte_scan(tref, db, cte_registry)
}

/// Build a plan leaf for a FROM-clause table reference (CTE, info_schema, or catalog table).
pub fn build_table_or_cte_scan(
    tref: &TableRef,
    db: &crate::catalog::Database,
    cte_registry: &std::collections::HashMap<String, (LogicalPlan, ColumnSchema)>,
) -> RookResult<LogicalPlan> {
    const CTE_PREFIX: &str = "__cte__:";
    if let Some(cte_name) = tref.name.strip_prefix(CTE_PREFIX) {
        let key = cte_name.to_ascii_lowercase();
        let (_inner, schema) = cte_registry.get(&key).ok_or_else(|| {
            RookError::Internal(format!(
                "CTE '{}' is referenced but not defined in WITH clause",
                cte_name
            ))
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
        let sys_table_name = info_schema_table(&sys_lower);
        let column_schema = crate::backend::system_table::info_schema_column_schema(&sys_lower);
        Ok(LogicalPlan::TableScan(LogicalTableScan {
            table: sys_name.to_string(),
            alias: tref.alias.clone(),
            schema: column_schema,
            system_table_name: Some(sys_table_name.to_string()),
        }))
    } else {
        let (resolved_name, alias) = resolve_table_ref(tref, db)?;
        let table_schema = db
            .tables
            .get(&resolved_name)
            .ok_or_else(|| not_found("Table", &resolved_name))?;
        let column_schema = catalog_columns_to_schema(&table_schema.columns);
        Ok(LogicalPlan::TableScan(LogicalTableScan {
            table: resolved_name,
            alias,
            schema: column_schema,
            system_table_name: None,
        }))
    }
}

/// A catalog table's columns as a logical-plan schema.
pub fn catalog_columns_to_schema(columns: &[crate::catalog::Column]) -> ColumnSchema {
    ColumnSchema {
        columns: columns
            .iter()
            .map(|col| ColumnInfo {
                name: col.name.clone(),
                data_type: col.data_type.to_string(),
                nullable: col.nullable,
            })
            .collect(),
    }
}

/// The system table an `information_schema` view (lower-cased) reads; its
/// rows carry that table's name as their qualifier.
pub fn info_schema_table(view: &str) -> &str {
    match view {
        "schemata" => "databases",
        "table_constraints" => "constraints",
        "statistics" | "indexes" => "indexes",
        "key_column_usage" => "columns",
        other => other,
    }
}

// ── Projection Expansion ──────────────────────────────────────────────────────

/// The SELECT items of an ungrouped query (bound: `*` already expanded)
/// as named expressions.
pub fn expand_projections(projections: &[SelectExpr]) -> RookResult<Vec<NamedExpr>> {
    projections
        .iter()
        .map(|item| match item {
            SelectExpr::UnnamedExpr(expr) => Ok(NamedExpr {
                name: expr_to_name(expr),
                expr: expr.clone(),
            }),
            SelectExpr::ExprWithAlias { expr, alias } => Ok(NamedExpr {
                name: alias.clone(),
                expr: expr.clone(),
            }),
            // The binder expands `*` before planning.
            SelectExpr::Wildcard | SelectExpr::QualifiedWildcard(_) => Err(RookError::Internal(
                "SELECT * reached the planner unexpanded".to_string(),
            )),
        })
        .collect()
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
            for key in &a.group_by {
                cols.push(ColumnInfo {
                    name: key.name.clone(),
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
            for key in &a.group_by {
                // A column key keeps the column's qualifier.
                let table = match &key.expr {
                    ExprNode::Compound(parts) if parts.len() >= 2 => {
                        Some(parts[parts.len() - 2].clone())
                    }
                    _ => None,
                };
                cols.push((
                    table,
                    ColumnInfo {
                        name: key.name.clone(),
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
        LogicalPlan::Subquery(sq) => derive_qualified_schema(&sq.subquery)
            .into_iter()
            .map(|(table, c)| (sq.alias.clone().or(table), c))
            .collect(),
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
        ExprNode::OuterColumn(r) => r.column.clone(),
        ExprNode::Constant(cv) => cv.to_string(),
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
        ExprNode::Predicate(..) => "expr".to_string(),
    }
}

// ── Aggregate Detection ───────────────────────────────────────────────────────

/// Check if a function name is an aggregate function.
pub fn is_aggregate_function(name: &str) -> bool {
    matches!(name, "COUNT" | "SUM" | "AVG" | "MIN" | "MAX")
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
