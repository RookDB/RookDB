//! Logical Planner — Translates `rook_ast::QueryPlan` into a `LogicalPlan` tree.
//!
//! The planner performs two main passes:
//!
//! 1. **Semantic analysis**: resolve column names, expand `SELECT *`, validate
//!    table/column existence, apply type coercions.
//! 2. **Plan building**: construct the logical operator tree matching the query's
//!    intent (filter, project, sort, aggregate, join, etc.).

pub mod alias;
pub mod helpers;
pub mod optimizer;
pub mod plan_cache;
pub mod semantic;
#[cfg(test)]
pub mod tests;

use rook_ast::logical::*;
use rook_ast::*;

use crate::catalog::Catalog;
// statistics collection now routed through backend::cache (size-validated)

use self::helpers::*;

/// Errors produced during planning / semantic analysis.
#[derive(Debug, Clone)]
pub struct PlanError {
    pub message: String,
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for PlanError {}

// ── Public API ────────────────────────────────────────────────────────────────

/// Load table statistics for all tables in the given database.
///
/// Collects row counts and per-column statistics from each table's heap file
/// (see `cache::table_statistics` for when they are gathered again).
/// Tables whose heap files don't exist yet (e.g. freshly created with no
/// data) are silently skipped. Returns an empty map if no stats can be
/// collected.
fn load_table_statistics(
    db_name: &str,
    catalog: &Catalog,
    plan: &LogicalPlan,
) -> std::collections::HashMap<String, std::sync::Arc<crate::statistics::TableStatistics>> {
    let db = match catalog.databases.get(db_name) {
        Some(d) => d,
        None => return std::collections::HashMap::new(),
    };

    // Statistics only steer join ordering, and only for the tables a join
    // reads. Loading them for every table in the database made planning
    // cost grow with the size of the schema.
    let mut joined_tables = std::collections::HashSet::new();
    collect_joined_tables(plan, false, &mut joined_tables);

    let mut stats = std::collections::HashMap::new();
    for table_name in db.tables.keys().filter(|t| joined_tables.contains(*t)) {
        // Process-cached until the table changes enough: collection reads
        // every heap page and would otherwise dominate per-query planning
        // latency.
        if let Ok(table_stats) =
            crate::backend::cache::table_statistics(catalog, db_name, table_name)
        {
            stats.insert(table_name.clone(), table_stats);
        }
    }
    stats
}

/// Collect the base tables scanned beneath any join in `plan`.
fn collect_joined_tables(
    plan: &LogicalPlan,
    under_join: bool,
    out: &mut std::collections::HashSet<String>,
) {
    match plan {
        LogicalPlan::TableScan(t) => {
            if under_join && t.system_table_name.is_none() {
                out.insert(t.table.clone());
            }
        }
        LogicalPlan::Join(j) => {
            collect_joined_tables(&j.left, true, out);
            collect_joined_tables(&j.right, true, out);
        }
        LogicalPlan::Filter(f) => collect_joined_tables(&f.child, under_join, out),
        LogicalPlan::Project(p) => collect_joined_tables(&p.child, under_join, out),
        LogicalPlan::Aggregate(a) => collect_joined_tables(&a.child, under_join, out),
        LogicalPlan::Sort(s) => collect_joined_tables(&s.child, under_join, out),
        LogicalPlan::Limit(l) => collect_joined_tables(&l.child, under_join, out),
        LogicalPlan::Distinct(d) => collect_joined_tables(&d.child, under_join, out),
        LogicalPlan::Subquery(sq) => collect_joined_tables(&sq.subquery, under_join, out),
        LogicalPlan::Insert(i) => collect_joined_tables(&i.child, under_join, out),
        LogicalPlan::SetOp(s) => {
            collect_joined_tables(&s.left, under_join, out);
            collect_joined_tables(&s.right, under_join, out);
        }
        LogicalPlan::Cte(c) => {
            collect_joined_tables(&c.inner, under_join, out);
            collect_joined_tables(&c.outer, under_join, out);
        }
        LogicalPlan::RecursiveCte(rc) => {
            collect_joined_tables(&rc.non_recursive, under_join, out);
            collect_joined_tables(&rc.recursive, under_join, out);
            collect_joined_tables(&rc.outer, under_join, out);
        }
        LogicalPlan::CteScan(_) => {}
    }
}

/// The optimizer for `plan`: with the statistics of the tables it joins,
/// and the engine's join-order setting.
fn optimizer_for(db_name: &str, catalog: &Catalog, plan: &LogicalPlan) -> optimizer::Optimizer {
    let table_stats = load_table_statistics(db_name, catalog, plan);
    let mut optimizer = if table_stats.is_empty() {
        optimizer::Optimizer::new()
    } else {
        optimizer::Optimizer::with_statistics(table_stats)
    };
    optimizer.skip_join_ordering = catalog.engine().keeps_join_order();
    optimizer
}

/// Plan an INSERT statement into a LogicalPlan.
///
/// Both forms route through the same pipeline:
/// - `INSERT INTO t SELECT …` — the SELECT becomes the child plan.
/// - `INSERT INTO t VALUES …` — the literal rows are expanded to full table
///   arity here (explicit-column mapping, column DEFAULTs, NULLs) and
///   carried as `values_rows`; the physical planner feeds them to the insert
///   operator via a constant-producing child.
pub fn plan_insert(
    insert: &InsertPlan,
    catalog: &Catalog,
    db_name: &str,
) -> Result<LogicalPlan, PlanError> {
    let dummy_child = || {
        Box::new(LogicalPlan::TableScan(LogicalTableScan {
            table: "__singlerow__".to_string(),
            alias: None,
            schema: ColumnSchema::empty(),
            system_table_name: Some("__singlerow__".to_string()),
        }))
    };

    if let Some(source_select) = &insert.source_select {
        let child_plan = plan_select(source_select, catalog, db_name)?;
        return Ok(LogicalPlan::Insert(LogicalInsert {
            table: insert.table.clone(),
            columns: insert.columns.clone(),
            child: Box::new(child_plan),
            values_rows: Vec::new(),
        }));
    }

    // ── INSERT ... VALUES ────────────────────────────────────────────────
    if insert.values.is_empty() {
        return Err(PlanError {
            message: "INSERT requires a VALUES clause or a SELECT source".to_string(),
        });
    }

    let db = catalog.databases.get(db_name).ok_or_else(|| PlanError {
        message: format!("Database '{}' not found", db_name),
    })?;
    let table = db.tables.get(&insert.table).ok_or_else(|| PlanError {
        message: format!("Table '{}' not found", insert.table),
    })?;

    // Expand every row to full table arity:
    //   explicit column list → position mapping
    //   missing column       → declared DEFAULT, else NULL
    let mut values_rows: Vec<Vec<ExprNode>> = Vec::with_capacity(insert.values.len());
    for row in &insert.values {
        if !insert.columns.is_empty() {
            let mut full: Vec<Option<&ExprNode>> = vec![None; table.columns.len()];
            for (pos, col_name) in insert.columns.iter().enumerate() {
                let expr = row.get(pos).ok_or_else(|| PlanError {
                    message: format!(
                        "INSERT row has {} value(s) but {} column(s) listed",
                        row.len(),
                        insert.columns.len()
                    ),
                })?;
                let target = table
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(col_name))
                    .ok_or_else(|| PlanError {
                        message: format!(
                            "Column '{}' does not exist in table '{}'",
                            col_name, insert.table
                        ),
                    })?;
                full[target] = Some(expr);
            }
            let expanded = table
                .columns
                .iter()
                .zip(full.iter())
                .map(|(col, slot)| match slot {
                    Some(e) => (*e).clone(),
                    None => col
                        .constraints
                        .default
                        .as_ref()
                        .map(|dv| ExprNode::Constant(default_to_constant(dv)))
                        .unwrap_or(ExprNode::Constant(ConstantValue::Null)),
                })
                .collect();
            values_rows.push(expanded);
        } else {
            if row.len() != table.columns.len() {
                return Err(PlanError {
                    message: format!(
                        "INSERT row has {} value(s) but table '{}' has {} column(s)",
                        row.len(),
                        insert.table,
                        table.columns.len()
                    ),
                });
            }
            values_rows.push(row.clone());
        }
    }

    Ok(LogicalPlan::Insert(LogicalInsert {
        table: insert.table.clone(),
        columns: insert.columns.clone(),
        child: dummy_child(),
        values_rows,
    }))
}

/// Render a stored DEFAULT `DataValue` back into an expression constant.
fn default_to_constant(dv: &crate::types::DataValue) -> ConstantValue {
    use crate::types::DataValue;
    match dv {
        DataValue::SmallInt(v) => ConstantValue::Int(*v as i64),
        DataValue::Int(v) => ConstantValue::Int(*v as i64),
        DataValue::BigInt(v) => ConstantValue::Int(*v),
        DataValue::Real(f) => ConstantValue::Float(f.0 as f64),
        DataValue::DoublePrecision(f) => ConstantValue::Float(f.0),
        DataValue::Bool(b) => ConstantValue::Boolean(*b),
        DataValue::Char(s) | DataValue::Varchar(s) => ConstantValue::Text(s.clone()),
        other => ConstantValue::Text(format!("{}", other)),
    }
}

/// Plan a `QueryPlan` into a `LogicalPlan` using the given catalog.
pub fn plan_query(
    query: &QueryPlan,
    catalog: &Catalog,
    db_name: &str,
) -> Result<LogicalPlan, PlanError> {
    match query {
        QueryPlan::Select(select) => {
            let plan = plan_select(select, catalog, db_name)?;
            Ok(optimizer_for(db_name, catalog, &plan).optimize(plan))
        }
        QueryPlan::SetOperation(set_op) => {
            let left = plan_select(&set_op.left, catalog, db_name)?;
            let right = plan_select(&set_op.right, catalog, db_name)?;
            let mut plan = LogicalPlan::SetOp(LogicalSetOp {
                left: Box::new(left),
                right: Box::new(right),
                op: match set_op.op.to_ascii_uppercase().as_str() {
                    "INTERSECT" => logical::SetOpType::Intersect,
                    "EXCEPT" => logical::SetOpType::Except,
                    _ => logical::SetOpType::Union,
                },
                all: set_op.all,
            });
            if !set_op.order_by.is_empty() {
                let limit_hint = set_op.limit.as_ref().map(|l| l.limit);
                plan = LogicalPlan::Sort(LogicalSort {
                    order_by: set_op.order_by.clone(),
                    child: Box::new(plan),
                    limit: limit_hint,
                });
            }
            if let Some(ref limit_clause) = set_op.limit {
                plan = LogicalPlan::Limit(LogicalLimit {
                    limit: limit_clause.limit,
                    offset: limit_clause.offset.unwrap_or(0),
                    child: Box::new(plan),
                });
            }
            Ok(optimizer_for(db_name, catalog, &plan).optimize(plan))
        }
        QueryPlan::Insert(ins) => plan_insert(ins, catalog, db_name),
        QueryPlan::CreateTable(_)
        | QueryPlan::DropTable(_)
        | QueryPlan::DropDatabase(_)
        | QueryPlan::AlterTable(_)
        | QueryPlan::CreateView(_)
        | QueryPlan::DropView(_)
        | QueryPlan::CreateIndex(_)
        | QueryPlan::CreateDatabase(_)
        | QueryPlan::CreateTableAsSelect(_)
        | QueryPlan::ShowTables
        | QueryPlan::ShowDatabases
        | QueryPlan::UseDatabase(_) => Err(PlanError {
            message: format!(
                "DDL/DQL statement '{}' must be handled by the executor, not the logical planner",
                query.statement_type()
            ),
        }),
        _ => Err(PlanError {
            message: format!("Planning not yet supported for {}", query.statement_type()),
        }),
    }
}

// ── SELECT planning ───────────────────────────────────────────────────────────

/// Plan a SELECT query into a LogicalPlan.
fn plan_select(
    select: &SelectPlan,
    catalog: &Catalog,
    db_name: &str,
) -> Result<LogicalPlan, PlanError> {
    plan_select_with_ctes(select, catalog, db_name, None)
}

/// Like `plan_select`, but accepts an optional pre-populated CTE registry.
fn plan_select_with_ctes(
    select: &SelectPlan,
    catalog: &Catalog,
    db_name: &str,
    existing_ctes: Option<&std::collections::HashMap<String, (LogicalPlan, ColumnSchema)>>,
) -> Result<LogicalPlan, PlanError> {
    // 0. Resolve CTEs and build a registry
    let db = catalog.databases.get(db_name).ok_or_else(|| PlanError {
        message: format!("Database '{}' not found", db_name),
    })?;

    let mut cte_registry: std::collections::HashMap<String, (LogicalPlan, ColumnSchema)> =
        std::collections::HashMap::new();

    if let Some(existing) = existing_ctes {
        for (key, val) in existing {
            cte_registry
                .entry(key.clone())
                .or_insert_with(|| val.clone());
        }
    }

    for cte_def in &select.ctes {
        let inner_plan =
            plan_select_with_ctes(&cte_def.query, catalog, db_name, Some(&cte_registry))?;
        let schema = derive_schema(&inner_plan);
        cte_registry.insert(cte_def.name.to_ascii_lowercase(), (inner_plan, schema));
    }

    // Resolve table aliases (e.g. `employees AS e`) so that qualified
    // identifiers like `e.name` carry the real table name downstream.
    // Scan operators stamp tuple columns with real table names, and the
    // runtime resolver matches on them — an unresolved alias would make
    // every aliased JOIN reference fail.
    //
    // NOTE: this mutates only a clone; CTE bodies are planned recursively
    // with their own scope and are not affected.
    let mut select = select.clone();
    let alias_map = alias::build_alias_map(&select, db);
    alias::rewrite_select_aliases(&mut select, &alias_map);
    let select = &select;

    // 1. Resolve table references and build the base scan
    let mut current_plan = if select.from.is_empty() {
        LogicalPlan::TableScan(LogicalTableScan {
            table: "__singlerow__".to_string(),
            alias: None,
            schema: ColumnSchema::empty(),
            system_table_name: Some("__singlerow__".to_string()),
        })
    } else {
        let first_table = &select.from[0];
        resolve_from_item(first_table, db, catalog, db_name, &cte_registry)?
    };

    // 2. Handle JOINs (if any)
    for join in &select.joins {
        let right_plan = resolve_from_item(&join.relation, db, catalog, db_name, &cte_registry)?;
        current_plan = LogicalPlan::Join(LogicalJoin {
            left: Box::new(current_plan),
            right: Box::new(right_plan),
            join_type: join.join_type,
            condition: join.condition.clone(),
            using: join.using.clone(),
        });
    }

    // 3. Apply WHERE filter
    if let Some(ref pred) = select.selection {
        current_plan = LogicalPlan::Filter(LogicalFilter {
            predicate: pred.clone(),
            child: Box::new(current_plan),
        });
    }

    // 4. Apply GROUP BY / HAVING
    let has_aggregates = !select.group_by.is_empty()
        || select.having.is_some()
        || select.projections.iter().any(contains_aggregate);

    if has_aggregates {
        let mut aggregates = extract_aggregates(&select.projections);

        // Aggregates that appear only inside HAVING must still be computed.
        // Merge them in, skipping calls already present in the SELECT list
        // (same function applied to the same arguments).
        if let Some(ref having) = select.having {
            for agg in extract_aggregates_from_predicate(having) {
                let id = aggregate_identity(&agg);
                if !aggregates.iter().any(|a| aggregate_identity(a) == id) {
                    aggregates.push(agg);
                }
            }
        }

        let mut group_by = select.group_by.clone();
        for gb_expr in &mut group_by {
            if let ExprNode::Constant(ConstantValue::Int(pos)) = gb_expr {
                let pos_usize = *pos as usize;
                if *pos >= 1 && pos_usize <= select.projections.len() {
                    match &select.projections[pos_usize - 1] {
                        SelectExpr::UnnamedExpr(e) | SelectExpr::ExprWithAlias { expr: e, .. } => {
                            *gb_expr = e.clone();
                        }
                        _ => {
                            let expanded = expand_projections(&select.projections, &current_plan)?;
                            if pos_usize <= expanded.len() {
                                *gb_expr = expanded[pos_usize - 1].expr.clone();
                            } else {
                                return Err(PlanError {
                                    message: format!(
                                        "GROUP BY position {} is out of range (1..{})",
                                        pos,
                                        expanded.len()
                                    ),
                                });
                            }
                        }
                    }
                } else {
                    return Err(PlanError {
                        message: format!(
                            "GROUP BY position {} is out of range (1..{})",
                            pos,
                            select.projections.len()
                        ),
                    });
                }
            }
        }

        for item in &select.projections {
            if let SelectExpr::UnnamedExpr(e) | SelectExpr::ExprWithAlias { expr: e, .. } = item
                && let Some(column) = ungrouped_column(e, &group_by)
            {
                return Err(PlanError {
                    message: format!(
                        "column '{}' must appear in the GROUP BY clause or be used in an aggregate function",
                        column
                    ),
                });
            }
        }

        current_plan = LogicalPlan::Aggregate(LogicalAggregate {
            group_by,
            aggregates,
            having: select.having.clone(),
            child: Box::new(current_plan),
        });
    }

    // 5. Apply projection
    let projections = expand_projections(&select.projections, &current_plan)?;
    current_plan = LogicalPlan::Project(LogicalProject {
        expressions: projections.clone(),
        child: Box::new(current_plan),
    });

    // 6. Apply DISTINCT
    if select.distinct {
        current_plan = LogicalPlan::Distinct(LogicalDistinct {
            child: Box::new(current_plan),
        });
    }

    // 7. Apply ORDER BY
    if !select.order_by.is_empty() {
        let limit_hint = select.limit.as_ref().map(|l| l.limit);
        let mut order_by = select.order_by.clone();
        // The sort runs above the projection, so a key that is one of the
        // SELECT outputs is read from that output column. The name is used
        // only when it identifies a single output.
        let output_ref = |idx: usize| {
            let name = &projections[idx].name;
            let unique = projections.iter().filter(|p| &p.name == name).count() == 1;
            if unique {
                rook_ast::ExprNode::Column(name.clone())
            } else {
                projections[idx].expr.clone()
            }
        };
        let select_exprs: Vec<&rook_ast::ExprNode> = select
            .projections
            .iter()
            .filter_map(|p| match p {
                SelectExpr::UnnamedExpr(e) | SelectExpr::ExprWithAlias { expr: e, .. } => Some(e),
                _ => None,
            })
            .collect();
        // Positions line up only when no wildcard expanded the list.
        let exprs_align = select_exprs.len() == projections.len();
        for ob in &mut order_by {
            if exprs_align
                && !matches!(
                    ob.expr,
                    rook_ast::ExprNode::Column(_)
                        | rook_ast::ExprNode::Compound(_)
                        | rook_ast::ExprNode::Constant(_)
                )
                && let Some(idx) = select_exprs.iter().position(|e| **e == ob.expr)
            {
                // `ORDER BY COUNT(*)` / `ORDER BY x + 1` repeating a SELECT item.
                ob.expr = output_ref(idx);
                continue;
            }
            if let rook_ast::ExprNode::Constant(rook_ast::ConstantValue::Int(pos)) = &ob.expr {
                if *pos >= 1 && (*pos as usize) <= projections.len() {
                    ob.expr = output_ref((*pos as usize) - 1);
                } else {
                    return Err(PlanError {
                        message: format!(
                            "ORDER BY position {} is out of range (1..{})",
                            pos,
                            projections.len()
                        ),
                    });
                }
            }
        }
        current_plan = LogicalPlan::Sort(LogicalSort {
            order_by,
            child: Box::new(current_plan),
            limit: limit_hint,
        });
    }

    // 8. Apply LIMIT / OFFSET
    if let Some(ref limit_clause) = select.limit {
        current_plan = LogicalPlan::Limit(LogicalLimit {
            limit: limit_clause.limit,
            offset: limit_clause.offset.unwrap_or(0),
            child: Box::new(current_plan),
        });
    }

    // 9. Wrap in LogicalCte / LogicalRecursiveCte nodes
    for cte_def in select.ctes.iter().rev() {
        let cte_key = cte_def.name.to_ascii_lowercase();

        if let Some(rec_select_plan) = &cte_def.recursive_term {
            let (non_rec_logical, non_rec_schema) = cte_registry
                .remove(&cte_key)
                .expect("CTE must be in registry at wrap time");

            let dummy_plan = LogicalPlan::TableScan(LogicalTableScan {
                table: "__recursive_cte_dummy__".to_string(),
                alias: None,
                schema: non_rec_schema.clone(),
                system_table_name: None,
            });
            cte_registry.insert(cte_key.clone(), (dummy_plan, non_rec_schema.clone()));

            let adjusted_rec_plan: rook_ast::SelectPlan = if rec_select_plan.from.is_empty() {
                let mut adjusted = (**rec_select_plan).clone();
                adjusted.from = vec![rook_ast::TableRef {
                    name: format!("__cte__:{}", cte_def.name),
                    alias: None,
                }];
                adjusted
            } else {
                (**rec_select_plan).clone()
            };

            let rec_logical =
                plan_select_with_ctes(&adjusted_rec_plan, catalog, db_name, Some(&cte_registry))?;

            cte_registry.remove(&cte_key);

            current_plan = LogicalPlan::RecursiveCte(LogicalRecursiveCte {
                name: cte_def.name.clone(),
                non_recursive: Box::new(non_rec_logical),
                recursive: Box::new(rec_logical),
                union_all: cte_def.union_all,
                schema: non_rec_schema,
                outer: Box::new(current_plan),
            });
        } else {
            let (inner_logical, _schema) = cte_registry
                .remove(&cte_key)
                .expect("CTE must be in registry at wrap time");
            current_plan = LogicalPlan::Cte(LogicalCte {
                name: cte_def.name.clone(),
                inner: Box::new(inner_logical),
                outer: Box::new(current_plan),
            });
        }
    }

    Ok(current_plan)
}
