//! Join, aggregate, and recursive CTE planning for the PhysicalPlanner.

use rook_ast::logical::*;

use super::super::expr::{ComparisonOp, Expr, Predicate, expr_from_ast, predicate_from_ast};
use super::super::operators::{
    AggregateFunction, AggregateInfo, AggregateOperator, HashJoinOperator,
    IndexNestedLoopJoinOperator, JoinType as PhysicalJoinType, NestedLoopJoinOperator,
    PhysicalOperator, infer_aggregate_output_type,
};
use super::super::tuple::ColumnInfo;
use super::PhysicalPlanner;
use super::helpers::infer_expr_type_from_ast;
use crate::backend::error::{RookError, RookResult};
use crate::types::datatype::DataType;

impl PhysicalPlanner {
    /// Plan a join logical node into a physical operator.
    ///
    /// Three-tier algorithm selection (see `plan_join_with_ctes` body):
    /// 1. IndexNestedLoopJoin — inner side is a plain table scan with an index
    ///    on the join key, join is INNER/LEFT, outer side is small.
    /// 2. HashJoin — INNER join with at least one cross-side equality conjunct;
    ///    leftover conjuncts become a residual post-join filter.
    /// 3. NestedLoopJoin — universal fallback for every other join type
    ///    (RIGHT/FULL/CROSS/NATURAL) and every predicate shape.
    pub(crate) fn plan_join_with_ctes(
        &self,
        j: &LogicalJoin,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::super::tuple::Tuple>>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        let left = self.plan_internal(&j.left, cte_registry)?;
        let right = self.plan_internal(&j.right, cte_registry)?;

        // Build combined schema for predicate resolution (left cols + right cols).
        // ColumnInfo.table is populated with actual table names from each operator's
        // schema (set during plan_table_scan), so table-qualified references like
        // `t1.id` and `t2.id` resolve correctly without sentinel markers.
        let combined_schema: Vec<ColumnInfo> = left
            .schema()
            .iter()
            .cloned()
            .chain(right.schema().iter().cloned())
            .collect();

        // Resolve the join condition against the combined schema.
        // For NATURAL JOIN, we build equality predicates on common column names.
        let predicate = match j.join_type {
            rook_ast::JoinType::Natural => {
                // Build equality predicates on all common column names.
                // Right-side columns have their actual table name in ColumnInfo.table
                // (set during plan_table_scan), so we use the table qualifier from
                // the right schema's column to correctly resolve the right-side ref.
                let left_schema = left.schema();
                let right_schema = right.schema();
                let mut equality_preds = Vec::new();
                for r_col in right_schema.iter() {
                    if left_schema
                        .iter()
                        .any(|l| l.name.eq_ignore_ascii_case(&r_col.name))
                    {
                        let col_name = r_col.name.clone();
                        // Use the right column's actual table qualifier for the rhs
                        let right_table = r_col.table.clone();
                        equality_preds.push(Predicate::Compare(
                            Expr::Column {
                                table: None,
                                column: col_name.clone(),
                            },
                            ComparisonOp::Equals,
                            Expr::Column {
                                table: right_table,
                                column: col_name,
                            },
                        ));
                    }
                }
                if equality_preds.is_empty() {
                    None
                } else {
                    Some(equality_preds.into_iter().reduce(Predicate::and).unwrap())
                }
            }
            _ => match &j.condition {
                Some(pred_node) => {
                    let column_names: Vec<String> =
                        combined_schema.iter().map(|c| c.name.clone()).collect();
                    Some(predicate_from_ast(pred_node, &column_names)?)
                }
                None => None,
            },
        };

        let join_type = match j.join_type {
            rook_ast::JoinType::Inner => PhysicalJoinType::Inner,
            rook_ast::JoinType::Left => PhysicalJoinType::Left,
            rook_ast::JoinType::Right => PhysicalJoinType::Right,
            rook_ast::JoinType::Full => PhysicalJoinType::Full,
            // A cross join that picked up a condition (pushed down from WHERE)
            // is an inner join; treating it as Cross would drop the condition.
            rook_ast::JoinType::Cross if predicate.is_some() => PhysicalJoinType::Inner,
            rook_ast::JoinType::Cross => PhysicalJoinType::Cross,
            rook_ast::JoinType::Natural => PhysicalJoinType::Inner,
        };

        log::info!(
            "[Volcano] Planning {:?} join with predicate={}",
            join_type,
            if predicate.is_some() { "yes" } else { "no" }
        );

        // 1. Try IndexNestedLoopJoin when inner side is a single-table scan with an index on the join key
        // and outer side is small.
        if matches!(join_type, PhysicalJoinType::Inner | PhysicalJoinType::Left)
            && let Some(pred) = &predicate
        {
            let left_schema = left.schema().to_vec();
            let right_schema = right.schema().to_vec();
            if let Some((build_keys, probe_keys, residual)) =
                extract_equi_join_keys(pred, &left_schema, &right_schema)
                && build_keys.len() == 1
                && probe_keys.len() == 1
                && let Expr::Column {
                    table: ref outer_table,
                    column: ref outer_col,
                } = build_keys[0]
                && let Expr::Column {
                    column: ref inner_col,
                    ..
                } = probe_keys[0]
                && let LogicalPlan::TableScan(ref inner_ts) = *j.right
            {
                let named_indexes =
                    crate::backend::executor::create_index::load_table_indexes_multi(
                        self.catalog.engine(),
                        &self.db_name,
                        &inner_ts.table,
                    )
                    .unwrap_or_default();

                let matched_idx = named_indexes.iter().find(|(_, cols, _)| {
                    cols.len() == 1
                        && cols
                            .first()
                            .map(|c| c.eq_ignore_ascii_case(inner_col))
                            .unwrap_or(false)
                });

                let outer_card = left.estimate_cardinality();
                let is_outer_small = outer_card <= 5000
                    || matches!(
                        j.left.as_ref(),
                        LogicalPlan::Limit(_) | LogicalPlan::Filter(_)
                    );

                if let Some((idx_name, _, _)) = matched_idx
                    && is_outer_small
                {
                    let idx_path = self.catalog.engine().paths().index(
                        &self.db_name,
                        &inner_ts.table,
                        idx_name,
                    );
                    let heap_path = self
                        .catalog
                        .engine()
                        .paths()
                        .heap(&self.db_name, &inner_ts.table);
                    if idx_path.exists()
                        && heap_path.exists()
                        && let Some(outer_key_idx) =
                            resolve_outer_key(&left_schema, outer_table.as_deref(), outer_col)
                    {
                        crate::backend::cache::checkpoint();
                        let mut btree = crate::backend::index::btree::BTree::open(idx_path.clone())
                            .map_err(|e| {
                                RookError::Io(e).with_context(format!(
                                    "opening index {} for table '{}'",
                                    idx_path.display(),
                                    inner_ts.table
                                ))
                            })?;
                        if let Some(idx_col) = right_schema
                            .iter()
                            .find(|c| c.name.eq_ignore_ascii_case(inner_col))
                        {
                            btree.set_key_type(idx_col.data_type.clone());
                        }
                        let heap_manager = crate::backend::heap::heap_manager::HeapManager::open(
                            heap_path.clone(),
                        )
                        .map_err(|e| {
                            RookError::Io(e).with_context(format!(
                                "opening heap {} for table '{}'",
                                heap_path.display(),
                                inner_ts.table
                            ))
                        })?;

                        log::info!(
                            "[Volcano] Using IndexNestedLoopJoin: outer_card={}, inner table '{}', index '{}'",
                            outer_card,
                            inner_ts.table,
                            idx_name
                        );
                        return Ok(Box::new(IndexNestedLoopJoinOperator::new(
                            left,
                            btree,
                            heap_manager,
                            outer_key_idx,
                            right_schema,
                            residual,
                            join_type,
                        )));
                    }
                }
            }
        }

        // 2. Use a hash join when the condition contains at least one
        // cross-side equality conjunct (INNER joins only) — turns O(n·m)
        // nested-loop joins into O(n+m). Remaining non-equality conjuncts are
        // evaluated as a post-join filter by HashJoinOperator.
        if matches!(join_type, PhysicalJoinType::Inner)
            && let Some(pred) = &predicate
        {
            let left_schema = left.schema().to_vec();
            let right_schema = right.schema().to_vec();
            if let Some((build_keys, probe_keys, residual)) =
                extract_equi_join_keys(pred, &left_schema, &right_schema)
                && !build_keys.is_empty()
            {
                log::info!(
                    "[Volcano] Using HashJoin: {} equi-key pair(s), residual predicate: {}",
                    build_keys.len(),
                    residual.is_some()
                );
                return Ok(Box::new(HashJoinOperator::new(
                    left, right, build_keys, probe_keys, residual,
                )));
            }
        }

        // 3. Fallback: NestedLoopJoin for all join types
        // Outer equi-joins: hash the right input instead of comparing every
        // pair. Limited to keys whose two sides hash alike (same type, or both
        // integers); other key pairs keep the plain nested loop.
        if matches!(
            join_type,
            PhysicalJoinType::Left | PhysicalJoinType::Right | PhysicalJoinType::Full
        ) && let Some(pred) = &predicate
        {
            let left_schema = left.schema().to_vec();
            let right_schema = right.schema().to_vec();
            if let Some((left_keys, right_keys, _)) =
                extract_equi_join_keys(pred, &left_schema, &right_schema)
                && !left_keys.is_empty()
                && left_keys
                    .iter()
                    .zip(right_keys.iter())
                    .all(|(l, r)| keys_hash_alike(l, &left_schema, r, &right_schema))
            {
                log::info!(
                    "[Volcano] Using HashOuterJoin: {} equi-key pair(s)",
                    left_keys.len()
                );
                return Ok(Box::new(NestedLoopJoinOperator::with_equi_keys(
                    left, right, predicate, join_type, left_keys, right_keys,
                )));
            }
        }

        Ok(Box::new(NestedLoopJoinOperator::new(
            left, right, predicate, join_type,
        )))
    }

    /// Plan an aggregate (GROUP BY / HAVING) logical node into a physical operator.
    pub(crate) fn plan_aggregate_with_ctes(
        &self,
        a: &LogicalAggregate,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::super::tuple::Tuple>>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        let child = self.plan_internal(&a.child, cte_registry)?;
        self.plan_aggregate_on_child(child, a)
    }

    pub(crate) fn plan_aggregate_on_child(
        &self,
        child: Box<dyn PhysicalOperator>,
        a: &LogicalAggregate,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        let child_schema = child.schema();
        let column_names: Vec<String> = child_schema.iter().map(|c| c.name.clone()).collect();
        let child_types: Vec<_> = child_schema.iter().map(|c| c.data_type.clone()).collect();

        // ─── Resolve GROUP BY expressions ────────────────────────────────
        let mut group_by_exprs = Vec::new();
        let mut group_by_names = Vec::new();
        let mut group_by_types = Vec::new();

        for expr_node in &a.group_by {
            let expr = expr_from_ast(expr_node, &column_names)?;
            let name = crate::planner::helpers::group_key_name(expr_node);
            let data_type = match expr_node {
                rook_ast::ExprNode::Column(name) => {
                    let idx = column_names
                        .iter()
                        .position(|c| c.eq_ignore_ascii_case(name))
                        .ok_or_else(|| RookError::NotFound {
                            entity: "Column",
                            name: name.clone(),
                        })?;
                    child_types[idx].clone()
                }
                rook_ast::ExprNode::Compound(parts) => {
                    let name = parts.last().ok_or_else(|| {
                        RookError::Internal("Empty compound identifier".to_string())
                    })?;
                    let idx = column_names
                        .iter()
                        .position(|c| c.eq_ignore_ascii_case(name))
                        .ok_or_else(|| RookError::NotFound {
                            entity: "Column",
                            name: name.clone(),
                        })?;
                    child_types[idx].clone()
                }
                rook_ast::ExprNode::Constant(cv) => match cv {
                    rook_ast::ConstantValue::Null => DataType::Int,
                    rook_ast::ConstantValue::Int(_) => DataType::Int,
                    rook_ast::ConstantValue::Float(_) => DataType::DoublePrecision,
                    rook_ast::ConstantValue::Text(_) => DataType::Varchar(u16::MAX),
                    rook_ast::ConstantValue::Boolean(_) => DataType::Bool,
                },
                rook_ast::ExprNode::Cast { data_type, .. } => {
                    data_type.parse::<DataType>().map_err(|e| {
                        RookError::TypeMismatch(format!(
                            "Invalid CAST target type '{}': {}",
                            data_type, e
                        ))
                    })?
                }
                rook_ast::ExprNode::Binary { .. } => {
                    infer_expr_type_from_ast(expr_node, &child_types, &column_names)
                        .unwrap_or(DataType::Int)
                }
                // Scalar subqueries are not expected in GROUP BY expressions
                rook_ast::ExprNode::ScalarSubquery(_) => {
                    return Err(RookError::Internal(
                        "Scalar subqueries are not allowed in GROUP BY".to_string(),
                    ));
                }
                // Aggregate functions in GROUP BY are not valid SQL — return error
                rook_ast::ExprNode::Function { .. } => {
                    return Err(RookError::Internal(
                        "Aggregate function calls are not allowed in GROUP BY".to_string(),
                    ));
                }
                rook_ast::ExprNode::Case { .. } => {
                    return Err(RookError::Internal(
                        "CASE expressions are not allowed in GROUP BY".to_string(),
                    ));
                }
                rook_ast::ExprNode::Compare { .. }
                | rook_ast::ExprNode::Logical { .. }
                | rook_ast::ExprNode::Not(_)
                | rook_ast::ExprNode::IsNull(_)
                | rook_ast::ExprNode::IsNotNull(_) => DataType::Bool,
            };
            group_by_exprs.push(expr);
            group_by_names.push(name);
            group_by_types.push(data_type);
        }

        // ─── Resolve aggregate function arguments ────────────────────────
        let mut aggregates = Vec::new();
        for agg_expr in &a.aggregates {
            // Resolve argument expressions
            let input_expr = if agg_expr.args.is_empty() {
                None
            } else {
                Some(expr_from_ast(&agg_expr.args[0], &column_names)?)
            };

            // Determine output name
            let output_name = agg_expr
                .alias
                .clone()
                .unwrap_or_else(|| format!("{:?}({})", agg_expr.function, agg_expr.args.len()));

            // Infer input type for output type calculation
            let function = match agg_expr.function {
                rook_ast::logical::AggregateFunction::Count => AggregateFunction::Count,
                rook_ast::logical::AggregateFunction::Sum => AggregateFunction::Sum,
                rook_ast::logical::AggregateFunction::Avg => AggregateFunction::Avg,
                rook_ast::logical::AggregateFunction::Min => AggregateFunction::Min,
                rook_ast::logical::AggregateFunction::Max => AggregateFunction::Max,
            };

            let input_type = if let Some(arg) = agg_expr.args.first() {
                infer_expr_type_from_ast(arg, &child_types, &column_names).ok()
            } else {
                None
            };
            let output_type = infer_aggregate_output_type(function, input_type.as_ref());

            aggregates.push(AggregateInfo {
                function,
                input: input_expr,
                output_name,
                output_type,
                distinct: agg_expr.distinct,
            });
        }

        // ─── Resolve HAVING predicate ────────────────────────────────────
        let having = if let Some(ref having_node) = a.having {
            // HAVING references the output schema (GROUP BY columns + aggregate columns).
            // Aggregate calls inside HAVING (`HAVING COUNT(*) >= 2`) are rewritten
            // into references to the corresponding computed output column so the
            // predicate evaluates against the post-aggregation tuple.
            let having_node = rewrite_having_aggregates(having_node, &a.aggregates, &aggregates);
            let mut having_schema: Vec<ColumnInfo> = group_by_names
                .iter()
                .zip(group_by_types.iter())
                .map(|(name, dt)| ColumnInfo {
                    name: name.clone(),
                    data_type: dt.clone(),
                    table: None,
                })
                .collect();
            for agg in &aggregates {
                having_schema.push(ColumnInfo {
                    name: agg.output_name.clone(),
                    data_type: agg.output_type.clone(),
                    table: None,
                });
            }
            let having_names: Vec<String> = having_schema.iter().map(|c| c.name.clone()).collect();
            Some(self.build_predicate_with_subqueries(
                &having_node,
                &having_names,
                &having_schema,
            )?)
        } else {
            None
        };

        Ok(Box::new(AggregateOperator::new(
            child,
            group_by_exprs,
            group_by_names,
            group_by_types,
            aggregates,
            having,
        )))
    }

    /// Plan a recursive CTE logical node into a physical operator via fixpoint iteration.
    ///
    /// 1. Evaluate the non-recursive term to seed the working table.
    /// 2. Loop:
    ///    a. Plan the recursive term (which contains CteScan leaves referencing the CTE).
    ///    b. Execute it — the CteScan leaves see the current working table.
    ///    c. Collect new tuples produced.
    ///    d. If no new tuples, break.
    ///    e. Append new tuples to accumulated result.
    ///    f. Replace the working table with the new tuples (for the next iteration).
    /// 3. Register all accumulated tuples and plan the outer query.
    pub(crate) fn plan_recursive_cte(
        &self,
        rc: &LogicalRecursiveCte,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::super::tuple::Tuple>>,
    ) -> RookResult<Box<dyn PhysicalOperator>> {
        log::info!("[Volcano] Planning recursive CTE '{}'", rc.name);
        let cte_key = rc.name.to_ascii_lowercase();
        // Use the recursive CTE name as the table qualifier so that
        // table-qualified column references and NATURAL JOIN disambiguation
        // work correctly when a recursive CTE appears in a join context.
        let cte_table_name = rc.name.clone();
        let _schema: Vec<super::super::tuple::ColumnInfo> = rc
            .schema
            .columns
            .iter()
            .map(|c| super::super::tuple::ColumnInfo {
                name: c.name.clone(),
                data_type: crate::types::datatype::DataType::Varchar(u16::MAX),
                table: Some(cte_table_name.clone()),
            })
            .collect();

        // 1. Evaluate the non-recursive term to seed the working table
        let mut non_rec_op = self.plan_internal(&rc.non_recursive, cte_registry)?;
        let mut working_table: Vec<super::super::tuple::Tuple> = Vec::new();
        while let Some(tuple) = non_rec_op.next()? {
            working_table.push(tuple);
        }
        log::info!(
            "[Volcano] Recursive CTE '{}' non-recursive term produced {} tuples",
            rc.name,
            working_table.len()
        );

        // UNION (without ALL) keeps one copy of every row: one running set
        // covers the anchor, each iteration, and everything seen before.
        let mut seen: std::collections::HashSet<Vec<Option<crate::types::value::DataValue>>> =
            std::collections::HashSet::new();
        if !rc.union_all {
            working_table.retain(|t| seen.insert(t.values.clone()));
        }

        // Accumulated result: start with non-recursive term's output
        let mut all_tuples: Vec<super::super::tuple::Tuple> = working_table.clone();

        // 2. Fixpoint iteration
        let max_iterations = 10_000; // safety limit to prevent infinite loops
        let mut iteration = 0;
        while !working_table.is_empty() {
            if iteration >= max_iterations {
                return Err(RookError::Internal(format!(
                    "Recursive CTE '{}' did not terminate within {} iterations",
                    rc.name, max_iterations
                )));
            }

            // Register the current working table so CteScan leaves can read it
            cte_registry.insert(cte_key.clone(), std::mem::take(&mut working_table));

            // Plan and execute the recursive term
            let mut rec_op = self.plan_internal(&rc.recursive, cte_registry)?;
            let mut new_tuples: Vec<super::super::tuple::Tuple> = Vec::new();
            while let Some(tuple) = rec_op.next()? {
                if rc.union_all || seen.insert(tuple.values.clone()) {
                    new_tuples.push(tuple);
                }
            }

            log::info!(
                "[Volcano] Recursive CTE '{}' iteration {}: produced {} new tuples",
                rc.name,
                iteration,
                new_tuples.len()
            );

            // Append to accumulated result (clone since new_tuples becomes the next working table)
            all_tuples.extend(new_tuples.iter().cloned());
            working_table = new_tuples;
            iteration += 1;
        }

        // 3. Register all accumulated tuples and plan the outer query
        log::info!(
            "[Volcano] Recursive CTE '{}' complete: {} total tuples",
            rc.name,
            all_tuples.len()
        );
        cte_registry.insert(cte_key, all_tuples);

        // Plan the outer query which may reference the CTE via CteScan
        self.plan_internal(&rc.outer, cte_registry)
    }
}

/// Position of the join key in the outer schema. A qualified reference must
/// match its table; an unqualified one must be unambiguous. `None` makes the
/// planner fall back to a join that evaluates the full predicate.
fn resolve_outer_key(schema: &[ColumnInfo], table: Option<&str>, column: &str) -> Option<usize> {
    let mut matches = schema.iter().enumerate().filter(|(_, c)| {
        c.name.eq_ignore_ascii_case(column)
            && match (table, c.table.as_deref()) {
                (Some(t), Some(ct)) => ct.eq_ignore_ascii_case(t),
                _ => true,
            }
    });
    let first = matches.next()?;
    matches.next().is_none().then_some(first.0)
}

/// True when two join-key columns produce equal hash keys exactly when they
/// compare equal: identical types, or two numeric types.
fn keys_hash_alike(
    left: &Expr,
    left_schema: &[ColumnInfo],
    right: &Expr,
    right_schema: &[ColumnInfo],
) -> bool {
    let column_type = |expr: &Expr, schema: &[ColumnInfo]| match expr {
        Expr::Column { table, column } => {
            resolve_outer_key(schema, table.as_deref(), column).map(|i| schema[i].data_type.clone())
        }
        _ => None,
    };
    // Every numeric type shares one canonical hash key.
    let is_numeric = |t: &DataType| {
        matches!(
            t,
            DataType::SmallInt
                | DataType::Int
                | DataType::BigInt
                | DataType::Real
                | DataType::DoublePrecision
                | DataType::Numeric { .. }
                | DataType::Decimal { .. }
        )
    };
    match (
        column_type(left, left_schema),
        column_type(right, right_schema),
    ) {
        (Some(l), Some(r)) => l == r || (is_numeric(&l) && is_numeric(&r)),
        _ => false,
    }
}

// ── HAVING aggregate rewriting ────────────────────────────────────────────────

/// Rewrite aggregate calls inside a HAVING predicate into column references
/// to the matching computed aggregate outputs.
///
/// `HAVING COUNT(*) >= 2` becomes `HAVING cnt >= 2`, where `cnt` is the
/// output name the aggregate operator assigned (the SELECT alias when one
/// was given, otherwise the canonical function name).
///
/// `logical_aggs[i]` and `physical_aggs[i]` describe the same aggregate call
/// (they are built in lockstep by [`Self::plan_aggregate_with_ctes`]).
pub(super) fn rewrite_having_aggregates(
    node: &rook_ast::PredicateNode,
    logical_aggs: &[rook_ast::logical::AggregateExpr],
    physical_aggs: &[AggregateInfo],
) -> rook_ast::PredicateNode {
    use rook_ast::PredicateNode as P;

    match node {
        P::BinaryOp { left, op, right } => P::BinaryOp {
            left: Box::new(rewrite_having_aggregates(left, logical_aggs, physical_aggs)),
            op: *op,
            right: Box::new(rewrite_having_aggregates(
                right,
                logical_aggs,
                physical_aggs,
            )),
        },
        P::Not(inner) => P::Not(Box::new(rewrite_having_aggregates(
            inner,
            logical_aggs,
            physical_aggs,
        ))),
        P::Compare { left, op, right } => P::Compare {
            left: Box::new(rewrite_having_expr(left, logical_aggs, physical_aggs)),
            op: *op,
            right: Box::new(rewrite_having_expr(right, logical_aggs, physical_aggs)),
        },
        P::IsNull(e) => P::IsNull(Box::new(rewrite_having_expr(
            e,
            logical_aggs,
            physical_aggs,
        ))),
        P::IsNotNull(e) => P::IsNotNull(Box::new(rewrite_having_expr(
            e,
            logical_aggs,
            physical_aggs,
        ))),
        P::Between { expr, low, high } => P::Between {
            expr: Box::new(rewrite_having_expr(expr, logical_aggs, physical_aggs)),
            low: Box::new(rewrite_having_expr(low, logical_aggs, physical_aggs)),
            high: Box::new(rewrite_having_expr(high, logical_aggs, physical_aggs)),
        },
        P::InList { expr, list } => P::InList {
            expr: Box::new(rewrite_having_expr(expr, logical_aggs, physical_aggs)),
            list: list
                .iter()
                .map(|e| rewrite_having_expr(e, logical_aggs, physical_aggs))
                .collect(),
        },
        P::Like {
            expr,
            pattern,
            escape_char,
        } => P::Like {
            expr: Box::new(rewrite_having_expr(expr, logical_aggs, physical_aggs)),
            pattern: pattern.clone(),
            escape_char: *escape_char,
        },
        P::IsDistinctFrom { left, right } => P::IsDistinctFrom {
            left: Box::new(rewrite_having_expr(left, logical_aggs, physical_aggs)),
            right: Box::new(rewrite_having_expr(right, logical_aggs, physical_aggs)),
        },
        P::IsBoolean {
            expr,
            test,
            negated,
        } => P::IsBoolean {
            expr: Box::new(rewrite_having_expr(expr, logical_aggs, physical_aggs)),
            test: *test,
            negated: *negated,
        },
        // Subquery predicates are materialised elsewhere; leave them untouched.
        other => other.clone(),
    }
}

/// Rewrite one HAVING expression, replacing aggregate function calls with
/// column references to their computed output columns.
fn rewrite_having_expr(
    expr: &rook_ast::ExprNode,
    logical_aggs: &[rook_ast::logical::AggregateExpr],
    physical_aggs: &[AggregateInfo],
) -> rook_ast::ExprNode {
    use rook_ast::ExprNode as E;

    match expr {
        E::Function {
            name,
            args,
            distinct,
        } if is_aggregate_name(name) => {
            let wanted_args = render_function_args(args);
            for (i, agg) in logical_aggs.iter().enumerate() {
                if aggregate_function_name(agg.function) == name.to_ascii_uppercase()
                    && agg.distinct == *distinct
                    && render_agg_args(agg) == wanted_args
                {
                    // Found the matching computed aggregate — reference it.
                    return E::Column(physical_aggs[i].output_name.clone());
                }
            }
            // No matching computed aggregate — keep the call; evaluation will
            // report a clear error instead of silently passing.
            expr.clone()
        }
        _ => expr.clone(),
    }
}

fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
    )
}

/// Map a logical aggregate function to its SQL spelling.
fn aggregate_function_name(f: rook_ast::logical::AggregateFunction) -> String {
    use rook_ast::logical::AggregateFunction as F;
    match f {
        F::Count => "COUNT".to_string(),
        F::Sum => "SUM".to_string(),
        F::Avg => "AVG".to_string(),
        F::Min => "MIN".to_string(),
        F::Max => "MAX".to_string(),
    }
}

/// Render the argument list of a HAVING function call for comparison:
/// `*` for the star argument, output names otherwise.
fn render_function_args(args: &[rook_ast::FunctionArg]) -> String {
    args.iter()
        .map(|a| match a {
            rook_ast::FunctionArg::Star => "*".to_string(),
            rook_ast::FunctionArg::Expr(e) => super::helpers::ast_expr_to_output_name(e),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Render a logical aggregate's arguments the same way. `COUNT(*)` carries no
/// AST arguments (the star is stripped during extraction), so it renders as "*".
fn render_agg_args(agg: &rook_ast::logical::AggregateExpr) -> String {
    if agg.args.is_empty() {
        "*".to_string()
    } else {
        agg.args
            .iter()
            .map(super::helpers::ast_expr_to_output_name)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

// ── Equi-join key extraction ─────────────────────────────────────────────────

/// Does `expr` reference only columns from `schema`?
fn expr_bound_to(expr: &Expr, schema: &[ColumnInfo]) -> bool {
    match expr {
        Expr::Column { table, column } => schema.iter().any(|c| {
            c.name.eq_ignore_ascii_case(column)
                && match table {
                    Some(t) => c
                        .table
                        .as_deref()
                        .map(|ct| ct.eq_ignore_ascii_case(t))
                        .unwrap_or(true),
                    None => true,
                }
        }),
        Expr::Constant(_) | Expr::Null => true,
        Expr::Add(a, b) | Expr::Sub(a, b) | Expr::Mul(a, b) | Expr::Div(a, b) => {
            expr_bound_to(a, schema) && expr_bound_to(b, schema)
        }
        // Conservative: anything else (CASE, CAST over unknown, correlated
        // params…) stays out of hash keys.
        _ => false,
    }
}

/// Split an equality join predicate into (build_keys, probe_keys, residual).
///
/// A conjunct `L = R` becomes a hash-key pair when one side references only
/// left-operator columns and the other only right-operator columns. Equality
/// conjuncts that don't qualify (same-side or non-column expressions) and all
/// non-equality conjuncts are returned as the residual predicate.
fn extract_equi_join_keys(
    pred: &Predicate,
    left_schema: &[ColumnInfo],
    right_schema: &[ColumnInfo],
) -> Option<(Vec<Expr>, Vec<Expr>, Option<Predicate>)> {
    let mut build_keys = Vec::new();
    let mut probe_keys = Vec::new();
    let mut residual: Vec<&Predicate> = Vec::new();

    fn walk<'p>(
        p: &'p Predicate,
        l: &[ColumnInfo],
        r: &[ColumnInfo],
        bk: &mut Vec<Expr>,
        pk: &mut Vec<Expr>,
        res: &mut Vec<&'p Predicate>,
    ) {
        match p {
            Predicate::And(a, b) => {
                walk(a, l, r, bk, pk, res);
                walk(b, l, r, bk, pk, res);
            }
            Predicate::Compare(lhs, ComparisonOp::Equals, rhs) => {
                let l_has_lhs = expr_bound_to(lhs, l);
                let r_has_lhs = expr_bound_to(lhs, r);
                let l_has_rhs = expr_bound_to(rhs, l);
                let r_has_rhs = expr_bound_to(rhs, r);
                if l_has_lhs && r_has_rhs && !r_has_lhs {
                    bk.push(lhs.clone());
                    pk.push(rhs.clone());
                } else if l_has_rhs && r_has_lhs && !l_has_lhs {
                    bk.push(rhs.clone());
                    pk.push(lhs.clone());
                } else {
                    res.push(p);
                }
            }
            other => res.push(other),
        }
    }

    walk(
        pred,
        left_schema,
        right_schema,
        &mut build_keys,
        &mut probe_keys,
        &mut residual,
    );

    let residual_pred = residual.into_iter().cloned().reduce(Predicate::and);
    Some((build_keys, probe_keys, residual_pred))
}
