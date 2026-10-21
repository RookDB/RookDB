//! Join, aggregate, and recursive CTE planning for the PhysicalPlanner.

use rook_ast::logical::*;

use super::super::expr::{
    ComparisonOp, Expr, Predicate, binding_names, expr_from_ast, predicate_from_ast,
};
use super::super::operators::{
    AggregateFunction, AggregateInfo, AggregateOperator, HashJoinOperator,
    IndexNestedLoopJoinOperator, JoinType as PhysicalJoinType, KeyClass, MergedColumnsOperator,
    NestedLoopJoinOperator, PhysicalOperator, infer_aggregate_output_type, mark_merged,
};
use super::super::tuple::ColumnInfo;
use super::helpers::infer_expr_type_in_schema;
use super::{PhysicalPlanner, PlannerStats};
use crate::backend::error::{RookError, RookResult};
use crate::statistics::estimator::{Estimator, hash_join_cost, index_nested_loop_cost};
use crate::types::datatype::DataType;

impl PhysicalPlanner {
    /// Plan a join logical node into a physical operator.
    ///
    /// Three-tier algorithm selection (see `plan_join_with_ctes` body):
    /// 1. IndexNestedLoopJoin — inner side is a plain table scan with an index
    ///    on the join key, join is INNER/LEFT, and probing the index once per
    ///    estimated outer row costs less than a hash join.
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
        // The columns a USING / NATURAL join shows once; the join's output
        // marks the extra copies so a bare reference to one is not ambiguous.
        let merged_names: Vec<String> = match j.join_type {
            rook_ast::JoinType::Natural => left
                .schema()
                .iter()
                .filter(|l| {
                    right
                        .schema()
                        .iter()
                        .any(|r| r.name.eq_ignore_ascii_case(&l.name))
                })
                .map(|l| l.name.clone())
                .collect(),
            _ => j.using.clone(),
        };
        let keep_right = j.join_type == rook_ast::JoinType::Right;
        let wrap = |op: Box<dyn PhysicalOperator>| -> Box<dyn PhysicalOperator> {
            if merged_names.is_empty() {
                op
            } else {
                Box::new(MergedColumnsOperator::new(
                    op,
                    &merged_names,
                    keep_right,
                    j.join_type == rook_ast::JoinType::Full,
                ))
            }
        };

        // A hash join outputs only the columns read above it, or failing
        // that the columns the query reads. Not under a USING / NATURAL
        // join, whose merged columns are found by position.
        let narrow = |join: HashJoinOperator| -> HashJoinOperator {
            let read = self.read_columns.borrow();
            let above = self.join_outputs.borrow();
            let names = above
                .get(&(j as *const LogicalJoin as usize))
                .or(read.as_ref());
            match names {
                Some(names) if read.is_some() && merged_names.is_empty() => {
                    join.keep_columns(|c| names.contains(&c.name.to_ascii_lowercase()))
                }
                _ => join,
            }
        };

        let mut combined_schema: Vec<ColumnInfo> = left
            .schema()
            .iter()
            .cloned()
            .chain(right.schema().iter().cloned())
            .collect();
        mark_merged(&mut combined_schema, &merged_names, keep_right);

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
                        // Qualified by the left column's table, so a chain
                        // of joins that already has the name twice is unambiguous.
                        let left_table = left_schema
                            .iter()
                            .find(|l| l.name.eq_ignore_ascii_case(&col_name) && !l.merged)
                            .or_else(|| {
                                left_schema
                                    .iter()
                                    .find(|l| l.name.eq_ignore_ascii_case(&col_name))
                            })
                            .and_then(|l| l.table.clone());
                        equality_preds.push(Predicate::Compare(
                            Expr::Column {
                                table: left_table,
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
                    let column_names = binding_names(&combined_schema);
                    let qualified;
                    let pred_node = if j.using.is_empty() {
                        pred_node
                    } else {
                        qualified = qualify_using_keys(pred_node, left.schema(), &j.using);
                        &qualified
                    };
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

                // Probe the index once per outer row, or scan the inner side
                // once through a hash table built on the outer rows.
                let outer_rows = left.estimate_cardinality() as f64;
                let inner_rows = right.estimate_cardinality() as f64;
                let matches = Estimator::new(&PlannerStats(self)).rows_per_key(&j.right, inner_col);
                let index_cost = index_nested_loop_cost(outer_rows, matches);
                let hash_cost = hash_join_cost(outer_rows, inner_rows);
                let index_is_cheaper = index_cost <= hash_cost;
                if matched_idx.is_some() && !index_is_cheaper {
                    self.note(|| {
                        format!(
                            "HashJoin chosen over the index on '{}': about {} outer and {} inner row(s), estimated cost {:.0} against {:.0} for probing",
                            inner_ts.table, outer_rows, inner_rows, hash_cost, index_cost
                        )
                    });
                }

                if let Some((idx_name, _, _)) = matched_idx
                    && index_is_cheaper
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
                    if crate::backend::cache::file_exists(self.catalog.engine(), &idx_path)
                        && crate::backend::cache::file_exists(self.catalog.engine(), &heap_path)
                        && let Some(outer_key_idx) =
                            resolve_outer_key(&left_schema, outer_table.as_deref(), outer_col)
                    {
                        let key_type = right_schema
                            .iter()
                            .find(|c| c.name.eq_ignore_ascii_case(inner_col))
                            .map(|c| c.data_type.clone())
                            .unwrap_or(DataType::Int);
                        let index = crate::backend::cache::CachedIndex::new(
                            std::sync::Arc::clone(self.catalog.engine()),
                            idx_path,
                            vec![key_type],
                        );
                        let heap_manager = crate::backend::cache::lease_heap_owned(
                            self.catalog.engine(),
                            &heap_path,
                        )
                        .map_err(|e| {
                            e.with_context(format!(
                                "opening heap {} for table '{}'",
                                heap_path.display(),
                                inner_ts.table
                            ))
                        })?;

                        self.note(|| {
                            format!(
                                "IndexNestedLoopJoin chosen: about {} outer row(s) probe index '{}' of '{}', estimated cost {:.0} against {:.0} for a hash join",
                                outer_rows, idx_name, inner_ts.table, index_cost, hash_cost
                            )
                        });
                        return Ok(wrap(Box::new(
                            IndexNestedLoopJoinOperator::new(
                                left,
                                index,
                                heap_manager,
                                outer_key_idx,
                                right_schema,
                                residual,
                                join_type,
                            )
                            .described(&inner_ts.table, idx_name),
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
                    .map(|keys| hashable_keys(keys, &left_schema, &right_schema))
                && !build_keys.is_empty()
            {
                // Hash the smaller input; ties keep the left one.
                let build_left = left.estimate_cardinality() <= right.estimate_cardinality();
                return Ok(wrap(Box::new(narrow(HashJoinOperator::with_build_side(
                    left,
                    right,
                    build_keys,
                    probe_keys,
                    residual,
                    PhysicalJoinType::Inner,
                    build_left,
                )))));
            }
        }

        // 3. Outer equi-joins use the hash join too. Limited to keys whose
        // two sides hash alike (one key class); other key pairs keep the
        // plain nested loop.
        if matches!(
            join_type,
            PhysicalJoinType::Left | PhysicalJoinType::Right | PhysicalJoinType::Full
        ) && let Some(pred) = &predicate
        {
            let left_schema = left.schema().to_vec();
            let right_schema = right.schema().to_vec();
            if let Some((left_keys, right_keys, residual)) =
                extract_equi_join_keys(pred, &left_schema, &right_schema)
                && !left_keys.is_empty()
                && left_keys
                    .iter()
                    .zip(right_keys.iter())
                    .all(|(l, r)| keys_hash_alike(l, &left_schema, r, &right_schema))
            {
                let build_left = left.estimate_cardinality() <= right.estimate_cardinality();
                return Ok(wrap(Box::new(narrow(HashJoinOperator::with_build_side(
                    left, right, left_keys, right_keys, residual, join_type, build_left,
                )))));
            }
        }

        // 4. Fallback: NestedLoopJoin for all join types
        Ok(wrap(Box::new(NestedLoopJoinOperator::new(
            left, right, predicate, join_type,
        ))))
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
        let column_names = binding_names(child_schema);

        // ─── Resolve GROUP BY expressions ────────────────────────────────
        let mut group_by_exprs = Vec::new();
        let mut group_by_names = Vec::new();
        let mut group_by_types = Vec::new();

        for expr_node in &a.group_by {
            let expr = expr_from_ast(expr_node, &column_names)?;
            let name = crate::planner::helpers::group_key_name(expr_node);
            let data_type = match expr_node {
                rook_ast::ExprNode::ScalarSubquery(_) => {
                    return Err(RookError::InvalidQuery(
                        "Scalar subqueries are not allowed in GROUP BY".to_string(),
                    ));
                }
                rook_ast::ExprNode::Function { name, .. } if is_aggregate_name(name) => {
                    return Err(RookError::InvalidQuery(
                        "Aggregate function calls are not allowed in GROUP BY".to_string(),
                    ));
                }
                // A NULL key has no type of its own; any type holds it.
                rook_ast::ExprNode::Constant(rook_ast::ConstantValue::Null) => DataType::Int,
                _ => infer_expr_type_in_schema(expr_node, child_schema)?,
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
                infer_expr_type_in_schema(arg, child_schema).ok()
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
                    merged: false,
                    name: name.clone(),
                    data_type: dt.clone(),
                    table: None,
                })
                .collect();
            for agg in &aggregates {
                having_schema.push(ColumnInfo {
                    merged: false,
                    name: agg.output_name.clone(),
                    data_type: agg.output_type.clone(),
                    table: None,
                });
            }
            let having_names = binding_names(&having_schema);
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
        let cte_key = rc.name.to_ascii_lowercase();
        // Scans of this CTE are untyped; an outer CTE of the same name is
        // hidden until the outer query has been planned.
        let shadowed = self.cte_types.borrow_mut().remove(&cte_key);
        // Use the recursive CTE name as the table qualifier so that
        // table-qualified column references and NATURAL JOIN disambiguation
        // work correctly when a recursive CTE appears in a join context.
        let cte_table_name = rc.name.clone();
        let _schema: Vec<super::super::tuple::ColumnInfo> = rc
            .schema
            .columns
            .iter()
            .map(|c| super::super::tuple::ColumnInfo {
                merged: false,
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
        self.note_cte(&rc.name, || non_rec_op.explain());
        let anchor_rows = working_table.len();

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
                return Err(RookError::Evaluation(format!(
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

            // Append to accumulated result (clone since new_tuples becomes the next working table)
            all_tuples.extend(new_tuples.iter().cloned());
            working_table = new_tuples;
            iteration += 1;
        }

        // 3. Register all accumulated tuples and plan the outer query
        self.note(|| {
            format!(
                "Recursive CTE '{}' materialised: {} anchor row(s), {} iteration(s), {} row(s) in total",
                rc.name,
                anchor_rows,
                iteration,
                all_tuples.len()
            )
        });
        cte_registry.insert(cte_key.clone(), all_tuples);

        // Plan the outer query which may reference the CTE via CteScan
        let outer = self.plan_internal(&rc.outer, cte_registry);
        if let Some(types) = shadowed {
            self.cte_types.borrow_mut().insert(cte_key, types);
        }
        outer
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

/// In a chain, `USING (k)` compares the merged `k` of everything on the
/// left (written bare) with the new table's. Qualify that bare name with
/// the table the left side's `k` comes from, so the key still separates the
/// two sides and the join can hash. A coalesced column has no table and
/// stays bare.
fn qualify_using_keys(
    node: &rook_ast::PredicateNode,
    left: &[ColumnInfo],
    names: &[String],
) -> rook_ast::PredicateNode {
    use rook_ast::{ExprNode, PredicateNode};
    match node {
        PredicateNode::BinaryOp {
            left: l,
            op,
            right: r,
        } => PredicateNode::BinaryOp {
            left: Box::new(qualify_using_keys(l, left, names)),
            op: *op,
            right: Box::new(qualify_using_keys(r, left, names)),
        },
        PredicateNode::Compare {
            left: l,
            op,
            right: r,
        } => {
            let table = match &**l {
                ExprNode::Column(c) if names.iter().any(|n| n.eq_ignore_ascii_case(c)) => {
                    super::super::expr::column_position(left, None, c)
                        .and_then(|i| left[i].table.clone())
                        .map(|t| (t, c.clone()))
                }
                _ => None,
            };
            PredicateNode::Compare {
                left: match table {
                    Some((t, c)) => Box::new(ExprNode::Compound(vec![t, c])),
                    None => l.clone(),
                },
                op: *op,
                right: r.clone(),
            }
        }
        other => other.clone(),
    }
}

/// Equi-join keys with each pair whose sides may hash differently (see
/// [`KeyClass`]) moved into the residual predicate, where it is compared
/// rather than hashed: sides of different classes, or an expression whose
/// class is not known.
fn hashable_keys(
    (left_keys, right_keys, residual): (Vec<Expr>, Vec<Expr>, Option<Predicate>),
    left_schema: &[ColumnInfo],
    right_schema: &[ColumnInfo],
) -> (Vec<Expr>, Vec<Expr>, Option<Predicate>) {
    let mut keys = (Vec::new(), Vec::new());
    let mut residual = residual;
    for (l, r) in left_keys.into_iter().zip(right_keys) {
        let column = |e: &Expr| matches!(e, Expr::Column { .. });
        let hash = match key_classes(&l, left_schema, &r, right_schema) {
            Some((a, b)) => a == b,
            // A column that does not resolve is left as it was planned.
            None => column(&l) && column(&r),
        };
        if hash {
            keys.0.push(l);
            keys.1.push(r);
        } else {
            let pair = Predicate::Compare(l, ComparisonOp::Equals, r);
            residual = Some(match residual {
                Some(rest) => Predicate::and(rest, pair),
                None => pair,
            });
        }
    }
    (keys.0, keys.1, residual)
}

/// True when two join-key columns produce equal hash keys exactly when they
/// compare equal: their types share a [`KeyClass`].
fn keys_hash_alike(
    left: &Expr,
    left_schema: &[ColumnInfo],
    right: &Expr,
    right_schema: &[ColumnInfo],
) -> bool {
    key_classes(left, left_schema, right, right_schema).is_some_and(|(l, r)| l == r)
}

/// The key classes of two join keys, when both are known.
fn key_classes(
    left: &Expr,
    left_schema: &[ColumnInfo],
    right: &Expr,
    right_schema: &[ColumnInfo],
) -> Option<(KeyClass, KeyClass)> {
    Some((
        key_class(left, left_schema)?,
        key_class(right, right_schema)?,
    ))
}

/// The [`KeyClass`] of a join key's values: a column's type, a constant's,
/// or exact arithmetic on exact operands. `None` when not known.
fn key_class(expr: &Expr, schema: &[ColumnInfo]) -> Option<KeyClass> {
    match expr {
        Expr::Column { table, column } => resolve_outer_key(schema, table.as_deref(), column)
            .map(|i| KeyClass::of_type(&schema[i].data_type)),
        Expr::Constant(value) => Some(KeyClass::of_value(value)),
        Expr::Add(a, b) | Expr::Sub(a, b) | Expr::Mul(a, b) | Expr::Div(a, b) => {
            let exact = Some(KeyClass::Exact);
            (key_class(a, schema) == exact && key_class(b, schema) == exact)
                .then_some(KeyClass::Exact)
        }
        _ => None,
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
