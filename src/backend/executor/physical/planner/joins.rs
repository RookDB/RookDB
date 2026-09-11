//! Join, aggregate, and recursive CTE planning for the PhysicalPlanner.

use rook_ast::logical::*;

use super::super::tuple::ColumnInfo;
use super::super::expr::{Predicate, Expr, ComparisonOp, expr_from_ast, predicate_from_ast};
use super::super::operators::{
    PhysicalOperator,
    NestedLoopJoinOperator,
    HashJoinOperator,
    JoinType as PhysicalJoinType,
    AggregateOperator,
    AggregateInfo,
    AggregateFunction,
    infer_aggregate_output_type,
};
use crate::types::datatype::DataType;
use super::PhysicalPlanner;
use super::helpers::{ast_expr_to_output_name, infer_expr_type_from_ast};

impl PhysicalPlanner {
    /// Plan a join logical node into a physical operator.
    ///
    /// Uses NestedLoopJoin for all join types. HashJoin will be added
    /// in a future optimisation pass when we can reliably decompose
    /// equality conditions from mixed predicates.
    pub(crate) fn plan_join_with_ctes(
        &self,
        j: &LogicalJoin,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::super::tuple::Tuple>>,
    ) -> Result<Box<dyn PhysicalOperator>, String> {
        let left = self.plan_internal(&j.left, cte_registry)?;
        let right = self.plan_internal(&j.right, cte_registry)?;

        // Build combined schema for predicate resolution (left cols + right cols).
        // ColumnInfo.table is populated with actual table names from each operator's
        // schema (set during plan_table_scan), so table-qualified references like
        // `t1.id` and `t2.id` resolve correctly without sentinel markers.
        let combined_schema: Vec<ColumnInfo> = left.schema().iter()
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
                    if left_schema.iter().any(|l| l.name.eq_ignore_ascii_case(&r_col.name)) {
                        let col_name = r_col.name.clone();
                        // Use the right column's actual table qualifier for the rhs
                        let right_table = r_col.table.clone();
                        equality_preds.push(Predicate::Compare(
                            Expr::Column { table: None, column: col_name.clone() },
                            ComparisonOp::Equals,
                            Expr::Column { table: right_table, column: col_name },
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
                    let column_names: Vec<String> = combined_schema.iter()
                        .map(|c| c.name.clone())
                        .collect();
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
            rook_ast::JoinType::Cross => PhysicalJoinType::Cross,
            rook_ast::JoinType::Natural => PhysicalJoinType::Inner,
        };

        log::info!(
            "[Volcano] Planning {:?} join with predicate={}",
            join_type,
            if predicate.is_some() { "yes" } else { "no" }
        );

        // Use a hash join when the condition contains at least one
        // cross-side equality conjunct (INNER joins only) — turns O(n·m)
        // nested-loop joins into O(n+m). Remaining non-equality conjuncts are
        // evaluated as a post-join filter by HashJoinOperator.
        if matches!(join_type, PhysicalJoinType::Inner)
            && let Some(pred) = &predicate {
                let left_schema = left.schema().to_vec();
                let right_schema = right.schema().to_vec();
                if let Some((build_keys, probe_keys, residual)) =
                    extract_equi_join_keys(pred, &left_schema, &right_schema)
                    && !build_keys.is_empty() {
                        let left_card = left.estimate_cardinality();
                        let right_card = right.estimate_cardinality();

                        // CBO Sizing & Swap:
                        // Build on the smaller relation to bound the in-memory hash table footprint.
                        let swap = right_card > 0 && (left_card == 0 || right_card < left_card);

                        log::info!(
                            "[Volcano] Using HashJoin: {} equi-key pair(s), residual: {}, swap={} (left_card={}, right_card={})",
                            build_keys.len(),
                            residual.is_some(),
                            swap,
                            left_card,
                            right_card
                        );

                        if swap {
                            return Ok(Box::new(HashJoinOperator::new_with_swap(
                                right,
                                left,
                                probe_keys,
                                build_keys,
                                residual,
                                true,
                            )));
                        } else {
                            return Ok(Box::new(HashJoinOperator::new_with_swap(
                                left,
                                right,
                                build_keys,
                                probe_keys,
                                residual,
                                false,
                            )));
                        }
                    }
            }

        // Use NestedLoopJoin for all join types
        Ok(Box::new(NestedLoopJoinOperator::new(
            left,
            right,
            predicate,
            join_type,
        )))
    }

    /// Plan an aggregate (GROUP BY / HAVING) logical node into a physical operator.
    pub(crate) fn plan_aggregate_with_ctes(
        &self,
        a: &LogicalAggregate,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::super::tuple::Tuple>>,
    ) -> Result<Box<dyn PhysicalOperator>, String> {
        let child = self.plan_internal(&a.child, cte_registry)?;
        let child_schema = child.schema();
        let column_names: Vec<String> = child_schema.iter().map(|c| c.name.clone()).collect();
        let child_types: Vec<_> = child_schema.iter().map(|c| c.data_type.clone()).collect();

        // ─── Resolve GROUP BY expressions ────────────────────────────────
        let mut group_by_exprs = Vec::new();
        let mut group_by_names = Vec::new();
        let mut group_by_types = Vec::new();

        for expr_node in &a.group_by {
            let expr = expr_from_ast(expr_node, &column_names)?;
            let name = ast_expr_to_output_name(expr_node);
            let data_type = match expr_node {
                rook_ast::ExprNode::Column(name) => {
                    let idx = column_names.iter().position(|c| c == name)
                        .ok_or_else(|| format!("Column '{}' not found in GROUP BY", name))?;
                    child_types[idx].clone()
                }
                rook_ast::ExprNode::Compound(parts) => {
                    let name = parts.last().ok_or_else(|| "Empty compound identifier".to_string())?;
                    let idx = column_names.iter().position(|c| c == name)
                        .ok_or_else(|| format!("Column '{}' not found in GROUP BY", name))?;
                    child_types[idx].clone()
                }
                rook_ast::ExprNode::Constant(cv) => {
                    match cv {
                        rook_ast::ConstantValue::Null => DataType::Int,
                        rook_ast::ConstantValue::Int(_) => DataType::Int,
                        rook_ast::ConstantValue::Float(_) => DataType::DoublePrecision,
                        rook_ast::ConstantValue::Text(_) => DataType::Varchar(u16::MAX),
                        rook_ast::ConstantValue::Boolean(_) => DataType::Bool,
                    }
                }
                rook_ast::ExprNode::Cast { data_type, .. } => {
                    data_type.parse::<DataType>()
                        .map_err(|e| format!("Invalid CAST target type '{}': {}", data_type, e))?
                }
                rook_ast::ExprNode::Binary { .. } => DataType::Int,
                // Scalar subqueries are not expected in GROUP BY expressions
                rook_ast::ExprNode::ScalarSubquery(_) => {
                    return Err("Scalar subqueries are not allowed in GROUP BY".to_string());
                }
                // Aggregate functions in GROUP BY are not valid SQL — return error
                rook_ast::ExprNode::Function { .. } => {
                    return Err("Aggregate function calls are not allowed in GROUP BY".to_string());
                }
                rook_ast::ExprNode::Case { .. } => {
                    return Err("CASE expressions are not allowed in GROUP BY".to_string());
                }
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

            // Warn if COUNT(DISTINCT expr) — not yet fully implemented
            if agg_expr.distinct {
                log::warn!(
                    "DISTINCT aggregate functions are not yet fully implemented; '{:?}' will behave as non-DISTINCT",
                    agg_expr.function
                );
            }

            // Determine output name
            let output_name = agg_expr.alias.clone().unwrap_or_else(|| {
                format!("{:?}({})", agg_expr.function, agg_expr.args.len())
            });

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
            let having_node =
                rewrite_having_aggregates(having_node, &a.aggregates, &aggregates);
            let mut having_schema: Vec<ColumnInfo> = group_by_names.iter().zip(group_by_types.iter())
                .map(|(name, dt)| ColumnInfo {
                    name: name.clone(),
                    data_type: dt.clone(), table: None })
                .collect();
            for agg in &aggregates {
                having_schema.push(ColumnInfo {
                    name: agg.output_name.clone(),
                    data_type: agg.output_type.clone(), table: None });
            }
            let having_names: Vec<String> = having_schema.iter().map(|c| c.name.clone()).collect();
            Some(predicate_from_ast(&having_node, &having_names)?)
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
    ) -> Result<Box<dyn PhysicalOperator>, String> {
        log::info!("[Volcano] Planning recursive CTE '{}'", rc.name);
        let cte_key = rc.name.to_ascii_lowercase();
        // Use the recursive CTE name as the table qualifier so that
        // table-qualified column references and NATURAL JOIN disambiguation
        // work correctly when a recursive CTE appears in a join context.
        let cte_table_name = rc.name.clone();
        let _schema: Vec<super::super::tuple::ColumnInfo> = rc.schema.columns.iter().map(|c| {
            super::super::tuple::ColumnInfo {
                name: c.name.clone(),
                data_type: crate::types::datatype::DataType::Varchar(u16::MAX),
                table: Some(cte_table_name.clone()),
            }
        }).collect();

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

        // Accumulated result: start with non-recursive term's output
        let mut all_tuples: Vec<super::super::tuple::Tuple> = working_table.clone();

        // 2. Fixpoint iteration
        let max_iterations = 10_000; // safety limit to prevent infinite loops
        for iteration in 0..max_iterations {
            if working_table.is_empty() {
                log::info!(
                    "[Volcano] Recursive CTE '{}' iteration {}: working table empty, done",
                    rc.name, iteration
                );
                break;
            }

            // Register the current working table so CteScan leaves can read it
            cte_registry.insert(cte_key.clone(), working_table.clone());

            // Plan and execute the recursive term
            let mut rec_op = self.plan_internal(&rc.recursive, cte_registry)?;
            let mut new_tuples: Vec<super::super::tuple::Tuple> = Vec::new();
            while let Some(tuple) = rec_op.next()? {
                new_tuples.push(tuple);
            }

            log::info!(
                "[Volcano] Recursive CTE '{}' iteration {}: produced {} tuples",
                rc.name, iteration, new_tuples.len()
            );

            if new_tuples.is_empty() {
                log::info!(
                    "[Volcano] Recursive CTE '{}' iteration {}: no new tuples, done",
                    rc.name, iteration
                );
                break;
            }

            // For UNION DISTINCT (not ALL), deduplicate new tuples against all_tuples
            let deduped_new: Vec<super::super::tuple::Tuple> = if !rc.union_all {
                let existing_set: std::collections::HashSet<String> = all_tuples.iter().map(|t| {
                    t.values.iter().map(|v| match v {
                        Some(dv) => format!("{:?}", dv),
                        None => "\x00N\x00".to_string(),
                    }).collect::<Vec<_>>().join("|")
                }).collect();
                new_tuples.into_iter().filter(|t| {
                    let key = t.values.iter().map(|v| match v {
                        Some(dv) => format!("{:?}", dv),
                        None => "\x00N\x00".to_string(),
                    }).collect::<Vec<_>>().join("|");
                    !existing_set.contains(&key)
                }).collect()
            } else {
                new_tuples
            };

            let num_appended = deduped_new.len();
            if num_appended == 0 {
                log::info!(
                    "[Volcano] Recursive CTE '{}' iteration {}: no new (non-duplicate) tuples, done",
                    rc.name, iteration
                );
                break;
            }

            // Append to accumulated result (clone since deduped_new becomes the next working table)
            all_tuples.extend(deduped_new.iter().cloned());
            working_table = deduped_new;
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
            right: Box::new(rewrite_having_aggregates(right, logical_aggs, physical_aggs)),
        },
        P::Not(inner) => {
            P::Not(Box::new(rewrite_having_aggregates(inner, logical_aggs, physical_aggs)))
        }
        P::Compare { left, op, right } => P::Compare {
            left: Box::new(rewrite_having_expr(left, logical_aggs, physical_aggs)),
            op: *op,
            right: Box::new(rewrite_having_expr(right, logical_aggs, physical_aggs)),
        },
        P::IsNull(e) => P::IsNull(Box::new(rewrite_having_expr(e, logical_aggs, physical_aggs))),
        P::IsNotNull(e) => {
            P::IsNotNull(Box::new(rewrite_having_expr(e, logical_aggs, physical_aggs)))
        }
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
        P::Like { expr, pattern, escape_char } => P::Like {
            expr: Box::new(rewrite_having_expr(expr, logical_aggs, physical_aggs)),
            pattern: pattern.clone(),
            escape_char: *escape_char,
        },
        P::IsDistinctFrom { left, right } => P::IsDistinctFrom {
            left: Box::new(rewrite_having_expr(left, logical_aggs, physical_aggs)),
            right: Box::new(rewrite_having_expr(right, logical_aggs, physical_aggs)),
        },
        P::IsBoolean { expr, test, negated } => P::IsBoolean {
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
        E::Function { name, args, .. } if is_aggregate_name(name) => {
            let wanted_args = render_function_args(args);
            for (i, agg) in logical_aggs.iter().enumerate() {
                if aggregate_function_name(agg.function) == name.to_ascii_uppercase()
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
            rook_ast::FunctionArg::Expr(e) => {
                super::helpers::ast_expr_to_output_name(e)
            }
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
                    Some(t) => c.table.as_deref().map(|ct| ct.eq_ignore_ascii_case(t)).unwrap_or(true),
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

    walk(pred, left_schema, right_schema, &mut build_keys, &mut probe_keys, &mut residual);

    let residual_pred = residual.into_iter().cloned().reduce(Predicate::and);
    Some((build_keys, probe_keys, residual_pred))
}
