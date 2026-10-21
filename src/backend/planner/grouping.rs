//! Planning a grouped SELECT: its keys, its aggregate calls, and the
//! expressions above the aggregation rewritten to read their outputs.
//!
//! The aggregate's output columns are identified by name. A key that is a
//! column keeps that column's name and qualifier, so it is read as it was
//! below; a computed key (`GROUP BY a + 1`) and each distinct aggregate call
//! get a made-up name (`#key0`, `#agg0`) that no query can spell, so they
//! never meet a user's alias or column (`SELECT a, SUM(b) AS a ... GROUP BY
//! a`). Everything above the aggregation (SELECT list, HAVING, ORDER BY) is
//! rewritten by [`Grouping::rewrite`] to read those columns, which is also
//! where a column that is neither grouped nor aggregated is reported.

use rook_ast::logical::{
    AggregateExpr, AggregateFunction, LogicalAggregate, LogicalPlan, NamedExpr,
};
use rook_ast::{ExprNode, FunctionArg, PredicateNode, SelectExpr, SelectPlan};

use crate::backend::error::{RookError, RookResult};

/// The keys and aggregate calls of one grouped SELECT.
pub struct Grouping {
    keys: Vec<NamedExpr>,
    aggregates: Vec<AggregateExpr>,
}

/// Whether `select` aggregates: it groups, filters groups, or calls an
/// aggregate function in its SELECT list.
pub fn is_grouped(select: &SelectPlan) -> bool {
    !select.group_by.is_empty()
        || select.having.is_some()
        || select.projections.iter().any(|p| match p {
            SelectExpr::UnnamedExpr(e) | SelectExpr::ExprWithAlias { expr: e, .. } => {
                !aggregate_calls(e).is_empty()
            }
            SelectExpr::Wildcard | SelectExpr::QualifiedWildcard(_) => false,
        })
}

/// The aggregate function a call names, if it is one.
pub fn aggregate_function(name: &str) -> Option<AggregateFunction> {
    match name.to_ascii_uppercase().as_str() {
        "COUNT" => Some(AggregateFunction::Count),
        "SUM" => Some(AggregateFunction::Sum),
        "AVG" => Some(AggregateFunction::Avg),
        "MIN" => Some(AggregateFunction::Min),
        "MAX" => Some(AggregateFunction::Max),
        _ => None,
    }
}

/// The aggregate calls in `expr`, outermost first. A subquery's calls are
/// its own and are not included.
fn aggregate_calls(expr: &ExprNode) -> Vec<&ExprNode> {
    match expr {
        ExprNode::Function { name, .. } if aggregate_function(name).is_some() => vec![expr],
        other => other
            .children()
            .into_iter()
            .flat_map(aggregate_calls)
            .collect(),
    }
}

/// `call` (an aggregate function call) as an aggregate of the plan, named
/// `name`.
fn aggregate_expr(call: &ExprNode, name: String) -> Option<AggregateExpr> {
    let ExprNode::Function {
        name: function,
        args,
        distinct,
    } = call
    else {
        return None;
    };
    Some(AggregateExpr {
        function: aggregate_function(function)?,
        // `COUNT(*)` has no argument.
        args: args
            .iter()
            .filter_map(|a| match a {
                FunctionArg::Expr(e) => Some((**e).clone()),
                FunctionArg::Star => None,
            })
            .collect(),
        alias: Some(name),
        distinct: *distinct,
    })
}

fn is_column(expr: &ExprNode) -> bool {
    matches!(expr, ExprNode::Column(_) | ExprNode::Compound(_))
}

impl Grouping {
    /// The keys and aggregate calls of a bound `select`: the calls its SELECT
    /// list, HAVING and ORDER BY make, each distinct call once.
    pub fn new(select: &SelectPlan) -> RookResult<Self> {
        let mut keys: Vec<NamedExpr> = Vec::new();
        for key in &select.group_by {
            if !aggregate_calls(key).is_empty() {
                return Err(RookError::InvalidQuery(
                    "Aggregate function calls are not allowed in GROUP BY".to_string(),
                ));
            }
            // A repeated key groups nothing further.
            if keys.iter().any(|k| k.expr == *key) {
                continue;
            }
            let name = match key {
                ExprNode::Compound(parts) => parts.last().cloned().unwrap_or_default(),
                ExprNode::Column(name) => name.clone(),
                _ => format!("#key{}", keys.len()),
            };
            keys.push(NamedExpr {
                name,
                expr: key.clone(),
            });
        }

        let mut calls: Vec<&ExprNode> = Vec::new();
        let items = select.projections.iter().filter_map(|p| match p {
            SelectExpr::UnnamedExpr(e) | SelectExpr::ExprWithAlias { expr: e, .. } => Some(e),
            SelectExpr::Wildcard | SelectExpr::QualifiedWildcard(_) => None,
        });
        let having = select.having.iter().flat_map(|h| h.exprs());
        let order_by = select.order_by.iter().map(|o| &o.expr);
        for e in items.chain(having).chain(order_by) {
            for call in aggregate_calls(e) {
                if !calls.contains(&call) {
                    calls.push(call);
                }
            }
        }
        let aggregates = calls
            .into_iter()
            .enumerate()
            .filter_map(|(i, call)| aggregate_expr(call, format!("#agg{}", i)))
            .collect();
        Ok(Grouping { keys, aggregates })
    }

    /// The aggregate calls, each named by its output column.
    pub fn aggregates(&self) -> &[AggregateExpr] {
        &self.aggregates
    }

    /// The aggregation over `child`, with HAVING (bound) as its filter.
    pub fn aggregate(
        &self,
        having: Option<&PredicateNode>,
        child: LogicalPlan,
    ) -> RookResult<LogicalPlan> {
        Ok(LogicalPlan::Aggregate(LogicalAggregate {
            group_by: self.keys.clone(),
            aggregates: self.aggregates.clone(),
            having: having.map(|h| self.rewrite_predicate(h)).transpose()?,
            child: Box::new(child),
        }))
    }

    /// The SELECT list (bound) as outputs computed from the aggregation.
    pub fn outputs(&self, projections: &[SelectExpr]) -> RookResult<Vec<NamedExpr>> {
        projections
            .iter()
            .filter_map(|p| match p {
                SelectExpr::UnnamedExpr(e) => Some((super::helpers::expr_to_name(e), e)),
                SelectExpr::ExprWithAlias { expr, alias } => Some((alias.clone(), expr)),
                SelectExpr::Wildcard | SelectExpr::QualifiedWildcard(_) => None,
            })
            .map(|(name, e)| {
                Ok(NamedExpr {
                    name,
                    expr: self.rewrite(e)?,
                })
            })
            .collect()
    }

    /// `expr`, computed above the aggregation: a key reads the key's column,
    /// an aggregate call its result. Any other column has no single value
    /// per group and is an error.
    pub fn rewrite(&self, expr: &ExprNode) -> RookResult<ExprNode> {
        if let Some(key) = self.keys.iter().find(|k| k.expr == *expr) {
            return Ok(if is_column(expr) {
                expr.clone()
            } else {
                ExprNode::Column(key.name.clone())
            });
        }
        match expr {
            ExprNode::Function { name, .. } if aggregate_function(name).is_some() => {
                let wanted = aggregate_expr(expr, String::new());
                self.aggregates
                    .iter()
                    .find(|a| {
                        wanted.as_ref().is_some_and(|w| {
                            a.function == w.function && a.distinct == w.distinct && a.args == w.args
                        })
                    })
                    .and_then(|a| a.alias.clone())
                    .map(ExprNode::Column)
                    .ok_or_else(|| {
                        RookError::Internal(format!("aggregate {:?} was not planned", expr))
                    })
            }
            ExprNode::Column(name) => Err(ungrouped(name)),
            ExprNode::Compound(parts) => Err(ungrouped(&parts.join("."))),
            ExprNode::Predicate(p) => Ok(ExprNode::Predicate(Box::new(self.rewrite_predicate(p)?))),
            other => {
                let mut error = None;
                let rewritten = other.map_children(&mut |e| match self.rewrite(e) {
                    Ok(e) => e,
                    Err(err) => {
                        error.get_or_insert(err);
                        e.clone()
                    }
                });
                error.map_or(Ok(rewritten), Err)
            }
        }
    }

    /// [`Grouping::rewrite`] for each expression of a condition.
    pub fn rewrite_predicate(&self, pred: &PredicateNode) -> RookResult<PredicateNode> {
        let mut error = None;
        let rewritten = pred.map_exprs(&mut |e| match self.rewrite(e) {
            Ok(e) => e,
            Err(err) => {
                error.get_or_insert(err);
                e.clone()
            }
        });
        error.map_or(Ok(rewritten), Err)
    }
}

fn ungrouped(column: &str) -> RookError {
    RookError::InvalidQuery(format!(
        "column '{}' must appear in the GROUP BY clause or be used in an aggregate function",
        column
    ))
}
