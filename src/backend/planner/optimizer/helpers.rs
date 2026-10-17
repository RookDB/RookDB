//! Helper functions for the rule-based optimizer.
//!
//! Provides expression folding, predicate folding, and column extraction
//! utilities used by the optimization passes.

use rook_ast::logical::*;
use rook_ast::{ArithOp, ComparisonOp, ConstantValue, ExprNode, PredicateNode};

use std::collections::HashSet;

// ─── Expression Folding ───────────────────────────────────────────────────────

/// Fold constants in an `ExprNode` tree.
pub fn fold_expr(expr: &ExprNode) -> ExprNode {
    match expr {
        ExprNode::Column(_) | ExprNode::Constant(_) => expr.clone(),
        ExprNode::Compound(_) => expr.clone(),
        ExprNode::Cast {
            expr: inner,
            data_type,
        } => {
            let folded = fold_expr(inner);
            ExprNode::Cast {
                expr: Box::new(folded),
                data_type: data_type.clone(),
            }
        }
        ExprNode::ScalarSubquery(_) => expr.clone(),
        ExprNode::Function {
            name,
            args,
            distinct,
        } => {
            let folded_args = args
                .iter()
                .map(|a| match a {
                    rook_ast::FunctionArg::Star => rook_ast::FunctionArg::Star,
                    rook_ast::FunctionArg::Expr(e) => {
                        rook_ast::FunctionArg::Expr(Box::new(fold_expr(e)))
                    }
                })
                .collect();
            ExprNode::Function {
                name: name.clone(),
                args: folded_args,
                distinct: *distinct,
            }
        }
        ExprNode::Case {
            when_then_pairs,
            else_result,
        } => {
            let folded_pairs: Vec<_> = when_then_pairs
                .iter()
                .map(|(when, then)| (Box::new(fold_expr(when)), Box::new(fold_expr(then))))
                .collect();
            let folded_else = else_result.as_ref().map(|e| Box::new(fold_expr(e)));
            ExprNode::Case {
                when_then_pairs: folded_pairs,
                else_result: folded_else,
            }
        }
        ExprNode::Binary { left, op, right } => {
            let left = fold_expr(left);
            let right = fold_expr(right);
            // Only constant-with-constant is folded. Identities such as
            // `x * 0 = 0` or `x + 0 = x` do not hold in SQL: NULL * 0 is
            // NULL, an error in `x` must still be raised, and `x + 0` must
            // still be type-checked (it is an error for text).
            match (&left, &right) {
                (ExprNode::Constant(a), ExprNode::Constant(b)) => fold_binary_op(a, *op, b),
                _ => ExprNode::Binary {
                    left: Box::new(left),
                    op: *op,
                    right: Box::new(right),
                },
            }
        }
        ExprNode::Compare { left, op, right } => ExprNode::Compare {
            left: Box::new(fold_expr(left)),
            op: *op,
            right: Box::new(fold_expr(right)),
        },
        ExprNode::Logical { left, op, right } => ExprNode::Logical {
            left: Box::new(fold_expr(left)),
            op: *op,
            right: Box::new(fold_expr(right)),
        },
        ExprNode::Not(inner) => ExprNode::Not(Box::new(fold_expr(inner))),
        ExprNode::IsNull(inner) => ExprNode::IsNull(Box::new(fold_expr(inner))),
        ExprNode::IsNotNull(inner) => ExprNode::IsNotNull(Box::new(fold_expr(inner))),
    }
}

/// Fold a binary operation between two constants.
///
/// Anything the executor would reject is left unfolded so the executor
/// reports it: division by zero, integer overflow (an INT literal, one
/// that fits in 32 bits, overflows at 32 bits, as it does at run time) and
/// operands that are not numbers. Folding `SELECT 10 / 0` to NULL made it
/// fail typing with "Cannot infer type for NULL literal"; saturating
/// `9223372036854775807 + 1` returned a wrong number instead of an error.
pub fn fold_binary_op(a: &ConstantValue, op: ArithOp, b: &ConstantValue) -> ExprNode {
    let unfolded = || ExprNode::Binary {
        left: Box::new(ExprNode::Constant(a.clone())),
        op,
        right: Box::new(ExprNode::Constant(b.clone())),
    };
    let as_float = |c: &ConstantValue| match c {
        ConstantValue::Int(i) => Some(*i as f64),
        ConstantValue::Float(f) => Some(*f),
        _ => None,
    };
    let result = match (a, b) {
        (ConstantValue::Null, _) | (_, ConstantValue::Null) => ConstantValue::Null,
        (ConstantValue::Int(ai), ConstantValue::Int(bi)) => {
            let v = match op {
                ArithOp::Add => ai.checked_add(*bi),
                ArithOp::Sub => ai.checked_sub(*bi),
                ArithOp::Mul => ai.checked_mul(*bi),
                // `checked_div` also declines i64::MIN / -1.
                ArithOp::Div => ai.checked_div(*bi),
            };
            let fits_i32 = |i: i64| i32::try_from(i).is_ok();
            match v {
                Some(v) if !(fits_i32(*ai) && fits_i32(*bi)) || fits_i32(v) => {
                    ConstantValue::Int(v)
                }
                _ => return unfolded(),
            }
        }
        _ => match (as_float(a), as_float(b)) {
            (Some(af), Some(bf)) => ConstantValue::Float(match op {
                ArithOp::Add => af + bf,
                ArithOp::Sub => af - bf,
                ArithOp::Mul => af * bf,
                ArithOp::Div if bf == 0.0 => return unfolded(),
                ArithOp::Div => af / bf,
            }),
            _ => return unfolded(),
        },
    };
    ExprNode::Constant(result)
}

// ─── Predicate Folding ────────────────────────────────────────────────────────

/// Fold constants in a `PredicateNode` tree.
pub fn fold_predicate(pred: &PredicateNode) -> PredicateNode {
    match pred {
        PredicateNode::BinaryOp { left, op, right } => {
            let left = fold_predicate(left);
            let right = fold_predicate(right);
            PredicateNode::BinaryOp {
                left: Box::new(left),
                op: *op,
                right: Box::new(right),
            }
        }
        PredicateNode::Not(inner) => {
            let inner = fold_predicate(inner);
            PredicateNode::Not(Box::new(inner))
        }
        PredicateNode::Compare { left, op, right } => {
            let left = fold_expr(left);
            let right = fold_expr(right);
            if let (ExprNode::Constant(a), ExprNode::Constant(b)) = (&left, &right)
                && let Some(result) = fold_comparison(a, *op, b)
            {
                // A folded comparison is a truth value: write it as
                // `result = TRUE`, which the evaluator can compare.
                return PredicateNode::Compare {
                    left: Box::new(ExprNode::Constant(ConstantValue::Boolean(result))),
                    op: ComparisonOp::Eq,
                    right: Box::new(ExprNode::Constant(ConstantValue::Boolean(true))),
                };
            }
            PredicateNode::Compare {
                left: Box::new(left),
                op: *op,
                right: Box::new(right),
            }
        }
        PredicateNode::IsNull(expr) => PredicateNode::IsNull(Box::new(fold_expr(expr))),
        PredicateNode::IsNotNull(expr) => PredicateNode::IsNotNull(Box::new(fold_expr(expr))),
        PredicateNode::Between { expr, low, high } => PredicateNode::Between {
            expr: Box::new(fold_expr(expr)),
            low: Box::new(fold_expr(low)),
            high: Box::new(fold_expr(high)),
        },
        PredicateNode::InList { expr, list } => PredicateNode::InList {
            expr: Box::new(fold_expr(expr)),
            list: list.iter().map(fold_expr).collect(),
        },
        PredicateNode::Like {
            expr,
            pattern,
            escape_char,
        } => PredicateNode::Like {
            expr: Box::new(fold_expr(expr)),
            pattern: pattern.clone(),
            escape_char: *escape_char,
        },
        PredicateNode::Exists(subquery) => PredicateNode::Exists(subquery.clone()),
        PredicateNode::InSubquery {
            expr,
            subquery,
            negated,
        } => PredicateNode::InSubquery {
            expr: Box::new(fold_expr(expr)),
            subquery: subquery.clone(),
            negated: *negated,
        },
        PredicateNode::IsDistinctFrom { left, right } => PredicateNode::IsDistinctFrom {
            left: Box::new(fold_expr(left)),
            right: Box::new(fold_expr(right)),
        },
        PredicateNode::IsBoolean {
            expr,
            test,
            negated,
        } => PredicateNode::IsBoolean {
            expr: Box::new(fold_expr(expr)),
            test: *test,
            negated: *negated,
        },
    }
}

/// Fold a comparison between two constants.
pub fn fold_comparison(a: &ConstantValue, op: ComparisonOp, b: &ConstantValue) -> Option<bool> {
    use std::cmp::Ordering;
    let ordering = match (a, b) {
        (ConstantValue::Int(ai), ConstantValue::Int(bi)) => ai.cmp(bi),
        (
            ConstantValue::Int(_) | ConstantValue::Float(_),
            ConstantValue::Int(_) | ConstantValue::Float(_),
        ) => {
            let f = |c: &ConstantValue| match c {
                ConstantValue::Int(i) => *i as f64,
                ConstantValue::Float(f) => *f,
                _ => unreachable!(),
            };
            f(a).partial_cmp(&f(b))?
        }
        (ConstantValue::Text(a), ConstantValue::Text(b)) => a.cmp(b),
        (ConstantValue::Boolean(a), ConstantValue::Boolean(b)) => match op {
            ComparisonOp::Eq => return Some(a == b),
            ComparisonOp::Ne => return Some(a != b),
            _ => return None,
        },
        _ => return None,
    };
    Some(match op {
        ComparisonOp::Eq => ordering == Ordering::Equal,
        ComparisonOp::Ne => ordering != Ordering::Equal,
        ComparisonOp::Lt => ordering == Ordering::Less,
        ComparisonOp::Le => ordering != Ordering::Greater,
        ComparisonOp::Gt => ordering == Ordering::Greater,
        ComparisonOp::Ge => ordering != Ordering::Less,
    })
}

// ─── Column Extraction ────────────────────────────────────────────────────────

/// Extract all column names referenced in a `PredicateNode`.
pub fn columns_in_predicate(pred: &PredicateNode) -> HashSet<String> {
    let mut cols = HashSet::new();
    columns_in_predicate_recursive(pred, &mut cols);
    cols
}

fn columns_in_predicate_recursive(pred: &PredicateNode, cols: &mut HashSet<String>) {
    match pred {
        PredicateNode::BinaryOp { left, right, .. } => {
            columns_in_predicate_recursive(left, cols);
            columns_in_predicate_recursive(right, cols);
        }
        PredicateNode::Not(inner) => columns_in_predicate_recursive(inner, cols),
        PredicateNode::Compare { left, right, .. } => {
            extract_expr_columns_into(left, cols);
            extract_expr_columns_into(right, cols);
        }
        PredicateNode::IsNull(expr)
        | PredicateNode::IsNotNull(expr)
        | PredicateNode::Like { expr, .. }
        | PredicateNode::Between { expr, .. } => {
            extract_expr_columns_into(expr, cols);
        }
        PredicateNode::InList { expr, list } => {
            extract_expr_columns_into(expr, cols);
            for item in list {
                extract_expr_columns_into(item, cols);
            }
        }
        PredicateNode::Exists(_) => {}
        PredicateNode::InSubquery { expr, .. } => extract_expr_columns_into(expr, cols),
        PredicateNode::IsDistinctFrom { left, right } => {
            extract_expr_columns_into(left, cols);
            extract_expr_columns_into(right, cols);
        }
        PredicateNode::IsBoolean { expr, .. } => extract_expr_columns_into(expr, cols),
    }
}

/// Extract column names from an `ExprNode` as a set.
pub fn extract_expr_columns(expr: &ExprNode) -> HashSet<String> {
    let mut cols = HashSet::new();
    extract_expr_columns_into(expr, &mut cols);
    cols
}

fn extract_expr_columns_into(expr: &ExprNode, cols: &mut HashSet<String>) {
    match expr {
        ExprNode::Column(name) => {
            cols.insert(name.clone());
        }
        ExprNode::Compound(parts) => {
            if let Some(last) = parts.last() {
                cols.insert(last.clone());
            }
        }
        ExprNode::Binary { left, right, .. } => {
            extract_expr_columns_into(left, cols);
            extract_expr_columns_into(right, cols);
        }
        ExprNode::Cast { expr: inner, .. } => extract_expr_columns_into(inner, cols),
        ExprNode::Constant(_) => {}
        ExprNode::ScalarSubquery(_) => {}
        ExprNode::Function { args, .. } => {
            for arg in args {
                if let rook_ast::FunctionArg::Expr(inner) = arg {
                    extract_expr_columns_into(inner, cols);
                }
            }
        }
        ExprNode::Case {
            when_then_pairs,
            else_result,
        } => {
            for (when, then) in when_then_pairs {
                extract_expr_columns_into(when, cols);
                extract_expr_columns_into(then, cols);
            }
            if let Some(else_node) = else_result {
                extract_expr_columns_into(else_node, cols);
            }
        }
        ExprNode::Compare { left, right, .. } | ExprNode::Logical { left, right, .. } => {
            extract_expr_columns_into(left, cols);
            extract_expr_columns_into(right, cols);
        }
        ExprNode::Not(inner) | ExprNode::IsNull(inner) | ExprNode::IsNotNull(inner) => {
            extract_expr_columns_into(inner, cols);
        }
    }
}

/// Collect all column names required by the ancestors of `TableScan`.
pub fn collect_required_columns(plan: &LogicalPlan) -> HashSet<String> {
    let mut cols = HashSet::new();
    collect_required_columns_recursive(plan, &mut cols);
    cols
}

fn collect_required_columns_recursive(plan: &LogicalPlan, cols: &mut HashSet<String>) {
    match plan {
        LogicalPlan::TableScan(_) => {}
        LogicalPlan::Filter(f) => {
            columns_in_predicate_recursive(&f.predicate, cols);
            collect_required_columns_recursive(&f.child, cols);
        }
        LogicalPlan::Project(p) => {
            for ne in &p.expressions {
                extract_expr_columns_into(&ne.expr, cols);
            }
        }
        LogicalPlan::Sort(s) => {
            for ob in &s.order_by {
                extract_expr_columns_into(&ob.expr, cols);
            }
            collect_required_columns_recursive(&s.child, cols);
        }
        LogicalPlan::Distinct(d) => collect_required_columns_recursive(&d.child, cols),
        LogicalPlan::Limit(l) => collect_required_columns_recursive(&l.child, cols),
        LogicalPlan::Aggregate(a) => {
            for expr in &a.group_by {
                extract_expr_columns_into(expr, cols);
            }
            for ag in &a.aggregates {
                for arg in &ag.args {
                    extract_expr_columns_into(arg, cols);
                }
            }
            if let Some(ref having) = a.having {
                columns_in_predicate_recursive(having, cols);
            }
            collect_required_columns_recursive(&a.child, cols);
        }
        LogicalPlan::Join(j) => {
            if let Some(ref cond) = j.condition {
                columns_in_predicate_recursive(cond, cols);
            }
            collect_required_columns_recursive(&j.left, cols);
            collect_required_columns_recursive(&j.right, cols);
        }
        LogicalPlan::SetOp(_) => {}
        LogicalPlan::Subquery(sq) => collect_required_columns_recursive(&sq.subquery, cols),
        LogicalPlan::Cte(c) => {
            collect_required_columns_recursive(&c.inner, cols);
            collect_required_columns_recursive(&c.outer, cols);
        }
        LogicalPlan::RecursiveCte(rc) => {
            collect_required_columns_recursive(&rc.non_recursive, cols);
            collect_required_columns_recursive(&rc.recursive, cols);
            collect_required_columns_recursive(&rc.outer, cols);
        }
        LogicalPlan::CteScan(_) => {}
        LogicalPlan::Insert(inp) => collect_required_columns_recursive(&inp.child, cols),
    }
}

/// Extract column names from a `PredicateNode` as a `Vec<String>`.
pub fn extract_column_names(pred: &PredicateNode) -> Vec<String> {
    extract_column_refs(pred)
        .into_iter()
        .map(|(_, name)| name)
        .collect()
}

/// Whether a predicate holds a subquery anywhere: `EXISTS`, `IN (subquery)`
/// or a scalar subquery inside an expression. Its column references name
/// only what the outer expression reads: a correlated subquery may also read
/// outer columns that no such list shows, so the predicate must stay where
/// every outer column is still in scope.
pub fn contains_subquery(pred: &PredicateNode) -> bool {
    match pred {
        PredicateNode::Exists(_) | PredicateNode::InSubquery { .. } => true,
        PredicateNode::BinaryOp { left, right, .. } => {
            contains_subquery(left) || contains_subquery(right)
        }
        PredicateNode::Not(inner) => contains_subquery(inner),
        PredicateNode::Compare { left, right, .. }
        | PredicateNode::IsDistinctFrom { left, right } => {
            expr_contains_subquery(left) || expr_contains_subquery(right)
        }
        PredicateNode::IsNull(expr)
        | PredicateNode::IsNotNull(expr)
        | PredicateNode::Like { expr, .. }
        | PredicateNode::IsBoolean { expr, .. } => expr_contains_subquery(expr),
        PredicateNode::Between { expr, low, high } => {
            expr_contains_subquery(expr)
                || expr_contains_subquery(low)
                || expr_contains_subquery(high)
        }
        PredicateNode::InList { expr, list } => {
            expr_contains_subquery(expr) || list.iter().any(expr_contains_subquery)
        }
    }
}

/// [`contains_subquery`] for an expression.
pub fn expr_contains_subquery(expr: &ExprNode) -> bool {
    match expr {
        ExprNode::ScalarSubquery(_) => true,
        ExprNode::Column(_) | ExprNode::Compound(_) | ExprNode::Constant(_) => false,
        ExprNode::Binary { left, right, .. }
        | ExprNode::Compare { left, right, .. }
        | ExprNode::Logical { left, right, .. } => {
            expr_contains_subquery(left) || expr_contains_subquery(right)
        }
        ExprNode::Cast { expr: inner, .. }
        | ExprNode::Not(inner)
        | ExprNode::IsNull(inner)
        | ExprNode::IsNotNull(inner) => expr_contains_subquery(inner),
        ExprNode::Function { args, .. } => args.iter().any(|arg| {
            matches!(arg, rook_ast::FunctionArg::Expr(inner) if expr_contains_subquery(inner))
        }),
        ExprNode::Case {
            when_then_pairs,
            else_result,
        } => {
            when_then_pairs
                .iter()
                .any(|(when, then)| expr_contains_subquery(when) || expr_contains_subquery(then))
                || else_result.as_deref().is_some_and(expr_contains_subquery)
        }
    }
}

/// Extract column references from a `PredicateNode` as `(table qualifier,
/// column name)` pairs, keeping the qualifier so `a.k` and `r.k` stay apart.
pub fn extract_column_refs(pred: &PredicateNode) -> Vec<(Option<String>, String)> {
    let mut result = Vec::new();
    extract_column_names_recursive(pred, &mut result);
    result
}

/// [`extract_column_refs`] for an expression.
pub fn extract_expr_column_refs(expr: &ExprNode) -> Vec<(Option<String>, String)> {
    let mut result = Vec::new();
    extract_column_names_from_expr(expr, &mut result);
    result
}

fn extract_column_names_recursive(pred: &PredicateNode, names: &mut Vec<(Option<String>, String)>) {
    match pred {
        PredicateNode::BinaryOp { left, right, .. } => {
            extract_column_names_recursive(left, names);
            extract_column_names_recursive(right, names);
        }
        PredicateNode::Not(inner) => extract_column_names_recursive(inner, names),
        PredicateNode::Compare { left, right, .. } => {
            extract_column_names_from_expr(left, names);
            extract_column_names_from_expr(right, names);
        }
        PredicateNode::IsNull(expr)
        | PredicateNode::IsNotNull(expr)
        | PredicateNode::Like { expr, .. } => {
            extract_column_names_from_expr(expr, names);
        }
        PredicateNode::Between { expr, low, high } => {
            extract_column_names_from_expr(expr, names);
            extract_column_names_from_expr(low, names);
            extract_column_names_from_expr(high, names);
        }
        PredicateNode::InList { expr, list } => {
            extract_column_names_from_expr(expr, names);
            for item in list {
                extract_column_names_from_expr(item, names);
            }
        }
        PredicateNode::Exists(_) => {}
        PredicateNode::InSubquery { expr, .. } => extract_column_names_from_expr(expr, names),
        PredicateNode::IsDistinctFrom { left, right } => {
            extract_column_names_from_expr(left, names);
            extract_column_names_from_expr(right, names);
        }
        PredicateNode::IsBoolean { expr, .. } => extract_column_names_from_expr(expr, names),
    }
}

fn extract_column_names_from_expr(expr: &ExprNode, names: &mut Vec<(Option<String>, String)>) {
    match expr {
        ExprNode::Column(name) => names.push((None, name.clone())),
        ExprNode::Compound(parts) => {
            if let Some(last) = parts.last() {
                let table = (parts.len() >= 2).then(|| parts[parts.len() - 2].clone());
                names.push((table, last.clone()));
            }
        }
        ExprNode::Binary { left, right, .. } => {
            extract_column_names_from_expr(left, names);
            extract_column_names_from_expr(right, names);
        }
        ExprNode::Cast { expr: inner, .. } => extract_column_names_from_expr(inner, names),
        ExprNode::Constant(_) => {}
        ExprNode::ScalarSubquery(_) => {}
        ExprNode::Function { args, .. } => {
            for arg in args {
                if let rook_ast::FunctionArg::Expr(inner) = arg {
                    extract_column_names_from_expr(inner, names);
                }
            }
        }
        ExprNode::Case {
            when_then_pairs,
            else_result,
        } => {
            for (when, then) in when_then_pairs {
                extract_column_names_from_expr(when, names);
                extract_column_names_from_expr(then, names);
            }
            if let Some(else_node) = else_result {
                extract_column_names_from_expr(else_node, names);
            }
        }
        ExprNode::Compare { left, right, .. } | ExprNode::Logical { left, right, .. } => {
            extract_column_names_from_expr(left, names);
            extract_column_names_from_expr(right, names);
        }
        ExprNode::Not(inner) | ExprNode::IsNull(inner) | ExprNode::IsNotNull(inner) => {
            extract_column_names_from_expr(inner, names);
        }
    }
}
