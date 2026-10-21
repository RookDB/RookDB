//! Conversion functions from `rook_ast` expression/predicate nodes to physical
//! `Expr` and `Predicate` types.

use crate::backend::error::{RookError, RookResult};
use crate::types::value::DataValue;

use super::Expr;
use super::predicate::{BooleanTest, ComparisonOp, Predicate};

/// Turns the subqueries met while converting an expression into physical
/// expressions. The physical planner runs them (or plans them per outer row);
/// elsewhere a subquery is an error ([`NoSubqueries`]).
pub trait Subqueries {
    /// `(SELECT ...)` used as a value.
    fn scalar(&self, info: &rook_ast::SubqueryInfo) -> RookResult<Expr>;
    /// `EXISTS (SELECT ...)`.
    fn exists(&self, info: &rook_ast::SubqueryInfo) -> RookResult<Predicate>;
    /// `lhs [NOT] IN (SELECT ...)`.
    fn in_subquery(
        &self,
        lhs: Expr,
        info: &rook_ast::SubqueryInfo,
        negated: bool,
    ) -> RookResult<Predicate>;
}

/// For callers with no subquery planner: any subquery is an internal error.
pub struct NoSubqueries;

impl Subqueries for NoSubqueries {
    fn scalar(&self, _: &rook_ast::SubqueryInfo) -> RookResult<Expr> {
        Err(RookError::Internal(
            "Scalar subqueries must be materialized before expr_from_ast".to_string(),
        ))
    }
    fn exists(&self, _: &rook_ast::SubqueryInfo) -> RookResult<Predicate> {
        Err(RookError::Internal(
            "Subquery predicates must be materialized before predicate_from_ast".to_string(),
        ))
    }
    fn in_subquery(
        &self,
        _: Expr,
        info: &rook_ast::SubqueryInfo,
        _: bool,
    ) -> RookResult<Predicate> {
        self.exists(info)
    }
}

/// Convert a `rook_ast::ExprNode` into a physical `Expr`, resolving column
/// names against a schema. `column_names` come from `binding_names`, so a
/// bare name that two columns share is rejected as ambiguous.
pub fn expr_from_ast(node: &rook_ast::ExprNode, column_names: &[String]) -> RookResult<Expr> {
    expr_with(node, column_names, &NoSubqueries)
}

/// [`expr_from_ast`], with subqueries anywhere in the tree turned into
/// physical expressions by `sq`.
pub fn expr_with(
    node: &rook_ast::ExprNode,
    column_names: &[String],
    sq: &dyn Subqueries,
) -> RookResult<Expr> {
    match node {
        rook_ast::ExprNode::Column(name) => {
            // Unqualified column reference — just store the column name.
            // Resolution happens at evaluation time against the tuple's schema.
            // Verify here, once, that it names exactly one column.
            let mut named = column_names.iter().filter(|c| c.eq_ignore_ascii_case(name));
            if named.next().is_none() {
                return Err(RookError::NotFound {
                    entity: "Column",
                    name: name.clone(),
                });
            }
            if named.next().is_some() {
                return Err(RookError::AmbiguousColumn(name.clone()));
            }
            Ok(Expr::Column {
                table: None,
                column: name.clone(),
            })
        }
        rook_ast::ExprNode::Compound(parts) => {
            // Qualified column reference — extract table and column name.
            let column = parts
                .last()
                .ok_or_else(|| RookError::Internal("Empty compound identifier".to_string()))?;
            let table = if parts.len() >= 2 {
                Some(parts[parts.len() - 2].clone())
            } else {
                None
            };
            // Verify the column exists in the schema for early error detection.
            if !column_names.iter().any(|c| c.eq_ignore_ascii_case(column)) {
                return Err(RookError::NotFound {
                    entity: "Column",
                    name: column.clone(),
                });
            }
            Ok(Expr::Column {
                table,
                column: column.clone(),
            })
        }
        // Bound per outer row before the subquery holding it is planned.
        rook_ast::ExprNode::OuterColumn(r) => Err(RookError::Internal(format!(
            "outer column '{}' reached the executor unbound",
            r.column
        ))),
        rook_ast::ExprNode::Constant(cv) => constant_from_ast(cv).map(|c| match c {
            Some(dv) => Expr::Constant(dv),
            None => Expr::Null,
        }),
        rook_ast::ExprNode::Binary { left, op, right } => {
            let l = expr_with(left, column_names, sq)?;
            let r = expr_with(right, column_names, sq)?;
            match op {
                rook_ast::ArithOp::Add => Ok(Expr::Add(Box::new(l), Box::new(r))),
                rook_ast::ArithOp::Sub => Ok(Expr::Sub(Box::new(l), Box::new(r))),
                rook_ast::ArithOp::Mul => Ok(Expr::Mul(Box::new(l), Box::new(r))),
                rook_ast::ArithOp::Div => Ok(Expr::Div(Box::new(l), Box::new(r))),
            }
        }
        rook_ast::ExprNode::Cast { expr, data_type } => {
            let inner = expr_with(expr, column_names, sq)?;
            let target_dt: crate::types::datatype::DataType =
                data_type.parse().map_err(|e: RookError| {
                    RookError::TypeMismatch(format!(
                        "Invalid CAST target type '{}': {}",
                        data_type, e
                    ))
                })?;
            Ok(Expr::Cast(Box::new(inner), target_dt))
        }
        rook_ast::ExprNode::ScalarSubquery(info) => sq.scalar(info),
        // Scalar function calls — convert each argument and create an Expr::Function.
        // Aggregate functions (COUNT, SUM, AVG, MIN, MAX) are handled by the
        // AggregateOperator and should not reach this path (the physical planner
        // strips them from projection expressions before calling expr_from_ast).
        rook_ast::ExprNode::Function {
            name,
            args,
            distinct: _,
        } => {
            let mut expr_args = Vec::new();
            for arg in args {
                match arg {
                    rook_ast::FunctionArg::Expr(inner) => {
                        let inner_expr = expr_with(inner, column_names, sq)?;
                        expr_args.push(inner_expr);
                    }
                    rook_ast::FunctionArg::Star => {
                        // Star (*) is used for COUNT(*) etc., which shouldn't reach here.
                        // If it does, just ignore it (push a NULL placeholder).
                        expr_args.push(Expr::Null);
                    }
                }
            }
            Ok(Expr::Function {
                name: name.clone(),
                args: expr_args,
            })
        }
        rook_ast::ExprNode::Case {
            when_then_pairs,
            else_result,
        } => {
            let mut pairs = Vec::new();
            for (cond, res) in when_then_pairs {
                let cond_expr = expr_with(cond, column_names, sq)?;
                let res_expr = expr_with(res, column_names, sq)?;
                pairs.push((cond_expr, res_expr));
            }
            let else_expr = match else_result {
                Some(expr) => Some(Box::new(expr_with(expr, column_names, sq)?)),
                None => None,
            };
            Ok(Expr::Case {
                when_then_pairs: pairs,
                else_result: else_expr,
            })
        }
        // Boolean expressions evaluate as predicates: one implementation of
        // comparison and three-valued logic for WHERE and SELECT alike.
        rook_ast::ExprNode::Compare { left, op, right } => {
            Ok(Expr::Predicate(Box::new(Predicate::Compare(
                expr_with(left, column_names, sq)?,
                comparison_op(*op),
                expr_with(right, column_names, sq)?,
            ))))
        }
        rook_ast::ExprNode::Logical { left, op, right } => {
            let l = as_predicate(expr_with(left, column_names, sq)?);
            let r = as_predicate(expr_with(right, column_names, sq)?);
            Ok(Expr::Predicate(Box::new(match op {
                rook_ast::BinaryOp::And => Predicate::and(l, r),
                rook_ast::BinaryOp::Or => Predicate::or(l, r),
            })))
        }
        rook_ast::ExprNode::Not(inner) => Ok(Expr::Predicate(Box::new(Predicate::not(
            as_predicate(expr_with(inner, column_names, sq)?),
        )))),
        rook_ast::ExprNode::IsNull(inner) => Ok(Expr::Predicate(Box::new(Predicate::IsNull(
            expr_with(inner, column_names, sq)?,
        )))),
        rook_ast::ExprNode::IsNotNull(inner) => Ok(Expr::Predicate(Box::new(
            Predicate::IsNotNull(expr_with(inner, column_names, sq)?),
        ))),
        rook_ast::ExprNode::Predicate(p) => Ok(Expr::Predicate(Box::new(predicate_with(
            p,
            column_names,
            sq,
        )?))),
    }
}

/// `expr` as a condition: a boolean expression is its predicate, any other
/// value `value = TRUE` (TRUE, FALSE or NULL for a boolean value, a type
/// error for anything else).
fn as_predicate(expr: Expr) -> Predicate {
    match expr {
        Expr::Predicate(p) => *p,
        other => Predicate::Compare(
            other,
            ComparisonOp::Equals,
            Expr::Constant(DataValue::Bool(true)),
        ),
    }
}

fn comparison_op(op: rook_ast::ComparisonOp) -> ComparisonOp {
    match op {
        rook_ast::ComparisonOp::Eq => ComparisonOp::Equals,
        rook_ast::ComparisonOp::Ne => ComparisonOp::NotEquals,
        rook_ast::ComparisonOp::Lt => ComparisonOp::LessThan,
        rook_ast::ComparisonOp::Le => ComparisonOp::LessOrEqual,
        rook_ast::ComparisonOp::Gt => ComparisonOp::GreaterThan,
        rook_ast::ComparisonOp::Ge => ComparisonOp::GreaterOrEqual,
    }
}

/// Convert a `rook_ast::PredicateNode` into a physical `Predicate`.
pub fn predicate_from_ast(
    node: &rook_ast::PredicateNode,
    column_names: &[String],
) -> RookResult<Predicate> {
    predicate_with(node, column_names, &NoSubqueries)
}

/// [`predicate_from_ast`], with subqueries anywhere in the tree turned into
/// physical predicates and expressions by `sq`.
pub fn predicate_with(
    node: &rook_ast::PredicateNode,
    column_names: &[String],
    sq: &dyn Subqueries,
) -> RookResult<Predicate> {
    match node {
        rook_ast::PredicateNode::BinaryOp { left, op, right } => {
            let l = predicate_with(left, column_names, sq)?;
            let r = predicate_with(right, column_names, sq)?;
            match op {
                rook_ast::BinaryOp::And => Ok(Predicate::and(l, r)),
                rook_ast::BinaryOp::Or => Ok(Predicate::or(l, r)),
            }
        }
        rook_ast::PredicateNode::Not(inner) => {
            predicate_with(inner, column_names, sq).map(Predicate::not)
        }
        rook_ast::PredicateNode::Compare { left, op, right } => {
            let l = expr_with(left, column_names, sq)?;
            let r = expr_with(right, column_names, sq)?;
            Ok(Predicate::Compare(l, comparison_op(*op), r))
        }
        rook_ast::PredicateNode::IsNull(expr) => {
            let e = expr_with(expr, column_names, sq)?;
            Ok(Predicate::IsNull(e))
        }
        rook_ast::PredicateNode::IsNotNull(expr) => {
            let e = expr_with(expr, column_names, sq)?;
            Ok(Predicate::IsNotNull(e))
        }
        rook_ast::PredicateNode::Between { expr, low, high } => {
            let e = expr_with(expr, column_names, sq)?;
            let l = expr_with(low, column_names, sq)?;
            let h = expr_with(high, column_names, sq)?;
            Ok(Predicate::and(
                Predicate::Compare(e.clone(), ComparisonOp::GreaterOrEqual, l),
                Predicate::Compare(e, ComparisonOp::LessOrEqual, h),
            ))
        }
        rook_ast::PredicateNode::InList { expr, list } => {
            let e = expr_with(expr, column_names, sq)?;
            let items: RookResult<Vec<Expr>> = list
                .iter()
                .map(|item| expr_with(item, column_names, sq))
                .collect();
            let items = items?;
            // x IN (a, b, c) → (x = a) OR (x = b) OR (x = c)
            let mut or_pred = Predicate::Compare(e.clone(), ComparisonOp::Equals, items[0].clone());
            for item in &items[1..] {
                or_pred = Predicate::or(
                    or_pred,
                    Predicate::Compare(e.clone(), ComparisonOp::Equals, item.clone()),
                );
            }
            Ok(or_pred)
        }
        rook_ast::PredicateNode::Like {
            expr,
            pattern,
            escape_char,
        } => {
            let e = expr_with(expr, column_names, sq)?;
            Ok(Predicate::Like(e, pattern.clone(), *escape_char))
        }
        rook_ast::PredicateNode::IsDistinctFrom { left, right } => {
            let l = expr_with(left, column_names, sq)?;
            let r = expr_with(right, column_names, sq)?;
            Ok(Predicate::IsDistinctFrom(l, r))
        }
        rook_ast::PredicateNode::IsBoolean {
            expr,
            test,
            negated,
        } => {
            let e = expr_with(expr, column_names, sq)?;
            let bt = match test {
                rook_ast::BooleanTest::True => BooleanTest::True,
                rook_ast::BooleanTest::False => BooleanTest::False,
                rook_ast::BooleanTest::Unknown => BooleanTest::Unknown,
            };
            Ok(Predicate::IsBoolean {
                expr: e,
                test: bt,
                negated: *negated,
            })
        }
        rook_ast::PredicateNode::Exists(info) => sq.exists(info),
        rook_ast::PredicateNode::InSubquery {
            expr,
            subquery,
            negated,
        } => sq.in_subquery(expr_with(expr, column_names, sq)?, subquery, *negated),
    }
}

fn constant_from_ast(cv: &rook_ast::ConstantValue) -> RookResult<Option<DataValue>> {
    match cv {
        rook_ast::ConstantValue::Null => Ok(None),
        rook_ast::ConstantValue::Int(i) => {
            if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                Ok(Some(DataValue::Int(*i as i32)))
            } else {
                Ok(Some(DataValue::BigInt(*i)))
            }
        }
        rook_ast::ConstantValue::Float(f) => Ok(Some(DataValue::DoublePrecision(
            crate::types::value::OrderedF64(*f),
        ))),
        rook_ast::ConstantValue::Text(s) => Ok(Some(DataValue::Varchar(s.clone()))),
        rook_ast::ConstantValue::Boolean(b) => Ok(Some(DataValue::Bool(*b))),
    }
}
