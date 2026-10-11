//! Conversion functions from `rook_ast` expression/predicate nodes to physical
//! `Expr` and `Predicate` types.

use crate::backend::error::{RookError, RookResult};
use crate::types::value::DataValue;

use super::Expr;
use super::predicate::{BooleanTest, ComparisonOp, Predicate};

/// Convert a `rook_ast::ExprNode` into a physical `Expr`, resolving column
/// names against a schema.
pub fn expr_from_ast(node: &rook_ast::ExprNode, column_names: &[String]) -> RookResult<Expr> {
    match node {
        rook_ast::ExprNode::Column(name) => {
            // Unqualified column reference — just store the column name.
            // Resolution happens at evaluation time against the tuple's schema.
            // Verify the column exists in the schema for early error detection.
            if !column_names.iter().any(|c| c.eq_ignore_ascii_case(name)) {
                return Err(RookError::NotFound {
                    entity: "Column",
                    name: name.clone(),
                });
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
        rook_ast::ExprNode::Constant(cv) => constant_from_ast(cv).map(|c| match c {
            Some(dv) => Expr::Constant(dv),
            None => Expr::Null,
        }),
        rook_ast::ExprNode::Binary { left, op, right } => {
            let l = expr_from_ast(left, column_names)?;
            let r = expr_from_ast(right, column_names)?;
            match op {
                rook_ast::ArithOp::Add => Ok(Expr::Add(Box::new(l), Box::new(r))),
                rook_ast::ArithOp::Sub => Ok(Expr::Sub(Box::new(l), Box::new(r))),
                rook_ast::ArithOp::Mul => Ok(Expr::Mul(Box::new(l), Box::new(r))),
                rook_ast::ArithOp::Div => Ok(Expr::Div(Box::new(l), Box::new(r))),
            }
        }
        rook_ast::ExprNode::Cast { expr, data_type } => {
            let inner = expr_from_ast(expr, column_names)?;
            let target_dt: crate::types::datatype::DataType =
                data_type.parse().map_err(|e: String| {
                    RookError::TypeMismatch(format!(
                        "Invalid CAST target type '{}': {}",
                        data_type, e
                    ))
                })?;
            Ok(Expr::Cast(Box::new(inner), target_dt))
        }
        // Scalar subqueries should be materialized before reaching this function.
        rook_ast::ExprNode::ScalarSubquery(_) => Err(RookError::Internal(
            "Scalar subqueries must be materialized before expr_from_ast".to_string(),
        )),
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
                        let inner_expr = expr_from_ast(inner, column_names)?;
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
                let cond_expr = expr_from_ast(cond, column_names)?;
                let res_expr = expr_from_ast(res, column_names)?;
                pairs.push((cond_expr, res_expr));
            }
            let else_expr = match else_result {
                Some(expr) => Some(Box::new(expr_from_ast(expr, column_names)?)),
                None => None,
            };
            Ok(Expr::Case {
                when_then_pairs: pairs,
                else_result: else_expr,
            })
        }
        rook_ast::ExprNode::Compare { left, op, right } => {
            let l = expr_from_ast(left, column_names)?;
            let r = expr_from_ast(right, column_names)?;
            match op {
                rook_ast::ComparisonOp::Eq => Ok(Expr::Eq(Box::new(l), Box::new(r))),
                rook_ast::ComparisonOp::Ne => Ok(Expr::Ne(Box::new(l), Box::new(r))),
                rook_ast::ComparisonOp::Lt => Ok(Expr::Lt(Box::new(l), Box::new(r))),
                rook_ast::ComparisonOp::Le => Ok(Expr::Le(Box::new(l), Box::new(r))),
                rook_ast::ComparisonOp::Gt => Ok(Expr::Gt(Box::new(l), Box::new(r))),
                rook_ast::ComparisonOp::Ge => Ok(Expr::Ge(Box::new(l), Box::new(r))),
            }
        }
        rook_ast::ExprNode::Logical { left, op, right } => {
            let l = expr_from_ast(left, column_names)?;
            let r = expr_from_ast(right, column_names)?;
            match op {
                rook_ast::BinaryOp::And => Ok(Expr::And(Box::new(l), Box::new(r))),
                rook_ast::BinaryOp::Or => Ok(Expr::Or(Box::new(l), Box::new(r))),
            }
        }
        rook_ast::ExprNode::Not(inner) => {
            let inner_expr = expr_from_ast(inner, column_names)?;
            Ok(Expr::Not(Box::new(inner_expr)))
        }
        rook_ast::ExprNode::IsNull(inner) => {
            let inner_expr = expr_from_ast(inner, column_names)?;
            Ok(Expr::IsNull(Box::new(inner_expr)))
        }
        rook_ast::ExprNode::IsNotNull(inner) => {
            let inner_expr = expr_from_ast(inner, column_names)?;
            Ok(Expr::IsNotNull(Box::new(inner_expr)))
        }
    }
}

/// Convert a `rook_ast::PredicateNode` into a physical `Predicate`.
pub fn predicate_from_ast(
    node: &rook_ast::PredicateNode,
    column_names: &[String],
) -> RookResult<Predicate> {
    match node {
        rook_ast::PredicateNode::BinaryOp { left, op, right } => {
            let l = predicate_from_ast(left, column_names)?;
            let r = predicate_from_ast(right, column_names)?;
            match op {
                rook_ast::BinaryOp::And => Ok(Predicate::and(l, r)),
                rook_ast::BinaryOp::Or => Ok(Predicate::or(l, r)),
            }
        }
        rook_ast::PredicateNode::Not(inner) => {
            predicate_from_ast(inner, column_names).map(Predicate::not)
        }
        rook_ast::PredicateNode::Compare { left, op, right } => {
            let l = expr_from_ast(left, column_names)?;
            let r = expr_from_ast(right, column_names)?;
            let cmp_op = match op {
                rook_ast::ComparisonOp::Eq => ComparisonOp::Equals,
                rook_ast::ComparisonOp::Ne => ComparisonOp::NotEquals,
                rook_ast::ComparisonOp::Lt => ComparisonOp::LessThan,
                rook_ast::ComparisonOp::Le => ComparisonOp::LessOrEqual,
                rook_ast::ComparisonOp::Gt => ComparisonOp::GreaterThan,
                rook_ast::ComparisonOp::Ge => ComparisonOp::GreaterOrEqual,
            };
            Ok(Predicate::Compare(l, cmp_op, r))
        }
        rook_ast::PredicateNode::IsNull(expr) => {
            let e = expr_from_ast(expr, column_names)?;
            Ok(Predicate::IsNull(e))
        }
        rook_ast::PredicateNode::IsNotNull(expr) => {
            let e = expr_from_ast(expr, column_names)?;
            Ok(Predicate::IsNotNull(e))
        }
        rook_ast::PredicateNode::Between { expr, low, high } => {
            let e = expr_from_ast(expr, column_names)?;
            let l = expr_from_ast(low, column_names)?;
            let h = expr_from_ast(high, column_names)?;
            Ok(Predicate::and(
                Predicate::Compare(e.clone(), ComparisonOp::GreaterOrEqual, l),
                Predicate::Compare(e, ComparisonOp::LessOrEqual, h),
            ))
        }
        rook_ast::PredicateNode::InList { expr, list } => {
            let e = expr_from_ast(expr, column_names)?;
            let items: RookResult<Vec<Expr>> = list
                .iter()
                .map(|item| expr_from_ast(item, column_names))
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
            let e = expr_from_ast(expr, column_names)?;
            Ok(Predicate::Like(e, pattern.clone(), *escape_char))
        }
        rook_ast::PredicateNode::IsDistinctFrom { left, right } => {
            let l = expr_from_ast(left, column_names)?;
            let r = expr_from_ast(right, column_names)?;
            Ok(Predicate::IsDistinctFrom(l, r))
        }
        rook_ast::PredicateNode::IsBoolean {
            expr,
            test,
            negated,
        } => {
            let e = expr_from_ast(expr, column_names)?;
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
        // EXISTS and IN-subquery predicates are materialized by the
        // PhysicalPlanner before reaching this converter. If they appear
        // here, something went wrong in the planning pipeline.
        rook_ast::PredicateNode::Exists(_) | rook_ast::PredicateNode::InSubquery { .. } => {
            Err(RookError::Internal(
                "Subquery predicates must be materialized before predicate_from_ast".to_string(),
            ))
        }
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
