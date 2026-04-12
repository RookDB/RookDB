use crate::backend::executor::selection::{
    ColumnReference, ComparisonOp, Constant, Expr, Predicate,
};
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, SetExpr, Statement, UnaryOperator, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn build_predicate_from_sql(sql: &str) -> Result<Predicate, String> {
    let mut ast = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|e| format!("Parse error: {}", e))?;

    if ast.len() != 1 {
        return Err("Expected exactly one SQL statement".to_string());
    }

    let statement = ast.pop().unwrap();
    let where_clause = extract_where_clause(statement)?;
    convert_predicate(where_clause)
}

fn extract_where_clause(statement: Statement) -> Result<SqlExpr, String> {
    match statement {
        Statement::Query(query) => match *query.body {
            SetExpr::Select(select) => select
                .selection
                .ok_or_else(|| "No WHERE clause found".to_string()),
            _ => Err("Unsupported statement type, expected SELECT".to_string()),
        },
        _ => Err("Unsupported statement type, expected SELECT".to_string()),
    }
}

fn convert_predicate(expr: SqlExpr) -> Result<Predicate, String> {
    match expr {
        SqlExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(Predicate::And(
                Box::new(convert_predicate(*left)?),
                Box::new(convert_predicate(*right)?),
            )),
            BinaryOperator::Or => Ok(Predicate::Or(
                Box::new(convert_predicate(*left)?),
                Box::new(convert_predicate(*right)?),
            )),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => Ok(Predicate::Compare(
                Box::new(convert_expr(*left)?),
                map_operator(op)?,
                Box::new(convert_expr(*right)?),
            )),
            other => Err(format!(
                "Unsupported predicate expression: {:?}",
                SqlExpr::BinaryOp {
                    left,
                    op: other,
                    right
                }
            )),
        },
        SqlExpr::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => Ok(Predicate::Not(Box::new(convert_predicate(*expr)?))),
            _ => Err(format!(
                "Unsupported predicate expression: {:?}",
                SqlExpr::UnaryOp { op, expr }
            )),
        },
        SqlExpr::IsNull(expr) => Ok(Predicate::IsNull(Box::new(convert_expr(*expr)?))),
        SqlExpr::IsNotNull(expr) => Ok(Predicate::IsNotNull(Box::new(convert_expr(*expr)?))),
        SqlExpr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let between = Predicate::Between(
                Box::new(convert_expr(*expr)?),
                Box::new(convert_expr(*low)?),
                Box::new(convert_expr(*high)?),
            );

            if negated {
                Ok(Predicate::Not(Box::new(between)))
            } else {
                Ok(between)
            }
        }
        SqlExpr::InList {
            expr,
            list,
            negated,
        } => {
            let items = list
                .into_iter()
                .map(convert_expr)
                .collect::<Result<Vec<_>, _>>()?;

            let in_pred = Predicate::In(Box::new(convert_expr(*expr)?), items);
            if negated {
                Ok(Predicate::Not(Box::new(in_pred)))
            } else {
                Ok(in_pred)
            }
        }
        SqlExpr::Like {
            negated,
            expr,
            pattern,
            escape_char: _,
            any: _,
        } => {
            let pattern_text = match *pattern {
                SqlExpr::Value(vws) => match vws.value.clone() {
                    Value::SingleQuotedString(s) => s,
                    _ => {
                        return Err(format!(
                            "Unsupported predicate expression: {:?}",
                            SqlExpr::Value(vws)
                        ))
                    }
                },
                other => {
                    return Err(format!("Unsupported predicate expression: {:?}", other))
                }
            };

            let like_pred = Predicate::Like(Box::new(convert_expr(*expr)?), pattern_text, None);
            if negated {
                Ok(Predicate::Not(Box::new(like_pred)))
            } else {
                Ok(like_pred)
            }
        }
        // Explicitly reject unsupported constructs.
        SqlExpr::Subquery(_)
        | SqlExpr::Exists { .. }
        | SqlExpr::Function(_)
        | SqlExpr::Case { .. }
        | SqlExpr::Cast { .. } => Err(format!("Unsupported predicate expression: {:?}", expr)),
        _ => Err(format!("Unsupported predicate expression: {:?}", expr)),
    }
}

fn convert_expr(expr: SqlExpr) -> Result<Expr, String> {
    match expr {
        SqlExpr::Identifier(ident) => Ok(Expr::Column(ColumnReference::new(ident.value))),
        SqlExpr::CompoundIdentifier(idents) => Ok(Expr::Column(ColumnReference::new(
            idents
                .last()
                .ok_or_else(|| "Unsupported expression: CompoundIdentifier([])".to_string())?
                .value
                .clone(),
        ))),
        SqlExpr::Value(val) => Ok(Expr::Constant(convert_value(val.into())?)),
        SqlExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Plus => Ok(Expr::Add(
                Box::new(convert_expr(*left)?),
                Box::new(convert_expr(*right)?),
            )),
            BinaryOperator::Minus => Ok(Expr::Sub(
                Box::new(convert_expr(*left)?),
                Box::new(convert_expr(*right)?),
            )),
            BinaryOperator::Multiply => Ok(Expr::Mul(
                Box::new(convert_expr(*left)?),
                Box::new(convert_expr(*right)?),
            )),
            BinaryOperator::Divide => Ok(Expr::Div(
                Box::new(convert_expr(*left)?),
                Box::new(convert_expr(*right)?),
            )),
            other => Err(format!(
                "Unsupported expression: {:?}",
                SqlExpr::BinaryOp {
                    left,
                    op: other,
                    right
                }
            )),
        },
        SqlExpr::Nested(inner) => convert_expr(*inner),
        // Explicitly reject unsupported constructs.
        SqlExpr::Subquery(_)
        | SqlExpr::Exists { .. }
        | SqlExpr::Function(_)
        | SqlExpr::Case { .. }
        | SqlExpr::Cast { .. } => Err(format!("Unsupported expression: {:?}", expr)),
        _ => Err(format!("Unsupported expression: {:?}", expr)),
    }
}

fn convert_value(v: Value) -> Result<Constant, String> {
    match v {
        Value::Number(n, _) => {
            if let Ok(i) = n.parse::<i32>() {
                Ok(Constant::Int(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(Constant::Float(f))
            } else {
                Err(format!("Unsupported value type: {:?}", Value::Number(n, false)))
            }
        }
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(Constant::Text(s)),
        Value::Null => Ok(Constant::Null),
        Value::Boolean(b) => Ok(Constant::Int(if b { 1 } else { 0 })),
        _ => Err(format!("Unsupported value type: {:?}", v)),
    }
}

fn map_operator(op: BinaryOperator) -> Result<ComparisonOp, String> {
    match op {
        BinaryOperator::Eq => Ok(ComparisonOp::Equals),
        BinaryOperator::NotEq => Ok(ComparisonOp::NotEquals),
        BinaryOperator::Lt => Ok(ComparisonOp::LessThan),
        BinaryOperator::LtEq => Ok(ComparisonOp::LessOrEqual),
        BinaryOperator::Gt => Ok(ComparisonOp::GreaterThan),
        BinaryOperator::GtEq => Ok(ComparisonOp::GreaterOrEqual),
        _ => Err(format!("Unsupported comparison operator: {:?}", op)),
    }
}
