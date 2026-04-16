use crate::backend::executor::tuple::Tuple;
use crate::backend::executor::value::Value;
use ordered_float::OrderedFloat;

pub enum Expr {
    Constant(Value),
    ColumnRef(usize),
    BinaryOp { left: Box<Expr>, op: BinaryOperator, right: Box<Expr> },
    Comparison { left: Box<Expr>, op: ComparisonOperator, right: Box<Expr> },
}

pub enum BinaryOperator { Add, Sub, Mul, Div }
pub enum ComparisonOperator { Eq, Neq, Lt, Gt, Leq, Geq }

use crate::backend::executor::iterator::ExecutorError;

pub fn evaluate(expr: &Expr, tuple: &Tuple) -> Result<Value, ExecutorError> {

    match expr {
        Expr::Constant(val) => Ok(val.clone()),
        Expr::ColumnRef(idx) => Ok(tuple.values[*idx].clone()),
        Expr::BinaryOp { left, op, right } => {
            let left_val=evaluate(left,tuple)?;
            let right_val=evaluate(right,tuple)?;
            match op {
                BinaryOperator::Add=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Ok(Value::Int(l + r));
                        }
                        _ => return Err(ExecutorError::TypeMismatch("Mathematical or comparative operator applied to incompatible types".to_string())),
                    }
                }
                BinaryOperator::Sub=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Ok(Value::Int(l - r));
                        }
                        _ => return Err(ExecutorError::TypeMismatch("Mathematical or comparative operator applied to incompatible types".to_string())),
                    }
                }
                BinaryOperator::Mul=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Ok(Value::Int(l * r));
                        }
                        _ => return Err(ExecutorError::TypeMismatch("Mathematical or comparative operator applied to incompatible types".to_string())),
                    }
                }
                BinaryOperator::Div=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Ok(Value::Float(OrderedFloat(l as f64 / r as f64)));
                        }
                        _ => return Err(ExecutorError::TypeMismatch("Mathematical or comparative operator applied to incompatible types".to_string())),
                    }
                }
            }
        },
        Expr::Comparison { left, op, right } => {
            let left_val=evaluate(left,tuple)?;
            let right_val=evaluate(right,tuple)?;
            match op {
                ComparisonOperator::Eq=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Ok(Value::Boolean(l==r));
                        }
                        _ => return Err(ExecutorError::TypeMismatch("Mathematical or comparative operator applied to incompatible types".to_string())),
                    }
                }
                ComparisonOperator::Neq=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Ok(Value::Boolean(l!=r));
                        }
                        _ => return Err(ExecutorError::TypeMismatch("Mathematical or comparative operator applied to incompatible types".to_string())),
                    }
                }
                ComparisonOperator::Lt=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Ok(Value::Boolean(l<r));
                        }
                        _ => return Err(ExecutorError::TypeMismatch("Mathematical or comparative operator applied to incompatible types".to_string())),
                    }
                }
                ComparisonOperator::Gt=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Ok(Value::Boolean(l>r));
                        }
                        _ => return Err(ExecutorError::TypeMismatch("Mathematical or comparative operator applied to incompatible types".to_string())),
                    }
                }
                ComparisonOperator::Leq=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Ok(Value::Boolean(l<=r));
                        }
                        _ => return Err(ExecutorError::TypeMismatch("Mathematical or comparative operator applied to incompatible types".to_string())),
                    }
                }
                ComparisonOperator::Geq=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Ok(Value::Boolean(l>=r));
                        }
                        _ => return Err(ExecutorError::TypeMismatch("Mathematical or comparative operator applied to incompatible types".to_string())),
                    }
                }
            }
        },
    }
}