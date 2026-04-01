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

pub fn evaluate(expr: &Expr, tuple: &Tuple) -> Value {
    match expr {
        Expr::Constant(val) => val.clone(),
        Expr::ColumnRef(idx) => tuple.values[*idx].clone(),
        Expr::BinaryOp { left, op, right } => {
            let left_val=evaluate(left,tuple);
            let right_val=evaluate(right,tuple);
            match op {
                BinaryOperator::Add=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Value::Int(l + r);
                        }
                        _ => {
                            return Value::Null
                        }
                    }
                }
                BinaryOperator::Sub=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Value::Int(l - r);
                        }
                        _ => {
                            return Value::Null
                        }
                    }
                }
                BinaryOperator::Mul=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Value::Int(l * r);
                        }
                        _ => {
                            return Value::Null
                        }
                    }
                }
                BinaryOperator::Div=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Value::Float(OrderedFloat(l as f64 / r as f64));
                        }
                        _ => {
                            return Value::Null
                        }
                    }
                }
            }
        },
        Expr::Comparison { left, op, right } => {
            let left_val=evaluate(left,tuple);
            let right_val=evaluate(right,tuple);
            match op {
                ComparisonOperator::Eq=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Value::Boolean(l==r);
                        }
                        _ => {
                            return Value::Null
                        }
                    }
                }
                ComparisonOperator::Neq=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Value::Boolean(l!=r);
                        }
                        _ => {
                            return Value::Null
                        }
                    }
                }
                ComparisonOperator::Lt=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Value::Boolean(l<r);
                        }
                        _ => {
                            return Value::Null
                        }
                    }
                }
                ComparisonOperator::Gt=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Value::Boolean(l>r);
                        }
                        _ => {
                            return Value::Null
                        }
                    }
                }
                ComparisonOperator::Leq=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Value::Boolean(l<=r);
                        }
                        _ => {
                            return Value::Null
                        }
                    }
                }
                ComparisonOperator::Geq=>{
                    match (left_val,right_val) {
                        (Value::Int(l), Value::Int(r))=>{
                            return Value::Boolean(l>=r);
                        }
                        _ => {
                            return Value::Null
                        }
                    }
                }
            }
        },
    }
}