//! Runtime value type used in expression evaluation and result rows.

use crate::catalog::types::DataType;
use std::fmt;
use std::hash::{Hash, Hasher};

/// A runtime typed value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
    Date(i32),      // days since Unix epoch
    Timestamp(i64), // microseconds since Unix epoch
    Null,
}

/// Errors that can occur during value operations.
#[derive(Debug, Clone)]
pub struct EvalError(pub String);

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EvalError: {}", self.0)
    }
}

impl From<EvalError> for std::io::Error {
    fn from(e: EvalError) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e.0)
    }
}

// Allow Values to be used as HashMap keys (needed for DISTINCT).
// f64 does not implement Eq/Hash natively, so we use bit representation.
impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Int(v) => { 0u8.hash(state); v.hash(state); }
            Value::Float(v) => { 1u8.hash(state); v.to_bits().hash(state); }
            Value::Bool(v) => { 2u8.hash(state); v.hash(state); }
            Value::Text(v) => { 3u8.hash(state); v.hash(state); }
            Value::Date(v) => { 4u8.hash(state); v.hash(state); }
            Value::Timestamp(v) => { 5u8.hash(state); v.hash(state); }
            Value::Null => { 6u8.hash(state); }
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Int(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Text(v) => write!(f, "{}", v),
            Value::Date(v) => write!(f, "Date({})", v),
            Value::Timestamp(v) => write!(f, "Timestamp({})", v),
            Value::Null => write!(f, "NULL"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CmpOp { Eq, Ne, Lt, Le, Gt, Ge }

#[derive(Debug, Clone, Copy)]
pub enum ArithOp { Add, Sub, Mul, Div }

impl Value {
    /// Cast to target DataType.  Returns Null if self is Null.
    pub fn cast(&self, target: &DataType) -> Result<Value, EvalError> {
        if matches!(self, Value::Null) {
            return Ok(Value::Null);
        }
        match target {
            DataType::Int => match self {
                Value::Int(v) => Ok(Value::Int(*v)),
                Value::Float(v) => Ok(Value::Int(*v as i64)),
                Value::Bool(v) => Ok(Value::Int(if *v { 1 } else { 0 })),
                Value::Text(s) => s.trim().parse::<i64>()
                    .map(Value::Int)
                    .map_err(|_| EvalError(format!("Cannot cast '{}' to INT", s))),
                Value::Date(v) => Ok(Value::Int(*v as i64)),
                Value::Timestamp(v) => Ok(Value::Int(*v)),
                Value::Null => unreachable!(),
            },
            DataType::Float => match self {
                Value::Int(v) => Ok(Value::Float(*v as f64)),
                Value::Float(v) => Ok(Value::Float(*v)),
                Value::Bool(v) => Ok(Value::Float(if *v { 1.0 } else { 0.0 })),
                Value::Text(s) => s.trim().parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| EvalError(format!("Cannot cast '{}' to FLOAT", s))),
                Value::Date(v) => Ok(Value::Float(*v as f64)),
                Value::Timestamp(v) => Ok(Value::Float(*v as f64)),
                Value::Null => unreachable!(),
            },
            DataType::Bool => match self {
                Value::Bool(v) => Ok(Value::Bool(*v)),
                Value::Int(v) => Ok(Value::Bool(*v != 0)),
                Value::Float(v) => Ok(Value::Bool(*v != 0.0)),
                Value::Text(s) => match s.trim().to_lowercase().as_str() {
                    "true" | "1" | "yes" => Ok(Value::Bool(true)),
                    "false" | "0" | "no" => Ok(Value::Bool(false)),
                    _ => Err(EvalError(format!("Cannot cast '{}' to BOOL", s))),
                },
                _ => Err(EvalError(format!("Cannot cast {:?} to BOOL", self))),
            },
            DataType::Text | DataType::Varchar(_) => Ok(Value::Text(self.to_string())),
            DataType::Date => match self {
                Value::Date(v) => Ok(Value::Date(*v)),
                Value::Int(v) => Ok(Value::Date(*v as i32)),
                _ => Err(EvalError(format!("Cannot cast {:?} to DATE", self))),
            },
            DataType::Timestamp => match self {
                Value::Timestamp(v) => Ok(Value::Timestamp(*v)),
                Value::Int(v) => Ok(Value::Timestamp(*v)),
                _ => Err(EvalError(format!("Cannot cast {:?} to TIMESTAMP", self))),
            },
        }
    }

    /// Coerce two values to a common numeric type for comparison.
    fn coerce_numeric(a: &Value, b: &Value) -> Option<(f64, f64)> {
        let fa = match a {
            Value::Int(v) => *v as f64,
            Value::Float(v) => *v,
            Value::Date(v) => *v as f64,
            Value::Timestamp(v) => *v as f64,
            _ => return None,
        };
        let fb = match b {
            Value::Int(v) => *v as f64,
            Value::Float(v) => *v,
            Value::Date(v) => *v as f64,
            Value::Timestamp(v) => *v as f64,
            _ => return None,
        };
        Some((fa, fb))
    }

    /// Compare two values with a comparison operator.
    /// NULL propagates: any comparison involving NULL returns false (SQL semantics).
    pub fn compare(&self, op: CmpOp, other: &Value) -> Result<bool, EvalError> {
        // NULL propagation
        if matches!(self, Value::Null) || matches!(other, Value::Null) {
            return Ok(false);
        }

        // Text comparison
        if let (Value::Text(a), Value::Text(b)) = (self, other) {
            return Ok(match op {
                CmpOp::Eq => a == b,
                CmpOp::Ne => a != b,
                CmpOp::Lt => a < b,
                CmpOp::Le => a <= b,
                CmpOp::Gt => a > b,
                CmpOp::Ge => a >= b,
            });
        }

        // Bool comparison
        if let (Value::Bool(a), Value::Bool(b)) = (self, other) {
            let (ai, bi) = (if *a { 1i64 } else { 0 }, if *b { 1i64 } else { 0 });
            return Ok(match op {
                CmpOp::Eq => ai == bi,
                CmpOp::Ne => ai != bi,
                CmpOp::Lt => ai < bi,
                CmpOp::Le => ai <= bi,
                CmpOp::Gt => ai > bi,
                CmpOp::Ge => ai >= bi,
            });
        }

        // Numeric comparison (coerce both sides to f64)
        if let Some((a, b)) = Self::coerce_numeric(self, other) {
            return Ok(match op {
                CmpOp::Eq => (a - b).abs() < f64::EPSILON,
                CmpOp::Ne => (a - b).abs() >= f64::EPSILON,
                CmpOp::Lt => a < b,
                CmpOp::Le => a <= b,
                CmpOp::Gt => a > b,
                CmpOp::Ge => a >= b,
            });
        }

        Err(EvalError(format!(
            "Cannot compare {:?} and {:?}",
            self, other
        )))
    }

    /// Arithmetic operation between two values.
    pub fn arith(&self, op: ArithOp, other: &Value) -> Result<Value, EvalError> {
        if matches!(self, Value::Null) || matches!(other, Value::Null) {
            return Ok(Value::Null);
        }

        // String concatenation via Add
        if let (ArithOp::Add, Value::Text(a), Value::Text(b)) = (&op, self, other) {
            return Ok(Value::Text(format!("{}{}", a, b)));
        }

        // Date + Int  =>  Date
        if let (ArithOp::Add, Value::Date(d), Value::Int(n)) = (&op, self, other) {
            return Ok(Value::Date(d + *n as i32));
        }
        if let (ArithOp::Add, Value::Int(n), Value::Date(d)) = (&op, self, other) {
            return Ok(Value::Date(d + *n as i32));
        }
        // Date - Date => Int (difference in days)
        if let (ArithOp::Sub, Value::Date(a), Value::Date(b)) = (&op, self, other) {
            return Ok(Value::Int((a - b) as i64));
        }

        // General numeric
        let (a, b) = Self::coerce_numeric(self, other)
            .ok_or_else(|| EvalError(format!("Cannot apply arithmetic to {:?} and {:?}", self, other)))?;

        // Prefer integer arithmetic when both operands are Int
        if let (Value::Int(ai), Value::Int(bi)) = (self, other) {
            return Ok(match op {
                ArithOp::Add => Value::Int(ai.wrapping_add(*bi)),
                ArithOp::Sub => Value::Int(ai.wrapping_sub(*bi)),
                ArithOp::Mul => Value::Int(ai.wrapping_mul(*bi)),
                ArithOp::Div => {
                    if *bi == 0 {
                        return Err(EvalError("Division by zero".to_string()));
                    }
                    Value::Int(ai / bi)
                }
            });
        }

        Ok(match op {
            ArithOp::Add => Value::Float(a + b),
            ArithOp::Sub => Value::Float(a - b),
            ArithOp::Mul => Value::Float(a * b),
            ArithOp::Div => {
                if b == 0.0 {
                    return Err(EvalError("Division by zero".to_string()));
                }
                Value::Float(a / b)
            }
        })
    }
}
