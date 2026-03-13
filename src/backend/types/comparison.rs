use std::cmp::Ordering;
use std::fmt;

use crate::types::value::DataValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComparisonError {
    TypeMismatch { left: String, right: String },
}

impl fmt::Display for ComparisonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComparisonError::TypeMismatch { left, right } => {
                write!(f, "Cannot compare {} with {}", left, right)
            }
        }
    }
}

impl std::error::Error for ComparisonError {}

pub trait Comparable: Sized {
    fn compare(&self, other: &Self) -> Result<Ordering, ComparisonError>;

    fn is_equal(&self, other: &Self) -> Result<bool, ComparisonError> {
        Ok(self.compare(other)? == Ordering::Equal)
    }
}

pub(crate) fn value_type_name(value: &DataValue) -> &'static str {
    match value {
        DataValue::SmallInt(_) => "SMALLINT",
        DataValue::Int(_) => "INT",
        DataValue::Varchar(_) => "VARCHAR",
        DataValue::Date(_) => "DATE",
        DataValue::Bit(_) => "BIT",
    }
}

impl Comparable for DataValue {
    fn compare(&self, other: &Self) -> Result<Ordering, ComparisonError> {
        match (self, other) {
            (DataValue::SmallInt(a), DataValue::SmallInt(b)) => Ok(a.cmp(b)),
            (DataValue::Int(a), DataValue::Int(b)) => Ok(a.cmp(b)),
            (DataValue::SmallInt(a), DataValue::Int(b)) => Ok((*a as i32).cmp(b)),
            (DataValue::Int(a), DataValue::SmallInt(b)) => Ok(a.cmp(&(*b as i32))),
            (DataValue::Varchar(a), DataValue::Varchar(b)) => Ok(a.cmp(b)),
            (DataValue::Date(a), DataValue::Date(b)) => Ok(a.cmp(b)),
            (DataValue::Bit(a), DataValue::Bit(b)) => Ok(a.cmp(b)),
            _ => Err(ComparisonError::TypeMismatch {
                left: value_type_name(self).to_string(),
                right: value_type_name(other).to_string(),
            }),
        }
    }
}

pub fn compare_nullable(
    left: Option<&DataValue>,
    right: Option<&DataValue>,
) -> Result<Option<Ordering>, ComparisonError> {
    match (left, right) {
        (Some(l), Some(r)) => Ok(Some(l.compare(r)?)),
        _ => Ok(None),
    }
}

pub fn nullable_equals(
    left: Option<&DataValue>,
    right: Option<&DataValue>,
) -> Result<Option<bool>, ComparisonError> {
    Ok(compare_nullable(left, right)?.map(|o| o == Ordering::Equal))
}
