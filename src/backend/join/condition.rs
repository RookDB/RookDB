//! Join condition evaluation.

use super::tuple::{ColumnValue, Tuple};

/// Join comparison operators.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinOp {
    Eq,  // =
    Ne,  // !=
    Lt,  // <
    Le,  // <=
    Gt,  // >
    Ge,  // >=
}

impl std::fmt::Display for JoinOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinOp::Eq => write!(f, "="),
            JoinOp::Ne => write!(f, "!="),
            JoinOp::Lt => write!(f, "<"),
            JoinOp::Le => write!(f, "<="),
            JoinOp::Gt => write!(f, ">"),
            JoinOp::Ge => write!(f, ">="),
        }
    }
}

/// Describes a join predicate between two relations.
#[derive(Debug, Clone)]
pub struct JoinCondition {
    pub left_table: String,
    pub left_col: String,
    pub operator: JoinOp,
    pub right_table: String,
    pub right_col: String,
}

impl JoinCondition {
    /// Evaluate the join predicate for one pair of tuples.
    /// Returns false if either value is Null.
    pub fn evaluate(&self, left: &Tuple, right: &Tuple) -> bool {
        let left_val = match left.get_field(&self.left_col) {
            Some(v) => v,
            None => return false,
        };
        let right_val = match right.get_field(&self.right_col) {
            Some(v) => v,
            None => return false,
        };

        // Null never matches
        if matches!(left_val, ColumnValue::Null) || matches!(right_val, ColumnValue::Null) {
            return false;
        }

        match self.operator {
            JoinOp::Eq => left_val.eq_value(right_val),
            JoinOp::Ne => !left_val.eq_value(right_val),
            JoinOp::Lt | JoinOp::Le | JoinOp::Gt | JoinOp::Ge => {
                match left_val.partial_cmp_values(right_val) {
                    Some(ord) => match self.operator {
                        JoinOp::Lt => ord == std::cmp::Ordering::Less,
                        JoinOp::Le => ord != std::cmp::Ordering::Greater,
                        JoinOp::Gt => ord == std::cmp::Ordering::Greater,
                        JoinOp::Ge => ord != std::cmp::Ordering::Less,
                        _ => unreachable!(),
                    },
                    None => false,
                }
            }
        }
    }

    /// Check if this is an equality condition (needed for hash join eligibility).
    pub fn is_equality(&self) -> bool {
        self.operator == JoinOp::Eq
    }
}

/// Evaluate all conditions (AND semantics).
pub fn evaluate_conditions(conditions: &[JoinCondition], left: &Tuple, right: &Tuple) -> bool {
    conditions.iter().all(|c| c.evaluate(left, right))
}
