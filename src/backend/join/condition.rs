//! Join condition evaluation.
//!
//! Provides [`JoinCondition`] for simple predicates (e.g. `L.id = R.id`)
//! and [`JoinPredicate`] for complex / advanced predicates (AND, OR,
//! Semi, Anti, Natural, Lateral).

use super::tuple::{ColumnValue, Tuple};
use std::fmt;

// ── Operators ────────────────────────────────────────────────────────

/// Comparison operator for join predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl fmt::Display for JoinOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

// ── Simple Condition ─────────────────────────────────────────────────

/// A single join predicate: `left_table.left_col <op> right_table.right_col`.
#[derive(Debug, Clone)]
pub struct JoinCondition {
    pub left_table:  String,
    pub left_col:    String,
    pub operator:    JoinOp,
    pub right_table: String,
    pub right_col:   String,
}

impl JoinCondition {
    /// Evaluate this predicate for one (left, right) tuple pair.
    /// Returns `false` if either value is NULL or missing.
    pub fn evaluate(&self, left: &Tuple, right: &Tuple) -> bool {
        let left_val = match left.get_field(&self.left_col) {
            Some(v) => v,
            None => return false,
        };
        let right_val = match right.get_field(&self.right_col) {
            Some(v) => v,
            None => return false,
        };

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
                        _          => unreachable!(),
                    },
                    None => false,
                }
            }
        }
    }

    /// Returns `true` if the operator is `=`.
    pub fn is_equality(&self) -> bool {
        self.operator == JoinOp::Eq
    }
}

/// Evaluate a slice of conditions with AND semantics.
pub fn evaluate_conditions(conditions: &[JoinCondition], left: &Tuple, right: &Tuple) -> bool {
    conditions.iter().all(|c| c.evaluate(left, right))
}

// ── Enhanced Predicate ───────────────────────────────────────────────

/// Composite join predicate supporting AND, OR, Semi, Anti, Natural, and Lateral.
#[derive(Debug, Clone)]
pub enum JoinPredicate {
    /// Single condition.
    Simple(JoinCondition),

    /// Conjunction (AND) — all must match.
    And(Vec<JoinCondition>),

    /// Disjunction (OR) — at least one must match.
    /// Only NLJ can evaluate OR predicates; hash/SMJ cannot.
    Or(Vec<JoinCondition>),

    /// Semi-join: left row passes if its value is IN the right set.
    SemiJoinExpr {
        left_col: String,
        right_subquery_values: Vec<String>,
    },

    /// Anti-join: left row passes if its value is NOT IN the right set.
    AntiJoinExpr {
        left_col: String,
        right_subquery_values: Vec<String>,
    },

    /// Natural join: auto-match on all columns that share a name.
    Natural {
        left_table:     String,
        right_table:    String,
        common_columns: Vec<String>,
    },

    /// Lateral (correlated) join: inner depends on outer row values.
    Lateral {
        base_condition:      Box<JoinCondition>,
        correlation_columns: Vec<String>,
    },
}

impl JoinPredicate {
    /// Evaluate this predicate for one (left, right) tuple pair.
    pub fn evaluate(&self, left: &Tuple, right: &Tuple) -> bool {
        match self {
            JoinPredicate::Simple(cond) => cond.evaluate(left, right),

            JoinPredicate::And(conds) => conds.iter().all(|c| c.evaluate(left, right)),

            JoinPredicate::Or(conds) => conds.iter().any(|c| c.evaluate(left, right)),

            JoinPredicate::SemiJoinExpr { left_col, right_subquery_values } => {
                match left.get_field(left_col) {
                    Some(ColumnValue::Null) | None => false,
                    Some(val) => right_subquery_values.contains(&val.to_string()),
                }
            }

            JoinPredicate::AntiJoinExpr { left_col, right_subquery_values } => {
                // SQL NULL semantics: NOT IN with NULL → UNKNOWN → exclude row.
                match left.get_field(left_col) {
                    Some(ColumnValue::Null) | None => false,
                    Some(val) => !right_subquery_values.contains(&val.to_string()),
                }
            }

            JoinPredicate::Natural { common_columns, .. } => {
                if common_columns.is_empty() {
                    return false;
                }
                common_columns.iter().all(|col| {
                    match (left.get_field(col), right.get_field(col)) {
                        (Some(lv), Some(rv)) => lv.eq_value(rv),
                        _ => false,
                    }
                })
            }

            JoinPredicate::Lateral { base_condition, .. } => {
                base_condition.evaluate(left, right)
            }
        }
    }

    /// Does this predicate contain at least one equality condition?
    pub fn has_equi_condition(&self) -> bool {
        match self {
            JoinPredicate::Simple(c)                       => c.is_equality(),
            JoinPredicate::And(cs)                         => cs.iter().any(|c| c.is_equality()),
            JoinPredicate::Or(cs)                          => cs.iter().any(|c| c.is_equality()),
            JoinPredicate::SemiJoinExpr { .. }             => true,
            JoinPredicate::AntiJoinExpr { .. }             => true,
            JoinPredicate::Natural { .. }                  => true,
            JoinPredicate::Lateral { base_condition, .. }  => base_condition.is_equality(),
        }
    }

    /// Does this predicate contain at least one non-equality condition?
    pub fn has_non_equi_condition(&self) -> bool {
        match self {
            JoinPredicate::Simple(c)                       => !c.is_equality(),
            JoinPredicate::And(cs)                         => cs.iter().any(|c| !c.is_equality()),
            JoinPredicate::Or(cs)                          => cs.iter().any(|c| !c.is_equality()),
            JoinPredicate::SemiJoinExpr { .. }             => false,
            JoinPredicate::AntiJoinExpr { .. }             => false,
            JoinPredicate::Natural { .. }                  => false,
            JoinPredicate::Lateral { base_condition, .. }  => !base_condition.is_equality(),
        }
    }

    /// Returns `true` for OR predicates. Hash join and SMJ cannot handle disjunctions.
    pub fn is_disjunctive(&self) -> bool {
        matches!(self, JoinPredicate::Or(_))
    }
}


