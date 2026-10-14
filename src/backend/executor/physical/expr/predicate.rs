//! Predicate types for evaluating filters on deserialised tuples.
//!
//! Supports SQL-style three-valued logic (True / False / UNKNOWN) and
//! correlated subqueries via `CorrelatedExists` and `CorrelatedInSubquery`.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;

use crate::types::comparison::compare_nullable;
use crate::types::value::DataValue;

use super::super::operators::PhysicalOperator;
use super::super::tuple::{ColumnInfo, Tuple};
use super::Expr;

// ── Boolean test variants ────────────────────────────────────────────────────

/// The kind of boolean test in an `IS TRUE`/`IS FALSE`/`IS UNKNOWN` predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BooleanTest {
    True,
    False,
    Unknown,
}

// ── Comparison operators ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Equals,
    NotEquals,
    LessThan,
    LessOrEqual,
    GreaterThan,
    GreaterOrEqual,
}

// ── Predicates ────────────────────────────────────────────────────────────────

/// What a [`Predicate::CorrelatedSubquery`] asks of its subquery.
#[derive(Clone, Debug)]
pub enum SubqueryKind {
    Exists,
    /// `lhs [NOT] IN (subquery)`, comparing against the first output column.
    In {
        lhs: Expr,
        negated: bool,
    },
}

/// A predicate that can be evaluated against a deserialised tuple.
///
/// Manual `Debug` impl because `CorrelatedExists`/`CorrelatedInSubquery` contain
/// `Box<dyn PhysicalOperator>` which does not implement `Debug`.
#[derive(Clone)]
pub enum Predicate {
    Compare(Expr, ComparisonOp, Expr),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
    IsNull(Expr),
    IsNotNull(Expr),
    /// SQL `expr LIKE 'pattern' [ESCAPE 'c']`.
    /// The third field is an optional escape character.
    Like(Expr, String, Option<char>),
    /// True for every tuple (used when there's no WHERE clause).
    AlwaysTrue,
    /// Pre-materialized EXISTS result (non-correlated).
    ExistsResult(bool),
    /// Pre-materialized IN (subquery) result.
    /// Contains the left-hand expression, the materialized set of values,
    /// and a negation flag (`NOT IN`).
    InSubqueryResult(Expr, Vec<Option<DataValue>>, bool),
    /// Per-row correlated EXISTS subquery execution.
    ///
    /// For each outer row, sets each `param` to the value at the corresponding
    /// `outer_col_idx` in the current tuple, then executes `inner_plan`.
    /// If the inner plan produces any tuples, EXISTS returns true.
    /// Supports multi-column correlations via multiple params/indices.
    CorrelatedExists {
        /// The inner physical operator tree (fully planned, with `CorrelatedParam`
        /// in its predicate). Reset and executed once per outer row.
        inner_plan: Rc<RefCell<Box<dyn PhysicalOperator>>>,
        /// Shared parameter cells — one per correlated column. Each is set to
        /// the corresponding outer column value before each inner plan execution.
        params: Vec<Rc<RefCell<Option<DataValue>>>>,
        /// Indices into the current (outer) tuple for each correlated column value.
        outer_col_indices: Vec<usize>,
    },
    /// Per-row correlated IN (subquery) execution.
    ///
    /// For each outer row, sets each `param` from the outer tuple, then executes
    /// `inner_plan`. Each inner tuple's first column value is compared against
    /// `lhs_expr` (evaluated against the outer tuple).
    CorrelatedInSubquery {
        inner_plan: Rc<RefCell<Box<dyn PhysicalOperator>>>,
        params: Vec<Rc<RefCell<Option<DataValue>>>>,
        outer_col_indices: Vec<usize>,
        lhs_expr: Expr,
        negated: bool,
    },
    /// A correlated EXISTS / IN subquery run as a full plan per outer key (the
    /// general case: joins, GROUP BY, LIMIT, expressions over outer columns).
    CorrelatedSubquery {
        sub: Rc<super::correlated::CorrelatedSubquery>,
        kind: SubqueryKind,
    },
    /// `a IS DISTINCT FROM b` — NULL-safe inequality.
    /// Returns true when values differ or one is NULL (NULL IS DISTINCT FROM non-NULL → true).
    IsDistinctFrom(Expr, Expr),
    /// Boolean test: `IS TRUE`, `IS FALSE`, `IS UNKNOWN` (and negated).
    IsBoolean {
        expr: Expr,
        test: BooleanTest,
        negated: bool,
    },
}

impl std::fmt::Debug for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Predicate::Compare(l, op, r) => f
                .debug_tuple("Compare")
                .field(l)
                .field(op)
                .field(r)
                .finish(),
            Predicate::And(l, r) => f.debug_tuple("And").field(l).field(r).finish(),
            Predicate::Or(l, r) => f.debug_tuple("Or").field(l).field(r).finish(),
            Predicate::Not(inner) => f.debug_tuple("Not").field(inner).finish(),
            Predicate::IsNull(e) => f.debug_tuple("IsNull").field(e).finish(),
            Predicate::IsNotNull(e) => f.debug_tuple("IsNotNull").field(e).finish(),
            Predicate::Like(e, pat, esc) => f
                .debug_tuple("Like")
                .field(e)
                .field(pat)
                .field(esc)
                .finish(),
            Predicate::AlwaysTrue => f.write_str("AlwaysTrue"),
            Predicate::ExistsResult(v) => f.debug_tuple("ExistsResult").field(v).finish(),
            Predicate::InSubqueryResult(e, vals, neg) => f
                .debug_tuple("InSubqueryResult")
                .field(e)
                .field(&format!("{} values", vals.len()))
                .field(neg)
                .finish(),
            Predicate::CorrelatedExists {
                inner_plan: _,
                params,
                outer_col_indices,
            } => f
                .debug_struct("CorrelatedExists")
                .field("inner_plan", &"<op>")
                .field("param_count", &params.len())
                .field("outer_col_indices", outer_col_indices)
                .finish(),
            Predicate::CorrelatedInSubquery {
                inner_plan: _,
                params,
                outer_col_indices,
                lhs_expr,
                negated,
            } => f
                .debug_struct("CorrelatedInSubquery")
                .field("inner_plan", &"<op>")
                .field("param_count", &params.len())
                .field("outer_col_indices", outer_col_indices)
                .field("lhs_expr", lhs_expr)
                .field("negated", negated)
                .finish(),
            Predicate::CorrelatedSubquery { sub, kind } => f
                .debug_struct("CorrelatedSubquery")
                .field("sub", sub)
                .field("kind", kind)
                .finish(),
            Predicate::IsDistinctFrom(l, r) => {
                f.debug_tuple("IsDistinctFrom").field(l).field(r).finish()
            }
            Predicate::IsBoolean {
                expr,
                test,
                negated,
            } => f
                .debug_struct("IsBoolean")
                .field("expr", expr)
                .field("test", test)
                .field("negated", negated)
                .finish(),
        }
    }
}

impl Predicate {
    pub fn and(left: Predicate, right: Predicate) -> Self {
        Predicate::And(Box::new(left), Box::new(right))
    }

    pub fn or(left: Predicate, right: Predicate) -> Self {
        Predicate::Or(Box::new(left), Box::new(right))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn not(inner: Predicate) -> Self {
        Predicate::Not(Box::new(inner))
    }

    /// Convenience: wrap two `Option<Predicate>` into a conjunction.
    pub fn combine(a: Option<Predicate>, b: Option<Predicate>) -> Option<Predicate> {
        match (a, b) {
            (Some(p1), Some(p2)) => Some(Predicate::and(p1, p2)),
            (Some(p), None) | (None, Some(p)) => Some(p),
            (None, None) => None,
        }
    }
}

/// Evaluate a predicate against a tuple and schema, returning a tri-value result.
///
/// Returns `None` for UNKNOWN (NULL involved), `Some(true)` for True,
/// `Some(false)` for False.
pub fn evaluate_predicate(
    pred: &Predicate,
    tuple: &Tuple,
    schema: &[ColumnInfo],
) -> Result<Option<bool>, String> {
    match pred {
        Predicate::AlwaysTrue => Ok(Some(true)),

        Predicate::Compare(left, op, right) => {
            let lv = left.evaluate(tuple, schema)?;
            let rv = right.evaluate(tuple, schema)?;

            match (lv, rv) {
                (None, _) | (_, None) => Ok(None), // NULL → UNKNOWN
                (Some(a), Some(b)) => {
                    let ordering = compare_nullable(Some(&a), Some(&b))
                        .map_err(|e| e.to_string())?
                        .unwrap(); // Safe: both non-NULL
                    let result = match op {
                        ComparisonOp::Equals => ordering == Ordering::Equal,
                        ComparisonOp::NotEquals => ordering != Ordering::Equal,
                        ComparisonOp::LessThan => ordering == Ordering::Less,
                        ComparisonOp::LessOrEqual => ordering != Ordering::Greater,
                        ComparisonOp::GreaterThan => ordering == Ordering::Greater,
                        ComparisonOp::GreaterOrEqual => ordering != Ordering::Less,
                    };
                    Ok(Some(result))
                }
            }
        }

        Predicate::And(left, right) => {
            let lv = evaluate_predicate(left, tuple, schema)?;
            match lv {
                Some(false) => Ok(Some(false)), // short-circuit
                _ => {
                    let rv = evaluate_predicate(right, tuple, schema)?;
                    Ok(match (lv, rv) {
                        (Some(true), Some(true)) => Some(true),
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        _ => None, // UNKNOWN
                    })
                }
            }
        }

        Predicate::Or(left, right) => {
            let lv = evaluate_predicate(left, tuple, schema)?;
            match lv {
                Some(true) => Ok(Some(true)), // short-circuit
                _ => {
                    let rv = evaluate_predicate(right, tuple, schema)?;
                    Ok(match (lv, rv) {
                        (Some(false), Some(false)) => Some(false),
                        (Some(true), _) | (_, Some(true)) => Some(true),
                        _ => None, // UNKNOWN
                    })
                }
            }
        }

        Predicate::Not(inner) => {
            let iv = evaluate_predicate(inner, tuple, schema)?;
            Ok(iv.map(|b| !b))
        }

        Predicate::IsNull(expr) => {
            let val = expr.evaluate(tuple, schema)?;
            Ok(Some(val.is_none()))
        }

        Predicate::IsNotNull(expr) => {
            let val = expr.evaluate(tuple, schema)?;
            Ok(Some(val.is_some()))
        }

        Predicate::Like(expr, pattern, escape_char) => {
            let val = expr.evaluate(tuple, schema)?;
            match val {
                None => Ok(None), // NULL LIKE anything → UNKNOWN
                Some(DataValue::Varchar(s)) => Ok(Some(like_match(&s, pattern, *escape_char))),
                Some(DataValue::Char(s)) => {
                    Ok(Some(like_match(s.trim_end(), pattern, *escape_char)))
                }
                Some(_) => Ok(None), // non-string type → UNKNOWN
            }
        }

        Predicate::ExistsResult(exists) => Ok(Some(*exists)),

        Predicate::InSubqueryResult(expr, values, negated) => {
            let val = expr.evaluate(tuple, schema)?;
            in_result(
                val.as_ref(),
                values.iter().map(|v| Ok(v.as_ref())),
                *negated,
            )
        }

        Predicate::CorrelatedExists {
            inner_plan,
            params,
            outer_col_indices,
        } => {
            // Extract all outer column values from the current tuple
            for (param, idx) in params.iter().zip(outer_col_indices.iter()) {
                let outer_val = tuple.values.get(*idx).and_then(|v| v.clone());
                *param.borrow_mut() = outer_val;
            }

            // Execute the inner plan — if any tuple passes, EXISTS is true
            let mut plan_ref = inner_plan.borrow_mut();
            plan_ref
                .reset()
                .map_err(|e| format!("Correlated EXISTS reset error: {}", e))?;
            let exists = plan_ref
                .next()
                .map_err(|e| format!("Correlated EXISTS error: {}", e))?
                .is_some();
            Ok(Some(exists))
        }

        Predicate::CorrelatedInSubquery {
            inner_plan,
            params,
            outer_col_indices,
            lhs_expr,
            negated,
        } => {
            let lhs_val = lhs_expr.evaluate(tuple, schema)?;
            for (param, idx) in params.iter().zip(outer_col_indices.iter()) {
                let outer_val = tuple.values.get(*idx).and_then(|v| v.clone());
                *param.borrow_mut() = outer_val;
            }
            let mut plan_ref = inner_plan.borrow_mut();
            plan_ref
                .reset()
                .map_err(|e| format!("Correlated IN reset error: {}", e))?;
            in_result(
                lhs_val.as_ref(),
                std::iter::from_fn(|| {
                    plan_ref
                        .next()
                        .map_err(|e| format!("Correlated IN error: {}", e))
                        .transpose()
                        .map(|r| r.map(|t| t.values.into_iter().next().flatten()))
                }),
                *negated,
            )
        }

        Predicate::CorrelatedSubquery { sub, kind } => match kind {
            SubqueryKind::Exists => Ok(Some(!sub.rows(tuple)?.is_empty())),
            SubqueryKind::In { lhs, negated } => {
                let lhs_val = lhs.evaluate(tuple, schema)?;
                let rows = sub.rows(tuple)?;
                in_result(
                    lhs_val.as_ref(),
                    rows.iter()
                        .map(|t| Ok(t.values.first().and_then(|v| v.as_ref()))),
                    *negated,
                )
            }
        },

        Predicate::IsDistinctFrom(left, right) => {
            let lv = left.evaluate(tuple, schema)?;
            let rv = right.evaluate(tuple, schema)?;

            // IS DISTINCT FROM: true if values differ OR one is NULL
            // (NULL IS DISTINCT FROM NULL → false; NULL IS DISTINCT FROM 5 → true)
            match (lv, rv) {
                (None, None) => Ok(Some(false)), // both NULL → not distinct
                (None, Some(_)) | (Some(_), None) => Ok(Some(true)), // one NULL → distinct
                (Some(a), Some(b)) => {
                    let ordering = compare_nullable(Some(&a), Some(&b))
                        .map_err(|e| e.to_string())?
                        .unwrap(); // Safe: both non-NULL
                    Ok(Some(ordering != Ordering::Equal))
                }
            }
        }

        Predicate::IsBoolean {
            expr,
            test,
            negated,
        } => {
            let val = expr.evaluate(tuple, schema)?;

            // Standard semantics:
            //   x IS TRUE     → true if x = Some(true), false otherwise (including NULL)
            //   x IS FALSE    → true if x = Some(false), false otherwise (including NULL)
            //   x IS UNKNOWN  → true if x = None, false otherwise
            //
            // Negated:
            //   x IS NOT TRUE     → same as IS FALSE OR IS UNKNOWN
            //   x IS NOT FALSE    → same as IS TRUE OR IS UNKNOWN
            //   x IS NOT UNKNOWN  → same as IS TRUE OR IS FALSE (i.e., known)
            let is_match = match (test, val) {
                (BooleanTest::True, Some(DataValue::Bool(b))) => b,
                (BooleanTest::True, _) => false,
                (BooleanTest::False, Some(DataValue::Bool(false))) => true,
                (BooleanTest::False, _) => false,
                (BooleanTest::Unknown, None) => true,
                (BooleanTest::Unknown, _) => false,
            };

            if *negated {
                Ok(Some(!is_match))
            } else {
                Ok(Some(is_match))
            }
        }
    }
}

/// SQL LIKE pattern matching with optional escape character.
fn like_match(text: &str, pattern: &str, escape_char: Option<char>) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    like_match_inner(&t, &p, escape_char)
}

fn like_match_inner(t: &[char], p: &[char], escape_char: Option<char>) -> bool {
    match (t, p) {
        (_, []) => t.is_empty(),
        // % matches any sequence (including empty)
        (_, ['%', rest @ ..]) => {
            for i in 0..=t.len() {
                if like_match_inner(&t[i..], rest, escape_char) {
                    return true;
                }
            }
            false
        }
        ([], _) => false,
        // Escape character: skip it and treat the next char as literal
        // ec is a &char from pattern slice; compare against escape_char
        ([_tc, tr @ ..], [ec, pr @ ..]) if Some(*ec) == escape_char && !pr.is_empty() => {
            // Next pattern char is literal (not a wildcard)
            let next_pc = pr[0];
            let rest_p = &pr[1..];
            if t[0] == next_pc {
                like_match_inner(&t[1..], rest_p, escape_char)
            } else {
                false
            }
        }
        // _ matches any single character
        ([_tc, tr @ ..], ['_', pr @ ..]) => like_match_inner(tr, pr, escape_char),
        // Regular character comparison
        ([tc, tr @ ..], [pc, pr @ ..]) => tc == pc && like_match_inner(tr, pr, escape_char),
    }
}

/// SQL three-valued `lhs [NOT] IN (values)`.
///
/// A NULL `lhs` is UNKNOWN against any non-empty set and plain false (true for
/// NOT IN) against an empty one; a NULL in the set turns "no match" into
/// UNKNOWN.
fn in_result<T: std::borrow::Borrow<DataValue>>(
    lhs: Option<&DataValue>,
    values: impl Iterator<Item = Result<Option<T>, String>>,
    negated: bool,
) -> Result<Option<bool>, String> {
    let mut saw_null = false;
    for value in values {
        let Some(lhs) = lhs else {
            return Ok(None);
        };
        match value? {
            Some(v) => {
                let equal = compare_nullable(Some(v.borrow()), Some(lhs))
                    .map_err(|e| e.to_string())?
                    .map(|o| o == Ordering::Equal)
                    .unwrap_or(false);
                if equal {
                    return Ok(Some(!negated));
                }
            }
            None => saw_null = true,
        }
    }
    if lhs.is_some() && saw_null {
        Ok(None)
    } else {
        Ok(Some(negated))
    }
}
