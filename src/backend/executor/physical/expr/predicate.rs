//! Predicate types for evaluating filters on deserialised tuples.
//!
//! Supports SQL-style three-valued logic (True / False / UNKNOWN) and
//! correlated subqueries via `CorrelatedExists` and `CorrelatedInSubquery`.

use crate::backend::error::{ResultContext, RookError, RookResult};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::rc::Rc;

use crate::types::comparison::compare_nullable;
use crate::types::value::DataValue;

use super::super::operators::{KeyClass, PhysicalOperator, canonical_key, canonical_key_ref};
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

/// The materialized values of an uncorrelated `IN (subquery)`, hashed so a
/// lookup costs O(1) rather than a pass over every value.
#[derive(Debug)]
pub struct InList {
    /// Every value, NULLs included, for a left-hand side the hash cannot
    /// answer.
    values: Vec<Option<DataValue>>,
    /// When the non-NULL values share one [`KeyClass`]: that class and each
    /// value as [`canonical_key`]. Only a left-hand side of the same class
    /// is looked up here; any other is compared with every value.
    keys: Option<(KeyClass, HashSet<DataValue>)>,
    has_null: bool,
}

impl InList {
    pub fn new(values: Vec<Option<DataValue>>) -> Self {
        let mut present = values.iter().flatten();
        let keys = present.next().map(KeyClass::of_value).and_then(|class| {
            present.all(|v| KeyClass::of_value(v) == class).then(|| {
                (
                    class,
                    values
                        .iter()
                        .flatten()
                        .cloned()
                        .map(canonical_key)
                        .collect(),
                )
            })
        });
        let has_null = values.iter().any(Option::is_none);
        Self {
            values,
            keys,
            has_null,
        }
    }

    /// `lhs [NOT] IN (list)` under three-valued logic.
    fn contains(&self, lhs: Option<&DataValue>, negated: bool) -> RookResult<Option<bool>> {
        if let (Some(lhs), Some((class, keys))) = (lhs, &self.keys)
            && KeyClass::of_value(lhs) == *class
        {
            return Ok(if keys.contains(canonical_key_ref(lhs).as_ref()) {
                Some(!negated)
            } else if self.has_null {
                None
            } else {
                Some(negated)
            });
        }
        in_result(lhs, self.values.iter().map(|v| Ok(v.as_ref())), negated)
    }
}

/// A predicate that can be evaluated against a deserialised tuple.
#[derive(Clone, Debug)]
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
    InSubqueryResult(Expr, Rc<InList>, bool),
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

impl Predicate {
    /// This predicate with its expressions bound to positions in `schema`;
    /// see [`Expr::bind`].
    pub fn bind(&self, schema: &[ColumnInfo]) -> Predicate {
        let b = |p: &Predicate| Box::new(p.bind(schema));
        match self {
            Predicate::Compare(l, op, r) => Predicate::Compare(l.bind(schema), *op, r.bind(schema)),
            Predicate::And(l, r) => Predicate::And(b(l), b(r)),
            Predicate::Or(l, r) => Predicate::Or(b(l), b(r)),
            Predicate::Not(p) => Predicate::Not(b(p)),
            Predicate::IsNull(e) => Predicate::IsNull(e.bind(schema)),
            Predicate::IsNotNull(e) => Predicate::IsNotNull(e.bind(schema)),
            Predicate::Like(e, pattern, escape) => {
                Predicate::Like(e.bind(schema), pattern.clone(), *escape)
            }
            Predicate::InSubqueryResult(e, values, negated) => {
                Predicate::InSubqueryResult(e.bind(schema), values.clone(), *negated)
            }
            Predicate::IsDistinctFrom(l, r) => {
                Predicate::IsDistinctFrom(l.bind(schema), r.bind(schema))
            }
            Predicate::IsBoolean {
                expr,
                test,
                negated,
            } => Predicate::IsBoolean {
                expr: expr.bind(schema),
                test: *test,
                negated: *negated,
            },
            // Subqueries read the outer row by their own indices.
            Predicate::AlwaysTrue
            | Predicate::ExistsResult(_)
            | Predicate::CorrelatedExists { .. }
            | Predicate::CorrelatedInSubquery { .. }
            | Predicate::CorrelatedSubquery { .. } => self.clone(),
        }
    }

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
) -> RookResult<Option<bool>> {
    match pred {
        Predicate::AlwaysTrue => Ok(Some(true)),

        Predicate::Compare(left, op, right) => {
            let lv = left.evaluate_ref(tuple, schema)?;
            let rv = right.evaluate_ref(tuple, schema)?;

            match (lv, rv) {
                (None, _) | (_, None) => Ok(None), // NULL → UNKNOWN
                (Some(a), Some(b)) => {
                    let ordering = compare_nullable(Some(a.as_ref()), Some(b.as_ref()))?.unwrap(); // Safe: both non-NULL
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

        Predicate::IsNull(expr) => Ok(Some(expr.evaluate_ref(tuple, schema)?.is_none())),

        Predicate::IsNotNull(expr) => Ok(Some(expr.evaluate_ref(tuple, schema)?.is_some())),

        Predicate::Like(expr, pattern, escape_char) => {
            let val = expr.evaluate_ref(tuple, schema)?;
            match val.as_deref() {
                None => Ok(None), // NULL LIKE anything → UNKNOWN
                Some(DataValue::Varchar(s)) => Ok(Some(like_match(s, pattern, *escape_char))),
                Some(DataValue::Char(s)) => {
                    Ok(Some(like_match(s.trim_end(), pattern, *escape_char)))
                }
                // No implicit casts: `int_col LIKE '%'` is a type error,
                // not a predicate that quietly matches nothing.
                Some(other) => Err(RookError::TypeMismatch(format!(
                    "LIKE needs a string operand, got {}; CAST it to VARCHAR",
                    other.data_type()
                ))),
            }
        }

        Predicate::ExistsResult(exists) => Ok(Some(*exists)),

        Predicate::InSubqueryResult(expr, list, negated) => {
            let val = expr.evaluate_ref(tuple, schema)?;
            list.contains(val.as_deref(), *negated)
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
            plan_ref.reset().context("Correlated EXISTS reset error")?;
            let exists = plan_ref
                .next()
                .context("Correlated EXISTS error")?
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
            plan_ref.reset().context("Correlated IN reset error")?;
            in_result(
                lhs_val.as_ref(),
                std::iter::from_fn(|| {
                    plan_ref
                        .next()
                        .context("Correlated IN error")
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
            let lv = left.evaluate_ref(tuple, schema)?;
            let rv = right.evaluate_ref(tuple, schema)?;

            // IS DISTINCT FROM: true if values differ OR one is NULL
            // (NULL IS DISTINCT FROM NULL → false; NULL IS DISTINCT FROM 5 → true)
            match (lv, rv) {
                (None, None) => Ok(Some(false)), // both NULL → not distinct
                (None, Some(_)) | (Some(_), None) => Ok(Some(true)), // one NULL → distinct
                (Some(a), Some(b)) => {
                    let ordering = compare_nullable(Some(a.as_ref()), Some(b.as_ref()))?.unwrap(); // Safe: both non-NULL
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
///
/// `%` matches any run of characters, `_` any one, and the escape character
/// makes the character after it literal. A greedy scan that, on a mismatch,
/// lets the last `%` absorb one more character: linear for typical
/// patterns, and it allocates nothing.
fn like_match(text: &str, pattern: &str, escape_char: Option<char>) -> bool {
    let char_at = |s: &str, i: usize| s[i..].chars().next();
    let (mut ti, mut pi) = (0usize, 0usize);
    // Pattern position after the last `%`, and the text position it resumes at.
    let mut star: Option<(usize, usize)> = None;
    loop {
        if let Some(pc) = char_at(pattern, pi) {
            let after_pc = pi + pc.len_utf8();
            if pc == '%' {
                star = Some((after_pc, ti));
                pi = after_pc;
                continue;
            }
            if let Some(tc) = char_at(text, ti) {
                let (matched, next_pi) = match char_at(pattern, after_pc) {
                    Some(lit) if Some(pc) == escape_char => (tc == lit, after_pc + lit.len_utf8()),
                    _ => (pc == '_' || tc == pc, after_pc),
                };
                if matched {
                    ti += tc.len_utf8();
                    pi = next_pi;
                    continue;
                }
            }
        } else if ti == text.len() {
            return true;
        }
        match star {
            Some((star_pi, star_ti)) if star_ti < text.len() => {
                let resume = star_ti + char_at(text, star_ti).map_or(1, char::len_utf8);
                star = Some((star_pi, resume));
                pi = star_pi;
                ti = resume;
            }
            _ => return false,
        }
    }
}

/// SQL three-valued `lhs [NOT] IN (values)`.
///
/// A NULL `lhs` is UNKNOWN against any non-empty set and plain false (true for
/// NOT IN) against an empty one; a NULL in the set turns "no match" into
/// UNKNOWN.
fn in_result<T: std::borrow::Borrow<DataValue>>(
    lhs: Option<&DataValue>,
    values: impl Iterator<Item = RookResult<Option<T>>>,
    negated: bool,
) -> RookResult<Option<bool>> {
    let mut saw_null = false;
    for value in values {
        let Some(lhs) = lhs else {
            return Ok(None);
        };
        match value? {
            Some(v) => {
                let equal = compare_nullable(Some(v.borrow()), Some(lhs))?
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

#[cfg(test)]
mod like_tests {
    use super::like_match;

    /// The recursive definition of LIKE, as the oracle.
    fn reference(t: &[char], p: &[char], esc: Option<char>) -> bool {
        match (t, p) {
            (_, []) => t.is_empty(),
            (_, ['%', rest @ ..]) => (0..=t.len()).any(|i| reference(&t[i..], rest, esc)),
            ([], _) => false,
            ([tc, tr @ ..], [ec, lit, pr @ ..]) if Some(*ec) == esc => {
                tc == lit && reference(tr, pr, esc)
            }
            ([_, tr @ ..], ['_', pr @ ..]) => reference(tr, pr, esc),
            ([tc, tr @ ..], [pc, pr @ ..]) => tc == pc && reference(tr, pr, esc),
        }
    }

    #[test]
    fn matches_the_recursive_definition() {
        let alphabet = ['a', 'b', 'é', '%', '_', '!'];
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut pick = |n: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % n as u64) as usize
        };
        for _ in 0..20_000 {
            let text: String = (0..pick(7)).map(|_| alphabet[pick(3)]).collect();
            let pattern: String = (0..pick(6))
                .map(|_| alphabet[pick(alphabet.len())])
                .collect();
            let esc = [None, Some('!')][pick(2)];
            let t: Vec<char> = text.chars().collect();
            let p: Vec<char> = pattern.chars().collect();
            assert_eq!(
                like_match(&text, &pattern, esc),
                reference(&t, &p, esc),
                "{:?} LIKE {:?} ESCAPE {:?}",
                text,
                pattern,
                esc
            );
        }
    }
}
