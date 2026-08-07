//! Join conditions.
//!
//! Reuses `executor::selection`'s `Predicate` and `Expr`, adding a resolver
//! that binds columns to a side and an evaluator that takes two rows.
//! `split_conjuncts` pulls the equijoin keys out of a condition and leaves the
//! rest as a residual.

use regex::Regex;

use crate::executor::selection::{
    ColumnReference, ComparisonOp, Expr, Instruction, Predicate, TriValue, apply_and, apply_or,
    compute_arithmetic, constant_to_data_value,
};
use crate::types::comparison::compare_nullable;
use crate::types::value::DataValue;

use super::algorithm::{JoinType, pushdown_plan};
use super::error::JoinError;
use super::key::{KeyColumn, KeySpec, resolve_key_class};
use super::schema::{RelationSchema, RelationSide};

// ── Column resolution ────────────────────────────────────────────────────────

/// A column reference bound to one side of the join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnBinding {
    pub side: RelationSide,
    pub index: usize,
}

/// Binds column names in a join condition to a side and an index.
#[derive(Debug)]
pub struct SideResolver<'a> {
    left: &'a RelationSchema,
    right: &'a RelationSchema,
}

impl<'a> SideResolver<'a> {
    /// Fails if the two relations share an alias, since no qualified name
    /// could then be resolved unambiguously. A self-join must alias its sides
    /// apart.
    pub fn new(left: &'a RelationSchema, right: &'a RelationSchema) -> Result<Self, JoinError> {
        if left.alias == right.alias {
            return Err(JoinError::schema(format!(
                "both sides of the join use the alias '{}'; give each side a distinct alias",
                left.alias
            )));
        }
        Ok(Self { left, right })
    }

    pub fn left(&self) -> &RelationSchema {
        self.left
    }

    pub fn right(&self) -> &RelationSchema {
        self.right
    }

    /// Index of a column in the concatenated `left ++ right` value space that
    /// the residual evaluator works in.
    pub fn virtual_index(&self, binding: ColumnBinding) -> usize {
        match binding.side {
            RelationSide::Left => binding.index,
            RelationSide::Right => self.left.len() + binding.index,
        }
    }

    pub fn resolve(&self, name: &str) -> Result<ColumnBinding, JoinError> {
        if let Some((qualifier, column)) = name.rsplit_once('.') {
            return self.resolve_qualified(qualifier, column, name);
        }
        self.resolve_unqualified(name)
    }

    fn resolve_qualified(
        &self,
        qualifier: &str,
        column: &str,
        original: &str,
    ) -> Result<ColumnBinding, JoinError> {
        let (relation, side) = if qualifier == self.left.alias {
            (self.left, RelationSide::Left)
        } else if qualifier == self.right.alias {
            (self.right, RelationSide::Right)
        } else {
            return Err(JoinError::schema(format!(
                "'{original}' refers to unknown relation '{qualifier}'; \
                 this join has '{}' and '{}'",
                self.left.alias, self.right.alias
            )));
        };

        relation
            .column_index(column)
            .map(|index| ColumnBinding { side, index })
            .ok_or_else(|| {
                JoinError::schema(format!("relation '{qualifier}' has no column '{column}'"))
            })
    }

    fn resolve_unqualified(&self, name: &str) -> Result<ColumnBinding, JoinError> {
        match (self.left.column_index(name), self.right.column_index(name)) {
            (Some(_), Some(_)) => Err(JoinError::schema(format!(
                "column '{name}' is ambiguous: both '{}' and '{}' have it; qualify it",
                self.left.alias, self.right.alias
            ))),
            (Some(index), None) => Ok(ColumnBinding {
                side: RelationSide::Left,
                index,
            }),
            (None, Some(index)) => Ok(ColumnBinding {
                side: RelationSide::Right,
                index,
            }),
            (None, None) => Err(JoinError::schema(format!(
                "no column '{name}' in '{}' or '{}'",
                self.left.alias, self.right.alias
            ))),
        }
    }
}

// ── Which sides a subexpression touches ──────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct Touch {
    left: bool,
    right: bool,
}

impl Touch {
    const NONE: Touch = Touch {
        left: false,
        right: false,
    };

    fn of(binding: ColumnBinding) -> Self {
        match binding.side {
            RelationSide::Left => Touch {
                left: true,
                right: false,
            },
            RelationSide::Right => Touch {
                left: false,
                right: true,
            },
        }
    }

    fn merge(self, other: Touch) -> Self {
        Touch {
            left: self.left || other.left,
            right: self.right || other.right,
        }
    }

    fn is_both(self) -> bool {
        self.left && self.right
    }
}

fn expr_touch(expr: &Expr, resolver: &SideResolver) -> Result<Touch, JoinError> {
    match expr {
        Expr::Column(reference) => Ok(Touch::of(resolver.resolve(&reference.column_name)?)),
        Expr::Constant(_) => Ok(Touch::NONE),
        Expr::Add(a, b) | Expr::Sub(a, b) | Expr::Mul(a, b) | Expr::Div(a, b) => {
            Ok(expr_touch(a, resolver)?.merge(expr_touch(b, resolver)?))
        }
    }
}

fn predicate_touch(predicate: &Predicate, resolver: &SideResolver) -> Result<Touch, JoinError> {
    match predicate {
        Predicate::Compare(a, _, b) => Ok(expr_touch(a, resolver)?.merge(expr_touch(b, resolver)?)),
        Predicate::IsNull(e) | Predicate::IsNotNull(e) => expr_touch(e, resolver),
        Predicate::Not(p) | Predicate::Exists(p) => predicate_touch(p, resolver),
        Predicate::And(a, b) | Predicate::Or(a, b) => {
            Ok(predicate_touch(a, resolver)?.merge(predicate_touch(b, resolver)?))
        }
        Predicate::Between(e, low, high) => Ok(expr_touch(e, resolver)?
            .merge(expr_touch(low, resolver)?)
            .merge(expr_touch(high, resolver)?)),
        Predicate::In(e, list) => {
            let mut touch = expr_touch(e, resolver)?;
            for item in list {
                touch = touch.merge(expr_touch(item, resolver)?);
            }
            Ok(touch)
        }
        Predicate::Like(e, _, _) => expr_touch(e, resolver),
    }
}

// ── Index rewriting ──────────────────────────────────────────────────────────

/// Which index space a rewritten predicate is expressed in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rebind {
    /// The concatenated `left ++ right` space the residual evaluator uses.
    /// Names are left as written, since only indices are consulted.
    Virtual,
    /// One relation's own space, for a filter pushed into its scan. Names must
    /// be stripped of their qualifier: `SelectionExecutor` resolves against a
    /// single table whose columns are unqualified.
    LeftLocal,
    RightLocal,
}

/// Resolve a binding into the target space, returning the index and the name
/// the rewritten reference should carry.
fn rebind(
    resolver: &SideResolver,
    binding: ColumnBinding,
    original_name: &str,
    mode: Rebind,
) -> Result<(usize, String), JoinError> {
    match mode {
        Rebind::Virtual => Ok((resolver.virtual_index(binding), original_name.to_string())),
        Rebind::LeftLocal => match binding.side {
            RelationSide::Left => Ok((
                binding.index,
                resolver.left().columns[binding.index].name.clone(),
            )),
            RelationSide::Right => Err(JoinError::schema(format!(
                "internal: '{original_name}' is a right-side column in a left-only conjunct"
            ))),
        },
        Rebind::RightLocal => match binding.side {
            RelationSide::Right => Ok((
                binding.index,
                resolver.right().columns[binding.index].name.clone(),
            )),
            RelationSide::Left => Err(JoinError::schema(format!(
                "internal: '{original_name}' is a left-side column in a right-only conjunct"
            ))),
        },
    }
}

/// Rewrite every column reference into `mode`'s index space, and compile any
/// LIKE pattern that has not been compiled yet.
fn rewrite_predicate(
    predicate: &Predicate,
    resolver: &SideResolver,
    mode: Rebind,
) -> Result<Predicate, JoinError> {
    Ok(match predicate {
        Predicate::Compare(a, op, b) => Predicate::Compare(
            Box::new(rewrite_expr(a, resolver, mode)?),
            *op,
            Box::new(rewrite_expr(b, resolver, mode)?),
        ),
        Predicate::IsNull(e) => Predicate::IsNull(Box::new(rewrite_expr(e, resolver, mode)?)),
        Predicate::IsNotNull(e) => Predicate::IsNotNull(Box::new(rewrite_expr(e, resolver, mode)?)),
        Predicate::Not(p) => Predicate::Not(Box::new(rewrite_predicate(p, resolver, mode)?)),
        Predicate::Exists(p) => Predicate::Exists(Box::new(rewrite_predicate(p, resolver, mode)?)),
        Predicate::And(a, b) => Predicate::And(
            Box::new(rewrite_predicate(a, resolver, mode)?),
            Box::new(rewrite_predicate(b, resolver, mode)?),
        ),
        Predicate::Or(a, b) => Predicate::Or(
            Box::new(rewrite_predicate(a, resolver, mode)?),
            Box::new(rewrite_predicate(b, resolver, mode)?),
        ),
        Predicate::Between(e, low, high) => Predicate::Between(
            Box::new(rewrite_expr(e, resolver, mode)?),
            Box::new(rewrite_expr(low, resolver, mode)?),
            Box::new(rewrite_expr(high, resolver, mode)?),
        ),
        Predicate::In(e, list) => {
            let mut rewritten = Vec::with_capacity(list.len());
            for item in list {
                rewritten.push(rewrite_expr(item, resolver, mode)?);
            }
            Predicate::In(Box::new(rewrite_expr(e, resolver, mode)?), rewritten)
        }
        Predicate::Like(e, pattern, compiled) => {
            let regex = match compiled {
                Some(regex) => regex.clone(),
                None => compile_like(pattern)?,
            };
            Predicate::Like(
                Box::new(rewrite_expr(e, resolver, mode)?),
                pattern.clone(),
                Some(regex),
            )
        }
    })
}

fn rewrite_expr(expr: &Expr, resolver: &SideResolver, mode: Rebind) -> Result<Expr, JoinError> {
    Ok(match expr {
        Expr::Column(reference) => {
            let binding = resolver.resolve(&reference.column_name)?;
            let (index, name) = rebind(resolver, binding, &reference.column_name, mode)?;
            Expr::Column(ColumnReference::with_index(name, index))
        }
        Expr::Constant(constant) => Expr::Constant(constant.clone()),
        Expr::Add(a, b) => Expr::Add(
            Box::new(rewrite_expr(a, resolver, mode)?),
            Box::new(rewrite_expr(b, resolver, mode)?),
        ),
        Expr::Sub(a, b) => Expr::Sub(
            Box::new(rewrite_expr(a, resolver, mode)?),
            Box::new(rewrite_expr(b, resolver, mode)?),
        ),
        Expr::Mul(a, b) => Expr::Mul(
            Box::new(rewrite_expr(a, resolver, mode)?),
            Box::new(rewrite_expr(b, resolver, mode)?),
        ),
        Expr::Div(a, b) => Expr::Div(
            Box::new(rewrite_expr(a, resolver, mode)?),
            Box::new(rewrite_expr(b, resolver, mode)?),
        ),
    })
}

/// Translate a SQL LIKE pattern into an anchored regex.
///
/// `%` matches any run of characters, `_` matches exactly one; everything else
/// is literal, so regex metacharacters in the pattern are escaped.
fn compile_like(pattern: &str) -> Result<Regex, JoinError> {
    let mut source = String::with_capacity(pattern.len() + 8);
    source.push('^');
    for character in pattern.chars() {
        match character {
            '%' => source.push_str(".*"),
            '_' => source.push('.'),
            other => source.push_str(&regex::escape(&other.to_string())),
        }
    }
    source.push('$');

    Regex::new(&source)
        .map_err(|e| JoinError::schema(format!("invalid LIKE pattern {pattern:?}: {e}")))
}

// ── Conjunct splitting ───────────────────────────────────────────────────────

/// The result of decomposing a join condition.
#[derive(Debug, Clone, Default)]
pub struct PredicateSplit {
    pub keys: KeySpec,
    pub residual: Option<Predicate>,
    pub left_local: Option<Predicate>,
    pub right_local: Option<Predicate>,
}

/// Decompose a join condition into keys, residual, and per-side filters.
pub fn split_conjuncts(
    condition: Option<&Predicate>,
    resolver: &SideResolver,
    join_type: JoinType,
) -> Result<PredicateSplit, JoinError> {
    let mut split = PredicateSplit::default();
    let Some(condition) = condition else {
        return Ok(split);
    };
    let pushdown = pushdown_plan(join_type);

    let mut conjuncts = Vec::new();
    flatten_and(condition, &mut conjuncts);

    let mut key_columns = Vec::new();
    let mut residual_parts = Vec::new();
    let mut left_parts = Vec::new();
    let mut right_parts = Vec::new();

    for conjunct in conjuncts {
        if let Some(key) = equi_key_column(conjunct, resolver)? {
            key_columns.push(key);
            continue;
        }

        let touch = predicate_touch(conjunct, resolver)?;
        if touch.is_both() || touch == Touch::NONE {
            residual_parts.push(rewrite_predicate(conjunct, resolver, Rebind::Virtual)?);
        } else if touch.left {
            if pushdown.left {
                left_parts.push(rewrite_predicate(conjunct, resolver, Rebind::LeftLocal)?);
            } else {
                residual_parts.push(rewrite_predicate(conjunct, resolver, Rebind::Virtual)?);
            }
        } else if pushdown.right {
            right_parts.push(rewrite_predicate(conjunct, resolver, Rebind::RightLocal)?);
        } else {
            residual_parts.push(rewrite_predicate(conjunct, resolver, Rebind::Virtual)?);
        }
    }

    split.keys = KeySpec::new(key_columns);
    split.residual = conjoin(residual_parts);
    split.left_local = conjoin(left_parts);
    split.right_local = conjoin(right_parts);
    Ok(split)
}

/// Recognise `left.col = right.col`, in either written order, and build the
/// key component for it.
fn equi_key_column(
    conjunct: &Predicate,
    resolver: &SideResolver,
) -> Result<Option<KeyColumn>, JoinError> {
    let Predicate::Compare(left_expr, ComparisonOp::Equals, right_expr) = conjunct else {
        return Ok(None);
    };
    let (Expr::Column(a), Expr::Column(b)) = (left_expr.as_ref(), right_expr.as_ref()) else {
        return Ok(None);
    };

    let first = resolver.resolve(&a.column_name)?;
    let second = resolver.resolve(&b.column_name)?;
    if first.side == second.side {
        // Both columns on one side: a filter, not a join key.
        return Ok(None);
    }

    // Normalise orientation here, once, so no executor has to.
    let (left_index, right_index) = match first.side {
        RelationSide::Left => (first.index, second.index),
        RelationSide::Right => (second.index, first.index),
    };

    let left_type = &resolver.left().columns[left_index].data_type;
    let right_type = &resolver.right().columns[right_index].data_type;
    let class = resolve_key_class(left_type, right_type)?;

    Ok(Some(KeyColumn {
        left_index,
        right_index,
        class,
    }))
}

fn flatten_and<'a>(predicate: &'a Predicate, out: &mut Vec<&'a Predicate>) {
    match predicate {
        Predicate::And(a, b) => {
            flatten_and(a, out);
            flatten_and(b, out);
        }
        other => out.push(other),
    }
}

fn conjoin(mut parts: Vec<Predicate>) -> Option<Predicate> {
    let first = parts.pop()?;
    Some(parts.into_iter().fold(first, Predicate::and))
}

// ── Two-relation evaluation ──────────────────────────────────────────────────

/// A residual join condition, ready to evaluate against a pair of rows.
#[derive(Debug, Clone)]
pub struct JoinPredicate {
    predicate: Predicate,
    left_width: usize,
}

impl JoinPredicate {
    /// `predicate` must already be resolved into the concatenated index space,
    /// which is what [`split_conjuncts`] produces.
    pub fn new(predicate: Predicate, left_width: usize) -> Self {
        Self {
            predicate,
            left_width,
        }
    }

    /// Evaluate against a left row and a right row.
    ///
    /// Only `TriValue::True` should produce output. `Unknown` never does, in
    /// any join type - that is what makes NULL comparisons non-matching.
    pub fn evaluate(
        &self,
        left: &[Option<DataValue>],
        right: &[Option<DataValue>],
    ) -> Result<TriValue, JoinError> {
        self.eval_predicate(&self.predicate, left, right)
    }

    fn value_at<'r>(
        &self,
        index: usize,
        left: &'r [Option<DataValue>],
        right: &'r [Option<DataValue>],
    ) -> Result<&'r Option<DataValue>, JoinError> {
        let slot = if index < self.left_width {
            left.get(index)
        } else {
            right.get(index - self.left_width)
        };

        slot.ok_or_else(|| {
            JoinError::schema(format!(
                "join condition refers to column {index}, but the row pair has {} + {} columns",
                left.len(),
                right.len()
            ))
        })
    }

    fn eval_expr(
        &self,
        expr: &Expr,
        left: &[Option<DataValue>],
        right: &[Option<DataValue>],
    ) -> Result<Option<DataValue>, JoinError> {
        match expr {
            Expr::Column(reference) => {
                let index = reference.column_index.ok_or_else(|| {
                    JoinError::schema(format!(
                        "column '{}' was never resolved to an index",
                        reference.column_name
                    ))
                })?;
                Ok(self.value_at(index, left, right)?.clone())
            }
            Expr::Constant(constant) => Ok(constant_to_data_value(constant)),
            Expr::Add(a, b) => self.arithmetic(&Instruction::Add, a, b, left, right),
            Expr::Sub(a, b) => self.arithmetic(&Instruction::Sub, a, b, left, right),
            Expr::Mul(a, b) => self.arithmetic(&Instruction::Mul, a, b, left, right),
            Expr::Div(a, b) => self.arithmetic(&Instruction::Div, a, b, left, right),
        }
    }

    fn arithmetic(
        &self,
        op: &Instruction,
        a: &Expr,
        b: &Expr,
        left: &[Option<DataValue>],
        right: &[Option<DataValue>],
    ) -> Result<Option<DataValue>, JoinError> {
        let lhs = self.eval_expr(a, left, right)?;
        let rhs = self.eval_expr(b, left, right)?;
        compute_arithmetic(op, lhs, rhs).map_err(JoinError::schema)
    }

    fn eval_predicate(
        &self,
        predicate: &Predicate,
        left: &[Option<DataValue>],
        right: &[Option<DataValue>],
    ) -> Result<TriValue, JoinError> {
        match predicate {
            Predicate::Compare(a, op, b) => {
                let lhs = self.eval_expr(a, left, right)?;
                let rhs = self.eval_expr(b, left, right)?;
                compare_values(lhs.as_ref(), rhs.as_ref(), *op)
            }

            Predicate::IsNull(e) => Ok(tri(self.eval_expr(e, left, right)?.is_none())),
            Predicate::IsNotNull(e) => Ok(tri(self.eval_expr(e, left, right)?.is_some())),

            Predicate::Not(inner) => Ok(match self.eval_predicate(inner, left, right)? {
                TriValue::True => TriValue::False,
                TriValue::False => TriValue::True,
                TriValue::Unknown => TriValue::Unknown,
            }),

            Predicate::Exists(inner) => self.eval_predicate(inner, left, right),

            Predicate::And(a, b) => {
                let lhs = self.eval_predicate(a, left, right)?;
                // Short-circuit: False AND anything is False, including
                // False AND Unknown.
                if lhs == TriValue::False {
                    return Ok(TriValue::False);
                }
                Ok(apply_and(lhs, self.eval_predicate(b, left, right)?))
            }

            Predicate::Or(a, b) => {
                let lhs = self.eval_predicate(a, left, right)?;
                // True OR anything is True, including True OR Unknown.
                if lhs == TriValue::True {
                    return Ok(TriValue::True);
                }
                Ok(apply_or(lhs, self.eval_predicate(b, left, right)?))
            }

            Predicate::Between(e, low, high) => {
                let value = self.eval_expr(e, left, right)?;
                let low = self.eval_expr(low, left, right)?;
                let high = self.eval_expr(high, left, right)?;
                let lower =
                    compare_values(value.as_ref(), low.as_ref(), ComparisonOp::GreaterOrEqual)?;
                if lower == TriValue::False {
                    return Ok(TriValue::False);
                }
                let upper =
                    compare_values(value.as_ref(), high.as_ref(), ComparisonOp::LessOrEqual)?;
                Ok(apply_and(lower, upper))
            }

            Predicate::In(e, list) => {
                let value = self.eval_expr(e, left, right)?;
                // NULL IN (...) is Unknown regardless of the list.
                if value.is_none() {
                    return Ok(TriValue::Unknown);
                }
                let mut result = TriValue::False;
                for item in list {
                    let candidate = self.eval_expr(item, left, right)?;
                    let matched =
                        compare_values(value.as_ref(), candidate.as_ref(), ComparisonOp::Equals)?;
                    if matched == TriValue::True {
                        return Ok(TriValue::True);
                    }
                    result = apply_or(result, matched);
                }
                Ok(result)
            }

            Predicate::Like(e, pattern, compiled) => {
                let Some(value) = self.eval_expr(e, left, right)? else {
                    return Ok(TriValue::Unknown);
                };
                let text = match &value {
                    DataValue::Varchar(s) | DataValue::Bit(s) => s.clone(),
                    // CHAR is blank-padded on disk; LIKE compares the logical
                    // value, so the padding is not part of it.
                    DataValue::Char(s) => s.trim_end().to_string(),
                    other => other.to_string(),
                };
                let regex = match compiled {
                    Some(regex) => regex.clone(),
                    None => compile_like(pattern)?,
                };
                Ok(tri(regex.is_match(&text)))
            }
        }
    }
}

fn tri(value: bool) -> TriValue {
    if value {
        TriValue::True
    } else {
        TriValue::False
    }
}

/// Compare two nullable values under an operator, in three-valued logic.
///
/// A type mismatch is an error, matching what `SelectionExecutor` does for the
/// same comparison - an incomparable pair is a broken query, not a false one.
fn compare_values(
    left: Option<&DataValue>,
    right: Option<&DataValue>,
    op: ComparisonOp,
) -> Result<TriValue, JoinError> {
    use std::cmp::Ordering;

    let ordering = compare_nullable(left, right).map_err(|e| JoinError::schema(e.to_string()))?;

    Ok(match ordering {
        None => TriValue::Unknown,
        Some(ordering) => tri(match op {
            ComparisonOp::Equals => ordering == Ordering::Equal,
            ComparisonOp::NotEquals => ordering != Ordering::Equal,
            ComparisonOp::LessThan => ordering == Ordering::Less,
            ComparisonOp::LessOrEqual => ordering != Ordering::Greater,
            ComparisonOp::GreaterThan => ordering == Ordering::Greater,
            ComparisonOp::GreaterOrEqual => ordering != Ordering::Less,
        }),
    })
}
