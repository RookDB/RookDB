//! The cardinality estimator: how many rows a plan node produces.
//!
//! One estimator serves the logical optimizer (join order, which side of a
//! join goes where) and the physical planner (index nested-loop or hash
//! join, the `est_rows` that `EXPLAIN` shows), so both agree. It reads the
//! per-column statistics of [`TableStatistics`]:
//!
//! * `col = c` keeps `1 / distinct` of the non-NULL rows;
//! * `col < c` and the other ranges interpolate `c` between the column's
//!   minimum and maximum, or keep a third when that is not possible;
//! * `IS NULL` keeps the column's NULL fraction;
//! * `AND` multiplies, `OR` is inclusion-exclusion;
//! * an equi-join keeps `1 / max(distinct left, distinct right)` of the
//!   cross product;
//! * `GROUP BY` yields the product of the keys' distinct counts, at most
//!   the input.
//!
//! Where a table or column has no statistics the estimate falls back to the
//! fixed factors the optimizer used before: a filter keeps a tenth, a join
//! a fifth of the cross product, an aggregate half.

use std::collections::HashMap;
use std::sync::Arc;

use rook_ast::logical::*;
use rook_ast::{ComparisonOp, ConstantValue, ExprNode, JoinType, PredicateNode};

use super::TableStatistics;
use super::column::{ColumnStatistics, numeric_position};
use crate::types::DataValue;

/// Rows assumed for an input nothing is known about.
const UNKNOWN_ROWS: f64 = 1000.0;
/// Fixed factors, used where there are no statistics.
const FILTER_FACTOR: f64 = 0.1;
const JOIN_FACTOR: f64 = 0.2;
const AGGREGATE_FACTOR: f64 = 0.5;
const DISTINCT_FACTOR: f64 = 0.8;
/// What a range keeps when it cannot be interpolated.
const RANGE_FACTOR: f64 = 1.0 / 3.0;

/// Where the estimator finds statistics.
pub trait StatsSource {
    /// The statistics of `table`, if it has any.
    fn table(&self, table: &str) -> Option<Arc<TableStatistics>>;

    /// The number of rows in a table that has no statistics, if cheap to
    /// tell.
    fn row_hint(&self, _table: &str) -> Option<f64> {
        None
    }
}

impl StatsSource for HashMap<String, Arc<TableStatistics>> {
    fn table(&self, table: &str) -> Option<Arc<TableStatistics>> {
        self.get(table).cloned()
    }
}

/// A column of a base table found under a plan node.
struct Column {
    table: Arc<TableStatistics>,
    index: usize,
}

impl Column {
    fn stats(&self) -> &ColumnStatistics {
        &self.table.columns[self.index]
    }

    fn null_fraction(&self) -> f64 {
        self.stats().null_fraction(self.table.total_tuple_count)
    }

    /// Distinct values, at most `rows` (a filter or join upstream cannot
    /// leave more distinct values than rows).
    fn distinct(&self, rows: f64) -> f64 {
        (self.stats().distinct as f64).min(rows.max(1.0)).max(1.0)
    }
}

pub struct Estimator<'a> {
    source: &'a dyn StatsSource,
    /// Rows of the CTEs already materialised, by name.
    cte_rows: HashMap<String, f64>,
}

impl<'a> Estimator<'a> {
    pub fn new(source: &'a dyn StatsSource) -> Self {
        Estimator {
            source,
            cte_rows: HashMap::new(),
        }
    }

    /// Tell the estimator how many rows a materialised CTE holds.
    pub fn with_cte_rows(mut self, rows: HashMap<String, f64>) -> Self {
        self.cte_rows = rows;
        self
    }

    /// Estimated rows `plan` produces.
    pub fn rows(&self, plan: &LogicalPlan) -> f64 {
        match plan {
            LogicalPlan::TableScan(t) => self.scan_rows(t),
            LogicalPlan::Filter(f) => {
                let child = self.rows(&f.child);
                self.after_filter(&f.predicate, &f.child, child)
            }
            LogicalPlan::Project(p) => self.rows(&p.child),
            LogicalPlan::Join(j) => self.join_rows(j),
            LogicalPlan::Aggregate(a) => self.aggregate_rows(a),
            LogicalPlan::Distinct(d) => self.distinct_rows(&d.child),
            LogicalPlan::Sort(s) => {
                let child = self.rows(&s.child);
                s.limit.map_or(child, |limit| child.min(limit as f64))
            }
            LogicalPlan::Limit(l) => {
                let child = self.rows(&l.child);
                (child - l.offset as f64).max(0.0).min(l.limit as f64)
            }
            LogicalPlan::SetOp(s) => {
                let left = self.rows(&s.left);
                let right = self.rows(&s.right);
                match s.op {
                    SetOpType::Union if s.all => left + right,
                    SetOpType::Union => (left + right) * 0.7,
                    _ => left.min(right) * 0.5,
                }
            }
            LogicalPlan::Subquery(sq) => self.rows(&sq.subquery),
            LogicalPlan::Cte(c) => self.rows(&c.outer),
            LogicalPlan::RecursiveCte(rc) => self.rows(&rc.outer),
            LogicalPlan::CteScan(c) => self.cte_rows.get(&c.name).copied().unwrap_or(UNKNOWN_ROWS),
            LogicalPlan::Insert(i) if !i.values_rows.is_empty() => i.values_rows.len() as f64,
            LogicalPlan::Insert(i) => self.rows(&i.child),
        }
    }

    fn scan_rows(&self, t: &LogicalTableScan) -> f64 {
        if let Some(system) = &t.system_table_name {
            return if system == "__singlerow__" {
                1.0
            } else {
                UNKNOWN_ROWS
            };
        }
        match self.source.table(&t.table) {
            Some(stats) => stats.total_tuple_count as f64,
            None => self.source.row_hint(&t.table).unwrap_or(UNKNOWN_ROWS),
        }
    }

    /// Rows left of `rows` input rows (from `scope`) by `predicate`.
    pub fn after_filter(&self, predicate: &PredicateNode, scope: &LogicalPlan, rows: f64) -> f64 {
        if rows <= 0.0 {
            return 0.0;
        }
        let selectivity = self
            .selectivity(predicate, scope, rows)
            .unwrap_or(FILTER_FACTOR);
        (rows * selectivity).max(1.0).min(rows)
    }

    // ── predicates ───────────────────────────────────────────────────────

    /// The fraction of `rows` rows of `scope` that satisfy `pred`, or `None`
    /// when no statistics speak to it.
    fn selectivity(&self, pred: &PredicateNode, scope: &LogicalPlan, rows: f64) -> Option<f64> {
        let sel = |p: &PredicateNode| self.selectivity(p, scope, rows);
        match pred {
            PredicateNode::BinaryOp { left, op, right } => {
                let (l, r) = (sel(left), sel(right));
                if l.is_none() && r.is_none() {
                    return None;
                }
                // A side nothing is known about keeps the old fixed fraction.
                let (l, r) = (l.unwrap_or(FILTER_FACTOR), r.unwrap_or(FILTER_FACTOR));
                Some(match op {
                    rook_ast::BinaryOp::And => l * r,
                    rook_ast::BinaryOp::Or => l + r - l * r,
                })
            }
            PredicateNode::Not(inner) => sel(inner).map(|s| 1.0 - s),
            PredicateNode::Compare { left, op, right } => {
                self.compare_selectivity(left, *op, right, scope, rows)
            }
            PredicateNode::IsNull(e) => {
                let col = self.column_of(e, scope)?;
                Some(col.null_fraction())
            }
            PredicateNode::IsNotNull(e) => {
                let col = self.column_of(e, scope)?;
                Some(1.0 - col.null_fraction())
            }
            PredicateNode::Between { expr, low, high } => {
                let col = self.column_of(expr, scope)?;
                let non_null = 1.0 - col.null_fraction();
                let range = match (
                    self.position(low, col.stats()),
                    self.position(high, col.stats()),
                    bounds(col.stats()),
                ) {
                    (Some(lo), Some(hi), Some((min, max))) => fraction_between(lo, hi, min, max),
                    _ => RANGE_FACTOR,
                };
                Some(range * non_null)
            }
            PredicateNode::InList { expr, list } => {
                let col = self.column_of(expr, scope)?;
                let non_null = 1.0 - col.null_fraction();
                Some((list.len() as f64 / col.distinct(rows)).min(1.0) * non_null)
            }
            PredicateNode::Like { expr, pattern, .. } => {
                let col = self.column_of(expr, scope)?;
                if pattern.contains(['%', '_']) {
                    return None;
                }
                Some((1.0 - col.null_fraction()) / col.distinct(rows))
            }
            _ => None,
        }
    }

    fn compare_selectivity(
        &self,
        left: &ExprNode,
        op: ComparisonOp,
        right: &ExprNode,
        scope: &LogicalPlan,
        rows: f64,
    ) -> Option<f64> {
        // Put the column on the left.
        let (col_expr, other, op) = if self.column_of(left, scope).is_some() {
            (left, right, op)
        } else {
            (right, left, op.flipped())
        };
        let col = self.column_of(col_expr, scope)?;
        let non_null = 1.0 - col.null_fraction();
        let distinct = col.distinct(rows);

        if let ExprNode::Constant(ConstantValue::Null) = other {
            return Some(0.0);
        }
        // Column against column: an equality between two columns of one input.
        if let Some(other_col) = self.column_of(other, scope) {
            return Some(match op {
                ComparisonOp::Eq => 1.0 / distinct.max(other_col.distinct(rows)),
                ComparisonOp::Ne => 1.0 - 1.0 / distinct.max(other_col.distinct(rows)),
                _ => RANGE_FACTOR,
            });
        }
        Some(match op {
            ComparisonOp::Eq => non_null / distinct,
            ComparisonOp::Ne => non_null * (1.0 - 1.0 / distinct),
            ComparisonOp::Lt | ComparisonOp::Le | ComparisonOp::Gt | ComparisonOp::Ge => {
                let below = match (self.position(other, col.stats()), bounds(col.stats())) {
                    (Some(c), Some((min, max))) => Some(fraction_below(c, min, max)),
                    _ => None,
                };
                let keeps = match (below, op) {
                    (Some(b), ComparisonOp::Lt | ComparisonOp::Le) => b,
                    (Some(b), _) => 1.0 - b,
                    (None, _) => RANGE_FACTOR,
                };
                keeps * non_null
            }
        })
    }

    /// The statistics of the column `expr` names, if it is a plain column of
    /// a base table under `scope`.
    fn column_of(&self, expr: &ExprNode, scope: &LogicalPlan) -> Option<Column> {
        let (qualifier, name) = column_ref(expr)?;
        self.resolve(scope, qualifier, name)
    }

    fn resolve(&self, plan: &LogicalPlan, qualifier: Option<&str>, name: &str) -> Option<Column> {
        match plan {
            LogicalPlan::TableScan(t) => {
                if t.system_table_name.is_some() || !t.schema.contains(name) {
                    return None;
                }
                let named = |q: &str| {
                    q.eq_ignore_ascii_case(&t.table)
                        || t.alias
                            .as_deref()
                            .is_some_and(|a| q.eq_ignore_ascii_case(a))
                };
                if qualifier.is_some_and(|q| !named(q)) {
                    return None;
                }
                let table = self.source.table(&t.table)?;
                let index = table
                    .columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(name))?;
                Some(Column { table, index })
            }
            LogicalPlan::Filter(f) => self.resolve(&f.child, qualifier, name),
            LogicalPlan::Sort(s) => self.resolve(&s.child, qualifier, name),
            LogicalPlan::Limit(l) => self.resolve(&l.child, qualifier, name),
            LogicalPlan::Distinct(d) => self.resolve(&d.child, qualifier, name),
            LogicalPlan::Join(j) => self
                .resolve(&j.left, qualifier, name)
                .or_else(|| self.resolve(&j.right, qualifier, name)),
            _ => None,
        }
    }

    /// Where the constant `expr` falls on the number line of the column.
    fn position(&self, expr: &ExprNode, column: &ColumnStatistics) -> Option<f64> {
        let ExprNode::Constant(c) = expr else {
            return None;
        };
        let like = column.min.as_ref()?;
        match (c, like) {
            (ConstantValue::Int(v), _) if is_plain_number(like) => Some(*v as f64),
            (ConstantValue::Float(v), _) if is_plain_number(like) => Some(*v),
            (ConstantValue::Text(s), DataValue::Date(_)) => {
                chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .ok()
                    .and_then(|d| numeric_position(&DataValue::Date(d)))
            }
            (ConstantValue::Text(s), DataValue::Timestamp(_)) => {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .ok()
                    .and_then(|t| numeric_position(&DataValue::Timestamp(t)))
            }
            _ => None,
        }
    }

    // ── joins ────────────────────────────────────────────────────────────

    fn join_rows(&self, j: &LogicalJoin) -> f64 {
        let left = self.rows(&j.left);
        let right = self.rows(&j.right);
        let selectivity = match &j.condition {
            Some(c) => self.join_selectivity(c, &[&j.left, &j.right], &[left, right]),
            None if j.join_type == JoinType::Natural => JOIN_FACTOR,
            None => 1.0,
        };
        let matched = left * right * selectivity;
        match j.join_type {
            JoinType::Left => matched.max(left),
            JoinType::Right => matched.max(right),
            JoinType::Full => matched.max(left).max(right),
            _ => matched,
        }
    }

    /// The fraction of the cross product of `inputs` (with `rows` rows each)
    /// that satisfies `condition`.
    pub fn join_selectivity(
        &self,
        condition: &PredicateNode,
        inputs: &[&LogicalPlan],
        rows: &[f64],
    ) -> f64 {
        let mut conjuncts = Vec::new();
        collect_conjuncts(condition, &mut conjuncts);
        conjuncts
            .into_iter()
            .map(|c| self.conjunct_selectivity(c, inputs, rows))
            .product()
    }

    /// The fraction of the cross product of `inputs` that one conjunct of a
    /// join condition keeps.
    pub fn conjunct_selectivity(
        &self,
        conjunct: &PredicateNode,
        inputs: &[&LogicalPlan],
        rows: &[f64],
    ) -> f64 {
        let find = |e: &ExprNode| -> Option<(usize, Column)> {
            let (q, name) = column_ref(e)?;
            inputs
                .iter()
                .enumerate()
                .find_map(|(i, p)| self.resolve(p, q, name).map(|c| (i, c)))
        };
        match conjunct {
            PredicateNode::Compare {
                left,
                op: ComparisonOp::Eq,
                right,
            } => {
                let (l, r) = (find(left), find(right));
                match (l, r) {
                    (None, None) => JOIN_FACTOR,
                    (l, r) => {
                        let ndv =
                            |side: Option<(usize, Column)>| side.map(|(i, c)| c.distinct(rows[i]));
                        let widest = ndv(l).unwrap_or(1.0).max(ndv(r).unwrap_or(1.0));
                        1.0 / widest
                    }
                }
            }
            PredicateNode::Compare { .. } => RANGE_FACTOR,
            other => inputs
                .iter()
                .zip(rows)
                .find_map(|(p, &r)| self.selectivity(other, p, r))
                .unwrap_or(JOIN_FACTOR),
        }
    }

    /// Average rows of `inner` per value of its column `name`: what an index
    /// probe on it returns.
    pub fn rows_per_key(&self, inner: &LogicalPlan, name: &str) -> f64 {
        let rows = self.rows(inner);
        match self.resolve(inner, None, name) {
            Some(col) => (rows / col.distinct(rows)).max(1.0),
            None => 1.0,
        }
    }

    // ── aggregation ──────────────────────────────────────────────────────

    fn aggregate_rows(&self, a: &LogicalAggregate) -> f64 {
        let child = self.rows(&a.child);
        if a.group_by.is_empty() {
            return 1.0;
        }
        let groups = self.distinct_product(&a.group_by, &a.child, child);
        let rows = match groups {
            Some(g) => g.min(child),
            None => child * AGGREGATE_FACTOR,
        };
        let rows = if a.having.is_some() {
            rows * RANGE_FACTOR
        } else {
            rows
        };
        rows.max(1.0).min(child.max(1.0))
    }

    fn distinct_rows(&self, child: &LogicalPlan) -> f64 {
        let rows = self.rows(child);
        if let LogicalPlan::Project(p) = child {
            let exprs: Vec<ExprNode> = p.expressions.iter().map(|e| e.expr.clone()).collect();
            let input = self.rows(&p.child);
            if let Some(g) = self.distinct_product(&exprs, &p.child, input) {
                return g.min(rows);
            }
        }
        rows * DISTINCT_FACTOR
    }

    /// Distinct combinations of `keys` over `rows` rows of `scope`, if every
    /// key is a column with statistics.
    fn distinct_product(&self, keys: &[ExprNode], scope: &LogicalPlan, rows: f64) -> Option<f64> {
        keys.iter()
            .map(|k| self.column_of(k, scope).map(|c| c.distinct(rows)))
            .try_fold(1.0, |acc: f64, d| d.map(|d| acc * d))
    }
}

/// The qualifier and name of a plain column reference.
pub fn column_ref(expr: &ExprNode) -> Option<(Option<&str>, &str)> {
    match expr {
        ExprNode::Column(name) => Some((None, name)),
        ExprNode::Compound(parts) => match parts.as_slice() {
            [name] => Some((None, name)),
            [table, name] => Some((Some(table), name)),
            _ => None,
        },
        _ => None,
    }
}

fn collect_conjuncts<'p>(pred: &'p PredicateNode, out: &mut Vec<&'p PredicateNode>) {
    match pred {
        PredicateNode::BinaryOp {
            left,
            op: rook_ast::BinaryOp::And,
            right,
        } => {
            collect_conjuncts(left, out);
            collect_conjuncts(right, out);
        }
        other => out.push(other),
    }
}

/// Numbers a literal like `5` or `2.5` can be compared with.
fn is_plain_number(v: &DataValue) -> bool {
    matches!(
        v,
        DataValue::SmallInt(_)
            | DataValue::Int(_)
            | DataValue::BigInt(_)
            | DataValue::Real(_)
            | DataValue::DoublePrecision(_)
            | DataValue::Numeric(_)
    )
}

/// The column's minimum and maximum on the number line.
fn bounds(c: &ColumnStatistics) -> Option<(f64, f64)> {
    Some((
        numeric_position(c.min.as_ref()?)?,
        numeric_position(c.max.as_ref()?)?,
    ))
}

/// The fraction of values in `min..=max` below `c`.
fn fraction_below(c: f64, min: f64, max: f64) -> f64 {
    if max <= min {
        return if c > min { 1.0 } else { 0.0 };
    }
    ((c - min) / (max - min)).clamp(0.0, 1.0)
}

/// The fraction of values in `min..=max` between `lo` and `hi`.
fn fraction_between(lo: f64, hi: f64, min: f64, max: f64) -> f64 {
    (fraction_below(hi, min, max) - fraction_below(lo, min, max)).max(0.0)
}

// ── join algorithm cost ──────────────────────────────────────────────────

/// Relative costs, in units of reading one row of a sequential scan.
mod cost {
    /// Reading and decoding one row in a scan.
    pub const SCAN_ROW: f64 = 1.0;
    /// Inserting one row into the hash table.
    pub const HASH_BUILD_ROW: f64 = 1.5;
    /// Looking one row up in the hash table.
    pub const HASH_PROBE_ROW: f64 = 0.5;
    /// Descending the B+Tree for one key.
    pub const INDEX_PROBE: f64 = 6.0;
    /// Fetching one matching row from the heap by its pointer.
    pub const HEAP_FETCH: f64 = 2.0;
}

/// Estimated cost of joining `outer` rows to `inner` rows by probing an
/// index on the inner side once per outer row, each probe returning
/// `matches` rows.
pub fn index_nested_loop_cost(outer: f64, matches: f64) -> f64 {
    outer * (cost::INDEX_PROBE + matches * cost::HEAP_FETCH)
}

/// Estimated cost of building a hash table on `outer` rows and scanning
/// all `inner` rows through it.
pub fn hash_join_cost(outer: f64, inner: f64) -> f64 {
    outer * cost::HASH_BUILD_ROW + inner * (cost::SCAN_ROW + cost::HASH_PROBE_ROW)
}
