//! Cardinality estimation and the cost model.
//!
//! Costs are in estimated milliseconds, built from PostgreSQL-shaped
//! coefficients. Absolute values matter less than relative ones: the planner
//! only ever compares them.
//!
//! **Arithmetic rules, enforced throughout this module.** Cardinalities are
//! `u64` and combine with saturating operations. Any product that could
//! overflow is computed in `f64` and converted back with `as u64`, which
//! saturates rather than wrapping. Costs are `f64`. `usize` is not used
//! anywhere here - it is 32 bits on some targets, and the previous
//! implementation's `tuple_count as usize * bytes_per_tuple` panicked in debug
//! builds and wrapped in release ones.
//!
//! **What is an estimate, and which way it is wrong**, is set out in
//! `docs/join/cost-model.md`. In short: distinct-value counts carry the
//! sketch's error; conjuncts are assumed independent, which under-estimates
//! output for correlated predicates; and buffer-cache hits are assumed to be
//! zero, which over-estimates I/O uniformly and so does not distort the
//! comparison between plans.

use crate::executor::selection::{ComparisonOp, Expr, Predicate};

use super::algorithm::{JoinAlgorithm, JoinType};
use super::key::KeySpec;
use super::stats::{ColumnStats, TableStats};

/// Selectivity assumed for a conjunct nothing is known about.
pub const UNKNOWN_SELECTIVITY: f64 = 0.25;

/// Selectivity assumed for a range comparison with no histogram, the classic
/// textbook default.
pub const DEFAULT_RANGE_SELECTIVITY: f64 = 1.0 / 3.0;

/// Bytes per page, matching the storage layer.
const PAGE_BYTES: f64 = crate::page::PAGE_SIZE as f64;

/// Machine-dependent coefficients, in milliseconds per unit of work.
///
/// Defaults are seeded from the ratios PostgreSQL uses and refined by the
/// benchmark binary; see `docs/join/cost-model.md` for how to recalibrate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CostCoefficients {
    pub seq_page: f64,
    pub random_page: f64,
    pub cpu_tuple: f64,
    pub cpu_key: f64,
    pub cpu_hash: f64,
    pub cpu_compare: f64,
}

impl Default for CostCoefficients {
    fn default() -> Self {
        Self {
            seq_page: 0.010,
            // Random access is roughly four times a sequential page.
            random_page: 0.040,
            cpu_tuple: 0.000_10,
            cpu_key: 0.000_05,
            cpu_hash: 0.000_04,
            cpu_compare: 0.000_02,
        }
    }
}

/// What the cost model needs to know about one input.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SideEstimate {
    pub rows: u64,
    pub pages: u64,
    pub row_bytes: f64,
    /// Distinct join-key values, never zero.
    pub distinct: u64,
    /// Fraction of rows whose key is NULL and so cannot match.
    pub null_fraction: f64,
}

impl SideEstimate {
    /// Derive an input estimate from a relation's statistics and its key
    /// columns.
    ///
    /// For a composite key the distinct count is the product of the
    /// per-column counts, capped at the row count - a table cannot hold more
    /// distinct combinations than it has rows. Independence between key
    /// columns is assumed, and stated as such.
    pub fn from_stats(stats: &TableStats, key_columns: &[usize]) -> Self {
        let mut distinct = 1.0_f64;
        let mut retained = 1.0_f64;

        for index in key_columns {
            let Some(column) = stats.column(*index) else {
                continue;
            };
            distinct *= column.distinct_or_one() as f64;
            // A NULL in any component suppresses the whole key.
            retained *= 1.0 - column.null_fraction.clamp(0.0, 1.0);
        }

        let capped = distinct.min(stats.rows.max(1) as f64).max(1.0);

        Self {
            rows: stats.rows,
            pages: u64::from(stats.data_pages),
            row_bytes: stats.avg_row_bytes,
            distinct: capped as u64,
            null_fraction: (1.0 - retained).clamp(0.0, 1.0),
        }
    }

    /// Rows whose key is not NULL, and so could match something.
    pub fn matchable_rows(&self) -> f64 {
        self.rows as f64 * (1.0 - self.null_fraction)
    }

    pub fn bytes(&self) -> u64 {
        to_u64(self.rows as f64 * self.row_bytes)
    }

    /// Pages, never zero, so a cost never collapses to nothing merely because
    /// a relation is small.
    fn pages_or_one(&self) -> f64 {
        (self.pages.max(1)) as f64
    }
}

/// Saturating conversion. Rust float-to-integer casts saturate, so this cannot
/// wrap; the `max` guards against a negative intermediate.
fn to_u64(value: f64) -> u64 {
    if !value.is_finite() {
        return u64::MAX;
    }
    value.max(0.0).round() as u64
}

// ── Cardinality ──────────────────────────────────────────────────────────────

/// Estimated shape of a join's output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JoinEstimate {
    /// Rows the join will produce.
    pub output_rows: u64,
    /// Left rows with at least one match. Never exceeds the left row count.
    pub matched_left_rows: u64,
    /// Right rows with at least one match.
    pub matched_right_rows: u64,
}

/// Estimate an equijoin's output before any residual is applied.
///
/// System-R containment: each side's matchable rows are spread over its
/// distinct values, and the join pairs them through the *larger* of the two
/// distinct counts.
pub fn equijoin_rows(left: &SideEstimate, right: &SideEstimate) -> f64 {
    let divisor = left.distinct.max(right.distinct).max(1) as f64;
    left.matchable_rows() * right.matchable_rows() / divisor
}

/// Fraction of left rows that match at least one right row.
///
/// Bounded by construction: `min(ndv) / ndv_left` is at most one, so a semi
/// join can never be estimated to produce more rows than its left input has.
/// The previous implementation used `min/max` scaled by both row counts and
/// over-estimated a ten-row semi join by a hundredfold.
pub fn semi_selectivity(left: &SideEstimate, right: &SideEstimate) -> f64 {
    let left_distinct = left.distinct.max(1) as f64;
    let shared = left.distinct.min(right.distinct).max(1) as f64;
    (shared / left_distinct).clamp(0.0, 1.0)
}

/// Estimate a join's cardinality, given the residual's selectivity.
pub fn estimate_join(
    join_type: JoinType,
    left: &SideEstimate,
    right: &SideEstimate,
    has_keys: bool,
    residual_selectivity: f64,
) -> JoinEstimate {
    let residual = residual_selectivity.clamp(0.0, 1.0);

    // Without an equality every pair is a candidate; the residual is all that
    // reduces it.
    let inner_rows = if has_keys {
        equijoin_rows(left, right) * residual
    } else {
        left.rows as f64 * right.rows as f64 * residual
    };

    let semi = if has_keys {
        semi_selectivity(left, right) * residual
    } else {
        // A pure theta join: assume the residual's selectivity applies per
        // pair, so a left row matches unless every right row fails.
        1.0 - (1.0 - residual).powf(right.rows.max(1) as f64)
    }
    .clamp(0.0, 1.0);

    let matched_left = left.matchable_rows() * semi;
    let right_semi = if has_keys {
        (semi_selectivity(right, left) * residual).clamp(0.0, 1.0)
    } else {
        semi
    };
    let matched_right = right.matchable_rows() * right_semi;

    let unmatched_left = left.rows as f64 - matched_left;
    let unmatched_right = right.rows as f64 - matched_right;

    let output = match join_type {
        JoinType::Inner => inner_rows,
        JoinType::Cross => left.rows as f64 * right.rows as f64,
        JoinType::LeftOuter => inner_rows + unmatched_left.max(0.0),
        JoinType::RightOuter => inner_rows + unmatched_right.max(0.0),
        JoinType::FullOuter => inner_rows + unmatched_left.max(0.0) + unmatched_right.max(0.0),
        JoinType::Semi => matched_left,
        JoinType::Anti => unmatched_left.max(0.0),
    };

    JoinEstimate {
        output_rows: to_u64(output),
        matched_left_rows: to_u64(matched_left).min(left.rows),
        matched_right_rows: to_u64(matched_right).min(right.rows),
    }
}

// ── Residual selectivity ─────────────────────────────────────────────────────

/// Estimate how much of the candidate pairs a residual predicate keeps.
///
/// Conjuncts are assumed independent, so their selectivities multiply. A
/// cross-relation range comparison between two columns is estimated by
/// convolving their histograms when both have been analyzed; everything else
/// falls back to a documented constant.
pub fn residual_selectivity(
    residual: Option<&Predicate>,
    left_stats: &TableStats,
    right_stats: &TableStats,
    left_width: u64,
) -> f64 {
    let Some(residual) = residual else {
        return 1.0;
    };

    let mut conjuncts = Vec::new();
    flatten(residual, &mut conjuncts);

    let mut selectivity = 1.0;
    for conjunct in conjuncts {
        selectivity *= conjunct_selectivity(conjunct, left_stats, right_stats, left_width);
    }
    selectivity.clamp(0.0, 1.0)
}

fn flatten<'a>(predicate: &'a Predicate, out: &mut Vec<&'a Predicate>) {
    match predicate {
        Predicate::And(a, b) => {
            flatten(a, out);
            flatten(b, out);
        }
        other => out.push(other),
    }
}

fn conjunct_selectivity(
    conjunct: &Predicate,
    left_stats: &TableStats,
    right_stats: &TableStats,
    left_width: u64,
) -> f64 {
    let Predicate::Compare(left_expr, op, right_expr) = conjunct else {
        return UNKNOWN_SELECTIVITY;
    };
    if !is_range(*op) {
        return UNKNOWN_SELECTIVITY;
    }

    let (Expr::Column(a), Expr::Column(b)) = (left_expr.as_ref(), right_expr.as_ref()) else {
        return DEFAULT_RANGE_SELECTIVITY;
    };
    let (Some(first), Some(second)) = (a.column_index, b.column_index) else {
        return DEFAULT_RANGE_SELECTIVITY;
    };

    let first = resolve(first, left_stats, right_stats, left_width);
    let second = resolve(second, left_stats, right_stats, left_width);
    let (Some(first), Some(second)) = (first, second) else {
        return DEFAULT_RANGE_SELECTIVITY;
    };

    let (Some(first_histogram), Some(second_histogram)) =
        (first.histogram.as_ref(), second.histogram.as_ref())
    else {
        return DEFAULT_RANGE_SELECTIVITY;
    };

    let less = first_histogram.fraction_less_than(second_histogram);
    match op {
        ComparisonOp::LessThan => less,
        ComparisonOp::LessOrEqual => (less + 0.5 / second_histogram.buckets() as f64).min(1.0),
        ComparisonOp::GreaterThan => 1.0 - less,
        ComparisonOp::GreaterOrEqual => {
            (1.0 - less + 0.5 / second_histogram.buckets() as f64).min(1.0)
        }
        _ => UNKNOWN_SELECTIVITY,
    }
}

fn is_range(op: ComparisonOp) -> bool {
    matches!(
        op,
        ComparisonOp::LessThan
            | ComparisonOp::LessOrEqual
            | ComparisonOp::GreaterThan
            | ComparisonOp::GreaterOrEqual
    )
}

/// Map a concatenated-space column index back to the side it belongs to.
fn resolve<'a>(
    index: usize,
    left_stats: &'a TableStats,
    right_stats: &'a TableStats,
    left_width: u64,
) -> Option<&'a ColumnStats> {
    let index = index as u64;
    if index < left_width {
        left_stats.column(index as usize)
    } else {
        right_stats.column((index - left_width) as usize)
    }
}

/// Distinct key values a composite key can take, given per-column statistics.
pub fn key_columns_of(keys: &KeySpec, left: bool) -> Vec<usize> {
    keys.columns
        .iter()
        .map(|column| {
            if left {
                column.left_index
            } else {
                column.right_index
            }
        })
        .collect()
}

// ── Cost ─────────────────────────────────────────────────────────────────────

/// Cost of one candidate plan, split so EXPLAIN can show where it went.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JoinCost {
    pub io: f64,
    pub cpu: f64,
    /// Extra passes an external sort or a partitioning hash join will make.
    pub spill_passes: f64,
}

impl JoinCost {
    pub fn total(&self) -> f64 {
        self.io + self.cpu
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CostModel {
    pub coefficients: CostCoefficients,
    /// Working memory, in pages.
    memory_pages: f64,
}

impl CostModel {
    pub fn new(work_memory_bytes: u64) -> Self {
        Self::with_coefficients(work_memory_bytes, CostCoefficients::default())
    }

    pub fn with_coefficients(work_memory_bytes: u64, coefficients: CostCoefficients) -> Self {
        Self {
            coefficients,
            memory_pages: (work_memory_bytes as f64 / PAGE_BYTES).max(1.0),
        }
    }

    pub fn memory_pages(&self) -> f64 {
        self.memory_pages
    }

    /// Passes an external sort makes over `pages`.
    ///
    /// Zero when the input fits in memory - no run is written, so there is
    /// nothing to merge. Otherwise one pass to write the runs plus
    /// `log_fanin(runs)` to merge them, with a fan-in one below the memory
    /// budget because one buffer is needed for output.
    pub fn sort_passes(&self, pages: f64) -> f64 {
        if pages <= self.memory_pages {
            return 0.0;
        }
        let runs = (pages / self.memory_pages).ceil().max(1.0);
        let fan_in = (self.memory_pages - 1.0).max(2.0);
        1.0 + (runs.ln() / fan_in.ln()).ceil().max(0.0)
    }

    /// Cost of a join, given both inputs and how many rows it will produce.
    pub fn cost(
        &self,
        algorithm: JoinAlgorithm,
        left: &SideEstimate,
        right: &SideEstimate,
        output_rows: u64,
        block_rows: u64,
    ) -> JoinCost {
        let c = &self.coefficients;
        let left_pages = left.pages_or_one();
        let right_pages = right.pages_or_one();
        let left_rows = left.rows as f64;
        let right_rows = right.rows as f64;
        let output = output_rows as f64;

        // Every algorithm pays to emit its output.
        let emit = output * c.cpu_tuple;

        match algorithm {
            JoinAlgorithm::SimpleNestedLoop => {
                // The inner relation is re-read once per outer row.
                let io = (left_pages + left_rows * right_pages) * c.seq_page;
                let cpu = left_rows * right_rows * c.cpu_compare + emit;
                JoinCost {
                    io,
                    cpu,
                    spill_passes: 0.0,
                }
            }

            JoinAlgorithm::BlockNestedLoop => {
                // One inner pass per block of outer rows.
                let blocks = (left_rows / block_rows.max(1) as f64).ceil().max(1.0);
                let io = (left_pages + blocks * right_pages) * c.seq_page;
                let cpu = left_rows * right_rows * c.cpu_compare + emit;
                JoinCost {
                    io,
                    cpu,
                    spill_passes: blocks - 1.0,
                }
            }

            JoinAlgorithm::IndexNestedLoop => {
                // One index descent and one row fetch per outer row, plus the
                // rows each probe returns.
                let matches = if right.distinct == 0 {
                    0.0
                } else {
                    right.matchable_rows() / right.distinct as f64
                };
                let descent = (right.rows.max(2) as f64).log2().max(1.0);
                let io = left_pages * c.seq_page + left_rows * (1.0 + matches) * c.random_page;
                let cpu = left_rows * descent * c.cpu_key + emit;
                JoinCost {
                    io,
                    cpu,
                    spill_passes: 0.0,
                }
            }

            JoinAlgorithm::SortMerge => {
                let left_passes = self.sort_passes(left_pages);
                let right_passes = self.sort_passes(right_pages);
                // Each pass reads and writes the relation.
                let sort_io = 2.0 * (left_pages * left_passes + right_pages * right_passes);
                let io = (left_pages + right_pages + sort_io) * c.seq_page;
                let cpu = (left_rows + right_rows)
                    * ((left_rows + right_rows).max(2.0).log2() * c.cpu_compare + c.cpu_key)
                    + emit;
                JoinCost {
                    io,
                    cpu,
                    spill_passes: left_passes + right_passes,
                }
            }

            JoinAlgorithm::Hash | JoinAlgorithm::Adaptive => {
                // The resident fraction of the build side needs no spilling;
                // the rest is written once and read once.
                let resident = (self.memory_pages / right_pages).min(1.0);
                let spilled = 1.0 - resident;
                let io = (left_pages + right_pages + 2.0 * spilled * (left_pages + right_pages))
                    * c.seq_page;
                let cpu = (left_rows + right_rows) * (c.cpu_hash + c.cpu_key) + emit;
                JoinCost {
                    io,
                    cpu,
                    spill_passes: if spilled > 0.0 { 2.0 } else { 0.0 },
                }
            }

            JoinAlgorithm::SymmetricHash => {
                // One pass over each input, both held in memory. The planner
                // does not offer this when they do not fit, so there is no
                // spilling term to model.
                let io = (left_pages + right_pages) * c.seq_page;
                let cpu = 2.0 * (left_rows + right_rows) * (c.cpu_hash + c.cpu_key) + emit;
                JoinCost {
                    io,
                    cpu,
                    spill_passes: 0.0,
                }
            }
        }
    }
}
