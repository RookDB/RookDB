//! Column-level statistics: HyperLogLog distinct counts, equi-depth histograms,
//! and selectivity estimation for cost-based query optimization.

use serde::{Deserialize, Serialize};

use crate::backend::statistics::hll::HllSketch;
use crate::types::comparison::Comparable;
use crate::types::value::DataValue;

/// Comparison operator for statistical selectivity estimation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl From<rook_ast::ComparisonOp> for StatOp {
    fn from(op: rook_ast::ComparisonOp) -> Self {
        match op {
            rook_ast::ComparisonOp::Eq => StatOp::Eq,
            rook_ast::ComparisonOp::Ne => StatOp::Ne,
            rook_ast::ComparisonOp::Lt => StatOp::Lt,
            rook_ast::ComparisonOp::Le => StatOp::Le,
            rook_ast::ComparisonOp::Gt => StatOp::Gt,
            rook_ast::ComparisonOp::Ge => StatOp::Ge,
        }
    }
}

impl From<crate::backend::executor::physical::expr::ComparisonOp> for StatOp {
    fn from(op: crate::backend::executor::physical::expr::ComparisonOp) -> Self {
        use crate::backend::executor::physical::expr::ComparisonOp as C;
        match op {
            C::Equals => StatOp::Eq,
            C::NotEquals => StatOp::Ne,
            C::LessThan => StatOp::Lt,
            C::LessOrEqual => StatOp::Le,
            C::GreaterThan => StatOp::Gt,
            C::GreaterOrEqual => StatOp::Ge,
        }
    }
}

/// A bucket in an equi-depth histogram.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub lower: DataValue,
    pub upper: DataValue,
    pub count: u64,
    pub distinct_count: u64,
}

/// Equi-depth histogram covering non-null column values.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnHistogram {
    pub buckets: Vec<HistogramBucket>,
}

/// Detailed column statistics used for cardinality estimation and index selection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    pub table_name: String,
    pub column_name: String,
    pub total_count: u64,
    pub null_count: u64,
    pub distinct_count: u64,
    pub min_value: Option<DataValue>,
    pub max_value: Option<DataValue>,
    pub histogram: ColumnHistogram,
}

impl ColumnStatistics {
    /// Total non-null tuples observed.
    pub fn non_null_count(&self) -> u64 {
        self.total_count.saturating_sub(self.null_count)
    }

    /// Estimate the selectivity (fraction in 0.0..=1.0) of a predicate `col <op> val`.
    pub fn estimate_selectivity(&self, op: StatOp, val: &DataValue) -> f64 {
        let n = self.non_null_count();
        if n == 0 {
            return 0.0;
        }

        match op {
            StatOp::Eq => self.estimate_eq(val),
            StatOp::Ne => (1.0 - self.estimate_eq(val)).clamp(0.0, 1.0),
            StatOp::Ge => self.estimate_ge(val),
            StatOp::Gt => {
                // gt is ge minus eq
                (self.estimate_ge(val) - self.estimate_eq(val)).clamp(0.0, 1.0)
            }
            StatOp::Le => (1.0 - self.estimate_ge(val) + self.estimate_eq(val)).clamp(0.0, 1.0),
            StatOp::Lt => (1.0 - self.estimate_ge(val)).clamp(0.0, 1.0),
        }
    }

    /// Estimate selectivity for `col BETWEEN low AND high`.
    pub fn estimate_between(&self, low: &DataValue, high: &DataValue) -> f64 {
        let n = self.non_null_count();
        if n == 0 {
            return 0.0;
        }
        let ge_low = self.estimate_ge(low);
        let gt_high = (self.estimate_ge(high) - self.estimate_eq(high)).max(0.0);
        (ge_low - gt_high).clamp(0.0, 1.0)
    }

    fn estimate_eq(&self, val: &DataValue) -> f64 {
        let n = self.non_null_count() as f64;
        if n == 0.0 {
            return 0.0;
        }

        // Boundary check against global min/max
        if let Some(min) = &self.min_value
            && let Ok(std::cmp::Ordering::Less) = val.compare(min) {
                return 0.0;
            }
        if let Some(max) = &self.max_value
            && let Ok(std::cmp::Ordering::Greater) = val.compare(max) {
                return 0.0;
            }

        // Check histogram buckets
        if !self.histogram.buckets.is_empty() {
            for b in &self.histogram.buckets {
                let in_lower = b.lower.compare(val).map(|o| o != std::cmp::Ordering::Greater).unwrap_or(false);
                let in_upper = val.compare(&b.upper).map(|o| o != std::cmp::Ordering::Greater).unwrap_or(false);
                if in_lower && in_upper {
                    let d = b.distinct_count.max(1) as f64;
                    return (b.count as f64 / (n * d)).clamp(0.0, 1.0);
                }
            }
        }

        // Fallback: 1 / distinct_count
        let d = self.distinct_count.max(1) as f64;
        (1.0 / d).clamp(0.0, 1.0)
    }

    fn estimate_ge(&self, val: &DataValue) -> f64 {
        let n = self.non_null_count() as f64;
        if n == 0.0 {
            return 0.0;
        }

        if let Some(min) = &self.min_value
            && let Ok(std::cmp::Ordering::Less) = val.compare(min) {
                return 1.0;
            }
        if let Some(max) = &self.max_value
            && let Ok(std::cmp::Ordering::Greater) = val.compare(max) {
                return 0.0;
            }

        if self.histogram.buckets.is_empty() {
            // Linear interpolation between min and max if numeric
            if let (Some(min), Some(max)) = (&self.min_value, &self.max_value)
                && let (Some(min_num), Some(max_num), Some(val_num)) = (
                    numeric_to_f64(min),
                    numeric_to_f64(max),
                    numeric_to_f64(val),
                ) {
                    if max_num > min_num {
                        return ((max_num - val_num) / (max_num - min_num)).clamp(0.0, 1.0);
                    }
                }
            return 0.33; // default heuristic
        }

        let mut matching_count = 0.0;
        for b in &self.histogram.buckets {
            let val_vs_lower = val.compare(&b.lower).unwrap_or(std::cmp::Ordering::Equal);
            let val_vs_upper = val.compare(&b.upper).unwrap_or(std::cmp::Ordering::Equal);

            if val_vs_lower == std::cmp::Ordering::Less || val_vs_lower == std::cmp::Ordering::Equal {
                // Entire bucket is >= val
                matching_count += b.count as f64;
            } else if val_vs_upper == std::cmp::Ordering::Greater {
                // Entire bucket is < val
                continue;
            } else {
                // Value falls inside bucket [lower, upper]
                let fraction = interpolate_fraction(&b.lower, &b.upper, val);
                matching_count += b.count as f64 * (1.0 - fraction);
            }
        }

        (matching_count / n).clamp(0.0, 1.0)
    }
}

fn numeric_to_f64(v: &DataValue) -> Option<f64> {
    match v {
        DataValue::SmallInt(i) => Some(*i as f64),
        DataValue::Int(i) => Some(*i as f64),
        DataValue::BigInt(i) => Some(*i as f64),
        DataValue::Real(f) => Some(f.0 as f64),
        DataValue::DoublePrecision(f) => Some(f.0),
        _ => None,
    }
}

fn interpolate_fraction(lower: &DataValue, upper: &DataValue, val: &DataValue) -> f64 {
    if let (Some(l), Some(u), Some(v)) = (
        numeric_to_f64(lower),
        numeric_to_f64(upper),
        numeric_to_f64(val),
    ) {
        if u > l {
            return ((v - l) / (u - l)).clamp(0.0, 1.0);
        }
    }
    0.5
}

/// Accumulator to build [`ColumnStatistics`] during a heap scan pass.
pub struct ColumnStatisticsCollector {
    table_name: String,
    column_name: String,
    total_count: u64,
    null_count: u64,
    hll: HllSketch,
    min_value: Option<DataValue>,
    max_value: Option<DataValue>,
    sample: Vec<DataValue>,
    max_sample_size: usize,
    rng_state: u64,
}

impl ColumnStatisticsCollector {
    pub const DEFAULT_MAX_SAMPLE: usize = 2000;
    pub const NUM_BUCKETS: usize = 20;

    pub fn new(table_name: String, column_name: String) -> Self {
        Self {
            table_name,
            column_name,
            total_count: 0,
            null_count: 0,
            hll: HllSketch::new(),
            min_value: None,
            max_value: None,
            sample: Vec::with_capacity(Self::DEFAULT_MAX_SAMPLE),
            max_sample_size: Self::DEFAULT_MAX_SAMPLE,
            rng_state: 0x853c49e6748fea9b,
        }
    }

    /// Ingest a single column value from a tuple.
    pub fn add(&mut self, value_opt: Option<&DataValue>) {
        self.total_count += 1;
        let Some(val) = value_opt else {
            self.null_count += 1;
            return;
        };

        self.hll.add(val);

        // Update min
        match &self.min_value {
            Some(curr) => {
                if let Ok(std::cmp::Ordering::Less) = val.compare(curr) {
                    self.min_value = Some(val.clone());
                }
            }
            None => self.min_value = Some(val.clone()),
        }

        // Update max
        match &self.max_value {
            Some(curr) => {
                if let Ok(std::cmp::Ordering::Greater) = val.compare(curr) {
                    self.max_value = Some(val.clone());
                }
            }
            None => self.max_value = Some(val.clone()),
        }

        // Reservoir sampling
        let non_null_so_far = self.total_count - self.null_count;
        if self.sample.len() < self.max_sample_size {
            self.sample.push(val.clone());
        } else {
            // Linear congruential generator step
            self.rng_state = self.rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let rand_slot = (self.rng_state % non_null_so_far) as usize;
            if rand_slot < self.max_sample_size {
                self.sample[rand_slot] = val.clone();
            }
        }
    }

    /// Finalize and compute buckets, distinct estimates, and statistics.
    pub fn finalize(mut self) -> ColumnStatistics {
        let non_null_count = self.total_count.saturating_sub(self.null_count);
        let mut distinct_est = self.hll.count().min(non_null_count);

        let mut buckets = Vec::new();

        if !self.sample.is_empty() {
            self.sample.sort_by(|a, b| a.compare(b).unwrap_or(std::cmp::Ordering::Equal));

            // If sample is small and unique count can be observed directly, refine distinct estimate
            let sample_distinct = {
                let mut d = 1u64;
                for i in 1..self.sample.len() {
                    if self.sample[i] != self.sample[i - 1] {
                        d += 1;
                    }
                }
                d
            };
            if self.total_count <= self.max_sample_size as u64 {
                distinct_est = sample_distinct;
            }

            let num_buckets = Self::NUM_BUCKETS.min(self.sample.len()).max(1);
            let chunk_size = (self.sample.len() + num_buckets - 1) / num_buckets;

            for chunk in self.sample.chunks(chunk_size) {
                let lower = chunk.first().unwrap().clone();
                let upper = chunk.last().unwrap().clone();
                let chunk_len = chunk.len() as f64;
                let bucket_count = ((chunk_len / self.sample.len() as f64) * non_null_count as f64).round() as u64;

                let mut b_distinct = 1u64;
                for i in 1..chunk.len() {
                    if chunk[i] != chunk[i - 1] {
                        b_distinct += 1;
                    }
                }

                buckets.push(HistogramBucket {
                    lower,
                    upper,
                    count: bucket_count.max(1),
                    distinct_count: b_distinct,
                });
            }
        }

        ColumnStatistics {
            table_name: self.table_name,
            column_name: self.column_name,
            total_count: self.total_count,
            null_count: self.null_count,
            distinct_count: distinct_est.max(1),
            min_value: self.min_value,
            max_value: self.max_value,
            histogram: ColumnHistogram { buckets },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collector_and_selectivity() {
        let mut coll = ColumnStatisticsCollector::new("staff".to_string(), "id".to_string());
        for i in 1..=1000i32 {
            let dv = DataValue::Int(i);
            coll.add(Some(&dv));
        }

        let stats = coll.finalize();
        assert_eq!(stats.total_count, 1000);
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.min_value, Some(DataValue::Int(1)));
        assert_eq!(stats.max_value, Some(DataValue::Int(1000)));
        assert!((750..=1000).contains(&stats.distinct_count));
        assert!(!stats.histogram.buckets.is_empty());

        // Point lookup selectivity
        let eq_sel = stats.estimate_selectivity(StatOp::Eq, &DataValue::Int(500));
        assert!(eq_sel < 0.05, "Point selectivity should be small, got {}", eq_sel);

        // Value out of range
        let out_sel = stats.estimate_selectivity(StatOp::Eq, &DataValue::Int(9999));
        assert_eq!(out_sel, 0.0);

        // Range selectivity: col >= 500 should be ~0.50
        let ge_sel = stats.estimate_selectivity(StatOp::Ge, &DataValue::Int(500));
        assert!((0.40..=0.60).contains(&ge_sel), "ge_sel should be ~0.5, got {}", ge_sel);

        // Range selectivity: col < 250 should be ~0.25
        let lt_sel = stats.estimate_selectivity(StatOp::Lt, &DataValue::Int(250));
        assert!((0.15..=0.35).contains(&lt_sel), "lt_sel should be ~0.25, got {}", lt_sel);

        // Between 200 and 800 should be ~0.60
        let bet_sel = stats.estimate_between(&DataValue::Int(200), &DataValue::Int(800));
        assert!((0.50..=0.70).contains(&bet_sel), "bet_sel should be ~0.6, got {}", bet_sel);
    }
}
