//! Per-column statistics: what the estimator knows about a column's values.
//!
//! One pass over the rows gathers, for every column, the number of NULLs,
//! the smallest and largest value and the number of distinct values. The
//! count of distinct values is exact up to [`SKETCH_SIZE`] values and an
//! estimate beyond that (see [`DistinctSketch`]), so the pass needs memory
//! for at most that many hashes per column however large the table is.

use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

use crate::catalog::types::Column;
use crate::types::{Comparable, DataValue, row::RowDecoder};

/// Distinct hashes a [`DistinctSketch`] keeps. The count is exact below
/// this many distinct values; above it the error is about `1/sqrt(k)`, so
/// 3% at 1,024.
pub const SKETCH_SIZE: usize = 1024;

/// A k-minimum-values sketch: keeps the `k` smallest hashes seen. The
/// distinct values hash evenly over the 64-bit range, so when the `k`-th
/// smallest hash sits at fraction `f` of it, about `(k - 1) / f` distinct
/// values were seen.
#[derive(Debug, Clone)]
pub struct DistinctSketch {
    smallest: BTreeSet<u64>,
    /// Hashes at or above this cannot enter: the `k`-th smallest so far.
    cutoff: u64,
}

impl Default for DistinctSketch {
    fn default() -> Self {
        DistinctSketch {
            smallest: BTreeSet::new(),
            cutoff: u64::MAX,
        }
    }
}

impl DistinctSketch {
    pub fn insert(&mut self, value: &DataValue) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        value.hash(&mut hasher);
        // DefaultHasher is SipHash: its low bits are as good as its high ones,
        // but the estimate reads the hash as a position in 0..2^64.
        let hash = hasher.finish();
        if hash >= self.cutoff && self.smallest.len() >= SKETCH_SIZE {
            return;
        }
        if self.smallest.insert(hash) && self.smallest.len() > SKETCH_SIZE {
            self.smallest.pop_last();
        }
        if self.smallest.len() >= SKETCH_SIZE {
            self.cutoff = *self.smallest.last().expect("sketch is full");
        }
    }

    /// The number of distinct values inserted: exact until the sketch fills.
    pub fn estimate(&self) -> u64 {
        if self.smallest.len() < SKETCH_SIZE {
            return self.smallest.len() as u64;
        }
        let kth = *self.smallest.last().expect("sketch is full") as f64;
        let fraction = (kth + 1.0) / (u64::MAX as f64 + 1.0);
        ((SKETCH_SIZE - 1) as f64 / fraction).round() as u64
    }
}

/// What is known about the values of one column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// The column's name as declared.
    pub name: String,
    /// The column's type, as the catalog spells it; statistics of a column
    /// whose type changed are not used.
    pub data_type: String,
    /// Distinct non-NULL values. Exact below [`SKETCH_SIZE`], else estimated.
    pub distinct: u64,
    /// Rows where the column is NULL.
    pub null_count: u64,
    /// Smallest and largest non-NULL value; `None` when every value is NULL.
    pub min: Option<DataValue>,
    pub max: Option<DataValue>,
}

impl ColumnStatistics {
    /// The fraction of `rows` rows where the column is NULL.
    pub fn null_fraction(&self, rows: u64) -> f64 {
        if rows == 0 {
            0.0
        } else {
            (self.null_count as f64 / rows as f64).min(1.0)
        }
    }
}

/// Gathers [`ColumnStatistics`] for every column of a table as its rows go by.
pub struct ColumnCollector {
    decoder: RowDecoder,
    columns: Vec<Column>,
    sketches: Vec<DistinctSketch>,
    nulls: Vec<u64>,
    mins: Vec<Option<DataValue>>,
    maxs: Vec<Option<DataValue>>,
    row: Vec<Option<DataValue>>,
}

impl ColumnCollector {
    pub fn new(columns: &[Column]) -> Self {
        let types: Vec<_> = columns.iter().map(|c| c.data_type.clone()).collect();
        let n = columns.len();
        ColumnCollector {
            decoder: RowDecoder::new(&types),
            columns: columns.to_vec(),
            sketches: vec![DistinctSketch::default(); n],
            nulls: vec![0; n],
            mins: vec![None; n],
            maxs: vec![None; n],
            row: Vec::with_capacity(n),
        }
    }

    /// Count one stored row. A row that does not decode is skipped.
    pub fn add(&mut self, bytes: &[u8]) {
        if self.decoder.decode_into(bytes, &mut self.row).is_err() {
            return;
        }
        for (i, value) in self.row.iter().enumerate() {
            let Some(value) = value else {
                self.nulls[i] += 1;
                continue;
            };
            self.sketches[i].insert(value);
            if self.mins[i]
                .as_ref()
                .is_none_or(|m| value.compare(m).is_ok_and(|o| o.is_lt()))
            {
                self.mins[i] = Some(value.clone());
            }
            if self.maxs[i]
                .as_ref()
                .is_none_or(|m| value.compare(m).is_ok_and(|o| o.is_gt()))
            {
                self.maxs[i] = Some(value.clone());
            }
        }
    }

    pub fn finish(self) -> Vec<ColumnStatistics> {
        self.columns
            .iter()
            .enumerate()
            .map(|(i, c)| ColumnStatistics {
                name: c.name.clone(),
                data_type: format!("{:?}", c.data_type),
                distinct: self.sketches[i].estimate(),
                null_count: self.nulls[i],
                min: self.mins[i].clone(),
                max: self.maxs[i].clone(),
            })
            .collect()
    }
}

/// A value's place on a number line, for interpolating a range between a
/// column's minimum and maximum. `None` for types with no such line.
pub fn numeric_position(value: &DataValue) -> Option<f64> {
    use chrono::Timelike;
    Some(match value {
        DataValue::SmallInt(v) => *v as f64,
        DataValue::Int(v) => *v as f64,
        DataValue::BigInt(v) => *v as f64,
        DataValue::Real(v) => v.0 as f64,
        DataValue::DoublePrecision(v) => v.0,
        DataValue::Numeric(v) => v.unscaled as f64 / 10f64.powi(v.scale as i32),
        DataValue::Date(d) => d.signed_duration_since(chrono::NaiveDate::default()).num_days() as f64,
        DataValue::Time(t) => t.num_seconds_from_midnight() as f64 + t.nanosecond() as f64 / 1e9,
        DataValue::Timestamp(t) => t.and_utc().timestamp_micros() as f64,
        DataValue::Bool(_) | DataValue::Char(_) | DataValue::Varchar(_) | DataValue::Bit(_) => {
            return None;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sketch_is_exact_below_its_size() {
        let mut s = DistinctSketch::default();
        for i in 0..500 {
            s.insert(&DataValue::Int(i % 300));
        }
        assert_eq!(s.estimate(), 300);
    }

    #[test]
    fn sketch_estimates_large_counts_within_a_few_percent() {
        let mut s = DistinctSketch::default();
        for i in 0..100_000 {
            s.insert(&DataValue::Int(i));
            s.insert(&DataValue::Int(i));
        }
        let est = s.estimate() as f64;
        assert!((est - 100_000.0).abs() < 10_000.0, "estimate {}", est);
    }
}
