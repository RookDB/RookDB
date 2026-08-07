//! Equi-depth histograms over key-encoded values.
//!
//! Used for range selectivity, where a distinct count says nothing useful.
//! Boundaries come from a bounded, seeded reservoir sample, so ANALYZE is
//! reproducible.

use serde::{Deserialize, Serialize};

use super::super::config::JoinTuning;
use super::rng::Rng;

/// Seed for reservoir sampling. Fixed so ANALYZE is deterministic.
const SAMPLE_SEED: u64 = 0x5EED_A15D_0000_0001;

/// Collects a bounded, uniform sample of a column's encoded values.
#[derive(Debug, Clone)]
pub struct ReservoirSampler {
    sample: Vec<Vec<u8>>,
    seen: u64,
    rng: Rng,
    limit: usize,
    buckets: usize,
}

impl Default for ReservoirSampler {
    fn default() -> Self {
        Self::new()
    }
}

impl ReservoirSampler {
    /// Uses the configured sample size and bucket count.
    pub fn new() -> Self {
        let tuning = JoinTuning::from_env();
        Self::with_limits(tuning.histogram_sample_rows, tuning.histogram_buckets)
    }

    pub fn with_limits(limit: usize, buckets: usize) -> Self {
        Self {
            sample: Vec::new(),
            seen: 0,
            rng: Rng::new(SAMPLE_SEED),
            limit: limit.max(buckets),
            buckets: buckets.max(2),
        }
    }

    /// Algorithm R: every value seen has an equal chance of being retained.
    pub fn add(&mut self, encoded: &[u8]) {
        self.seen += 1;
        if self.sample.len() < self.limit {
            self.sample.push(encoded.to_vec());
            return;
        }
        let slot = self.rng.below(self.seen);
        if slot < self.limit as u64 {
            self.sample[slot as usize] = encoded.to_vec();
        }
    }

    pub fn seen(&self) -> u64 {
        self.seen
    }

    /// Cut the sample into equi-depth buckets.
    pub fn finish(mut self) -> Option<Histogram> {
        if self.sample.len() < self.buckets {
            return None;
        }
        self.sample.sort();

        let per_bucket = self.sample.len() as f64 / self.buckets as f64;
        let mut bounds = Vec::with_capacity(self.buckets);
        for bucket in 1..=self.buckets {
            // Upper bound of each bucket, by sample position.
            let position = ((bucket as f64 * per_bucket).ceil() as usize)
                .saturating_sub(1)
                .min(self.sample.len() - 1);
            bounds.push(self.sample[position].clone());
        }

        Some(Histogram {
            bounds,
            rows_represented: self.seen,
        })
    }
}

/// Bucket upper bounds, ascending, each holding an equal share of the rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Histogram {
    pub bounds: Vec<Vec<u8>>,
    /// Rows the histogram was built from.
    pub rows_represented: u64,
}

impl Histogram {
    pub fn buckets(&self) -> usize {
        self.bounds.len()
    }

    /// Fraction of rows at or below `value`.
    ///
    /// Buckets are equal-weight, so this is bucket position over bucket count,
    /// interpolated to the middle of the bucket a value falls in.
    pub fn fraction_at_or_below(&self, value: &[u8]) -> f64 {
        if self.bounds.is_empty() {
            return 0.5;
        }
        let below = self
            .bounds
            .iter()
            .filter(|bound| bound.as_slice() < value)
            .count();
        if below == self.bounds.len() {
            return 1.0;
        }
        // Halfway through the bucket the value lands in.
        (below as f64 + 0.5) / self.bounds.len() as f64
    }

    /// Fraction of this histogram's rows that are strictly below a randomly
    /// chosen row of `other`, assuming the two columns are independent.
    pub fn fraction_less_than(&self, other: &Histogram) -> f64 {
        if self.bounds.is_empty() || other.bounds.is_empty() {
            return 1.0 / 3.0;
        }

        let mut total = 0.0;
        for mine in &self.bounds {
            for theirs in &other.bounds {
                total += match mine.cmp(theirs) {
                    std::cmp::Ordering::Less => 1.0,
                    std::cmp::Ordering::Greater => 0.0,
                    // Equal boundaries mean the ranges touch; half the pairs
                    // in an overlapping bucket satisfy a strict inequality.
                    std::cmp::Ordering::Equal => 0.5,
                };
            }
        }

        total / (self.bounds.len() * other.bounds.len()) as f64
    }
}
