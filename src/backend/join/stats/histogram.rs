//! Equi-depth histograms over key-encoded column values.
//!
//! Used for range selectivity - `l.a < r.b` - where a distinct-value count
//! says nothing useful. Two histograms are convolved bucket-pair by
//! bucket-pair, which is cheap at 64 buckets a side and vastly better than the
//! hardcoded constant it replaces.
//!
//! Boundaries are drawn from a reservoir sample rather than the whole column,
//! so building one costs a bounded amount of memory whatever the table's size.
//! The sample is seeded, so the boundaries - and therefore every estimate
//! derived from them - are reproducible.

use serde::{Deserialize, Serialize};

use super::rng::Rng;

/// Values retained for boundary selection.
pub const SAMPLE_LIMIT: usize = 20_000;

/// Buckets in a finished histogram.
pub const BUCKETS: usize = 64;

/// Seed for reservoir sampling. Fixed so ANALYZE is deterministic.
const SAMPLE_SEED: u64 = 0x5EED_A15D_0000_0001;

/// Collects a bounded, uniform sample of a column's encoded values.
#[derive(Debug, Clone)]
pub struct ReservoirSampler {
    sample: Vec<Vec<u8>>,
    seen: u64,
    rng: Rng,
}

impl Default for ReservoirSampler {
    fn default() -> Self {
        Self::new()
    }
}

impl ReservoirSampler {
    pub fn new() -> Self {
        Self {
            sample: Vec::new(),
            seen: 0,
            rng: Rng::new(SAMPLE_SEED),
        }
    }

    /// Algorithm R: every value seen has an equal chance of being retained.
    pub fn add(&mut self, encoded: &[u8]) {
        self.seen += 1;
        if self.sample.len() < SAMPLE_LIMIT {
            self.sample.push(encoded.to_vec());
            return;
        }
        let slot = self.rng.below(self.seen);
        if slot < SAMPLE_LIMIT as u64 {
            self.sample[slot as usize] = encoded.to_vec();
        }
    }

    pub fn seen(&self) -> u64 {
        self.seen
    }

    /// Cut the sample into equi-depth buckets.
    ///
    /// Returns `None` when there is too little data for boundaries to mean
    /// anything; callers fall back to a default selectivity rather than
    /// trusting a histogram built from three rows.
    pub fn finish(mut self) -> Option<Histogram> {
        if self.sample.len() < BUCKETS {
            return None;
        }
        self.sample.sort();

        let per_bucket = self.sample.len() as f64 / BUCKETS as f64;
        let mut bounds = Vec::with_capacity(BUCKETS);
        for bucket in 1..=BUCKETS {
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
    ///
    /// Each of the `buckets × buckets` pairs contributes its share: wholly
    /// below counts fully, wholly above not at all, overlapping counts half.
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
