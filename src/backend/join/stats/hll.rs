//! HyperLogLog, for estimating how many distinct values a column holds.
//!
//! Distinct-value counts are the single most important input to equijoin
//! selectivity: the System-R estimate divides by the larger of the two sides'
//! counts. The previous implementation had none at all and used a hardcoded
//! `0.01` for every predicate.
//!
//! An exact `HashSet` would be simpler but grows without bound; this is 4 KiB
//! per column regardless of cardinality, and mergeable. The small-range
//! correction is not optional: without it a fifty-row test fixture estimates
//! wildly, and a cost model that cannot be checked on small fixtures cannot be
//! checked at all.

use serde::{Deserialize, Serialize};

use super::rng::hash_bytes;

/// Register-index bits. 2^12 registers is 4 KiB per column at a standard
/// error of about 1.6%.
const PRECISION: u32 = 12;
const REGISTERS: usize = 1 << PRECISION;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperLogLog {
    /// Largest leading-zero rank seen for each register.
    registers: Vec<u8>,
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLog {
    pub fn new() -> Self {
        Self {
            registers: vec![0u8; REGISTERS],
        }
    }

    /// Add an encoded value.
    ///
    /// The caller passes key-encoded bytes, so two values the join considers
    /// equal are counted once - which is what makes this estimate meaningful
    /// for join selectivity rather than merely for the column.
    pub fn add(&mut self, encoded: &[u8]) {
        let hash = hash_bytes(encoded);
        let index = (hash >> (64 - PRECISION)) as usize;

        // Rank is the position of the first set bit in the remaining bits.
        let remaining = hash << PRECISION;
        let rank = if remaining == 0 {
            (64 - PRECISION + 1) as u8
        } else {
            (remaining.leading_zeros() + 1) as u8
        };

        if let Some(register) = self.registers.get_mut(index) {
            if rank > *register {
                *register = rank;
            }
        }
    }

    /// Fold another sketch of the same precision into this one.
    pub fn merge(&mut self, other: &HyperLogLog) {
        for (mine, theirs) in self.registers.iter_mut().zip(&other.registers) {
            if *theirs > *mine {
                *mine = *theirs;
            }
        }
    }

    fn empty_registers(&self) -> usize {
        self.registers.iter().filter(|value| **value == 0).count()
    }

    /// Estimated number of distinct values added.
    pub fn estimate(&self) -> u64 {
        let m = REGISTERS as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);

        let harmonic: f64 = self
            .registers
            .iter()
            .map(|rank| 2.0_f64.powi(-(i32::from(*rank))))
            .sum();
        if harmonic == 0.0 {
            return 0;
        }
        let raw = alpha * m * m / harmonic;

        // Small-range correction: with most registers still empty, linear
        // counting is far more accurate than the raw estimate.
        let empty = self.empty_registers();
        if raw <= 2.5 * m && empty > 0 {
            let linear = m * (m / empty as f64).ln();
            return linear.round().max(0.0) as u64;
        }

        // The 64-bit hash makes the classic large-range correction
        // unnecessary: collisions do not become significant below 2^57.
        raw.round().max(0.0) as u64
    }
}
