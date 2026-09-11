//! HyperLogLog (HLL) cardinality estimator.
//!
//! Provides probabilistic counting of unique values in a single pass with
//! fixed memory ($m = 64$ 6-bit registers = 64 bytes).
//! Standard error is $\approx 1.04 / \sqrt{m} \approx 13\%$.

use std::hash::{Hash, Hasher};
use serde::{Deserialize, Serialize};

/// HyperLogLog sketch with 64 registers ($p = 6$).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HllSketch {
    pub registers: Vec<u8>,
}

impl Default for HllSketch {
    fn default() -> Self {
        Self::new()
    }
}

impl HllSketch {
    pub const M: usize = 64;
    pub const P: u32 = 6;

    /// Create a new, empty HyperLogLog sketch.
    pub fn new() -> Self {
        Self {
            registers: vec![0u8; 64],
        }
    }

    /// Add a hashable item to the sketch.
    pub fn add<T: Hash + ?Sized>(&mut self, item: &T) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        item.hash(&mut hasher);
        let h = hasher.finish();

        // Register index: lower 6 bits
        let idx = (h & 0x3F) as usize;

        // Remaining 58 bits for leading-zero count
        let remaining = h >> 6;
        // In remaining 58 bits, leading zeros in a 64-bit int will have at least 6 zeros.
        // Saturating sub 6 gives zeros in the 58-bit portion.
        let lz = (remaining.leading_zeros().saturating_sub(6) + 1).min(59) as u8;

        if lz > self.registers[idx] {
            self.registers[idx] = lz;
        }
    }

    /// Merge another sketch into this one.
    pub fn merge(&mut self, other: &HllSketch) {
        for (r1, r2) in self.registers.iter_mut().zip(other.registers.iter()) {
            if *r2 > *r1 {
                *r1 = *r2;
            }
        }
    }

    /// Estimate the number of distinct elements observed.
    pub fn count(&self) -> u64 {
        let m = Self::M as f64;
        // Alpha constant for m = 64: 0.7213 / (1 + 1.079 / 64) ≈ 0.709
        let alpha = 0.709;
        let mut sum = 0.0;
        let mut zeros = 0usize;

        for &reg in &self.registers {
            sum += 2.0_f64.powi(-(reg as i32));
            if reg == 0 {
                zeros += 1;
            }
        }

        let mut estimate = alpha * m * m / sum;

        // Small range correction: LinearCounting
        if estimate <= 2.5 * m && zeros > 0 {
            estimate = m * (m / zeros as f64).ln();
        }

        estimate.round().max(0.0) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hll_empty() {
        let hll = HllSketch::new();
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn test_hll_single_value_repeated() {
        let mut hll = HllSketch::new();
        for _ in 0..100 {
            hll.add("test_key");
        }
        assert_eq!(hll.count(), 1);
    }

    #[test]
    fn test_hll_100_distinct() {
        let mut hll = HllSketch::new();
        for i in 0..100 {
            hll.add(&format!("key_{}", i));
        }
        let est = hll.count();
        // Check estimate within 25% of 100
        assert!((75..=125).contains(&est), "Estimate {} not in [75, 125]", est);
    }

    #[test]
    fn test_hll_1000_distinct() {
        let mut hll = HllSketch::new();
        for i in 0..1000 {
            hll.add(&format!("user_item_id_{}", i));
        }
        let est = hll.count();
        // Check estimate within 25% of 1000
        assert!((750..=1250).contains(&est), "Estimate {} not in [750, 1250]", est);
    }

    #[test]
    fn test_hll_merge() {
        let mut hll1 = HllSketch::new();
        let mut hll2 = HllSketch::new();
        for i in 0..500 {
            hll1.add(&format!("key_{}", i));
        }
        for i in 500..1000 {
            hll2.add(&format!("key_{}", i));
        }
        hll1.merge(&hll2);
        let est = hll1.count();
        assert!((750..=1250).contains(&est), "Estimate {} not in [750, 1250]", est);
    }
}
