//! Bloom filter for join optimization.
//!
//! Used during the hash-join build phase to quickly reject probe tuples
//! that cannot match. Uses deterministic FNV-1a hashing.

/// Probabilistic set membership filter.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    bit_capacity: usize,
    num_hash_functions: usize,
    element_count: usize,
}

impl BloomFilter {
    /// Create a filter with the given capacity in bits (k = 7 hash functions).
    pub fn new(bit_capacity: usize) -> Self {
        Self::with_hash_functions(bit_capacity, 7)
    }

    /// Create a filter with a custom number of hash functions.
    pub fn with_hash_functions(bit_capacity: usize, num_hash_functions: usize) -> Self {
        let byte_capacity = (bit_capacity + 7) / 8;
        BloomFilter {
            bits: vec![0u8; byte_capacity],
            bit_capacity,
            num_hash_functions: num_hash_functions.clamp(1, 20),
            element_count: 0,
        }
    }

    /// Insert an element (as raw bytes) into the filter.
    pub fn insert(&mut self, element: &[u8]) {
        for i in 0..self.num_hash_functions {
            let hash = Self::fnv1a_hash(element, i as u64);
            let bit_pos = hash % (self.bit_capacity as u64);
            let byte_pos = (bit_pos / 8) as usize;
            let bit_offset = (bit_pos % 8) as u8;
            if byte_pos < self.bits.len() {
                self.bits[byte_pos] |= 1 << bit_offset;
            }
        }
        self.element_count += 1;
    }

    /// Test membership. `false` = definitely absent, `true` = possibly present.
    pub fn might_contain(&self, element: &[u8]) -> bool {
        for i in 0..self.num_hash_functions {
            let hash = Self::fnv1a_hash(element, i as u64);
            let bit_pos = hash % (self.bit_capacity as u64);
            let byte_pos = (bit_pos / 8) as usize;
            let bit_offset = (bit_pos % 8) as u8;
            if byte_pos >= self.bits.len() || (self.bits[byte_pos] & (1 << bit_offset)) == 0 {
                return false;
            }
        }
        true
    }

    /// Theoretical false-positive rate: (1 − e^(−kn/m))^k.
    pub fn false_positive_rate(&self) -> f64 {
        if self.element_count == 0 { return 0.0; }
        let k = self.num_hash_functions as f64;
        let n = self.element_count as f64;
        let m = self.bit_capacity as f64;
        (1.0 - (-k * n / m).exp()).powf(k)
    }

    /// Returns `true` when the filter is too full for reliable operation.
    pub fn is_saturated(&self) -> bool {
        if self.bit_capacity == 0 { return false; }
        let k = self.num_hash_functions as f64;
        (self.element_count as f64) > (self.bit_capacity as f64) * (0.693 / (k * 2.0))
    }

    /// Saturation level (0.0 = empty, 1.0 = saturated).
    pub fn saturation_percent(&self) -> f64 {
        if self.bit_capacity == 0 { return 0.0; }
        let k = self.num_hash_functions as f64;
        let point = (self.bit_capacity as f64) * (0.693 / (k * 2.0));
        ((self.element_count as f64) / point).min(1.0)
    }

    /// Reset the filter to empty.
    pub fn clear(&mut self) {
        self.bits.iter_mut().for_each(|b| *b = 0);
        self.element_count = 0;
    }

    /// Snapshot of current filter statistics.
    pub fn stats(&self) -> BloomFilterStats {
        BloomFilterStats {
            capacity_bits: self.bit_capacity,
            elements_inserted: self.element_count,
            hash_functions: self.num_hash_functions,
            false_positive_rate: self.false_positive_rate(),
            is_saturated: self.is_saturated(),
            saturation_percent: self.saturation_percent(),
        }
    }

    /// FNV-1a hash seeded by `seed` for multi-hash-function support.
    pub fn fnv1a_hash(data: &[u8], seed: u64) -> u64 {
        const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;
        let mut hash = FNV_OFFSET_BASIS ^ seed;
        for &byte in data {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }
}

#[derive(Debug, Clone)]
pub struct BloomFilterStats {
    pub capacity_bits: usize,
    pub elements_inserted: usize,
    pub hash_functions: usize,
    pub false_positive_rate: f64,
    pub is_saturated: bool,
    pub saturation_percent: f64,
}
