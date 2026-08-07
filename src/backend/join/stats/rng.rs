//! A small deterministic pseudo-random generator.
//!
//! Statistics sample rows, and a sample driven by an unseeded generator makes
//! every plan - and therefore every EXPLAIN - different between runs. This is
//! seeded from a constant, so `ANALYZE` over the same data always produces the
//! same histogram and the same estimates. That reproducibility is itself
//! tested.
//!
//! SplitMix64 is used because it is a dozen lines, needs no dependency, and
//! passes the statistical quality bar for reservoir sampling comfortably.

/// SplitMix64.
#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..bound`; zero when `bound` is zero.
    pub fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            0
        } else {
            self.next_u64() % bound
        }
    }
}

/// Final mixing step of SplitMix64, used as a standalone 64-bit hash
/// finaliser. Applied to an FNV-1a digest it gives the avalanche behaviour
/// HyperLogLog's register selection needs.
pub fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// FNV-1a over bytes, finalised with [`mix64`].
pub fn hash_bytes(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    mix64(hash)
}
