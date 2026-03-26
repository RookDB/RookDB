//! Linear hash index with incremental, pointer-driven bucket splitting.
//!
//! Linear hashing avoids the sudden directory-doubling of extendible hashing
//! by splitting buckets one at a time in order, controlled by a *split pointer*
//! (`sp`).  The directory grows by exactly one bucket per split.
//!
//! # Hash functions
//!
//! At hash level `l` the primary hash function is:
//! ```text
//! h_l(k) = hash(k) mod (N₀ · 2^l)
//! ```
//! where `N₀` is the initial bucket count.
//!
//! If `h_l(k) < sp` (meaning that bucket has already been split this round),
//! the level-up function is applied instead:
//! ```text
//! h_{l+1}(k) = hash(k) mod (N₀ · 2^{l+1})
//! ```
//!
//! A split is triggered whenever the global load factor exceeds
//! `LINEAR_HASH_LOAD_FACTOR_THRESHOLD`.  When `sp` reaches `N₀ · 2^l` the
//! level is incremented and `sp` is reset to 0.
//!
//! # Diagram (N₀=4, l=0, sp pointing at bucket 1)
//! ```text
//!  Bucket 0 (already split)
//!  Bucket 1 ◄── split pointer
//!  Bucket 2
//!  Bucket 3
//!  Bucket 4 (new bucket from split of bucket 0)
//! ```

use std::fs;
use std::io;

use serde::{Deserialize, Serialize};

use crate::index::config::{
    LINEAR_HASH_INITIAL_BUCKETS, LINEAR_HASH_LOAD_FACTOR_THRESHOLD, STATIC_HASH_BUCKET_CAPACITY,
};
use crate::index::index_trait::{HashBasedIndex, IndexKey, IndexTrait, RecordId};

// ─── Internal types ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LHEntry {
    key: IndexKey,
    records: Vec<RecordId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OverflowSegment {
    entries: Vec<LHEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LHBucket {
    entries: Vec<LHEntry>,
    overflow: Vec<OverflowSegment>,
}

impl LHBucket {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            overflow: Vec::new(),
        }
    }

    fn find_mut(&mut self, key: &IndexKey) -> Option<&mut LHEntry> {
        if let Some(e) = self.entries.iter_mut().find(|e| &e.key == key) {
            return Some(e);
        }
        for seg in &mut self.overflow {
            if let Some(e) = seg.entries.iter_mut().find(|e| &e.key == key) {
                return Some(e);
            }
        }
        None
    }

    fn find(&self, key: &IndexKey) -> Option<&LHEntry> {
        if let Some(e) = self.entries.iter().find(|e| &e.key == key) {
            return Some(e);
        }
        for seg in &self.overflow {
            if let Some(e) = seg.entries.iter().find(|e| &e.key == key) {
                return Some(e);
            }
        }
        None
    }

    fn insert_entry(&mut self, key: IndexKey, record_id: RecordId) {
        if let Some(e) = self.find_mut(&key) {
            if !e.records.contains(&record_id) {
                e.records.push(record_id);
            }
            return;
        }
        let new_entry = LHEntry {
            key,
            records: vec![record_id],
        };
        if self.entries.len() < STATIC_HASH_BUCKET_CAPACITY {
            self.entries.push(new_entry);
            return;
        }
        if let Some(seg) = self.overflow.last_mut() {
            if seg.entries.len() < STATIC_HASH_BUCKET_CAPACITY {
                seg.entries.push(new_entry);
                return;
            }
        }
        self.overflow.push(OverflowSegment {
            entries: vec![new_entry],
        });
    }

    fn drain_all(&mut self) -> Vec<LHEntry> {
        let mut all: Vec<LHEntry> = std::mem::take(&mut self.entries);
        for seg in self.overflow.drain(..) {
            all.extend(seg.entries);
        }
        all
    }

    fn total_records(&self) -> usize {
        let p: usize = self.entries.iter().map(|e| e.records.len()).sum();
        let o: usize = self
            .overflow
            .iter()
            .flat_map(|s| s.entries.iter())
            .map(|e| e.records.len())
            .sum();
        p + o
    }
}

// ─── Public index type ────────────────────────────────────────────────────────

/// Linear hash index with incremental splitting.
///
/// Grows one bucket at a time via a deterministic split pointer, providing
/// uniform distribution without the sudden large restructurings of extendible
/// hashing.
#[derive(Debug, Serialize, Deserialize)]
pub struct LinearHashIndex {
    /// Current hash level.
    level: u32,
    /// Index of the next bucket to be split this round.
    split_ptr: usize,
    /// Initial bucket count `N₀`.
    initial_buckets: usize,
    /// All buckets: primary plus all buckets created by splits.
    buckets: Vec<LHBucket>,
    /// Load-factor threshold that triggers a split.
    load_factor_threshold: f64,
}

impl LinearHashIndex {
    /// Create a new index.
    pub fn new(initial_buckets: usize, load_factor_threshold: f64) -> Self {
        assert!(initial_buckets > 0);
        assert!(load_factor_threshold > 0.0 && load_factor_threshold <= 1.0);
        let buckets = (0..initial_buckets).map(|_| LHBucket::new()).collect();
        Self {
            level: 0,
            split_ptr: 0,
            initial_buckets,
            buckets,
            load_factor_threshold,
        }
    }

    /// Create with the defaults from [`config`](crate::index::config).
    pub fn with_defaults() -> Self {
        Self::new(LINEAR_HASH_INITIAL_BUCKETS, LINEAR_HASH_LOAD_FACTOR_THRESHOLD)
    }

    /// Load a persisted index from the JSON file at `path`.
    pub fn load(path: &str) -> io::Result<Self> {
        let data = fs::read_to_string(path)?;
        serde_json::from_str(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Apply hash function at level `l`.
    #[inline]
    fn h_level(&self, key: &IndexKey, level: u32) -> usize {
        let modulus = self.initial_buckets * (1 << level);
        (key.hash_code() as usize) % modulus
    }

    /// Return the bucket index for a lookup / insert.
    #[inline]
    fn bucket_for(&self, key: &IndexKey) -> usize {
        let h = self.h_level(key, self.level);
        if h < self.split_ptr {
            self.h_level(key, self.level + 1)
        } else {
            h
        }
    }

    fn total_entries(&self) -> usize {
        self.buckets.iter().map(|b| b.total_records()).sum()
    }

    /// Perform one split step at `split_ptr`.
    fn split(&mut self) {
        let sp = self.split_ptr;

        // Create new bucket at the end.
        self.buckets.push(LHBucket::new());

        // Drain all entries from the split bucket and redistribute.
        let old_entries = self.buckets[sp].drain_all();
        let new_level = self.level + 1;
        let new_modulus = self.initial_buckets * (1 << new_level);

        for entry in old_entries {
            let target = (entry.key.hash_code() as usize) % new_modulus;
            for rid in &entry.records {
                self.buckets[target].insert_entry(entry.key.clone(), rid.clone());
            }
        }

        // Advance split pointer; wrap to next level when round completes.
        self.split_ptr += 1;
        let round_size = self.initial_buckets * (1 << self.level);
        if self.split_ptr >= round_size {
            self.level += 1;
            self.split_ptr = 0;
        }
    }

    /// Trigger a split if load factor exceeds threshold.
    fn maybe_split(&mut self) {
        if self.load_factor() > self.load_factor_threshold {
            self.split();
        }
    }
}

impl IndexTrait for LinearHashIndex {
    fn insert(&mut self, key: IndexKey, record_id: RecordId) -> io::Result<()> {
        let idx = self.bucket_for(&key);
        self.buckets[idx].insert_entry(key, record_id);
        self.maybe_split();
        Ok(())
    }

    fn search(&self, key: &IndexKey) -> io::Result<Vec<RecordId>> {
        let idx = self.bucket_for(key);
        Ok(self.buckets[idx]
            .find(key)
            .map(|e| e.records.clone())
            .unwrap_or_default())
    }

    fn delete(&mut self, key: &IndexKey, record_id: &RecordId) -> io::Result<bool> {
        let idx = self.bucket_for(key);
        if let Some(entry) = self.buckets[idx].find_mut(key) {
            let before = entry.records.len();
            entry.records.retain(|r| r != record_id);
            return Ok(entry.records.len() < before);
        }
        Ok(false)
    }

    fn save(&self, path: &str) -> io::Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, json)
    }

    fn entry_count(&self) -> usize {
        self.total_entries()
    }

    fn index_type_name(&self) -> &'static str {
        "linear_hash"
    }

    fn all_entries(&self) -> io::Result<Vec<(IndexKey, RecordId)>> {
        let mut out = Vec::new();
        for bucket in &self.buckets {
            for entry in &bucket.entries {
                for rid in &entry.records {
                    out.push((entry.key.clone(), rid.clone()));
                }
            }
            for seg in &bucket.overflow {
                for entry in &seg.entries {
                    for rid in &entry.records {
                        out.push((entry.key.clone(), rid.clone()));
                    }
                }
            }
        }
        Ok(out)
    }

    fn validate_structure(&self) -> io::Result<()> {
        if self.initial_buckets == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "linear_hash: initial_buckets must be > 0",
            ));
        }
        if self.buckets.len() < self.initial_buckets {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "linear_hash: bucket array smaller than initial_buckets",
            ));
        }

        let round_size = self.initial_buckets.checked_shl(self.level).unwrap_or(0);
        if round_size == 0 || self.split_ptr >= round_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "linear_hash: split pointer is out of range for current level",
            ));
        }

        for bucket in &self.buckets {
            if bucket.entries.len() > STATIC_HASH_BUCKET_CAPACITY {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "linear_hash: primary bucket exceeds configured capacity",
                ));
            }
            for entry in &bucket.entries {
                if entry.records.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "linear_hash: found entry with empty record list",
                    ));
                }
            }
            for seg in &bucket.overflow {
                if seg.entries.is_empty() || seg.entries.len() > STATIC_HASH_BUCKET_CAPACITY {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "linear_hash: invalid overflow segment size",
                    ));
                }
                for entry in &seg.entries {
                    if entry.records.is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "linear_hash: found overflow entry with empty record list",
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

impl HashBasedIndex for LinearHashIndex {
    fn load_factor(&self) -> f64 {
        let slots = self.buckets.len() * STATIC_HASH_BUCKET_CAPACITY;
        if slots == 0 {
            return 0.0;
        }
        self.total_entries() as f64 / slots as f64
    }

    fn bucket_count(&self) -> usize {
        self.buckets.len()
    }
}
