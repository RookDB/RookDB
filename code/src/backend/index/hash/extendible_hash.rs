//! Extendible hash index with directory doubling and per-bucket splitting.
//!
//! Each bucket carries a *local depth* `ld`.  The directory has a *global depth*
//! `d`.  Multiple directory slots may point to the same physical bucket
//! (a "shared page" in the classic sense).
//!
//! When a bucket overflows:
//! 1. If `ld < d`: split the bucket, redirect high-bit directory slots to the
//!    new sibling.
//! 2. If `ld == d`: double the directory first, then split.
//!
//! # Complexity (amortised)
//! - Point Lookup : O(1)
//! - Insert        : O(1) amortised (O(n) on rare directory doublings)
//! - Space         : O(n)
//!
//! # Diagram
//! ```text
//!  Global depth d = 2
//!  Directory (4 slots)
//!  ┌────┬────────────────────────────┐
//!  │ 00 │──► Bucket A  (ld=2)        │
//!  │ 01 │──► Bucket B  (ld=2)        │
//!  │ 10 │──► Bucket C  (ld=1) ◄──┐  │
//!  │ 11 │───────────────────────────┘ │
//!  └────┘                             │
//!        C is shared by slots 10 & 11 │
//! ```

use std::collections::HashSet;
use std::fs;
use std::io;

use serde::{Deserialize, Serialize};

use crate::index::config::EXTENDIBLE_HASH_BUCKET_CAPACITY;
use crate::index::index_trait::{HashBasedIndex, IndexKey, IndexTrait, RecordId};

// ─── Internal types ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EHEntry {
    key: IndexKey,
    records: Vec<RecordId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EHBucket {
    local_depth: u32,
    entries: Vec<EHEntry>,
}

impl EHBucket {
    fn new(local_depth: u32) -> Self {
        Self {
            local_depth,
            entries: Vec::new(),
        }
    }

    fn is_full(&self) -> bool {
        self.entries.len() >= EXTENDIBLE_HASH_BUCKET_CAPACITY
    }

    fn find(&self, key: &IndexKey) -> Option<&EHEntry> {
        self.entries.iter().find(|e| &e.key == key)
    }

    fn find_mut(&mut self, key: &IndexKey) -> Option<&mut EHEntry> {
        self.entries.iter_mut().find(|e| &e.key == key)
    }
}

// ─── Public index type ────────────────────────────────────────────────────────

/// Extendible hash index.
///
/// The directory is stored as a flat `Vec<usize>` where each element indexes
/// into the `buckets` pool. Multiple directory slots may share the same
/// physical bucket when their keys' low-order bits are identical.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExtendibleHashIndex {
    global_depth: u32,
    /// `directory[i]` is an index into `buckets`.
    directory: Vec<usize>,
    buckets: Vec<EHBucket>,
}

impl ExtendibleHashIndex {
    /// Create a new index starting with `2^initial_depth` buckets.
    pub fn new(initial_depth: u32) -> Self {
        let num = 1usize << initial_depth;
        let buckets: Vec<EHBucket> = (0..num).map(|_| EHBucket::new(initial_depth)).collect();
        let directory: Vec<usize> = (0..num).collect();
        Self {
            global_depth: initial_depth,
            directory,
            buckets,
        }
    }

    /// Create with `global_depth = 1` (2 initial buckets).
    pub fn with_defaults() -> Self {
        Self::new(1)
    }

    /// Load a persisted index from the JSON file at `path`.
    pub fn load(path: &str) -> io::Result<Self> {
        let data = fs::read_to_string(path)?;
        serde_json::from_str(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Map a key to a directory index using the lower `global_depth` bits.
    #[inline]
    fn dir_index(&self, key: &IndexKey) -> usize {
        (key.hash_code() as usize) & ((1 << self.global_depth) - 1)
    }

    /// Double the directory: each existing slot is duplicated in the top half.
    fn double_directory(&mut self) {
        self.global_depth += 1;
        let extension = self.directory.clone();
        self.directory.extend(extension);
    }

    /// Split the bucket currently mapped to `dir_idx`.
    ///
    /// If the bucket's local depth equals the global depth the directory is
    /// doubled first.  Existing entries are redistributed between the original
    /// bucket and its newly created sibling.
    fn split_bucket(&mut self, dir_idx: usize) {
        let bucket_idx = self.directory[dir_idx];
        let old_ld = self.buckets[bucket_idx].local_depth;

        // If local depth == global depth, double the directory first.
        if old_ld == self.global_depth {
            self.double_directory();
        }

        let new_ld = old_ld + 1;
        let new_bucket_idx = self.buckets.len();
        self.buckets.push(EHBucket::new(new_ld));
        self.buckets[bucket_idx].local_depth = new_ld;

        // Re-point directory slots that previously pointed at the old bucket
        // and whose new high bit (bit `old_ld`) is set.
        let low_mask = (1usize << old_ld) - 1;
        let old_prefix = dir_idx & low_mask; // prefix before the new depth bit
        let new_prefix = old_prefix | (1 << old_ld); // prefix with new high bit set

        for slot in 0..self.directory.len() {
            if self.directory[slot] == bucket_idx && (slot & ((1 << new_ld) - 1)) == new_prefix {
                self.directory[slot] = new_bucket_idx;
            }
        }

        // Redistribute existing entries.
        let old_entries: Vec<EHEntry> = std::mem::take(&mut self.buckets[bucket_idx].entries);
        for entry in old_entries {
            let target_dir = entry.key.hash_code() as usize & ((1 << self.global_depth) - 1);
            let target_bucket = self.directory[target_dir];
            self.buckets[target_bucket].entries.push(entry);
        }
    }
}

impl IndexTrait for ExtendibleHashIndex {
    fn insert(&mut self, key: IndexKey, record_id: RecordId) -> io::Result<()> {
        loop {
            let dir_idx = self.dir_index(&key);
            let bucket_idx = self.directory[dir_idx];

            // Update existing key.
            if let Some(entry) = self.buckets[bucket_idx].find_mut(&key) {
                if !entry.records.contains(&record_id) {
                    entry.records.push(record_id);
                }
                return Ok(());
            }

            // Room in bucket?
            if !self.buckets[bucket_idx].is_full() {
                self.buckets[bucket_idx].entries.push(EHEntry {
                    key,
                    records: vec![record_id],
                });
                return Ok(());
            }

            // Bucket full — split and retry.
            self.split_bucket(dir_idx);
        }
    }

    fn search(&self, key: &IndexKey) -> io::Result<Vec<RecordId>> {
        let dir_idx = self.dir_index(key);
        let bucket_idx = self.directory[dir_idx];
        Ok(self.buckets[bucket_idx]
            .find(key)
            .map(|e| e.records.clone())
            .unwrap_or_default())
    }

    fn delete(&mut self, key: &IndexKey, record_id: &RecordId) -> io::Result<bool> {
        let dir_idx = self.dir_index(key);
        let bucket_idx = self.directory[dir_idx];
        if let Some(entry) = self.buckets[bucket_idx].find_mut(key) {
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
        let unique: HashSet<usize> = self.directory.iter().copied().collect();
        unique
            .iter()
            .map(|&bi| self.buckets[bi].entries.iter().map(|e| e.records.len()).sum::<usize>())
            .sum()
    }

    fn index_type_name(&self) -> &'static str {
        "extendible_hash"
    }
}

impl HashBasedIndex for ExtendibleHashIndex {
    fn load_factor(&self) -> f64 {
        let unique: HashSet<usize> = self.directory.iter().copied().collect();
        let total_slots = unique.len() * EXTENDIBLE_HASH_BUCKET_CAPACITY;
        if total_slots == 0 {
            return 0.0;
        }
        self.entry_count() as f64 / total_slots as f64
    }

    fn bucket_count(&self) -> usize {
        let unique: HashSet<usize> = self.directory.iter().copied().collect();
        unique.len()
    }
}
