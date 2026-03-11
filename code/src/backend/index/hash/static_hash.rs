use std::fs;
use std::io;

use serde::{Deserialize, Serialize};

use crate::index::config::{STATIC_HASH_BUCKET_CAPACITY, STATIC_HASH_NUM_BUCKETS};
use crate::index::index_trait::{HashBasedIndex, IndexKey, IndexTrait, RecordId};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BucketEntry {
    key: IndexKey,
    records: Vec<RecordId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OverflowSegment {
    entries: Vec<BucketEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Bucket {
    entries: Vec<BucketEntry>,
    overflow: Vec<OverflowSegment>,
}

impl Bucket {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(STATIC_HASH_BUCKET_CAPACITY),
            overflow: Vec::new(),
        }
    }

    fn find_mut(&mut self, key: &IndexKey) -> Option<&mut BucketEntry> {
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

    fn find(&self, key: &IndexKey) -> Option<&BucketEntry> {
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

    fn insert(&mut self, key: IndexKey, record_id: RecordId) {
        if let Some(entry) = self.find_mut(&key) {
            if !entry.records.contains(&record_id) {
                entry.records.push(record_id);
            }
            return;
        }

        let new_entry = BucketEntry {
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

    fn total_records(&self) -> usize {
        let primary: usize = self.entries.iter().map(|e| e.records.len()).sum();
        let overflow: usize = self
            .overflow
            .iter()
            .flat_map(|seg| seg.entries.iter())
            .map(|e| e.records.len())
            .sum();
        primary + overflow
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StaticHashIndex {
    num_buckets: usize,
    buckets: Vec<Bucket>,
}

impl StaticHashIndex {
    pub fn new(num_buckets: usize) -> Self {
        assert!(num_buckets > 0, "num_buckets must be > 0");
        let buckets = (0..num_buckets).map(|_| Bucket::new()).collect();
        Self { num_buckets, buckets }
    }

    pub fn with_defaults() -> Self {
        Self::new(STATIC_HASH_NUM_BUCKETS)
    }

    pub fn load(path: &str) -> io::Result<Self> {
        let data = fs::read_to_string(path)?;
        serde_json::from_str(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    #[inline]
    fn bucket_index(&self, key: &IndexKey) -> usize {
        (key.hash_code() as usize) % self.num_buckets
    }
}

impl IndexTrait for StaticHashIndex {
    fn insert(&mut self, key: IndexKey, record_id: RecordId) -> io::Result<()> {
        let idx = self.bucket_index(&key);
        self.buckets[idx].insert(key, record_id);
        Ok(())
    }

    fn search(&self, key: &IndexKey) -> io::Result<Vec<RecordId>> {
        let idx = self.bucket_index(key);
        Ok(self.buckets[idx]
            .find(key)
            .map(|e| e.records.clone())
            .unwrap_or_default())
    }

    fn delete(&mut self, key: &IndexKey, record_id: &RecordId) -> io::Result<bool> {
        let idx = self.bucket_index(key);
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
        self.buckets.iter().map(|b| b.total_records()).sum()
    }

    fn index_type_name(&self) -> &'static str {
        "static_hash"
    }
}

impl HashBasedIndex for StaticHashIndex {
    fn load_factor(&self) -> f64 {
        let total_slots = self.num_buckets.checked_mul(STATIC_HASH_BUCKET_CAPACITY).unwrap_or(usize::MAX);
        if total_slots == 0 {
            return 0.0;
        }
        self.entry_count() as f64 / total_slots as f64
    }

    fn bucket_count(&self) -> usize {
        self.num_buckets
    }
}
