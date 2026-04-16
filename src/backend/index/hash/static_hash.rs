use std::io;

use serde::{Deserialize, Serialize};

use crate::index::config::{STATIC_HASH_BUCKET_CAPACITY, STATIC_HASH_NUM_BUCKETS};
use crate::index::index_trait::{HashBasedIndex, IndexKey, IndexTrait, RecordId};
use crate::index::paged_store;

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
        let mut index = Self::with_defaults();
        paged_store::load_entries_stream(path, |key, rid| index.insert(key, rid))?;
        Ok(index)
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
        paged_store::save_entries(path, self.all_entries()?.into_iter())
    }

    fn entry_count(&self) -> usize {
        self.buckets.iter().map(|b| b.total_records()).sum()
    }

    fn index_type_name(&self) -> &'static str {
        "static_hash"
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
        if self.num_buckets == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "static_hash: num_buckets must be > 0",
            ));
        }
        if self.num_buckets != self.buckets.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "static_hash: num_buckets does not match buckets length",
            ));
        }

        for bucket in &self.buckets {
            if bucket.entries.len() > STATIC_HASH_BUCKET_CAPACITY {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "static_hash: primary bucket exceeds configured capacity",
                ));
            }
            for entry in &bucket.entries {
                if entry.records.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "static_hash: found entry with empty record list",
                    ));
                }
            }
            for seg in &bucket.overflow {
                if seg.entries.is_empty() || seg.entries.len() > STATIC_HASH_BUCKET_CAPACITY {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "static_hash: invalid overflow segment size",
                    ));
                }
                for entry in &seg.entries {
                    if entry.records.is_empty() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "static_hash: found overflow entry with empty record list",
                        ));
                    }
                }
            }
        }

        Ok(())
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
