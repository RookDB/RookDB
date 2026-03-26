use std::fs;
use std::io;

use serde::{Deserialize, Serialize};

use crate::index::config::STATIC_HASH_NUM_BUCKETS;
use crate::index::index_trait::{HashBasedIndex, IndexKey, IndexTrait, RecordId};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChainEntry {
    key: IndexKey,
    records: Vec<RecordId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChainedHashIndex {
    bucket_count: usize,
    buckets: Vec<Vec<ChainEntry>>,
}

impl ChainedHashIndex {
    pub fn new(bucket_count: usize) -> Self {
        assert!(bucket_count > 0, "bucket_count must be > 0");
        Self {
            bucket_count,
            buckets: vec![Vec::new(); bucket_count],
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(STATIC_HASH_NUM_BUCKETS)
    }

    pub fn load(path: &str) -> io::Result<Self> {
        let data = fs::read_to_string(path)?;
        serde_json::from_str(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    #[inline]
    fn bucket_idx(&self, key: &IndexKey) -> usize {
        (key.hash_code() as usize) % self.bucket_count
    }
}

impl IndexTrait for ChainedHashIndex {
    fn insert(&mut self, key: IndexKey, record_id: RecordId) -> io::Result<()> {
        let idx = self.bucket_idx(&key);
        if let Some(entry) = self.buckets[idx].iter_mut().find(|e| e.key == key) {
            if !entry.records.contains(&record_id) {
                entry.records.push(record_id);
            }
            return Ok(());
        }

        self.buckets[idx].push(ChainEntry {
            key,
            records: vec![record_id],
        });
        Ok(())
    }

    fn search(&self, key: &IndexKey) -> io::Result<Vec<RecordId>> {
        let idx = self.bucket_idx(key);
        Ok(self.buckets[idx]
            .iter()
            .find(|e| &e.key == key)
            .map(|e| e.records.clone())
            .unwrap_or_default())
    }

    fn delete(&mut self, key: &IndexKey, record_id: &RecordId) -> io::Result<bool> {
        let idx = self.bucket_idx(key);
        if let Some(entry) = self.buckets[idx].iter_mut().find(|e| &e.key == key) {
            let before = entry.records.len();
            entry.records.retain(|r| r != record_id);
            let removed = entry.records.len() < before;
            if entry.records.is_empty() {
                self.buckets[idx].retain(|e| &e.key != key);
            }
            return Ok(removed);
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
        self.buckets
            .iter()
            .flat_map(|b| b.iter())
            .map(|e| e.records.len())
            .sum()
    }

    fn index_type_name(&self) -> &'static str {
        "chained_hash"
    }

    fn all_entries(&self) -> io::Result<Vec<(IndexKey, RecordId)>> {
        let mut out = Vec::new();
        for bucket in &self.buckets {
            for entry in bucket {
                for rid in &entry.records {
                    out.push((entry.key.clone(), rid.clone()));
                }
            }
        }
        Ok(out)
    }

    fn validate_structure(&self) -> io::Result<()> {
        if self.bucket_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chained_hash: bucket_count must be > 0",
            ));
        }
        if self.bucket_count != self.buckets.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chained_hash: bucket_count does not match buckets length",
            ));
        }
        for bucket in &self.buckets {
            for entry in bucket {
                if entry.records.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "chained_hash: found entry with empty record list",
                    ));
                }
            }
        }
        Ok(())
    }
}

impl HashBasedIndex for ChainedHashIndex {
    fn load_factor(&self) -> f64 {
        if self.bucket_count == 0 {
            return 0.0;
        }
        self.entry_count() as f64 / self.bucket_count as f64
    }

    fn bucket_count(&self) -> usize {
        self.bucket_count
    }
}
