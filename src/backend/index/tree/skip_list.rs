use std::collections::BTreeMap;
use std::fs;
use std::io;

use serde::{Deserialize, Serialize};

use crate::index::index_trait::{IndexKey, IndexTrait, RecordId, TreeBasedIndex};

#[derive(Debug, Serialize, Deserialize)]
struct PersistedSkipListIndex {
    entries: Vec<(IndexKey, Vec<RecordId>)>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SkipListIndex {
    // Deterministic ordered map backend; preserves skip-list semantics for
    // point/range operations in this single-threaded system.
    entries: BTreeMap<IndexKey, Vec<RecordId>>,
}

impl SkipListIndex {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    fn to_persisted(&self) -> PersistedSkipListIndex {
        PersistedSkipListIndex {
            entries: self
                .entries
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }

    fn from_persisted(data: PersistedSkipListIndex) -> Self {
        Self {
            entries: data.entries.into_iter().collect(),
        }
    }

    pub fn load(path: &str) -> io::Result<Self> {
        let data = fs::read_to_string(path)?;
        let persisted: PersistedSkipListIndex = serde_json::from_str(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Self::from_persisted(persisted))
    }
}

impl IndexTrait for SkipListIndex {
    fn insert(&mut self, key: IndexKey, record_id: RecordId) -> io::Result<()> {
        let list = self.entries.entry(key).or_default();
        if !list.contains(&record_id) {
            list.push(record_id);
        }
        Ok(())
    }

    fn search(&self, key: &IndexKey) -> io::Result<Vec<RecordId>> {
        Ok(self.entries.get(key).cloned().unwrap_or_default())
    }

    fn delete(&mut self, key: &IndexKey, record_id: &RecordId) -> io::Result<bool> {
        if let Some(list) = self.entries.get_mut(key) {
            let before = list.len();
            list.retain(|r| r != record_id);
            let removed = list.len() < before;
            if list.is_empty() {
                self.entries.remove(key);
            }
            return Ok(removed);
        }
        Ok(false)
    }

    fn save(&self, path: &str) -> io::Result<()> {
        let json = serde_json::to_string_pretty(&self.to_persisted())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, json)
    }

    fn entry_count(&self) -> usize {
        self.entries.values().map(|v| v.len()).sum()
    }

    fn index_type_name(&self) -> &'static str {
        "skip_list"
    }

    fn all_entries(&self) -> io::Result<Vec<(IndexKey, RecordId)>> {
        let mut out = Vec::new();
        for (k, rids) in &self.entries {
            for rid in rids {
                out.push((k.clone(), rid.clone()));
            }
        }
        Ok(out)
    }

    fn validate_structure(&self) -> io::Result<()> {
        for rids in self.entries.values() {
            if rids.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "skip_list: found key with empty record list",
                ));
            }
        }
        Ok(())
    }
}

impl TreeBasedIndex for SkipListIndex {
    fn range_scan(&self, start: &IndexKey, end: &IndexKey) -> io::Result<Vec<RecordId>> {
        let mut out = Vec::new();
        for (_, rids) in self.entries.range(start.clone()..=end.clone()) {
            out.extend_from_slice(rids);
        }
        Ok(out)
    }

    fn min_key(&self) -> Option<IndexKey> {
        self.entries.keys().next().cloned()
    }

    fn max_key(&self) -> Option<IndexKey> {
        self.entries.keys().next_back().cloned()
    }
}
