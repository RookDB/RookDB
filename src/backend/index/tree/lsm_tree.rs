use std::collections::BTreeMap;
use std::fs;
use std::io;

use serde::{Deserialize, Serialize};

use crate::index::index_trait::{IndexKey, IndexTrait, RecordId, TreeBasedIndex};

const DEFAULT_MEMTABLE_LIMIT: usize = 1024;

#[derive(Debug, Serialize, Deserialize)]
struct LsmRun {
    entries: BTreeMap<IndexKey, Vec<RecordId>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LsmTreeIndex {
    memtable: BTreeMap<IndexKey, Vec<RecordId>>,
    runs: Vec<LsmRun>,
    memtable_limit: usize,
}

impl LsmTreeIndex {
    pub fn new(memtable_limit: usize) -> Self {
        Self {
            memtable: BTreeMap::new(),
            runs: Vec::new(),
            memtable_limit,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_MEMTABLE_LIMIT)
    }

    pub fn load(path: &str) -> io::Result<Self> {
        let data = fs::read_to_string(path)?;
        serde_json::from_str(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn maybe_flush(&mut self) {
        if self.memtable.len() < self.memtable_limit {
            return;
        }

        let flushed = std::mem::take(&mut self.memtable);
        self.runs.insert(0, LsmRun { entries: flushed });

        // Keep this implementation simple and bounded: compact when too many runs.
        if self.runs.len() > 8 {
            self.compact_all();
        }
    }

    fn compact_all(&mut self) {
        if self.runs.is_empty() {
            return;
        }

        let mut merged: BTreeMap<IndexKey, Vec<RecordId>> = BTreeMap::new();
        // Newer runs first: preserve latest logical content for duplicates.
        for run in &self.runs {
            for (k, v) in &run.entries {
                merged.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
        self.runs = vec![LsmRun { entries: merged }];
    }

    fn merged_view(&self) -> BTreeMap<IndexKey, Vec<RecordId>> {
        let mut out = BTreeMap::new();

        for (k, v) in &self.memtable {
            out.insert(k.clone(), v.clone());
        }
        for run in &self.runs {
            for (k, v) in &run.entries {
                out.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }

        out
    }
}

impl IndexTrait for LsmTreeIndex {
    fn insert(&mut self, key: IndexKey, record_id: RecordId) -> io::Result<()> {
        let list = self.memtable.entry(key).or_default();
        if !list.contains(&record_id) {
            list.push(record_id);
        }
        self.maybe_flush();
        Ok(())
    }

    fn search(&self, key: &IndexKey) -> io::Result<Vec<RecordId>> {
        if let Some(v) = self.memtable.get(key) {
            return Ok(v.clone());
        }
        for run in &self.runs {
            if let Some(v) = run.entries.get(key) {
                return Ok(v.clone());
            }
        }
        Ok(Vec::new())
    }

    fn delete(&mut self, key: &IndexKey, record_id: &RecordId) -> io::Result<bool> {
        if let Some(v) = self.memtable.get_mut(key) {
            let before = v.len();
            v.retain(|r| r != record_id);
            let removed = v.len() < before;
            if v.is_empty() {
                self.memtable.remove(key);
            }
            return Ok(removed);
        }

        // Bring key to memtable as latest version and remove RID there.
        let current = self.search(key)?;
        if current.is_empty() {
            return Ok(false);
        }

        let mut latest = current;
        let before = latest.len();
        latest.retain(|r| r != record_id);
        if latest.is_empty() {
            self.memtable.remove(key);
        } else {
            self.memtable.insert(key.clone(), latest);
        }
        Ok(before != self.search(key)?.len())
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
        self.merged_view().values().map(|v| v.len()).sum()
    }

    fn index_type_name(&self) -> &'static str {
        "lsm_tree"
    }
}

impl TreeBasedIndex for LsmTreeIndex {
    fn range_scan(&self, start: &IndexKey, end: &IndexKey) -> io::Result<Vec<RecordId>> {
        let merged = self.merged_view();
        let mut out = Vec::new();
        for (_, v) in merged.range(start.clone()..=end.clone()) {
            out.extend_from_slice(v);
        }
        Ok(out)
    }

    fn min_key(&self) -> Option<IndexKey> {
        self.merged_view().keys().next().cloned()
    }

    fn max_key(&self) -> Option<IndexKey> {
        self.merged_view().keys().next_back().cloned()
    }
}
