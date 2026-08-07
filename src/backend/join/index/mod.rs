//! Indexes usable by a join.
//!
//! The contract that matters: a probe returns *candidates*, never the
//! authority. Callers re-verify, which is what lets an index with a coarser key
//! model than the join's be adopted safely. See
//! `docs/join/pr48-index-adapter.md`.

pub mod sorted_array;

use std::path::{Path, PathBuf};
use std::rc::Rc;

use super::error::JoinError;
use super::key::{JoinKey, KeyClass, KeyColumn, KeySpec};
use super::source::TableRef;
use super::stats::ValidityStamp;

pub use sorted_array::SortedKeyIndex;

/// Where a row lives in its heap file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowLocator {
    pub page_id: u32,
    pub slot_id: u32,
}

/// Which columns an index covers, and in which key classes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexKeySpec {
    /// Column indices in the indexed relation, in index order.
    pub columns: Vec<usize>,
    pub classes: Vec<KeyClass>,
}

impl IndexKeySpec {
    pub fn new(columns: Vec<usize>, classes: Vec<KeyClass>) -> Self {
        Self { columns, classes }
    }
}

/// An index a join can probe.
pub trait JoinIndex {
    /// Rows that *may* carry `key`.
    fn probe(&self, key: &JoinKey) -> Result<Vec<RowLocator>, JoinError>;

    fn key_spec(&self) -> &IndexKeySpec;

    /// Entries held. Reported by EXPLAIN and used to size the probe cost.
    fn entry_count(&self) -> u64;
}

/// Restrict a join's key specification to the columns an index covers.
pub fn probe_spec(index: &dyn JoinIndex, keys: &KeySpec) -> Option<KeySpec> {
    let spec = index.key_spec();
    if spec.columns.is_empty() || spec.columns.len() != spec.classes.len() {
        return None;
    }

    let mut columns = Vec::with_capacity(spec.columns.len());
    for (position, indexed_column) in spec.columns.iter().enumerate() {
        let matched = keys
            .columns
            .iter()
            .find(|column| column.right_index == *indexed_column)?;
        if matched.class != spec.classes[position] {
            return None;
        }
        columns.push(KeyColumn {
            left_index: matched.left_index,
            right_index: matched.right_index,
            class: matched.class,
        });
    }

    Some(KeySpec::new(columns))
}

/// Path of the index sidecar for a relation and a set of columns.
pub fn index_path(table_path: &Path, columns: &[usize]) -> PathBuf {
    let stem = table_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("table");
    let suffix: Vec<String> = columns.iter().map(usize::to_string).collect();
    let name = format!("{stem}.{}.jidx", suffix.join("-"));
    match table_path.parent() {
        Some(parent) => parent.join(name),
        None => PathBuf::from(name),
    }
}

/// Find an index on the inner relation that can serve these join keys.
pub fn find_usable(table: &TableRef, keys: &KeySpec) -> Option<(Rc<dyn JoinIndex>, KeySpec)> {
    if keys.is_empty() {
        return None;
    }

    let stamp = ValidityStamp::read(&table.path).ok()?;

    // Try the full key first, then successively shorter prefixes - a narrower
    // index still produces usable candidates.
    for width in (1..=keys.columns.len()).rev() {
        let columns: Vec<usize> = keys.columns[..width]
            .iter()
            .map(|column| column.right_index)
            .collect();
        let path = index_path(&table.path, &columns);
        if !path.exists() {
            continue;
        }

        let index = match SortedKeyIndex::load(&path, stamp) {
            Ok(index) => index,
            Err(e) => {
                log::debug!("[join] not using index {}: {e}", path.display());
                continue;
            }
        };

        let index: Rc<dyn JoinIndex> = Rc::new(index);
        if let Some(spec) = probe_spec(index.as_ref(), keys) {
            return Some((index, spec));
        }
    }

    None
}
