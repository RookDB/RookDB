//! A bulk-built, sorted-array index.
//!
//! `JoinKey` is already an order-preserving encoding, so a sorted array gives
//! O(log n) probes with no separate comparator - and it is built with the same
//! encoder the join matches on, so the two cannot drift.
//!
//! It has no update path: an index is either built for a query or written as a
//! stamped sidecar and rebuilt when the table changes.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::heap::HeapManager;
use crate::types::value::DataValue;

use super::super::error::JoinError;
use super::super::key::{JoinKey, KeyClass, KeySpec, encode_value};
use super::super::row::RowCodec;
use super::super::source::TableRef;
use super::super::stats::ValidityStamp;
use super::{IndexKeySpec, JoinIndex, RowLocator};

const MAGIC: [u8; 8] = *b"RKJIDX\0\x01";

/// Entries sorted by key, with the row each points at.
#[derive(Debug, Clone)]
pub struct SortedKeyIndex {
    entries: Vec<(JoinKey, RowLocator)>,
    key_spec: IndexKeySpec,
    stamp: ValidityStamp,
}

impl SortedKeyIndex {
    /// Build an index over `columns` of `table`, in one pass.
    pub fn build(table: &TableRef, columns: &[usize]) -> Result<Self, JoinError> {
        if columns.is_empty() {
            return Err(JoinError::plan(
                "an index needs at least one column".to_string(),
            ));
        }

        let types: Vec<_> = table.columns.iter().map(|c| c.data_type.clone()).collect();

        let mut classes = Vec::with_capacity(columns.len());
        for column in columns {
            let data_type = types.get(*column).ok_or_else(|| {
                JoinError::plan(format!(
                    "column {column} is out of range for '{}'",
                    table.alias
                ))
            })?;
            classes.push(KeyClass::of(data_type));
        }

        let codec = RowCodec::new(types);
        let manager = HeapManager::open(table.path.clone())
            .map_err(|e| JoinError::Io(format!("cannot open {}: {e}", table.path.display())))?;

        // Read the stamp after opening: `HeapManager::open` synchronises the
        // header and so may rewrite the file.
        let stamp = ValidityStamp::read(&table.path)?;

        let mut entries = Vec::new();
        let mut values: Vec<Option<DataValue>> = Vec::new();

        for item in manager.scan() {
            let (page_id, slot_id, bytes) =
                item.map_err(|e| JoinError::Io(format!("scan failed: {e}")))?;
            codec.decode_into(&bytes, &mut values)?;

            // A NULL in any component means no key, and a row with no key can
            // never match - so it is not indexed at all.
            let mut encoded = Vec::new();
            let mut complete = true;
            for (position, column) in columns.iter().enumerate() {
                match values.get(*column).and_then(|value| value.as_ref()) {
                    Some(value) => encoded.extend(encode_value(classes[position], value)?),
                    None => {
                        complete = false;
                        break;
                    }
                }
            }
            if !complete {
                continue;
            }

            entries.push((
                JoinKey::from_bytes(encoded),
                RowLocator { page_id, slot_id },
            ));
        }

        entries.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(Self {
            entries,
            key_spec: IndexKeySpec::new(columns.to_vec(), classes),
            stamp,
        })
    }

    pub fn stamp(&self) -> ValidityStamp {
        self.stamp
    }

    /// Distinct keys held, counted exactly - the entries are sorted, so this
    /// is one pass with no estimation.
    pub fn distinct_keys(&self) -> u64 {
        let mut distinct = 0u64;
        let mut previous: Option<&JoinKey> = None;
        for (key, _) in &self.entries {
            if previous != Some(key) {
                distinct += 1;
                previous = Some(key);
            }
        }
        distinct
    }

    /// Write the index beside its table.
    pub fn save(&self, path: &Path) -> Result<(), JoinError> {
        let file = File::create(path)
            .map_err(|e| JoinError::Io(format!("cannot create {}: {e}", path.display())))?;
        let mut file = BufWriter::new(file);

        let write = |file: &mut BufWriter<File>| -> std::io::Result<()> {
            file.write_all(&MAGIC)?;
            file.write_all(&self.stamp.file_len.to_le_bytes())?;
            file.write_all(&self.stamp.modified_secs.to_le_bytes())?;
            file.write_all(&self.stamp.total_tuples.to_le_bytes())?;

            let columns = self.key_spec.columns.len() as u32;
            file.write_all(&columns.to_le_bytes())?;
            for (column, class) in self.key_spec.columns.iter().zip(&self.key_spec.classes) {
                file.write_all(&(*column as u32).to_le_bytes())?;
                let rendered = format!("{class:?}");
                file.write_all(&(rendered.len() as u32).to_le_bytes())?;
                file.write_all(rendered.as_bytes())?;
            }

            file.write_all(&(self.entries.len() as u64).to_le_bytes())?;
            for (key, locator) in &self.entries {
                file.write_all(&(key.byte_len() as u32).to_le_bytes())?;
                file.write_all(key.as_bytes())?;
                file.write_all(&locator.page_id.to_le_bytes())?;
                file.write_all(&locator.slot_id.to_le_bytes())?;
            }
            file.flush()
        };

        write(&mut file).map_err(|e| JoinError::Io(format!("cannot write {}: {e}", path.display())))
    }

    /// Read an index back, refusing it unless it still describes the table.
    pub fn load(path: &Path, expected: ValidityStamp) -> Result<Self, JoinError> {
        let file = File::open(path)
            .map_err(|e| JoinError::Io(format!("cannot open {}: {e}", path.display())))?;
        let mut file = BufReader::new(file);

        let mut magic = [0u8; 8];
        read_exact(&mut file, &mut magic, path)?;
        if magic != MAGIC {
            return Err(JoinError::Io(format!(
                "{} is not a join index",
                path.display()
            )));
        }

        let stamp = ValidityStamp {
            file_len: read_u64(&mut file, path)?,
            modified_secs: read_u64(&mut file, path)? as i64,
            total_tuples: read_u64(&mut file, path)?,
        };
        // A stale index would point at rows that have moved. Rejecting is the
        // only safe answer; the planner falls back to another algorithm.
        if stamp != expected {
            return Err(JoinError::Io(format!(
                "{} was built for a different version of the table",
                path.display()
            )));
        }

        let column_count = read_u32(&mut file, path)? as usize;
        let mut columns = Vec::with_capacity(column_count);
        let mut classes = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            columns.push(read_u32(&mut file, path)? as usize);
            let length = read_u32(&mut file, path)? as usize;
            let mut rendered = vec![0u8; length];
            read_exact(&mut file, &mut rendered, path)?;
            let rendered = String::from_utf8(rendered).map_err(|_| {
                JoinError::Io(format!("{} has a corrupt key class", path.display()))
            })?;
            classes.push(parse_class(&rendered).ok_or_else(|| {
                JoinError::Io(format!("{} names an unknown key class", path.display()))
            })?);
        }

        let entry_count = read_u64(&mut file, path)?;
        let mut entries = Vec::with_capacity(entry_count.min(1_000_000) as usize);
        for _ in 0..entry_count {
            let key_length = read_u32(&mut file, path)? as usize;
            let mut key = vec![0u8; key_length];
            read_exact(&mut file, &mut key, path)?;
            let page_id = read_u32(&mut file, path)?;
            let slot_id = read_u32(&mut file, path)?;
            entries.push((JoinKey::from_bytes(key), RowLocator { page_id, slot_id }));
        }

        Ok(Self {
            entries,
            key_spec: IndexKeySpec::new(columns, classes),
            stamp,
        })
    }
}

impl JoinIndex for SortedKeyIndex {
    fn probe(&self, key: &JoinKey) -> Result<Vec<RowLocator>, JoinError> {
        // The entries are sorted by the same byte encoding the probe key uses,
        // so the matches are one contiguous run.
        let start = self.entries.partition_point(|(entry, _)| entry < key);
        let mut matches = Vec::new();
        for (entry, locator) in &self.entries[start..] {
            if entry != key {
                break;
            }
            matches.push(*locator);
        }
        Ok(matches)
    }

    fn key_spec(&self) -> &IndexKeySpec {
        &self.key_spec
    }

    fn entry_count(&self) -> u64 {
        self.entries.len() as u64
    }
}

/// Build an index and write it beside the table.
pub fn create_index(
    table: &TableRef,
    columns: &[usize],
) -> Result<(SortedKeyIndex, std::path::PathBuf), JoinError> {
    let index = SortedKeyIndex::build(table, columns)?;
    let path = super::index_path(&table.path, columns);
    index.save(&path)?;
    Ok((index, path))
}

/// Remove an index sidecar. Returns whether one was there.
pub fn drop_index(table: &TableRef, columns: &[usize]) -> Result<bool, JoinError> {
    let path = super::index_path(&table.path, columns);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(JoinError::Io(format!(
            "cannot remove {}: {e}",
            path.display()
        ))),
    }
}

/// Columns of the inner relation an equijoin would probe.
pub fn inner_key_columns(keys: &KeySpec) -> Vec<usize> {
    keys.columns
        .iter()
        .map(|column| column.right_index)
        .collect()
}

fn parse_class(rendered: &str) -> Option<KeyClass> {
    // `KeyClass` is a small closed set; matching its debug rendering keeps the
    // on-disk form readable without a bespoke encoding.
    match rendered {
        "Integer" => Some(KeyClass::Integer),
        "Real" => Some(KeyClass::Real),
        "Double" => Some(KeyClass::Double),
        "Bool" => Some(KeyClass::Bool),
        "Char" => Some(KeyClass::Char),
        "Varchar" => Some(KeyClass::Varchar),
        "Date" => Some(KeyClass::Date),
        "Time" => Some(KeyClass::Time),
        "Timestamp" => Some(KeyClass::Timestamp),
        "Bit" => Some(KeyClass::Bit),
        other => other
            .strip_prefix("Numeric { scale: ")
            .and_then(|rest| rest.strip_suffix(" }"))
            .and_then(|scale| scale.parse::<u8>().ok())
            .map(|scale| KeyClass::Numeric { scale }),
    }
}

fn read_exact(file: &mut BufReader<File>, buffer: &mut [u8], path: &Path) -> Result<(), JoinError> {
    file.read_exact(buffer)
        .map_err(|e| JoinError::Io(format!("{} is truncated: {e}", path.display())))
}

fn read_u32(file: &mut BufReader<File>, path: &Path) -> Result<u32, JoinError> {
    let mut buffer = [0u8; 4];
    read_exact(file, &mut buffer, path)?;
    Ok(u32::from_le_bytes(buffer))
}

fn read_u64(file: &mut BufReader<File>, path: &Path) -> Result<u64, JoinError> {
    let mut buffer = [0u8; 8];
    read_exact(file, &mut buffer, path)?;
    Ok(u64::from_le_bytes(buffer))
}
