//! External sort on join keys.
//!
//! Rows accumulate until the budget is spent, then sort and flush as a run;
//! runs are merged k-way. Ordering is `JoinKey` bytes, which are a total order,
//! so the merge never sees an unsorted run. NULL-keyed rows are set aside
//! rather than sorted - they cannot match anything.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::Arc;

use super::error::JoinError;
use super::key::{JoinKey, KeySpec};
use super::memory::{MemoryAccountant, row_footprint};
use super::row::RowCodec;
use super::spill::{RowBuffer, RowBufferBuilder, RunHandle, RunReader, RunWriter, SpillScope};

/// Which side of the key specification a relation supplies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySide {
    Left,
    Right,
}

impl KeySide {
    fn extract(
        self,
        keys: &KeySpec,
        values: &[Option<crate::types::value::DataValue>],
    ) -> Result<Option<JoinKey>, JoinError> {
        match self {
            KeySide::Left => keys.left_key(values),
            KeySide::Right => keys.right_key(values),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SortStats {
    /// Runs written to disk. Zero means the sort stayed in memory.
    pub runs: u64,
    /// Passes over the runs, including the final merge.
    pub merge_passes: u64,
    pub spilled_bytes: u64,
    pub sorted_rows: u64,
    pub null_keyed_rows: u64,
}

/// The result of sorting one relation.
pub struct SortOutput {
    /// Rows with a non-NULL key, ascending.
    pub rows: SortedRows,
    /// Rows whose key had a NULL component, in input order.
    pub null_keyed: RowBuffer,
    pub stats: SortStats,
}

/// Sort a stream of serialized rows by join key.
#[allow(clippy::too_many_arguments)]
pub fn sort_rows(
    input: &mut dyn Iterator<Item = Result<Vec<u8>, JoinError>>,
    codec: &RowCodec,
    keys: &KeySpec,
    side: KeySide,
    budget: &MemoryAccountant,
    scope: &Arc<SpillScope>,
    label: &str,
    fingerprint: u64,
    merge_buffer_bytes: u64,
) -> Result<SortOutput, JoinError> {
    let mut stats = SortStats::default();
    let mut resident: Vec<(JoinKey, Vec<u8>)> = Vec::new();
    let mut charged: u64 = 0;
    let mut runs: Vec<RunHandle> = Vec::new();
    let mut nulls = RowBufferBuilder::new(scope, format!("{label}-null"), fingerprint);

    for row in input {
        let bytes = row?;
        let values = codec.decode(&bytes)?;

        let Some(key) = side.extract(keys, &values)? else {
            stats.null_keyed_rows += 1;
            nulls.push(&bytes, budget)?;
            continue;
        };

        stats.sorted_rows += 1;
        let footprint = entry_footprint(&key, &bytes);

        let mut accepted = budget.charge(footprint).is_ok();
        if !accepted && !resident.is_empty() {
            // Spend what is held on a run, then try again with an empty
            // buffer.
            let handle = flush_run(&mut resident, scope, label, fingerprint)?;
            stats.runs += 1;
            stats.spilled_bytes += handle.bytes();
            runs.push(handle);
            budget.release(charged);
            charged = 0;
            accepted = budget.charge(footprint).is_ok();
        }

        // A single row larger than the whole budget is held anyway: refusing
        // it would leave the operator unable to make any progress at all.
        if accepted {
            charged += footprint;
        }
        resident.push((key, bytes));
    }

    let null_keyed = nulls.finish(budget)?;

    // Everything fit: sort in place and hand the rows straight over.
    if runs.is_empty() {
        resident.sort_by(|a, b| a.0.cmp(&b.0));
        return Ok(SortOutput {
            rows: SortedRows::Memory(resident.into_iter()),
            null_keyed,
            stats,
        });
    }

    if !resident.is_empty() {
        let handle = flush_run(&mut resident, scope, label, fingerprint)?;
        stats.runs += 1;
        stats.spilled_bytes += handle.bytes();
        runs.push(handle);
        budget.release(charged);
    }

    let fan_in = merge_fan_in(budget.budget(), merge_buffer_bytes);

    // Reduce to at most `fan_in` runs, so the final merge can be lazy.
    while runs.len() > fan_in {
        stats.merge_passes += 1;
        let mut merged = Vec::with_capacity(runs.len().div_ceil(fan_in));
        for group in runs.chunks(fan_in) {
            let handle = merge_group(group, scope, label, fingerprint, codec, keys, side)?;
            stats.spilled_bytes += handle.bytes();
            merged.push(handle);
        }
        runs = merged;
    }

    stats.merge_passes += 1;
    Ok(SortOutput {
        rows: SortedRows::Merge(MergeReader::open(&runs, codec, keys, side)?),
        null_keyed,
        stats,
    })
}

fn entry_footprint(key: &JoinKey, row: &[u8]) -> u64 {
    // The row, the key bytes, and the tuple that holds them.
    row_footprint(row.len()) + key.byte_len() as u64 + 24
}

fn merge_fan_in(budget: u64, buffer_bytes: u64) -> usize {
    // At least two, or the reduction loop would never terminate.
    ((budget / buffer_bytes.max(1)) as usize).max(2)
}

fn flush_run(
    resident: &mut Vec<(JoinKey, Vec<u8>)>,
    scope: &Arc<SpillScope>,
    label: &str,
    fingerprint: u64,
) -> Result<RunHandle, JoinError> {
    resident.sort_by(|a, b| a.0.cmp(&b.0));

    let mut writer = RunWriter::create(scope, label, fingerprint)?;
    for (_key, row) in resident.iter() {
        writer.write_row(row)?;
    }
    resident.clear();
    writer.finish()
}

fn merge_group(
    group: &[RunHandle],
    scope: &Arc<SpillScope>,
    label: &str,
    fingerprint: u64,
    codec: &RowCodec,
    keys: &KeySpec,
    side: KeySide,
) -> Result<RunHandle, JoinError> {
    let mut reader = MergeReader::open(group, codec, keys, side)?;
    let mut writer = RunWriter::create(scope, &format!("{label}-merge"), fingerprint)?;

    while let Some(item) = reader.next() {
        let (_key, row) = item?;
        writer.write_row(&row)?;
    }

    writer.finish()
}

/// A sorted sequence of `(key, row)` pairs.
pub enum SortedRows {
    Memory(std::vec::IntoIter<(JoinKey, Vec<u8>)>),
    Merge(MergeReader),
}

impl Iterator for SortedRows {
    type Item = Result<(JoinKey, Vec<u8>), JoinError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SortedRows::Memory(rows) => rows.next().map(Ok),
            SortedRows::Merge(reader) => reader.next(),
        }
    }
}

/// One row at the head of a run, waiting its turn in the merge.
struct HeapEntry {
    key: JoinKey,
    row: Vec<u8>,
    source: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    /// A total order, always: key bytes first, then the run index to break
    /// ties. Ordering never depends on comparing values, so it can never
    /// collapse to `Equal` for want of a comparison - which is what let the
    /// previous implementation feed an unsorted sequence to its merge.
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.source.cmp(&other.source))
    }
}

/// K-way merge over run files.
pub struct MergeReader {
    readers: Vec<RunReader>,
    heap: BinaryHeap<Reverse<HeapEntry>>,
    codec: RowCodec,
    keys: KeySpec,
    side: KeySide,
}

impl MergeReader {
    fn open(
        runs: &[RunHandle],
        codec: &RowCodec,
        keys: &KeySpec,
        side: KeySide,
    ) -> Result<Self, JoinError> {
        let mut readers = Vec::with_capacity(runs.len());
        for run in runs {
            readers.push(run.reader()?);
        }

        let mut reader = Self {
            readers,
            heap: BinaryHeap::new(),
            codec: codec.clone(),
            keys: keys.clone(),
            side,
        };

        for source in 0..reader.readers.len() {
            reader.pull(source)?;
        }
        Ok(reader)
    }

    /// Move the next row of `source` into the heap, if it has one.
    fn pull(&mut self, source: usize) -> Result<(), JoinError> {
        let Some(row) = self.readers[source].next() else {
            return Ok(());
        };
        let row = row?;
        let values = self.codec.decode(&row)?;

        let Some(key) = self.side.extract(&self.keys, &values)? else {
            // NULL-keyed rows are diverted before any run is written, so one
            // appearing here means a run file no longer matches its schema.
            return Err(JoinError::Io(
                "a spilled row decoded to a NULL join key; the run file is corrupt".to_string(),
            ));
        };

        self.heap.push(Reverse(HeapEntry { key, row, source }));
        Ok(())
    }
}

impl Iterator for MergeReader {
    type Item = Result<(JoinKey, Vec<u8>), JoinError>;

    fn next(&mut self) -> Option<Self::Item> {
        let Reverse(entry) = self.heap.pop()?;
        if let Err(e) = self.pull(entry.source) {
            return Some(Err(e));
        }
        Some(Ok((entry.key, entry.row)))
    }
}
