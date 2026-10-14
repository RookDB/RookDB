//! External merge sort: the part of `ORDER BY` that lives on disk.
//!
//! [`SortOperator`](super::operators::SortOperator) collects rows until they
//! exceed its memory budget, sorts them and hands them to an
//! [`ExternalSorter`], which writes them out as one sorted run. When the
//! input ends, [`ExternalSorter::finish`] merges the runs: at most
//! `merge_fan_in` of them are open at a time, so more runs than that are
//! first merged group by group into longer runs, pass after pass, until one
//! merge can read them all. That last merge is the operator's output.
//!
//! Ties keep their arrival order, as in the in-memory stable sort: runs are
//! numbered in the order their rows arrived, a merge prefers the lower
//! numbered run, and a pass merges neighbouring runs only.
//!
//! # Run file format
//!
//! ```text
//! [u32 length][row bytes]   one per row, in sort order
//! [u32 0xFFFFFFFF][u64 n]   end marker and the number of rows before it
//! ```
//!
//! All integers are little-endian. A row is encoded with its values' own
//! types (see `encode_tuple`), not the operator's schema, so whatever the
//! child produced comes back unchanged. A run that ends before its end
//! marker, or does not decode, is an error.
//!
//! Run files are deleted as soon as they have been merged, when the query
//! finishes, and when it fails.

use std::cmp::Ordering;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

use super::operators::compare_rows;
use super::tuple::Tuple;
use crate::backend::error::{RookError, RookResult};
use crate::types::value::{DataValue, NumericValue, OrderedF32, OrderedF64};

/// Memory a sort may use before it spills, unless the engine says otherwise.
pub const DEFAULT_SORT_MEMORY_BYTES: usize = 256 * 1024 * 1024;

/// Runs one merge reads at a time, unless the engine says otherwise.
pub const DEFAULT_MERGE_FAN_IN: usize = 64;

/// Buffer of each open run file.
const RUN_BUFFER_BYTES: usize = 64 * 1024;

/// Length prefix that ends a run.
const END_OF_RUN: u32 = u32::MAX;

/// Gives every sorter of this process its own file names.
static SORTER_ID: AtomicU64 = AtomicU64::new(0);

/// When and where a sort spills.
#[derive(Debug, Clone)]
pub struct SortSpill {
    /// Bytes of rows held in memory before they are written out as a run.
    pub memory_budget: usize,
    /// Most runs one merge reads at a time (at least 2).
    pub merge_fan_in: usize,
    /// Directory for run files, created on first use.
    pub temp_dir: PathBuf,
}

/// Roughly the memory `tuple` occupies, for the sort's memory budget.
pub(crate) fn tuple_bytes(tuple: &Tuple) -> usize {
    let heap: usize = tuple
        .values
        .iter()
        .map(|v| match v {
            Some(DataValue::Char(s)) | Some(DataValue::Varchar(s)) | Some(DataValue::Bit(s)) => {
                s.capacity()
            }
            _ => 0,
        })
        .sum();
    size_of::<Tuple>() + tuple.values.capacity() * size_of::<Option<DataValue>>() + heap
}

fn corrupt(path: &std::path::Path, what: impl std::fmt::Display) -> RookError {
    RookError::Io(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("sort run {}: {}", path.display(), what),
    ))
}

// ── Row encoding ─────────────────────────────────────────────────────────────

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn put_date(out: &mut Vec<u8>, d: &NaiveDate) {
    out.extend_from_slice(&d.num_days_from_ce().to_le_bytes());
}

fn put_time(out: &mut Vec<u8>, t: &NaiveTime) {
    out.extend_from_slice(&t.num_seconds_from_midnight().to_le_bytes());
    out.extend_from_slice(&t.nanosecond().to_le_bytes());
}

/// Append `tuple` to `out`: heap location, column count, then each value as
/// a tag byte (0 = NULL) and its payload.
fn encode_tuple(tuple: &Tuple, out: &mut Vec<u8>) {
    out.push(tuple.page_id.is_some() as u8 | (tuple.slot_id.is_some() as u8) << 1);
    for id in [tuple.page_id, tuple.slot_id].into_iter().flatten() {
        out.extend_from_slice(&id.to_le_bytes());
    }
    out.extend_from_slice(&(tuple.values.len() as u32).to_le_bytes());
    for value in &tuple.values {
        match value {
            None => out.push(0),
            Some(DataValue::SmallInt(v)) => {
                out.push(1);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Some(DataValue::Int(v)) => {
                out.push(2);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Some(DataValue::BigInt(v)) => {
                out.push(3);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Some(DataValue::Real(v)) => {
                out.push(4);
                out.extend_from_slice(&v.0.to_bits().to_le_bytes());
            }
            Some(DataValue::DoublePrecision(v)) => {
                out.push(5);
                out.extend_from_slice(&v.0.to_bits().to_le_bytes());
            }
            Some(DataValue::Numeric(v)) => {
                out.push(6);
                out.extend_from_slice(&v.unscaled.to_le_bytes());
                out.push(v.scale);
            }
            Some(DataValue::Bool(v)) => {
                out.push(7);
                out.push(*v as u8);
            }
            Some(DataValue::Char(s)) => {
                out.push(8);
                put_str(out, s);
            }
            Some(DataValue::Varchar(s)) => {
                out.push(9);
                put_str(out, s);
            }
            Some(DataValue::Date(d)) => {
                out.push(10);
                put_date(out, d);
            }
            Some(DataValue::Time(t)) => {
                out.push(11);
                put_time(out, t);
            }
            Some(DataValue::Bit(s)) => {
                out.push(12);
                put_str(out, s);
            }
            Some(DataValue::Timestamp(ts)) => {
                out.push(13);
                put_date(out, &ts.date());
                put_time(out, &ts.time());
            }
        }
    }
}

/// Reads the fields of one encoded row.
struct RowBytes<'a>(&'a [u8]);

impl<'a> RowBytes<'a> {
    fn take(&mut self, n: usize) -> RookResult<&'a [u8]> {
        if self.0.len() < n {
            return Err(RookError::Internal(
                "row is shorter than its fields".to_string(),
            ));
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> RookResult<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("take returns N bytes"))
    }

    fn u32(&mut self) -> RookResult<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn string(&mut self) -> RookResult<String> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| RookError::Internal("text is not UTF-8".to_string()))
    }

    fn date(&mut self) -> RookResult<NaiveDate> {
        NaiveDate::from_num_days_from_ce_opt(i32::from_le_bytes(self.array()?))
            .ok_or_else(|| RookError::Internal("date out of range".to_string()))
    }

    fn time(&mut self) -> RookResult<NaiveTime> {
        let (secs, nanos) = (self.u32()?, self.u32()?);
        NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
            .ok_or_else(|| RookError::Internal("time out of range".to_string()))
    }
}

/// The inverse of [`encode_tuple`].
fn decode_tuple(bytes: &[u8]) -> RookResult<Tuple> {
    let mut row = RowBytes(bytes);
    let flags = row.array::<1>()?[0];
    let page_id = if flags & 1 != 0 {
        Some(row.u32()?)
    } else {
        None
    };
    let slot_id = if flags & 2 != 0 {
        Some(row.u32()?)
    } else {
        None
    };
    let columns = row.u32()? as usize;
    // Every value takes at least its tag byte.
    if columns > row.0.len() {
        return Err(RookError::Internal(
            "row is shorter than its fields".to_string(),
        ));
    }
    let mut values = Vec::with_capacity(columns);
    for _ in 0..columns {
        let value = match row.array::<1>()?[0] {
            0 => None,
            1 => Some(DataValue::SmallInt(i16::from_le_bytes(row.array()?))),
            2 => Some(DataValue::Int(i32::from_le_bytes(row.array()?))),
            3 => Some(DataValue::BigInt(i64::from_le_bytes(row.array()?))),
            4 => Some(DataValue::Real(OrderedF32(f32::from_bits(row.u32()?)))),
            5 => Some(DataValue::DoublePrecision(OrderedF64(f64::from_bits(
                u64::from_le_bytes(row.array()?),
            )))),
            6 => Some(DataValue::Numeric(NumericValue {
                unscaled: i128::from_le_bytes(row.array()?),
                scale: row.array::<1>()?[0],
            })),
            7 => Some(DataValue::Bool(row.array::<1>()?[0] != 0)),
            8 => Some(DataValue::Char(row.string()?)),
            9 => Some(DataValue::Varchar(row.string()?)),
            10 => Some(DataValue::Date(row.date()?)),
            11 => Some(DataValue::Time(row.time()?)),
            12 => Some(DataValue::Bit(row.string()?)),
            13 => Some(DataValue::Timestamp(NaiveDateTime::new(
                row.date()?,
                row.time()?,
            ))),
            tag => return Err(RookError::Internal(format!("unknown value tag {}", tag))),
        };
        values.push(value);
    }
    if !row.0.is_empty() {
        return Err(RookError::Internal(
            "row is longer than its fields".to_string(),
        ));
    }
    Ok(Tuple {
        values,
        page_id,
        slot_id,
    })
}

// ── Run files ────────────────────────────────────────────────────────────────

/// One sorted run on disk. Dropping it deletes the file.
struct RunFile {
    path: PathBuf,
}

impl Drop for RunFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Writes rows, already in sort order, to a new run file.
struct RunWriter {
    run: RunFile,
    file: BufWriter<File>,
    rows: u64,
    /// The row being encoded, reused between rows.
    scratch: Vec<u8>,
}

impl RunWriter {
    fn create(path: PathBuf) -> RookResult<Self> {
        let file = File::create(&path)?;
        Ok(Self {
            run: RunFile { path },
            file: BufWriter::with_capacity(RUN_BUFFER_BYTES, file),
            rows: 0,
            scratch: Vec::new(),
        })
    }

    fn write(&mut self, tuple: &Tuple) -> RookResult<()> {
        self.scratch.clear();
        encode_tuple(tuple, &mut self.scratch);
        let len = u32::try_from(self.scratch.len())
            .ok()
            .filter(|&len| len != END_OF_RUN)
            .ok_or_else(|| RookError::Internal("row too large to sort on disk".to_string()))?;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&self.scratch)?;
        self.rows += 1;
        Ok(())
    }

    /// Write the end marker and hand over the finished run.
    fn finish(mut self) -> RookResult<RunFile> {
        self.file.write_all(&END_OF_RUN.to_le_bytes())?;
        self.file.write_all(&self.rows.to_le_bytes())?;
        self.file.flush()?;
        Ok(self.run)
    }
}

/// Reads a run file back, row by row.
struct RunReader {
    run: RunFile,
    file: BufReader<File>,
    rows: u64,
    done: bool,
    scratch: Vec<u8>,
}

impl RunReader {
    fn open(run: RunFile) -> RookResult<Self> {
        let file = File::open(&run.path)?;
        Ok(Self {
            run,
            file: BufReader::with_capacity(RUN_BUFFER_BYTES, file),
            rows: 0,
            done: false,
            scratch: Vec::new(),
        })
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> RookResult<()> {
        self.file.read_exact(buf).map_err(|e| match e.kind() {
            io::ErrorKind::UnexpectedEof => corrupt(&self.run.path, "file is truncated"),
            _ => RookError::Io(e),
        })
    }

    /// The next row, or `None` after the last one.
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if self.done {
            return Ok(None);
        }
        let mut len = [0u8; 4];
        self.read_exact(&mut len)?;
        let len = u32::from_le_bytes(len);
        if len == END_OF_RUN {
            let mut rows = [0u8; 8];
            self.read_exact(&mut rows)?;
            let rows = u64::from_le_bytes(rows);
            if rows != self.rows {
                return Err(corrupt(
                    &self.run.path,
                    format!("holds {} rows, its end marker says {}", self.rows, rows),
                ));
            }
            self.done = true;
            return Ok(None);
        }
        // Read through `take` so a damaged length cannot ask for more memory
        // than the file has bytes.
        self.scratch.clear();
        let read = (&mut self.file)
            .take(len as u64)
            .read_to_end(&mut self.scratch)?;
        if read < len as usize {
            return Err(corrupt(&self.run.path, "file is truncated"));
        }
        self.rows += 1;
        decode_tuple(&self.scratch)
            .map(Some)
            .map_err(|e| corrupt(&self.run.path, e))
    }
}

// ── Merge ────────────────────────────────────────────────────────────────────

/// Merges sorted runs into one sorted stream.
///
/// A binary min-heap holds the next row of each run that still has one,
/// ordered by the sort keys and then by run number.
pub(crate) struct Merge {
    sort_keys: Vec<(usize, bool)>,
    readers: Vec<RunReader>,
    /// (next row of a run, index of the run in `readers`).
    heap: Vec<(Tuple, usize)>,
}

impl Merge {
    fn open(runs: Vec<RunFile>, sort_keys: &[(usize, bool)]) -> RookResult<Self> {
        let mut merge = Self {
            sort_keys: sort_keys.to_vec(),
            readers: Vec::with_capacity(runs.len()),
            heap: Vec::with_capacity(runs.len()),
        };
        for (idx, run) in runs.into_iter().enumerate() {
            let mut reader = RunReader::open(run)?;
            if let Some(tuple) = reader.next()? {
                merge.heap.push((tuple, idx));
            }
            merge.readers.push(reader);
        }
        for i in (0..merge.heap.len() / 2).rev() {
            merge.sift_down(i);
        }
        Ok(merge)
    }

    /// Whether heap entry `a` comes out before entry `b`.
    fn before(&self, a: usize, b: usize) -> bool {
        let (a, b) = (&self.heap[a], &self.heap[b]);
        compare_rows(&self.sort_keys, &a.0, &b.0).then(a.1.cmp(&b.1)) == Ordering::Less
    }

    fn sift_down(&mut self, mut i: usize) {
        loop {
            let mut first = i;
            for child in [2 * i + 1, 2 * i + 2] {
                if child < self.heap.len() && self.before(child, first) {
                    first = child;
                }
            }
            if first == i {
                return;
            }
            self.heap.swap(i, first);
            i = first;
        }
    }

    /// The next row in sort order, or `None` when every run is used up.
    pub(crate) fn next(&mut self) -> RookResult<Option<Tuple>> {
        if self.heap.is_empty() {
            return Ok(None);
        }
        let run = self.heap[0].1;
        let tuple = match self.readers[run].next()? {
            Some(next) => std::mem::replace(&mut self.heap[0].0, next),
            None => self.heap.swap_remove(0).0,
        };
        self.sift_down(0);
        Ok(Some(tuple))
    }
}

// ── Sorter ───────────────────────────────────────────────────────────────────

/// Collects sorted runs and merges them. See the module documentation.
pub(crate) struct ExternalSorter {
    sort_keys: Vec<(usize, bool)>,
    temp_dir: PathBuf,
    merge_fan_in: usize,
    id: u64,
    /// Run files created so far, for their names.
    created: u64,
    /// The runs, in the order their rows arrived.
    runs: Vec<RunFile>,
}

impl ExternalSorter {
    pub(crate) fn new(sort_keys: Vec<(usize, bool)>, spill: &SortSpill) -> RookResult<Self> {
        fs::create_dir_all(&spill.temp_dir)?;
        Ok(Self {
            sort_keys,
            temp_dir: spill.temp_dir.clone(),
            merge_fan_in: spill.merge_fan_in.max(2),
            id: SORTER_ID.fetch_add(1, AtomicOrdering::Relaxed),
            created: 0,
            runs: Vec::new(),
        })
    }

    fn writer(&mut self) -> RookResult<RunWriter> {
        let path = self.temp_dir.join(format!(
            "sort_{}_{}_{}.run",
            std::process::id(),
            self.id,
            self.created
        ));
        self.created += 1;
        RunWriter::create(path)
    }

    /// Write `rows`, which must be in sort order, as the next run, leaving
    /// `rows` empty.
    pub(crate) fn spill(&mut self, rows: &mut Vec<Tuple>) -> RookResult<()> {
        let mut writer = self.writer()?;
        for tuple in rows.drain(..) {
            writer.write(&tuple)?;
        }
        self.runs.push(writer.finish()?);
        Ok(())
    }

    /// Merge the runs down to at most `merge_fan_in` and return the merge of
    /// those.
    pub(crate) fn finish(mut self) -> RookResult<Merge> {
        let mut passes = 0;
        while self.runs.len() > self.merge_fan_in {
            let mut runs = std::mem::take(&mut self.runs).into_iter();
            let mut merged = Vec::new();
            loop {
                let mut group: Vec<RunFile> = runs.by_ref().take(self.merge_fan_in).collect();
                match group.len() {
                    0 => break,
                    1 => merged.extend(group.pop()),
                    _ => {
                        let mut merge = Merge::open(group, &self.sort_keys)?;
                        let mut writer = self.writer()?;
                        while let Some(tuple) = merge.next()? {
                            writer.write(&tuple)?;
                        }
                        merged.push(writer.finish()?);
                    }
                }
            }
            self.runs = merged;
            passes += 1;
        }
        log::debug!(
            "[Sort] {} run files written, {} intermediate merge passes, final merge of {}",
            self.created,
            passes,
            self.runs.len()
        );
        Merge::open(std::mem::take(&mut self.runs), &self.sort_keys)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let dir =
                std::env::temp_dir().join(format!("rookdb_extsort_{}_{}", std::process::id(), tag));
            let _ = fs::remove_dir_all(&dir);
            TempDir(dir)
        }

        fn files(&self) -> usize {
            fs::read_dir(&self.0).map(|d| d.count()).unwrap_or(0)
        }

        fn spill(&self, merge_fan_in: usize) -> SortSpill {
            SortSpill {
                memory_budget: 0,
                merge_fan_in,
                temp_dir: self.0.clone(),
            }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn row(key: i32, tag: i32) -> Tuple {
        Tuple::new(vec![Some(DataValue::Int(key)), Some(DataValue::Int(tag))])
    }

    fn drain(mut merge: Merge) -> RookResult<Vec<Tuple>> {
        let mut out = Vec::new();
        while let Some(tuple) = merge.next()? {
            out.push(tuple);
        }
        Ok(out)
    }

    #[test]
    fn every_value_type_round_trips() {
        let date = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        let time = NaiveTime::from_hms_micro_opt(23, 59, 58, 123_456).unwrap();
        let tuple = Tuple::new_with_location(
            vec![
                None,
                Some(DataValue::SmallInt(-7)),
                Some(DataValue::Int(i32::MIN)),
                Some(DataValue::BigInt(i64::MAX)),
                Some(DataValue::Real(OrderedF32(f32::NAN))),
                Some(DataValue::DoublePrecision(OrderedF64(-0.0))),
                Some(DataValue::Numeric(NumericValue {
                    unscaled: -12_345_678_901_234_567_890_123,
                    scale: 4,
                })),
                Some(DataValue::Bool(true)),
                Some(DataValue::Char("ab ".to_string())),
                Some(DataValue::Varchar("héllo".to_string())),
                Some(DataValue::Date(date)),
                Some(DataValue::Time(time)),
                Some(DataValue::Bit("0101".to_string())),
                Some(DataValue::Timestamp(NaiveDateTime::new(date, time))),
            ],
            9,
            3,
        );
        for tuple in [tuple, Tuple::empty(), row(1, 2)] {
            let mut bytes = Vec::new();
            encode_tuple(&tuple, &mut bytes);
            assert_eq!(decode_tuple(&bytes).unwrap(), tuple);
        }
    }

    /// More runs than the fan-in: several passes, ties in arrival order,
    /// never more files than runs, none left behind.
    #[test]
    fn multi_pass_merge_is_stable_and_cleans_up() {
        let dir = TempDir::new("multipass");
        let keys = vec![(0, false)];
        let mut sorter = ExternalSorter::new(keys, &dir.spill(3)).unwrap();
        // 40 runs of 5 rows; keys repeat across runs, tags record arrival.
        let mut expected = Vec::new();
        for run in 0..40 {
            let mut rows: Vec<Tuple> = (0..5).map(|k| row(k, run)).collect();
            expected.extend(rows.iter().cloned());
            sorter.spill(&mut rows).unwrap();
        }
        assert_eq!(dir.files(), 40);
        expected.sort_by(|a, b| compare_rows(&[(0, false)], a, b));

        let merge = sorter.finish().unwrap();
        assert!(merge.readers.len() <= 3);
        assert!(dir.files() <= 3, "merged runs are deleted");
        assert_eq!(drain(merge).unwrap(), expected);
        assert_eq!(dir.files(), 0);
    }

    #[test]
    fn truncated_run_is_an_error() {
        for cut in [1, 9, 13, 30] {
            let dir = TempDir::new(&format!("truncated{}", cut));
            let mut sorter = ExternalSorter::new(vec![(0, false)], &dir.spill(4)).unwrap();
            for run in 0..2 {
                sorter
                    .spill(&mut (0..50).map(|k| row(k, run)).collect())
                    .unwrap();
            }
            let path = sorter.runs[1].path.clone();
            let len = fs::metadata(&path).unwrap().len();
            File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(len - cut)
                .unwrap();

            let err = sorter
                .finish()
                .and_then(drain)
                .expect_err("a truncated run must not read as a short one");
            assert!(err.to_string().contains("truncated"), "{}", err);
            assert_eq!(dir.files(), 0, "run files are removed on error");
        }
    }

    #[test]
    fn corrupt_run_is_an_error() {
        let dir = TempDir::new("corrupt");
        let mut sorter = ExternalSorter::new(vec![(0, false)], &dir.spill(4)).unwrap();
        sorter
            .spill(&mut (0..50).map(|k| row(k, 0)).collect())
            .unwrap();
        let path = sorter.runs[0].path.clone();
        let mut bytes = fs::read(&path).unwrap();
        bytes[4 + 5] = 99; // first row's first value tag
        fs::write(&path, bytes).unwrap();

        let err = sorter.finish().and_then(drain).expect_err("corrupt run");
        assert!(err.to_string().contains("unknown value tag"), "{}", err);
        assert_eq!(dir.files(), 0);
    }
}
