//! Table and column statistics for the join planner.
//!
//! Two decisions shape this module.
//!
//! **Row counts come from the heap header, not from a page scan.**
//! `HeaderMetadata::total_tuples` is maintained on insert and delete, so it is
//! exact and free. The engine's own `collect_table_statistics` counts slot
//! entries and does not skip dead ones, so it over-reports after a DELETE
//! until the table is compacted; it is used here only for tuple widths, and
//! only behind the cache.
//!
//! **Column values are measured in join key encoding.** A distinct-value count
//! is only useful for join selectivity if it counts the equivalence classes
//! the join actually matches on. Encoding first means two CHARs differing in
//! trailing spaces count once, exactly as the join treats them, so estimates
//! and execution cannot drift apart.
//!
//! Statistics are never refreshed implicitly. A stale sidecar is detected by
//! its validity stamp and the planner degrades to what it can prove, reporting
//! which - see [`StatsConfidence`]. Silently planning from stale numbers is
//! worse than planning from admitted ignorance.

pub mod histogram;
pub mod hll;
pub mod rng;

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};

use crate::heap::HeapManager;
use crate::types::value::DataValue;

use super::error::JoinError;
use super::key::{KeyClass, encode_value};
use super::row::RowCodec;
use super::source::TableRef;
use histogram::{Histogram, ReservoirSampler};
use hll::HyperLogLog;

/// Extension of a table's statistics sidecar.
pub const STATS_EXTENSION: &str = "stats.json";

/// Assumed row count when a table cannot be read at all.
const DEFAULT_ROWS: u64 = 1_000;
/// Assumed data pages in the same case.
const DEFAULT_PAGES: u32 = 10;
/// Assumed row width in the same case.
const DEFAULT_ROW_BYTES: f64 = 100.0;

/// How much the planner actually knows about a relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsConfidence {
    /// A current sidecar: real distinct-value counts and histograms.
    Analyzed,
    /// No sidecar, or a stale one. Row and page counts are exact; anything
    /// per-column is inferred.
    HeaderOnly,
    /// The relation could not be read. Everything is a constant.
    Defaults,
}

impl StatsConfidence {
    pub fn label(self) -> &'static str {
        match self {
            StatsConfidence::Analyzed => "analyzed",
            StatsConfidence::HeaderOnly => "header-only",
            StatsConfidence::Defaults => "defaults",
        }
    }
}

/// Identifies the exact state of a relation the statistics were taken from.
///
/// A mismatch means the table changed, so the sidecar is ignored rather than
/// trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidityStamp {
    pub file_len: u64,
    pub modified_secs: i64,
    pub total_tuples: u64,
}

impl ValidityStamp {
    /// Read the current stamp without opening a `HeapManager`, which would
    /// take a write handle and may rewrite the header.
    pub fn read(path: &Path) -> Result<Self, JoinError> {
        let metadata = std::fs::metadata(path)
            .map_err(|e| JoinError::Io(format!("cannot stat {}: {e}", path.display())))?;

        let modified_secs = metadata
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|e| JoinError::Io(format!("cannot open {}: {e}", path.display())))?;
        let header = crate::disk::read_header_page(&mut file)
            .map_err(|e| JoinError::Io(format!("cannot read {} header: {e}", path.display())))?;

        Ok(Self {
            file_len: metadata.len(),
            modified_secs,
            total_tuples: header.total_tuples,
        })
    }
}

/// Per-column statistics, in join key encoding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnStats {
    pub name: String,
    /// Exact, counted during the scan.
    pub null_fraction: f64,
    /// Estimated distinct non-NULL values.
    pub distinct: u64,
    /// Smallest and largest encoded values, absent when every row is NULL.
    pub min: Option<Vec<u8>>,
    pub max: Option<Vec<u8>>,
    /// Absent when there were too few rows for boundaries to mean anything.
    pub histogram: Option<Histogram>,
}

impl ColumnStats {
    /// Distinct values, never zero: a zero would divide selectivity by zero.
    pub fn distinct_or_one(&self) -> u64 {
        self.distinct.max(1)
    }
}

/// Everything the planner knows about one relation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableStats {
    pub stamp: ValidityStamp,
    /// Live rows. Exact - the heap maintains this counter.
    pub rows: u64,
    pub data_pages: u32,
    pub avg_row_bytes: f64,
    pub columns: Vec<ColumnStats>,
}

impl TableStats {
    pub fn column(&self, index: usize) -> Option<&ColumnStats> {
        self.columns.get(index)
    }

    /// Bytes the relation occupies, as the cost model sees it.
    pub fn total_bytes(&self) -> u64 {
        (self.rows as f64 * self.avg_row_bytes).round().max(0.0) as u64
    }
}

/// Path of a relation's statistics sidecar.
pub fn stats_path(table_path: &Path) -> PathBuf {
    let mut path = table_path.to_path_buf();
    path.set_extension(STATS_EXTENSION);
    path
}

// ── Collecting ───────────────────────────────────────────────────────────────

/// Scan a relation once and measure every column.
///
/// The scan goes through `HeapScanIterator`, which skips dead slots, so a
/// deleted row is not counted - unlike a slot-directory walk.
pub fn analyze_table(table: &TableRef) -> Result<TableStats, JoinError> {
    let stamp = ValidityStamp::read(&table.path)?;

    let manager = HeapManager::open(table.path.clone()).map_err(|e| {
        JoinError::Io(format!(
            "cannot open '{}' at {}: {e}",
            table.alias,
            table.path.display()
        ))
    })?;

    let types: Vec<_> = table.columns.iter().map(|c| c.data_type.clone()).collect();
    let classes: Vec<KeyClass> = types.iter().map(KeyClass::of).collect();
    let codec = RowCodec::new(types);

    let mut sketches: Vec<HyperLogLog> = (0..table.columns.len())
        .map(|_| HyperLogLog::new())
        .collect();
    let mut samplers: Vec<ReservoirSampler> = (0..table.columns.len())
        .map(|_| ReservoirSampler::new())
        .collect();
    let mut nulls = vec![0u64; table.columns.len()];
    let mut minima: Vec<Option<Vec<u8>>> = vec![None; table.columns.len()];
    let mut maxima: Vec<Option<Vec<u8>>> = vec![None; table.columns.len()];

    let mut rows: u64 = 0;
    let mut row_bytes: u64 = 0;
    let mut values: Vec<Option<DataValue>> = Vec::new();

    for item in manager.scan() {
        let (_page, _slot, bytes) = item.map_err(|e| JoinError::Io(format!("scan failed: {e}")))?;
        rows += 1;
        row_bytes += bytes.len() as u64;
        codec.decode_into(&bytes, &mut values)?;

        for (index, value) in values.iter().enumerate() {
            let Some(value) = value else {
                nulls[index] += 1;
                continue;
            };
            let encoded = encode_value(classes[index], value)?;

            sketches[index].add(&encoded);
            samplers[index].add(&encoded);

            match &minima[index] {
                Some(current) if current.as_slice() <= encoded.as_slice() => {}
                _ => minima[index] = Some(encoded.clone()),
            }
            match &maxima[index] {
                Some(current) if current.as_slice() >= encoded.as_slice() => {}
                _ => maxima[index] = Some(encoded),
            }
        }
    }

    let columns = table
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| ColumnStats {
            name: column.name.clone(),
            null_fraction: if rows == 0 {
                0.0
            } else {
                nulls[index] as f64 / rows as f64
            },
            distinct: sketches[index].estimate(),
            min: minima[index].clone(),
            max: maxima[index].clone(),
            histogram: std::mem::replace(&mut samplers[index], ReservoirSampler::new()).finish(),
        })
        .collect();

    Ok(TableStats {
        stamp,
        rows,
        data_pages: manager.header.page_count.saturating_sub(1),
        avg_row_bytes: if rows == 0 {
            0.0
        } else {
            row_bytes as f64 / rows as f64
        },
        columns,
    })
}

/// Write a relation's statistics beside its heap file.
pub fn save_stats(table: &TableRef, stats: &TableStats) -> Result<PathBuf, JoinError> {
    let path = stats_path(&table.path);
    let encoded = serde_json::to_vec_pretty(stats)
        .map_err(|e| JoinError::Io(format!("cannot encode statistics: {e}")))?;
    std::fs::write(&path, encoded)
        .map_err(|e| JoinError::Io(format!("cannot write {}: {e}", path.display())))?;
    Ok(path)
}

/// Read a relation's statistics, if a sidecar exists and parses.
pub fn load_stats(table: &TableRef) -> Option<TableStats> {
    let path = stats_path(&table.path);
    let bytes = std::fs::read(&path).ok()?;
    match serde_json::from_slice::<TableStats>(&bytes) {
        Ok(stats) => Some(stats),
        Err(e) => {
            // A sidecar written by an older format is not an error; it just
            // means the planner has less to work with.
            log::warn!(
                "[join] ignoring unreadable statistics at {}: {e}",
                path.display()
            );
            None
        }
    }
}

// ── Caching ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct CacheEntry {
    stats: Rc<TableStats>,
    confidence: StatsConfidence,
}

/// Per-process statistics cache, keyed by relation path and validated by
/// stamp.
///
/// Without it the planner would re-read a table's pages for every candidate
/// plan it costs.
#[derive(Debug, Default)]
pub struct TableStatsCache {
    entries: RefCell<HashMap<PathBuf, CacheEntry>>,
}

impl TableStatsCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Statistics for a relation, with an honest statement of how much they
    /// are worth.
    pub fn stats_for(&self, table: &TableRef) -> (Rc<TableStats>, StatsConfidence) {
        match self.try_stats_for(table) {
            Ok(result) => result,
            Err(e) => {
                log::warn!(
                    "[join] no statistics for '{}' ({e}); planning from defaults",
                    table.alias
                );
                (Rc::new(default_stats(table)), StatsConfidence::Defaults)
            }
        }
    }

    fn try_stats_for(
        &self,
        table: &TableRef,
    ) -> Result<(Rc<TableStats>, StatsConfidence), JoinError> {
        let stamp = ValidityStamp::read(&table.path)?;

        if let Some(entry) = self.entries.borrow().get(&table.path) {
            if entry.stats.stamp == stamp {
                return Ok((Rc::clone(&entry.stats), entry.confidence));
            }
        }

        // A sidecar is only usable if the table has not changed since it was
        // written. A stale one is ignored, never partially trusted.
        let entry = match load_stats(table) {
            Some(stats) if stats.stamp == stamp => CacheEntry {
                stats: Rc::new(stats),
                confidence: StatsConfidence::Analyzed,
            },
            _ => CacheEntry {
                stats: Rc::new(header_only_stats(table, stamp)?),
                confidence: StatsConfidence::HeaderOnly,
            },
        };

        self.entries
            .borrow_mut()
            .insert(table.path.clone(), entry.clone());
        Ok((entry.stats, entry.confidence))
    }

    /// Forget everything, so the next lookup re-reads. Used after ANALYZE.
    pub fn invalidate(&self, table: &TableRef) {
        self.entries.borrow_mut().remove(&table.path);
    }
}

/// Exact cardinality and page counts, with per-column values inferred.
///
/// The distinct-value guess is `n^0.75`, the usual default for an unanalyzed
/// column: it grows with the table but sub-linearly, so it neither claims
/// every value is unique nor that they all collide. A column the catalog
/// declares unique is known to have exactly `n`.
fn header_only_stats(table: &TableRef, stamp: ValidityStamp) -> Result<TableStats, JoinError> {
    let mut file = OpenOptions::new()
        .read(true)
        .open(&table.path)
        .map_err(|e| JoinError::Io(format!("cannot open {}: {e}", table.path.display())))?;
    let header = crate::disk::read_header_page(&mut file)
        .map_err(|e| JoinError::Io(format!("cannot read header: {e}")))?;

    let rows = header.total_tuples;
    let avg_row_bytes = measure_row_width(&mut file).unwrap_or(DEFAULT_ROW_BYTES);

    let columns = table
        .columns
        .iter()
        .map(|column| {
            let distinct = if column.constraints.unique {
                rows
            } else {
                (rows as f64).powf(0.75).round().max(1.0) as u64
            };
            ColumnStats {
                name: column.name.clone(),
                // Not measured: assume the declared nullability is exercised
                // lightly rather than pretending to know.
                null_fraction: if column.nullable { 0.05 } else { 0.0 },
                distinct,
                min: None,
                max: None,
                histogram: None,
            }
        })
        .collect();

    Ok(TableStats {
        stamp,
        rows,
        data_pages: header.page_count.saturating_sub(1),
        avg_row_bytes,
        columns,
    })
}

/// Average tuple width, from the engine's own page statistics.
///
/// Only the width is taken: that function's row count does not skip dead
/// slots and would over-report after a DELETE.
fn measure_row_width(file: &mut File) -> Option<f64> {
    let stats = crate::statistics::collect_table_statistics_from_file(file).ok()?;
    let average = stats.avg_tuple_bytes();
    if average > 0.0 { Some(average) } else { None }
}

fn default_stats(table: &TableRef) -> TableStats {
    TableStats {
        stamp: ValidityStamp {
            file_len: 0,
            modified_secs: 0,
            total_tuples: 0,
        },
        rows: DEFAULT_ROWS,
        data_pages: DEFAULT_PAGES,
        avg_row_bytes: DEFAULT_ROW_BYTES,
        columns: table
            .columns
            .iter()
            .map(|column| ColumnStats {
                name: column.name.clone(),
                null_fraction: 0.0,
                distinct: (DEFAULT_ROWS as f64).powf(0.75).round() as u64,
                min: None,
                max: None,
                histogram: None,
            })
            .collect(),
    }
}
