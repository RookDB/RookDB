use std::io;
use std::path::PathBuf;

use crate::backend::heap::heap_manager::{HeapManager, HeapScanIterator};
use crate::backend::index::btree::BTree;

use super::super::tuple::{ColumnInfo, Tuple};
use super::trait_::PhysicalOperator;

use crate::types::datatype::DataType;
use crate::types::value::DataValue;

// ── SeqScan Operator ──────────────────────────────────────────────────────────

/// Scans all tuples from a heap file by iterating through all data pages.
///
/// Deserialises raw tuple bytes into `Tuple` values using the table's schema.
pub struct SeqScanOperator {
    /// The HeapManager provides file access.
    heap_manager: HeapManager,
    /// The scan iterator (reads pages sequentially).
    scan_iter: HeapScanIterator,
    /// Column metadata for the scanned table (view column names + physical DataTypes).
    column_info: Vec<ColumnInfo>,
    /// Data types in PHYSICAL column order (used for correct deserialisation).
    schema_types: Vec<DataType>,
    /// Decoder for rows of `schema_types`, built once for the scan.
    decoder: crate::types::RowDecoder,
    /// Optional mapping from view column position to physical column position.
    /// When non-empty, deserialised values are reordered from physical order
    /// to view column order before being yielded.
    column_mapping: Vec<usize>,
    /// Whether the scan has been fully consumed.
    exhausted: bool,
}

impl SeqScanOperator {
    /// Create a new sequential scan operator (identity mapping).
    ///
    /// `heap_manager` must be already opened/created for the target table.
    /// `column_info` should contain the table's column metadata (from the catalog).
    /// The DataTypes in `column_info` are used both for deserialisation and labeling.
    pub fn new(heap_manager: HeapManager, column_info: Vec<ColumnInfo>) -> Self {
        let schema_types: Vec<DataType> = column_info.iter().map(|c| c.data_type.clone()).collect();
        let identity: Vec<usize> = (0..column_info.len()).collect();
        let scan_iter = heap_manager.scan();
        Self {
            heap_manager,
            scan_iter,
            column_info,
            decoder: crate::types::RowDecoder::new(&schema_types),
            schema_types,
            column_mapping: identity,
            exhausted: false,
        }
    }

    /// Create a new sequential scan operator with explicit column mapping.
    ///
    /// `schema_types` — DataTypes in PHYSICAL column order (for deserialisation).
    /// `column_info` — Column metadata with VIEW column names.
    /// `column_mapping` — Maps each view position (0..column_info.len()) to the
    ///    corresponding physical column index in `schema_types`.
    ///
    /// After deserialisation using `schema_types`, values are reordered according
    /// to `column_mapping` so that each view column gets the correct data value.
    pub fn new_with_mapping(
        heap_manager: HeapManager,
        schema_types: Vec<DataType>,
        column_info: Vec<ColumnInfo>,
        column_mapping: Vec<usize>,
    ) -> Self {
        let scan_iter = heap_manager.scan();
        Self {
            heap_manager,
            scan_iter,
            column_info,
            decoder: crate::types::RowDecoder::new(&schema_types),
            schema_types,
            column_mapping,
            exhausted: false,
        }
    }

    /// Decode only the columns at these PHYSICAL positions; the others are
    /// produced as NULL. For a query that reads only some of the columns.
    pub fn read_only(mut self, physical_columns: impl IntoIterator<Item = usize>) -> Self {
        self.decoder = self.decoder.only(physical_columns);
        self
    }
}

use crate::backend::error::{RookError, RookResult};

impl PhysicalOperator for SeqScanOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if self.exhausted {
            return Ok(None);
        }

        match self.scan_iter.next_ref() {
            Some(Ok((page_id, slot_id, raw_bytes))) => {
                // Deserialise the raw tuple bytes using PHYSICAL column schema
                let phys_values = self.decoder.decode(raw_bytes).map_err(|e| {
                    RookError::Internal(format!("Failed to deserialise tuple: {}", e))
                })?;

                // Reorder values from physical order to view column order
                let values: Vec<Option<DataValue>> = if self.column_mapping.len()
                    == phys_values.len()
                    && self
                        .column_mapping
                        .iter()
                        .enumerate()
                        .all(|(i, &idx)| idx == i)
                {
                    // Identity mapping — skip reordering for efficiency
                    phys_values
                } else {
                    self.column_mapping
                        .iter()
                        .map(|&phys_idx| phys_values.get(phys_idx).cloned().unwrap_or(None))
                        .collect()
                };

                Ok(Some(Tuple::new_with_location(values, page_id, slot_id)))
            }
            Some(Err(e)) => Err(RookError::Io(e)),
            None => {
                self.exhausted = true;
                Ok(None)
            }
        }
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        batch.clear();
        if self.exhausted {
            return Ok(0);
        }

        let is_identity = self.column_mapping.len() == self.schema_types.len()
            && self
                .column_mapping
                .iter()
                .enumerate()
                .all(|(i, &idx)| idx == i);

        while batch.len() < super::trait_::DEFAULT_BATCH_SIZE {
            match self.scan_iter.next_ref() {
                Some(Ok((page_id, slot_id, raw_bytes))) => {
                    let phys_values = self.decoder.decode(raw_bytes).map_err(|e| {
                        RookError::Internal(format!("Failed to deserialise tuple: {}", e))
                    })?;

                    let values = if is_identity {
                        phys_values
                    } else {
                        self.column_mapping
                            .iter()
                            .map(|&phys_idx| phys_values.get(phys_idx).cloned().unwrap_or(None))
                            .collect()
                    };

                    batch.push(Tuple::new_with_location(values, page_id, slot_id));
                }
                Some(Err(e)) => return Err(RookError::Io(e)),
                None => {
                    self.exhausted = true;
                    break;
                }
            }
        }

        Ok(batch.len())
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.column_info
    }

    fn reset(&mut self) -> RookResult<()> {
        self.scan_iter = self.heap_manager.scan();
        self.exhausted = false;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "SeqScan"
    }

    fn estimate_cardinality(&self) -> usize {
        self.heap_manager.page_count() as usize * 100
    }
}

// ── IndexScan Operator ────────────────────────────────────────────────

/// Mode of index scan (point lookup, range scan, or full scan).
#[derive(Debug, Clone)]
pub enum IndexScanMode {
    /// Exact point lookup: only tuples matching this key.
    PointLookup(DataValue),
    /// Exact lookup on a composite key: one value per key segment, all
    /// matched with equality.
    CompositePointLookup(Vec<DataValue>),
    /// Range scan: all tuples with keys in `[low, high]` inclusive.
    RangeLookup(DataValue, DataValue),
    /// Full scan: scan all entries in the index via the leaf linked list.
    /// This replaces a SeqScan when an index is available.
    FullScan,
}

/// An index scan operator that uses a B+ Tree index for efficient lookups.
///
/// # Point lookup mode
/// Searches the B+ Tree for an exact key match, retrieves the heap tuple
/// identified by the returned `(page_id, slot_id)`, and yields it.
///
/// # Range scan mode
/// Searches the B+ Tree for all keys in `[low, high]`, retrieves each
/// heap tuple, and yields them in index order.
///
/// In both modes, the operator materializes all matching tuples on the
/// first `next()` call, then yields them one at a time.
pub struct IndexScanOperator {
    /// B+ Tree index for key lookups, when this scan owns its handle.
    btree: Option<BTree>,
    /// Engine, index file and key types, when lookups go through the
    /// engine's cached handle.
    cached_index: Option<(
        std::sync::Arc<crate::backend::engine::Engine>,
        PathBuf,
        Vec<DataType>,
    )>,
    /// Heap manager for fetching actual tuple data from (page_id, slot_id).
    heap_manager: HeapManager,
    /// The scan mode (point lookup or range scan).
    mode: IndexScanMode,
    /// Column metadata for the full table schema.
    column_info: Vec<ColumnInfo>,
    /// Column indices in `column_info` that form the index key.
    pub indexed_cols: Vec<usize>,
    /// Decodes the heap rows, set up once for the scan.
    decoder: crate::types::RowDecoder,
    /// Materialized list of heap tuple locations from the index.
    results: Vec<(u32, u32)>,
    /// Position in results of the next location to fetch.
    pos: usize,
    /// Rows fetched and not yet returned, in index order.
    ready: std::collections::VecDeque<Tuple>,
    /// Whether the index has been queried and results loaded.
    loaded: bool,
}

impl IndexScanOperator {
    /// Create a new index scan operator.
    ///
    /// `btree` is an opened B+ Tree index.
    /// `heap_manager` provides access to the heap file for fetching tuple data.
    /// `mode` specifies point lookup or range scan.
    /// `column_info` contains the full table schema (used for output tuple deserialization).
    /// `indexed_cols` specifies the column indices of the index keys in `column_info`.
    pub fn new(
        btree: BTree,
        heap_manager: HeapManager,
        mode: IndexScanMode,
        column_info: Vec<ColumnInfo>,
        indexed_cols: Vec<usize>,
    ) -> Self {
        let schema_types: Vec<DataType> = column_info.iter().map(|c| c.data_type.clone()).collect();
        Self {
            btree: Some(btree),
            cached_index: None,
            heap_manager,
            mode,
            column_info,
            decoder: crate::types::RowDecoder::new(&schema_types),
            indexed_cols,
            results: Vec::new(),
            pos: 0,
            ready: std::collections::VecDeque::new(),
            loaded: false,
        }
    }

    /// Create an index scan that looks keys up through the process-wide
    /// cached handle for `idx_path` (see `cache::with_btree`) instead of
    /// opening the index file for this one scan.
    ///
    /// The cached handle is the one DML maintains, so the scan reads the same
    /// pages DML writes and reuses pages already cached by earlier queries.
    pub fn with_cached_index(
        engine: std::sync::Arc<crate::backend::engine::Engine>,
        idx_path: PathBuf,
        key_types: Vec<DataType>,
        heap_manager: HeapManager,
        mode: IndexScanMode,
        column_info: Vec<ColumnInfo>,
        indexed_cols: Vec<usize>,
    ) -> Self {
        let schema_types: Vec<DataType> = column_info.iter().map(|c| c.data_type.clone()).collect();
        Self {
            btree: None,
            cached_index: Some((engine, idx_path, key_types)),
            heap_manager,
            mode,
            column_info,
            decoder: crate::types::RowDecoder::new(&schema_types),
            indexed_cols,
            results: Vec::new(),
            pos: 0,
            ready: std::collections::VecDeque::new(),
            loaded: false,
        }
    }

    /// Run this scan's lookup against `btree`.
    fn lookup(mode: &IndexScanMode, btree: &mut BTree) -> io::Result<Vec<(u32, u32)>> {
        match mode {
            // search_range with the same key for both bounds retrieves ALL
            // matching entries, not just the first one found by search().
            // This handles non-unique indexes where multiple rows share the
            // same key value (e.g., multiple employees in the same department).
            IndexScanMode::PointLookup(key) => btree.search_range(key, key),
            IndexScanMode::CompositePointLookup(keys) => btree.search_range_keys(keys, keys),
            IndexScanMode::RangeLookup(low, high) => btree.search_range(low, high),
            IndexScanMode::FullScan => btree.scan_all(),
        }
    }

    /// Look the keys up in the owned tree, or in the cached handle.
    fn search_index(&mut self) -> io::Result<Vec<(u32, u32)>> {
        if self.btree.is_none()
            && let Some((engine, path, key_types)) = &self.cached_index
        {
            let open = || -> io::Result<BTree> {
                let mut btree = BTree::open(path.clone())?;
                btree.set_key_types(key_types.clone());
                Ok(btree)
            };
            let mode = &self.mode;
            let hit = crate::backend::cache::with_btree(engine, path, open, |btree| {
                // A handle cached with other key types would compare keys
                // differently; leave it alone and use a private one.
                if btree.key_types() != key_types.as_slice() {
                    return Ok(None);
                }
                Self::lookup(mode, btree).map(Some)
            })?;
            if let Some(tids) = hit {
                return Ok(tids);
            }
            self.btree = Some(open()?);
        }
        let btree = self
            .btree
            .as_mut()
            .ok_or_else(|| io::Error::other("index scan has no index handle"))?;
        Self::lookup(&self.mode, btree)
    }

    /// Query the B+ Tree and materialize the matching heap tuple locations.
    fn load_results(&mut self) -> RookResult<()> {
        let what = match &self.mode {
            IndexScanMode::PointLookup(_) => "point lookup",
            IndexScanMode::CompositePointLookup(_) => "composite point lookup",
            IndexScanMode::RangeLookup(..) => "range lookup",
            IndexScanMode::FullScan => "full scan",
        };
        let tids = self
            .search_index()
            .map_err(|e| RookError::Internal(format!("Index scan {} error: {}", what, e)))?;

        if matches!(self.mode, IndexScanMode::FullScan) {
            let mut tid_set: std::collections::HashSet<(u32, u32)> = tids.iter().cloned().collect();
            let mut all_results = tids;
            for (page_id, slot_id, _) in self.heap_manager.scan().flatten() {
                if !tid_set.contains(&(page_id, slot_id)) {
                    all_results.push((page_id, slot_id));
                    tid_set.insert((page_id, slot_id));
                }
            }
            self.results = all_results;
        } else {
            self.results = tids;
        }
        self.loaded = true;
        Ok(())
    }

    /// Fetch the next batch of index matches into `ready`.
    ///
    /// The batch's locations are visited in page order, so each heap page is
    /// fetched from the pool once however many of its rows match; the rows
    /// are then returned in index order. Each row carries its heap location
    /// so DML can tell which physical row to modify.
    fn fill(&mut self) -> RookResult<()> {
        let end = (self.pos + super::trait_::DEFAULT_BATCH_SIZE).min(self.results.len());
        let batch = &self.results[self.pos..end];
        let mut by_page: Vec<usize> = (0..batch.len()).collect();
        by_page.sort_unstable_by_key(|&i| batch[i]);

        let mut rows: Vec<Option<Tuple>> = (0..batch.len()).map(|_| None).collect();
        let mut slots = Vec::new();
        let mut start = 0;
        while start < by_page.len() {
            let page_id = batch[by_page[start]].0;
            let mut stop = start;
            slots.clear();
            while stop < by_page.len() && batch[by_page[stop]].0 == page_id {
                slots.push(batch[by_page[stop]].1);
                stop += 1;
            }
            let positions = &by_page[start..stop];
            let decoder = &self.decoder;
            let mut next = 0;
            self.heap_manager
                .for_tuples_on_page(page_id, &slots, |slot_id, bytes| {
                    let values = decoder
                        .decode(bytes)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    rows[positions[next]] =
                        Some(Tuple::new_with_location(values, page_id, slot_id));
                    next += 1;
                    Ok(())
                })
                .map_err(|e| {
                    RookError::Internal(format!(
                        "Failed to fetch heap tuples on page {}: {}",
                        page_id, e
                    ))
                })?;
            start = stop;
        }
        self.pos = end;
        self.ready.extend(rows.into_iter().flatten());
        Ok(())
    }

    /// Load the index matches on first use, and fetch the next batch of rows
    /// when the fetched ones are used up.
    fn ensure_ready(&mut self) -> RookResult<()> {
        if !self.loaded {
            self.load_results()?;
        }
        if self.ready.is_empty() && self.pos < self.results.len() {
            self.fill()?;
        }
        Ok(())
    }
}

impl PhysicalOperator for IndexScanOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        self.ensure_ready()?;
        Ok(self.ready.pop_front())
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        batch.clear();
        self.ensure_ready()?;
        batch.extend(self.ready.drain(..));
        Ok(batch.len())
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.column_info
    }

    fn reset(&mut self) -> RookResult<()> {
        self.results.clear();
        self.pos = 0;
        self.ready.clear();
        self.loaded = false;
        Ok(())
    }

    fn name(&self) -> &'static str {
        match self.mode {
            IndexScanMode::PointLookup(_) => "IndexScan(Point)",
            IndexScanMode::CompositePointLookup(_) => "IndexScan(CompositePoint)",
            IndexScanMode::RangeLookup(..) => "IndexScan(Range)",
            IndexScanMode::FullScan => "IndexScan(Full)",
        }
    }

    fn ordering(&self) -> Option<Vec<(usize, bool)>> {
        match &self.mode {
            IndexScanMode::RangeLookup(..) => {
                if self.indexed_cols.is_empty() {
                    None
                } else {
                    Some(self.indexed_cols.iter().map(|&c| (c, false)).collect())
                }
            }
            IndexScanMode::PointLookup(_) | IndexScanMode::CompositePointLookup(_) => {
                if self.indexed_cols.is_empty() {
                    None
                } else {
                    Some(self.indexed_cols.iter().map(|&c| (c, false)).collect())
                }
            }
            IndexScanMode::FullScan => None,
        }
    }
}
