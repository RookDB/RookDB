
use crate::backend::heap::heap_manager::{HeapManager, HeapScanIterator};
use crate::backend::index::btree::BTree;

use super::super::tuple::{Tuple, ColumnInfo};
use super::trait_::PhysicalOperator;

use crate::types::value::DataValue;
use crate::types::datatype::DataType;

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
    pub fn new(
        heap_manager: HeapManager,
        column_info: Vec<ColumnInfo>,
    ) -> Self {
        let schema_types: Vec<DataType> = column_info.iter().map(|c| c.data_type.clone()).collect();
        let identity: Vec<usize> = (0..column_info.len()).collect();
        let scan_iter = heap_manager.scan();
        Self {
            heap_manager,
            scan_iter,
            column_info,
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
            schema_types,
            column_mapping,
            exhausted: false,
        }
    }
}

use crate::backend::error::{RookError, RookResult};

impl PhysicalOperator for SeqScanOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if self.exhausted {
            return Ok(None);
        }

        match self.scan_iter.next() {
            Some(Ok((page_id, slot_id, raw_bytes))) => {
                // Deserialise the raw tuple bytes using PHYSICAL column schema
                let phys_values = crate::types::deserialize_nullable_row(&self.schema_types, &raw_bytes)
                    .map_err(|e| RookError::Internal(format!("Failed to deserialise tuple: {}", e)))?;

                // Reorder values from physical order to view column order
                let values: Vec<Option<DataValue>> = if self.column_mapping.len() == phys_values.len()
                    && self.column_mapping.iter().enumerate().all(|(i, &idx)| idx == i)
                {
                    // Identity mapping — skip reordering for efficiency
                    phys_values
                } else {
                    self.column_mapping.iter()
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
            && self.column_mapping.iter().enumerate().all(|(i, &idx)| idx == i);

        while batch.len() < super::trait_::DEFAULT_BATCH_SIZE {
            match self.scan_iter.next() {
                Some(Ok((page_id, slot_id, raw_bytes))) => {
                    let phys_values = crate::types::deserialize_nullable_row(&self.schema_types, &raw_bytes)
                        .map_err(|e| RookError::Internal(format!("Failed to deserialise tuple: {}", e)))?;

                    let values = if is_identity {
                        phys_values
                    } else {
                        self.column_mapping.iter()
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
        self.heap_manager.header.page_count as usize * 100
    }
}

// ── IndexScan Operator ────────────────────────────────────────────────

/// Mode of index scan (point lookup, range scan, or full scan).
#[derive(Debug, Clone)]
pub enum IndexScanMode {
    /// Exact point lookup: only tuples matching this key.
    PointLookup(DataValue),
    /// Exact lookup on a composite key: one value per key segment, all
    /// matched with equality (ANALYSIS.md Tier 2 #8).
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
    /// B+ Tree index for key lookups.
    btree: BTree,
    /// Heap manager for fetching actual tuple data from (page_id, slot_id).
    heap_manager: HeapManager,
    /// The scan mode (point lookup or range scan).
    mode: IndexScanMode,
    /// Column metadata for the full table schema.
    column_info: Vec<ColumnInfo>,
    /// Data types for deserialising raw tuple bytes.
    schema_types: Vec<DataType>,
    /// Materialized list of heap tuple locations from the index.
    results: Vec<(u32, u32)>,
    /// Current read position in results.
    pos: usize,
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
    pub fn new(
        btree: BTree,
        heap_manager: HeapManager,
        mode: IndexScanMode,
        column_info: Vec<ColumnInfo>,
    ) -> Self {
        let schema_types: Vec<DataType> = column_info.iter().map(|c| c.data_type.clone()).collect();
        Self {
            btree,
            heap_manager,
            mode,
            column_info,
            schema_types,
            results: Vec::new(),
            pos: 0,
            loaded: false,
        }
    }

    /// Query the B+ Tree and materialize the matching heap tuple locations.
    fn load_results(&mut self) -> RookResult<()> {
        match &self.mode {
            IndexScanMode::PointLookup(key) => {
                // Use search_range with the same key for both bounds to retrieve
                // ALL matching entries, not just the first one found by search().
                // This handles non-unique indexes where multiple rows share the
                // same key value (e.g., multiple employees in the same department).
                let tids = self.btree.search_range(key, key)
                    .map_err(|e| RookError::Internal(format!("Index scan point lookup error: {}", e)))?;
                self.results = tids;
            }
            IndexScanMode::CompositePointLookup(keys) => {
                let tids = self.btree.search_range_keys(keys, keys)
                    .map_err(|e| RookError::Internal(format!("Index scan composite point lookup error: {}", e)))?;
                self.results = tids;
            }
            IndexScanMode::RangeLookup(low, high) => {
                let tids = self.btree.search_range(low, high)
                    .map_err(|e| RookError::Internal(format!("Index scan range lookup error: {}", e)))?;
                self.results = tids;
            }
            IndexScanMode::FullScan => {
                let tids = self.btree.scan_all()
                    .map_err(|e| RookError::Internal(format!("Index scan full scan error: {}", e)))?;
                self.results = tids;
            }
        }
        self.loaded = true;
        Ok(())
    }

    /// Fetch a tuple from the heap by (page_id, slot_id) and deserialize it.
    /// Propagates the heap location metadata so DML operations (UPDATE/DELETE)
    /// can identify which physical row to modify.
    fn fetch_tuple(&mut self, page_id: u32, slot_id: u32) -> RookResult<Tuple> {
        let raw_bytes = self.heap_manager.get_tuple(page_id, slot_id)
            .map_err(|e| RookError::Internal(format!("Failed to fetch heap tuple (page={}, slot={}): {}", page_id, slot_id, e)))?;
        let values = crate::types::deserialize_nullable_row(&self.schema_types, &raw_bytes)
            .map_err(|e| RookError::Internal(format!("Failed to deserialise tuple: {}", e)))?;
        Ok(Tuple::new_with_location(values, page_id, slot_id))
    }
}

impl PhysicalOperator for IndexScanOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if !self.loaded {
            self.load_results()?;
        }

        if self.pos < self.results.len() {
            let (page_id, slot_id) = self.results[self.pos];
            self.pos += 1;
            let tuple = self.fetch_tuple(page_id, slot_id)?;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        batch.clear();
        if !self.loaded {
            self.load_results()?;
        }

        while self.pos < self.results.len() && batch.len() < super::trait_::DEFAULT_BATCH_SIZE {
            let (page_id, slot_id) = self.results[self.pos];
            self.pos += 1;
            let tuple = self.fetch_tuple(page_id, slot_id)?;
            batch.push(tuple);
        }

        Ok(batch.len())
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.column_info
    }

    fn reset(&mut self) -> RookResult<()> {
        self.results.clear();
        self.pos = 0;
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

    fn estimate_cardinality(&self) -> usize {
        match self.mode {
            IndexScanMode::PointLookup(_) | IndexScanMode::CompositePointLookup(_) => 1,
            IndexScanMode::RangeLookup(..) => (self.heap_manager.header.page_count as usize * 10).max(1),
            IndexScanMode::FullScan => self.heap_manager.header.page_count as usize * 100,
        }
    }
}
