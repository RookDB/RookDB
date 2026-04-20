//! Scan operations on ordered files.
//!
//! Provides:
//! - `OrderedScanIterator`: sequential scan of all tuples in sort order
//! - `RangeScanIterator`: range-based scan with binary search seek
//! - `ordered_scan()`: convenience function to collect all tuples
//! - `range_scan()`: convenience function to collect tuples in a range

use std::cmp::Ordering;
use std::fs::File;
use std::io;

use crate::catalog::types::{Catalog, Column, SortDirection, SortKey};
use crate::disk::read_page;
use crate::ordered::delta_store::scan_all_delta_tuples;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::sorting::comparator::TupleComparator;
use crate::table::page_count;

/// Iterator for sequential scanning of all tuples in an ordered file (in sort order).
pub struct OrderedScanIterator {
    pub current_page: u32,
    pub current_slot: u32,
    pub total_pages: u32,
    pub is_exhausted: bool,
    page_buffer: Page,
    num_items_on_page: u32,
    page_loaded: bool,
}

impl OrderedScanIterator {
    /// Create a new ordered scan iterator starting from the first data page.
    pub fn new(total_pages: u32) -> Self {
        Self {
            current_page: 1,
            current_slot: 0,
            total_pages,
            is_exhausted: total_pages <= 1, // no data pages
            page_buffer: Page::new(),
            num_items_on_page: 0,
            page_loaded: false,
        }
    }

    /// Return the next tuple in sorted order, or None if all tuples consumed.
    pub fn next(&mut self, file: &mut File) -> io::Result<Option<Vec<u8>>> {
        loop {
            if self.is_exhausted {
                return Ok(None);
            }

            // Load page if needed
            if !self.page_loaded {
                read_page(file, &mut self.page_buffer, self.current_page)?;
                let lower = u32::from_le_bytes(self.page_buffer.data[0..4].try_into().unwrap());
                self.num_items_on_page = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
                self.current_slot = 0;
                self.page_loaded = true;

                // Skip empty pages via loop continuation
                if self.num_items_on_page == 0 {
                    self.current_page += 1;
                    if self.current_page >= self.total_pages {
                        self.is_exhausted = true;
                        return Ok(None);
                    }
                    self.page_loaded = false;
                    continue;
                }
            }

            if self.current_slot >= self.num_items_on_page {
                // Advance to next page via loop continuation
                self.current_page += 1;
                if self.current_page >= self.total_pages {
                    self.is_exhausted = true;
                    return Ok(None);
                }
                self.page_loaded = false;
                continue;
            }

            // Read tuple at current slot
            let base = (PAGE_HEADER_SIZE + self.current_slot * ITEM_ID_SIZE) as usize;
            let offset =
                u32::from_le_bytes(self.page_buffer.data[base..base + 4].try_into().unwrap())
                    as usize;
            let length = u32::from_le_bytes(
                self.page_buffer.data[base + 4..base + 8]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let tuple = self.page_buffer.data[offset..offset + length].to_vec();

            self.current_slot += 1;
            return Ok(Some(tuple));
        }
    }
}

/// Iterator for range-based scanning of an ordered file.
pub struct RangeScanIterator {
    pub key_column_index: u32,
    pub start_key: Option<Vec<u8>>,
    pub end_key: Option<Vec<u8>>,
    pub current_page: u32,
    pub current_slot: u32,
    pub comparator: TupleComparator,
    pub is_exhausted: bool,
    pub total_pages: u32,
    page_buffer: Page,
    num_items_on_page: u32,
    page_loaded: bool,
}

impl RangeScanIterator {
    /// Create a new range scan iterator.
    pub fn new(
        key_column_index: u32,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
        columns: Vec<Column>,
        sort_keys: Vec<SortKey>,
        total_pages: u32,
    ) -> Self {
        let comparator = TupleComparator::new(columns, sort_keys);
        Self {
            key_column_index,
            start_key,
            end_key,
            current_page: 1,
            current_slot: 0,
            comparator,
            is_exhausted: total_pages <= 1,
            total_pages,
            page_buffer: Page::new(),
            num_items_on_page: 0,
            page_loaded: false,
        }
    }

    /// Initialize the iterator: find the starting page and slot via binary search.
    pub fn seek_start(&mut self, file: &mut File) -> io::Result<()> {
        if self.is_exhausted {
            return Ok(());
        }

        match &self.start_key {
            None => {
                // Unbounded start: begin at page 1, slot 0
                self.current_page = 1;
                self.current_slot = 0;
            }
            Some(start) => {
                // Binary search across pages to find start page
                let mut low: u32 = 1;
                let mut high: u32 = self.total_pages - 1;

                while low < high {
                    let mid = low + (high - low) / 2;
                    let mut page = Page::new();
                    read_page(file, &mut page, mid)?;

                    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
                    let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

                    if num_items == 0 {
                        low = mid + 1;
                        continue;
                    }

                    // Read last tuple on this page
                    let last_base = (PAGE_HEADER_SIZE + (num_items - 1) * ITEM_ID_SIZE) as usize;
                    let last_offset =
                        u32::from_le_bytes(page.data[last_base..last_base + 4].try_into().unwrap())
                            as usize;
                    let last_length = u32::from_le_bytes(
                        page.data[last_base + 4..last_base + 8].try_into().unwrap(),
                    ) as usize;
                    let last_tuple = &page.data[last_offset..last_offset + last_length];

                    let cmp = self.comparator.compare_key(last_tuple, 0, start);
                    if cmp == Ordering::Less {
                        low = mid + 1;
                    } else {
                        high = mid;
                    }
                }

                self.current_page = low;

                // Binary search within page for first slot >= start_key
                read_page(file, &mut self.page_buffer, self.current_page)?;
                let lower = u32::from_le_bytes(self.page_buffer.data[0..4].try_into().unwrap());
                self.num_items_on_page = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
                self.page_loaded = true;

                let lo: u32 = 0;
                let hi: u32 = self.num_items_on_page;
                let mut result = hi;

                let mut lo_search = lo;
                let mut hi_search = hi;
                while lo_search < hi_search {
                    let mid = lo_search + (hi_search - lo_search) / 2;
                    let base = (PAGE_HEADER_SIZE + mid * ITEM_ID_SIZE) as usize;
                    let offset = u32::from_le_bytes(
                        self.page_buffer.data[base..base + 4].try_into().unwrap(),
                    ) as usize;
                    let length = u32::from_le_bytes(
                        self.page_buffer.data[base + 4..base + 8]
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    let tuple = &self.page_buffer.data[offset..offset + length];

                    let cmp = self.comparator.compare_key(tuple, 0, start);
                    if cmp == Ordering::Less {
                        lo_search = mid + 1;
                    } else {
                        result = mid;
                        hi_search = mid;
                    }
                }
                // If no tuple on this page >= start_key, result stays at hi
                if result == hi {
                    // No tuple on this page >= start_key, move to next page
                    self.current_page += 1;
                    self.page_loaded = false;
                    self.current_slot = 0;
                } else {
                    self.current_slot = result;
                }
            }
        }

        if self.current_page >= self.total_pages {
            self.is_exhausted = true;
        }

        Ok(())
    }

    /// Return the next tuple in the range, or None if scan is complete.
    pub fn next(&mut self, file: &mut File) -> io::Result<Option<Vec<u8>>> {
        loop {
            if self.is_exhausted {
                return Ok(None);
            }

            // Load page if needed
            if !self.page_loaded {
                if self.current_page >= self.total_pages {
                    self.is_exhausted = true;
                    return Ok(None);
                }
                read_page(file, &mut self.page_buffer, self.current_page)?;
                let lower = u32::from_le_bytes(self.page_buffer.data[0..4].try_into().unwrap());
                self.num_items_on_page = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
                self.current_slot = 0;
                self.page_loaded = true;
            }

            if self.current_slot >= self.num_items_on_page {
                // Move to next page via loop continuation
                self.current_page += 1;
                self.page_loaded = false;
                continue;
            }

            // Read tuple at current slot
            let base = (PAGE_HEADER_SIZE + self.current_slot * ITEM_ID_SIZE) as usize;
            let offset =
                u32::from_le_bytes(self.page_buffer.data[base..base + 4].try_into().unwrap())
                    as usize;
            let length = u32::from_le_bytes(
                self.page_buffer.data[base + 4..base + 8]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let tuple = self.page_buffer.data[offset..offset + length].to_vec();

            // Check end boundary
            if let Some(end) = &self.end_key {
                let cmp = self.comparator.compare_key(&tuple, 0, end);
                if cmp == Ordering::Greater {
                    self.is_exhausted = true;
                    return Ok(None);
                }
            }

            self.current_slot += 1;
            return Ok(Some(tuple));
        }
    }
}

/// Scans all tuples from an ordered file in sorted order.
pub fn ordered_scan(
    file: &mut File,
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
) -> io::Result<Vec<Vec<u8>>> {
    let total_pages = page_count(file)?;
    let mut iter = OrderedScanIterator::new(total_pages);
    let mut results = Vec::new();

    while let Some(tuple) = iter.next(file)? {
        results.push(tuple);
    }

    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Database '{}' not found", db_name),
        )
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Table '{}' not found", table_name),
        )
    })?;

    if table.file_type.as_deref() == Some("ordered") {
        let mut delta_tuples = scan_all_delta_tuples(db_name, table_name)?;
        if !delta_tuples.is_empty() {
            results.append(&mut delta_tuples);

            let sort_keys = table.sort_keys.as_ref().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Ordered table missing sort keys",
                )
            })?;
            let comparator = TupleComparator::new(table.columns.clone(), sort_keys.clone());
            results.sort_by(|a, b| comparator.compare(a, b));
        }
    }

    Ok(results)
}

/// Scans an ordered file and returns all tuples whose sort key falls
/// within the specified range.
pub fn range_scan(
    file: &mut File,
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    key_column_name: &str,
    start_value: Option<&str>,
    end_value: Option<&str>,
) -> io::Result<Vec<Vec<u8>>> {
    // Look up schema and sort keys
    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Database '{}' not found", db_name),
        )
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Table '{}' not found", table_name),
        )
    })?;
    let columns = &table.columns;

    // Find the key column index
    let key_col_idx = columns
        .iter()
        .position(|c| c.name == key_column_name)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Column '{}' not found", key_column_name),
            )
        })?;

    let sort_keys = table.sort_keys.as_ref().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "Table is not an ordered file")
    })?;

    // Validate that the range column is the leading sort key
    if sort_keys.is_empty() || sort_keys[0].column_index != key_col_idx as u32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Column '{}' is not the leading sort key. Range scan is only efficient on the leading sort key.",
                key_column_name
            ),
        ));
    }

    let col_type = &columns[key_col_idx].data_type;

    // Serialize start/end values and normalize to natural [low, high] order.
    let mut start_key_raw = start_value.map(|v| serialize_value(v, col_type));
    let mut end_key_raw = end_value.map(|v| serialize_value(v, col_type));

    if let (Some(start), Some(end)) = (&start_key_raw, &end_key_raw) {
        if compare_serialized_values(start, end, col_type) == Ordering::Greater {
            std::mem::swap(&mut start_key_raw, &mut end_key_raw);
        }
    }

    // Convert natural bounds to comparator-space bounds.
    // Ascending: start=low, end=high
    // Descending: start=high, end=low
    let (effective_start_key, effective_end_key) = match sort_keys[0].direction {
        SortDirection::Ascending => (start_key_raw.clone(), end_key_raw.clone()),
        SortDirection::Descending => (end_key_raw.clone(), start_key_raw.clone()),
    };

    let total_pages = page_count(file)?;
    let comparator = TupleComparator::new(columns.clone(), sort_keys.clone());

    let mut iter = RangeScanIterator::new(
        key_col_idx as u32,
        effective_start_key.clone(),
        effective_end_key.clone(),
        columns.clone(),
        sort_keys.clone(),
        total_pages,
    );

    iter.seek_start(file)?;

    let mut results = Vec::new();
    while let Some(tuple) = iter.next(file)? {
        results.push(tuple);
    }

    let mut delta_tuples = scan_all_delta_tuples(db_name, table_name)?;
    if !delta_tuples.is_empty() {
        delta_tuples.retain(|tuple| {
            let ge_start = effective_start_key
                .as_ref()
                .map(|start| comparator.compare_key(tuple, 0, start) != Ordering::Less)
                .unwrap_or(true);
            let le_end = effective_end_key
                .as_ref()
                .map(|end| comparator.compare_key(tuple, 0, end) != Ordering::Greater)
                .unwrap_or(true);
            ge_start && le_end
        });

        results.append(&mut delta_tuples);
        results.sort_by(|a, b| comparator.compare(a, b));
    }

    Ok(results)
}

/// Serialize a string value to bytes matching the column data type.
fn serialize_value(value: &str, data_type: &str) -> Vec<u8> {
    match data_type {
        "INT" => {
            let num: i32 = value.parse().unwrap_or(0);
            num.to_le_bytes().to_vec()
        }
        "TEXT" => {
            let mut bytes = value.as_bytes().to_vec();
            if bytes.len() > 10 {
                bytes.truncate(10);
            } else if bytes.len() < 10 {
                bytes.extend(vec![b' '; 10 - bytes.len()]);
            }
            bytes
        }
        _ => Vec::new(),
    }
}

fn compare_serialized_values(a: &[u8], b: &[u8], data_type: &str) -> Ordering {
    match data_type {
        "INT" => {
            if a.len() < 4 || b.len() < 4 {
                Ordering::Equal
            } else {
                let av = i32::from_le_bytes(a[0..4].try_into().unwrap());
                let bv = i32::from_le_bytes(b[0..4].try_into().unwrap());
                av.cmp(&bv)
            }
        }
        "TEXT" => a.cmp(b),
        _ => Ordering::Equal,
    }
}
