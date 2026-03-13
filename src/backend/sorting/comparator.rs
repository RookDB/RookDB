//! Tuple comparison logic for sorting operations.
//! Supports INT (4 bytes, i32 LE) and TEXT (10 bytes, fixed-width, lexicographic).

use std::cmp::Ordering;

use crate::catalog::types::{Column, SortDirection, SortKey};

/// Provides comparison functionality for tuples based on sort keys.
pub struct TupleComparator {
    /// Full schema (needed for offset calculation)
    pub columns: Vec<Column>,
    /// Which columns to compare, in priority order
    pub sort_keys: Vec<SortKey>,
    /// Precomputed total tuple size in bytes
    pub tuple_size: usize,
    /// Precomputed byte offset of each column within a tuple
    pub column_offsets: Vec<usize>,
}

impl TupleComparator {
    /// Create a new comparator from schema and sort keys.
    ///
    /// Precomputes column byte offsets for efficient repeated comparisons.
    pub fn new(columns: Vec<Column>, sort_keys: Vec<SortKey>) -> Self {
        let mut column_offsets = Vec::with_capacity(columns.len());
        let mut offset = 0usize;

        for col in &columns {
            column_offsets.push(offset);
            offset += column_byte_size(&col.data_type);
        }

        let tuple_size = offset;

        Self {
            columns,
            sort_keys,
            tuple_size,
            column_offsets,
        }
    }

    /// Compare two tuple byte slices according to the sort keys.
    ///
    /// Returns `Ordering::Less`, `Equal`, or `Greater`.
    /// Iterates through sort keys in priority order; the first non-equal
    /// comparison determines the result.
    pub fn compare(&self, tuple_a: &[u8], tuple_b: &[u8]) -> Ordering {
        for sort_key in &self.sort_keys {
            let col_idx = sort_key.column_index as usize;
            let col_offset = self.column_offsets[col_idx];
            let col = &self.columns[col_idx];

            let cmp = compare_column_values(
                &col.data_type,
                &tuple_a[col_offset..],
                &tuple_b[col_offset..],
            );

            if cmp != Ordering::Equal {
                return match sort_key.direction {
                    SortDirection::Ascending => cmp,
                    SortDirection::Descending => cmp.reverse(),
                };
            }
        }
        Ordering::Equal
    }

    /// Compare a specific column of a tuple against a raw key value.
    ///
    /// Used for range scan boundary checks.
    pub fn compare_key(&self, tuple: &[u8], key_index: usize, key_value: &[u8]) -> Ordering {
        let col_idx = self.sort_keys[key_index].column_index as usize;
        let col_offset = self.column_offsets[col_idx];
        let col = &self.columns[col_idx];

        let cmp = compare_column_values(&col.data_type, &tuple[col_offset..], key_value);

        match self.sort_keys[key_index].direction {
            SortDirection::Ascending => cmp,
            SortDirection::Descending => cmp.reverse(),
        }
    }

    /// Extract the sort key bytes from a tuple for the given sort key index.
    pub fn extract_key(&self, tuple: &[u8], key_index: usize) -> Vec<u8> {
        let col_idx = self.sort_keys[key_index].column_index as usize;
        let col_offset = self.column_offsets[col_idx];
        let col_size = column_byte_size(&self.columns[col_idx].data_type);
        tuple[col_offset..col_offset + col_size].to_vec()
    }
}

/// Returns the byte size of a column given its data type string.
pub fn column_byte_size(data_type: &str) -> usize {
    match data_type {
        "INT" => 4,
        "TEXT" => 10,
        _ => panic!("Unsupported data type: {}", data_type),
    }
}

/// Compare two column values given their data type.
///
/// The slices must start at the column offset; only the relevant bytes are read.
fn compare_column_values(data_type: &str, a: &[u8], b: &[u8]) -> Ordering {
    match data_type {
        "INT" => {
            let val_a = i32::from_le_bytes(a[0..4].try_into().unwrap());
            let val_b = i32::from_le_bytes(b[0..4].try_into().unwrap());
            val_a.cmp(&val_b)
        }
        "TEXT" => {
            let text_a = &a[0..10];
            let text_b = &b[0..10];
            text_a.cmp(text_b)
        }
        _ => Ordering::Equal,
    }
}
