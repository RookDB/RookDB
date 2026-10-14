//! Per-row NULL tracking using a compact bitmap.
//!
//! Each row stores one `NullBitmap` whose bits correspond to **logical** column
//! indices (column 0 → bit 0 of byte 0, column 1 → bit 1 of byte 0, etc.).
//!
//! The bitmap occupies `ceil(column_count / 8)` bytes. A `1` bit means the
//! column is NULL; a `0` bit means it has a value present in the row's data
//! region. The bitmap is always written first in the serialized row, immediately
//! after the 4-byte header.

/// Quickly check whether a specific column is NULL from raw row bytes,
/// without performing a full deserialisation.
use crate::backend::error::{RookError, RookResult};

///
/// Row layout: `[4B header][null_bitmap][var-len offset table][fixed data][var-len data]`
/// - Header: u16 num_cols, u16 num_varlen
/// - Null bitmap: ceil(num_cols / 8) bytes, starting at offset 4
///
/// Returns `Ok(true)` if the column is NULL, `Ok(false)` if non-NULL,
/// or `Err` if the row is too short to contain the bitmap.
pub fn is_column_null_in_row(row_bytes: &[u8], column_index: usize) -> RookResult<bool> {
    if row_bytes.len() < 4 {
        return Err(RookError::Internal(
            "Row too short to contain header".to_string(),
        ));
    }
    let num_cols = u16::from_le_bytes([row_bytes[0], row_bytes[1]]) as usize;
    if column_index >= num_cols {
        return Err(RookError::Internal(format!(
            "Column index {} out of bounds (num_cols={})",
            column_index, num_cols
        )));
    }
    let bm_size = num_cols.div_ceil(8);
    if row_bytes.len() < 4 + bm_size {
        return Err(RookError::Internal(format!(
            "Row too short to contain null bitmap: need {} bytes, got {}",
            4 + bm_size,
            row_bytes.len()
        )));
    }
    let byte_idx = column_index / 8;
    let bit_idx = column_index % 8;
    Ok((row_bytes[4 + byte_idx] & (1 << bit_idx)) != 0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NullBitmap {
    /// Total number of logical columns this bitmap covers.
    column_count: usize,
    /// Packed bit data; length is `ceil(column_count / 8)`.
    data: Vec<u8>,
}

impl NullBitmap {
    /// Create a new all-non-NULL bitmap for `column_count` columns.
    /// All bits are initialised to `0` (non-NULL).
    pub fn new(column_count: usize) -> Self {
        let byte_len = column_count.div_ceil(8);
        Self {
            column_count,
            data: vec![0u8; byte_len],
        }
    }

    /// Reconstruct a `NullBitmap` from raw bytes previously written by
    /// [`as_bytes`](Self::as_bytes).
    ///
    /// Returns an error if `raw.len()` does not equal `ceil(column_count / 8)`.
    pub fn from_bytes(column_count: usize, raw: &[u8]) -> RookResult<Self> {
        let expected = column_count.div_ceil(8);
        if raw.len() != expected {
            return Err(RookError::Internal(format!(
                "Invalid NULL bitmap length: expected {}, found {}",
                expected,
                raw.len()
            )));
        }
        Ok(Self {
            column_count,
            data: raw.to_vec(),
        })
    }

    /// Mark logical column `column_index` as NULL (sets its bit to `1`).
    pub fn set_null(&mut self, column_index: usize) {
        assert!(
            column_index < self.column_count,
            "column index out of range"
        );
        let byte_idx = column_index / 8;
        let bit_idx = column_index % 8;
        self.data[byte_idx] |= 1 << bit_idx;
    }

    /// Mark logical column `column_index` as non-NULL (clears its bit to `0`).
    pub fn clear_null(&mut self, column_index: usize) {
        assert!(
            column_index < self.column_count,
            "column index out of range"
        );
        let byte_idx = column_index / 8;
        let bit_idx = column_index % 8;
        self.data[byte_idx] &= !(1 << bit_idx);
    }

    /// Returns `true` if logical column `column_index` is NULL.
    pub fn is_null(&self, column_index: usize) -> bool {
        assert!(
            column_index < self.column_count,
            "column index out of range"
        );
        let byte_idx = column_index / 8;
        let bit_idx = column_index % 8;
        (self.data[byte_idx] & (1 << bit_idx)) != 0
    }

    /// Return the raw bitmap bytes, suitable for writing directly into a
    /// serialized row after the 4-byte header.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}
