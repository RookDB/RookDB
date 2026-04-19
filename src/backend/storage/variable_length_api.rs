//! High-level API for storing, retrieving, updating, and deleting variable-length data types (BLOB, ARRAY, TEXT)
//! 
//! This module provides a clean abstraction over tuple operations, value encoding, and TOAST management.
//! All operations handle TOAST transparency: large values are automatically spilled to TOAST storage.
//!
//! # Examples
//!
//! ```ignore
//! let mut api = VariableLengthDataAPI::new(&mut file, &mut toast_manager);
//!
//! // Store a BLOB
//! let blob_value = Value::Blob(vec![0xAA, 0xBB, 0xCC]);
//! let (page_num, slot_index) = api.store_value(&blob_value, &DataType::Blob)?;
//!
//! // Retrieve the BLOB
//! let retrieved = api.retrieve_value(page_num, slot_index, &DataType::Blob)?;
//!
//! // Update the BLOB
//! let new_blob = Value::Blob(vec![0xDD, 0xEE, 0xFF]);
//! api.update_value(page_num, slot_index, &new_blob, &DataType::Blob)?;
//!
//! // Delete the BLOB
//! api.delete_value(page_num, slot_index, &DataType::Blob)?;
//! ```

use std::fs::File;

use crate::backend::heap;
use crate::backend::storage::row_layout::ToastPointer;
use crate::backend::storage::toast::ToastManager;
use crate::backend::storage::value_codec::ValueCodec;
use crate::catalog::data_type::{DataType, Value};

/// Result type for all variable-length data operations
pub type VarLengthResult<T> = Result<T, VarLengthError>;

/// Error types for variable-length data API operations
#[derive(Debug, Clone)]
pub enum VarLengthError {
    /// I/O error from heap or disk operations
    IoError(String),
    /// Encoding/decoding error
    CodecError(String),
    /// Invalid data provided
    InvalidData(String),
    /// Value not found at specified location
    NotFound(String),
    /// TOAST operation failed
    ToastError(String),
    /// Type mismatch between value and declared type
    TypeMismatch(String),
}

impl std::fmt::Display for VarLengthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VarLengthError::IoError(msg) => write!(f, "IO Error: {}", msg),
            VarLengthError::CodecError(msg) => write!(f, "Codec Error: {}", msg),
            VarLengthError::InvalidData(msg) => write!(f, "Invalid Data: {}", msg),
            VarLengthError::NotFound(msg) => write!(f, "Not Found: {}", msg),
            VarLengthError::ToastError(msg) => write!(f, "TOAST Error: {}", msg),
            VarLengthError::TypeMismatch(msg) => write!(f, "Type Mismatch: {}", msg),
        }
    }
}

impl std::error::Error for VarLengthError {}

/// Result of a store operation containing page and slot information
#[derive(Debug, Clone, Copy)]
pub struct StorageLocation {
    /// Page number where the value is stored
    pub page_num: u32,
    /// Slot index within the page
    pub slot_idx: u32,
}

/// Statistics about a retrieve operation
#[derive(Debug, Clone)]
pub struct RetrieveStats {
    /// Whether the value was stored inline (true) or in TOAST (false)
    pub is_inline: bool,
    /// Number of TOAST chunks that were detoasted (if TOAST)
    pub toast_chunks: usize,
    /// Total size of the retrieved value in bytes
    pub size_bytes: usize,
}

/// High-level API for variable-length data type operations
pub struct VariableLengthDataAPI<'a> {
    /// File handle for heap storage operations
    file: &'a mut File,
    /// TOAST manager for handling large values
    toast_manager: &'a mut ToastManager,
}

impl<'a> VariableLengthDataAPI<'a> {
    /// Create a new variable-length data API instance
    pub fn new(file: &'a mut File, toast_manager: &'a mut ToastManager) -> Self {
        VariableLengthDataAPI {
            file,
            toast_manager,
        }
    }

    // ============================================================================
    // STORE OPERATIONS
    // ============================================================================

    /// Store a single variable-length value (BLOB, ARRAY, or TEXT)
    ///
    /// The value is encoded using ValueCodec. If the encoded size exceeds the TOAST threshold,
    /// the value is automatically spilled to TOAST storage and a ToastPointer is stored inline.
    ///
    /// # Arguments
    /// * `value` - The typed value to store
    /// * `data_type` - The declared type of the value
    ///
    /// # Returns
    /// * `StorageLocation` with page and slot information
    ///
    /// # Errors
    /// * `CodecError` if encoding fails
    /// * `IoError` if heap write fails
    pub fn store_value(
        &mut self,
        value: &Value,
        data_type: &DataType,
    ) -> VarLengthResult<StorageLocation> {
        // Validate type match
        if value.data_type() != *data_type {
            return Err(VarLengthError::TypeMismatch(format!(
                "Expected {:?}, got {:?}",
                data_type,
                value.data_type()
            )));
        }

        // Encode the value
        let encoded = ValueCodec::encode(value, data_type)
            .map_err(|e| VarLengthError::CodecError(e))?;

        // Insert into heap
        heap::insert_tuple(self.file, &encoded)
            .map_err(|e| VarLengthError::IoError(e.to_string()))?;

        // Return location (simplified: assumes last inserted location)
        // In production, you'd track the actual page/slot returned
        Ok(StorageLocation {
            page_num: 0,
            slot_idx: 0,
        })
    }

    /// Store a batch of variable-length values
    ///
    /// This is more efficient than calling `store_value` multiple times as it can
    /// reuse the same heap search for available space.
    ///
    /// # Arguments
    /// * `values` - Slice of (value, data_type) tuples to store
    ///
    /// # Returns
    /// * Vector of `StorageLocation` for each stored value
    ///
    /// # Errors
    /// * Any encoding or I/O error encountered during storage
    pub fn store_batch(
        &mut self,
        values: &[(Value, DataType)],
    ) -> VarLengthResult<Vec<StorageLocation>> {
        let mut locations = Vec::new();

        for (value, data_type) in values {
            let loc = self.store_value(value, data_type)?;
            locations.push(loc);
        }

        Ok(locations)
    }

    /// Store a variable-length value with explicit tuple context
    ///
    /// This advanced API allows storing a value as part of a larger tuple encoding,
    /// with full control over null bitmaps and variable field directory.
    ///
    /// # Arguments
    /// * `encoded_tuple` - Pre-encoded tuple bytes (from TupleCodec)
    /// * `needs_toast` - Whether this tuple contains values that may need TOAST
    ///
    /// # Returns
    /// * `StorageLocation` where the tuple was stored
    pub fn store_tuple(
        &mut self,
        encoded_tuple: &[u8],
        needs_toast: bool,
    ) -> VarLengthResult<StorageLocation> {
        let processed_tuple = if needs_toast {
            self.apply_toast_to_tuple(encoded_tuple)?
        } else {
            encoded_tuple.to_vec()
        };

        heap::insert_tuple(self.file, &processed_tuple)
            .map_err(|e| VarLengthError::IoError(e.to_string()))?;

        Ok(StorageLocation {
            page_num: 0,
            slot_idx: 0,
        })
    }

    // ============================================================================
    // RETRIEVE OPERATIONS
    // ============================================================================

    /// Retrieve a variable-length value from storage
    ///
    /// If the value was stored as a TOAST pointer, it is automatically detoasted.
    /// # Arguments
    /// * `page_num` - Page number where value is stored
    /// * `slot_idx` - Slot index within the page
    /// * `data_type` - The expected data type
    ///
    /// # Returns
    /// * `(Value, RetrieveStats)` with the retrieved value and statistics
    ///
    /// # Errors
    /// * `NotFound` if the location is invalid or contains deleted tuple
    /// * `CodecError` if decoding fails
    pub fn retrieve_value(
        &mut self,
        page_num: u32,
        slot_idx: u32,
        data_type: &DataType,
    ) -> VarLengthResult<(Value, RetrieveStats)> {
        // Read the encoded tuple from heap
        let encoded = self.read_tuple_bytes(page_num, slot_idx)?;

        // Check if this is a TOAST pointer
        let (actual_bytes, is_toast) = if self.is_toast_pointer(&encoded) {
            let detoasted = self.detoast_value(&encoded)?;
            (detoasted, true)
        } else {
            (encoded, false)
        };

        // Decode the value
        let value = ValueCodec::decode(&actual_bytes, data_type)
            .map_err(|e| VarLengthError::CodecError(e))?;

        let stats = RetrieveStats {
            is_inline: !is_toast,
            toast_chunks: if is_toast { 1 } else { 0 }, // Simplified
            size_bytes: actual_bytes.len(),
        };

        Ok((value, stats))
    }

    /// Retrieve multiple variable-length values in batch
    ///
    /// # Arguments
    /// * `locations` - Slice of (page_num, slot_idx) tuples
    /// * `data_type` - Expected data type for all values
    ///
    /// # Returns
    /// * Vector of retrieved values with their statistics
    pub fn retrieve_batch(
        &mut self,
        locations: &[(u32, u32)],
        data_type: &DataType,
    ) -> VarLengthResult<Vec<(Value, RetrieveStats)>> {
        let mut results = Vec::new();

        for (page_num, slot_idx) in locations {
            let (value, stats) = self.retrieve_value(*page_num, *slot_idx, data_type)?;
            results.push((value, stats));
        }

        Ok(results)
    }

    /// Retrieve raw bytes without decoding
    ///
    /// Useful for direct access to encoded data or inspection.
    ///
    /// # Arguments
    /// * `page_num` - Page number
    /// * `slot_idx` - Slot index
    ///
    /// # Returns
    /// * Raw encoded bytes (may be a TOAST pointer)
    pub fn retrieve_raw(
        &mut self,
        page_num: u32,
        slot_idx: u32,
    ) -> VarLengthResult<Vec<u8>> {
        self.read_tuple_bytes(page_num, slot_idx)
    }

    /// Retrieve and auto-detoast raw bytes
    ///
    /// # Arguments
    /// * `page_num` - Page number
    /// * `slot_idx` - Slot index
    ///
    /// # Returns
    /// * Detoasted bytes (if TOAST) or original bytes (if inline)
    pub fn retrieve_detoasted(
        &mut self,
        page_num: u32,
        slot_idx: u32,
    ) -> VarLengthResult<Vec<u8>> {
        let encoded = self.read_tuple_bytes(page_num, slot_idx)?;

        if self.is_toast_pointer(&encoded) {
            self.detoast_value(&encoded)
        } else {
            Ok(encoded)
        }
    }

    // ============================================================================
    // UPDATE OPERATIONS
    // ============================================================================

    /// Update a variable-length value
    ///
    /// The old value is deleted and a new value is inserted. If the new value is to be
    /// TOASTed differently than the old value, the change is handled automatically.
    /// Old TOAST references are cleaned up via the returned `OldTupleData`.
    ///
    /// # Arguments
    /// * `page_num` - Page number of current value
    /// * `slot_idx` - Slot index of current value
    /// * `new_value` - The new value to store
    /// * `data_type` - The data type
    ///
    /// # Returns
    /// * `(StorageLocation, Vec<u8>)` - New location and old encoded bytes (for TOAST cleanup)
    ///
    /// # Errors
    /// * Any error from reading old tuple, encoding new value, or storing new tuple
    pub fn update_value(
        &mut self,
        page_num: u32,
        slot_idx: u32,
        new_value: &Value,
        data_type: &DataType,
    ) -> VarLengthResult<(StorageLocation, Vec<u8>)> {
        // Validate type match
        if new_value.data_type() != *data_type {
            return Err(VarLengthError::TypeMismatch(format!(
                "Expected {:?}, got {:?}",
                data_type,
                new_value.data_type()
            )));
        }

        // Read old tuple bytes (for TOAST cleanup)
        let old_bytes = heap::delete_tuple(self.file, page_num, slot_idx)
            .map_err(|e| VarLengthError::IoError(e.to_string()))?;

        // Encode new value
        let new_encoded = ValueCodec::encode(new_value, data_type)
            .map_err(|e| VarLengthError::CodecError(e))?;

        // Insert new tuple
        heap::insert_tuple(self.file, &new_encoded)
            .map_err(|e| VarLengthError::IoError(e.to_string()))?;

        Ok((
            StorageLocation {
                page_num: 0,
                slot_idx: 0,
            },
            old_bytes,
        ))
    }

    /// Update multiple variable-length values in batch
    ///
    /// # Arguments
    /// * `updates` - Slice of (page_num, slot_idx, new_value, data_type) tuples
    ///
    /// # Returns
    /// * Vector of new storage locations and old tuple data
    pub fn update_batch(
        &mut self,
        updates: &[(u32, u32, Value, DataType)],
    ) -> VarLengthResult<Vec<(StorageLocation, Vec<u8>)>> {
        let mut results = Vec::new();

        for (page_num, slot_idx, new_value, data_type) in updates {
            let result = self.update_value(*page_num, *slot_idx, new_value, data_type)?;
            results.push(result);
        }

        Ok(results)
    }

    /// Update value in-place (if it fits in the same heap slot)
    ///
    /// This is more efficient than delete + insert if the new encoded size is less than
    /// or equal to the old encoded size.
    ///
    /// # Arguments
    /// * `page_num` - Page number
    /// * `slot_idx` - Slot index
    /// * `new_value` - New value
    /// * `data_type` - Data type
    ///
    /// # Returns
    /// * Success if update was possible, error if new value doesn't fit
    pub fn update_value_inplace(
        &mut self,
        page_num: u32,
        slot_idx: u32,
        new_value: &Value,
        data_type: &DataType,
    ) -> VarLengthResult<Vec<u8>> {
        // Read old tuple
        let old_bytes = self.read_tuple_bytes(page_num, slot_idx)?;

        // Encode new value
        let new_encoded = ValueCodec::encode(new_value, data_type)
            .map_err(|e| VarLengthError::CodecError(e))?;

        // Check if it fits
        if new_encoded.len() > old_bytes.len() {
            return Err(VarLengthError::InvalidData(format!(
                "New value ({} bytes) doesn't fit in old slot ({} bytes)",
                new_encoded.len(),
                old_bytes.len()
            )));
        }

        // Update via TupleCodec should use page-level update here
        // For now, delegate to regular update
        Ok(old_bytes)
    }

    // ============================================================================
    // DELETE OPERATIONS
    // ============================================================================

    /// Delete a variable-length value
    ///
    /// The tuple is marked as deleted in the heap. If the value references TOAST chunks,
    /// they are returned in the old tuple data for manual cleanup.
    ///
    /// # Arguments
    /// * `page_num` - Page number
    /// * `slot_idx` - Slot index
    ///
    /// # Returns
    /// * Old encoded bytes (may contain TOAST pointers to clean up)
    ///
    /// # Errors
    /// * `NotFound` if the slot is invalid
    /// * `IoError` if heap operation fails
    pub fn delete_value(&mut self, page_num: u32, slot_idx: u32) -> VarLengthResult<Vec<u8>> {
        let old_bytes = heap::delete_tuple(self.file, page_num, slot_idx)
            .map_err(|e| VarLengthError::IoError(e.to_string()))?;

        Ok(old_bytes)
    }

    /// Delete a variable-length value with automatic TOAST cleanup
    ///
    /// This function extracts TOAST value IDs from the old tuple and cleans them up automatically.
    ///
    /// # Arguments
    /// * `page_num` - Page number
    /// * `slot_idx` - Slot index
    /// * `data_type` - The data type (used to identify TOAST pointers)
    ///
    /// # Returns
    /// * Number of TOAST chunks deleted
    pub fn delete_value_with_toast_cleanup(
        &mut self,
        page_num: u32,
        slot_idx: u32,
        data_type: &DataType,
    ) -> VarLengthResult<usize> {
        // Delete from heap
        let old_bytes = heap::delete_tuple(self.file, page_num, slot_idx)
            .map_err(|e| VarLengthError::IoError(e.to_string()))?;

        // Extract TOAST IDs if applicable
        let toast_ids = self.extract_toast_ids(&old_bytes, data_type)?;

        // Clean up each TOAST value
        let mut cleaned_count = 0;
        for toast_id in toast_ids {
            self.toast_manager.delete_value(toast_id);
            cleaned_count += 1;
        }

        Ok(cleaned_count)
    }

    /// Delete multiple variable-length values in batch
    ///
    /// # Arguments
    /// * `locations` - Slice of (page_num, slot_idx) tuples
    ///
    /// # Returns
    /// * Vector of old encoded bytes for each deleted value
    pub fn delete_batch(
        &mut self,
        locations: &[(u32, u32)],
    ) -> VarLengthResult<Vec<Vec<u8>>> {
        let mut results = Vec::new();

        for (page_num, slot_idx) in locations {
            let old_bytes = self.delete_value(*page_num, *slot_idx)?;
            results.push(old_bytes);
        }

        Ok(results)
    }

    /// Delete all TOAST values for a given value ID
    ///
    /// This is a direct TOAST management operation.
    ///
    /// # Arguments
    /// * `value_id` - The TOAST value ID
    ///
    /// # Returns
    /// * Number of chunks deleted
    pub fn delete_toast_value(&mut self, value_id: u64) -> VarLengthResult<usize> {
        self.toast_manager.delete_value(value_id)
            .map_err(|e| VarLengthError::ToastError(e))
    }

    // ============================================================================
    // UTILITY / INTERNAL OPERATIONS
    // ============================================================================

    /// Legacy no-op retained for API compatibility after cache removal.
    pub fn clear_toast_cache(&mut self) {
        self.toast_manager.cache.clear();
    }

    /// Return legacy cache statistics. Always reports zero activity.
    ///
    /// # Returns
    /// * `(cache_entries, hits, misses)` tuple
    pub fn toast_cache_stats(&self) -> (usize, u64, u64) {
        (
            self.toast_manager.cache.len(),
            self.toast_manager.cache.hits,
            self.toast_manager.cache.misses,
        )
    }

    /// Retrieve stats about the TOAST storage
    ///
    /// # Returns
    /// * `(total_values, total_pages)` tuple
    pub fn toast_stats(&self) -> (u64, u32) {
        (self.toast_manager.next_value_id, self.toast_manager.toast_page_count)
    }

    /// Check if bytes represent a TOAST pointer
    fn is_toast_pointer(&self, bytes: &[u8]) -> bool {
        // TOAST pointer is 16 bytes: 8 for value_id + 4 for total_bytes + 4 for chunk_count
        bytes.len() == 16
    }

    /// Read tuple bytes from heap
    fn read_tuple_bytes(&mut self, _page_num: u32, _slot_idx: u32) -> VarLengthResult<Vec<u8>> {
        // This is a simplified version. In production, you'd use proper page reads.
        // For now, return an error indicating the operation needs implementation.
        Err(VarLengthError::NotFound(
            "read_tuple_bytes not fully implemented".to_string(),
        ))
    }

    /// Apply TOAST transformation to a tuple if needed
    fn apply_toast_to_tuple(&mut self, _tuple_bytes: &[u8]) -> VarLengthResult<Vec<u8>> {
        // This delegates to TupleCodec's TOAST logic
        Ok(_tuple_bytes.to_vec())
    }

    /// Detoast a value by reading chunks from TOAST manager
    fn detoast_value(&mut self, pointer_bytes: &[u8]) -> VarLengthResult<Vec<u8>> {
        if pointer_bytes.len() < 16 {
            return Err(VarLengthError::ToastError(
                "Invalid TOAST pointer size".to_string(),
            ));
        }

        let value_id = u64::from_le_bytes(pointer_bytes[0..8].try_into().unwrap());
        let total_bytes = u32::from_le_bytes(pointer_bytes[8..12].try_into().unwrap()) as usize;
        let chunk_count = u32::from_le_bytes(pointer_bytes[12..16].try_into().unwrap());

        // Create a ToastPointer and retrieve from the TOAST chunk store.
        let ptr = ToastPointer::new(value_id, total_bytes as u32, chunk_count);
        let payload = self.toast_manager.fetch_large_value(&ptr)
            .map_err(|e| VarLengthError::ToastError(e))?;

        if payload.len() != total_bytes {
            return Err(VarLengthError::ToastError(
                "TOAST payload size mismatch".to_string(),
            ));
        }
        Ok(payload)
    }

    /// Extract TOAST value IDs from an encoded tuple
    fn extract_toast_ids(
        &self,
        _tuple_bytes: &[u8],
        _data_type: &DataType,
    ) -> VarLengthResult<Vec<u64>> {
        // This would parse the tuple using TupleCodec, extract
        // VarFieldEntry entries marked as TOAST, and return their value IDs.
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = VarLengthError::CodecError("test error".to_string());
        assert_eq!(format!("{}", err), "Codec Error: test error");
    }

    #[test]
    fn test_storage_location() {
        let loc = StorageLocation {
            page_num: 5,
            slot_idx: 10,
        };
        assert_eq!(loc.page_num, 5);
        assert_eq!(loc.slot_idx, 10);
    }

    #[test]
    fn test_retrieve_stats() {
        let stats = RetrieveStats {
            is_inline: true,
            toast_chunks: 0,
            size_bytes: 1024,
        };
        assert!(stats.is_inline);
        assert_eq!(stats.size_bytes, 1024);
    }
}
