//! TOAST (The Oversized Attribute Storage Technique) manager
//! Handles out-of-line storage of large BLOB and ARRAY values

use std::collections::HashMap;

use crate::backend::storage::row_layout::{ToastChunk, ToastPointer};

/// Default chunk size for TOAST storage (4 KB)
pub const TOAST_CHUNK_SIZE: usize = 4096;

/// Default threshold for moving values to TOAST storage (8 KB)
pub const TOAST_THRESHOLD: usize = 8192;

/// Manages TOAST storage for large values
pub struct ToastManager {
    /// Next available value ID
    pub next_value_id: u64,
    /// Number of pages used by TOAST table
    pub toast_page_count: u32,
    /// In-memory chunk store keyed by TOAST value ID
    chunks: HashMap<u64, Vec<ToastChunk>>,
}

impl ToastManager {
    pub fn new() -> Self {
        ToastManager {
            next_value_id: 1,
            toast_page_count: 0,
            chunks: HashMap::new(),
        }
    }

    /// Store a large payload in TOAST, returning a pointer
    pub fn store_large_value(&mut self, payload: &[u8]) -> Result<ToastPointer, String> {
        let value_id = self.next_value_id;
        self.next_value_id += 1;

        let total_bytes = payload.len() as u32;
        let chunk_count = (payload.len() + TOAST_CHUNK_SIZE - 1) / TOAST_CHUNK_SIZE;

        // Split payload into chunks
        let mut stored_chunks = Vec::with_capacity(chunk_count);
        let mut chunk_no = 0;
        for chunk_data in payload.chunks(TOAST_CHUNK_SIZE) {
            stored_chunks.push(ToastChunk::new(value_id, chunk_no, chunk_data.to_vec()));
            chunk_no += 1;
        }
        self.chunks.insert(value_id, stored_chunks);

        Ok(ToastPointer::new(value_id, total_bytes, chunk_count as u32))
    }

    /// Read a large value from the in-memory TOAST store using a pointer
    pub fn fetch_large_value(&self, ptr: &ToastPointer) -> Result<Vec<u8>, String> {
        let chunks = self
            .chunks
            .get(&ptr.value_id)
            .ok_or_else(|| format!("TOAST value {} not found", ptr.value_id))?;

        if chunks.len() != ptr.chunk_count as usize {
            return Err(format!(
                "TOAST chunk count mismatch for value {}: expected {}, found {}",
                ptr.value_id,
                ptr.chunk_count,
                chunks.len()
            ));
        }

        let mut ordered_chunks: Vec<&ToastChunk> = chunks.iter().collect();
        ordered_chunks.sort_by_key(|chunk| chunk.chunk_no);

        let mut result = Vec::with_capacity(ptr.total_bytes as usize);
        for (expected_chunk_no, chunk) in ordered_chunks.into_iter().enumerate() {
            if chunk.chunk_no as usize != expected_chunk_no {
                return Err(format!(
                    "Missing or out-of-order TOAST chunk {} for value {}",
                    expected_chunk_no,
                    ptr.value_id
                ));
            }
            result.extend_from_slice(&chunk.data);
        }

        if result.len() != ptr.total_bytes as usize {
            return Err(format!(
                "TOAST byte count mismatch for value {}: expected {}, found {}",
                ptr.value_id,
                ptr.total_bytes,
                result.len()
            ));
        }

        Ok(result)
    }

    /// Read a large value from TOAST using a pointer
    pub fn read_large_value(
        &self,
        _toast_file: &mut std::fs::File,
        ptr: &ToastPointer,
    ) -> Result<Vec<u8>, String> {
        self.fetch_large_value(ptr)
    }

    /// Check if a value should be stored in TOAST
    pub fn should_use_toast(value_size: usize) -> bool {
        value_size > TOAST_THRESHOLD
    }

    /// Serialize TOAST manager metadata
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12);
        bytes.extend_from_slice(&self.next_value_id.to_le_bytes());
        bytes.extend_from_slice(&self.toast_page_count.to_le_bytes());
        bytes
    }

    /// Deserialize TOAST manager metadata
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 12 {
            return Err("Insufficient bytes for TOAST manager metadata".to_string());
        }

        Ok(ToastManager {
            next_value_id: u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            toast_page_count: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            chunks: HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_toast_manager_creation() {
        let manager = ToastManager::new();
        assert_eq!(manager.next_value_id, 1);
        assert_eq!(manager.toast_page_count, 0);
    }

    #[test]
    fn test_store_large_value() {
        let mut manager = ToastManager::new();
        let payload = vec![1; 10000];
        let ptr = manager.store_large_value(&payload).unwrap();

        assert_eq!(ptr.value_id, 1);
        assert_eq!(ptr.total_bytes, 10000);
        assert!(ptr.chunk_count > 0);
        assert_eq!(manager.next_value_id, 2);
        assert_eq!(manager.fetch_large_value(&ptr).unwrap(), payload);
    }

    #[test]
    fn test_should_use_toast() {
        assert!(!ToastManager::should_use_toast(1000));
        assert!(ToastManager::should_use_toast(10000));
        assert!(ToastManager::should_use_toast(TOAST_THRESHOLD + 1));
    }

    #[test]
    fn test_toast_manager_serialization() {
        let mut manager = ToastManager::new();
        manager.next_value_id = 100;
        manager.toast_page_count = 5;

        let bytes = manager.to_bytes();
        let restored = ToastManager::from_bytes(&bytes).unwrap();

        assert_eq!(manager.next_value_id, restored.next_value_id);
        assert_eq!(manager.toast_page_count, restored.toast_page_count);
    }
}
