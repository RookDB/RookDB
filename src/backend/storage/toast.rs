//! TOAST (The Oversized Attribute Storage Technique) manager
//! Handles out-of-line storage of large BLOB and ARRAY values

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
}

impl ToastManager {
    pub fn new() -> Self {
        ToastManager {
            next_value_id: 1,
            toast_page_count: 0,
        }
    }

    /// Store a large payload in TOAST, returning a pointer
    pub fn store_large_value(&mut self, payload: &[u8]) -> Result<ToastPointer, String> {
        let value_id = self.next_value_id;
        self.next_value_id += 1;

        let total_bytes = payload.len() as u32;
        let chunk_count = (payload.len() + TOAST_CHUNK_SIZE - 1) / TOAST_CHUNK_SIZE;

        // Split payload into chunks
        let mut chunk_no = 0;
        for chunk_data in payload.chunks(TOAST_CHUNK_SIZE) {
            let _chunk = ToastChunk::new(value_id, chunk_no, chunk_data.to_vec());
            // In a real implementation, these chunks would be written to a TOAST file
            chunk_no += 1;
        }

        Ok(ToastPointer::new(value_id, total_bytes, chunk_count as u32))
    }

    /// Read a large value from TOAST using a pointer
    pub fn read_large_value(
        &self,
        _toast_file: &mut std::fs::File,
        ptr: &ToastPointer,
    ) -> Result<Vec<u8>, String> {
        let mut result = Vec::with_capacity(ptr.total_bytes as usize);

        // In this simplified implementation, we would read from the TOAST file
        // For now, we return a placeholder
        result.resize(ptr.total_bytes as usize, 0);

        Ok(result)
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
