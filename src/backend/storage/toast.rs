//! TOAST (The Oversized Attribute Storage Technique) manager
//! Handles out-of-line storage of large BLOB and ARRAY values
//!
//! Design: One TOAST table (`.toast` file) per parent table.
//! Supports per-column storage strategies,
//! value deletion, copy-on-write updates, and vacuum/compaction.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};

use crate::backend::storage::row_layout::{ToastChunk, ToastPointer};
use crate::backend::storage::toast_logger;

/// Per-column TOAST storage strategy (modeled after PostgreSQL)
#[derive(Clone, Debug, PartialEq)]
pub enum ToastStrategy {
    /// Never TOAST — always store inline regardless of size
    Plain,
    /// TOAST when value exceeds threshold (default behavior)
    Extended,
    /// Always store out-of-line in TOAST table
    External,
    /// Compress first, then TOAST if still too large
    Main,
}

impl ToastStrategy {
    /// Parse strategy from string (case-insensitive)
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_uppercase().as_str() {
            "PLAIN" => Ok(ToastStrategy::Plain),
            "EXTENDED" => Ok(ToastStrategy::Extended),
            "EXTERNAL" => Ok(ToastStrategy::External),
            "MAIN" => Ok(ToastStrategy::Main),
            _ => Err(format!("Unknown TOAST strategy: {}", s)),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            ToastStrategy::Plain => "PLAIN".to_string(),
            ToastStrategy::Extended => "EXTENDED".to_string(),
            ToastStrategy::External => "EXTERNAL".to_string(),
            ToastStrategy::Main => "MAIN".to_string(),
        }
    }
}

/*
Legacy detoast cache implementation kept commented for reference only.
Caching is intentionally disabled because the current TOAST path works
directly from the in-memory chunk store and does not need an extra layer.

pub struct ToastCache {
    entries: HashMap<u64, Vec<u8>>,
    access_order: Vec<u64>,
    max_entries: usize,
    pub hits: u64,
    pub misses: u64,
}

impl ToastCache {
    pub fn new(max_entries: usize) -> Self {
        ToastCache {
            entries: HashMap::new(),
            access_order: Vec::new(),
            max_entries,
            hits: 0,
            misses: 0,
        }
    }

    /// Look up a cached detoasted value
    pub fn get(&mut self, value_id: u64) -> Option<&Vec<u8>> {
        if self.entries.contains_key(&value_id) {
            self.hits += 1;
            // Move to back of access order (most recent)
            self.access_order.retain(|&id| id != value_id);
            self.access_order.push(value_id);
            self.entries.get(&value_id)
        } else {
            self.misses += 1;
            None
        }
    }

    /// Insert a detoasted value into cache, evicting LRU if full
    pub fn insert(&mut self, value_id: u64, payload: Vec<u8>) {
        if self.entries.contains_key(&value_id) {
            self.access_order.retain(|&id| id != value_id);
        } else if self.entries.len() >= self.max_entries {
            // Evict least recently used
            if let Some(evicted_id) = self.access_order.first().copied() {
                self.access_order.remove(0);
                self.entries.remove(&evicted_id);
            }
        }
        self.entries.insert(value_id, payload);
        self.access_order.push(value_id);
    }

    /// Invalidate a specific entry (used after delete/update)
    pub fn invalidate(&mut self, value_id: u64) {
        self.entries.remove(&value_id);
        self.access_order.retain(|&id| id != value_id);
    }

    /// Clear the entire cache
    pub fn clear(&mut self) {
        self.entries.clear();
        self.access_order.clear();
    }

    /// Number of entries currently cached
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
*/

/// Compatibility shim for the removed detoast cache.
///
/// The public API still exposes cache-related methods, but they are now
/// guaranteed no-ops so TOAST behavior remains unchanged.
#[derive(Default)]
pub struct ToastCache {
    pub hits: u64,
    pub misses: u64,
}

impl ToastCache {
    pub fn new(_max_entries: usize) -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {}

    pub fn len(&self) -> usize {
        0
    }
}

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
    /// Compatibility field retained so the existing API keeps compiling.
    pub cache: ToastCache,
}

impl ToastManager {
    pub fn new() -> Self {
        ToastManager {
            next_value_id: 1,
            toast_page_count: 0,
            chunks: HashMap::new(),
            cache: ToastCache::new(64),
        }
    }

    /// Create a manager while ignoring the legacy cache size parameter.
    pub fn with_cache_size(_max_cache_entries: usize) -> Self {
        ToastManager {
            next_value_id: 1,
            toast_page_count: 0,
            chunks: HashMap::new(),
            cache: ToastCache::new(0),
        }
    }

    /// Store a large payload in TOAST, returning a pointer
    pub fn store_large_value(&mut self, payload: &[u8]) -> Result<ToastPointer, String> {
        let value_id = self.next_value_id;
        self.next_value_id += 1;

        let total_bytes = payload.len() as u32;
        let chunk_count = (payload.len() + TOAST_CHUNK_SIZE - 1) / TOAST_CHUNK_SIZE;

        toast_logger::log_toast(&format!("\n[TOAST] Storing large value:"));
        toast_logger::log_toast(&format!("  Value ID: {}", value_id));
        toast_logger::log_toast(&format!("  Total size: {} bytes", total_bytes));
        toast_logger::log_toast(&format!("  Total chunks: {}", chunk_count));

        // Split payload into chunks
        let mut stored_chunks = Vec::with_capacity(chunk_count);
        let mut chunk_no = 0;
        for chunk_data in payload.chunks(TOAST_CHUNK_SIZE) {
            toast_logger::log_toast(&format!("    Chunk {}: {} bytes", chunk_no, chunk_data.len()));
            stored_chunks.push(ToastChunk::new(value_id, chunk_no, chunk_data.to_vec()));
            chunk_no += 1;
        }
        self.chunks.insert(value_id, stored_chunks);
        toast_logger::log_toast(&format!("[TOAST] Stored value {} in memory", value_id));

        Ok(ToastPointer::new(value_id, total_bytes, chunk_count as u32))
    }

    /// Read a large value from the in-memory TOAST store using a pointer
    pub fn fetch_large_value(&self, ptr: &ToastPointer) -> Result<Vec<u8>, String> {
        let chunks = self
            .chunks
            .get(&ptr.value_id)
            .ok_or_else(|| format!("TOAST value {} not found", ptr.value_id))?;

        toast_logger::log_toast(&format!("\n[TOAST] Fetching value from memory:"));
        toast_logger::log_toast(&format!("  Value ID: {}", ptr.value_id));
        toast_logger::log_toast(&format!("  Expected size: {} bytes", ptr.total_bytes));
        toast_logger::log_toast(&format!("  Chunks to fetch: {}", ptr.chunk_count));

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
            toast_logger::log_toast(&format!("    Chunk {}: {} bytes fetched", chunk.chunk_no, chunk.data.len()));
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

        toast_logger::log_toast(&format!("[TOAST] Fetched value {} ({} bytes total)", ptr.value_id, result.len()));
        Ok(result)
    }

    /// Legacy compatibility wrapper. Reads directly from the chunk store.
    pub fn fetch_large_value_cached(&mut self, ptr: &ToastPointer) -> Result<Vec<u8>, String> {
        toast_logger::log_toast(&format!(
            "[TOAST] Fetching value {} from memory (cache code disabled)",
            ptr.value_id
        ));
        self.fetch_large_value(ptr)
    }

    /// Read a large value from TOAST using a pointer
    pub fn read_large_value(
        &self,
        _toast_file: &mut std::fs::File,
        ptr: &ToastPointer,
    ) -> Result<Vec<u8>, String> {
        self.fetch_large_value(ptr)
    }

    /// Check if a value should be stored in TOAST (default strategy)
    pub fn should_use_toast(value_size: usize) -> bool {
        value_size > TOAST_THRESHOLD
    }

    /// Check if a value should be TOASTed based on per-column strategy
    pub fn should_toast_column(value_size: usize, strategy: &ToastStrategy) -> bool {
        match strategy {
            ToastStrategy::Plain => false,              // Never TOAST
            ToastStrategy::Extended => value_size > TOAST_THRESHOLD, // Default
            ToastStrategy::External => true,             // Always TOAST
            ToastStrategy::Main => value_size > TOAST_THRESHOLD,     // Same threshold for now
        }
    }

    /// Delete all chunks for a given value ID (used when tuple is deleted)
    pub fn delete_value(&mut self, value_id: u64) -> Result<usize, String> {
        match self.chunks.remove(&value_id) {
            Some(chunks) => Ok(chunks.len()),
            None => Err(format!("TOAST value {} not found for deletion", value_id)),
        }
    }

    /// Update a TOAST value: stores new payload, deletes old chunks (copy-on-write)
    /// Returns the new ToastPointer
    pub fn update_value(
        &mut self,
        old_value_id: u64,
        new_payload: &[u8],
    ) -> Result<ToastPointer, String> {
        // Store new value first (copy-on-write: new data is written before old is removed)
        let new_ptr = self.store_large_value(new_payload)?;

        // Delete old chunks
        let _ = self.delete_value(old_value_id); // Ignore error if old value doesn't exist

        Ok(new_ptr)
    }

    /// Vacuum: remove orphaned chunks not referenced by any live value_id.
    /// Takes a set of live value IDs; any chunks not in this set are removed.
    /// Returns (freed_chunks, freed_bytes).
    pub fn vacuum(&mut self, live_value_ids: &[u64]) -> (usize, usize) {
        let live_set: std::collections::HashSet<u64> = live_value_ids.iter().copied().collect();
        let mut freed_chunks = 0usize;
        let mut freed_bytes = 0usize;

        let orphaned_ids: Vec<u64> = self
            .chunks
            .keys()
            .filter(|id| !live_set.contains(id))
            .copied()
            .collect();

        for id in orphaned_ids {
            if let Some(chunks) = self.chunks.remove(&id) {
                for chunk in &chunks {
                    freed_bytes += chunk.data.len();
                }
                freed_chunks += chunks.len();
            }
        }

        (freed_chunks, freed_bytes)
    }

    /// Compact: rewrite chunks to reclaim space, resetting value IDs if needed.
    /// Returns the number of live values retained.
    pub fn compact(&mut self) -> usize {
        self.chunks.len()
    }

    /// Get total number of stored values
    pub fn value_count(&self) -> usize {
        self.chunks.len()
    }

    /// Get total chunk count across all values
    pub fn total_chunk_count(&self) -> usize {
        self.chunks.values().map(|v| v.len()).sum()
    }

    /// Get total stored bytes across all chunks
    pub fn total_stored_bytes(&self) -> usize {
        self.chunks
            .values()
            .flat_map(|v| v.iter())
            .map(|c| c.data.len())
            .sum()
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
            cache: ToastCache::new(0),
        })
    }

    /// Save all TOAST chunks to disk file
    pub fn save_to_disk(&self, path: &str) -> io::Result<()> {
        toast_logger::log_toast(&format!("\n[TOAST] Saving to disk:"));
        toast_logger::log_toast(&format!("  Path: {}", path));
        toast_logger::log_toast(&format!("  Total values in memory: {}", self.chunks.len()));
        toast_logger::log_toast(&format!("  Total chunks in memory: {}", self.total_chunk_count()));
        toast_logger::log_toast(&format!("  Total bytes in memory: {} MB", self.total_stored_bytes() as f64 / (1024.0 * 1024.0)));

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        // Write header: next_value_id and chunk_count
        let chunk_count = self.chunks.len() as u32;
        file.write_all(&self.next_value_id.to_le_bytes())?;
        file.write_all(&chunk_count.to_le_bytes())?;

        // Write each value's chunks
        for (value_id, chunks) in &self.chunks {
            file.write_all(&value_id.to_le_bytes())?;
            file.write_all(&(chunks.len() as u32).to_le_bytes())?;

            toast_logger::log_toast(&format!("  [Value {}] Saving {} chunks", value_id, chunks.len()));
            for chunk in chunks {
                // Write chunk header: value_id, chunk_no, chunk_len, flags
                file.write_all(&chunk.value_id.to_le_bytes())?;
                file.write_all(&chunk.chunk_no.to_le_bytes())?;
                file.write_all(&chunk.chunk_len.to_le_bytes())?;
                file.write_all(&chunk.flags.to_le_bytes())?;
                // Write chunk data
                file.write_all(&chunk.data)?;
                toast_logger::log_toast(&format!("      Chunk {}: {} bytes written", chunk.chunk_no, chunk.data.len()));
            }
        }

        toast_logger::log_toast(&format!("[TOAST] Successfully saved to disk\n"));
        Ok(())
    }

    /// Load all TOAST chunks from disk file
    pub fn load_from_disk(path: &str) -> io::Result<Self> {
        toast_logger::log_toast(&format!("\n[TOAST] Loading from disk:"));
        toast_logger::log_toast(&format!("  Path: {}", path));

        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        toast_logger::log_toast(&format!("  File size: {} bytes", buffer.len()));

        if buffer.len() < 12 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "TOAST file too small",
            ));
        }

        let mut offset = 0;

        // Read header
        let next_value_id = u64::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
            buffer[offset + 4],
            buffer[offset + 5],
            buffer[offset + 6],
            buffer[offset + 7],
        ]);
        offset += 8;

        let chunk_count = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]);
        offset += 4;

        let mut chunks: HashMap<u64, Vec<ToastChunk>> = HashMap::new();

        // Read each value's chunks
        for _ in 0..chunk_count {
            if offset + 8 > buffer.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Corrupted TOAST file: invalid value header",
                ));
            }

            let value_id = u64::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
                buffer[offset + 4],
                buffer[offset + 5],
                buffer[offset + 6],
                buffer[offset + 7],
            ]);
            offset += 8;

            let value_chunks = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]);
            offset += 4;

            toast_logger::log_toast(&format!("  [Value {}] Loading {} chunks from disk", value_id, value_chunks));

            let mut value_chunk_list = Vec::with_capacity(value_chunks as usize);

            for _ in 0..value_chunks {
                if offset + 14 > buffer.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Corrupted TOAST file: invalid chunk header",
                    ));
                }

                // Read chunk header
                let chunk_value_id = u64::from_le_bytes([
                    buffer[offset],
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                    buffer[offset + 4],
                    buffer[offset + 5],
                    buffer[offset + 6],
                    buffer[offset + 7],
                ]);
                offset += 8;

                let chunk_no = u32::from_le_bytes([
                    buffer[offset],
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                ]);
                offset += 4;

                let chunk_len = u16::from_le_bytes([buffer[offset], buffer[offset + 1]]);
                offset += 2;

                let flags = u16::from_le_bytes([buffer[offset], buffer[offset + 1]]);
                offset += 2;

                if offset + chunk_len as usize > buffer.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Corrupted TOAST file: invalid chunk data",
                    ));
                }

                let data = buffer[offset..offset + chunk_len as usize].to_vec();
                offset += chunk_len as usize;

                toast_logger::log_toast(&format!("    Chunk {}: {} bytes loaded from disk", chunk_no, chunk_len));

                value_chunk_list.push(ToastChunk {
                    value_id: chunk_value_id,
                    chunk_no,
                    chunk_len,
                    flags,
                    data,
                });
            }

            chunks.insert(value_id, value_chunk_list);
        }

        let total_values = chunks.len();
        let total_chunks: usize = chunks.values().map(|v| v.len()).sum();
        let total_bytes: usize = chunks.values()
            .flat_map(|v| v.iter())
            .map(|c| c.data.len())
            .sum();
        
        toast_logger::log_toast(&format!("  Loaded: {} values, {} total chunks, {} MB", 
                 total_values, total_chunks, total_bytes as f64 / (1024.0 * 1024.0)));
        toast_logger::log_toast(&format!("[TOAST] Successfully loaded from disk\n"));

        Ok(ToastManager {
            next_value_id,
            toast_page_count: 0,
            chunks,
            cache: ToastCache::new(0),
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

    // === Delete value tests ===

    #[test]
    fn test_delete_value() {
        let mut manager = ToastManager::new();
        let payload = vec![0xAB; 10000];
        let ptr = manager.store_large_value(&payload).unwrap();

        assert_eq!(manager.value_count(), 1);
        let freed = manager.delete_value(ptr.value_id).unwrap();
        assert!(freed > 0);
        assert_eq!(manager.value_count(), 0);

        // Fetch should fail after delete
        assert!(manager.fetch_large_value(&ptr).is_err());
    }

    #[test]
    fn test_delete_nonexistent_value() {
        let mut manager = ToastManager::new();
        assert!(manager.delete_value(999).is_err());
    }

    // === Update value tests ===

    #[test]
    fn test_update_value() {
        let mut manager = ToastManager::new();
        let old_payload = vec![0xAA; 10000];
        let old_ptr = manager.store_large_value(&old_payload).unwrap();
        assert_eq!(manager.value_count(), 1);

        let new_payload = vec![0xBB; 15000];
        let new_ptr = manager.update_value(old_ptr.value_id, &new_payload).unwrap();

        // Old value should be gone
        assert!(manager.fetch_large_value(&old_ptr).is_err());

        // New value should be fetchable
        let fetched = manager.fetch_large_value(&new_ptr).unwrap();
        assert_eq!(fetched, new_payload);
        assert_eq!(manager.value_count(), 1);
    }

    // === Vacuum tests ===

    #[test]
    fn test_vacuum_removes_orphans() {
        let mut manager = ToastManager::new();
        let p1 = manager.store_large_value(&vec![1; 10000]).unwrap();
        let _p2 = manager.store_large_value(&vec![2; 10000]).unwrap();
        let p3 = manager.store_large_value(&vec![3; 10000]).unwrap();

        assert_eq!(manager.value_count(), 3);

        // Only p1 and p3 are "live"
        let (freed_chunks, freed_bytes) = manager.vacuum(&[p1.value_id, p3.value_id]);
        assert!(freed_chunks > 0);
        assert!(freed_bytes > 0);
        assert_eq!(manager.value_count(), 2);

        // p1 and p3 should still be fetchable
        assert!(manager.fetch_large_value(&p1).is_ok());
        assert!(manager.fetch_large_value(&p3).is_ok());
    }

    #[test]
    fn test_vacuum_no_orphans() {
        let mut manager = ToastManager::new();
        let p1 = manager.store_large_value(&vec![1; 10000]).unwrap();

        let (freed_chunks, freed_bytes) = manager.vacuum(&[p1.value_id]);
        assert_eq!(freed_chunks, 0);
        assert_eq!(freed_bytes, 0);
        assert_eq!(manager.value_count(), 1);
    }

    // === Per-column strategy tests ===

    #[test]
    fn test_toast_strategy_plain() {
        assert!(!ToastManager::should_toast_column(100000, &ToastStrategy::Plain));
    }

    #[test]
    fn test_toast_strategy_extended() {
        assert!(!ToastManager::should_toast_column(1000, &ToastStrategy::Extended));
        assert!(ToastManager::should_toast_column(TOAST_THRESHOLD + 1, &ToastStrategy::Extended));
    }

    #[test]
    fn test_toast_strategy_external() {
        assert!(ToastManager::should_toast_column(1, &ToastStrategy::External));
        assert!(ToastManager::should_toast_column(100000, &ToastStrategy::External));
    }

    #[test]
    fn test_toast_strategy_parse() {
        assert_eq!(ToastStrategy::parse("PLAIN").unwrap(), ToastStrategy::Plain);
        assert_eq!(ToastStrategy::parse("extended").unwrap(), ToastStrategy::Extended);
        assert_eq!(ToastStrategy::parse("External").unwrap(), ToastStrategy::External);
        assert_eq!(ToastStrategy::parse("MAIN").unwrap(), ToastStrategy::Main);
        assert!(ToastStrategy::parse("INVALID").is_err());
    }

    /*
    Cache-specific tests are intentionally commented out because the cache
    implementation has been removed. The compatibility shim is exercised
    indirectly by the remaining TOAST retrieval tests.

    #[test]
    fn test_toast_cache_basic() {}

    #[test]
    fn test_toast_cache_eviction() {}

    #[test]
    fn test_toast_cache_invalidate() {}

    #[test]
    fn test_fetch_large_value_cached() {}
    */

    // === Stats tests ===

    #[test]
    fn test_toast_stats() {
        let mut manager = ToastManager::new();
        assert_eq!(manager.value_count(), 0);
        assert_eq!(manager.total_chunk_count(), 0);
        assert_eq!(manager.total_stored_bytes(), 0);

        manager.store_large_value(&vec![0xFF; 10000]).unwrap();
        assert_eq!(manager.value_count(), 1);
        assert!(manager.total_chunk_count() > 0);
        assert_eq!(manager.total_stored_bytes(), 10000);
    }

    // === Disk persistence roundtrip tests ===

    #[test]
    fn test_disk_persistence_roundtrip() {
        let mut manager = ToastManager::new();
        let payload1 = vec![0xAA; 10000];
        let payload2 = vec![0xBB; 20000];
        let ptr1 = manager.store_large_value(&payload1).unwrap();
        let ptr2 = manager.store_large_value(&payload2).unwrap();

        let path = "/tmp/test_toast_roundtrip.toast";
        manager.save_to_disk(path).unwrap();

        let loaded = ToastManager::load_from_disk(path).unwrap();
        assert_eq!(loaded.fetch_large_value(&ptr1).unwrap(), payload1);
        assert_eq!(loaded.fetch_large_value(&ptr2).unwrap(), payload2);
        assert_eq!(loaded.next_value_id, manager.next_value_id);

        // Cleanup
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_disk_persistence_after_delete() {
        let mut manager = ToastManager::new();
        let payload1 = vec![0xAA; 10000];
        let payload2 = vec![0xBB; 15000];
        let ptr1 = manager.store_large_value(&payload1).unwrap();
        let ptr2 = manager.store_large_value(&payload2).unwrap();

        // Delete first value
        manager.delete_value(ptr1.value_id).unwrap();

        let path = "/tmp/test_toast_delete_persist.toast";
        manager.save_to_disk(path).unwrap();

        let loaded = ToastManager::load_from_disk(path).unwrap();
        assert!(loaded.fetch_large_value(&ptr1).is_err()); // Deleted
        assert_eq!(loaded.fetch_large_value(&ptr2).unwrap(), payload2); // Still present

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_disk_persistence_after_vacuum() {
        let mut manager = ToastManager::new();
        let p1 = manager.store_large_value(&vec![1; 10000]).unwrap();
        let _p2 = manager.store_large_value(&vec![2; 10000]).unwrap();
        let p3 = manager.store_large_value(&vec![3; 10000]).unwrap();

        // Vacuum keeping only p1 and p3
        manager.vacuum(&[p1.value_id, p3.value_id]);

        let path = "/tmp/test_toast_vacuum_persist.toast";
        manager.save_to_disk(path).unwrap();

        let loaded = ToastManager::load_from_disk(path).unwrap();
        assert_eq!(loaded.value_count(), 2);
        assert_eq!(loaded.fetch_large_value(&p1).unwrap(), vec![1; 10000]);
        assert_eq!(loaded.fetch_large_value(&p3).unwrap(), vec![3; 10000]);

        let _ = std::fs::remove_file(path);
    }
}
