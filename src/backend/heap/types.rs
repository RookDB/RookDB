/// HeaderMetadata: Serializable metadata stored on Page 0 of a heap file.
/// 
/// This struct represents the heap file's metadata, occupying exactly 24 bytes:
/// - Offset 0-4: page_count (u32) - Total heap pages in file
/// - Offset 4-8: fsm_page_count (u32) - Total pages in FSM fork file
/// - Offset 8-16: total_tuples (u64) - Total tuples inserted (survives crashes)
/// - Offset 16-20: last_vacuum (u32) - Timestamp of last vacuum
/// /// - Offset 20..24 : dead_tuple_count (u32)
///
/// These fields enable O(1) COUNT(*) queries and FSM fork reconstruction.

use std::io::{self, Read, Write, Cursor};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderMetadata {
    pub page_count: u32,      // Total heap pages (including Page 0)
    pub fsm_page_count: u32,  // Total pages in FSM fork file
    pub total_tuples: u64,    // Total tuples inserted
    pub last_vacuum: u32,     // Last vacuum timestamp (unix seconds)
    pub dead_tuple_count: u32, // Number of soft-deleted (dead) tuples not yet physically removed
}

impl HeaderMetadata {
    /// Create a new header with initial state (1 page = Page 0 + Page 1 for data).
    pub fn new() -> Self {
        log::trace!("[HeaderMetadata::new] Creating initial header metadata");
        Self {
            page_count: 1,           // Page 0 only initially
            fsm_page_count: 0,       // Will be set by FSM::build_from_heap
            total_tuples: 0,
            last_vacuum: 0,
            dead_tuple_count: 0,
        }
    }

    /// Serialize header to 24 bytes (little-endian).
    /// 
    /// # Errors
    /// Returns io::Error if write fails.
    pub fn serialize(&self) -> io::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(24);
        
        buf.write_all(&self.page_count.to_le_bytes())?;
        buf.write_all(&self.fsm_page_count.to_le_bytes())?;
        buf.write_all(&self.total_tuples.to_le_bytes())?;
        buf.write_all(&self.last_vacuum.to_le_bytes())?;
        buf.write_all(&self.dead_tuple_count.to_le_bytes())?;
        
        log::trace!(
            "[HeaderMetadata::serialize] Serialized: page_count={}, fsm_page_count={}, total_tuples={}, last_vacuum={}, dead_tuple_count={}",
            self.page_count, self.fsm_page_count, self.total_tuples, self.last_vacuum, self.dead_tuple_count
        );
        
        Ok(buf)
    }

    /// Deserialize header from bytes (little-endian).
    /// 
    /// # Errors
    /// Returns io::Error if buffer is too small or read fails.
    pub fn deserialize(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < 24 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Header buffer too small: {} < 24", bytes.len()),
            ));
        }

        let mut cursor = Cursor::new(bytes);
        let mut buf = [0u8; 4];

        // Read page_count
        cursor.read_exact(&mut buf)?;
        let page_count = u32::from_le_bytes(buf);

        // Read fsm_page_count
        cursor.read_exact(&mut buf)?;
        let fsm_page_count = u32::from_le_bytes(buf);

        // Read total_tuples (8 bytes)
        let mut buf8 = [0u8; 8];
        cursor.read_exact(&mut buf8)?;
        let total_tuples = u64::from_le_bytes(buf8);

        // Read last_vacuum
        cursor.read_exact(&mut buf)?;
        let last_vacuum = u32::from_le_bytes(buf);

         // dead_tuple_count
        cursor.read_exact(&mut buf)?;
        let dead_tuple_count = u32::from_le_bytes(buf);

        log::trace!(
            "[HeaderMetadata::deserialize] Deserialized: page_count={}, fsm_page_count={}, total_tuples={}, last_vacuum={}, dead_tuple_count={}",
            page_count, fsm_page_count, total_tuples, last_vacuum, dead_tuple_count
        );

        Ok(Self {
            page_count,
            fsm_page_count,
            total_tuples,
            last_vacuum,
            dead_tuple_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize() {
        let meta = HeaderMetadata {
            page_count: 100,
            fsm_page_count: 5,
            total_tuples: 999_999_999,
            last_vacuum: 1234567890,
            dead_tuple_count: 42,
        };

        let bytes = meta.serialize().unwrap();
        assert_eq!(bytes.len(), 24);

        let meta2 = HeaderMetadata::deserialize(&bytes).unwrap();
        assert_eq!(meta, meta2);
    }

    #[test]
    fn test_new_header() {
        let meta = HeaderMetadata::new();
        assert_eq!(meta.page_count, 1);
        assert_eq!(meta.fsm_page_count, 0);
        assert_eq!(meta.total_tuples, 0);
        assert_eq!(meta.last_vacuum, 0);
    }

    #[test]
    fn test_deserialize_invalid_size() {
        let short_buf = vec![0u8; 10];
        let result = HeaderMetadata::deserialize(&short_buf);
        assert!(result.is_err());
    }
}
