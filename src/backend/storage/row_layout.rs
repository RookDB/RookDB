//! Row layout structures for RookDB tuple storage
//! Defines the format for storing mixed fixed/variable-length columns in pages

use serde::{Deserialize, Serialize};

/// Metadata header for a tuple containing variable-length fields
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TupleHeader {
    /// Number of columns in this tuple
    pub column_count: u16,
    /// Number of bytes in the null bitmap
    pub null_bitmap_bytes: u16,
    /// Number of variable-length fields
    pub var_field_count: u16,
    /// Flags for future extensibility
    pub flags: u16,
}

impl TupleHeader {
    pub fn new(
        column_count: u16,
        null_bitmap_bytes: u16,
        var_field_count: u16,
    ) -> Self {
        TupleHeader {
            column_count,
            null_bitmap_bytes,
            var_field_count,
            flags: 0,
        }
    }

    pub fn size() -> usize {
        8 // 4 u16 fields
    }

    /// Serialize header to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::size());
        bytes.extend_from_slice(&self.column_count.to_le_bytes());
        bytes.extend_from_slice(&self.null_bitmap_bytes.to_le_bytes());
        bytes.extend_from_slice(&self.var_field_count.to_le_bytes());
        bytes.extend_from_slice(&self.flags.to_le_bytes());
        bytes
    }

    /// Deserialize header from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < Self::size() {
            return Err("Insufficient bytes for tuple header".to_string());
        }

        Ok(TupleHeader {
            column_count: u16::from_le_bytes([bytes[0], bytes[1]]),
            null_bitmap_bytes: u16::from_le_bytes([bytes[2], bytes[3]]),
            var_field_count: u16::from_le_bytes([bytes[4], bytes[5]]),
            flags: u16::from_le_bytes([bytes[6], bytes[7]]),
        })
    }
}

/// Directory entry for a variable-length field within a tuple
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct VarFieldEntry {
    /// Offset of field data from start of tuple
    pub offset: u32,
    /// Length of field data in bytes
    pub length: u32,
    /// Flags: bit 0 = is_toast (external), bit 1 = is_compressed
    pub flags: u16,
    /// Reserved for future use
    pub reserved: u16,
}
// CAN ALSO INCLUDE TRANSACTION ID
impl VarFieldEntry {
    pub fn new(offset: u32, length: u32, is_toast: bool) -> Self {
        let flags = if is_toast { 1u16 } else { 0u16 };
        VarFieldEntry {
            offset,
            length,
            flags,
            reserved: 0,
        }
    }

    pub fn size() -> usize {
        12 // u32 + u32 + u16 + u16
    }

    pub fn is_toast(&self) -> bool {
        (self.flags & 1) != 0
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::size());
        bytes.extend_from_slice(&self.offset.to_le_bytes());
        bytes.extend_from_slice(&self.length.to_le_bytes());
        bytes.extend_from_slice(&self.flags.to_le_bytes());
        bytes.extend_from_slice(&self.reserved.to_le_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < Self::size() {
            return Err("Insufficient bytes for var field entry".to_string());
        }

        Ok(VarFieldEntry {
            offset: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            length: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            flags: u16::from_le_bytes([bytes[8], bytes[9]]),
            reserved: u16::from_le_bytes([bytes[10], bytes[11]]),
        })
    }
}

/// TOAST (The Oversized Attribute Storage Technique) pointer for out-of-line values
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ToastPointer {
    /// Unique identifier for this large value in TOAST storage
    pub value_id: u64,
    /// Total size of the value in bytes
    pub total_bytes: u32,
    /// Number of chunks this value is split into
    pub chunk_count: u32,
}

impl ToastPointer {
    pub fn new(value_id: u64, total_bytes: u32, chunk_count: u32) -> Self {
        ToastPointer {
            value_id,
            total_bytes,
            chunk_count,
        }
    }

    pub fn size() -> usize {
        16 // u64 + u32 + u32
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::size());
        bytes.extend_from_slice(&self.value_id.to_le_bytes());
        bytes.extend_from_slice(&self.total_bytes.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_count.to_le_bytes());
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < Self::size() {
            return Err("Insufficient bytes for TOAST pointer".to_string());
        }

        Ok(ToastPointer {
            value_id: u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            total_bytes: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            chunk_count: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
        })
    }
}

/// A single chunk of a TOAST value
#[derive(Clone, Debug)]
pub struct ToastChunk {
    /// Value ID this chunk belongs to
    pub value_id: u64,
    /// Sequential chunk number (0-based)
    pub chunk_no: u32,
    /// Length of chunk data
    pub chunk_len: u16,
    /// Flags for compression, encryption, etc.
    pub flags: u16,
    /// The actual chunk data
    pub data: Vec<u8>,
}

impl ToastChunk {
    pub fn new(value_id: u64, chunk_no: u32, data: Vec<u8>) -> Self {
        let chunk_len = data.len() as u16;
        ToastChunk {
            value_id,
            chunk_no,
            chunk_len,
            flags: 0,
            data,
        }
    }

    /// Get header size (metadata before data)
    pub fn header_size() -> usize {
        8 + 4 + 2 + 2 // u64 + u32 + u16 + u16
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::header_size() + self.data.len());
        bytes.extend_from_slice(&self.value_id.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_no.to_le_bytes());
        bytes.extend_from_slice(&self.chunk_len.to_le_bytes());
        bytes.extend_from_slice(&self.flags.to_le_bytes());
        bytes.extend_from_slice(&self.data);
        bytes
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < Self::header_size() {
            return Err("Insufficient bytes for TOAST chunk header".to_string());
        }

        let value_id =
            u64::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]);
        let chunk_no = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        let chunk_len = u16::from_le_bytes([bytes[12], bytes[13]]);
        let flags = u16::from_le_bytes([bytes[14], bytes[15]]);

        let data_len = chunk_len as usize;
        if bytes.len() < Self::header_size() + data_len {
            return Err("Insufficient bytes for TOAST chunk data".to_string());
        }

        let data = bytes[Self::header_size()..Self::header_size() + data_len].to_vec();

        Ok(ToastChunk {
            value_id,
            chunk_no,
            chunk_len,
            flags,
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_header_serialization() {
        let header = TupleHeader::new(5, 1, 2);
        let bytes = header.to_bytes();
        let restored = TupleHeader::from_bytes(&bytes).unwrap();

        assert_eq!(header.column_count, restored.column_count);
        assert_eq!(header.null_bitmap_bytes, restored.null_bitmap_bytes);
        assert_eq!(header.var_field_count, restored.var_field_count);
    }

    #[test]
    fn test_var_field_entry_serialization() {
        let entry = VarFieldEntry::new(100, 50, false);
        let bytes = entry.to_bytes();
        let restored = VarFieldEntry::from_bytes(&bytes).unwrap();

        assert_eq!(entry.offset, restored.offset);
        assert_eq!(entry.length, restored.length);
        assert_eq!(entry.is_toast(), restored.is_toast());
    }

    #[test]
    fn test_toast_pointer_serialization() {
        let ptr = ToastPointer::new(12345, 50000, 10);
        let bytes = ptr.to_bytes();
        let restored = ToastPointer::from_bytes(&bytes).unwrap();

        assert_eq!(ptr.value_id, restored.value_id);
        assert_eq!(ptr.total_bytes, restored.total_bytes);
        assert_eq!(ptr.chunk_count, restored.chunk_count);
    }

    #[test]
    fn test_toast_chunk_serialization() {
        let data = vec![1, 2, 3, 4, 5];
        let chunk = ToastChunk::new(12345, 0, data.clone());
        let bytes = chunk.to_bytes();
        let restored = ToastChunk::from_bytes(&bytes).unwrap();

        assert_eq!(chunk.value_id, restored.value_id);
        assert_eq!(chunk.chunk_no, restored.chunk_no);
        assert_eq!(chunk.data, restored.data);
    }
}
