pub mod compression;
pub mod toast_reader;
pub mod toast_writer;

// the threshold above which the data is compressed using TOAST
pub const TOAST_THRESHOLD: usize = 2048;

// the size of each chunk when TOAST is used
pub const TOAST_CHUNK_SIZE: usize = 2000;

// the tag used for inline TOAST data (0x00)
pub const TOAST_INLINE_TAG: u8 = 0x00;

// the tag used for pointer-based TOAST data (0x01)
pub const TOAST_POINTER_TAG: u8 = 0x01;

// the size of a pointer-based TOAST chunk (18 bytes)
pub const TOAST_POINTER_SIZE: usize = 18;

// pointer to toast table data
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ToastPointer {
    pub tag: u8,             // 0x01
    pub compression: u8,     // 0x00=none, 0x01=lZ4
    pub toast_value_id: u32, // unique id in the toast table
    pub original_size: u32,  // uncompressed byte count
    pub stored_size: u32,    // compressed byte count
    pub num_chunks: u32,     // number of chunks written
}

impl ToastPointer {
    pub fn to_bytes(&self) -> [u8; TOAST_POINTER_SIZE] {
        let mut buf = [0u8; TOAST_POINTER_SIZE];
        buf[0] = self.tag;
        buf[1] = self.compression;
        buf[2..6].copy_from_slice(&self.toast_value_id.to_le_bytes());
        buf[6..10].copy_from_slice(&self.original_size.to_le_bytes());
        buf[10..14].copy_from_slice(&self.stored_size.to_le_bytes());
        buf[14..18].copy_from_slice(&self.num_chunks.to_le_bytes());
        buf
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        Self {
            tag: data[0],
            compression: data[1],
            toast_value_id: u32::from_le_bytes(data[2..6].try_into().unwrap()),
            original_size: u32::from_le_bytes(data[6..10].try_into().unwrap()),
            stored_size: u32::from_le_bytes(data[10..14].try_into().unwrap()),
            num_chunks: u32::from_le_bytes(data[14..18].try_into().unwrap()),
        }
    }
}
