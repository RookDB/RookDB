//! TupleHeader: null bitmap + variable-length column offset table.
//!
//! On-disk layout per tuple:
//!   [ null_bitmap: ceil(n_cols/8) bytes ]
//!   [ var_col_offsets: n_var_cols * 4 bytes ]  -- u32 le each, offset from start of tuple data
//!   [ fixed column bytes ... ]
//!   [ variable column bytes (each: 4-byte u32 length prefix + raw bytes) ]

/// Header prepended to every stored tuple.
#[derive(Debug, Clone)]
pub struct TupleHeader {
    /// One bit per column; bit i set means column i is NULL.
    pub null_bitmap: Vec<u8>,
    /// Byte offset (from the very start of the tuple including this header) for each
    /// variable-length column.  Only variable-length columns appear here, in schema order.
    pub var_col_offsets: Vec<u32>,
    /// Number of columns in the schema (needed to decode).
    pub num_cols: usize,
    /// Number of variable-length columns.
    pub num_var_cols: usize,
}

impl TupleHeader {
    /// Create a zeroed header for `num_cols` total columns, `num_var_cols` of which are variable.
    pub fn new(num_cols: usize, num_var_cols: usize) -> Self {
        let bitmap_bytes = (num_cols + 7) / 8;
        Self {
            null_bitmap: vec![0u8; bitmap_bytes],
            var_col_offsets: vec![0u32; num_var_cols],
            num_cols,
            num_var_cols,
        }
    }

    /// How many bytes does this header occupy on disk?
    pub fn header_size(num_cols: usize, num_var_cols: usize) -> usize {
        let bitmap_bytes = (num_cols + 7) / 8;
        bitmap_bytes + num_var_cols * 4
    }

    pub fn encoded_size(&self) -> usize {
        Self::header_size(self.num_cols, self.num_var_cols)
    }

    pub fn is_null(&self, col_idx: usize) -> bool {
        let byte = col_idx / 8;
        let bit = col_idx % 8;
        if byte < self.null_bitmap.len() {
            (self.null_bitmap[byte] >> bit) & 1 == 1
        } else {
            false
        }
    }

    pub fn set_null(&mut self, col_idx: usize) {
        let byte = col_idx / 8;
        let bit = col_idx % 8;
        if byte < self.null_bitmap.len() {
            self.null_bitmap[byte] |= 1 << bit;
        }
    }

    pub fn clear_null(&mut self, col_idx: usize) {
        let byte = col_idx / 8;
        let bit = col_idx % 8;
        if byte < self.null_bitmap.len() {
            self.null_bitmap[byte] &= !(1 << bit);
        }
    }

    /// Encode to bytes for writing into a page.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_size());
        out.extend_from_slice(&self.null_bitmap);
        for &off in &self.var_col_offsets {
            out.extend_from_slice(&off.to_le_bytes());
        }
        out
    }

    /// Decode from the front of a byte slice.
    pub fn decode(bytes: &[u8], num_cols: usize, num_var_cols: usize) -> Self {
        let bitmap_bytes = (num_cols + 7) / 8;
        let null_bitmap = bytes[..bitmap_bytes].to_vec();
        let mut var_col_offsets = Vec::with_capacity(num_var_cols);
        for i in 0..num_var_cols {
            let base = bitmap_bytes + i * 4;
            let off = u32::from_le_bytes(bytes[base..base + 4].try_into().unwrap_or([0; 4]));
            var_col_offsets.push(off);
        }
        Self {
            null_bitmap,
            var_col_offsets,
            num_cols,
            num_var_cols,
        }
    }
}
