#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NullBitmap {
    column_count: usize,
    data: Vec<u8>,
}

impl NullBitmap {
    pub fn new(column_count: usize) -> Self {
        let byte_len = column_count.div_ceil(8);
        Self {
            column_count,
            data: vec![0u8; byte_len],
        }
    }

    pub fn from_bytes(column_count: usize, raw: &[u8]) -> Result<Self, String> {
        let expected = column_count.div_ceil(8);
        if raw.len() != expected {
            return Err(format!(
                "Invalid NULL bitmap length: expected {}, found {}",
                expected,
                raw.len()
            ));
        }
        Ok(Self {
            column_count,
            data: raw.to_vec(),
        })
    }

    pub fn set_null(&mut self, column_index: usize) {
        assert!(column_index < self.column_count, "column index out of range");
        let byte_idx = column_index / 8;
        let bit_idx = column_index % 8;
        self.data[byte_idx] |= 1 << bit_idx;
    }

    pub fn clear_null(&mut self, column_index: usize) {
        assert!(column_index < self.column_count, "column index out of range");
        let byte_idx = column_index / 8;
        let bit_idx = column_index % 8;
        self.data[byte_idx] &= !(1 << bit_idx);
    }

    pub fn is_null(&self, column_index: usize) -> bool {
        assert!(column_index < self.column_count, "column index out of range");
        let byte_idx = column_index / 8;
        let bit_idx = column_index % 8;
        (self.data[byte_idx] & (1 << bit_idx)) != 0
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}
