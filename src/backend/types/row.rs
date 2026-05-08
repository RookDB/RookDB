use crate::types::comparison::value_type_name;
use crate::types::datatype::DataType;
use crate::types::null_bitmap::NullBitmap;
use crate::types::row_layout::{PhysicalSchema, RowLayout};
use crate::types::value::DataValue;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn data_value_matches_type(ty: &DataType, value: &DataValue) -> bool {
    matches!(
        (ty, value),
        (DataType::SmallInt, DataValue::SmallInt(_))
            | (DataType::Int, DataValue::Int(_))
            | (DataType::BigInt, DataValue::BigInt(_))
            | (DataType::Real, DataValue::Real(_))
            | (DataType::DoublePrecision, DataValue::DoublePrecision(_))
            | (DataType::Numeric { .. }, DataValue::Numeric(_))
            | (DataType::Decimal { .. }, DataValue::Numeric(_))
            | (DataType::Bool, DataValue::Bool(_))
            | (DataType::Char(_), DataValue::Char(_))
            | (DataType::Character(_), DataValue::Char(_))
            | (DataType::Varchar(_), DataValue::Varchar(_))
            | (DataType::Date, DataValue::Date(_))
            | (DataType::Time, DataValue::Time(_))
            | (DataType::Bit(_), DataValue::Bit(_))
            | (DataType::Timestamp, DataValue::Timestamp(_))
    )
}

// ── Row byte-size helper (free function) ──────────────────────────────────────

/// Returns the exact on-disk byte size of a row given a schema and concrete
/// values, without constructing a `Row` or serialising anything.
/// Useful for pre-insert capacity checks in the heap/page manager.
pub fn row_byte_size(schema: &[DataType], values: &[Option<&DataValue>]) -> usize {
    let physical = PhysicalSchema::from_logical(schema);
    let layout = RowLayout::compute(&physical);
    let mut size = layout.min_row_size();
    for (k, &log_idx) in physical.varlen_indices_logical.iter().enumerate() {
        if let Some(Some(val)) = values.get(log_idx) {
            let bytes = val
                .to_bytes_for_type(&schema[log_idx])
                .unwrap_or_default();
            // stored bytes for varchar are exclusively raw payload
            let payload_len = bytes.len();
            let _ = k; // offset-table slot already counted in min_row_size
            size += payload_len;
        }
    }
    size
}

// ── Public serialize / deserialize ───────────────────────────────────────────

/// Encode a row from string literals.
/// Layout: [Header 4B][Null Bitmap][Var-Len Offset Table][Fixed Data][Var-Len Data]
/// Var-len payloads are stored **without** a length prefix; lengths are derived
/// from adjacent offset-table entries and the total row size.
pub fn serialize_nullable_row(
    schema: &[DataType],
    values: &[Option<&str>],
) -> Result<Vec<u8>, String> {
    if schema.len() != values.len() {
        return Err(format!(
            "Schema/value length mismatch: schema={}, values={}",
            schema.len(),
            values.len()
        ));
    }

    // Pre-encode all values so we know payload sizes upfront.
    let mut encoded_values: Vec<Option<Vec<u8>>> = Vec::with_capacity(schema.len());
    let mut bitmap = NullBitmap::new(schema.len());

    for (i, (ty, maybe_raw)) in schema.iter().zip(values.iter()).enumerate() {
        match maybe_raw {
            Some(raw) => {
                let enc = DataValue::parse_and_encode(ty, raw)?;
                encoded_values.push(Some(enc));
            }
            None => {
                bitmap.set_null(i);
                encoded_values.push(None);
            }
        }
    }

    serialize_encoded(schema, &encoded_values, &bitmap)
}

/// Encode a row from typed `DataValue` instances.
pub fn serialize_nullable_typed_row(
    schema: &[DataType],
    values: &[Option<DataValue>],
) -> Result<Vec<u8>, String> {
    if schema.len() != values.len() {
        return Err(format!(
            "Schema/value length mismatch: schema={}, values={}",
            schema.len(),
            values.len()
        ));
    }

    let mut encoded_values: Vec<Option<Vec<u8>>> = Vec::with_capacity(schema.len());
    let mut bitmap = NullBitmap::new(schema.len());

    for (i, (ty, maybe_val)) in schema.iter().zip(values.iter()).enumerate() {
        match maybe_val {
            Some(val) => {
                if !data_value_matches_type(ty, val) {
                    return Err(format!(
                        "Type mismatch at column {}: expected {}, got {}",
                        i,
                        ty,
                        value_type_name(val)
                    ));
                }
                encoded_values.push(Some(val.to_bytes_for_type(ty)?));
            }
            None => {
                bitmap.set_null(i);
                encoded_values.push(None);
            }
        }
    }

    serialize_encoded(schema, &encoded_values, &bitmap)
}

/// Core serialization path — shared by both public entry points.
///
/// `encoded_values[i]` is the raw bytes produced by the type's encoder:
/// - Fixed-length types: exactly `fixed_size()` bytes.
/// - Varchar: Raw UTF-8 payload bytes exclusively.
fn serialize_encoded(
    schema: &[DataType],
    encoded_values: &[Option<Vec<u8>>],
    bitmap: &NullBitmap,
) -> Result<Vec<u8>, String> {
    let physical = PhysicalSchema::from_logical(schema);
    let layout = RowLayout::compute(&physical);

    // Compute total var-len payload size (raw bytes, no prefix).
    let varlen_payload_total: usize = physical
        .varlen_indices_logical
        .iter()
        .map(|&log_idx| {
            encoded_values[log_idx]
                .as_ref()
                .map(|enc| enc.len())
                .unwrap_or(0)
        })
        .sum();

    let total_size = layout.min_row_size() + varlen_payload_total;
    let mut buf = vec![0u8; total_size];

    // ── Header ────────────────────────────────────────────────────────────────
    let num_cols = schema.len() as u16;
    let num_varlen = physical.num_varlen() as u16;
    buf[0..2].copy_from_slice(&num_cols.to_le_bytes());
    buf[2..4].copy_from_slice(&num_varlen.to_le_bytes());

    // ── Null bitmap ───────────────────────────────────────────────────────────
    let bm_start = RowLayout::bitmap_offset();
    buf[bm_start..bm_start + layout.null_bitmap_size]
        .copy_from_slice(bitmap.as_bytes());

    // ── Var-len offset table (initialised to 0x0000) ──────────────────────────
    // (Already zeroed by vec![0u8; …])

    // ── Fixed-length data region ───────────────────────────────────────────────
    for (rank, &log_idx) in physical.fixed_indices_logical.iter().enumerate() {
        if let Some(enc) = &encoded_values[log_idx] {
            let col_start = layout.fixed_data_start + layout.fixed_col_offsets[rank];
            buf[col_start..col_start + enc.len()].copy_from_slice(enc);
        }
        // NULL → bytes stay 0x00 (correct; bitmap is the authority)
    }

    // ── Var-len data region + fill offset table ────────────────────────────────
    let vt_start = layout.varlen_table_offset();
    let mut append_cursor = layout.min_row_size(); // write pointer into buf tail

    for (vl_rank, &log_idx) in physical.varlen_indices_logical.iter().enumerate() {
        let offset_slot = vt_start + vl_rank * 2;
        if let Some(payload) = &encoded_values[log_idx] {
            // Record offset of this payload from row start
            let row_offset = append_cursor as u16;
            buf[offset_slot..offset_slot + 2].copy_from_slice(&row_offset.to_le_bytes());

            buf[append_cursor..append_cursor + payload.len()].copy_from_slice(payload);
            append_cursor += payload.len();
        }
        // NULL → offset table stays 0x0000 (bitmap is the authority)
    }

    Ok(buf)
}

/// Decode a serialized row back into typed values (logical order).
/// `total_row_size` must be the byte length of `row_bytes` (available from
/// the slot directory); it is used to derive the last var-len payload length.
pub fn deserialize_nullable_row(
    schema: &[DataType],
    row_bytes: &[u8],
) -> Result<Vec<Option<DataValue>>, String> {
    let total_row_size = row_bytes.len();

    if total_row_size < 4 {
        return Err("Row too short to contain header".to_string());
    }

    // ── Header ────────────────────────────────────────────────────────────────
    let num_cols_stored = u16::from_le_bytes([row_bytes[0], row_bytes[1]]) as usize;
    let num_varlen_stored = u16::from_le_bytes([row_bytes[2], row_bytes[3]]) as usize;

    if num_cols_stored != schema.len() {
        return Err(format!(
            "Header column count {} does not match schema length {}",
            num_cols_stored,
            schema.len()
        ));
    }

    let physical = PhysicalSchema::from_logical(schema);
    let layout = RowLayout::compute(&physical);

    if num_varlen_stored != physical.num_varlen() {
        return Err(format!(
            "Header var-len count {} does not match schema var-len count {}",
            num_varlen_stored,
            physical.num_varlen()
        ));
    }

    if total_row_size < layout.min_row_size() {
        return Err(format!(
            "Row ({} bytes) shorter than minimum layout size ({} bytes)",
            total_row_size,
            layout.min_row_size()
        ));
    }

    // ── Null bitmap ───────────────────────────────────────────────────────────
    let bm_start = RowLayout::bitmap_offset();
    let bitmap = NullBitmap::from_bytes(
        schema.len(),
        &row_bytes[bm_start..bm_start + layout.null_bitmap_size],
    )?;

    // ── Var-len offset table ───────────────────────────────────────────────────
    let vt_start = layout.varlen_table_offset();
    let mut varlen_offsets: Vec<Option<usize>> = Vec::with_capacity(physical.num_varlen());

    for vl_rank in 0..physical.num_varlen() {
        let slot = vt_start + vl_rank * 2;
        let raw = u16::from_le_bytes([row_bytes[slot], row_bytes[slot + 1]]) as usize;
        // 0x0000 = NULL sentinel; bitmap is authoritative but we read the table anyway
        varlen_offsets.push(if raw == 0 { None } else { Some(raw) });
    }

    // Build a lookup: varlen physical-group rank → row-byte range
    // length[k] = offset[k+1] - offset[k];  length[last] = total_row_size - offset[last]
    let mut varlen_ranges: Vec<Option<(usize, usize)>> =
        vec![None; physical.num_varlen()];

    // Collect non-null offsets with their ranks for range computation
    let non_null_slots: Vec<(usize, usize)> = varlen_offsets
        .iter()
        .enumerate()
        .filter_map(|(rank, opt)| opt.map(|off| (rank, off)))
        .collect();

    for (i, &(rank, start)) in non_null_slots.iter().enumerate() {
        let end = if i + 1 < non_null_slots.len() {
            non_null_slots[i + 1].1 // start of next non-null var-len col
        } else {
            total_row_size // last payload extends to end of row
        };
        varlen_ranges[rank] = Some((start, end));
    }

    // ── Decode columns in logical order ───────────────────────────────────────
    let mut out: Vec<Option<DataValue>> = vec![None; schema.len()];

    for log_idx in 0..schema.len() {
        if bitmap.is_null(log_idx) {
            out[log_idx] = None;
            continue;
        }

        let ty = &schema[log_idx];
        let phys_idx = physical.logical_to_physical[log_idx];

        if ty.is_fixed_length() {
            // rank within the fixed group = phys_idx (fixed cols occupy 0..n_fixed)
            let rank = phys_idx;
            let col_start = layout.fixed_data_start + layout.fixed_col_offsets[rank];
            let col_size =
                ty.fixed_size().expect("fixed-length type must have fixed_size") as usize;
            let value = DataValue::from_bytes(ty, &row_bytes[col_start..col_start + col_size])?;
            out[log_idx] = Some(value);
        } else {
            // var-len column — rank within the var-len group
            let vl_rank = phys_idx - physical.num_fixed();
            if let Some((start, end)) = varlen_ranges[vl_rank] {
                // Pass exact payload bytes to the decoder
                let payload = &row_bytes[start..end];
                let value = DataValue::from_bytes(ty, payload)?;
                out[log_idx] = Some(value);
            }
            // else: offset was 0x0000 but bitmap said non-null — treat as null
        }
    }

    Ok(out)
}

// ── Row struct ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Row {
    /// User-defined column order (logical schema). Public API indices use this.
    logical_schema: Vec<DataType>,
    /// On-disk column reordering (fixed-first, then var-len).
    physical: PhysicalSchema,
    /// Byte-level geometry derived from `physical`.
    layout: RowLayout,
    /// NULL flags keyed on **logical** column index.
    null_bitmap: NullBitmap,
    /// On-disk bytes: [var-len offset table][fixed-data][var-len data].
    /// Does NOT include the 4-byte header or null bitmap (those are prepended
    /// by `serialize()`).
    data: Vec<u8>,
}

impl Row {
    /// Create a new all-NULL row for the given schema.
    pub fn new(schema: Vec<DataType>) -> Self {
        let physical = PhysicalSchema::from_logical(&schema);
        let layout = RowLayout::compute(&physical);
        let mut null_bitmap = NullBitmap::new(schema.len());
        for i in 0..schema.len() {
            null_bitmap.set_null(i);
        }
        // Pre-allocate the static regions (var-len table + fixed data) all zeroed.
        let data = vec![0u8; layout.varlen_table_size + layout.fixed_data_size];
        Self {
            logical_schema: schema,
            physical,
            layout,
            null_bitmap,
            data,
        }
    }

    // ── Size helper ───────────────────────────────────────────────────────────

    /// Returns the exact on-disk byte size of this row without serialising it.
    /// Used by the heap/page manager to check if a row fits before writing.
    pub fn byte_size(&self) -> usize {
        RowLayout::bitmap_offset() + self.layout.null_bitmap_size + self.data.len()
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    pub fn set_value(&mut self, column_index: usize, value: &DataValue) -> Result<(), String> {
        if column_index >= self.logical_schema.len() {
            return Err(format!("Column index {} out of bounds", column_index));
        }
        let ty = &self.logical_schema[column_index];
        if !data_value_matches_type(ty, value) {
            return Err(format!(
                "Type mismatch at column {}: expected {}, got {}",
                column_index,
                ty,
                value_type_name(value)
            ));
        }

        let enc = value.to_bytes_for_type(ty)?;
        let phys_idx = self.physical.logical_to_physical[column_index];
        let bm_start = RowLayout::bitmap_offset();
        let data_start_in_row = bm_start + self.layout.null_bitmap_size;
        
        self.null_bitmap.clear_null(column_index);

        if ty.is_fixed_length() {
            let rank = phys_idx;
            let row_col_start = self.layout.fixed_data_start + self.layout.fixed_col_offsets[rank];
            let self_col_start = row_col_start - data_start_in_row;
            self.data[self_col_start..self_col_start + enc.len()].copy_from_slice(&enc);
            return Ok(());
        }

        let vl_rank = phys_idx - self.physical.num_fixed();
        let (payload_start, payload_end) = self.get_varlen_bounds(vl_rank);
        let old_len = payload_end - payload_start;
        let new_len = enc.len();
        
        // Ensure its offset is active
        let slot = vl_rank * 2;
        let mut raw_offset = u16::from_le_bytes([self.data[slot], self.data[slot + 1]]) as usize;
        
        if raw_offset == 0 {
            raw_offset = payload_start + data_start_in_row;
            self.data[slot..slot + 2].copy_from_slice(&(raw_offset as u16).to_le_bytes());
        }

        if new_len > old_len {
            self.data.splice(payload_start..payload_end, enc);
            self.adjust_varlen_offsets(vl_rank, (new_len - old_len) as isize);
        } else if new_len < old_len {
            self.data.splice(payload_start..payload_end, enc);
            self.adjust_varlen_offsets(vl_rank, -((old_len - new_len) as isize));
        } else {
            self.data[payload_start..payload_end].copy_from_slice(&enc);
        }

        Ok(())
    }

    pub fn set_null(&mut self, column_index: usize) -> Result<(), String> {
        if column_index >= self.logical_schema.len() {
            return Err(format!("Column index {} out of bounds", column_index));
        }

        if self.null_bitmap.is_null(column_index) {
            return Ok(()); // Already NULL
        }

        let ty = &self.logical_schema[column_index];
        let phys_idx = self.physical.logical_to_physical[column_index];
        self.null_bitmap.set_null(column_index);

        if ty.is_fixed_length() {
            return Ok(()); // Zero dead space handling needed for fixed
        }

        let vl_rank = phys_idx - self.physical.num_fixed();
        let (payload_start, payload_end) = self.get_varlen_bounds(vl_rank);
        let bytes_to_remove = payload_end - payload_start;
        
        if bytes_to_remove > 0 {
            self.data.drain(payload_start..payload_end);
            self.adjust_varlen_offsets(vl_rank, -(bytes_to_remove as isize));
        }

        // Set offset to 0 sentinel
        let slot = vl_rank * 2;
        self.data[slot..slot + 2].copy_from_slice(&0u16.to_le_bytes());

        Ok(())
    }

    pub fn get_value(&self, column_index: usize) -> Result<Option<DataValue>, String> {
        if column_index >= self.logical_schema.len() {
            return Err(format!("Column index {} out of bounds", column_index));
        }

        if self.null_bitmap.is_null(column_index) {
            return Ok(None);
        }

        let ty = &self.logical_schema[column_index];
        let phys_idx = self.physical.logical_to_physical[column_index];

        let bm_start = RowLayout::bitmap_offset();
        let data_start_in_row = bm_start + self.layout.null_bitmap_size;

        if ty.is_fixed_length() {
            let rank = phys_idx;
            let row_col_start = self.layout.fixed_data_start + self.layout.fixed_col_offsets[rank];
            let self_col_start = row_col_start - data_start_in_row;
            
            let col_size = ty.fixed_size().expect("fixed-length type must have fixed_size") as usize;
            let bytes = &self.data[self_col_start..self_col_start + col_size];
            return Ok(Some(DataValue::from_bytes(ty, bytes)?));
        }

        let vl_rank = phys_idx - self.physical.num_fixed();
        let (start, end) = self.get_varlen_bounds(vl_rank);
        let bytes = &self.data[start..end];
        Ok(Some(DataValue::from_bytes(ty, bytes)?))
    }

    // ── Serialisation ─────────────────────────────────────────────────────────

    /// Produce the full on-disk byte sequence.
    pub fn serialize(&self) -> Vec<u8> {
        let bm_start = RowLayout::bitmap_offset();
        let data_start = bm_start + self.layout.null_bitmap_size;
        let total = data_start + self.data.len();

        let mut out = vec![0u8; total];
        // Header
        out[0..2].copy_from_slice(&(self.logical_schema.len() as u16).to_le_bytes());
        out[2..4].copy_from_slice(&(self.physical.num_varlen() as u16).to_le_bytes());
        // Null bitmap
        out[bm_start..bm_start + self.layout.null_bitmap_size]
            .copy_from_slice(self.null_bitmap.as_bytes());
        // Remaining data (var-len table + fixed + var-len payloads)
        out[data_start..].copy_from_slice(&self.data);
        out
    }

    pub fn deserialize(schema: &[DataType], bytes: &[u8]) -> Result<Self, String> {
        let physical = PhysicalSchema::from_logical(schema);
        let layout = RowLayout::compute(&physical);

        if bytes.len() < 4 {
            return Err("Row too short to contain header".to_string());
        }

        // Validate via full decode first (catches corrupt rows early).
        let _ = deserialize_nullable_row(schema, bytes)?;

        // Extract null bitmap and raw data from the byte slice.
        let bm_start = RowLayout::bitmap_offset();
        let null_bitmap =
            NullBitmap::from_bytes(schema.len(), &bytes[bm_start..bm_start + layout.null_bitmap_size])?;

        // `data` = everything after the header and null bitmap
        let data_start = bm_start + layout.null_bitmap_size;
        let data = bytes[data_start..].to_vec();

        Ok(Self {
            logical_schema: schema.to_vec(),
            physical,
            layout,
            null_bitmap,
            data,
        })
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// Returns the absolute `(start, end)` offset bounds for a var-len payload
    /// inside `self.data`. Uses exactly the same logic as deserialize, ensuring
    /// we can splice values optimally without breaking layout geometry.
    fn get_varlen_bounds(&self, vl_rank: usize) -> (usize, usize) {
        let bm_start = RowLayout::bitmap_offset();
        let data_start_in_row = bm_start + self.layout.null_bitmap_size;
        
        let slot = vl_rank * 2;
        let raw_offset = u16::from_le_bytes([self.data[slot], self.data[slot + 1]]) as usize;
        
        if raw_offset == 0 {
            // Null column: its theoretical insertion point is the beginning 
            // of the next sequence.
            let mut insertion_point = self.data.len();
            for r in (vl_rank + 1)..self.physical.num_varlen() {
                let next_slot = r * 2;
                let next_off = u16::from_le_bytes([self.data[next_slot], self.data[next_slot + 1]]) as usize;
                if next_off != 0 {
                    insertion_point = next_off - data_start_in_row;
                    break;
                }
            }
            return (insertion_point, insertion_point);
        }

        let start = raw_offset - data_start_in_row;
        let mut end = self.data.len();
        for r in (vl_rank + 1)..self.physical.num_varlen() {
            let next_slot = r * 2;
            let next_off = u16::from_le_bytes([self.data[next_slot], self.data[next_slot + 1]]) as usize;
            if next_off != 0 {
                end = next_off - data_start_in_row;
                break;
            }
        }
        
        (start, end)
    }

    /// Shift the absolute offset reference for all variable-length rows mapped
    /// geometrically *after* the given rank up or down by `delta`.
    fn adjust_varlen_offsets(&mut self, after_vl_rank: usize, delta: isize) {
        for r in (after_vl_rank + 1)..self.physical.num_varlen() {
            let slot = r * 2;
            let current_off = u16::from_le_bytes([self.data[slot], self.data[slot + 1]]) as usize;
            if current_off != 0 {
                let updated = (current_off as isize + delta) as u16;
                self.data[slot..slot + 2].copy_from_slice(&updated.to_le_bytes());
            }
        }
    }
}


