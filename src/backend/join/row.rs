//! Row encoding and decoding for joins.
//!
//! `RowCodec` matches `types::row` exactly but precomputes the layout once per
//! schema instead of once per row. `tests/test_join_row_codec.rs` checks the
//! two agree.

use crate::types::datatype::DataType;
use crate::types::null_bitmap::NullBitmap;
use crate::types::row_layout::{PhysicalSchema, RowLayout};
use crate::types::value::DataValue;

use super::error::JoinError;
use super::schema::OutputSchema;

/// Returns `true` if `value` is the `DataValue` variant that `ty` stores.
fn value_matches_type(ty: &DataType, value: &DataValue) -> bool {
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

/// A schema-bound row encoder/decoder with the physical layout precomputed.
#[derive(Debug, Clone)]
pub struct RowCodec {
    types: Vec<DataType>,
    physical: PhysicalSchema,
    layout: RowLayout,
}

impl RowCodec {
    pub fn new(types: Vec<DataType>) -> Self {
        let physical = PhysicalSchema::from_logical(&types);
        let layout = RowLayout::compute(&physical);
        Self {
            types,
            physical,
            layout,
        }
    }

    pub fn for_schema(schema: &OutputSchema) -> Self {
        Self::new(schema.types.clone())
    }

    pub fn types(&self) -> &[DataType] {
        &self.types
    }

    pub fn column_count(&self) -> usize {
        self.types.len()
    }

    // ── Decoding ─────────────────────────────────────────────────────────────

    /// Validate the row header against this codec's schema and return the
    /// null bitmap. Every decode path goes through here first.
    fn frame<'a>(&self, bytes: &'a [u8]) -> Result<(NullBitmap, &'a [u8]), JoinError> {
        if bytes.len() < 4 {
            return Err(JoinError::codec(format!(
                "row of {} bytes is too short to contain a header",
                bytes.len()
            )));
        }

        let stored_columns = usize::from(u16::from_le_bytes([bytes[0], bytes[1]]));
        let stored_varlen = usize::from(u16::from_le_bytes([bytes[2], bytes[3]]));

        if stored_columns != self.types.len() {
            return Err(JoinError::codec(format!(
                "row declares {} columns but the schema has {}",
                stored_columns,
                self.types.len()
            )));
        }
        if stored_varlen != self.physical.num_varlen() {
            return Err(JoinError::codec(format!(
                "row declares {} variable-length columns but the schema has {}",
                stored_varlen,
                self.physical.num_varlen()
            )));
        }
        if bytes.len() < self.layout.min_row_size() {
            return Err(JoinError::codec(format!(
                "row of {} bytes is shorter than the {}-byte minimum for this schema",
                bytes.len(),
                self.layout.min_row_size()
            )));
        }

        let bitmap_start = RowLayout::bitmap_offset();
        let bitmap_bytes = bytes
            .get(bitmap_start..bitmap_start + self.layout.null_bitmap_size)
            .ok_or_else(|| JoinError::codec("row is truncated inside its null bitmap"))?;
        let bitmap = NullBitmap::from_bytes(self.types.len(), bitmap_bytes)
            .map_err(|e| JoinError::codec(format!("null bitmap: {e}")))?;

        Ok((bitmap, bytes))
    }

    /// Byte range of a variable-length payload, or `None` when the offset
    /// slot holds the NULL sentinel.
    fn varlen_range(
        &self,
        bytes: &[u8],
        varlen_rank: usize,
    ) -> Result<Option<(usize, usize)>, JoinError> {
        let table_start = self.layout.varlen_table_offset();

        let read_slot = |rank: usize| -> Result<usize, JoinError> {
            let slot = table_start + rank * 2;
            let pair = bytes.get(slot..slot + 2).ok_or_else(|| {
                JoinError::codec("row is truncated inside its var-len offset table")
            })?;
            Ok(usize::from(u16::from_le_bytes([pair[0], pair[1]])))
        };

        let start = read_slot(varlen_rank)?;
        if start == 0 {
            return Ok(None);
        }

        let mut end = bytes.len();
        for rank in (varlen_rank + 1)..self.physical.num_varlen() {
            let next = read_slot(rank)?;
            if next != 0 {
                end = next;
                break;
            }
        }

        if start > end || end > bytes.len() {
            return Err(JoinError::codec(format!(
                "var-len payload range {start}..{end} is outside the {}-byte row",
                bytes.len()
            )));
        }
        Ok(Some((start, end)))
    }

    /// Decode every column, in logical order.
    pub fn decode(&self, bytes: &[u8]) -> Result<Vec<Option<DataValue>>, JoinError> {
        let mut out = Vec::with_capacity(self.types.len());
        self.decode_into(bytes, &mut out)?;
        Ok(out)
    }

    /// Decode into a caller-owned buffer, reusing its allocation.
    pub fn decode_into(
        &self,
        bytes: &[u8],
        out: &mut Vec<Option<DataValue>>,
    ) -> Result<(), JoinError> {
        let (bitmap, bytes) = self.frame(bytes)?;
        out.clear();

        for (logical_index, ty) in self.types.iter().enumerate() {
            if bitmap.is_null(logical_index) {
                out.push(None);
                continue;
            }
            out.push(self.decode_non_null(bytes, logical_index, ty)?);
        }
        Ok(())
    }

    /// Decode a single column without touching the others.
    pub fn decode_column(
        &self,
        bytes: &[u8],
        logical_index: usize,
    ) -> Result<Option<DataValue>, JoinError> {
        let ty = self.types.get(logical_index).ok_or_else(|| {
            JoinError::codec(format!(
                "column index {logical_index} is out of range for a {}-column schema",
                self.types.len()
            ))
        })?;

        let (bitmap, bytes) = self.frame(bytes)?;
        if bitmap.is_null(logical_index) {
            return Ok(None);
        }
        self.decode_non_null(bytes, logical_index, ty)
    }

    fn decode_non_null(
        &self,
        bytes: &[u8],
        logical_index: usize,
        ty: &DataType,
    ) -> Result<Option<DataValue>, JoinError> {
        let physical_index = self.physical.logical_to_physical[logical_index];

        let payload = if ty.is_fixed_length() {
            let offset = *self
                .layout
                .fixed_col_offsets
                .get(physical_index)
                .ok_or_else(|| {
                    JoinError::codec("fixed column offset is missing from the layout")
                })?;
            let start = self.layout.fixed_data_start + offset;
            let size = ty.fixed_size().ok_or_else(|| {
                JoinError::codec(format!(
                    "type {ty} reports no fixed size but is fixed-length"
                ))
            })? as usize;
            bytes
                .get(start..start + size)
                .ok_or_else(|| JoinError::codec("row is truncated inside its fixed-data region"))?
        } else {
            let varlen_rank = physical_index - self.physical.num_fixed();
            match self.varlen_range(bytes, varlen_rank)? {
                // The offset slot says NULL while the bitmap said otherwise.
                // The bitmap is authoritative upstream, and it decodes this as
                // NULL; matching that keeps the two codecs equivalent.
                None => return Ok(None),
                Some((start, end)) => &bytes[start..end],
            }
        };

        DataValue::from_bytes(ty, payload)
            .map(Some)
            .map_err(|e| JoinError::codec(format!("column {logical_index} ({ty}): {e}")))
    }

    // ── Encoding ─────────────────────────────────────────────────────────────

    /// Encode a full row.
    pub fn encode(&self, values: &[Option<DataValue>]) -> Result<Vec<u8>, JoinError> {
        self.encode_pair(values, &[])
    }

    /// Encode a row whose columns are `first` followed by `second`.
    ///
    /// Joins always build output this way, and splitting the input avoids
    /// cloning either side into a combined buffer first.
    pub fn encode_pair(
        &self,
        first: &[Option<DataValue>],
        second: &[Option<DataValue>],
    ) -> Result<Vec<u8>, JoinError> {
        let column_count = self.types.len();
        if first.len() + second.len() != column_count {
            return Err(JoinError::codec(format!(
                "got {} + {} values for a {}-column schema",
                first.len(),
                second.len(),
                column_count
            )));
        }

        let value_at = |index: usize| -> &Option<DataValue> {
            if index < first.len() {
                &first[index]
            } else {
                &second[index - first.len()]
            }
        };

        // Encode every value up front so payload sizes are known before the
        // buffer is sized.
        let mut encoded: Vec<Option<Vec<u8>>> = Vec::with_capacity(column_count);
        let mut bitmap = NullBitmap::new(column_count);

        for index in 0..column_count {
            let ty = &self.types[index];
            match value_at(index) {
                Some(value) => {
                    if !value_matches_type(ty, value) {
                        return Err(JoinError::codec(format!(
                            "column {index} expects {ty} but got {value:?}"
                        )));
                    }
                    let bytes = value
                        .to_bytes_for_type(ty)
                        .map_err(|e| JoinError::codec(format!("column {index} ({ty}): {e}")))?;

                    // `to_bytes_for_type` enforces the declared width for
                    // CHAR but not for VARCHAR, while the decoder enforces it
                    // for both.
                    if let DataType::Varchar(limit) = ty {
                        if bytes.len() > usize::from(*limit) {
                            return Err(JoinError::codec(format!(
                                "column {index}: VARCHAR payload of {} bytes exceeds the \
                                 declared limit of {limit}",
                                bytes.len()
                            )));
                        }
                    }

                    encoded.push(Some(bytes));
                }
                None => {
                    bitmap.set_null(index);
                    encoded.push(None);
                }
            }
        }

        let varlen_total: usize = self
            .physical
            .varlen_indices_logical
            .iter()
            .map(|&logical| encoded[logical].as_ref().map_or(0, Vec::len))
            .sum();

        let total_size = self.layout.min_row_size() + varlen_total;
        let mut buf = vec![0u8; total_size];

        // Header.
        let columns_u16 = u16::try_from(column_count)
            .map_err(|_| JoinError::codec("schema has more than 65535 columns"))?;
        let varlen_u16 = u16::try_from(self.physical.num_varlen())
            .map_err(|_| JoinError::codec("schema has more than 65535 var-len columns"))?;
        buf[0..2].copy_from_slice(&columns_u16.to_le_bytes());
        buf[2..4].copy_from_slice(&varlen_u16.to_le_bytes());

        // Null bitmap.
        let bitmap_start = RowLayout::bitmap_offset();
        buf[bitmap_start..bitmap_start + self.layout.null_bitmap_size]
            .copy_from_slice(bitmap.as_bytes());

        // Fixed-length region. NULL columns keep their zero bytes; the bitmap
        // is what makes them NULL.
        for (rank, &logical) in self.physical.fixed_indices_logical.iter().enumerate() {
            if let Some(payload) = &encoded[logical] {
                let start = self.layout.fixed_data_start + self.layout.fixed_col_offsets[rank];
                let end = start + payload.len();
                let slot = buf.get_mut(start..end).ok_or_else(|| {
                    JoinError::codec(format!(
                        "encoded {} bytes for fixed column {logical} but the layout reserves less",
                        payload.len()
                    ))
                })?;
                slot.copy_from_slice(payload);
            }
        }

        // Var-len payloads, appended in physical var-len order, with the
        // offset table filled in as we go.
        let table_start = self.layout.varlen_table_offset();
        let mut cursor = self.layout.min_row_size();

        for (rank, &logical) in self.physical.varlen_indices_logical.iter().enumerate() {
            let Some(payload) = &encoded[logical] else {
                // NULL: offset slot stays at the 0x0000 sentinel.
                continue;
            };

            // Offsets are u16 and 0 means NULL, so a row that grows past
            // 65535 bytes cannot be addressed. Upstream truncates silently
            // here; refusing is the only non-corrupting option.
            let offset = u16::try_from(cursor).map_err(|_| {
                JoinError::codec(format!(
                    "row exceeds the 65535-byte limit imposed by u16 var-len offsets \
                     (payload for column {logical} would start at {cursor})"
                ))
            })?;

            let slot = table_start + rank * 2;
            buf[slot..slot + 2].copy_from_slice(&offset.to_le_bytes());
            buf[cursor..cursor + payload.len()].copy_from_slice(payload);
            cursor += payload.len();
        }

        Ok(buf)
    }
}

/// Builds joined output rows: concatenation, with NULL extension for the
/// unmatched side of an outer join.
#[derive(Debug, Clone)]
pub struct RowBuilder {
    codec: RowCodec,
    null_left: Vec<Option<DataValue>>,
    null_right: Vec<Option<DataValue>>,
}

impl RowBuilder {
    pub fn new(schema: &OutputSchema) -> Self {
        Self {
            codec: RowCodec::for_schema(schema),
            null_left: vec![None; schema.left_width()],
            null_right: vec![None; schema.right_width()],
        }
    }

    pub fn codec(&self) -> &RowCodec {
        &self.codec
    }

    /// Emit one output row. `None` on a side null-extends that side, which is
    /// exactly what an outer join does for an unmatched row.
    pub fn build(
        &self,
        left: Option<&[Option<DataValue>]>,
        right: Option<&[Option<DataValue>]>,
    ) -> Result<Vec<u8>, JoinError> {
        let left_values = left.unwrap_or(&self.null_left);
        let right_values = right.unwrap_or(&self.null_right);
        self.codec.encode_pair(left_values, right_values)
    }
}
