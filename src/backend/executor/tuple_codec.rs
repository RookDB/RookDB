//! Encode and decode tuples using the new TupleHeader format.
//!
//! On-disk tuple layout:
//!   [ TupleHeader bytes ]
//!   [ fixed-length column data ]
//!   [ variable-length column data: each is (4-byte u32 len)(raw bytes) ]

use crate::catalog::types::{Column, DataType};
use crate::executor::value::Value;
use crate::page::tuple::TupleHeader;

/// Encode a row of Values into bytes using TupleHeader format.
pub fn encode_tuple(values: &[Value], schema: &[Column]) -> Vec<u8> {
    assert_eq!(values.len(), schema.len());

    let num_cols = schema.len();
    let var_col_indices: Vec<usize> = schema
        .iter()
        .enumerate()
        .filter(|(_, c)| !c.parsed_type().is_fixed())
        .map(|(i, _)| i)
        .collect();
    let num_var_cols = var_col_indices.len();

    let header_size = TupleHeader::header_size(num_cols, num_var_cols);

    // First pass: collect fixed bytes and variable bytes separately.
    let mut fixed_bytes: Vec<u8> = Vec::new();
    let mut var_bytes: Vec<u8> = Vec::new();

    // Track where each variable column's data will start (offset from tuple start = header_size + fixed_size + var_offset).
    let mut var_offsets: Vec<u32> = Vec::with_capacity(num_var_cols);

    // Calculate total fixed size first.
    let fixed_size: usize = schema
        .iter()
        .map(|c| c.parsed_type().fixed_size().unwrap_or(0))
        .sum();

    let mut var_idx = 0usize;
    let mut current_var_offset = 0u32;

    for (i, col) in schema.iter().enumerate() {
        let val = &values[i];
        let dt = col.parsed_type();

        if dt.is_fixed() {
            encode_fixed_value(val, &dt, &mut fixed_bytes);
        } else {
            // Variable-length: record offset, then write (len_prefix)(data)
            let offset_in_tuple = (header_size + fixed_size) as u32 + current_var_offset;
            var_offsets.push(offset_in_tuple);
            var_idx += 1;

            let bytes = encode_var_value(val, &dt);
            current_var_offset += bytes.len() as u32;
            var_bytes.extend(bytes);
        }
    }
    let _ = var_idx;

    // Build the header
    let mut header = TupleHeader::new(num_cols, num_var_cols);
    for (i, val) in values.iter().enumerate() {
        if matches!(val, Value::Null) {
            header.set_null(i);
        }
    }
    // Copy the offsets we computed above
    for (i, &off) in var_offsets.iter().enumerate() {
        if i < header.var_col_offsets.len() {
            header.var_col_offsets[i] = off;
        }
    }

    // Assemble: header ++ fixed ++ variable
    let mut out = header.encode();
    out.extend(fixed_bytes);
    out.extend(var_bytes);
    out
}

/// Decode a raw tuple byte slice into a Vec<Value> using the schema.
pub fn decode_tuple(bytes: &[u8], schema: &[Column]) -> Vec<Value> {
    let num_cols = schema.len();
    let var_col_indices: Vec<usize> = schema
        .iter()
        .enumerate()
        .filter(|(_, c)| !c.parsed_type().is_fixed())
        .map(|(i, _)| i)
        .collect();
    let num_var_cols = var_col_indices.len();

    if bytes.is_empty() {
        return vec![Value::Null; num_cols];
    }

    let header = TupleHeader::decode(bytes, num_cols, num_var_cols);
    let header_size = header.encoded_size();

    let mut values = Vec::with_capacity(num_cols);
    let mut fixed_cursor = header_size;
    let mut var_col_iter = var_col_indices.iter();
    let mut var_offset_idx = 0usize;

    for (i, col) in schema.iter().enumerate() {
        if header.is_null(i) {
            values.push(Value::Null);
            let dt = col.parsed_type();
            if dt.is_fixed() {
                fixed_cursor += dt.fixed_size().unwrap_or(0);
            }
            continue;
        }

        let dt = col.parsed_type();
        if let Some(sz) = dt.fixed_size() {
            // Fixed-length column
            if fixed_cursor + sz <= bytes.len() {
                let v = decode_fixed_value(&bytes[fixed_cursor..fixed_cursor + sz], &dt);
                values.push(v);
            } else {
                values.push(Value::Null);
            }
            fixed_cursor += sz;
        } else {
            // Variable-length column: find offset from header
            let abs_offset = if var_offset_idx < header.var_col_offsets.len() {
                header.var_col_offsets[var_offset_idx] as usize
            } else {
                bytes.len() // safe fallback
            };
            var_offset_idx += 1;

            if abs_offset + 4 <= bytes.len() {
                let len = u32::from_le_bytes(
                    bytes[abs_offset..abs_offset + 4].try_into().unwrap_or([0; 4])
                ) as usize;
                let data_start = abs_offset + 4;
                if data_start + len <= bytes.len() {
                    let v = decode_var_value(&bytes[data_start..data_start + len], &dt);
                    values.push(v);
                } else {
                    values.push(Value::Null);
                }
            } else {
                values.push(Value::Null);
            }
            let _ = var_col_iter.next();
        }
    }

    values
}

// ─── private helpers ────────────────────────────────────────────────────────

fn encode_fixed_value(val: &Value, dt: &DataType, out: &mut Vec<u8>) {
    match (val, dt) {
        (Value::Null, _) => {
            // Write zeros for fixed-length nulls (bitmap marks them as null)
            let sz = dt.fixed_size().unwrap_or(0);
            out.extend(vec![0u8; sz]);
        }
        (Value::Int(v), DataType::Int) => {
            out.extend_from_slice(&(*v as i32).to_le_bytes());
        }
        (Value::Int(v), DataType::Float) => {
            out.extend_from_slice(&(*v as f64).to_le_bytes());
        }
        (Value::Float(v), DataType::Float) => {
            out.extend_from_slice(&v.to_le_bytes());
        }
        (Value::Float(v), DataType::Int) => {
            out.extend_from_slice(&(*v as i32).to_le_bytes());
        }
        (Value::Bool(b), DataType::Bool) => {
            out.push(if *b { 1 } else { 0 });
        }
        (Value::Date(d), DataType::Date) => {
            out.extend_from_slice(&d.to_le_bytes());
        }
        (Value::Timestamp(t), DataType::Timestamp) => {
            out.extend_from_slice(&t.to_le_bytes());
        }
        _ => {
            // Best-effort: write zeros for unmatched types
            let sz = dt.fixed_size().unwrap_or(0);
            out.extend(vec![0u8; sz]);
        }
    }
}

fn decode_fixed_value(bytes: &[u8], dt: &DataType) -> Value {
    match dt {
        DataType::Int => {
            let v = i32::from_le_bytes(bytes[..4].try_into().unwrap_or([0; 4]));
            Value::Int(v as i64)
        }
        DataType::Float => {
            let v = f64::from_le_bytes(bytes[..8].try_into().unwrap_or([0; 8]));
            Value::Float(v)
        }
        DataType::Bool => {
            Value::Bool(bytes[0] != 0)
        }
        DataType::Date => {
            let v = i32::from_le_bytes(bytes[..4].try_into().unwrap_or([0; 4]));
            Value::Date(v)
        }
        DataType::Timestamp => {
            let v = i64::from_le_bytes(bytes[..8].try_into().unwrap_or([0; 8]));
            Value::Timestamp(v)
        }
        _ => Value::Null,
    }
}

fn encode_var_value(val: &Value, dt: &DataType) -> Vec<u8> {
    let raw = match (val, dt) {
        (Value::Null, _) => vec![],
        (Value::Text(s), _) => s.as_bytes().to_vec(),
        (Value::Int(v), _) => v.to_string().into_bytes(),
        (Value::Float(v), _) => v.to_string().into_bytes(),
        _ => vec![],
    };
    let mut out = Vec::with_capacity(4 + raw.len());
    out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    out.extend(raw);
    out
}

fn decode_var_value(bytes: &[u8], dt: &DataType) -> Value {
    match dt {
        DataType::Text | DataType::Varchar(_) => {
            Value::Text(String::from_utf8_lossy(bytes).trim_end_matches('\0').to_string())
        }
        _ => Value::Text(String::from_utf8_lossy(bytes).to_string()),
    }
}

// ─── Legacy decoder (for existing INT/TEXT-only tables) ─────────────────────

/// Decode an old-style raw tuple (no TupleHeader) using INT=4 bytes, TEXT=10 bytes.
pub fn decode_legacy_tuple(bytes: &[u8], schema: &[Column]) -> Vec<Value> {
    let mut values = Vec::new();
    let mut cursor = 0usize;
    for col in schema {
        match col.data_type.to_uppercase().as_str() {
            "INT" => {
                if cursor + 4 <= bytes.len() {
                    let v = i32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap_or([0; 4]));
                    values.push(Value::Int(v as i64));
                    cursor += 4;
                } else {
                    values.push(Value::Null);
                }
            }
            "TEXT" => {
                if cursor + 10 <= bytes.len() {
                    let s = String::from_utf8_lossy(&bytes[cursor..cursor + 10])
                        .trim()
                        .to_string();
                    values.push(Value::Text(s));
                    cursor += 10;
                } else {
                    values.push(Value::Null);
                }
            }
            _ => {
                values.push(Value::Null);
            }
        }
    }
    values
}
