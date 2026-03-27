// TupleAccessor extended edge-case tests
// Tests get_value_fast vs get_value consistency, invalid column indices,
// offset violations, and NULL bitmap accuracy.

use storage_manager::executor::selection::*;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Build a single-column INT tuple.
fn make_int_tuple(id: i32) -> Vec<u8> {
    let num_cols = 1usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8usize;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let total = data_start + 4;
    let mut t = vec![0u8; total];
    t[0..4].copy_from_slice(&(total as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0;
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&4u32.to_le_bytes());
    t[data_start..data_start+4].copy_from_slice(&id.to_le_bytes());
    t
}

/// Build a single-column INT tuple with column 0 set to NULL.
fn make_null_int_tuple() -> Vec<u8> {
    let mut t = make_int_tuple(0);
    t[8] = 0b00000001;
    t
}

/// Build a 2-column INT tuple.
fn make_two_int_tuple(a: i32, b: i32) -> Vec<u8> {
    let num_cols = 2usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8usize;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let total = data_start + 8;
    let mut t = vec![0u8; total];
    t[0..4].copy_from_slice(&(total as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0;
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&4u32.to_le_bytes());
    t[os+8..os+12].copy_from_slice(&8u32.to_le_bytes());
    t[data_start..data_start+4].copy_from_slice(&a.to_le_bytes());
    t[data_start+4..data_start+8].copy_from_slice(&b.to_le_bytes());
    t
}

/// Tuple with non-monotonic offsets: offset[1] < offset[0].
fn make_non_monotonic_tuple() -> Vec<u8> {
    let num_cols = 2usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8usize;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let total = data_start + 8;
    let mut t = vec![0u8; total];
    t[0..4].copy_from_slice(&(total as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0;
    let os = 9usize;
    // Reversed offsets: 8, 4, 8 (offset[1]=4 < offset[0]=8 → non-monotonic)
    t[os..os+4].copy_from_slice(&8u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&4u32.to_le_bytes());
    t[os+8..os+12].copy_from_slice(&8u32.to_le_bytes());
    t
}

/// Tuple with offset pointing beyond the buffer.
fn make_out_of_bounds_tuple() -> Vec<u8> {
    let num_cols = 1usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8usize;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let total = data_start + 4;
    let mut t = vec![0u8; total];
    t[0..4].copy_from_slice(&(total as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0;
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    // Sentinel offset points far beyond the buffer
    t[os+4..os+8].copy_from_slice(&9999u32.to_le_bytes());
    t
}

// ── tests ────────────────────────────────────────────────────────────────────

#[test]
fn test_get_value_and_get_value_fast_agree() {
    // Both methods must return the same Value for a valid INT column.
    let t = make_int_tuple(42);
    let accessor = TupleAccessor::new(&t, 1).unwrap();
    let by_str  = accessor.get_value(0, "INT").unwrap();
    let by_fast = accessor.get_value_fast(0, &DataType::Int).unwrap();
    assert_eq!(by_str, by_fast);
    assert_eq!(by_str, Value::Int(42));
}

#[test]
fn test_get_value_fast_float_agrees_with_get_value() {
    // Build a 1-column FLOAT tuple.
    let num_cols = 1usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8usize;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let total = data_start + 8;
    let mut t = vec![0u8; total];
    t[0..4].copy_from_slice(&(total as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0;
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&8u32.to_le_bytes());
    t[data_start..data_start+8].copy_from_slice(&3.14f64.to_le_bytes());

    let accessor = TupleAccessor::new(&t, 1).unwrap();
    let by_str  = accessor.get_value(0, "FLOAT").unwrap();
    let by_fast = accessor.get_value_fast(0, &DataType::Float).unwrap();
    assert_eq!(by_str, by_fast);
    assert_eq!(by_str, Value::Float(3.14));
}

#[test]
fn test_get_value_null_returns_value_null() {
    let t = make_null_int_tuple();
    let accessor = TupleAccessor::new(&t, 1).unwrap();
    assert_eq!(accessor.get_value(0, "INT").unwrap(), Value::Null);
    assert_eq!(accessor.get_value_fast(0, &DataType::Int).unwrap(), Value::Null);
}

#[test]
fn test_is_null_correct_for_non_null_column() {
    let t = make_int_tuple(99);
    let accessor = TupleAccessor::new(&t, 1).unwrap();
    assert_eq!(accessor.is_null(0).unwrap(), false);
}

#[test]
fn test_is_null_correct_for_null_column() {
    let t = make_null_int_tuple();
    let accessor = TupleAccessor::new(&t, 1).unwrap();
    assert_eq!(accessor.is_null(0).unwrap(), true);
}

#[test]
fn test_invalid_column_index_is_null() {
    let t = make_int_tuple(1);
    let accessor = TupleAccessor::new(&t, 1).unwrap();
    assert_eq!(accessor.is_null(99).unwrap_err(), TupleError::InvalidColumnIndex);
}

#[test]
fn test_invalid_column_index_get_value() {
    let t = make_int_tuple(1);
    let accessor = TupleAccessor::new(&t, 1).unwrap();
    assert!(accessor.get_value(99, "INT").is_err());
}

#[test]
fn test_non_monotonic_offsets_rejected() {
    let t = make_non_monotonic_tuple();
    let result = TupleAccessor::new(&t, 2);
    assert_eq!(result.err().unwrap(), TupleError::OffsetNotMonotonic,
        "Non-monotonic offset array must be rejected");
}

#[test]
fn test_offset_out_of_bounds_rejected() {
    let t = make_out_of_bounds_tuple();
    let result = TupleAccessor::new(&t, 1);
    assert_eq!(result.err().unwrap(), TupleError::OffsetOutOfBounds,
        "Offset exceeding tuple length must be rejected");
}

#[test]
fn test_num_columns_matches_schema() {
    let t = make_two_int_tuple(1, 2);
    let accessor = TupleAccessor::new(&t, 2).unwrap();
    assert_eq!(accessor.num_columns(), 2);
}

#[test]
fn test_tuple_length_matches_buffer() {
    let t = make_int_tuple(7);
    let accessor = TupleAccessor::new(&t, 1).unwrap();
    assert_eq!(accessor.tuple_length() as usize, t.len());
}

#[test]
fn test_column_count_mismatch_rejected() {
    // Tuple built with 1 col but accessor asked for 2 cols → IncompleteOffsetArray.
    let t = make_int_tuple(5);
    let result = TupleAccessor::new(&t, 2);
    assert!(result.is_err(), "Column count mismatch must be rejected");
}

#[test]
fn test_get_field_bytes_returns_correct_slice() {
    let t = make_int_tuple(300);
    let accessor = TupleAccessor::new(&t, 1).unwrap();
    let bytes = accessor.get_field_bytes(0).unwrap();
    let decoded = i32::from_le_bytes(bytes.try_into().unwrap());
    assert_eq!(decoded, 300);
}

#[test]
fn test_new_unchecked_reads_same_value_as_new() {
    // new_unchecked skips validation but must read the same value.
    let t = make_int_tuple(77);
    let checked   = TupleAccessor::new(&t, 1).unwrap();
    let unchecked = TupleAccessor::new_unchecked(&t, 1);
    assert_eq!(
        checked.get_value(0, "INT").unwrap(),
        unchecked.get_value(0, "INT").unwrap()
    );
}
