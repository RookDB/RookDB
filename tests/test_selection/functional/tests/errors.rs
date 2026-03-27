// Error handling tests
// Verifies that all documented error paths surface the correct error variants.

use storage_manager::catalog::{Column, Table};
use storage_manager::executor::selection::*;

fn int_schema() -> Table {
    Table { columns: vec![Column { name: "id".to_string(), data_type: "INT".to_string() }] }
}

fn text_schema() -> Table {
    Table { columns: vec![Column { name: "name".to_string(), data_type: "TEXT".to_string() }] }
}

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

// ── Planning-time (SelectionExecutor::new) errors ─────────────────────────────

#[test]
fn test_error_column_not_found() {
    // Referencing a column that doesn't exist in schema → Err at planning time.
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("nonexistent".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let result = SelectionExecutor::new(pred, schema);
    assert!(result.is_err(), "Unknown column name must cause planning error");
}

#[test]
fn test_error_text_column_in_arithmetic() {
    // TEXT column used in Add → type mismatch → Err.
    let schema = text_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Add(
            Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
            Box::new(Expr::Constant(Constant::Int(1))),
        )),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let result = SelectionExecutor::new(pred, schema);
    assert!(result.is_err(), "TEXT in arithmetic must fail at planning");
}

#[test]
fn test_error_text_column_vs_int_constant() {
    // TEXT column compared directly with INT constant → type mismatch → Err.
    let schema = text_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("name".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(5))),
    );
    let result = SelectionExecutor::new(pred, schema);
    assert!(result.is_err(), "TEXT vs INT comparison must fail at planning");
}

// ── TupleAccessor errors ──────────────────────────────────────────────────────

#[test]
fn test_error_tuple_too_short() {
    // Slice is shorter than minimum tuple size for 1 column.
    let tiny = vec![0u8; 4]; // far too small
    let result = TupleAccessor::new(&tiny, 1);
    assert_eq!(result.err().unwrap(), TupleError::TupleTooShort);
}

#[test]
fn test_error_length_mismatch() {
    // Header reports a different length than the actual slice.
    let mut t = make_int_tuple(1);
    // Write a wrong value into the header length field.
    let wrong_len = (t.len() as u32) + 10;
    t[0..4].copy_from_slice(&wrong_len.to_le_bytes());
    let result = TupleAccessor::new(&t, 1);
    assert_eq!(result.err().unwrap(), TupleError::LengthMismatch);
}

#[test]
fn test_error_column_count_mismatch_incomplete_offset_array() {
    // Tuple says 1 column in header, but accessor asked for 2 → IncompleteOffsetArray.
    let t = make_int_tuple(1);
    let result = TupleAccessor::new(&t, 2);
    assert_eq!(result.err().unwrap(), TupleError::IncompleteOffsetArray);
}

#[test]
fn test_error_offset_not_monotonic() {
    // Build tuple with reversed offsets.
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
    // offset[0]=8, offset[1]=4 — non-monotonic
    t[os..os+4].copy_from_slice(&8u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&4u32.to_le_bytes());
    t[os+8..os+12].copy_from_slice(&8u32.to_le_bytes());
    let result = TupleAccessor::new(&t, 2);
    assert_eq!(result.err().unwrap(), TupleError::OffsetNotMonotonic);
}

#[test]
fn test_error_offset_out_of_bounds() {
    // Sentinel offset points beyond the buffer.
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
    t[os+4..os+8].copy_from_slice(&9999u32.to_le_bytes()); // out of bounds
    let result = TupleAccessor::new(&t, 1);
    assert_eq!(result.err().unwrap(), TupleError::OffsetOutOfBounds);
}

#[test]
fn test_error_invalid_column_index_is_null() {
    let t = make_int_tuple(0);
    let accessor = TupleAccessor::new(&t, 1).unwrap();
    assert_eq!(accessor.is_null(1).unwrap_err(), TupleError::InvalidColumnIndex);
}

#[test]
fn test_error_invalid_column_index_get_field_bytes() {
    let t = make_int_tuple(0);
    let accessor = TupleAccessor::new(&t, 1).unwrap();
    assert_eq!(accessor.get_field_bytes(1).unwrap_err(), TupleError::InvalidColumnIndex);
}

// ── runtime evaluate_tuple errors ─────────────────────────────────────────────

#[test]
fn test_runtime_wrong_field_size_returns_err() {
    // Build a tuple where the INT field is only 2 bytes (should be 4).
    // evaluate_tuple will call get_value_fast which expects 4 bytes and
    // returns Err(FieldRegionOutOfBounds).
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();

    // Build a 1-column tuple but with only 2 bytes of field data instead of 4.
    let num_cols = 1usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8usize;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let field_size = 2usize; // deliberately wrong: INT needs 4 bytes
    let total = data_start + field_size;
    let mut t = vec![0u8; total];
    t[0..4].copy_from_slice(&(total as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0; // not null
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&(field_size as u32).to_le_bytes()); // sentinel: 2 bytes
    // Leave field data as zeros (2 bytes)

    let result = executor.evaluate_tuple(&t);
    assert!(result.is_err(),
        "INT field with 2 bytes instead of 4 must cause evaluate_tuple to return Err");
}
