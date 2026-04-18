//! Tests for TupleHeader encode/decode and null bitmap.

use storage_manager::page::tuple::TupleHeader;

#[test]
fn test_null_bitmap_set_clear() {
    let mut h = TupleHeader::new(10, 0);
    assert!(!h.is_null(0));
    h.set_null(0);
    assert!(h.is_null(0));
    assert!(!h.is_null(1));
    h.set_null(9);
    assert!(h.is_null(9));
    h.clear_null(0);
    assert!(!h.is_null(0));
}

#[test]
fn test_encode_decode_no_var_cols() {
    let mut h = TupleHeader::new(4, 0);
    h.set_null(1);
    h.set_null(3);
    let encoded = h.encode();
    let decoded = TupleHeader::decode(&encoded, 4, 0);
    assert!(decoded.is_null(1));
    assert!(decoded.is_null(3));
    assert!(!decoded.is_null(0));
    assert!(!decoded.is_null(2));
}

#[test]
fn test_encode_decode_with_var_offsets() {
    let mut h = TupleHeader::new(3, 2);
    h.var_col_offsets[0] = 12;
    h.var_col_offsets[1] = 50;
    h.set_null(2);
    let encoded = h.encode();
    let decoded = TupleHeader::decode(&encoded, 3, 2);
    assert_eq!(decoded.var_col_offsets[0], 12);
    assert_eq!(decoded.var_col_offsets[1], 50);
    assert!(decoded.is_null(2));
}

#[test]
fn test_header_size_calculation() {
    // 4 cols → 1 byte bitmap; 2 var cols → 8 bytes offsets → total 9
    assert_eq!(TupleHeader::header_size(4, 2), 9);
    // 9 cols → 2 byte bitmap; 0 var cols → total 2
    assert_eq!(TupleHeader::header_size(9, 0), 2);
    // 8 cols → 1 byte bitmap; 3 var cols → 12 bytes offsets → total 13
    assert_eq!(TupleHeader::header_size(8, 3), 13);
}

#[test]
fn test_all_nulls() {
    let mut h = TupleHeader::new(8, 0);
    for i in 0..8 {
        h.set_null(i);
    }
    let encoded = h.encode();
    let decoded = TupleHeader::decode(&encoded, 8, 0);
    for i in 0..8 {
        assert!(decoded.is_null(i), "column {} should be null", i);
    }
}
