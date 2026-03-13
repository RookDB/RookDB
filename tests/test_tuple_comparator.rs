//! Tests for TupleComparator: INT/TEXT comparisons, multi-column sort keys, ASC/DESC.

use std::cmp::Ordering;

use storage_manager::catalog::types::{Column, SortDirection, SortKey};
use storage_manager::sorting::comparator::{column_byte_size, TupleComparator};

/// Helper: build a tuple from (i32, &str) for schema (INT, TEXT).
fn make_tuple_int_text(id: i32, name: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(14); // 4 + 10
    buf.extend_from_slice(&id.to_le_bytes());
    let mut name_bytes = name.as_bytes().to_vec();
    name_bytes.resize(10, b' ');
    buf.extend_from_slice(&name_bytes[..10]);
    buf
}

/// Standard (INT, TEXT) schema used by most tests.
fn schema_int_text() -> Vec<Column> {
    vec![
        Column {
            name: "id".to_string(),
            data_type: "INT".to_string(),
        },
        Column {
            name: "name".to_string(),
            data_type: "TEXT".to_string(),
        },
    ]
}

// ---- column_byte_size ----

#[test]
fn test_column_byte_size_int() {
    assert_eq!(column_byte_size("INT"), 4);
}

#[test]
fn test_column_byte_size_text() {
    assert_eq!(column_byte_size("TEXT"), 10);
}

#[test]
#[should_panic(expected = "Unsupported data type: BLOB")]
fn test_column_byte_size_unknown() {
    column_byte_size("BLOB");
}

// ---- TupleComparator::new ----

#[test]
fn test_comparator_precomputes_offsets() {
    let cols = schema_int_text();
    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];
    let cmp = TupleComparator::new(cols, sort_keys);

    assert_eq!(cmp.column_offsets, vec![0, 4]);
    assert_eq!(cmp.tuple_size, 14);
}

// ---- INT ASC comparison ----

#[test]
fn test_compare_int_asc_less() {
    let cols = schema_int_text();
    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];
    let cmp = TupleComparator::new(cols, sort_keys);

    let a = make_tuple_int_text(5, "Alice");
    let b = make_tuple_int_text(10, "Bob");
    assert_eq!(cmp.compare(&a, &b), Ordering::Less);
}

#[test]
fn test_compare_int_asc_greater() {
    let cols = schema_int_text();
    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];
    let cmp = TupleComparator::new(cols, sort_keys);

    let a = make_tuple_int_text(20, "Alice");
    let b = make_tuple_int_text(10, "Bob");
    assert_eq!(cmp.compare(&a, &b), Ordering::Greater);
}

#[test]
fn test_compare_int_asc_equal() {
    let cols = schema_int_text();
    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];
    let cmp = TupleComparator::new(cols, sort_keys);

    let a = make_tuple_int_text(10, "Alice");
    let b = make_tuple_int_text(10, "Bob");
    assert_eq!(cmp.compare(&a, &b), Ordering::Equal);
}

// ---- INT DESC comparison ----

#[test]
fn test_compare_int_desc_reverses() {
    let cols = schema_int_text();
    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Descending,
    }];
    let cmp = TupleComparator::new(cols, sort_keys);

    let a = make_tuple_int_text(5, "Alice");
    let b = make_tuple_int_text(10, "Bob");
    // In DESC, 5 > 10 reversed => Greater
    assert_eq!(cmp.compare(&a, &b), Ordering::Greater);
}

// ---- TEXT ASC comparison ----

#[test]
fn test_compare_text_asc() {
    let cols = schema_int_text();
    let sort_keys = vec![SortKey {
        column_index: 1,
        direction: SortDirection::Ascending,
    }];
    let cmp = TupleComparator::new(cols, sort_keys);

    let a = make_tuple_int_text(1, "Alice");
    let b = make_tuple_int_text(2, "Bob");
    // "Alice     " < "Bob       " lexicographically
    assert_eq!(cmp.compare(&a, &b), Ordering::Less);
}

#[test]
fn test_compare_text_desc() {
    let cols = schema_int_text();
    let sort_keys = vec![SortKey {
        column_index: 1,
        direction: SortDirection::Descending,
    }];
    let cmp = TupleComparator::new(cols, sort_keys);

    let a = make_tuple_int_text(1, "Alice");
    let b = make_tuple_int_text(2, "Bob");
    assert_eq!(cmp.compare(&a, &b), Ordering::Greater);
}

// ---- Multi-column sort key (tie-breaking) ----

#[test]
fn test_compare_multicolumn_tiebreak() {
    let cols = schema_int_text();
    // Sort by name ASC first, then id ASC
    let sort_keys = vec![
        SortKey {
            column_index: 1,
            direction: SortDirection::Ascending,
        },
        SortKey {
            column_index: 0,
            direction: SortDirection::Ascending,
        },
    ];
    let cmp = TupleComparator::new(cols, sort_keys);

    // Same name, different id
    let a = make_tuple_int_text(3, "Alice");
    let b = make_tuple_int_text(7, "Alice");
    assert_eq!(cmp.compare(&a, &b), Ordering::Less); // 3 < 7
}

#[test]
fn test_compare_multicolumn_primary_wins() {
    let cols = schema_int_text();
    // Sort by name ASC, id ASC
    let sort_keys = vec![
        SortKey {
            column_index: 1,
            direction: SortDirection::Ascending,
        },
        SortKey {
            column_index: 0,
            direction: SortDirection::Ascending,
        },
    ];
    let cmp = TupleComparator::new(cols, sort_keys);

    let a = make_tuple_int_text(100, "Alice");
    let b = make_tuple_int_text(1, "Bob");
    // Primary: "Alice" < "Bob" => Less (id doesn't matter)
    assert_eq!(cmp.compare(&a, &b), Ordering::Less);
}

#[test]
fn test_compare_multicolumn_mixed_directions() {
    let cols = schema_int_text();
    // Sort by name ASC, id DESC
    let sort_keys = vec![
        SortKey {
            column_index: 1,
            direction: SortDirection::Ascending,
        },
        SortKey {
            column_index: 0,
            direction: SortDirection::Descending,
        },
    ];
    let cmp = TupleComparator::new(cols, sort_keys);

    // Same name, id 7 vs 3 — in DESC, 7 should come first (Less)
    let a = make_tuple_int_text(7, "Alice");
    let b = make_tuple_int_text(3, "Alice");
    assert_eq!(cmp.compare(&a, &b), Ordering::Less); // 7 DESC < 3 => Less
}

// ---- Negative INT values ----

#[test]
fn test_compare_negative_int_asc() {
    let cols = schema_int_text();
    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];
    let cmp = TupleComparator::new(cols, sort_keys);

    let a = make_tuple_int_text(-5, "Alice");
    let b = make_tuple_int_text(3, "Bob");
    assert_eq!(cmp.compare(&a, &b), Ordering::Less);
}

// ---- Duplicate keys ----

#[test]
fn test_compare_all_equal() {
    let cols = schema_int_text();
    let sort_keys = vec![
        SortKey {
            column_index: 0,
            direction: SortDirection::Ascending,
        },
        SortKey {
            column_index: 1,
            direction: SortDirection::Ascending,
        },
    ];
    let cmp = TupleComparator::new(cols, sort_keys);

    let a = make_tuple_int_text(42, "Hello");
    let b = make_tuple_int_text(42, "Hello");
    assert_eq!(cmp.compare(&a, &b), Ordering::Equal);
}

// ---- compare_key ----

#[test]
fn test_compare_key_int() {
    let cols = schema_int_text();
    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];
    let cmp = TupleComparator::new(cols, sort_keys);

    let tuple = make_tuple_int_text(15, "Alice");
    let key = 10i32.to_le_bytes().to_vec();
    assert_eq!(cmp.compare_key(&tuple, 0, &key), Ordering::Greater);
}

#[test]
fn test_compare_key_text() {
    let cols = schema_int_text();
    let sort_keys = vec![SortKey {
        column_index: 1,
        direction: SortDirection::Ascending,
    }];
    let cmp = TupleComparator::new(cols, sort_keys);

    let tuple = make_tuple_int_text(1, "Alice");
    let mut key = b"Bob".to_vec();
    key.resize(10, b' ');
    assert_eq!(cmp.compare_key(&tuple, 0, &key), Ordering::Less);
}

// ---- extract_key ----

#[test]
fn test_extract_key_int() {
    let cols = schema_int_text();
    let sort_keys = vec![SortKey {
        column_index: 0,
        direction: SortDirection::Ascending,
    }];
    let cmp = TupleComparator::new(cols, sort_keys);

    let tuple = make_tuple_int_text(42, "Hello");
    let key = cmp.extract_key(&tuple, 0);
    assert_eq!(key, 42i32.to_le_bytes().to_vec());
}

#[test]
fn test_extract_key_text() {
    let cols = schema_int_text();
    let sort_keys = vec![SortKey {
        column_index: 1,
        direction: SortDirection::Ascending,
    }];
    let cmp = TupleComparator::new(cols, sort_keys);

    let tuple = make_tuple_int_text(1, "Hello");
    let key = cmp.extract_key(&tuple, 0);
    let mut expected = b"Hello".to_vec();
    expected.resize(10, b' ');
    assert_eq!(key, expected);
}

// ---- Edge: TEXT with special characters ----

#[test]
fn test_compare_text_space_padding() {
    let cols = schema_int_text();
    let sort_keys = vec![SortKey {
        column_index: 1,
        direction: SortDirection::Ascending,
    }];
    let cmp = TupleComparator::new(cols, sort_keys);

    // "A" padded = "A         ", "AB" padded = "AB        "
    let a = make_tuple_int_text(1, "A");
    let b = make_tuple_int_text(1, "AB");
    // 'A' == 'A', then ' ' (0x20) vs 'B' (0x42) => Less
    assert_eq!(cmp.compare(&a, &b), Ordering::Less);
}
