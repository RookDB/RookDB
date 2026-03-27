// Execution correctness tests (extended)
// Verifies the actual CONTENT of returned tuples, not just their count.
// Complements execution.rs which only checked len().

use storage_manager::catalog::{Column, Table};
use storage_manager::executor::selection::*;

fn schema_id_name() -> Table {
    Table {
        columns: vec![
            Column { name: "id".to_string(),   data_type: "INT".to_string()  },
            Column { name: "name".to_string(), data_type: "TEXT".to_string() },
        ],
    }
}

fn make_tuple(id: i32, name: &str) -> Vec<u8> {
    let num_cols = 2usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
    let header_size = 8usize;
    let data_start = header_size + null_bitmap_len + offset_array_len;
    let name_bytes = name.as_bytes();
    let total_length = data_start + 4 + name_bytes.len();
    let mut t = vec![0u8; total_length];
    t[0..4].copy_from_slice(&(total_length as u32).to_le_bytes());
    t[4] = 1; t[5] = 0;
    t[6..8].copy_from_slice(&(num_cols as u16).to_le_bytes());
    t[8] = 0;
    let os = 9usize;
    t[os..os+4].copy_from_slice(&0u32.to_le_bytes());
    t[os+4..os+8].copy_from_slice(&4u32.to_le_bytes());
    t[os+8..os+12].copy_from_slice(&((4 + name_bytes.len()) as u32).to_le_bytes());
    t[data_start..data_start+4].copy_from_slice(&id.to_le_bytes());
    t[data_start+4..data_start+4+name_bytes.len()].copy_from_slice(name_bytes);
    t
}

fn decode(tuple: &[u8]) -> (i32, String) {
    let accessor = TupleAccessor::new(tuple, 2).unwrap();
    let id = match accessor.get_value(0, "INT").unwrap() {
        Value::Int(v) => v,
        _ => panic!("Expected Int"),
    };
    let name = match accessor.get_value(1, "TEXT").unwrap() {
        Value::Text(s) => s,
        _ => panic!("Expected Text"),
    };
    (id, name)
}

// ── tests ────────────────────────────────────────────────────────────────────

#[test]
fn test_filter_tuples_content_correct_ids() {
    // id > 50: only Alice(60) and Charlie(70) should be in the result
    let schema = schema_id_name();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let tuples = vec![
        make_tuple(30, "Dave"),
        make_tuple(60, "Alice"),
        make_tuple(70, "Charlie"),
        make_tuple(40, "Bob"),
    ];
    let filtered = filter_tuples(&executor, &tuples).unwrap();
    assert_eq!(filtered.len(), 2);
    let (id0, name0) = decode(&filtered[0]);
    let (id1, name1) = decode(&filtered[1]);
    assert_eq!(id0, 60);   assert_eq!(name0, "Alice");
    assert_eq!(id1, 70);   assert_eq!(name1, "Charlie");
}

#[test]
fn test_filter_tuples_preserves_order() {
    // Order of matching tuples must preserve input order.
    let schema = schema_id_name();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let tuples = vec![
        make_tuple(3, "C"),
        make_tuple(1, "A"),
        make_tuple(2, "B"),
    ];
    let filtered = filter_tuples(&executor, &tuples).unwrap();
    let ids: Vec<i32> = filtered.iter().map(|r| decode(r).0).collect();
    assert_eq!(ids, vec![3, 1, 2], "Output order must match input order");
}

#[test]
fn test_filter_tuples_detailed_content_matched() {
    // filter_tuples_detailed: matched bucket must contain tuples with id > 50.
    let schema = schema_id_name();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let tuples = vec![
        make_tuple(20, "A"),
        make_tuple(80, "B"),
        make_tuple(10, "C"),
        make_tuple(90, "D"),
    ];
    let (matched, rejected, unknown) = filter_tuples_detailed(&executor, tuples).unwrap();
    assert_eq!(matched.len(), 2);
    assert_eq!(rejected.len(), 2);
    assert_eq!(unknown.len(), 0);
    // Verify matched content
    let matched_ids: Vec<i32> = matched.iter().map(|r| decode(r).0).collect();
    assert!(matched_ids.contains(&80));
    assert!(matched_ids.contains(&90));
    // Verify rejected content
    let rejected_ids: Vec<i32> = rejected.iter().map(|r| decode(r).0).collect();
    assert!(rejected_ids.contains(&20));
    assert!(rejected_ids.contains(&10));
}

#[test]
fn test_filter_tuples_detailed_unknown_bucket() {
    // Tuples with NULL id go into unknown bucket.
    let schema = schema_id_name();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();

    // Build tuple with NULL id
    let mut null_tuple = make_tuple(0, "Alice");
    null_tuple[8] = 0b00000001; // bit 0 = id is NULL

    let tuples = vec![make_tuple(5, "Bob"), null_tuple.clone()];
    let (matched, rejected, unknown) = filter_tuples_detailed(&executor, tuples).unwrap();
    assert_eq!(matched.len(), 1);
    assert_eq!(rejected.len(), 0);
    assert_eq!(unknown.len(), 1);
    // The unknown tuple should be our null_tuple
    assert_eq!(unknown[0], null_tuple);
}

#[test]
fn test_count_matching_tuples_correct_value() {
    // Verify count_matching_tuples returns exactly 3 for id > 40.
    let schema = schema_id_name();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(40))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let tuples = vec![
        make_tuple(10, "A"),
        make_tuple(50, "B"),
        make_tuple(60, "C"),
        make_tuple(30, "D"),
        make_tuple(70, "E"),
    ];
    let count = count_matching_tuples(&executor, &tuples).unwrap();
    assert_eq!(count, 3, "id > 40 should match 50, 60, 70");
}

#[test]
fn test_exact_tuple_bytes_returned() {
    // The Vec<u8> in filter result must be byte-identical to the input tuple.
    let schema = schema_id_name();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::Equals,
        Box::new(Expr::Constant(Constant::Int(42))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let orig = make_tuple(42, "Target");
    let decoy = make_tuple(99, "Other");
    let tuples = vec![decoy, orig.clone()];
    let filtered = filter_tuples(&executor, &tuples).unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0], orig, "Returned tuple bytes must be identical to the input");
}
