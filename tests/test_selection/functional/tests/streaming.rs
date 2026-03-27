// Streaming and iterator tests
// Tests filter_tuples_streaming (callback-based, non-buffering) and filter_iter (lazy iterator).

use storage_manager::catalog::{Column, Table};
use storage_manager::executor::selection::*;

fn int_schema() -> Table {
    Table { columns: vec![Column { name: "id".to_string(), data_type: "INT".to_string() }] }
}

fn make_int_tuple(id: i32) -> Vec<u8> {
    let num_cols = 1usize;
    let header_size = 8usize;
    let null_bitmap_len = 1;
    let offset_array_len = (num_cols + 1) * 4;
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

/// Wraps a Vec<Vec<u8>> into an iterator of Result<Vec<u8>, String>.
struct TupleStream {
    data: std::vec::IntoIter<Vec<u8>>,
}

impl TupleStream {
    fn new(v: Vec<Vec<u8>>) -> Self {
        TupleStream { data: v.into_iter() }
    }
    /// Returns a stream that yields one Err then ends.
    fn with_error(good: Vec<u8>, bad_message: &'static str) -> impl Iterator<Item = Result<Vec<u8>, String>> {
        vec![
            Ok(good),
            Err(bad_message.to_string()),
        ].into_iter()
    }
}

impl Iterator for TupleStream {
    type Item = Result<Vec<u8>, String>;
    fn next(&mut self) -> Option<Self::Item> {
        self.data.next().map(Ok)
    }
}

// ── filter_tuples_streaming ───────────────────────────────────────────────────

#[test]
fn test_streaming_count_matches_filter_tuples() {
    // filter_tuples_streaming count must equal filter_tuples count for the same input.
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let tuples = vec![
        make_int_tuple(10),
        make_int_tuple(60),
        make_int_tuple(70),
        make_int_tuple(40),
        make_int_tuple(80),
    ];
    // batch count
    let batch_count = filter_tuples(&executor, &tuples).unwrap().len();
    // streaming count
    let stream = TupleStream::new(tuples);
    let mut callback_count = 0usize;
    let returned_count = filter_tuples_streaming(&executor, stream, |_| callback_count += 1).unwrap();
    assert_eq!(batch_count, 3);
    assert_eq!(returned_count, 3);
    assert_eq!(callback_count, 3);
}

#[test]
fn test_streaming_callback_receives_matched_tuples() {
    // Callback should only be called for matching tuples (id > 50).
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let tuples = vec![make_int_tuple(30), make_int_tuple(60), make_int_tuple(90)];
    let stream = TupleStream::new(tuples);
    let mut received_ids: Vec<i32> = Vec::new();
    filter_tuples_streaming(&executor, stream, |raw| {
        let accessor = TupleAccessor::new(raw, 1).unwrap();
        if let Value::Int(v) = accessor.get_value(0, "INT").unwrap() {
            received_ids.push(v);
        }
    }).unwrap();
    assert_eq!(received_ids, vec![60, 90], "Only ids > 50 should reach the callback");
}

#[test]
fn test_streaming_empty_input() {
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let stream = TupleStream::new(vec![]);
    let mut count = 0usize;
    let returned = filter_tuples_streaming(&executor, stream, |_| count += 1).unwrap();
    assert_eq!(returned, 0);
    assert_eq!(count, 0);
}

#[test]
fn test_streaming_none_match() {
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(1000))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let tuples = vec![make_int_tuple(1), make_int_tuple(2), make_int_tuple(3)];
    let stream = TupleStream::new(tuples);
    let mut count = 0usize;
    let returned = filter_tuples_streaming(&executor, stream, |_| count += 1).unwrap();
    assert_eq!(returned, 0);
    assert_eq!(count, 0);
}

#[test]
fn test_streaming_error_propagated() {
    // If the iterator yields Err, filter_tuples_streaming must return Err.
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let good = make_int_tuple(5);
    let stream = TupleStream::with_error(good, "simulated stream error");
    let result = filter_tuples_streaming(&executor, stream, |_| {});
    assert!(result.is_err(), "Err from iterator must propagate out of filter_tuples_streaming");
}

// ── filter_iter ───────────────────────────────────────────────────────────────

#[test]
fn test_filter_iter_lazy_filtering() {
    // filter_iter yields only tuples where predicate is True (id > 50).
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(50))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let tuples = vec![
        make_int_tuple(10),
        make_int_tuple(70),
        make_int_tuple(30),
        make_int_tuple(90),
    ];
    let stream = TupleStream::new(tuples);
    let results: Vec<_> = filter_iter(&executor, stream).collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(results.len(), 2, "Only ids 70 and 90 should pass");
}

#[test]
fn test_filter_iter_error_propagated() {
    // An Err in the source iterator propagates through filter_iter.
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let good = make_int_tuple(100);
    let stream = TupleStream::with_error(good, "iterator error");
    let results: Vec<_> = filter_iter(&executor, stream).collect();
    let has_err = results.iter().any(|r| r.is_err());
    assert!(has_err, "Err in source iterator should propagate through filter_iter");
}

#[test]
fn test_filter_iter_empty_yields_nothing() {
    let schema = int_schema();
    let pred = Predicate::Compare(
        Box::new(Expr::Column(ColumnReference::new("id".to_string()))),
        ComparisonOp::GreaterThan,
        Box::new(Expr::Constant(Constant::Int(0))),
    );
    let executor = SelectionExecutor::new(pred, schema).unwrap();
    let stream = TupleStream::new(vec![]);
    let results: Vec<_> = filter_iter(&executor, stream).collect();
    assert!(results.is_empty());
}
