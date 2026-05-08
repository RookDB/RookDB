// ============================================================================
// 0_surjit/selection_test/mod.rs
//
// Entry point for the SelectionExecutor correctness test suite.
//
// Structure:
//   mod.rs              — shared helpers (make_schema, make_table, serialize)
//   test_basic.rs       — basic predicate filtering
//   test_null_logic.rs  — SQL 3-valued logic / NULL semantics
//   test_arithmetic.rs  — arithmetic ops and edge cases
//   test_in_like.rs     — IN clause and LIKE pattern matching
//   test_short_circuit.rs — AND / OR short-circuit jump correctness
//   test_varlen.rs      — variable-length column extraction
//   test_streaming.rs   — streaming / iterator filter APIs
//
// All sub-modules are integration tests that live in `tests/` in the actual
// cargo project. Each file in this folder is mirrored there so that
// `cargo test` can discover and run it. The content in this folder is the
// authoritative source.
// ============================================================================

// ── Shared helpers ────────────────────────────────────────────────────────────

/// Build a `catalog::types::Table` from a list of `(name, DataType)` pairs.
///
/// # Example
/// ```rust
/// let tbl = make_table(&[("age", DataType::Int), ("name", DataType::Varchar(32))]);
/// ```
#[macro_export]
macro_rules! make_table {
    ( $( ($name:expr, $ty:expr) ),* $(,)? ) => {{
        use storage_manager::catalog::types::{Column, Table};
        Table {
            columns: vec![
                $( Column::new($name.to_string(), $ty), )*
            ],
        }
    }};
}

/// Shortcut: serialize a row from string literals using the public row API.
/// Panics if serialization fails (test setup failure, not the thing under test).
#[macro_export]
macro_rules! make_row {
    ($schema:expr, $( $val:expr ),* $(,)?) => {{
        use storage_manager::types::serialize_nullable_row;
        let vals: Vec<Option<&str>> = vec![ $( $val ),* ];
        serialize_nullable_row($schema, &vals).expect("make_row: serialization failed")
    }};
}
