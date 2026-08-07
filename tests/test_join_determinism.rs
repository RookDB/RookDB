//! A join must return the same rows in the same order every time it runs.
//!
//! Hash joins bucket rows in a `HashMap`, whose iteration order is randomised
//! per process. If that order ever reached the output, results would differ
//! between runs and every downstream comparison would become flaky. Emission
//! order is therefore contractual: probe order first, then build-insertion
//! order within a bucket.

#[path = "join_common/mod.rs"]
mod common;

use common::{TempDb, col, collect_rows, eq};
use storage_manager::join::{JoinBuilder, JoinType};
use storage_manager::types::{DataType, DataValue};

fn int(value: i32) -> Option<DataValue> {
    Some(DataValue::Int(value))
}

/// Repeated runs of the same join produce byte-identical output, in order.
#[test]
fn repeated_runs_produce_identical_output() {
    let db = TempDb::new();

    let mut left = db.create_table("l", &[("k", DataType::Int), ("v", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int), ("v", DataType::Int)]);

    // Duplicate keys on both sides, so ordering within a match group matters.
    for i in 0..40 {
        left.insert(vec![int(i % 7), int(i)]);
        right.insert(vec![int(i % 5), int(i)]);
    }
    left.flush();
    right.flush();

    for join_type in [
        JoinType::Inner,
        JoinType::LeftOuter,
        JoinType::RightOuter,
        JoinType::FullOuter,
        JoinType::Semi,
        JoinType::Anti,
    ] {
        let run = || {
            collect_rows(
                JoinBuilder::new(left.table_ref(), right.table_ref(), join_type)
                    .with_condition(eq(col("l.k"), col("r.k")))
                    .execute()
                    .expect("plans"),
            )
            .expect("runs")
        };

        let first = run();
        assert!(
            !first.is_empty(),
            "{join_type:?} produced nothing to compare"
        );

        for attempt in 1..10 {
            assert_eq!(
                run(),
                first,
                "{join_type:?} differed on run {attempt}; output order is not stable"
            );
        }
    }
}

/// Changing the block size changes how the work is batched, but must not
/// change which rows come out.
#[test]
fn block_size_does_not_change_the_result_set() {
    let db = TempDb::new();

    let mut left = db.create_table("l", &[("k", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int)]);
    for i in 0..25 {
        left.insert(vec![int(i % 6)]);
        right.insert(vec![int(i % 4)]);
    }
    left.flush();
    right.flush();

    for join_type in [
        JoinType::Inner,
        JoinType::LeftOuter,
        JoinType::RightOuter,
        JoinType::FullOuter,
        JoinType::Semi,
        JoinType::Anti,
    ] {
        let run = |block_rows: usize| {
            let mut rows = collect_rows(
                JoinBuilder::new(left.table_ref(), right.table_ref(), join_type)
                    .with_condition(eq(col("l.k"), col("r.k")))
                    .with_block_rows(block_rows)
                    .execute()
                    .expect("plans"),
            )
            .expect("runs");
            // Blocking reorders output within a block, so compare as a
            // multiset; what must not change is the multiset itself.
            rows.sort_by_key(|row| format!("{row:?}"));
            rows
        };

        let baseline = run(1);
        for block_rows in [2, 3, 7, 1024] {
            assert_eq!(
                run(block_rows),
                baseline,
                "{join_type:?} changed with block_rows={block_rows}"
            );
        }
    }
}
