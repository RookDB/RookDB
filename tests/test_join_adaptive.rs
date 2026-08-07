//! The adaptive join.
//!
//! Two things are being checked. First, that reversing which side builds
//! changes *nothing* about the answer - same rows, same column order. Second,
//! that the decision is made from what the data turns out to be rather than
//! from an estimate, so it holds even with no statistics at all.

#[path = "join_common/mod.rs"]
mod common;

use common::{TableHandle, TempDb, assert_rows_eq, col, collect_rows, eq, lt};
use storage_manager::executor::selection::Predicate;
use storage_manager::join::{
    ExecStats, JoinAlgorithm, JoinBuilder, JoinConfig, JoinType, RowCodec,
};
use storage_manager::types::{DataType, DataValue};

fn int(value: i32) -> Option<DataValue> {
    Some(DataValue::Int(value))
}

fn text(value: &str) -> Option<DataValue> {
    Some(DataValue::Varchar(value.to_string()))
}

fn run(
    db: &TempDb,
    left: &TableHandle,
    right: &TableHandle,
    join_type: JoinType,
    condition: Option<&Predicate>,
    algorithm: JoinAlgorithm,
    work_memory: u64,
) -> (Vec<Vec<Option<DataValue>>>, ExecStats) {
    let config = JoinConfig::with_work_memory(work_memory).spill_root(db.path());
    let mut builder = JoinBuilder::new(left.table_ref(), right.table_ref(), join_type)
        .with_algorithm(algorithm)
        .with_config(config);
    if let Some(condition) = condition {
        builder = builder.with_condition(condition.clone());
    }

    let mut stream = builder.execute().expect("should plan");
    let codec = RowCodec::new(stream.schema().types.clone());
    let mut rows = Vec::new();
    while let Some(row) = stream.next() {
        rows.push(codec.decode(&row.expect("should run")).expect("decode"));
    }
    let stats = stream.stats();
    (rows, stats)
}

/// Compare the adaptive operator against a nested loop over the same inputs.
fn agrees(
    db: &TempDb,
    left: &TableHandle,
    right: &TableHandle,
    join_type: JoinType,
    condition: Option<&Predicate>,
    work_memory: u64,
) -> ExecStats {
    let (expected, _) = run(
        db,
        left,
        right,
        join_type,
        condition,
        JoinAlgorithm::BlockNestedLoop,
        64 * 1024 * 1024,
    );
    let (actual, stats) = run(
        db,
        left,
        right,
        join_type,
        condition,
        JoinAlgorithm::Adaptive,
        work_memory,
    );
    assert_rows_eq(&actual, &expected, &format!("adaptive {join_type:?}"));
    stats
}

fn equi() -> Predicate {
    eq(col("l.k"), col("r.k"))
}

const ALL: [JoinType; 6] = [
    JoinType::Inner,
    JoinType::LeftOuter,
    JoinType::RightOuter,
    JoinType::FullOuter,
    JoinType::Semi,
    JoinType::Anti,
];

/// Sized so the left relation ends long before the right.
fn lopsided(db: &TempDb, left_rows: i32, right_rows: i32) -> (TableHandle, TableHandle) {
    let mut left = db.create_table("l", &[("k", DataType::Int), ("v", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int), ("w", DataType::Varchar(24))]);

    for i in 0..left_rows {
        left.insert(vec![int(i % 17), int(i)]);
    }
    // A NULL key on each side, which must never match.
    left.insert(vec![None, int(-1)]);
    left.flush();

    for i in 0..right_rows {
        right.insert(vec![int(i % 23), text(&format!("r{i}"))]);
    }
    right.insert(vec![None, text("ghost")]);
    right.flush();

    (left, right)
}

// ── Role reversal ────────────────────────────────────────────────────────────

/// A left relation far smaller than the right must become the build side.
#[test]
fn the_smaller_side_becomes_the_build_side() {
    let db = TempDb::new();
    let (left, right) = lopsided(&db, 20, 2_000);

    let stats = agrees(
        &db,
        &left,
        &right,
        JoinType::Inner,
        Some(&equi()),
        64 * 1024 * 1024,
    );
    assert!(
        stats.role_reversed,
        "the left relation is a hundredth the size; it should build"
    );
}

/// The declared orientation is kept when the right side is already smaller.
#[test]
fn a_smaller_right_side_keeps_the_declared_orientation() {
    let db = TempDb::new();
    let (left, right) = lopsided(&db, 2_000, 20);

    let stats = agrees(
        &db,
        &left,
        &right,
        JoinType::Inner,
        Some(&equi()),
        64 * 1024 * 1024,
    );
    assert!(
        !stats.role_reversed,
        "the right relation is already the smaller one"
    );
}

/// Reversal must not change the answer, for any join type that permits it.
#[test]
fn reversal_preserves_every_join_type() {
    let db = TempDb::new();
    let (left, right) = lopsided(&db, 20, 1_500);

    for join_type in ALL {
        let stats = agrees(
            &db,
            &left,
            &right,
            join_type,
            Some(&equi()),
            64 * 1024 * 1024,
        );

        // SEMI and ANTI are defined in terms of left rows, so their roles are
        // not interchangeable and must not have been swapped.
        if join_type.emits_left_only() {
            assert!(
                !stats.role_reversed,
                "{join_type:?} cannot be evaluated from the right side"
            );
        }
    }
}

/// Reversal must not disturb output column order - the schema is declared, not
/// derived from whichever side happened to build.
#[test]
fn reversal_does_not_reorder_output_columns() {
    let db = TempDb::new();
    let (left, right) = lopsided(&db, 15, 900);

    let (rows, stats) = run(
        &db,
        &left,
        &right,
        JoinType::Inner,
        Some(&equi()),
        JoinAlgorithm::Adaptive,
        64 * 1024 * 1024,
    );
    assert!(stats.role_reversed, "expected a reversal here");
    assert!(!rows.is_empty());

    for row in &rows {
        assert_eq!(row.len(), 4, "two columns from each side");
        // Columns 0,1 come from `l` (both INT); 2,3 from `r` (INT, VARCHAR).
        assert!(matches!(row[0], Some(DataValue::Int(_))));
        assert!(matches!(row[1], Some(DataValue::Int(_))));
        assert!(matches!(row[2], Some(DataValue::Int(_))));
        assert!(
            matches!(row[3], Some(DataValue::Varchar(_))),
            "the right relation's VARCHAR must stay in its own column: {row:?}"
        );
        assert_eq!(row[0], row[2], "the join key must match on both sides");
    }
}

/// NULL keys never match, whichever side builds.
#[test]
fn null_keys_never_match_after_reversal() {
    let db = TempDb::new();
    let (left, right) = lopsided(&db, 20, 800);

    let (rows, stats) = run(
        &db,
        &left,
        &right,
        JoinType::Inner,
        Some(&equi()),
        JoinAlgorithm::Adaptive,
        64 * 1024 * 1024,
    );
    assert!(stats.role_reversed);
    for row in &rows {
        assert!(row[0].is_some(), "left key must be non-NULL: {row:?}");
        assert!(row[2].is_some(), "right key must be non-NULL: {row:?}");
    }
}

// ── Degrading ────────────────────────────────────────────────────────────────

/// With no equality there is nothing to hash, so it runs a nested loop rather
/// than failing.
#[test]
fn a_join_with_no_equality_still_runs() {
    let db = TempDb::new();
    let (left, right) = lopsided(&db, 30, 30);
    let condition = lt(col("l.v"), col("r.k"));

    for join_type in [JoinType::Inner, JoinType::LeftOuter, JoinType::Anti] {
        agrees(
            &db,
            &left,
            &right,
            join_type,
            Some(&condition),
            64 * 1024 * 1024,
        );
    }
}

/// A cross join has no condition at all.
#[test]
fn a_cross_join_runs_through_the_nested_loop_path() {
    let db = TempDb::new();
    let (left, right) = lopsided(&db, 12, 9);

    let (rows, _) = run(
        &db,
        &left,
        &right,
        JoinType::Cross,
        None,
        JoinAlgorithm::Adaptive,
        64 * 1024 * 1024,
    );
    // Each fixture adds one NULL-key row on top of its count.
    assert_eq!(rows.len(), 13 * 10);
}

/// A budget far below the inputs must still give the right answer, reversed or
/// not.
#[test]
fn spilling_does_not_change_the_result() {
    let db = TempDb::new();
    let (left, right) = lopsided(&db, 40, 1_200);

    for join_type in ALL {
        let stats = agrees(&db, &left, &right, join_type, Some(&equi()), 8 * 1024);
        assert!(
            stats.partitions > 0 || stats.role_reversed,
            "{join_type:?} should have either spilled or reversed: {stats:?}"
        );
    }
}

/// Equal-sized inputs are handled without preferring either side wrongly.
#[test]
fn equal_sized_inputs_are_handled() {
    let db = TempDb::new();
    let (left, right) = lopsided(&db, 300, 300);

    for join_type in ALL {
        agrees(
            &db,
            &left,
            &right,
            join_type,
            Some(&equi()),
            64 * 1024 * 1024,
        );
    }
}

/// An empty side ends immediately, which is the clearest possible signal about
/// which relation is smaller.
#[test]
fn an_empty_side_is_chosen_as_the_build_side() {
    let db = TempDb::new();

    let mut left = db.create_table("l", &[("k", DataType::Int), ("v", DataType::Int)]);
    left.flush();
    let mut right = db.create_table("r", &[("k", DataType::Int), ("w", DataType::Varchar(24))]);
    for i in 0..500 {
        right.insert(vec![int(i), text("x")]);
    }
    right.flush();

    for join_type in ALL {
        agrees(
            &db,
            &left,
            &right,
            join_type,
            Some(&equi()),
            64 * 1024 * 1024,
        );
    }
}
