//! Every algorithm must compute the same join.
//!
//! Each case is run through all five executors and compared against the
//! reference join, at both a generous and a tiny memory budget. This is the
//! test that would have caught the old implementation's silent divergences:
//! NULL keys that matched after a spill round-trip, semi joins that quietly
//! computed an inner join, and sort-merge joins that returned nothing when the
//! condition was written the other way round.

#[path = "join_common/mod.rs"]
mod common;

use common::{TableHandle, TempDb, all_of, assert_rows_eq, col, eq, lt, reference_join};
use storage_manager::executor::selection::Predicate;
use storage_manager::join::{
    ExecStats, JoinAlgorithm, JoinBuilder, JoinConfig, JoinError, JoinPredicate, JoinType,
    MatchEvaluator, RowCodec, SideResolver, split_conjuncts,
};
use storage_manager::types::{DataType, DataValue};

fn int(value: i32) -> Option<DataValue> {
    Some(DataValue::Int(value))
}

fn text(value: &str) -> Option<DataValue> {
    Some(DataValue::Varchar(value.to_string()))
}

/// Algorithms that accept an equijoin. Nested loops also accept anything else.
const EQUI_ALGORITHMS: [JoinAlgorithm; 6] = [
    JoinAlgorithm::SimpleNestedLoop,
    JoinAlgorithm::BlockNestedLoop,
    JoinAlgorithm::Hash,
    JoinAlgorithm::SortMerge,
    JoinAlgorithm::SymmetricHash,
    JoinAlgorithm::Adaptive,
];

const ALL_JOIN_TYPES: [JoinType; 6] = [
    JoinType::Inner,
    JoinType::LeftOuter,
    JoinType::RightOuter,
    JoinType::FullOuter,
    JoinType::Semi,
    JoinType::Anti,
];

fn run(
    db: &TempDb,
    left: &TableHandle,
    right: &TableHandle,
    join_type: JoinType,
    condition: Option<&Predicate>,
    algorithm: JoinAlgorithm,
    work_memory: u64,
) -> Result<(Vec<Vec<Option<DataValue>>>, ExecStats), JoinError> {
    let config = JoinConfig::with_work_memory(work_memory).spill_root(db.path());
    let mut builder = JoinBuilder::new(left.table_ref(), right.table_ref(), join_type)
        .with_algorithm(algorithm)
        .with_config(config);
    if let Some(condition) = condition {
        builder = builder.with_condition(condition.clone());
    }

    let mut stream = builder.execute()?;
    let codec = RowCodec::new(stream.schema().types.clone());
    let mut rows = Vec::new();
    while let Some(row) = stream.next() {
        rows.push(codec.decode(&row?)?);
    }
    Ok((rows, stream.stats()))
}

/// The reference result, computed from the rows as inserted.
///
/// The evaluator is built by splitting with FULL OUTER, where nothing may be
/// pushed into a scan - so the reference sees the whole condition, and any
/// algorithm that pushes filters is checked against a version that does not.
fn expected(
    left: &TableHandle,
    right: &TableHandle,
    join_type: JoinType,
    condition: Option<&Predicate>,
) -> Vec<Vec<Option<DataValue>>> {
    let left_relation = left.relation_schema();
    let right_relation = right.relation_schema();
    let resolver = SideResolver::new(&left_relation, &right_relation).expect("aliases");
    let split = split_conjuncts(condition, &resolver, JoinType::FullOuter).expect("splits");
    let evaluator = MatchEvaluator::new(
        split.keys,
        split
            .residual
            .map(|predicate| JoinPredicate::new(predicate, left_relation.len())),
    );

    reference_join(
        left.rows(),
        right.rows(),
        join_type,
        &evaluator,
        left_relation.len(),
        right_relation.len(),
    )
    .expect("reference join")
}

/// Run every applicable algorithm at both budgets and compare.
fn check_all(
    db: &TempDb,
    left: &TableHandle,
    right: &TableHandle,
    join_type: JoinType,
    condition: Option<&Predicate>,
    algorithms: &[JoinAlgorithm],
) {
    let reference = expected(left, right, join_type, condition);

    for &algorithm in algorithms {
        for work_memory in [64 * 1024 * 1024, 8 * 1024] {
            // The symmetric hash join cannot spill; running out of memory is a
            // reported error, not a wrong answer.
            match run(
                db,
                left,
                right,
                join_type,
                condition,
                algorithm,
                work_memory,
            ) {
                Ok((actual, _)) => assert_rows_eq(
                    &actual,
                    &reference,
                    &format!("{algorithm:?} / {join_type:?} / {work_memory} bytes"),
                ),
                Err(JoinError::OutOfMemory(message)) => {
                    assert_eq!(
                        algorithm,
                        JoinAlgorithm::SymmetricHash,
                        "only the symmetric hash join may run out of memory: {message}"
                    );
                }
                Err(e) => panic!("{algorithm:?} / {join_type:?} failed: {e}"),
            }
        }
    }
}

// ── Fixtures ─────────────────────────────────────────────────────────────────

/// Duplicates on both sides, unmatched rows on both sides, NULL keys on both
/// sides.
fn mixed(db: &TempDb) -> (TableHandle, TableHandle) {
    let mut left = db.create_table(
        "l",
        &[
            ("id", DataType::Int),
            ("k", DataType::Int),
            ("v", DataType::Int),
        ],
    );
    left.insert_all(vec![
        vec![int(1), int(10), int(100)],
        vec![int(2), int(10), int(200)],
        vec![int(3), int(20), int(300)],
        vec![int(4), int(99), int(400)],
        vec![int(5), None, int(500)],
        vec![int(6), None, int(600)],
    ]);
    left.flush();

    let mut right = db.create_table(
        "r",
        &[
            ("k", DataType::Int),
            ("name", DataType::Varchar(12)),
            ("cap", DataType::Int),
        ],
    );
    right.insert_all(vec![
        vec![int(10), text("a"), int(250)],
        vec![int(10), text("b"), int(150)],
        vec![int(20), text("c"), int(350)],
        vec![int(30), text("d"), int(50)],
        vec![None, text("ghost"), int(0)],
    ]);
    right.flush();

    (left, right)
}

fn equi() -> Predicate {
    eq(col("l.k"), col("r.k"))
}

// ── The matrix ───────────────────────────────────────────────────────────────

#[test]
fn every_algorithm_agrees_on_every_join_type() {
    let db = TempDb::new();
    let (left, right) = mixed(&db);

    for join_type in ALL_JOIN_TYPES {
        check_all(
            &db,
            &left,
            &right,
            join_type,
            Some(&equi()),
            &EQUI_ALGORITHMS,
        );
    }
}

/// Many-to-many: several left rows and several right rows share each key, so
/// every algorithm has to produce the full cross product per group.
#[test]
fn every_algorithm_agrees_on_many_to_many_groups() {
    let db = TempDb::new();

    let mut left = db.create_table("l", &[("k", DataType::Int), ("v", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int), ("w", DataType::Int)]);
    for i in 0..40 {
        left.insert(vec![int(i % 4), int(i)]);
        right.insert(vec![int(i % 3), int(i)]);
    }
    left.flush();
    right.flush();

    for join_type in ALL_JOIN_TYPES {
        check_all(
            &db,
            &left,
            &right,
            join_type,
            Some(&equi()),
            &EQUI_ALGORITHMS,
        );
    }
}

/// A key plus a residual: bucket or group membership settles the key, and the
/// residual must still be applied on top.
#[test]
fn every_algorithm_applies_the_residual() {
    let db = TempDb::new();
    let (left, right) = mixed(&db);
    let condition = all_of(vec![equi(), lt(col("l.v"), col("r.cap"))]);

    for join_type in ALL_JOIN_TYPES {
        check_all(
            &db,
            &left,
            &right,
            join_type,
            Some(&condition),
            &EQUI_ALGORITHMS,
        );
    }
}

/// The equality written right-side-first must give identical results in every
/// algorithm. Sort-merge got this wrong before and silently returned nothing.
#[test]
fn every_algorithm_handles_reversed_orientation() {
    let db = TempDb::new();
    let (left, right) = mixed(&db);
    let reversed = eq(col("r.k"), col("l.k"));

    for join_type in ALL_JOIN_TYPES {
        check_all(
            &db,
            &left,
            &right,
            join_type,
            Some(&reversed),
            &EQUI_ALGORITHMS,
        );
    }
}

/// A two-column key, so composite key encoding is exercised end to end.
#[test]
fn every_algorithm_handles_composite_keys() {
    let db = TempDb::new();

    let mut left = db.create_table(
        "l",
        &[
            ("a", DataType::Int),
            ("b", DataType::Varchar(8)),
            ("v", DataType::Int),
        ],
    );
    let mut right = db.create_table(
        "r",
        &[
            ("a", DataType::Int),
            ("b", DataType::Varchar(8)),
            ("w", DataType::Int),
        ],
    );
    for i in 0..30 {
        left.insert(vec![int(i % 5), text(&format!("k{}", i % 3)), int(i)]);
        right.insert(vec![int(i % 4), text(&format!("k{}", i % 3)), int(i)]);
    }
    // A NULL in one key component suppresses the whole key.
    left.insert(vec![int(1), None, int(999)]);
    right.insert(vec![None, text("k0"), int(999)]);
    left.flush();
    right.flush();

    let condition = all_of(vec![eq(col("l.a"), col("r.a")), eq(col("l.b"), col("r.b"))]);
    for join_type in ALL_JOIN_TYPES {
        check_all(
            &db,
            &left,
            &right,
            join_type,
            Some(&condition),
            &EQUI_ALGORITHMS,
        );
    }
}

/// Empty inputs, in every algorithm.
#[test]
fn every_algorithm_handles_empty_inputs() {
    let db = TempDb::new();

    let mut empty = db.create_table("l", &[("k", DataType::Int)]);
    empty.flush();
    let mut full = db.create_table("r", &[("k", DataType::Int)]);
    full.insert_all(vec![vec![int(1)], vec![int(2)]]);
    full.flush();

    for join_type in ALL_JOIN_TYPES {
        check_all(
            &db,
            &empty,
            &full,
            join_type,
            Some(&equi()),
            &EQUI_ALGORITHMS,
        );
        check_all(
            &db,
            &full,
            &empty,
            join_type,
            Some(&equi()),
            &EQUI_ALGORITHMS,
        );
    }
}

// ── Sort-merge specifics ─────────────────────────────────────────────────────

/// A key whose group is larger than the budget must spill the group rather
/// than hold it, and must still produce every pair. The old implementation
/// held the group in a plain `Vec` and had no bound at all.
#[test]
fn sort_merge_spills_an_oversized_duplicate_group() {
    let db = TempDb::new();

    let mut left = db.create_table("l", &[("k", DataType::Int), ("v", DataType::Varchar(40))]);
    let mut right = db.create_table("r", &[("k", DataType::Int), ("w", DataType::Varchar(40))]);

    // One key, many rows on both sides: a group that cannot be split.
    for i in 0..120 {
        right.insert(vec![int(7), text(&format!("r{i}-{}", "x".repeat(30)))]);
    }
    for i in 0..5 {
        left.insert(vec![int(7), text(&format!("l{i}"))]);
    }
    left.flush();
    right.flush();

    let reference = expected(&left, &right, JoinType::Inner, Some(&equi()));
    let (actual, stats) = run(
        &db,
        &left,
        &right,
        JoinType::Inner,
        Some(&equi()),
        JoinAlgorithm::SortMerge,
        8 * 1024,
    )
    .expect("sort-merge should run");

    assert_eq!(actual.len(), 5 * 120, "every pair of the group");
    assert_rows_eq(&actual, &reference, "sort-merge with a spilled group");
    assert!(
        stats.spilled_groups > 0,
        "the group should have spilled: {stats:?}"
    );
}

/// A budget below the input size must produce sorted runs, and merging them
/// must not change the answer.
#[test]
fn sort_merge_spills_runs_without_changing_the_result() {
    let db = TempDb::new();

    let mut left = db.create_table("l", &[("k", DataType::Int), ("v", DataType::Varchar(32))]);
    let mut right = db.create_table("r", &[("k", DataType::Int), ("w", DataType::Varchar(32))]);
    for i in 0..300 {
        left.insert(vec![int(i % 47), text(&format!("l{i}-padding-here"))]);
        right.insert(vec![int(i % 31), text(&format!("r{i}-padding-here"))]);
    }
    left.flush();
    right.flush();

    for join_type in ALL_JOIN_TYPES {
        let reference = expected(&left, &right, join_type, Some(&equi()));
        let (actual, stats) = run(
            &db,
            &left,
            &right,
            join_type,
            Some(&equi()),
            JoinAlgorithm::SortMerge,
            8 * 1024,
        )
        .expect("sort-merge should run");

        assert_rows_eq(&actual, &reference, &format!("sort-merge {join_type:?}"));
        assert!(
            stats.sort_runs > 0,
            "{join_type:?} should have written runs: {stats:?}"
        );
    }
}

// ── Symmetric hash specifics ─────────────────────────────────────────────────

/// It cannot spill, so an input larger than the budget is an explicit error -
/// never an unbounded allocation and never a wrong answer.
#[test]
fn symmetric_hash_reports_running_out_of_memory() {
    let db = TempDb::new();

    let mut left = db.create_table("l", &[("k", DataType::Int), ("v", DataType::Varchar(40))]);
    let mut right = db.create_table("r", &[("k", DataType::Int), ("w", DataType::Varchar(40))]);
    for i in 0..500 {
        left.insert(vec![int(i), text(&"x".repeat(35))]);
        right.insert(vec![int(i), text(&"y".repeat(35))]);
    }
    left.flush();
    right.flush();

    let outcome = run(
        &db,
        &left,
        &right,
        JoinType::Inner,
        Some(&equi()),
        JoinAlgorithm::SymmetricHash,
        8 * 1024,
    );

    match outcome {
        Err(JoinError::OutOfMemory(message)) => {
            assert!(
                message.contains("sort-merge") || message.contains("hash"),
                "the error should suggest an alternative: {message}"
            );
        }
        Err(e) => panic!("expected an out-of-memory error, got {e}"),
        Ok((rows, _)) => panic!("expected an out-of-memory error, got {} rows", rows.len()),
    }
}

/// With room to work it must agree with everything else.
#[test]
fn symmetric_hash_agrees_when_it_fits() {
    let db = TempDb::new();
    let (left, right) = mixed(&db);

    for join_type in ALL_JOIN_TYPES {
        let reference = expected(&left, &right, join_type, Some(&equi()));
        let (actual, _) = run(
            &db,
            &left,
            &right,
            join_type,
            Some(&equi()),
            JoinAlgorithm::SymmetricHash,
            64 * 1024 * 1024,
        )
        .expect("should run");
        assert_rows_eq(
            &actual,
            &reference,
            &format!("symmetric hash {join_type:?}"),
        );
    }
}
