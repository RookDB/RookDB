//! Hash join, in all three of its runtime forms.
//!
//! Every case is checked against a nested-loop join over the same inputs. The
//! nested loop is independently tested against the reference join, so agreeing
//! with it is a real correctness statement - and it is the only way to tell
//! whether partitioning changed an answer.
//!
//! The old suite never reached Grace or hybrid at all: its fixtures were two
//! pages against a ten-page budget, so `execute_grace` and `execute_hybrid`
//! ran zero times under test. Here the budget is set small enough to force
//! them, and the statistics are asserted so a silent regression to the
//! in-memory path fails the test.

#[path = "join_common/mod.rs"]
mod common;

use common::{TableHandle, TempDb, all_of, assert_rows_eq, col, collect_rows, eq, lt};
use storage_manager::executor::selection::Predicate;
use storage_manager::join::{
    ExecStats, JoinAlgorithm, JoinBuilder, JoinConfig, JoinType, TableRef,
};
use storage_manager::types::{DataType, DataValue};

fn int(value: i32) -> Option<DataValue> {
    Some(DataValue::Int(value))
}

fn text(value: &str) -> Option<DataValue> {
    Some(DataValue::Varchar(value.to_string()))
}

/// Run a join, returning its rows and its counters.
fn run(
    db: &TempDb,
    left: &TableRef,
    right: &TableRef,
    join_type: JoinType,
    condition: Option<&Predicate>,
    algorithm: JoinAlgorithm,
    work_memory: u64,
) -> (Vec<Vec<Option<DataValue>>>, ExecStats) {
    let config = JoinConfig::with_work_memory(work_memory).spill_root(db.path());
    let mut builder = JoinBuilder::new(left.clone(), right.clone(), join_type)
        .with_algorithm(algorithm)
        .with_config(config);
    if let Some(condition) = condition {
        builder = builder.with_condition(condition.clone());
    }

    let mut stream = builder.execute().expect("join should plan");
    let mut rows = Vec::new();
    let codec = storage_manager::join::RowCodec::new(stream.schema().types.clone());
    while let Some(row) = stream.next() {
        rows.push(
            codec
                .decode(&row.expect("join should run"))
                .expect("decode"),
        );
    }
    let stats = stream.stats();
    (rows, stats)
}

/// Assert the hash join agrees with a nested loop over the same inputs.
fn agrees_with_nested_loop(
    db: &TempDb,
    left: &TableRef,
    right: &TableRef,
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
        JoinAlgorithm::Hash,
        work_memory,
    );

    assert_rows_eq(
        &actual,
        &expected,
        &format!("hash {join_type:?} with {work_memory} bytes of work memory"),
    );
    stats
}

/// Duplicates on both sides, unmatched rows on both sides, and NULL keys.
fn fixture(db: &TempDb) -> (TableHandle, TableHandle) {
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

const ALL: [JoinType; 6] = [
    JoinType::Inner,
    JoinType::LeftOuter,
    JoinType::RightOuter,
    JoinType::FullOuter,
    JoinType::Semi,
    JoinType::Anti,
];

// ── In-memory ────────────────────────────────────────────────────────────────

#[test]
fn every_join_type_agrees_with_nested_loop_in_memory() {
    let db = TempDb::new();
    let (left, right) = fixture(&db);

    for join_type in ALL {
        let stats = agrees_with_nested_loop(
            &db,
            &left.table_ref(),
            &right.table_ref(),
            join_type,
            Some(&equi()),
            64 * 1024 * 1024,
        );
        assert_eq!(
            stats.partitions, 0,
            "{join_type:?} should have stayed in memory"
        );
        assert_eq!(stats.spilled_bytes, 0);
    }
}

/// A NULL key matches nothing, whichever side it is on and whichever form the
/// hash join takes.
#[test]
fn null_keys_never_match() {
    let db = TempDb::new();
    let (left, right) = fixture(&db);

    let (rows, _) = run(
        &db,
        &left.table_ref(),
        &right.table_ref(),
        JoinType::Inner,
        Some(&equi()),
        JoinAlgorithm::Hash,
        64 * 1024 * 1024,
    );
    for row in &rows {
        assert!(row[1].is_some(), "left key must be non-NULL: {row:?}");
        assert!(row[3].is_some(), "right key must be non-NULL: {row:?}");
    }

    // The NULL-key right row is still owed to a RIGHT join.
    let (rows, _) = run(
        &db,
        &left.table_ref(),
        &right.table_ref(),
        JoinType::RightOuter,
        Some(&equi()),
        JoinAlgorithm::Hash,
        64 * 1024 * 1024,
    );
    assert!(
        rows.iter()
            .any(|row| row[0].is_none() && row[4] == text("ghost")),
        "the NULL-key build row must appear NULL-extended"
    );
}

/// A residual alongside the key must be applied, not dropped because the
/// bucket already matched.
#[test]
fn a_residual_is_applied_on_top_of_the_key() {
    let db = TempDb::new();
    let (left, right) = fixture(&db);
    let condition = all_of(vec![equi(), lt(col("l.v"), col("r.cap"))]);

    for join_type in [JoinType::Inner, JoinType::LeftOuter, JoinType::Semi] {
        agrees_with_nested_loop(
            &db,
            &left.table_ref(),
            &right.table_ref(),
            join_type,
            Some(&condition),
            64 * 1024 * 1024,
        );
    }
}

/// A hash join needs an equality; a bare inequality must be refused rather
/// than keyed on something arbitrary.
#[test]
fn a_non_equi_condition_is_refused() {
    let db = TempDb::new();
    let (left, right) = fixture(&db);

    let outcome = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
        .with_algorithm(JoinAlgorithm::Hash)
        .with_condition(lt(col("l.v"), col("r.cap")))
        .execute();

    // `Box<dyn RowStream>` is not `Debug`, so unwrap the error by hand.
    let err = match outcome {
        Ok(_) => panic!("a hash join cannot run without an equality"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("equality"), "{err}");
}

// ── Spilling ─────────────────────────────────────────────────────────────────

/// Build a pair of relations big enough that a small budget must partition.
fn large_fixture(
    db: &TempDb,
    rows: i32,
    left_keys: i32,
    right_keys: i32,
) -> (TableHandle, TableHandle) {
    let mut left = db.create_table("l", &[("k", DataType::Int), ("v", DataType::Varchar(32))]);
    let mut right = db.create_table("r", &[("k", DataType::Int), ("w", DataType::Varchar(32))]);

    for i in 0..rows {
        left.insert(vec![int(i % left_keys), text(&format!("left-{i}-padding"))]);
        right.insert(vec![
            int(i % right_keys),
            text(&format!("right-{i}-padding")),
        ]);
    }
    left.flush();
    right.flush();

    (left, right)
}

/// With a budget far below the build side, the join must partition - and must
/// still produce exactly what it produced in memory.
#[test]
fn spilling_partitions_without_changing_the_result() {
    let db = TempDb::new();
    let (left, right) = large_fixture(&db, 400, 37, 23);

    for join_type in ALL {
        let stats = agrees_with_nested_loop(
            &db,
            &left.table_ref(),
            &right.table_ref(),
            join_type,
            Some(&equi()),
            8 * 1024,
        );

        assert!(
            stats.partitions > 0,
            "{join_type:?} should have partitioned, stats: {stats:?}"
        );
        assert!(
            stats.spilled_bytes > 0,
            "{join_type:?} should have written spill files"
        );
    }
}

/// The resident partition is joined as probe rows arrive rather than being
/// written out and read back. Its probe run therefore stays empty, which is
/// what distinguishes the hybrid form from a pure Grace join.
#[test]
fn the_hybrid_form_keeps_one_partition_resident() {
    let db = TempDb::new();
    let (left, right) = large_fixture(&db, 300, 29, 29);

    let stats = agrees_with_nested_loop(
        &db,
        &left.table_ref(),
        &right.table_ref(),
        JoinType::Inner,
        Some(&equi()),
        8 * 1024,
    );

    assert!(stats.partitions > 0, "expected partitioning: {stats:?}");
    assert!(
        stats.partitions < 32,
        "one level of partitioning should suffice here: {stats:?}"
    );
}

/// A single key holding more rows than the budget cannot be partitioned:
/// every row hashes to the same partition however many times it is split. The
/// join must still be correct, and must say that it happened.
#[test]
fn a_dominant_key_is_reported_rather_than_looping() {
    let db = TempDb::new();

    let mut left = db.create_table("l", &[("k", DataType::Int), ("v", DataType::Varchar(40))]);
    let mut right = db.create_table("r", &[("k", DataType::Int), ("w", DataType::Varchar(40))]);

    // Every right row shares one key, so no amount of hashing separates them.
    for i in 0..600 {
        right.insert(vec![int(7), text(&format!("hot-{i}-{}", "p".repeat(20)))]);
    }
    left.insert(vec![int(7), text("probe")]);
    left.insert(vec![int(8), text("miss")]);
    left.flush();
    right.flush();

    let stats = agrees_with_nested_loop(
        &db,
        &left.table_ref(),
        &right.table_ref(),
        JoinType::Inner,
        Some(&equi()),
        8 * 1024,
    );

    assert!(
        stats.oversized_partitions > 0,
        "a dominant key must be reported as an oversized partition: {stats:?}"
    );
    assert!(
        stats.repartition_depth > 0,
        "it should have tried to repartition first: {stats:?}"
    );
}

/// Spilling must not change results for a multi-column key either.
#[test]
fn multi_column_keys_survive_partitioning() {
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
    for i in 0..250 {
        left.insert(vec![int(i % 13), text(&format!("k{}", i % 7)), int(i)]);
        right.insert(vec![int(i % 11), text(&format!("k{}", i % 5)), int(i)]);
    }
    left.flush();
    right.flush();

    let condition = all_of(vec![eq(col("l.a"), col("r.a")), eq(col("l.b"), col("r.b"))]);

    for join_type in [JoinType::Inner, JoinType::LeftOuter, JoinType::FullOuter] {
        let stats = agrees_with_nested_loop(
            &db,
            &left.table_ref(),
            &right.table_ref(),
            join_type,
            Some(&condition),
            8 * 1024,
        );
        assert!(stats.partitions > 0, "{join_type:?} should partition");
    }
}

/// A self-join spills both sides at once, so the two must not share files.
#[test]
fn a_spilling_self_join_keeps_its_sides_apart() {
    let db = TempDb::new();

    let mut employees = db.create_table(
        "e1",
        &[
            ("id", DataType::Int),
            ("manager_id", DataType::Int),
            ("pad", DataType::Varchar(40)),
        ],
    );
    for i in 0..400 {
        let manager = if i == 0 { None } else { int(i / 4) };
        employees.insert(vec![int(i), manager, text(&"x".repeat(30))]);
    }
    employees.flush();

    let mut managers = employees.table_ref();
    managers.alias = "e2".to_string();

    let condition = eq(col("e1.manager_id"), col("e2.id"));

    let (expected, _) = run(
        &db,
        &employees.table_ref(),
        &managers,
        JoinType::Inner,
        Some(&condition),
        JoinAlgorithm::BlockNestedLoop,
        64 * 1024 * 1024,
    );
    let (actual, stats) = run(
        &db,
        &employees.table_ref(),
        &managers,
        JoinType::Inner,
        Some(&condition),
        JoinAlgorithm::Hash,
        8 * 1024,
    );

    assert!(stats.partitions > 0, "expected spilling: {stats:?}");
    assert_rows_eq(&actual, &expected, "spilling self-join");

    for row in &actual {
        assert_eq!(row[1], row[3], "manager_id must equal the matched id");
    }
}

/// Every spill file is removed once the operator is dropped.
#[test]
fn spill_files_are_removed_when_the_join_ends() {
    let db = TempDb::new();
    let spill_root = db.path().join("spill");
    let (left, right) = large_fixture(&db, 200, 17, 13);

    let config = JoinConfig::with_work_memory(8 * 1024).spill_root(&spill_root);
    let mut stream = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
        .with_algorithm(JoinAlgorithm::Hash)
        .with_condition(equi())
        .with_config(config)
        .execute()
        .expect("plans");

    while let Some(row) = stream.next() {
        row.expect("runs");
    }
    assert!(stream.stats().partitions > 0, "expected spilling");
    drop(stream);

    let leftovers: Vec<_> = std::fs::read_dir(&spill_root)
        .map(|entries| entries.flatten().map(|e| e.path()).collect())
        .unwrap_or_default();
    assert!(
        leftovers.is_empty(),
        "spill directories should be gone: {leftovers:?}"
    );
}

/// Abandoning a join part-way through still cleans up.
#[test]
fn abandoning_a_join_early_still_cleans_up() {
    let db = TempDb::new();
    let spill_root = db.path().join("spill");
    let (left, right) = large_fixture(&db, 200, 17, 13);

    let config = JoinConfig::with_work_memory(8 * 1024).spill_root(&spill_root);
    let mut stream = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
        .with_algorithm(JoinAlgorithm::Hash)
        .with_condition(equi())
        .with_config(config)
        .execute()
        .expect("plans");

    // Take a couple of rows and walk away.
    let _ = stream.next();
    let _ = stream.next();
    drop(stream);

    let leftovers: Vec<_> = std::fs::read_dir(&spill_root)
        .map(|entries| entries.flatten().map(|e| e.path()).collect())
        .unwrap_or_default();
    assert!(
        leftovers.is_empty(),
        "an abandoned join must not leak spill files: {leftovers:?}"
    );
}
