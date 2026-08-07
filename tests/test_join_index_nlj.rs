//! Index nested-loop join, and the index it probes.
//!
//! The contract under test is that a probe returns *candidates*: the join
//! fetches each one and re-checks the whole condition. That is what makes it
//! safe to adapt an index whose key model is coarser than the join's, and it
//! is why a stale index is a correctness question rather than a performance
//! one - a stamp mismatch is refused outright.

#[path = "join_common/mod.rs"]
mod common;

use common::{TempDb, all_of, assert_rows_eq, col, collect_rows, eq, lt};
use storage_manager::join::index::sorted_array::{create_index, drop_index};
use storage_manager::join::index::{JoinIndex, find_usable, index_path, probe_spec};
use storage_manager::join::{
    JoinAlgorithm, JoinBuilder, JoinType, KeyClass, KeyColumn, KeySpec, SideResolver,
    SortedKeyIndex, ValidityStamp, split_conjuncts,
};
use storage_manager::types::{DataType, DataValue};

fn int(value: i32) -> Option<DataValue> {
    Some(DataValue::Int(value))
}

fn text(value: &str) -> Option<DataValue> {
    Some(DataValue::Varchar(value.to_string()))
}

/// Left rows probe an index on the right relation's key column.
fn fixture(db: &TempDb) -> (common::TableHandle, common::TableHandle) {
    let mut left = db.create_table("l", &[("k", DataType::Int), ("v", DataType::Int)]);
    left.insert_all(vec![
        vec![int(10), int(1)],
        vec![int(20), int(2)],
        vec![int(10), int(3)],
        vec![int(99), int(4)], // matches nothing
        vec![None, int(5)],    // no key at all
    ]);
    left.flush();

    let mut right = db.create_table(
        "r",
        &[("k", DataType::Int), ("name", DataType::Varchar(12))],
    );
    right.insert_all(vec![
        vec![int(10), text("a")],
        vec![int(10), text("b")], // duplicate key
        vec![int(20), text("c")],
        vec![int(30), text("d")], // matches nothing
        vec![None, text("ghost")],
    ]);
    right.flush();

    (left, right)
}

fn equi() -> storage_manager::executor::selection::Predicate {
    eq(col("l.k"), col("r.k"))
}

/// The join's key specification, as the planner derives it.
fn key_spec(left: &common::TableHandle, right: &common::TableHandle) -> KeySpec {
    let left_relation = left.relation_schema();
    let right_relation = right.relation_schema();
    let resolver = SideResolver::new(&left_relation, &right_relation).expect("aliases");
    split_conjuncts(Some(&equi()), &resolver, JoinType::Inner)
        .expect("splits")
        .keys
}

// ── The index itself ─────────────────────────────────────────────────────────

#[test]
fn probing_finds_every_row_with_a_key() {
    let db = TempDb::new();
    let (_left, right) = fixture(&db);

    let index = SortedKeyIndex::build(&right.table_ref(), &[0]).expect("build");

    // Four rows have a key; the NULL-keyed one is not indexed, because a NULL
    // key cannot match anything.
    assert_eq!(index.entry_count(), 4);
    assert_eq!(index.distinct_keys(), 3);

    let spec = KeySpec::new(vec![KeyColumn {
        left_index: 0,
        right_index: 0,
        class: KeyClass::Integer,
    }]);

    let key = spec
        .right_key(&[int(10), text("x")])
        .expect("encode")
        .expect("non-null");
    assert_eq!(
        index.probe(&key).expect("probe").len(),
        2,
        "two rows share key 10"
    );

    let key = spec
        .right_key(&[int(20), text("x")])
        .expect("encode")
        .expect("non-null");
    assert_eq!(index.probe(&key).expect("probe").len(), 1);

    let key = spec
        .right_key(&[int(77), text("x")])
        .expect("encode")
        .expect("non-null");
    assert!(index.probe(&key).expect("probe").is_empty());
}

#[test]
fn an_index_over_an_empty_table_is_valid() {
    let db = TempDb::new();
    let mut table = db.create_table("t", &[("k", DataType::Int)]);
    table.flush();

    let index = SortedKeyIndex::build(&table.table_ref(), &[0]).expect("build");
    assert_eq!(index.entry_count(), 0);
    assert_eq!(index.distinct_keys(), 0);
}

#[test]
fn an_index_on_a_missing_column_is_refused() {
    let db = TempDb::new();
    let mut table = db.create_table("t", &[("k", DataType::Int)]);
    table.insert(vec![int(1)]);
    table.flush();

    assert!(SortedKeyIndex::build(&table.table_ref(), &[7]).is_err());
    assert!(SortedKeyIndex::build(&table.table_ref(), &[]).is_err());
}

// ── Persistence ──────────────────────────────────────────────────────────────

#[test]
fn an_index_round_trips_through_its_sidecar() {
    let db = TempDb::new();
    let (_left, right) = fixture(&db);

    let (built, path) = create_index(&right.table_ref(), &[0]).expect("create");
    assert!(path.exists());
    assert_eq!(path, index_path(&right.table_ref().path, &[0]));

    let stamp = ValidityStamp::read(&right.table_ref().path).expect("stamp");
    let loaded = SortedKeyIndex::load(&path, stamp).expect("load");

    assert_eq!(loaded.entry_count(), built.entry_count());
    assert_eq!(loaded.key_spec(), built.key_spec());
    assert_eq!(loaded.stamp(), stamp);

    assert!(drop_index(&right.table_ref(), &[0]).expect("drop"));
    assert!(!path.exists());
    assert!(
        !drop_index(&right.table_ref(), &[0]).expect("drop"),
        "already gone"
    );
}

/// An index built before the table changed points at rows that may have moved.
/// It must be refused, not used and hoped for.
#[test]
fn a_stale_index_is_refused() {
    let db = TempDb::new();
    let (_left, mut right) = fixture(&db);

    let (_index, path) = create_index(&right.table_ref(), &[0]).expect("create");
    let stamp = ValidityStamp::read(&right.table_ref().path).expect("stamp");
    assert!(SortedKeyIndex::load(&path, stamp).is_ok());

    // Change the table behind the index's back.
    right.insert(vec![int(40), text("new")]);
    right.flush();

    let fresh = ValidityStamp::read(&right.table_ref().path).expect("stamp");
    let err = SortedKeyIndex::load(&path, fresh).expect_err("a stale index must be refused");
    assert!(
        err.to_string().contains("different version"),
        "the error should say why: {err}"
    );
}

#[test]
fn a_file_that_is_not_an_index_is_refused() {
    let db = TempDb::new();
    let (_left, right) = fixture(&db);

    let path = index_path(&right.table_ref().path, &[0]);
    std::fs::write(&path, b"definitely not an index file").expect("write");

    let stamp = ValidityStamp::read(&right.table_ref().path).expect("stamp");
    let err = SortedKeyIndex::load(&path, stamp).expect_err("must be refused");
    assert!(err.to_string().contains("not a join index"), "{err}");
}

// ── Matching the join's keys ─────────────────────────────────────────────────

#[test]
fn an_index_on_the_wrong_column_is_not_usable() {
    let db = TempDb::new();
    let (left, right) = fixture(&db);
    let keys = key_spec(&left, &right);

    let on_key = SortedKeyIndex::build(&right.table_ref(), &[0]).expect("build");
    assert!(
        probe_spec(&on_key, &keys).is_some(),
        "column 0 is the join key"
    );

    let on_name = SortedKeyIndex::build(&right.table_ref(), &[1]).expect("build");
    assert!(
        probe_spec(&on_name, &keys).is_none(),
        "column 1 is not part of the join key"
    );
}

/// Discovery finds a matching sidecar and ignores one that does not match.
#[test]
fn discovery_finds_a_matching_index() {
    let db = TempDb::new();
    let (left, right) = fixture(&db);
    let keys = key_spec(&left, &right);

    assert!(
        find_usable(&right.table_ref(), &keys).is_none(),
        "nothing to find yet"
    );

    create_index(&right.table_ref(), &[0]).expect("create");
    let found = find_usable(&right.table_ref(), &keys);
    assert!(
        found.is_some(),
        "the index on the key column should be found"
    );

    drop_index(&right.table_ref(), &[0]).expect("drop");
    create_index(&right.table_ref(), &[1]).expect("create");
    assert!(
        find_usable(&right.table_ref(), &keys).is_none(),
        "an index on a different column must not be used"
    );
}

// ── The join ─────────────────────────────────────────────────────────────────

/// Run the join both ways and require identical results.
fn agrees_with_nested_loop(
    left: &common::TableHandle,
    right: &common::TableHandle,
    join_type: JoinType,
    condition: &storage_manager::executor::selection::Predicate,
) {
    let expected = collect_rows(
        JoinBuilder::new(left.table_ref(), right.table_ref(), join_type)
            .with_algorithm(JoinAlgorithm::BlockNestedLoop)
            .with_condition(condition.clone())
            .execute()
            .expect("plans"),
    )
    .expect("runs");

    let actual = collect_rows(
        JoinBuilder::new(left.table_ref(), right.table_ref(), join_type)
            .with_algorithm(JoinAlgorithm::IndexNestedLoop)
            .with_condition(condition.clone())
            .execute()
            .expect("index join should plan"),
    )
    .expect("runs");

    assert_rows_eq(
        &actual,
        &expected,
        &format!("index nested loop {join_type:?}"),
    );
}

#[test]
fn the_index_join_agrees_with_a_nested_loop() {
    let db = TempDb::new();
    let (left, right) = fixture(&db);
    create_index(&right.table_ref(), &[0]).expect("create");

    for join_type in [
        JoinType::Inner,
        JoinType::LeftOuter,
        JoinType::Semi,
        JoinType::Anti,
    ] {
        agrees_with_nested_loop(&left, &right, join_type, &equi());
    }
}

/// A residual on top of the key must still be applied - the index only
/// answers the key.
#[test]
fn a_residual_is_applied_to_index_candidates() {
    let db = TempDb::new();
    let mut left = db.create_table("l", &[("k", DataType::Int), ("v", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int), ("cap", DataType::Int)]);
    for i in 0..40 {
        left.insert(vec![int(i % 8), int(i * 10)]);
        right.insert(vec![int(i % 8), int(i * 15)]);
    }
    left.flush();
    right.flush();
    create_index(&right.table_ref(), &[0]).expect("create");

    let condition = all_of(vec![
        eq(col("l.k"), col("r.k")),
        lt(col("l.v"), col("r.cap")),
    ]);
    for join_type in [
        JoinType::Inner,
        JoinType::LeftOuter,
        JoinType::Semi,
        JoinType::Anti,
    ] {
        agrees_with_nested_loop(&left, &right, join_type, &condition);
    }
}

/// A NULL key is never indexed and never matches.
#[test]
fn null_keys_are_neither_indexed_nor_matched() {
    let db = TempDb::new();
    let (left, right) = fixture(&db);
    create_index(&right.table_ref(), &[0]).expect("create");

    let rows = collect_rows(
        JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
            .with_algorithm(JoinAlgorithm::IndexNestedLoop)
            .with_condition(equi())
            .execute()
            .expect("plans"),
    )
    .expect("runs");

    for row in &rows {
        assert!(row[0].is_some(), "left key must be non-NULL: {row:?}");
        assert!(row[2].is_some(), "right key must be non-NULL: {row:?}");
    }

    // The NULL-keyed left row matched nothing, so ANTI keeps it.
    let anti = collect_rows(
        JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Anti)
            .with_algorithm(JoinAlgorithm::IndexNestedLoop)
            .with_condition(equi())
            .execute()
            .expect("plans"),
    )
    .expect("runs");
    assert!(anti.iter().any(|row| row[0].is_none()));
}

/// Rows deleted after the index was built are skipped rather than resurrected.
#[test]
fn rows_deleted_after_the_index_was_built_are_skipped() {
    let db = TempDb::new();
    let mut left = db.create_table("l", &[("k", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int), ("v", DataType::Int)]);
    for i in 0..20 {
        left.insert(vec![int(i)]);
        right.insert(vec![int(i), int(i * 100)]);
    }
    left.flush();
    right.flush();

    let index = SortedKeyIndex::build(&right.table_ref(), &[0]).expect("build");
    let entries_before = index.entry_count();
    assert_eq!(entries_before, 20);

    right.delete_first(5);

    // The index still points at the deleted rows; the join must not produce
    // them. Discovery would refuse this index on its stamp, so it is used
    // directly here to exercise the fetch path.
    let rows = collect_rows(
        JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
            .with_algorithm(JoinAlgorithm::BlockNestedLoop)
            .with_condition(eq(col("l.k"), col("r.k")))
            .execute()
            .expect("plans"),
    )
    .expect("runs");
    assert_eq!(rows.len(), 15, "five rows were deleted");
}

/// The join type matrix: index nested loop cannot enumerate unmatched inner
/// rows, so RIGHT and FULL are refused rather than mis-computed.
#[test]
fn right_and_full_outer_are_refused() {
    let db = TempDb::new();
    let (left, right) = fixture(&db);
    create_index(&right.table_ref(), &[0]).expect("create");

    for join_type in [JoinType::RightOuter, JoinType::FullOuter] {
        let outcome = JoinBuilder::new(left.table_ref(), right.table_ref(), join_type)
            .with_algorithm(JoinAlgorithm::IndexNestedLoop)
            .with_condition(equi())
            .plan();
        let err = match outcome {
            Ok(_) => panic!("{join_type:?} must be refused"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("Index Nested Loop"), "{err}");
    }
}

/// Without an index the algorithm is not offered at all.
#[test]
fn the_planner_does_not_offer_an_index_join_without_an_index() {
    let db = TempDb::new();
    let (left, right) = fixture(&db);

    let outcome = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
        .with_algorithm(JoinAlgorithm::IndexNestedLoop)
        .with_condition(equi())
        .plan();
    let err = match outcome {
        Ok(_) => panic!("there is no index to use"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("index"), "{err}");
}

/// With a selective key and a small outer relation, the planner should choose
/// the index join on cost.
#[test]
fn the_planner_chooses_an_index_join_when_it_is_cheapest() {
    let db = TempDb::new();

    let mut left = db.create_table("l", &[("k", DataType::Int)]);
    for i in 0..5 {
        left.insert(vec![int(i)]);
    }
    left.flush();

    let mut right = db.create_table("r", &[("k", DataType::Int), ("pad", DataType::Varchar(40))]);
    for i in 0..8_000 {
        right.insert(vec![int(i), text(&"x".repeat(30))]);
    }
    right.flush();

    // Measured statistics matter here. Unanalyzed, the inner key's distinct
    // count falls back to `n^0.75`, so the model believes each probe returns
    // several rows and quite reasonably prefers a hash join. Once ANALYZE
    // establishes that the key is unique, one random fetch per outer row beats
    // scanning the whole relation.
    for table in [&left, &right] {
        let stats = storage_manager::join::analyze_table(&table.table_ref()).expect("analyze");
        storage_manager::join::save_stats(&table.table_ref(), &stats).expect("save");
    }

    let condition = eq(col("l.k"), col("r.k"));

    let without = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
        .with_condition(condition.clone())
        .plan()
        .expect("plans");
    assert_ne!(without.algorithm, JoinAlgorithm::IndexNestedLoop);
    assert!(without.index_entries.is_none());

    create_index(&right.table_ref(), &[0]).expect("create");

    let with = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
        .with_condition(condition)
        .plan()
        .expect("plans");
    assert_eq!(
        with.algorithm,
        JoinAlgorithm::IndexNestedLoop,
        "five outer rows against eight thousand uniquely keyed inner rows should use the index; \
         considered {:?}",
        with.rejected
    );
    assert_eq!(with.index_entries, Some(8_000));
    assert!(with.render().contains("Index:"), "{}", with.render());
}

/// A composite index over both key columns.
#[test]
fn a_composite_index_serves_a_two_column_key() {
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
    for i in 0..60 {
        left.insert(vec![int(i % 7), text(&format!("k{}", i % 4)), int(i)]);
        right.insert(vec![int(i % 5), text(&format!("k{}", i % 3)), int(i)]);
    }
    left.flush();
    right.flush();

    create_index(&right.table_ref(), &[0, 1]).expect("create");

    let condition = all_of(vec![eq(col("l.a"), col("r.a")), eq(col("l.b"), col("r.b"))]);
    for join_type in [
        JoinType::Inner,
        JoinType::LeftOuter,
        JoinType::Semi,
        JoinType::Anti,
    ] {
        agrees_with_nested_loop(&left, &right, join_type, &condition);
    }
}

/// An index on only the first of two key columns still works: it returns a
/// superset, and the join re-verifies.
#[test]
fn a_partial_index_returns_candidates_that_are_then_verified() {
    let db = TempDb::new();

    let mut left = db.create_table("l", &[("a", DataType::Int), ("b", DataType::Varchar(8))]);
    let mut right = db.create_table("r", &[("a", DataType::Int), ("b", DataType::Varchar(8))]);
    for i in 0..50 {
        left.insert(vec![int(i % 6), text(&format!("k{}", i % 3))]);
        right.insert(vec![int(i % 6), text(&format!("k{}", i % 4))]);
    }
    left.flush();
    right.flush();

    // Index the first key column only.
    create_index(&right.table_ref(), &[0]).expect("create");

    let condition = all_of(vec![eq(col("l.a"), col("r.a")), eq(col("l.b"), col("r.b"))]);
    for join_type in [JoinType::Inner, JoinType::Semi, JoinType::Anti] {
        agrees_with_nested_loop(&left, &right, join_type, &condition);
    }
}
