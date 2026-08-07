//! Cardinality estimation, cost arithmetic, and algorithm selection.
//!
//! Three properties matter here. Estimates must be *bounded* - a semi join can
//! never produce more rows than its left input, however wrong the underlying
//! statistics are. Arithmetic must survive absurd inputs: the previous
//! implementation multiplied row counts as `usize`, which panicked in debug
//! builds and wrapped in release ones. And the choice between algorithms must
//! actually respond to the data rather than being fixed in advance.

#[path = "join_common/mod.rs"]
mod common;

use common::{TempDb, col, eq, lt};
use storage_manager::join::cost::{
    CostModel, SideEstimate, equijoin_rows, estimate_join, semi_selectivity,
};
use storage_manager::join::{
    JoinAlgorithm, JoinBuilder, JoinConfig, JoinType, StatsConfidence, analyze_table, save_stats,
};
use storage_manager::types::{DataType, DataValue};

fn int(value: i32) -> Option<DataValue> {
    Some(DataValue::Int(value))
}

fn side(rows: u64, pages: u64, distinct: u64) -> SideEstimate {
    SideEstimate {
        rows,
        pages,
        row_bytes: 64.0,
        distinct,
        null_fraction: 0.0,
    }
}

// ── Bounds that must hold whatever the statistics say ────────────────────────

/// A semi join emits a subset of its left input, so its estimate must never
/// exceed the left row count. The previous implementation scaled by both row
/// counts and over-estimated a ten-row semi join a hundredfold.
#[test]
fn semi_and_anti_estimates_are_bounded_by_the_left_input() {
    for (left_rows, right_rows, left_ndv, right_ndv) in [
        (10u64, 100u64, 10u64, 100u64),
        (100, 10, 100, 10),
        (1_000, 1_000_000, 5, 5),
        (7, 999_999, 1, 1),
        (1, 1, 1, 1),
    ] {
        let left = side(left_rows, 1, left_ndv);
        let right = side(right_rows, 1, right_ndv);

        let semi = estimate_join(JoinType::Semi, &left, &right, true, 1.0);
        assert!(
            semi.output_rows <= left_rows,
            "semi estimated {} rows from a {left_rows}-row left input",
            semi.output_rows
        );

        let anti = estimate_join(JoinType::Anti, &left, &right, true, 1.0);
        assert!(
            anti.output_rows <= left_rows,
            "anti estimated {} rows from a {left_rows}-row left input",
            anti.output_rows
        );

        // Every left row either matches or does not.
        assert!(
            semi.output_rows + anti.output_rows <= left_rows + 1,
            "semi ({}) and anti ({}) together exceed {left_rows}",
            semi.output_rows,
            anti.output_rows
        );
    }
}

/// The specific shape that used to be wrong: ten left rows against a hundred
/// right rows was estimated at a thousand.
#[test]
fn a_small_semi_join_is_not_inflated_by_a_large_right_side() {
    let left = side(10, 1, 10);
    let right = side(100, 1, 100);

    let estimate = estimate_join(JoinType::Semi, &left, &right, true, 1.0);
    assert!(
        estimate.output_rows <= 10,
        "estimated {} rows; the left input only has 10",
        estimate.output_rows
    );
    assert!(semi_selectivity(&left, &right) <= 1.0);
}

/// An outer join emits at least as many rows as the corresponding inner one,
/// and at least one row per preserved input row.
#[test]
fn outer_estimates_dominate_the_inner_estimate() {
    let left = side(500, 10, 50);
    let right = side(300, 6, 30);

    let inner = estimate_join(JoinType::Inner, &left, &right, true, 1.0).output_rows;
    let left_outer = estimate_join(JoinType::LeftOuter, &left, &right, true, 1.0).output_rows;
    let right_outer = estimate_join(JoinType::RightOuter, &left, &right, true, 1.0).output_rows;
    let full = estimate_join(JoinType::FullOuter, &left, &right, true, 1.0).output_rows;

    assert!(left_outer >= inner);
    assert!(right_outer >= inner);
    assert!(full >= left_outer);
    assert!(full >= right_outer);
}

/// A cross join is exactly the product, with no estimation involved.
#[test]
fn a_cross_join_is_the_product_of_its_inputs() {
    let left = side(40, 2, 40);
    let right = side(25, 2, 25);
    let estimate = estimate_join(JoinType::Cross, &left, &right, false, 1.0);
    assert_eq!(estimate.output_rows, 1_000);
}

/// More distinct values means fewer matches per value.
///
/// System-R containment divides by the *larger* of the two distinct counts, so
/// the estimate responds to whichever side dominates - raising the smaller
/// side's count below that threshold correctly changes nothing.
#[test]
fn equijoin_output_falls_as_distinct_values_rise() {
    let left = side(1_000, 20, 10);

    let few = equijoin_rows(&left, &side(1_000, 20, 10));
    let more = equijoin_rows(&left, &side(1_000, 20, 100));
    let many = equijoin_rows(&left, &side(1_000, 20, 1_000));

    assert!(few > more, "{few} should exceed {more}");
    assert!(more > many, "{more} should exceed {many}");

    // Below the dominant count the estimate is unchanged, which is the
    // containment assumption doing its job rather than a bug.
    let dominant = side(1_000, 20, 1_000);
    assert_eq!(
        equijoin_rows(&dominant, &side(1_000, 20, 10)),
        equijoin_rows(&dominant, &side(1_000, 20, 100)),
    );
}

/// A NULL key cannot match, so a column full of NULLs produces nothing.
#[test]
fn null_keys_reduce_the_estimate() {
    let mut left = side(1_000, 20, 100);
    let right = side(1_000, 20, 100);

    let baseline = equijoin_rows(&left, &right);
    left.null_fraction = 0.5;
    let halved = equijoin_rows(&left, &right);

    assert!((halved - baseline / 2.0).abs() < baseline * 0.01);

    left.null_fraction = 1.0;
    assert_eq!(equijoin_rows(&left, &right), 0.0);
}

// ── Arithmetic that must not overflow ────────────────────────────────────────

/// Absurd cardinalities must give a finite cost, not a panic and not a wrapped
/// value. In a debug build the old `usize` multiplication aborted here.
#[test]
fn enormous_inputs_produce_finite_costs() {
    let huge = side(u64::MAX / 2, u64::MAX / 4, 2);
    let model = CostModel::new(64 * 1024);

    for algorithm in [
        JoinAlgorithm::SimpleNestedLoop,
        JoinAlgorithm::BlockNestedLoop,
        JoinAlgorithm::IndexNestedLoop,
        JoinAlgorithm::SortMerge,
        JoinAlgorithm::Hash,
        JoinAlgorithm::SymmetricHash,
        JoinAlgorithm::Adaptive,
    ] {
        let cost = model.cost(algorithm, &huge, &huge, u64::MAX, 1024, true);
        assert!(
            cost.total() >= 0.0 && !cost.total().is_nan(),
            "{algorithm:?} produced {}",
            cost.total()
        );
    }

    // And the cardinality estimate itself must saturate rather than wrap.
    let estimate = estimate_join(JoinType::Inner, &huge, &huge, true, 1.0);
    assert!(estimate.output_rows > 0);
    assert!(estimate.matched_left_rows <= huge.rows);
}

#[test]
fn zero_sized_inputs_do_not_divide_by_zero() {
    let empty = side(0, 0, 0);
    let model = CostModel::new(64 * 1024);

    for algorithm in [
        JoinAlgorithm::SimpleNestedLoop,
        JoinAlgorithm::SortMerge,
        JoinAlgorithm::Hash,
        JoinAlgorithm::IndexNestedLoop,
    ] {
        let cost = model.cost(algorithm, &empty, &empty, 0, 1024, true);
        assert!(cost.total().is_finite(), "{algorithm:?}");
    }

    let estimate = estimate_join(JoinType::Inner, &empty, &empty, true, 1.0);
    assert_eq!(estimate.output_rows, 0);
}

// ── Sort passes ──────────────────────────────────────────────────────────────

/// An input that fits in memory needs no runs and therefore no merge. The
/// previous cost model charged a pass count derived from the row count, which
/// is not a pass count at all.
#[test]
fn a_sort_that_fits_in_memory_costs_no_extra_passes() {
    // 64 KiB is eight 8 KiB pages.
    let model = CostModel::new(8 * 8192);
    assert_eq!(model.memory_pages(), 8.0);

    assert_eq!(model.sort_passes(1.0), 0.0);
    assert_eq!(model.sort_passes(8.0), 0.0, "exactly fits");
    assert!(model.sort_passes(9.0) > 0.0, "one page over must spill");
}

/// Pass counts grow with the input, logarithmically in the fan-in.
#[test]
fn sort_passes_grow_with_the_input() {
    let model = CostModel::new(4 * 8192);

    let small = model.sort_passes(100.0);
    let medium = model.sort_passes(10_000.0);
    let large = model.sort_passes(10_000_000.0);

    assert!(small >= 1.0);
    assert!(medium >= small, "{medium} should be at least {small}");
    assert!(large >= medium, "{large} should be at least {medium}");
    assert!(
        large < 100.0,
        "pass counts must stay logarithmic, got {large}"
    );
}

// ── Algorithm selection ──────────────────────────────────────────────────────

/// A join with no equality cannot use a key-based algorithm, so the planner
/// must fall to a nested loop rather than refusing or picking one anyway.
#[test]
fn a_non_equi_join_is_planned_as_a_nested_loop() {
    let db = TempDb::new();
    let mut left = db.create_table("l", &[("k", DataType::Int), ("v", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int), ("w", DataType::Int)]);
    for i in 0..50 {
        left.insert(vec![int(i), int(i)]);
        right.insert(vec![int(i), int(i)]);
    }
    left.flush();
    right.flush();

    let plan = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
        .with_condition(lt(col("l.v"), col("r.w")))
        .plan()
        .expect("should plan");

    assert!(
        matches!(
            plan.algorithm,
            JoinAlgorithm::SimpleNestedLoop | JoinAlgorithm::BlockNestedLoop
        ),
        "expected a nested loop, got {:?}",
        plan.algorithm
    );
    assert!(plan.key_conditions.is_empty());
    assert!(plan.residual.is_some());
}

/// With an equality available, a key-based algorithm must win: a nested loop
/// over anything but tiny inputs is quadratic.
#[test]
fn an_equi_join_prefers_a_key_based_algorithm() {
    let db = TempDb::new();
    let mut left = db.create_table("l", &[("k", DataType::Int), ("v", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int), ("w", DataType::Int)]);
    for i in 0..2_000 {
        left.insert(vec![int(i % 500), int(i)]);
        right.insert(vec![int(i % 400), int(i)]);
    }
    left.flush();
    right.flush();

    let plan = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
        .with_condition(eq(col("l.k"), col("r.k")))
        .plan()
        .expect("should plan");

    assert!(
        matches!(
            plan.algorithm,
            JoinAlgorithm::Hash
                | JoinAlgorithm::SortMerge
                | JoinAlgorithm::SymmetricHash
                | JoinAlgorithm::Adaptive
        ),
        "expected a key-based algorithm, got {:?}",
        plan.algorithm
    );
    assert!(
        !matches!(
            plan.algorithm,
            JoinAlgorithm::SimpleNestedLoop | JoinAlgorithm::BlockNestedLoop
        ),
        "a nested loop is quadratic here"
    );
    assert_eq!(plan.key_conditions, vec!["l.k = r.k".to_string()]);
    assert!(
        !plan.rejected.is_empty(),
        "other algorithms should have been costed and ranked"
    );
}

/// Estimate confidence decides whether adaptivity is worth paying for.
///
/// With guessed statistics the operator that can correct itself mid-flight is
/// preferred; once they are measured, the simpler one is - adaptivity buys
/// nothing if the prediction is already right.
#[test]
fn adaptivity_is_preferred_only_while_the_estimates_are_guesses() {
    let db = TempDb::new();
    let mut left = db.create_table("l", &[("k", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int)]);
    for i in 0..2_000 {
        left.insert(vec![int(i % 500)]);
        right.insert(vec![int(i % 400)]);
    }
    left.flush();
    right.flush();

    let plan_now = || {
        JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
            .with_condition(eq(col("l.k"), col("r.k")))
            .plan()
            .expect("plans")
    };

    let guessed = plan_now();
    assert_eq!(guessed.confidence, StatsConfidence::HeaderOnly);
    assert_eq!(
        guessed.algorithm,
        JoinAlgorithm::Adaptive,
        "unanalyzed, the self-correcting operator should win"
    );

    for table in [&left, &right] {
        let stats = analyze_table(&table.table_ref()).expect("analyze");
        save_stats(&table.table_ref(), &stats).expect("save");
    }

    let measured = plan_now();
    assert_eq!(measured.confidence, StatsConfidence::Analyzed);
    assert_ne!(
        measured.algorithm,
        JoinAlgorithm::Adaptive,
        "measured, the plain operator should win; considered {:?}",
        measured.rejected
    );
}

/// The symmetric hash join holds both inputs at once, so it must not be
/// offered when they do not fit.
#[test]
fn symmetric_hash_is_not_offered_when_the_inputs_do_not_fit() {
    let db = TempDb::new();
    let mut left = db.create_table("l", &[("k", DataType::Int), ("v", DataType::Varchar(40))]);
    let mut right = db.create_table("r", &[("k", DataType::Int), ("w", DataType::Varchar(40))]);
    for i in 0..2_000 {
        left.insert(vec![int(i % 100), Some(DataValue::Varchar("x".repeat(35)))]);
        right.insert(vec![int(i % 100), Some(DataValue::Varchar("y".repeat(35)))]);
    }
    left.flush();
    right.flush();

    let plan = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
        .with_condition(eq(col("l.k"), col("r.k")))
        .with_config(JoinConfig::with_work_memory(8 * 1024))
        .plan()
        .expect("should plan");

    assert_ne!(plan.algorithm, JoinAlgorithm::SymmetricHash);
    assert!(
        !plan
            .rejected
            .iter()
            .any(|(algorithm, _)| *algorithm == JoinAlgorithm::SymmetricHash),
        "it should not even be a candidate: {:?}",
        plan.rejected
    );
}

/// Forcing an algorithm the join type does not support is refused, not
/// silently replaced.
#[test]
fn forcing_an_unsupported_algorithm_is_refused() {
    let db = TempDb::new();
    let mut left = db.create_table("l", &[("k", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int)]);
    left.insert(vec![int(1)]);
    right.insert(vec![int(1)]);
    left.flush();
    right.flush();

    let err = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Cross)
        .with_algorithm(JoinAlgorithm::Hash)
        .plan()
        .expect_err("a hash join cannot serve a cross join");
    assert!(err.to_string().contains("Hash"), "{err}");
}

// ── EXPLAIN ──────────────────────────────────────────────────────────────────

#[test]
fn explain_reports_the_plan_and_its_confidence() {
    let db = TempDb::new();
    let mut left = db.create_table("emp", &[("dept", DataType::Int), ("salary", DataType::Int)]);
    let mut right = db.create_table("dept", &[("id", DataType::Int), ("budget", DataType::Int)]);
    for i in 0..300 {
        left.insert(vec![int(i % 20), int(i * 10)]);
        right.insert(vec![int(i % 20), int(i * 100)]);
    }
    left.flush();
    right.flush();

    let builder = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
        .with_condition(common::all_of(vec![
            eq(col("emp.dept"), col("dept.id")),
            lt(col("emp.salary"), col("dept.budget")),
        ]));

    let unanalyzed = builder.explain().expect("explain");
    assert!(
        unanalyzed.contains("Join Cond: emp.dept = dept.id"),
        "{unanalyzed}"
    );
    assert!(unanalyzed.contains("Residual:"), "{unanalyzed}");
    assert!(unanalyzed.contains("stats=header-only"), "{unanalyzed}");
    assert!(unanalyzed.contains("Scan on emp"), "{unanalyzed}");
    assert!(unanalyzed.contains("Scan on dept"), "{unanalyzed}");
    assert!(unanalyzed.contains("Considered:"), "{unanalyzed}");

    // After ANALYZE the same plan reports measured statistics.
    for table in [&left, &right] {
        let stats = analyze_table(&table.table_ref()).expect("analyze");
        save_stats(&table.table_ref(), &stats).expect("save");
    }

    let analyzed = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
        .with_condition(eq(col("emp.dept"), col("dept.id")))
        .explain()
        .expect("explain");
    assert!(analyzed.contains("stats=analyzed"), "{analyzed}");
}

/// The plan a relation gets must not depend on which run it is.
#[test]
fn explain_output_is_reproducible() {
    let db = TempDb::new();
    let mut left = db.create_table("l", &[("k", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int)]);
    for i in 0..500 {
        left.insert(vec![int(i % 60)]);
        right.insert(vec![int(i % 40)]);
    }
    left.flush();
    right.flush();
    for table in [&left, &right] {
        let stats = analyze_table(&table.table_ref()).expect("analyze");
        save_stats(&table.table_ref(), &stats).expect("save");
    }

    let explain = || {
        JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
            .with_condition(eq(col("l.k"), col("r.k")))
            .explain()
            .expect("explain")
    };

    let first = explain();
    for _ in 0..5 {
        assert_eq!(explain(), first, "EXPLAIN must be stable across runs");
    }
}

/// A plan built from measured statistics should say so, and one built from a
/// table that cannot be read should admit that instead of pretending.
#[test]
fn confidence_is_the_weaker_of_the_two_inputs() {
    let db = TempDb::new();
    let mut left = db.create_table("l", &[("k", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int)]);
    for i in 0..100 {
        left.insert(vec![int(i)]);
        right.insert(vec![int(i)]);
    }
    left.flush();
    right.flush();

    // Analyze only one side.
    let stats = analyze_table(&left.table_ref()).expect("analyze");
    save_stats(&left.table_ref(), &stats).expect("save");

    let plan = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
        .with_condition(eq(col("l.k"), col("r.k")))
        .plan()
        .expect("plan");

    assert_eq!(
        plan.confidence,
        StatsConfidence::HeaderOnly,
        "one analyzed side does not make the whole plan analyzed"
    );
}

/// Estimates should be in the right ballpark once the statistics are real.
#[test]
fn estimates_are_close_to_reality_after_analyze() {
    let db = TempDb::new();
    let mut left = db.create_table("l", &[("k", DataType::Int)]);
    let mut right = db.create_table("r", &[("k", DataType::Int)]);
    // 600 left rows over 60 keys, 300 right rows over 60 keys: each key has
    // 10 left and 5 right rows, so the join produces 60 * 10 * 5 = 3000.
    for i in 0..600 {
        left.insert(vec![int(i % 60)]);
    }
    for i in 0..300 {
        right.insert(vec![int(i % 60)]);
    }
    left.flush();
    right.flush();
    for table in [&left, &right] {
        let stats = analyze_table(&table.table_ref()).expect("analyze");
        save_stats(&table.table_ref(), &stats).expect("save");
    }

    let plan = JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
        .with_condition(eq(col("l.k"), col("r.k")))
        .plan()
        .expect("plan");

    assert_eq!(plan.confidence, StatsConfidence::Analyzed);
    let estimated = plan.estimate.output_rows as f64;
    assert!(
        (estimated - 3_000.0).abs() / 3_000.0 < 0.20,
        "estimated {estimated} rows, actual is 3000"
    );
}
