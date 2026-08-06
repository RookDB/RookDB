//! End-to-end nested-loop joins, checked against the reference join.
//!
//! Every assertion compares full rows as a multiset, not row counts. The
//! previous suite only ever checked `result.tuples.len()`, which cannot see a
//! wrong value, a wrong column order, or a NULL in the wrong place.

#[path = "join_common/mod.rs"]
mod common;

use common::{
    TableHandle, TempDb, all_of, assert_rows_eq, col, collect_rows, compare, eq, int_literal, lt,
    reference_join,
};
use storage_manager::executor::selection::{ComparisonOp, Predicate};
use storage_manager::join::{
    JoinBuilder, JoinPredicate, JoinType, MatchEvaluator, SideResolver, split_conjuncts,
};
use storage_manager::types::{DataType, DataValue};

fn int(value: i32) -> Option<DataValue> {
    Some(DataValue::Int(value))
}

fn text(value: &str) -> Option<DataValue> {
    Some(DataValue::Varchar(value.to_string()))
}

/// Build the oracle's match evaluator.
///
/// Splitting with FULL OUTER is deliberate: that is the one join type where
/// nothing may be pushed into a scan, so every conjunct lands in the keys or
/// the residual and the oracle sees the *whole* condition. The join under test
/// splits with its own join type and may push filters into its scans, so
/// comparing the two also proves pushdown is result-preserving.
fn oracle_evaluator(
    left: &TableHandle,
    right: &TableHandle,
    condition: Option<&Predicate>,
) -> MatchEvaluator {
    let left_relation = left.relation_schema();
    let right_relation = right.relation_schema();
    let resolver = SideResolver::new(&left_relation, &right_relation).expect("distinct aliases");
    let split = split_conjuncts(condition, &resolver, JoinType::FullOuter).expect("splits");

    MatchEvaluator::new(
        split.keys,
        split
            .residual
            .map(|predicate| JoinPredicate::new(predicate, left_relation.len())),
    )
}

/// Run a join and assert it equals the reference result.
fn check(
    left: &TableHandle,
    right: &TableHandle,
    join_type: JoinType,
    condition: Option<Predicate>,
    block_rows: usize,
) {
    let mut builder = JoinBuilder::new(left.table_ref(), right.table_ref(), join_type)
        .with_block_rows(block_rows);
    if let Some(condition) = condition.clone() {
        builder = builder.with_condition(condition);
    }

    let stream = builder.execute().expect("join should plan");
    let actual = collect_rows(stream).expect("join should run");

    let evaluator = oracle_evaluator(left, right, condition.as_ref());
    let expected = reference_join(
        left.rows(),
        right.rows(),
        join_type,
        &evaluator,
        left.table_ref().columns.len(),
        right.table_ref().columns.len(),
    )
    .expect("reference join should run");

    assert_rows_eq(
        &actual,
        &expected,
        &format!("{join_type:?} with block_rows={block_rows}"),
    );
}

/// Employees and departments with duplicate keys on both sides, an unmatched
/// row on each side, and a NULL key.
fn fixture(db: &TempDb) -> (TableHandle, TableHandle) {
    let mut employees = db.create_table(
        "e",
        &[
            ("id", DataType::Int),
            ("dept_id", DataType::Int),
            ("salary", DataType::Int),
        ],
    );
    employees.insert_all(vec![
        vec![int(1), int(10), int(100)],
        vec![int(2), int(10), int(200)],
        vec![int(3), int(20), int(300)],
        vec![int(4), int(99), int(400)], // matches nothing
        vec![int(5), None, int(500)],    // NULL key: matches nothing, ever
    ]);
    employees.flush();

    let mut departments = db.create_table(
        "d",
        &[
            ("id", DataType::Int),
            ("name", DataType::Varchar(12)),
            ("budget", DataType::Int),
        ],
    );
    departments.insert_all(vec![
        vec![int(10), text("eng"), int(250)],
        vec![int(10), text("eng-2"), int(150)], // duplicate on the right
        vec![int(20), text("sales"), int(350)],
        vec![int(30), text("hr"), int(50)], // matches nothing
        vec![None, text("ghost"), int(0)],  // NULL key
    ]);
    departments.flush();

    (employees, departments)
}

fn equi() -> Predicate {
    eq(col("e.dept_id"), col("d.id"))
}

// ── Every join type, both block sizes ────────────────────────────────────────

/// The full matrix of join types, run as a simple nested loop (block of one)
/// and as a blocked one, both compared against the reference.
#[test]
fn every_join_type_matches_the_reference() {
    let db = TempDb::new();
    let (employees, departments) = fixture(&db);

    for join_type in [
        JoinType::Inner,
        JoinType::LeftOuter,
        JoinType::RightOuter,
        JoinType::FullOuter,
        JoinType::Semi,
        JoinType::Anti,
    ] {
        for block_rows in [1, 2, 1024] {
            check(
                &employees,
                &departments,
                join_type,
                Some(equi()),
                block_rows,
            );
        }
    }
}

/// A CROSS join carries no condition and produces every pair.
#[test]
fn cross_join_produces_the_full_product() {
    let db = TempDb::new();
    let (employees, departments) = fixture(&db);

    for block_rows in [1, 2, 1024] {
        check(&employees, &departments, JoinType::Cross, None, block_rows);
    }

    let stream = JoinBuilder::new(
        employees.table_ref(),
        departments.table_ref(),
        JoinType::Cross,
    )
    .execute()
    .expect("cross join should plan");
    let rows = collect_rows(stream).expect("cross join should run");
    assert_eq!(rows.len(), 5 * 5, "every pair, including the NULL-key rows");
    assert_eq!(rows[0].len(), 6, "three columns from each side");
}

// ── NULL keys ────────────────────────────────────────────────────────────────

/// A NULL join key matches nothing - not another NULL, not anything else.
/// The employee with a NULL `dept_id` and the department with a NULL `id` must
/// never be paired.
#[test]
fn null_keys_never_match_each_other() {
    let db = TempDb::new();
    let (employees, departments) = fixture(&db);

    let stream = JoinBuilder::new(
        employees.table_ref(),
        departments.table_ref(),
        JoinType::Inner,
    )
    .with_condition(equi())
    .execute()
    .expect("plans");
    let rows = collect_rows(stream).expect("runs");

    for row in &rows {
        assert!(
            row[1].is_some(),
            "an INNER join emitted a row with a NULL join key: {row:?}"
        );
        assert!(row[3].is_some(), "right-side key must be non-NULL: {row:?}");
    }

    // The NULL-key employee still appears in a LEFT OUTER, NULL-extended.
    let stream = JoinBuilder::new(
        employees.table_ref(),
        departments.table_ref(),
        JoinType::LeftOuter,
    )
    .with_condition(equi())
    .execute()
    .expect("plans");
    let rows = collect_rows(stream).expect("runs");

    let null_key_row: Vec<_> = rows.iter().filter(|row| row[0] == int(5)).collect();
    assert_eq!(null_key_row.len(), 1);
    assert_eq!(
        null_key_row[0][3], None,
        "the unmatched employee must be NULL-extended on the right"
    );

    // And in an ANTI join, because it matched nothing.
    let stream = JoinBuilder::new(
        employees.table_ref(),
        departments.table_ref(),
        JoinType::Anti,
    )
    .with_condition(equi())
    .execute()
    .expect("plans");
    let rows = collect_rows(stream).expect("runs");
    assert!(
        rows.iter().any(|row| row[0] == int(5)),
        "a NULL-key row matches nothing, so ANTI must keep it"
    );
}

// ── Multi-predicate and non-equi conditions ──────────────────────────────────

/// An equality plus an inequality: the equality drives the key, the inequality
/// stays as a residual, and both are applied.
#[test]
fn mixed_equi_and_non_equi_conditions_apply_both() {
    let db = TempDb::new();
    let (employees, departments) = fixture(&db);

    let condition = all_of(vec![equi(), lt(col("e.salary"), col("d.budget"))]);

    for join_type in [JoinType::Inner, JoinType::LeftOuter, JoinType::Semi] {
        check(
            &employees,
            &departments,
            join_type,
            Some(condition.clone()),
            2,
        );
    }

    let stream = JoinBuilder::new(
        employees.table_ref(),
        departments.table_ref(),
        JoinType::Inner,
    )
    .with_condition(condition)
    .execute()
    .expect("plans");
    let rows = collect_rows(stream).expect("runs");

    for row in &rows {
        let salary = row[2].clone().expect("salary is not null");
        let budget = row[5].clone().expect("budget is not null");
        assert!(
            matches!(
                (&salary, &budget),
                (DataValue::Int(s), DataValue::Int(b)) if s < b
            ),
            "the residual was not applied: {row:?}"
        );
    }
}

/// Writing the equality with the right relation first must not change
/// anything.
#[test]
fn reversed_condition_orientation_gives_the_same_result() {
    let db = TempDb::new();
    let (employees, departments) = fixture(&db);

    for join_type in [JoinType::Inner, JoinType::LeftOuter, JoinType::FullOuter] {
        let forward = collect_rows(
            JoinBuilder::new(employees.table_ref(), departments.table_ref(), join_type)
                .with_condition(eq(col("e.dept_id"), col("d.id")))
                .execute()
                .expect("plans"),
        )
        .expect("runs");

        let reversed = collect_rows(
            JoinBuilder::new(employees.table_ref(), departments.table_ref(), join_type)
                .with_condition(eq(col("d.id"), col("e.dept_id")))
                .execute()
                .expect("plans"),
        )
        .expect("runs");

        assert_rows_eq(&reversed, &forward, &format!("{join_type:?} reversed"));
    }
}

/// A pure inequality has no key at all; nested loop must still handle it.
#[test]
fn a_non_equi_only_condition_still_joins() {
    let db = TempDb::new();
    let (employees, departments) = fixture(&db);

    let condition = lt(col("e.salary"), col("d.budget"));
    for join_type in [JoinType::Inner, JoinType::LeftOuter, JoinType::Anti] {
        check(
            &employees,
            &departments,
            join_type,
            Some(condition.clone()),
            2,
        );
    }
}

// ── Pushdown ─────────────────────────────────────────────────────────────────

/// A single-relation conjunct is pushed into that relation's scan for an INNER
/// join, and must produce the same rows as evaluating it in the condition.
#[test]
fn pushed_down_filters_do_not_change_the_result() {
    let db = TempDb::new();
    let (employees, departments) = fixture(&db);

    let condition = all_of(vec![
        equi(),
        compare(col("e.salary"), ComparisonOp::GreaterThan, int_literal(150)),
        compare(col("d.budget"), ComparisonOp::LessThan, int_literal(400)),
    ]);

    // INNER pushes both filters; LEFT OUTER may only push the right one;
    // FULL OUTER pushes neither. All three are compared against a reference
    // that pushes nothing.
    for join_type in [JoinType::Inner, JoinType::LeftOuter, JoinType::FullOuter] {
        check(
            &employees,
            &departments,
            join_type,
            Some(condition.clone()),
            2,
        );
    }
}

// ── Self-join ────────────────────────────────────────────────────────────────

/// A self-join must resolve each side independently. The old hash join
/// resolved both build and probe keys to the same column and silently missed
/// every match.
#[test]
fn a_self_join_resolves_the_two_sides_separately() {
    let db = TempDb::new();
    let mut employees = db.create_table(
        "e1",
        &[("id", DataType::Int), ("manager_id", DataType::Int)],
    );
    employees.insert_all(vec![
        vec![int(1), None],
        vec![int(2), int(1)],
        vec![int(3), int(1)],
        vec![int(4), int(2)],
    ]);
    employees.flush();

    // A second handle over the same file, aliased differently.
    let mut managers = employees.table_ref();
    managers.alias = "e2".to_string();

    let stream = JoinBuilder::new(employees.table_ref(), managers, JoinType::Inner)
        .with_condition(eq(col("e1.manager_id"), col("e2.id")))
        .execute()
        .expect("plans");
    let rows = collect_rows(stream).expect("runs");

    // 2→1, 3→1, 4→2.
    assert_eq!(rows.len(), 3, "got {rows:#?}");
    for row in &rows {
        assert_eq!(
            row[1], row[2],
            "manager_id on the left must equal id on the right: {row:?}"
        );
        assert_ne!(row[0], row[2], "nobody here manages themselves: {row:?}");
    }
}

// ── Output shape ─────────────────────────────────────────────────────────────

/// SEMI and ANTI emit the left relation's columns only.
#[test]
fn semi_and_anti_emit_left_columns_only() {
    let db = TempDb::new();
    let (employees, departments) = fixture(&db);

    for join_type in [JoinType::Semi, JoinType::Anti] {
        let builder = JoinBuilder::new(employees.table_ref(), departments.table_ref(), join_type)
            .with_condition(equi());
        let schema = builder.output_schema().expect("schema");
        assert_eq!(schema.len(), 3, "{join_type:?} must emit only e's columns");
        assert_eq!(schema.right_width(), 0);

        let rows = collect_rows(builder.execute().expect("plans")).expect("runs");
        for row in &rows {
            assert_eq!(row.len(), 3, "{join_type:?} row width");
        }
    }
}

/// Outer joins mark the null-extendable side's columns nullable, and emit
/// output columns in left-then-right order.
#[test]
fn output_schema_reflects_the_join_type() {
    let db = TempDb::new();
    let (employees, departments) = fixture(&db);

    let names = |join_type: JoinType| {
        JoinBuilder::new(employees.table_ref(), departments.table_ref(), join_type)
            .output_schema()
            .expect("schema")
            .columns
            .iter()
            .map(|c| c.qualified_name.clone())
            .collect::<Vec<_>>()
    };

    assert_eq!(
        names(JoinType::Inner),
        vec![
            "e.id",
            "e.dept_id",
            "e.salary",
            "d.id",
            "d.name",
            "d.budget"
        ]
    );

    let left_outer = JoinBuilder::new(
        employees.table_ref(),
        departments.table_ref(),
        JoinType::LeftOuter,
    )
    .output_schema()
    .expect("schema");
    assert!(
        left_outer.columns[3].nullable,
        "unmatched left rows put NULLs in the right columns"
    );

    let right_outer = JoinBuilder::new(
        employees.table_ref(),
        departments.table_ref(),
        JoinType::RightOuter,
    )
    .output_schema()
    .expect("schema");
    assert!(
        right_outer.columns[0].nullable,
        "unmatched right rows put NULLs in the left columns"
    );
}

// ── Counters ─────────────────────────────────────────────────────────────────

/// A larger block means fewer passes over the inner relation. This is the only
/// difference between the simple and blocked variants, so it is worth pinning.
#[test]
fn block_size_controls_the_number_of_inner_scans() {
    let db = TempDb::new();
    let (employees, departments) = fixture(&db);

    let rescans = |block_rows: usize| {
        let mut stream = JoinBuilder::new(
            employees.table_ref(),
            departments.table_ref(),
            JoinType::Inner,
        )
        .with_condition(equi())
        .with_block_rows(block_rows)
        .execute()
        .expect("plans");
        while let Some(row) = stream.next() {
            row.expect("runs");
        }
        stream.stats().inner_rescans
    };

    // Five outer rows: one scan each, three scans of two, one scan of all.
    assert_eq!(rescans(1), 5);
    assert_eq!(rescans(2), 3);
    assert_eq!(rescans(1024), 1);
}
