//! Splitting a join condition into equijoin keys plus a residual.
//!
//! The previous implementation took its join key from `conditions[0]` and
//! never checked orientation, so multi-predicate joins, joins whose equality
//! was not written first, and joins written `right = left` all silently
//! returned wrong results. Those three cases are the reason this file exists.

#[path = "join_common/mod.rs"]
mod common;

use common::{all_of, col, compare, eq, int_literal, lt, named_relation};
use storage_manager::executor::selection::{ComparisonOp, Predicate};
use storage_manager::join::{
    JoinError, JoinType, KeyClass, PredicateSplit, SideResolver, pushdown_plan, split_conjuncts,
};
use storage_manager::types::DataType;

fn employees() -> storage_manager::join::RelationSchema {
    named_relation(
        "e",
        &[
            ("id", DataType::Int),
            ("dept_id", DataType::Int),
            ("region", DataType::Varchar(8)),
            ("salary", DataType::BigInt),
        ],
    )
}

fn departments() -> storage_manager::join::RelationSchema {
    named_relation(
        "d",
        &[
            ("id", DataType::Int),
            ("region", DataType::Varchar(8)),
            ("budget", DataType::BigInt),
        ],
    )
}

fn split_as(
    condition: Option<&Predicate>,
    join_type: JoinType,
) -> Result<PredicateSplit, JoinError> {
    let left = employees();
    let right = departments();
    let resolver = SideResolver::new(&left, &right).expect("distinct aliases");
    split_conjuncts(condition, &resolver, join_type)
}

fn split(condition: Option<&Predicate>) -> Result<PredicateSplit, JoinError> {
    split_as(condition, JoinType::Inner)
}

fn split_ok(condition: &Predicate) -> PredicateSplit {
    split(Some(condition)).expect("condition should split")
}

// ── Key extraction ───────────────────────────────────────────────────────────

#[test]
fn a_single_equality_becomes_the_key() {
    let result = split_ok(&eq(col("e.dept_id"), col("d.id")));

    assert_eq!(result.keys.len(), 1);
    assert_eq!(result.keys.columns[0].left_index, 1, "e.dept_id");
    assert_eq!(result.keys.columns[0].right_index, 0, "d.id");
    assert_eq!(result.keys.columns[0].class, KeyClass::Integer);
    assert!(result.residual.is_none());
    assert!(result.left_local.is_none());
    assert!(result.right_local.is_none());
}

/// Writing the equality right-side-first must produce the same key. The old
/// sort-merge join assigned `left_col = c.left_col` without checking which
/// relation it belonged to, and silently returned zero rows for this.
#[test]
fn orientation_is_normalised() {
    let forward = split_ok(&eq(col("e.dept_id"), col("d.id")));
    let reversed = split_ok(&eq(col("d.id"), col("e.dept_id")));

    assert_eq!(forward.keys, reversed.keys);
    assert_eq!(reversed.keys.columns[0].left_index, 1);
    assert_eq!(reversed.keys.columns[0].right_index, 0);
}

#[test]
fn every_equality_conjunct_becomes_a_key_component() {
    let result = split_ok(&all_of(vec![
        eq(col("e.dept_id"), col("d.id")),
        eq(col("e.region"), col("d.region")),
    ]));

    assert_eq!(result.keys.len(), 2);
    assert_eq!(
        result.keys.columns[0].class,
        KeyClass::Integer,
        "dept_id is INT"
    );
    assert_eq!(
        result.keys.columns[1].class,
        KeyClass::Varchar,
        "region is VARCHAR"
    );
    assert!(result.residual.is_none());
}

/// The exact shape the old code got wrong: a non-equi conjunct written first
/// became "the" join key, so only rows where `salary == budget` were ever
/// compared.
#[test]
fn a_leading_non_equi_conjunct_does_not_hide_the_key() {
    let result = split_ok(&all_of(vec![
        lt(col("e.salary"), col("d.budget")),
        eq(col("e.dept_id"), col("d.id")),
    ]));

    assert_eq!(result.keys.len(), 1, "the equality must still be found");
    assert_eq!(result.keys.columns[0].left_index, 1);
    assert_eq!(result.keys.columns[0].right_index, 0);
    assert!(
        result.residual.is_some(),
        "the inequality must be kept as a residual, not dropped"
    );
}

#[test]
fn a_condition_with_no_cross_relation_equality_yields_no_key() {
    let result = split_ok(&lt(col("e.salary"), col("d.budget")));

    assert!(
        result.keys.is_empty(),
        "an inequality is not a key; hash and sort-merge must be refused, not fed garbage"
    );
    assert!(result.residual.is_some());
}

// ── Things that look like equalities but are not join keys ───────────────────

/// Hoisting an equality out of a disjunction would change the result: the OR
/// can be satisfied by its other branch.
#[test]
fn equality_inside_an_or_is_not_hoisted() {
    let result = split_ok(&Predicate::or(
        eq(col("e.dept_id"), col("d.id")),
        lt(col("e.salary"), col("d.budget")),
    ));

    assert!(result.keys.is_empty());
    assert!(result.residual.is_some());
}

#[test]
fn negated_equality_is_not_hoisted() {
    let result = split_ok(&Predicate::not(eq(col("e.dept_id"), col("d.id"))));

    assert!(result.keys.is_empty());
    assert!(result.residual.is_some());
}

/// `e.dept_id + 1 = d.id` is an equality, but not between two columns, so it
/// cannot drive a key.
#[test]
fn equality_over_arithmetic_is_not_hoisted() {
    let condition = eq(
        storage_manager::executor::selection::Expr::Add(
            Box::new(col("e.dept_id")),
            Box::new(int_literal(1)),
        ),
        col("d.id"),
    );
    let result = split_ok(&condition);

    assert!(result.keys.is_empty());
    assert!(result.residual.is_some());
}

/// Both columns on the same side is a filter on that side, never a join key.
#[test]
fn same_side_equality_becomes_a_local_filter() {
    let result = split_ok(&eq(col("e.id"), col("e.dept_id")));

    assert!(result.keys.is_empty());
    assert!(result.left_local.is_some());
    assert!(result.residual.is_none());
    assert!(result.right_local.is_none());
}

// ── Local filters ────────────────────────────────────────────────────────────

#[test]
fn single_relation_conjuncts_are_separated_by_side() {
    let result = split_ok(&all_of(vec![
        eq(col("e.dept_id"), col("d.id")),
        compare(col("e.salary"), ComparisonOp::GreaterThan, int_literal(100)),
        compare(col("d.budget"), ComparisonOp::LessThan, int_literal(999)),
    ]));

    assert_eq!(result.keys.len(), 1);
    assert!(result.left_local.is_some(), "the e.salary filter");
    assert!(result.right_local.is_some(), "the d.budget filter");
    assert!(
        result.residual.is_none(),
        "nothing here spans both relations"
    );
}

/// Under LEFT OUTER the left side is row-preserving, so a left-only conjunct
/// cannot be pushed into the left scan - it must become part of the join
/// condition instead. Pushing it would drop rows the join is required to emit
/// NULL-extended.
#[test]
fn a_left_conjunct_moves_to_the_residual_under_left_outer() {
    let condition = all_of(vec![
        eq(col("e.dept_id"), col("d.id")),
        compare(col("e.salary"), ComparisonOp::GreaterThan, int_literal(100)),
        compare(col("d.budget"), ComparisonOp::LessThan, int_literal(999)),
    ]);

    let inner = split_as(Some(&condition), JoinType::Inner).expect("splits");
    assert!(inner.left_local.is_some(), "INNER may push the left filter");
    assert!(inner.right_local.is_some());
    assert!(inner.residual.is_none());

    let left_outer = split_as(Some(&condition), JoinType::LeftOuter).expect("splits");
    assert!(
        left_outer.left_local.is_none(),
        "LEFT OUTER must not push a left-only conjunct"
    );
    assert!(
        left_outer.residual.is_some(),
        "it has to be evaluated as part of the condition instead"
    );
    assert!(
        left_outer.right_local.is_some(),
        "the right side is not row-preserving, so its filter still pushes"
    );

    // The key survives regardless of where the filters land.
    assert_eq!(left_outer.keys, inner.keys);
}

#[test]
fn full_outer_pushes_nothing_down() {
    let condition = all_of(vec![
        eq(col("e.dept_id"), col("d.id")),
        compare(col("e.salary"), ComparisonOp::GreaterThan, int_literal(100)),
        compare(col("d.budget"), ComparisonOp::LessThan, int_literal(999)),
    ]);

    let result = split_as(Some(&condition), JoinType::FullOuter).expect("splits");
    assert!(result.left_local.is_none());
    assert!(result.right_local.is_none());
    assert!(
        result.residual.is_some(),
        "both filters have to be evaluated in the join condition"
    );
    assert_eq!(result.keys.len(), 1);
}

/// A conjunct with no column at all is constant-valued and must still be
/// evaluated, so it belongs in the residual rather than being dropped.
#[test]
fn constant_conjuncts_go_to_the_residual() {
    let result = split_ok(&compare(
        int_literal(1),
        ComparisonOp::Equals,
        int_literal(2),
    ));

    assert!(result.keys.is_empty());
    assert!(result.residual.is_some());
}

#[test]
fn an_absent_condition_splits_into_nothing() {
    let result = split(None).expect("no condition is valid");

    assert!(result.keys.is_empty());
    assert!(result.residual.is_none());
    assert!(result.left_local.is_none());
    assert!(result.right_local.is_none());
}

// ── Resolution errors ────────────────────────────────────────────────────────

/// `region` exists on both sides. Choosing one silently is how a self-join
/// ends up comparing a column to itself.
#[test]
fn an_ambiguous_unqualified_column_is_rejected() {
    let err = split(Some(&eq(col("region"), col("d.region"))))
        .expect_err("ambiguous column must be rejected");

    assert!(matches!(err, JoinError::Schema(_)), "got {err:?}");
    let rendered = err.to_string();
    assert!(rendered.contains("ambiguous"), "{rendered}");
    assert!(rendered.contains("region"), "{rendered}");
}

#[test]
fn an_unambiguous_unqualified_column_resolves() {
    let result = split_ok(&eq(col("dept_id"), col("d.id")));
    assert_eq!(result.keys.len(), 1);
    assert_eq!(result.keys.columns[0].left_index, 1);
}

#[test]
fn an_unknown_qualifier_is_rejected() {
    let err =
        split(Some(&eq(col("x.id"), col("d.id")))).expect_err("unknown relation must be rejected");
    assert!(err.to_string().contains("unknown relation"), "{err}");
}

#[test]
fn an_unknown_column_is_rejected() {
    let err = split(Some(&eq(col("e.missing"), col("d.id"))))
        .expect_err("unknown column must be rejected");
    assert!(err.to_string().contains("no column"), "{err}");

    let err = split(Some(&eq(col("nowhere"), col("d.id"))))
        .expect_err("unknown bare column must be rejected");
    assert!(err.to_string().contains("nowhere"), "{err}");
}

/// A self-join must alias its sides apart, or no qualified name means
/// anything.
#[test]
fn a_shared_alias_is_rejected() {
    let left = employees();
    let right = employees();
    let err = SideResolver::new(&left, &right).expect_err("a shared alias must be rejected");
    assert!(err.to_string().contains("distinct alias"), "{err}");
}

/// A self-join with distinct aliases resolves each side independently - the
/// case the old hash join got wrong by resolving both sides to the same
/// column.
#[test]
fn a_self_join_with_distinct_aliases_resolves_each_side() {
    let left = named_relation(
        "e1",
        &[("id", DataType::Int), ("manager_id", DataType::Int)],
    );
    let right = named_relation(
        "e2",
        &[("id", DataType::Int), ("manager_id", DataType::Int)],
    );
    let resolver = SideResolver::new(&left, &right).expect("distinct aliases");

    let result = split_conjuncts(
        Some(&eq(col("e1.manager_id"), col("e2.id"))),
        &resolver,
        JoinType::Inner,
    )
    .expect("should split");

    assert_eq!(result.keys.len(), 1);
    assert_eq!(result.keys.columns[0].left_index, 1, "e1.manager_id");
    assert_eq!(result.keys.columns[0].right_index, 0, "e2.id");
    assert_ne!(
        result.keys.columns[0].left_index, result.keys.columns[0].right_index,
        "the two sides must not collapse onto one column"
    );
}

#[test]
fn an_incomparable_key_pair_is_rejected_at_split_time() {
    let left = named_relation("l", &[("v", DataType::Int)]);
    let right = named_relation("r", &[("v", DataType::Real)]);
    let resolver = SideResolver::new(&left, &right).expect("distinct aliases");

    let err = split_conjuncts(
        Some(&eq(col("l.v"), col("r.v"))),
        &resolver,
        JoinType::Inner,
    )
    .expect_err("INT and REAL are not comparable");
    assert!(
        matches!(err, JoinError::KeyTypeMismatch { .. }),
        "got {err:?}"
    );
}

// ── Pushdown legality ────────────────────────────────────────────────────────

/// Pushing a conjunct into the row-preserving side of an outer join would drop
/// rows the join must emit NULL-extended.
#[test]
fn pushdown_is_blocked_on_row_preserving_sides() {
    assert!(pushdown_plan(JoinType::Inner).left);
    assert!(pushdown_plan(JoinType::Inner).right);

    assert!(
        !pushdown_plan(JoinType::LeftOuter).left,
        "left rows are preserved, so a left filter changes the result"
    );
    assert!(pushdown_plan(JoinType::LeftOuter).right);

    assert!(pushdown_plan(JoinType::RightOuter).left);
    assert!(!pushdown_plan(JoinType::RightOuter).right);

    assert!(!pushdown_plan(JoinType::FullOuter).left);
    assert!(!pushdown_plan(JoinType::FullOuter).right);

    for join_type in [JoinType::Semi, JoinType::Anti, JoinType::Cross] {
        let plan = pushdown_plan(join_type);
        assert!(plan.left && plan.right, "{join_type:?} preserves no rows");
    }
}
