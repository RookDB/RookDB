//! Two-relation residual evaluation under SQL three-valued logic.
//!
//! Only `TriValue::True` may produce an output row. The distinction that
//! matters throughout is `False` versus `Unknown`: a comparison involving NULL
//! is `Unknown`, and `NOT Unknown` is still `Unknown`, so a NULL can never be
//! made to match by negating a condition.

#[path = "join_common/mod.rs"]
mod common;

use common::{compare, int_literal, null_literal, text_literal, vcol};
use storage_manager::executor::selection::{ComparisonOp, Expr, Predicate, TriValue};
use storage_manager::join::{JoinError, JoinPredicate};
use storage_manager::types::{DataValue, OrderedF64};

/// Left row has two columns (indices 0, 1); right row has two (indices 2, 3).
const LEFT_WIDTH: usize = 2;

fn eval(
    predicate: Predicate,
    left: &[Option<DataValue>],
    right: &[Option<DataValue>],
) -> Result<TriValue, JoinError> {
    JoinPredicate::new(predicate, LEFT_WIDTH).evaluate(left, right)
}

fn eval_ok(
    predicate: Predicate,
    left: &[Option<DataValue>],
    right: &[Option<DataValue>],
) -> TriValue {
    eval(predicate, left, right).expect("evaluation should succeed")
}

fn int(value: i32) -> Option<DataValue> {
    Some(DataValue::Int(value))
}

fn text(value: &str) -> Option<DataValue> {
    Some(DataValue::Varchar(value.to_string()))
}

// ── Comparison across the two relations ──────────────────────────────────────

#[test]
fn cross_relation_comparison_reads_from_both_rows() {
    let left = vec![int(5), text("a")];
    let right = vec![int(9), text("b")];

    // v0 (left col 0) < v2 (right col 0)
    assert_eq!(
        eval_ok(
            compare(vcol(0), ComparisonOp::LessThan, vcol(2)),
            &left,
            &right
        ),
        TriValue::True
    );
    assert_eq!(
        eval_ok(
            compare(vcol(0), ComparisonOp::GreaterThan, vcol(2)),
            &left,
            &right
        ),
        TriValue::False
    );
    // v1 (left col 1) = v3 (right col 1)
    assert_eq!(
        eval_ok(
            compare(vcol(1), ComparisonOp::Equals, vcol(3)),
            &left,
            &right
        ),
        TriValue::False
    );
}

#[test]
fn every_comparison_operator_behaves() {
    let left = vec![int(5), None];
    let right = vec![int(5), None];

    for (op, expected) in [
        (ComparisonOp::Equals, TriValue::True),
        (ComparisonOp::NotEquals, TriValue::False),
        (ComparisonOp::LessThan, TriValue::False),
        (ComparisonOp::LessOrEqual, TriValue::True),
        (ComparisonOp::GreaterThan, TriValue::False),
        (ComparisonOp::GreaterOrEqual, TriValue::True),
    ] {
        assert_eq!(
            eval_ok(compare(vcol(0), op, vcol(2)), &left, &right),
            expected,
            "5 {op:?} 5"
        );
    }
}

// ── NULL semantics ───────────────────────────────────────────────────────────

/// A comparison with a NULL operand is Unknown, for every operator - including
/// `<>`, which is where two-valued implementations usually go wrong.
#[test]
fn any_null_operand_makes_a_comparison_unknown() {
    let left = vec![None, None];
    let right = vec![int(1), None];

    for op in [
        ComparisonOp::Equals,
        ComparisonOp::NotEquals,
        ComparisonOp::LessThan,
        ComparisonOp::LessOrEqual,
        ComparisonOp::GreaterThan,
        ComparisonOp::GreaterOrEqual,
    ] {
        assert_eq!(
            eval_ok(compare(vcol(0), op, vcol(2)), &left, &right),
            TriValue::Unknown,
            "NULL {op:?} 1 must be Unknown"
        );
    }

    // NULL compared to NULL is still Unknown, not True.
    assert_eq!(
        eval_ok(
            compare(vcol(0), ComparisonOp::Equals, vcol(3)),
            &left,
            &right
        ),
        TriValue::Unknown
    );
}

/// Negating an Unknown does not turn it into a match. This is what stops an
/// ANTI join from emitting rows whose key is NULL.
#[test]
fn not_unknown_is_still_unknown() {
    let left = vec![None, None];
    let right = vec![int(1), None];

    let inner = compare(vcol(0), ComparisonOp::Equals, vcol(2));
    assert_eq!(eval_ok(inner.clone(), &left, &right), TriValue::Unknown);
    assert_eq!(
        eval_ok(Predicate::not(inner), &left, &right),
        TriValue::Unknown
    );
}

#[test]
fn not_inverts_known_values() {
    let left = vec![int(1), None];
    let right = vec![int(1), None];

    let equal = compare(vcol(0), ComparisonOp::Equals, vcol(2));
    assert_eq!(eval_ok(equal.clone(), &left, &right), TriValue::True);
    assert_eq!(
        eval_ok(Predicate::not(equal), &left, &right),
        TriValue::False
    );
}

/// `IS NULL` and `IS NOT NULL` are always definite - they never return
/// Unknown, which is exactly what makes them usable to test for NULL.
#[test]
fn is_null_is_never_unknown() {
    let left = vec![None, int(1)];
    let right = vec![None, None];

    assert_eq!(
        eval_ok(Predicate::IsNull(Box::new(vcol(0))), &left, &right),
        TriValue::True
    );
    assert_eq!(
        eval_ok(Predicate::IsNull(Box::new(vcol(1))), &left, &right),
        TriValue::False
    );
    assert_eq!(
        eval_ok(Predicate::IsNotNull(Box::new(vcol(1))), &left, &right),
        TriValue::True
    );
    assert_eq!(
        eval_ok(Predicate::IsNotNull(Box::new(vcol(2))), &left, &right),
        TriValue::False
    );
}

// ── Three-valued AND / OR ────────────────────────────────────────────────────

/// The full 3VL truth tables. `False AND Unknown` is False and
/// `True OR Unknown` is True - the two cases short-circuiting must preserve.
#[test]
fn and_or_follow_the_three_valued_truth_tables() {
    // Column 0 = TRUE source, column 1 = FALSE source, column 2 = NULL source.
    let left = vec![int(1), int(2)];
    let right = vec![None, int(1)];

    let is_true = compare(vcol(0), ComparisonOp::Equals, vcol(3)); // 1 = 1
    let is_false = compare(vcol(1), ComparisonOp::Equals, vcol(3)); // 2 = 1
    let is_unknown = compare(vcol(0), ComparisonOp::Equals, vcol(2)); // 1 = NULL

    assert_eq!(eval_ok(is_true.clone(), &left, &right), TriValue::True);
    assert_eq!(eval_ok(is_false.clone(), &left, &right), TriValue::False);
    assert_eq!(
        eval_ok(is_unknown.clone(), &left, &right),
        TriValue::Unknown
    );

    let cases = [
        (&is_true, &is_true, TriValue::True, TriValue::True),
        (&is_true, &is_false, TriValue::False, TriValue::True),
        (&is_true, &is_unknown, TriValue::Unknown, TriValue::True),
        (&is_false, &is_false, TriValue::False, TriValue::False),
        (&is_false, &is_unknown, TriValue::False, TriValue::Unknown),
        (
            &is_unknown,
            &is_unknown,
            TriValue::Unknown,
            TriValue::Unknown,
        ),
    ];

    for (a, b, expected_and, expected_or) in cases {
        assert_eq!(
            eval_ok(Predicate::and(a.clone(), b.clone()), &left, &right),
            expected_and,
            "AND"
        );
        assert_eq!(
            eval_ok(Predicate::or(a.clone(), b.clone()), &left, &right),
            expected_or,
            "OR"
        );
        // AND and OR are commutative in 3VL, so short-circuiting must not
        // make the order observable.
        assert_eq!(
            eval_ok(Predicate::and(b.clone(), a.clone()), &left, &right),
            expected_and,
            "AND is commutative"
        );
        assert_eq!(
            eval_ok(Predicate::or(b.clone(), a.clone()), &left, &right),
            expected_or,
            "OR is commutative"
        );
    }
}

// ── Arithmetic ───────────────────────────────────────────────────────────────

#[test]
fn arithmetic_in_a_join_condition_is_evaluated() {
    let left = vec![int(10), None];
    let right = vec![int(3), None];

    // v0 - v2 = 7
    let condition = compare(
        Expr::Sub(Box::new(vcol(0)), Box::new(vcol(2))),
        ComparisonOp::Equals,
        int_literal(7),
    );
    assert_eq!(eval_ok(condition, &left, &right), TriValue::True);

    // Integer division truncates toward zero, matching the rest of the engine.
    let condition = compare(
        Expr::Div(Box::new(vcol(0)), Box::new(vcol(2))),
        ComparisonOp::Equals,
        int_literal(3),
    );
    assert_eq!(eval_ok(condition, &left, &right), TriValue::True);
}

#[test]
fn arithmetic_with_a_null_operand_is_null() {
    let left = vec![None, None];
    let right = vec![int(3), None];

    let condition = compare(
        Expr::Add(Box::new(vcol(0)), Box::new(vcol(2))),
        ComparisonOp::Equals,
        int_literal(3),
    );
    assert_eq!(eval_ok(condition, &left, &right), TriValue::Unknown);
}

#[test]
fn division_by_zero_is_an_error_not_a_silent_infinity() {
    let left = vec![int(1), None];
    let right = vec![int(0), None];

    let condition = compare(
        Expr::Div(Box::new(vcol(0)), Box::new(vcol(2))),
        ComparisonOp::Equals,
        int_literal(0),
    );
    let err = eval(condition, &left, &right).expect_err("division by zero must be reported");
    assert!(err.to_string().contains("zero"), "{err}");
}

// ── BETWEEN, IN, LIKE ────────────────────────────────────────────────────────

#[test]
fn between_is_inclusive_on_both_ends() {
    let left = vec![int(5), None];
    let right = vec![int(1), int(9)];

    let between =
        |value: Expr| Predicate::Between(Box::new(value), Box::new(vcol(2)), Box::new(vcol(3)));

    assert_eq!(eval_ok(between(vcol(0)), &left, &right), TriValue::True);
    assert_eq!(
        eval_ok(between(int_literal(1)), &left, &right),
        TriValue::True,
        "lower bound is included"
    );
    assert_eq!(
        eval_ok(between(int_literal(9)), &left, &right),
        TriValue::True,
        "upper bound is included"
    );
    assert_eq!(
        eval_ok(between(int_literal(10)), &left, &right),
        TriValue::False
    );
}

#[test]
fn between_with_a_null_bound_is_unknown_unless_already_false() {
    let left = vec![int(5), None];
    let right = vec![None, int(9)];

    // Lower bound is NULL, value is within the upper bound → Unknown.
    assert_eq!(
        eval_ok(
            Predicate::Between(Box::new(vcol(0)), Box::new(vcol(2)), Box::new(vcol(3))),
            &left,
            &right
        ),
        TriValue::Unknown
    );

    // Value exceeds the known upper bound, so the answer is False regardless
    // of the unknown lower bound.
    let left = vec![int(50), None];
    assert_eq!(
        eval_ok(
            Predicate::Between(Box::new(vcol(0)), Box::new(vcol(2)), Box::new(vcol(3))),
            &left,
            &right
        ),
        TriValue::False
    );
}

#[test]
fn in_returns_unknown_only_when_it_cannot_decide() {
    let left = vec![int(2), None];
    let right = vec![int(1), None];

    let in_list = |items: Vec<Expr>| Predicate::In(Box::new(vcol(0)), items);

    assert_eq!(
        eval_ok(in_list(vec![int_literal(1), int_literal(2)]), &left, &right),
        TriValue::True
    );
    assert_eq!(
        eval_ok(in_list(vec![int_literal(3), int_literal(4)]), &left, &right),
        TriValue::False
    );
    // A NULL in the list and no match → Unknown, not False.
    assert_eq!(
        eval_ok(in_list(vec![int_literal(3), null_literal()]), &left, &right),
        TriValue::Unknown
    );
    // A NULL in the list but a definite match → True.
    assert_eq!(
        eval_ok(in_list(vec![int_literal(2), null_literal()]), &left, &right),
        TriValue::True
    );
    // NULL IN anything is Unknown.
    let left_null = vec![None, None];
    assert_eq!(
        eval_ok(in_list(vec![int_literal(1)]), &left_null, &right),
        TriValue::Unknown
    );
}

#[test]
fn like_handles_percent_and_underscore() {
    let left = vec![text("hello"), None];
    let right = vec![None, None];

    let like = |pattern: &str| Predicate::Like(Box::new(vcol(0)), pattern.to_string(), None);

    assert_eq!(eval_ok(like("hello"), &left, &right), TriValue::True);
    assert_eq!(eval_ok(like("h%"), &left, &right), TriValue::True);
    assert_eq!(eval_ok(like("%llo"), &left, &right), TriValue::True);
    assert_eq!(eval_ok(like("h_llo"), &left, &right), TriValue::True);
    assert_eq!(eval_ok(like("h_lo"), &left, &right), TriValue::False);
    assert_eq!(eval_ok(like("ello"), &left, &right), TriValue::False);

    // The pattern is anchored, so a bare substring does not match.
    assert_eq!(eval_ok(like("ell"), &left, &right), TriValue::False);
    assert_eq!(eval_ok(like("%ell%"), &left, &right), TriValue::True);
}

/// Regex metacharacters in a LIKE pattern are literals, not operators.
#[test]
fn like_treats_regex_metacharacters_literally() {
    let left = vec![text("a.c"), None];
    let right = vec![None, None];

    let like = |pattern: &str| Predicate::Like(Box::new(vcol(0)), pattern.to_string(), None);

    assert_eq!(eval_ok(like("a.c"), &left, &right), TriValue::True);
    assert_eq!(
        eval_ok(like("a.b"), &left, &right),
        TriValue::False,
        "'.' must not match any character"
    );

    let left = vec![text("abc"), None];
    assert_eq!(
        eval_ok(like("a.c"), &left, &right),
        TriValue::False,
        "'.' is a literal dot, so it must not match 'b'"
    );
}

#[test]
fn like_against_null_is_unknown() {
    let left = vec![None, None];
    let right = vec![None, None];

    assert_eq!(
        eval_ok(
            Predicate::Like(Box::new(vcol(0)), "%".to_string(), None),
            &left,
            &right
        ),
        TriValue::Unknown
    );
}

// ── Errors rather than wrong answers ─────────────────────────────────────────

/// Comparing incomparable types is an error, exactly as it is in the engine's
/// single-relation selection path. Returning False here would silently drop
/// rows instead of reporting a broken query.
#[test]
fn comparing_incomparable_types_is_an_error() {
    let left = vec![Some(DataValue::Int(1)), None];
    let right = vec![Some(DataValue::DoublePrecision(OrderedF64(1.0))), None];

    let err = eval(
        compare(vcol(0), ComparisonOp::Equals, vcol(2)),
        &left,
        &right,
    )
    .expect_err("INT and DOUBLE PRECISION are not comparable");
    assert!(matches!(err, JoinError::Schema(_)), "got {err:?}");
}

/// A comparison against a text literal reaches the same limitation the rest of
/// the engine has: `Constant::Text` becomes VARCHAR, which is not comparable
/// to CHAR. Recording it here so a future upstream fix is noticed.
#[test]
fn char_against_a_text_literal_is_currently_an_error() {
    let left = vec![Some(DataValue::Char("ab".to_string())), None];
    let right = vec![None, None];

    let result = eval(
        compare(vcol(0), ComparisonOp::Equals, text_literal("ab")),
        &left,
        &right,
    );
    assert!(
        result.is_err(),
        "if this now succeeds, upstream made CHAR and VARCHAR comparable"
    );
}

#[test]
fn an_out_of_range_column_index_is_reported() {
    let left = vec![int(1), None];
    let right = vec![int(1), None];

    let err = eval(
        compare(vcol(99), ComparisonOp::Equals, vcol(0)),
        &left,
        &right,
    )
    .expect_err("index 99 does not exist");
    assert!(matches!(err, JoinError::Schema(_)), "got {err:?}");
}

#[test]
fn an_unresolved_column_is_reported() {
    let left = vec![int(1), None];
    let right = vec![int(1), None];

    let unresolved = Expr::Column(storage_manager::executor::selection::ColumnReference::new(
        "x".to_string(),
    ));
    let err = eval(
        compare(unresolved, ComparisonOp::Equals, vcol(0)),
        &left,
        &right,
    )
    .expect_err("an unresolved column reference must be reported");
    assert!(err.to_string().contains("never resolved"), "{err}");
}
