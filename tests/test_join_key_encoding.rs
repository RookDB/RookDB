//! The join key encoding must agree with `Comparable::compare` exactly.
//!
//! Every equi-based algorithm in the subsystem decides matching and ordering
//! from `JoinKey` bytes alone. If those bytes disagree with the engine's own
//! comparison semantics, joins return wrong answers rather than errors - so
//! this file is the load-bearing test for the whole subsystem.

#[path = "join_common/mod.rs"]
mod common;

use std::cmp::Ordering;

use common::{Rng, random_data_type, random_value, seed_from_env};
use storage_manager::join::{JoinError, KeyClass, KeyColumn, KeySpec, resolve_key_class};
use storage_manager::types::comparison::Comparable;
use storage_manager::types::{DataType, DataValue, NumericValue, OrderedF32, OrderedF64};

/// Build a single-column key spec over `data_type` for both sides.
fn spec_for(data_type: &DataType) -> KeySpec {
    KeySpec::new(vec![KeyColumn {
        left_index: 0,
        right_index: 0,
        class: KeyClass::of(data_type),
    }])
}

fn key_of(data_type: &DataType, value: DataValue) -> Vec<u8> {
    spec_for(data_type)
        .left_key(&[Some(value)])
        .expect("encoding should succeed")
        .expect("a non-NULL value has a key")
        .as_bytes()
        .to_vec()
}

// ── The central invariant ────────────────────────────────────────────────────

/// For any two values of the same type, byte order of their keys must equal
/// `Comparable::compare`, and key equality must equal `is_equal`.
#[test]
fn key_order_matches_comparable_for_every_type() {
    let seed = seed_from_env(0x5EED_1001);
    println!("test_join_key_encoding::order seed = {seed}");
    let mut rng = Rng::new(seed);

    let mut checked = 0usize;
    for _ in 0..4_000 {
        let data_type = random_data_type(&mut rng);
        let left = random_value(&mut rng, &data_type);
        let right = random_value(&mut rng, &data_type);

        let expected = match left.compare(&right) {
            Ok(ordering) => ordering,
            // Same type on both sides, so this should not happen; if it does,
            // the class table and `compare` have diverged.
            Err(e) => panic!("values of type {data_type} were not comparable: {e}"),
        };

        let left_key = key_of(&data_type, left.clone());
        let right_key = key_of(&data_type, right.clone());
        let actual = left_key.cmp(&right_key);

        assert_eq!(
            actual, expected,
            "key order disagrees with Comparable for {data_type}\n\
             left  = {left:?}\nright = {right:?}"
        );

        assert_eq!(
            left_key == right_key,
            left.is_equal(&right) == Ok(true),
            "key equality disagrees with Comparable for {data_type}\n\
             left = {left:?}\nright = {right:?}"
        );

        checked += 1;
    }

    assert!(checked > 3_500, "only {checked} pairs were compared");
}

// ── The cases where upstream's own APIs disagree ─────────────────────────────

/// `Comparable::compare` says `+0.0 == -0.0`; `DataValue`'s derived `PartialEq`
/// says they differ. The key follows `compare`.
#[test]
fn signed_zero_encodes_identically() {
    for (ty, positive, negative) in [
        (
            DataType::Real,
            DataValue::Real(OrderedF32(0.0)),
            DataValue::Real(OrderedF32(-0.0)),
        ),
        (
            DataType::DoublePrecision,
            DataValue::DoublePrecision(OrderedF64(0.0)),
            DataValue::DoublePrecision(OrderedF64(-0.0)),
        ),
    ] {
        assert_eq!(
            positive.compare(&negative),
            Ok(Ordering::Equal),
            "premise: compare treats signed zeros as equal"
        );
        assert_ne!(positive, negative, "premise: derived PartialEq does not");
        assert_eq!(
            key_of(&ty, positive),
            key_of(&ty, negative),
            "{ty}: signed zeros must share one key"
        );
    }
}

/// All NaNs are equal to each other and greater than every real value.
#[test]
fn nan_is_canonical_and_sorts_last() {
    let ty = DataType::DoublePrecision;
    let nan = key_of(&ty, DataValue::DoublePrecision(OrderedF64(f64::NAN)));
    let other_nan = key_of(
        &ty,
        DataValue::DoublePrecision(OrderedF64(f64::from_bits(0x7FF8_0000_0000_0001))),
    );
    let infinity = key_of(&ty, DataValue::DoublePrecision(OrderedF64(f64::INFINITY)));
    let large = key_of(&ty, DataValue::DoublePrecision(OrderedF64(f64::MAX)));

    assert_eq!(nan, other_nan, "NaNs with different payloads must agree");
    assert!(nan > infinity, "NaN must sort above +INFINITY");
    assert!(infinity > large, "+INFINITY must sort above MAX");
}

/// CHAR comparison strips trailing whitespace - all of it, not just spaces,
/// because `compare` uses `str::trim_end`.
#[test]
fn char_ignores_trailing_whitespace() {
    let ty = DataType::Char(8);
    let plain = key_of(&ty, DataValue::Char("ab".to_string()));

    for padded in ["ab ", "ab   ", "ab\t", "ab \t "] {
        assert_eq!(
            key_of(&ty, DataValue::Char(padded.to_string())),
            plain,
            "CHAR {padded:?} must key the same as \"ab\""
        );
    }

    // Leading whitespace is significant.
    assert_ne!(key_of(&ty, DataValue::Char(" ab".to_string())), plain);
}

/// VARCHAR keeps every byte, so it must not collapse the way CHAR does.
#[test]
fn varchar_keeps_trailing_spaces() {
    let ty = DataType::Varchar(8);
    assert_ne!(
        key_of(&ty, DataValue::Varchar("ab".to_string())),
        key_of(&ty, DataValue::Varchar("ab ".to_string()))
    );
}

/// SMALLINT, INT and BIGINT all widen to one class, so equal values key
/// identically regardless of declared width.
#[test]
fn integer_widths_share_one_encoding() {
    let small = key_of(&DataType::SmallInt, DataValue::SmallInt(42));
    let medium = key_of(&DataType::Int, DataValue::Int(42));
    let large = key_of(&DataType::BigInt, DataValue::BigInt(42));

    assert_eq!(small, medium);
    assert_eq!(medium, large);

    // And negatives still sort below positives.
    assert!(key_of(&DataType::Int, DataValue::Int(-1)) < medium);
}

// ── Composite keys ───────────────────────────────────────────────────────────

fn composite_key(types: &[DataType], values: Vec<Option<DataValue>>) -> Option<Vec<u8>> {
    let spec = KeySpec::new(
        types
            .iter()
            .enumerate()
            .map(|(i, ty)| KeyColumn {
                left_index: i,
                right_index: i,
                class: KeyClass::of(ty),
            })
            .collect(),
    );
    spec.left_key(&values)
        .expect("encoding should succeed")
        .map(|k| k.as_bytes().to_vec())
}

/// Concatenated components must order like the tuple of components, and
/// splitting a string differently must not collide.
#[test]
fn composite_keys_are_prefix_free() {
    let types = vec![DataType::Varchar(8), DataType::Varchar(8)];

    let ab_c = composite_key(
        &types,
        vec![
            Some(DataValue::Varchar("ab".to_string())),
            Some(DataValue::Varchar("c".to_string())),
        ],
    )
    .expect("non-null key");
    let a_bc = composite_key(
        &types,
        vec![
            Some(DataValue::Varchar("a".to_string())),
            Some(DataValue::Varchar("bc".to_string())),
        ],
    )
    .expect("non-null key");

    assert_ne!(
        ab_c, a_bc,
        "(\"ab\",\"c\") must not collide with (\"a\",\"bc\")"
    );
    assert!(
        a_bc < ab_c,
        "tuple order must be lexicographic by component"
    );

    // A prefix sorts before a longer string in the same position.
    let a_empty = composite_key(
        &types,
        vec![
            Some(DataValue::Varchar("a".to_string())),
            Some(DataValue::Varchar(String::new())),
        ],
    )
    .expect("non-null key");
    assert!(a_empty < a_bc);
}

/// Strings containing NUL must still order correctly, which is what the
/// escaping is for.
#[test]
fn embedded_nul_does_not_break_ordering() {
    let types = vec![DataType::Varchar(8), DataType::Varchar(8)];

    let with_nul = composite_key(
        &types,
        vec![
            Some(DataValue::Varchar("a\0b".to_string())),
            Some(DataValue::Varchar("z".to_string())),
        ],
    )
    .expect("non-null key");
    let plain = composite_key(
        &types,
        vec![
            Some(DataValue::Varchar("a".to_string())),
            Some(DataValue::Varchar("z".to_string())),
        ],
    )
    .expect("non-null key");

    assert_ne!(with_nul, plain);
    // "a" < "a\0b" bytewise, so the same must hold for the keys.
    assert!(plain < with_nul);
}

// ── NULL ─────────────────────────────────────────────────────────────────────

/// A NULL in any component makes the whole key absent. There is no byte
/// sequence a NULL encodes to, so two NULLs can never be found equal.
#[test]
fn any_null_component_yields_no_key() {
    let types = vec![DataType::Int, DataType::Varchar(4)];

    assert!(
        composite_key(&types, vec![None, Some(DataValue::Varchar("a".into()))]).is_none(),
        "NULL in the first component must suppress the key"
    );
    assert!(
        composite_key(&types, vec![Some(DataValue::Int(1)), None]).is_none(),
        "NULL in the second component must suppress the key"
    );
    assert!(
        composite_key(&types, vec![None, None]).is_none(),
        "all-NULL must suppress the key"
    );
    assert!(
        composite_key(
            &types,
            vec![
                Some(DataValue::Int(1)),
                Some(DataValue::Varchar("a".into()))
            ]
        )
        .is_some(),
        "a fully non-NULL row must have a key"
    );
}

// ── Class resolution ─────────────────────────────────────────────────────────

/// Types that `Comparable` accepts must resolve; types it refuses must be
/// rejected at plan time rather than silently mis-joined.
#[test]
fn class_resolution_matches_what_comparable_accepts() {
    // Accepted.
    for (left, right) in [
        (DataType::Int, DataType::BigInt),
        (DataType::SmallInt, DataType::Int),
        (DataType::Char(4), DataType::Character(9)),
        (
            DataType::Numeric {
                precision: 10,
                scale: 2,
            },
            DataType::Decimal {
                precision: 18,
                scale: 2,
            },
        ),
    ] {
        assert!(
            resolve_key_class(&left, &right).is_ok(),
            "{left} and {right} should resolve to one key class"
        );
    }

    // Refused.
    for (left, right) in [
        (DataType::Int, DataType::Real),
        (DataType::Real, DataType::DoublePrecision),
        (DataType::Char(4), DataType::Varchar(4)),
        (DataType::Date, DataType::Timestamp),
        (
            DataType::Numeric {
                precision: 10,
                scale: 2,
            },
            DataType::Numeric {
                precision: 10,
                scale: 3,
            },
        ),
    ] {
        let err = resolve_key_class(&left, &right)
            .expect_err("{left} and {right} must not resolve to one key class");
        assert!(
            matches!(err, JoinError::KeyTypeMismatch { .. }),
            "expected a key type mismatch, got {err:?}"
        );
        // The message must name both sides and suggest the fix.
        let rendered = err.to_string();
        assert!(
            rendered.contains("cast"),
            "message should suggest a cast: {rendered}"
        );
    }
}

/// Every refused pair really is refused by `Comparable` too - the class table
/// must not be stricter than the engine.
#[test]
fn refused_classes_are_refused_by_comparable() {
    let cases = [
        (
            DataType::Int,
            DataValue::Int(1),
            DataType::Real,
            DataValue::Real(OrderedF32(1.0)),
        ),
        (
            DataType::Char(4),
            DataValue::Char("a".into()),
            DataType::Varchar(4),
            DataValue::Varchar("a".into()),
        ),
        (
            DataType::Real,
            DataValue::Real(OrderedF32(1.0)),
            DataType::DoublePrecision,
            DataValue::DoublePrecision(OrderedF64(1.0)),
        ),
    ];

    for (left_ty, left, right_ty, right) in cases {
        assert!(
            resolve_key_class(&left_ty, &right_ty).is_err(),
            "{left_ty} vs {right_ty} should be refused"
        );
        assert!(
            left.compare(&right).is_err(),
            "premise: Comparable refuses {left_ty} vs {right_ty} too"
        );
    }
}

/// Cross-scale NUMERIC is refused because `compare` rescales with unchecked
/// arithmetic. This records that the hazard is real.
#[test]
fn cross_scale_numeric_is_refused_because_compare_can_overflow() {
    let wide = DataValue::Numeric(NumericValue {
        unscaled: i128::MAX / 2,
        scale: 0,
    });
    let narrow = DataValue::Numeric(NumericValue {
        unscaled: 1,
        scale: 30,
    });

    assert!(
        resolve_key_class(
            &DataType::Numeric {
                precision: 38,
                scale: 0
            },
            &DataType::Numeric {
                precision: 38,
                scale: 30
            }
        )
        .is_err(),
        "cross-scale NUMERIC must be refused at plan time"
    );

    // Characterisation: comparing these directly overflows upstream. Debug
    // builds panic; release builds wrap. Either way the answer is not usable.
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| wide.compare(&narrow)));
    std::panic::set_hook(previous_hook);

    if cfg!(debug_assertions) {
        assert!(
            outcome.is_err(),
            "upstream no longer overflows on cross-scale NUMERIC comparison"
        );
    }
}

/// Key encoding must reject a value whose scale does not match its resolved
/// class instead of comparing unlike magnitudes.
#[test]
fn numeric_scale_mismatch_is_an_error_not_a_wrong_answer() {
    let spec = KeySpec::new(vec![KeyColumn {
        left_index: 0,
        right_index: 0,
        class: KeyClass::Numeric { scale: 2 },
    }]);

    let err = spec
        .left_key(&[Some(DataValue::Numeric(NumericValue {
            unscaled: 5,
            scale: 7,
        }))])
        .expect_err("a scale mismatch must be reported");
    assert!(matches!(err, JoinError::KeyEncoding(_)), "got {err:?}");
}

/// An out-of-range key column index is reported, not a panic.
#[test]
fn out_of_range_key_column_is_reported() {
    let spec = KeySpec::new(vec![KeyColumn {
        left_index: 5,
        right_index: 0,
        class: KeyClass::Integer,
    }]);

    let err = spec
        .left_key(&[Some(DataValue::Int(1))])
        .expect_err("index 5 is out of range for a 1-column row");
    assert!(matches!(err, JoinError::KeyEncoding(_)), "got {err:?}");
}
