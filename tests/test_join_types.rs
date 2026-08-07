//! The algorithm capability matrix.
//!
//! The previous implementation accepted SEMI, ANTI and NATURAL joins into its
//! hash, sort-merge and symmetric-hash executors and silently computed an
//! INNER join instead. Here, an algorithm that does not implement a join type
//! cannot be constructed for it: `ValidatedJoinSpec` has private fields and
//! only `AlgorithmSpec::validate` produces one.
//!
//! This file checks the matrix is self-consistent and that every rejection is
//! an explained error. That each *supported* combination computes the right
//! answer is checked against the reference join in the execution tests.

#[path = "join_common/mod.rs"]
mod common;

use storage_manager::join::{
    ALGORITHMS, JoinAlgorithm, JoinError, JoinRequest, JoinType, KeyClass, KeyColumn, KeySpec,
    spec_for,
};

fn no_keys() -> KeySpec {
    KeySpec::default()
}

fn one_key() -> KeySpec {
    KeySpec::new(vec![KeyColumn {
        left_index: 0,
        right_index: 0,
        class: KeyClass::Integer,
    }])
}

// ── Matrix consistency ───────────────────────────────────────────────────────

/// Every algorithm appears exactly once, and `spec_for` finds the right entry.
#[test]
fn the_matrix_lists_every_algorithm_once() {
    let algorithms = [
        JoinAlgorithm::SimpleNestedLoop,
        JoinAlgorithm::BlockNestedLoop,
        JoinAlgorithm::IndexNestedLoop,
        JoinAlgorithm::SortMerge,
        JoinAlgorithm::Hash,
        JoinAlgorithm::SymmetricHash,
        JoinAlgorithm::Adaptive,
    ];

    assert_eq!(ALGORITHMS.len(), algorithms.len());

    for algorithm in algorithms {
        let matches = ALGORITHMS
            .iter()
            .filter(|spec| spec.algorithm == algorithm)
            .count();
        assert_eq!(matches, 1, "{algorithm:?} must appear exactly once");
        assert_eq!(spec_for(algorithm).algorithm, algorithm);
    }
}

/// Walk the entire algorithm × join-type space. Each combination must either
/// validate or be refused with a reason, and the outcome must agree with the
/// declared `supported` list.
#[test]
fn every_algorithm_and_join_type_combination_is_decided() {
    for spec in &ALGORITHMS {
        for join_type in JoinType::ALL {
            // Give the request everything it could need, so the only reason
            // for refusal is the join type itself.
            let keys = if join_type == JoinType::Cross {
                no_keys()
            } else {
                one_key()
            };
            let request = JoinRequest {
                join_type,
                keys: &keys,
                has_residual: false,
                has_inner_index: true,
            };

            let declared = spec.supported.contains(&join_type);
            // CROSS needs no keys, so an algorithm requiring them cannot serve
            // it even if the matrix lists it.
            let expected = declared && !(join_type == JoinType::Cross && spec.requires_equi_keys);

            match spec.validate(&request) {
                Ok(validated) => {
                    assert!(
                        expected,
                        "{:?} accepted {join_type:?} but the matrix does not list it",
                        spec.algorithm
                    );
                    assert_eq!(validated.algorithm(), spec.algorithm);
                    assert_eq!(validated.join_type(), join_type);
                }
                Err(err) => {
                    assert!(
                        !expected,
                        "{:?} refused {join_type:?} though the matrix lists it: {err}",
                        spec.algorithm
                    );
                    assert!(matches!(err, JoinError::Plan(_)), "got {err:?}");
                    assert!(!err.to_string().is_empty(), "a refusal must explain itself");
                }
            }
        }
    }
}

/// Nested-loop variants and the adaptive operator handle every join type;
/// key-based algorithms handle everything except CROSS.
#[test]
fn coverage_matches_the_documented_capabilities() {
    let supports = |algorithm: JoinAlgorithm, join_type: JoinType| {
        spec_for(algorithm).supported.contains(&join_type)
    };

    for join_type in JoinType::ALL {
        for algorithm in [
            JoinAlgorithm::SimpleNestedLoop,
            JoinAlgorithm::BlockNestedLoop,
            JoinAlgorithm::Adaptive,
        ] {
            assert!(
                supports(algorithm, join_type),
                "{algorithm:?} must cover every join type, including {join_type:?}"
            );
        }
    }

    for algorithm in [
        JoinAlgorithm::SortMerge,
        JoinAlgorithm::Hash,
        JoinAlgorithm::SymmetricHash,
    ] {
        assert!(
            !supports(algorithm, JoinType::Cross),
            "{algorithm:?} needs keys, so it cannot serve CROSS"
        );
        for join_type in [
            JoinType::Inner,
            JoinType::LeftOuter,
            JoinType::RightOuter,
            JoinType::FullOuter,
            JoinType::Semi,
            JoinType::Anti,
        ] {
            assert!(
                supports(algorithm, join_type),
                "{algorithm:?} / {join_type:?}"
            );
        }
    }

    // Index nested loop drives from the outer side, so it cannot enumerate
    // unmatched inner rows.
    assert!(!supports(
        JoinAlgorithm::IndexNestedLoop,
        JoinType::RightOuter
    ));
    assert!(!supports(
        JoinAlgorithm::IndexNestedLoop,
        JoinType::FullOuter
    ));
    assert!(!supports(JoinAlgorithm::IndexNestedLoop, JoinType::Cross));
    for join_type in [
        JoinType::Inner,
        JoinType::LeftOuter,
        JoinType::Semi,
        JoinType::Anti,
    ] {
        assert!(supports(JoinAlgorithm::IndexNestedLoop, join_type));
    }
}

// ── Individual refusals ──────────────────────────────────────────────────────

#[test]
fn key_based_algorithms_are_refused_without_keys() {
    let keys = no_keys();
    for algorithm in [
        JoinAlgorithm::Hash,
        JoinAlgorithm::SortMerge,
        JoinAlgorithm::SymmetricHash,
        JoinAlgorithm::IndexNestedLoop,
    ] {
        let request = JoinRequest {
            join_type: JoinType::Inner,
            keys: &keys,
            has_residual: true,
            has_inner_index: true,
        };
        let err = spec_for(algorithm)
            .validate(&request)
            .expect_err("no equality means no key-based join");
        assert!(err.to_string().contains("equality"), "{algorithm:?}: {err}");
    }
}

/// A non-equi join must fall to a nested-loop variant, never to a key-based
/// algorithm that would quietly key on the wrong thing.
#[test]
fn a_non_equi_join_is_only_accepted_by_nested_loop_variants() {
    let keys = no_keys();
    let request = JoinRequest {
        join_type: JoinType::Inner,
        keys: &keys,
        has_residual: true,
        has_inner_index: true,
    };

    let accepting: Vec<JoinAlgorithm> = ALGORITHMS
        .iter()
        .filter(|spec| spec.accepts(&request))
        .map(|spec| spec.algorithm)
        .collect();

    assert_eq!(
        accepting,
        vec![
            JoinAlgorithm::SimpleNestedLoop,
            JoinAlgorithm::BlockNestedLoop,
            JoinAlgorithm::Adaptive,
        ]
    );
}

#[test]
fn index_nested_loop_is_refused_without_an_index() {
    let keys = one_key();
    let request = JoinRequest {
        join_type: JoinType::Inner,
        keys: &keys,
        has_residual: false,
        has_inner_index: false,
    };

    let err = spec_for(JoinAlgorithm::IndexNestedLoop)
        .validate(&request)
        .expect_err("no index means no index join");
    assert!(err.to_string().contains("index"), "{err}");

    // Every other algorithm is unaffected by the absence of an index.
    for algorithm in [JoinAlgorithm::Hash, JoinAlgorithm::SortMerge] {
        assert!(spec_for(algorithm).accepts(&request));
    }
}

/// A CROSS join carrying a condition is a mis-stated INNER join. Dropping the
/// condition or applying it would both be wrong, so it is refused.
#[test]
fn cross_join_cannot_carry_a_condition() {
    let with_keys = one_key();
    let without_keys = no_keys();

    let err = spec_for(JoinAlgorithm::BlockNestedLoop)
        .validate(&JoinRequest {
            join_type: JoinType::Cross,
            keys: &with_keys,
            has_residual: false,
            has_inner_index: false,
        })
        .expect_err("CROSS with keys must be refused");
    assert!(err.to_string().contains("INNER"), "{err}");

    let err = spec_for(JoinAlgorithm::BlockNestedLoop)
        .validate(&JoinRequest {
            join_type: JoinType::Cross,
            keys: &without_keys,
            has_residual: true,
            has_inner_index: false,
        })
        .expect_err("CROSS with a residual must be refused");
    assert!(err.to_string().contains("INNER"), "{err}");

    // A bare CROSS is fine.
    assert!(
        spec_for(JoinAlgorithm::BlockNestedLoop)
            .validate(&JoinRequest {
                join_type: JoinType::Cross,
                keys: &without_keys,
                has_residual: false,
                has_inner_index: false,
            })
            .is_ok()
    );
}

/// The validated token carries exactly what was checked, so an executor never
/// has to re-derive it.
#[test]
fn the_validated_token_reports_what_was_checked() {
    let keys = one_key();
    let validated = spec_for(JoinAlgorithm::Hash)
        .validate(&JoinRequest {
            join_type: JoinType::LeftOuter,
            keys: &keys,
            has_residual: true,
            has_inner_index: false,
        })
        .expect("hash join supports LEFT OUTER with keys");

    assert_eq!(validated.algorithm(), JoinAlgorithm::Hash);
    assert_eq!(validated.join_type(), JoinType::LeftOuter);
    assert_eq!(validated.keys(), &keys);
    assert!(validated.has_residual());
}

// ── Join type properties ─────────────────────────────────────────────────────

#[test]
fn join_type_row_preservation_is_declared_correctly() {
    assert!(JoinType::LeftOuter.keeps_unmatched_left());
    assert!(!JoinType::LeftOuter.keeps_unmatched_right());

    assert!(!JoinType::RightOuter.keeps_unmatched_left());
    assert!(JoinType::RightOuter.keeps_unmatched_right());

    assert!(JoinType::FullOuter.keeps_unmatched_left());
    assert!(JoinType::FullOuter.keeps_unmatched_right());

    for join_type in [JoinType::Inner, JoinType::Cross, JoinType::Semi] {
        assert!(!join_type.keeps_unmatched_left(), "{join_type:?}");
        assert!(!join_type.keeps_unmatched_right(), "{join_type:?}");
    }

    // ANTI emits left rows that did *not* match, which is not the same as
    // NULL-extending them, so it preserves neither side in that sense.
    assert!(!JoinType::Anti.keeps_unmatched_right());
}

/// SEMI and ANTI project the left relation only. Their output schema having no
/// right columns is what stops an executor emitting a concatenated row.
#[test]
fn semi_and_anti_emit_left_columns_only() {
    assert!(JoinType::Semi.emits_left_only());
    assert!(JoinType::Anti.emits_left_only());

    for join_type in [
        JoinType::Inner,
        JoinType::LeftOuter,
        JoinType::RightOuter,
        JoinType::FullOuter,
        JoinType::Cross,
    ] {
        assert!(!join_type.emits_left_only(), "{join_type:?}");
    }
}
