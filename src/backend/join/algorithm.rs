//! Join types and the algorithm capability matrix.
//!
//! The matrix lives in one place, and executors can only be built from a
//! `ValidatedJoinSpec` - so an algorithm cannot run a join type it does not
//! implement.

use super::error::JoinError;
use super::key::KeySpec;

// ── Join types ───────────────────────────────────────────────────────────────

/// The join types the subsystem executes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
    /// Left rows that have at least one match. Emits left columns only.
    Semi,
    /// Left rows that have no match. Emits left columns only.
    Anti,
}

impl JoinType {
    pub const ALL: [JoinType; 7] = [
        JoinType::Inner,
        JoinType::LeftOuter,
        JoinType::RightOuter,
        JoinType::FullOuter,
        JoinType::Cross,
        JoinType::Semi,
        JoinType::Anti,
    ];

    pub fn name(self) -> &'static str {
        match self {
            JoinType::Inner => "Inner",
            JoinType::LeftOuter => "Left Outer",
            JoinType::RightOuter => "Right Outer",
            JoinType::FullOuter => "Full Outer",
            JoinType::Cross => "Cross",
            JoinType::Semi => "Semi",
            JoinType::Anti => "Anti",
        }
    }

    /// Whether unmatched left rows are emitted, NULL-extended on the right.
    pub fn keeps_unmatched_left(self) -> bool {
        matches!(self, JoinType::LeftOuter | JoinType::FullOuter)
    }

    /// Whether unmatched right rows are emitted, NULL-extended on the left.
    pub fn keeps_unmatched_right(self) -> bool {
        matches!(self, JoinType::RightOuter | JoinType::FullOuter)
    }

    /// SEMI and ANTI project the left relation only, so their output schema
    /// has no right-side columns at all.
    pub fn emits_left_only(self) -> bool {
        matches!(self, JoinType::Semi | JoinType::Anti)
    }
}

/// Whether single-relation conjuncts may be pushed into each side's scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pushdown {
    pub left: bool,
    pub right: bool,
}

/// A conjunct touching only the row-preserving side of an outer join must stay
/// in the join condition. Pushing it into that side's scan would drop rows the
/// join is required to emit NULL-extended, which silently changes the answer.
pub fn pushdown_plan(join_type: JoinType) -> Pushdown {
    match join_type {
        JoinType::Inner | JoinType::Cross | JoinType::Semi | JoinType::Anti => Pushdown {
            left: true,
            right: true,
        },
        JoinType::LeftOuter => Pushdown {
            left: false,
            right: true,
        },
        JoinType::RightOuter => Pushdown {
            left: true,
            right: false,
        },
        JoinType::FullOuter => Pushdown {
            left: false,
            right: false,
        },
    }
}

// ── Algorithms ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinAlgorithm {
    SimpleNestedLoop,
    BlockNestedLoop,
    IndexNestedLoop,
    SortMerge,
    /// One operator covering in-memory, hybrid and Grace partitioning; which
    /// it uses is a runtime decision, not a separate algorithm.
    Hash,
    SymmetricHash,
    /// Starts as a hash join and degrades under memory pressure, ending at
    /// block nested loop for a single oversized key group.
    Adaptive,
}

impl JoinAlgorithm {
    pub fn name(self) -> &'static str {
        match self {
            JoinAlgorithm::SimpleNestedLoop => "Simple Nested Loop",
            JoinAlgorithm::BlockNestedLoop => "Block Nested Loop",
            JoinAlgorithm::IndexNestedLoop => "Index Nested Loop",
            JoinAlgorithm::SortMerge => "Sort Merge",
            JoinAlgorithm::Hash => "Hash",
            JoinAlgorithm::SymmetricHash => "Symmetric Hash",
            JoinAlgorithm::Adaptive => "Adaptive",
        }
    }
}

/// What one algorithm can do.
#[derive(Debug, Clone, Copy)]
pub struct AlgorithmSpec {
    pub algorithm: JoinAlgorithm,
    /// Cannot run without at least one equijoin key component.
    pub requires_equi_keys: bool,
    /// Cannot run without a usable index on the inner relation.
    pub requires_inner_index: bool,
    pub supported: &'static [JoinType],
    /// Holds both inputs in memory at once and cannot spill, so the planner
    /// must not offer it when they will not fit.
    pub holds_both_inputs: bool,
}

const NESTED_LOOP_TYPES: &[JoinType] = &JoinType::ALL;

/// Every join type except CROSS, which needs no keys and so is nested-loop
/// only.
const EQUI_TYPES: &[JoinType] = &[
    JoinType::Inner,
    JoinType::LeftOuter,
    JoinType::RightOuter,
    JoinType::FullOuter,
    JoinType::Semi,
    JoinType::Anti,
];

/// Index nested loop drives from the outer side and probes the inner.
const INDEX_NESTED_LOOP_TYPES: &[JoinType] = &[
    JoinType::Inner,
    JoinType::LeftOuter,
    JoinType::Semi,
    JoinType::Anti,
];

pub const ALGORITHMS: [AlgorithmSpec; 7] = [
    AlgorithmSpec {
        algorithm: JoinAlgorithm::SimpleNestedLoop,
        requires_equi_keys: false,
        requires_inner_index: false,
        supported: NESTED_LOOP_TYPES,
        holds_both_inputs: false,
    },
    AlgorithmSpec {
        algorithm: JoinAlgorithm::BlockNestedLoop,
        requires_equi_keys: false,
        requires_inner_index: false,
        supported: NESTED_LOOP_TYPES,
        holds_both_inputs: false,
    },
    AlgorithmSpec {
        algorithm: JoinAlgorithm::IndexNestedLoop,
        requires_equi_keys: true,
        requires_inner_index: true,
        supported: INDEX_NESTED_LOOP_TYPES,
        holds_both_inputs: false,
    },
    AlgorithmSpec {
        algorithm: JoinAlgorithm::SortMerge,
        requires_equi_keys: true,
        requires_inner_index: false,
        supported: EQUI_TYPES,
        holds_both_inputs: false,
    },
    AlgorithmSpec {
        algorithm: JoinAlgorithm::Hash,
        requires_equi_keys: true,
        requires_inner_index: false,
        supported: EQUI_TYPES,
        holds_both_inputs: false,
    },
    AlgorithmSpec {
        algorithm: JoinAlgorithm::SymmetricHash,
        requires_equi_keys: true,
        requires_inner_index: false,
        supported: EQUI_TYPES,
        holds_both_inputs: true,
    },
    AlgorithmSpec {
        algorithm: JoinAlgorithm::Adaptive,
        requires_equi_keys: false,
        requires_inner_index: false,
        supported: NESTED_LOOP_TYPES,
        holds_both_inputs: false,
    },
];

pub fn spec_for(algorithm: JoinAlgorithm) -> &'static AlgorithmSpec {
    // `ALGORITHMS` covers every variant, so the search always succeeds; the
    // fallback keeps this total without a panic.
    ALGORITHMS
        .iter()
        .find(|spec| spec.algorithm == algorithm)
        .unwrap_or(&ALGORITHMS[0])
}

/// What the planner is asking an algorithm to do.
#[derive(Debug, Clone, Copy)]
pub struct JoinRequest<'a> {
    pub join_type: JoinType,
    pub keys: &'a KeySpec,
    /// Whether a residual predicate remains after key extraction.
    pub has_residual: bool,
    /// Whether a usable index exists on the inner relation.
    pub has_inner_index: bool,
}

/// Proof that an (algorithm, join type, keys) combination was checked.
///
/// Fields are private and there is no public constructor, so the only way to
/// hold one is to have passed [`AlgorithmSpec::validate`].
#[derive(Debug, Clone)]
pub struct ValidatedJoinSpec {
    algorithm: JoinAlgorithm,
    join_type: JoinType,
    keys: KeySpec,
    has_residual: bool,
}

impl ValidatedJoinSpec {
    pub fn algorithm(&self) -> JoinAlgorithm {
        self.algorithm
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn keys(&self) -> &KeySpec {
        &self.keys
    }

    pub fn has_residual(&self) -> bool {
        self.has_residual
    }
}

impl AlgorithmSpec {
    /// Whether this algorithm could serve the request at all.
    pub fn accepts(&self, request: &JoinRequest) -> bool {
        self.validate(request).is_ok()
    }

    /// Check the request against this algorithm's capabilities.
    pub fn validate(&self, request: &JoinRequest) -> Result<ValidatedJoinSpec, JoinError> {
        let name = self.algorithm.name();

        if !self.supported.contains(&request.join_type) {
            return Err(JoinError::plan(format!(
                "{name} join does not support a {} join",
                request.join_type.name()
            )));
        }

        if self.requires_equi_keys && request.keys.is_empty() {
            return Err(JoinError::plan(format!(
                "{name} join needs at least one equality between the two relations"
            )));
        }

        if self.requires_inner_index && !request.has_inner_index {
            return Err(JoinError::plan(format!(
                "{name} join needs an index on the inner relation's key columns"
            )));
        }

        // CROSS is the absence of a condition. A CROSS carrying one is a
        // request for an INNER join and should say so, rather than having the
        // condition silently dropped or silently applied.
        if request.join_type == JoinType::Cross
            && (!request.keys.is_empty() || request.has_residual)
        {
            return Err(JoinError::plan(
                "a CROSS join cannot carry a join condition; use INNER JOIN ... ON instead"
                    .to_string(),
            ));
        }

        Ok(ValidatedJoinSpec {
            algorithm: self.algorithm,
            join_type: request.join_type,
            keys: request.keys.clone(),
            has_residual: request.has_residual,
        })
    }
}
