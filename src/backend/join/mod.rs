//! Join subsystem.
//!
//! This module documents *what* each piece does. The reasoning behind the
//! design - why keys are a single order-preserving byte encoding, why NULL
//! join keys are unrepresentable rather than merely handled, why spilling
//! uses length-framed run files, and what the cost model does and does not
//! know - lives in `docs/join/design-rationale.md`.

// User input reaches this subsystem through the CLI, so a panic here is a
// crash of the whole engine. Every fallible path returns `JoinError`.
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

pub mod algorithm;
pub mod error;
pub mod key;
pub mod predicate;
pub mod row;
pub mod schema;

pub use algorithm::{
    ALGORITHMS, AlgorithmSpec, JoinAlgorithm, JoinRequest, JoinType, Pushdown, ValidatedJoinSpec,
    pushdown_plan, spec_for,
};
pub use error::JoinError;
pub use key::{JoinKey, KeyClass, KeyColumn, KeySpec, resolve_key_class};
pub use predicate::{ColumnBinding, JoinPredicate, PredicateSplit, SideResolver, split_conjuncts};
pub use row::{RowBuilder, RowCodec};
pub use schema::{OutputColumn, OutputSchema, RelationSchema, RelationSide};
