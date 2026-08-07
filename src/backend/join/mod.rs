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
pub mod config;
pub mod cost;
pub mod error;
pub mod exec;
pub mod index;
pub mod key;
pub mod memory;
pub mod order;
pub mod plan;
pub mod predicate;
pub mod row;
pub mod schema;
pub mod sort;
pub mod source;
pub mod spill;
pub mod stats;

pub use algorithm::{
    ALGORITHMS, AlgorithmSpec, JoinAlgorithm, JoinRequest, JoinType, Pushdown, ValidatedJoinSpec,
    pushdown_plan, spec_for,
};
pub use config::JoinConfig;
pub use cost::{CostCoefficients, CostModel, JoinCost, JoinEstimate, SideEstimate};
pub use error::JoinError;
pub use exec::{ExecStats, MatchEvaluator, RowStream};
pub use index::{IndexKeySpec, JoinIndex, RowLocator, SortedKeyIndex};
pub use key::{JoinKey, KeyClass, KeyColumn, KeySpec, resolve_key_class};
pub use memory::{MemoryAccountant, OverBudget};
pub use order::{JoinEdge, JoinGraph, OrderedPlan, optimize};
pub use plan::{JoinBuilder, PhysicalPlan, PlanSide};
pub use predicate::{ColumnBinding, JoinPredicate, PredicateSplit, SideResolver, split_conjuncts};
pub use row::{RowBuilder, RowCodec};
pub use schema::{OutputColumn, OutputSchema, RelationSchema, RelationSide};
pub use sort::{KeySide, SortOutput, SortStats, SortedRows, sort_rows};
pub use source::{RowSource, TableRef, TableSource};
pub use spill::{RowBuffer, RowBufferBuilder, RunHandle, RunReader, RunWriter, SpillScope};
pub use stats::{
    ColumnStats, StatsConfidence, TableStats, TableStatsCache, ValidityStamp, analyze_table,
    load_stats, save_stats,
};
