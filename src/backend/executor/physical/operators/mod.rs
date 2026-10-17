//! PhysicalOperator trait and all concrete operator implementations.
//!
//! Each operator implements the Volcano pull-based `next()` protocol and
//! wraps zero or more child operators. Operators form a tree where the root
//! produces the final result set.
//!
//! This module is split into per-category files for maintainability:
//! - `trait_.rs`: PhysicalOperator trait definition
//! - `scans.rs`: SeqScanOperator, IndexScanOperator, IndexScanMode
//! - `joins.rs`: NestedLoopJoinOperator, HashJoinOperator, JoinType
//! - `filter_project.rs`: Filter, Projection, Limit, Distinct, SingleRow,
//!   Null and CteScan operators
//! - `estimated.rs`: attaches the planner's row estimate to an operator
//! - `merged.rs`: relabels the shared columns of USING / NATURAL joins
//! - `sort.rs`: SortOperator
//! - `aggregate.rs`: AggregateOperator with GROUP BY / HAVING support
//! - `set_op.rs`: `UNION [ALL] / INTERSECT [ALL] / EXCEPT [ALL]`
//! - `subquery.rs`: scalar and EXISTS subquery execution
//! - `insert.rs`: INSERT INTO ... SELECT pipeline operator
//! - `utils.rs`: canonical hash keys
//! - `tests.rs`: unit tests (mock operator + operator behaviour)

mod aggregate;
mod correlated_lookup;
mod estimated;
mod filter_project;
mod insert;
mod joins;
mod merged;
mod scans;
mod set_op;
mod sort;
mod subquery;
mod trait_;
mod utils;

#[cfg(test)]
mod tests;

// Re-export everything from submodules
pub use correlated_lookup::CorrelatedLookupOperator;
pub use estimated::Estimated;
pub use insert::{InsertOperator, ValuesOperator};
pub use joins::{HashJoinOperator, IndexNestedLoopJoinOperator, JoinType, NestedLoopJoinOperator};
pub use joins::{
    SPILL_BUDGET_BYTES as HASH_JOIN_SPILL_BUDGET_BYTES, SPILL_COUNT as HASH_JOIN_SPILL_COUNT,
};
pub use scans::{IndexScanMode, IndexScanOperator, SeqScanOperator};
pub use trait_::{DEFAULT_BATCH_SIZE, PhysicalOperator};

pub use aggregate::{
    AggregateFunction, AggregateInfo, AggregateOperator, PerGroupState, infer_aggregate_output_type,
};
pub use filter_project::{
    CteScanOperator, DistinctOperator, FilterOperator, LimitOperator, NullOperator,
    ProjectionOperator, SingleRowOperator,
};
pub use merged::{MergedColumnsOperator, mark_merged};
pub use set_op::{SetOpOperator, SetOpType};
pub use sort::SortOperator;
pub(crate) use sort::compare_rows;
pub use subquery::{SubqueryExecOperator, SubqueryType};
pub(crate) use utils::{KeyClass, canonical_key};
