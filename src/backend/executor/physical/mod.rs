//! Volcano-style physical execution engine.
//!
//! Converts a `LogicalPlan` into a tree of `PhysicalOperator` nodes and
//! drives them in a pull-based (Volcano) model. Each operator implements
//! `next() -> Option<Result<Tuple>>` and the root of the tree produces
//! the final result set as an iterator of `Tuple` values.
//!
//! # Architecture
//!
//! ```text
//!            ┌──────────────┐
//!            │  Display /   │
//!            │  CLI output  │
//!            └──────┬───────┘
//!                   │ next()
//!            ┌──────▼───────┐
//!            │  Sort /      │
//!            │  Distinct /  │
//!            │  Limit /     │
//!            │  Project     │
//!            └──────┬───────┘
//!                   │ next()
//!            ┌──────▼───────┐
//!            │   Filter     │
//!            └──────┬───────┘
//!                   │ next()
//!            ┌──────▼───────┐
//!            │   SeqScan    │
//!            │  (heap file) │
//!            └──────────────┘
//! ```

pub mod engine;
pub mod expr;
pub mod external_sort;
pub mod operators;
pub mod planner;
pub mod tuple;

pub use engine::{execute_plan, execute_plan_collect};
pub use expr::{ComparisonOp, Expr, Predicate, evaluate_predicate};
pub use external_sort::SortSpill;
pub use operators::{
    AggregateFunction, AggregateInfo, AggregateOperator, CteScanOperator, DistinctOperator,
    FilterOperator, HashJoinOperator, IndexNestedLoopJoinOperator, IndexScanMode,
    IndexScanOperator, JoinType, LimitOperator, NestedLoopJoinOperator, NullOperator,
    PhysicalOperator, ProjectionOperator, SeqScanOperator, SetOpOperator, SetOpType,
    SingleRowOperator, SortOperator, SubqueryExecOperator, SubqueryType,
    infer_aggregate_output_type,
};
pub use planner::PhysicalPlanner;
pub use tuple::{ColumnInfo, Tuple, display_tuples, format_tuple};
