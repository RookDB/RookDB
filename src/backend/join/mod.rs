//! Join module — provides join algorithms (NLJ, SMJ, HJ),
//! cost-based optimizer, and shared infrastructure.

pub mod tuple;
pub mod condition;
pub mod scanner;
pub mod result;
pub mod nlj;
pub mod smj;
pub mod hj;
pub mod shj;
pub mod direct;
pub mod cost_model;
pub mod planner;
pub mod bloom_filter;
pub mod algorithm;
pub mod join_order;

pub use tuple::{ColumnValue, Tuple, deserialize_tuple};
pub use condition::{JoinCondition, JoinOp, JoinPredicate, evaluate_conditions};
pub use scanner::TupleScanner;
pub use result::JoinResult;
pub use nlj::NLJExecutor;
pub use smj::SMJExecutor;
pub use hj::{HashJoinExecutor, HashJoinMode};
pub use shj::SymmetricHashJoinExecutor;
pub use direct::DirectJoinExecutor;
pub use cost_model::{CostModel, JoinCost, CardinalityEstimator, SkewEstimate};
pub use planner::{JoinPlanner, JoinPlannerConfig, PhysicalPlan};
pub use bloom_filter::BloomFilter;
pub use algorithm::{JoinAlgorithm, AlgorithmMetadata, JoinExecutionStats};
pub use join_order::{MultiJoinOptimizer, JoinTreeNode, RelationSet};

use std::fmt;

/// Logical join type (e.g. INNER, LEFT OUTER, SEMI).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
    SemiJoin,
    AntiJoin,
    Natural,
    Lateral,
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Inner      => write!(f, "INNER"),
            JoinType::LeftOuter  => write!(f, "LEFT OUTER"),
            JoinType::RightOuter => write!(f, "RIGHT OUTER"),
            JoinType::FullOuter  => write!(f, "FULL OUTER"),
            JoinType::Cross      => write!(f, "CROSS"),
            JoinType::SemiJoin   => write!(f, "SEMI"),
            JoinType::AntiJoin   => write!(f, "ANTI"),
            JoinType::Natural    => write!(f, "NATURAL"),
            JoinType::Lateral    => write!(f, "LATERAL"),
        }
    }
}

/// Physical join algorithm identifier used by the cost-based optimizer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JoinAlgorithmType {
    SimpleNLJ,
    BlockNLJ,
    IndexedNLJ,
    SortMergeJoin,
    InMemoryHashJoin,
    GraceHashJoin,
    HybridHashJoin,
    SymmetricHashJoin,
    BroadcastHashJoin,
    ShuffleSortMergeJoin,
    AdaptiveJoin,
    DirectJoin,
}

impl fmt::Display for JoinAlgorithmType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinAlgorithmType::SimpleNLJ           => write!(f, "Simple Nested Loop Join"),
            JoinAlgorithmType::BlockNLJ            => write!(f, "Block Nested Loop Join"),
            JoinAlgorithmType::IndexedNLJ          => write!(f, "Indexed Nested Loop Join"),
            JoinAlgorithmType::SortMergeJoin       => write!(f, "Sort-Merge Join"),
            JoinAlgorithmType::InMemoryHashJoin    => write!(f, "In-Memory Hash Join"),
            JoinAlgorithmType::GraceHashJoin       => write!(f, "Grace Hash Join"),
            JoinAlgorithmType::HybridHashJoin      => write!(f, "Hybrid Hash Join"),
            JoinAlgorithmType::SymmetricHashJoin   => write!(f, "Symmetric Hash Join"),
            JoinAlgorithmType::BroadcastHashJoin   => write!(f, "Broadcast Hash Join"),
            JoinAlgorithmType::ShuffleSortMergeJoin => write!(f, "Shuffle Sort-Merge Join"),
            JoinAlgorithmType::AdaptiveJoin        => write!(f, "Adaptive Join"),
            JoinAlgorithmType::DirectJoin          => write!(f, "Direct Join"),
        }
    }
}

/// Nested-loop join execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NLJMode {
    Simple,
    Block,
    Indexed,
}
