// JOIN module: provides join algorithms (NLJ, SMJ, HJ) and shared infrastructure.
pub mod tuple;
pub mod condition;
pub mod scanner;
pub mod result;
pub mod metrics;
pub mod nlj;
pub mod smj;
pub mod hj;

pub use tuple::{ColumnValue, Tuple, deserialize_tuple};
pub use condition::{JoinCondition, JoinOp};
pub use scanner::TupleScanner;
pub use result::JoinResult;
pub use metrics::JoinMetrics;
pub use nlj::NLJExecutor;
pub use smj::SMJExecutor;
pub use hj::HashJoinExecutor;

/// Type of join operation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
}

/// NLJ execution mode.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NLJMode {
    Simple,
    Block,
}