//! An operator with the planner's row estimate attached.

use super::super::explain::PlanNode;
use super::super::tuple::{ColumnInfo, Tuple};
use super::trait_::PhysicalOperator;
use crate::backend::error::RookResult;

/// Wraps the operator the planner built for one node of the plan and
/// carries the estimate the shared estimator made for it: what
/// `estimate_cardinality` reports and `EXPLAIN` shows as `est_rows`. Every
/// other call goes straight to the operator.
pub struct Estimated {
    inner: Box<dyn PhysicalOperator>,
    rows: usize,
}

impl Estimated {
    pub fn new(inner: Box<dyn PhysicalOperator>, rows: f64) -> Self {
        Estimated {
            inner,
            rows: rows.round().clamp(0.0, usize::MAX as f64) as usize,
        }
    }
}

impl PhysicalOperator for Estimated {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        self.inner.next()
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        self.inner.next_batch(batch)
    }

    fn schema(&self) -> &[ColumnInfo] {
        self.inner.schema()
    }

    fn reset(&mut self) -> RookResult<()> {
        self.inner.reset()
    }

    fn estimate_cardinality(&self) -> usize {
        self.rows
    }

    fn is_estimated(&self) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn explain(&self) -> PlanNode {
        self.inner.explain().with("est_rows", self.rows)
    }

    fn ordering(&self) -> Option<Vec<(usize, bool)>> {
        self.inner.ordering()
    }
}
