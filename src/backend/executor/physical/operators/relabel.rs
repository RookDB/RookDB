//! Gives a relation read in FROM (a view) the qualifier it is read by.

use super::super::explain::PlanNode;
use super::super::tuple::{ColumnInfo, Tuple};
use super::PhysicalOperator;
use crate::backend::error::RookResult;

/// Passes its child's rows through unchanged, with every column of the
/// schema qualified by `name`: `FROM v` (or `FROM v AS w`) makes `v.a`
/// (`w.a`) read the view's column `a`, as for a table.
pub struct RelabelOperator {
    child: Box<dyn PhysicalOperator>,
    schema: Vec<ColumnInfo>,
}

impl RelabelOperator {
    pub fn new(child: Box<dyn PhysicalOperator>, name: &str) -> Self {
        let schema = child
            .schema()
            .iter()
            .map(|c| ColumnInfo {
                merged: false,
                table: Some(name.to_string()),
                ..c.clone()
            })
            .collect();
        Self { child, schema }
    }
}

impl PhysicalOperator for RelabelOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        self.child.next()
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        self.child.next_batch(batch)
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.child.reset()
    }

    fn estimate_cardinality(&self) -> usize {
        self.child.estimate_cardinality()
    }

    fn is_estimated(&self) -> bool {
        self.child.is_estimated()
    }

    fn name(&self) -> &'static str {
        self.child.name()
    }

    fn explain(&self) -> PlanNode {
        self.child.explain()
    }

    fn ordering(&self) -> Option<Vec<(usize, bool)>> {
        self.child.ordering()
    }
}
