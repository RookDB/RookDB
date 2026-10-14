//! Marks the duplicate copies of `USING` / `NATURAL` join columns.

use super::super::explain::PlanNode;
use super::super::tuple::{ColumnInfo, Tuple};
use super::PhysicalOperator;
use crate::backend::error::RookResult;

/// Passes a join's rows through unchanged, relabelling its schema so a bare
/// reference to a merged column finds one column, not two. No row is copied.
pub struct MergedColumnsOperator {
    child: Box<dyn PhysicalOperator>,
    schema: Vec<ColumnInfo>,
}

/// Mark the duplicate copies of the shared columns `names` in a join's
/// combined schema: the copy from the join's right side, or the left ones
/// for a RIGHT join (`keep_right`), whose right side always has a value.
pub fn mark_merged(schema: &mut [ColumnInfo], names: &[String], keep_right: bool) {
    for name in names {
        let mut copies: Vec<usize> = schema
            .iter()
            .enumerate()
            .filter(|(_, c)| !c.merged && c.name.eq_ignore_ascii_case(name))
            .map(|(i, _)| i)
            .collect();
        let Some(last) = copies.pop() else { continue };
        if copies.is_empty() {
            continue;
        }
        if keep_right {
            for i in copies {
                schema[i].merged = true;
            }
        } else {
            schema[last].merged = true;
        }
    }
}

impl MergedColumnsOperator {
    pub fn new(child: Box<dyn PhysicalOperator>, names: &[String], keep_right: bool) -> Self {
        let mut schema = child.schema().to_vec();
        mark_merged(&mut schema, names, keep_right);
        Self { child, schema }
    }
}

impl PhysicalOperator for MergedColumnsOperator {
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
