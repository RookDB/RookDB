//! Marks the duplicate copies of `USING` / `NATURAL` join columns.

use super::super::explain::PlanNode;
use super::super::tuple::{ColumnInfo, Tuple};
use super::PhysicalOperator;
use crate::backend::error::RookResult;

/// Passes a join's rows through, relabelling its schema so a bare reference
/// to a merged column finds one column, not two. For a FULL join the one
/// column is a new last column holding `COALESCE(left, right)` (neither copy
/// alone is the SQL value); otherwise no row changes.
pub struct MergedColumnsOperator {
    child: Box<dyn PhysicalOperator>,
    schema: Vec<ColumnInfo>,
    /// For each FULL-join merged column, the positions of the copies it
    /// coalesces. Empty for the other join types.
    coalesce: Vec<Vec<usize>>,
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
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        names: &[String],
        keep_right: bool,
        full: bool,
    ) -> Self {
        let mut schema = child.schema().to_vec();
        let mut coalesce = Vec::new();
        if full {
            for name in names {
                let copies: Vec<usize> = schema
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| !c.merged && c.name.eq_ignore_ascii_case(name))
                    .map(|(i, _)| i)
                    .collect();
                if copies.len() < 2 {
                    continue;
                }
                let mut merged_col = schema[copies[0]].clone();
                merged_col.table = None;
                for &i in &copies {
                    schema[i].merged = true;
                }
                schema.push(merged_col);
                coalesce.push(copies);
            }
        } else {
            mark_merged(&mut schema, names, keep_right);
        }
        Self {
            child,
            schema,
            coalesce,
        }
    }

    fn add_coalesced(&self, tuple: &mut Tuple) {
        for copies in &self.coalesce {
            let value = copies
                .iter()
                .find_map(|&i| tuple.values.get(i).and_then(|v| v.clone()));
            tuple.values.push(value);
        }
    }
}

impl PhysicalOperator for MergedColumnsOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        let mut tuple = self.child.next()?;
        if let Some(t) = tuple.as_mut() {
            self.add_coalesced(t);
        }
        Ok(tuple)
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        let n = self.child.next_batch(batch)?;
        if !self.coalesce.is_empty() {
            for t in batch.iter_mut() {
                self.add_coalesced(t);
            }
        }
        Ok(n)
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
