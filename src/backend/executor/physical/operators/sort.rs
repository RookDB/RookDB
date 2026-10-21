use std::cmp::Ordering;

use super::super::explain::PlanNode;
use super::super::external_sort::{ExternalSorter, Merge, SortSpill, tuple_bytes};
use super::super::tuple::{ColumnInfo, Tuple};
use super::trait_::PhysicalOperator;
use crate::backend::error::RookResult;

use crate::types::DataValue;
use crate::types::comparison::compare_nullable;

// ── Sort Operator ─────────────────────────────────────────────────────────────

/// Sorts all tuples from the child by the specified columns.
///
/// Sorts in memory. Given a [`SortSpill`] it holds no more rows than the
/// memory budget allows: beyond that they go to disk as sorted runs, which
/// are merged to produce the output (see `external_sort`). Both ways give
/// the same rows in the same order.
pub struct SortOperator {
    child: Box<dyn PhysicalOperator>,
    /// Sort columns: (column_index, descending).
    sort_keys: Vec<(usize, bool)>,
    /// When and where to spill; `None` sorts in memory whatever the size.
    spill: Option<SortSpill>,
    /// Whether the child has been fully consumed.
    loaded: bool,
    /// Sorted buffer of tuples (when the input stayed in memory).
    buffer: Vec<Tuple>,
    /// Current position in buffer.
    pos: usize,
    /// Merge of the sorted runs (when the input was spilled).
    merge: Option<Merge>,
    /// Cached schema.
    cached_schema: Vec<ColumnInfo>,
    /// Only the first `limit` rows of the sorted output are needed (top-N).
    limit: Option<usize>,
    /// How the input was sorted, once it has been.
    outcome: Option<&'static str>,
}

impl SortOperator {
    /// Create a new in-memory-only sort operator.
    pub fn new(child: Box<dyn PhysicalOperator>, sort_keys: Vec<(usize, bool)>) -> Self {
        let cached_schema = child.schema().to_vec();
        Self {
            child,
            sort_keys,
            spill: None,
            loaded: false,
            buffer: Vec::new(),
            pos: 0,
            merge: None,
            cached_schema,
            limit: None,
            outcome: None,
        }
    }

    /// Spill to sorted runs on disk when the input outgrows
    /// `spill.memory_budget`. A sort with a limit keeps only the rows it
    /// needs and does not spill.
    pub fn with_spill(mut self, spill: SortSpill) -> Self {
        self.spill = Some(spill);
        self
    }

    /// Only the first `limit` rows of the sorted output will be read. The
    /// sort then keeps a bounded set of candidates instead of every input
    /// row.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Ensure the child has been consumed and sorted, in memory or into runs.
    fn load_if_needed(&mut self) -> RookResult<()> {
        if self.loaded {
            return Ok(());
        }

        if let Some(limit) = self.limit {
            self.buffer = self.top_n(limit)?;
            self.outcome = Some("top-N in memory");
            self.loaded = true;
            return Ok(());
        }

        // Consume tuples from child in batches, writing the buffer out as a
        // sorted run each time it outgrows the memory budget.
        let mut sorter: Option<ExternalSorter> = None;
        let mut buffered_bytes = 0usize;
        let mut child_batch = Vec::with_capacity(super::trait_::DEFAULT_BATCH_SIZE);
        while self.child.next_batch(&mut child_batch)? > 0 {
            let Some(spill) = &self.spill else {
                self.buffer.append(&mut child_batch);
                continue;
            };
            for tuple in child_batch.drain(..) {
                buffered_bytes += tuple_bytes(&tuple);
                self.buffer.push(tuple);
                if buffered_bytes > spill.memory_budget {
                    if sorter.is_none() {
                        log::debug!(
                            "[Sort] Input exceeds {} bytes, sorting on disk",
                            spill.memory_budget
                        );
                        sorter = Some(ExternalSorter::new(self.sort_keys.clone(), spill)?);
                    }
                    let keys = &self.sort_keys;
                    self.buffer.sort_by(|a, b| compare_rows(keys, a, b));
                    if let Some(sorter) = &mut sorter {
                        sorter.spill(&mut self.buffer)?;
                    }
                    buffered_bytes = 0;
                }
            }
        }

        if let Some(mut sorter) = sorter {
            if !self.buffer.is_empty() {
                let keys = &self.sort_keys;
                self.buffer.sort_by(|a, b| compare_rows(keys, a, b));
                sorter.spill(&mut self.buffer)?;
            }
            self.buffer = Vec::new();
            self.merge = Some(sorter.finish()?);
            self.outcome = Some("on disk");
            self.loaded = true;
            return Ok(());
        }

        // Check if child already satisfies ordering
        let already_sorted = if let Some(child_order) = self.child.ordering() {
            self.sort_keys.len() <= child_order.len()
                && self
                    .sort_keys
                    .iter()
                    .zip(&child_order)
                    .all(|(req, actual)| req == actual)
        } else {
            false
        };

        if already_sorted {
            log::debug!(
                "[Sort] Child output already satisfies sort keys {:?}, skipping sort",
                self.sort_keys
            );
        } else {
            // Sort in place
            let keys = &self.sort_keys;
            self.buffer.sort_by(|a, b| compare_rows(keys, a, b));
        }

        self.outcome = Some("in memory");
        self.loaded = true;
        Ok(())
    }

    /// The first `limit` rows of the child in sort order, in the order a full
    /// stable sort would give them.
    ///
    /// Keeps at most `2 * limit` (at least 1,024) candidates: when the set is
    /// full, the best `limit` are selected and the rest dropped. Ties are
    /// broken by arrival order, so the result matches the stable sort.
    fn top_n(&mut self, limit: usize) -> RookResult<Vec<Tuple>> {
        let keys = self.sort_keys.clone();
        let cmp = |a: &(u64, Tuple), b: &(u64, Tuple)| {
            compare_rows(&keys, &a.1, &b.1).then(a.0.cmp(&b.0))
        };
        let cap = limit.saturating_mul(2).max(1024);
        let mut kept: Vec<(u64, Tuple)> = Vec::new();
        let mut seq = 0u64;
        let mut batch = Vec::with_capacity(super::trait_::DEFAULT_BATCH_SIZE);
        while self.child.next_batch(&mut batch)? > 0 {
            for tuple in batch.drain(..) {
                kept.push((seq, tuple));
                seq += 1;
                if kept.len() >= cap {
                    if limit == 0 {
                        kept.clear();
                    } else {
                        kept.select_nth_unstable_by(limit - 1, cmp);
                        kept.truncate(limit);
                    }
                }
            }
        }
        kept.sort_by(cmp);
        kept.truncate(limit);
        Ok(kept.into_iter().map(|(_, t)| t).collect())
    }
}

/// Order two rows by `keys` (column, descending): NULLs first ascending,
/// last descending.
/// The kind of a value, for [`compare_rows`]: values of one kind compare.
fn value_kind(v: &DataValue) -> u8 {
    match v {
        DataValue::SmallInt(_)
        | DataValue::Int(_)
        | DataValue::BigInt(_)
        | DataValue::Real(_)
        | DataValue::DoublePrecision(_)
        | DataValue::Numeric(_) => 0,
        DataValue::Bool(_) => 1,
        DataValue::Char(_) | DataValue::Varchar(_) => 2,
        DataValue::Date(_) | DataValue::Timestamp(_) => 3,
        DataValue::Time(_) => 4,
        DataValue::Bit(_) => 5,
    }
}

pub(crate) fn compare_rows(keys: &[(usize, bool)], a: &Tuple, b: &Tuple) -> Ordering {
    for &(key_idx, descending) in keys {
        let av = a.values.get(key_idx).and_then(|v| v.as_ref());
        let bv = b.values.get(key_idx).and_then(|v| v.as_ref());
        let ordering = match (av, bv) {
            // Values of different kinds (only an untyped string literal
            // beside a date, say, puts them in one column) are ordered by
            // kind, so the order stays total: comparing across kinds only
            // sometimes works ('5' with 3, not 'x'), and sort_by panics on
            // an order that is not total.
            (Some(a_val), Some(b_val)) if value_kind(a_val) != value_kind(b_val) => {
                value_kind(a_val).cmp(&value_kind(b_val))
            }
            (Some(a_val), Some(b_val)) => compare_nullable(Some(a_val), Some(b_val))
                .unwrap_or(None)
                .unwrap_or(Ordering::Equal),
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
        };
        let ordering = if descending {
            ordering.reverse()
        } else {
            ordering
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

impl PhysicalOperator for SortOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        self.load_if_needed()?;

        if let Some(merge) = &mut self.merge {
            let tuple = merge.next()?;
            if tuple.is_none() {
                // Done with the run files.
                self.merge = None;
            }
            return Ok(tuple);
        }

        // Each row is emitted once (`reset` sorts again), so it is moved out.
        if self.pos < self.buffer.len() {
            let tuple = std::mem::replace(&mut self.buffer[self.pos], Tuple::empty());
            self.pos += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        self.load_if_needed()?;

        batch.clear();
        if self.merge.is_some() {
            while batch.len() < super::trait_::DEFAULT_BATCH_SIZE {
                match self.next()? {
                    Some(tuple) => batch.push(tuple),
                    None => break,
                }
            }
            return Ok(batch.len());
        }

        let available = self.buffer.len().saturating_sub(self.pos);
        if available == 0 {
            return Ok(0);
        }
        let take = available.min(super::trait_::DEFAULT_BATCH_SIZE);
        batch.extend(
            self.buffer[self.pos..self.pos + take]
                .iter_mut()
                .map(|t| std::mem::replace(t, Tuple::empty())),
        );
        self.pos += take;
        Ok(take)
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.cached_schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.child.reset()?;
        self.loaded = false;
        self.outcome = None;
        self.buffer.clear();
        self.pos = 0;
        self.merge = None;
        Ok(())
    }

    fn estimate_cardinality(&self) -> usize {
        self.child.estimate_cardinality()
    }

    fn name(&self) -> &'static str {
        "Sort"
    }

    fn explain(&self) -> PlanNode {
        let keys: Vec<String> = self
            .sort_keys
            .iter()
            .map(|&(idx, descending)| {
                let name = self
                    .cached_schema
                    .get(idx)
                    .map_or_else(|| format!("#{}", idx), |c| c.name.clone());
                format!("{} {}", name, if descending { "DESC" } else { "ASC" })
            })
            .collect();
        let mut node = PlanNode::new("Sort").with("keys", keys.join(", "));
        match (self.limit, &self.spill) {
            // A top-N sort keeps only the rows it needs and never spills.
            (Some(limit), _) => node = node.with("top_n", limit),
            (None, Some(spill)) => {
                node = node.with("memory_budget", format!("{} bytes", spill.memory_budget))
            }
            (None, None) => {}
        }
        if let Some(outcome) = self.outcome {
            node = node.with("sorted", outcome);
        }
        node.child(self.child.explain())
    }

    fn ordering(&self) -> Option<Vec<(usize, bool)>> {
        Some(self.sort_keys.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Values of different kinds in one sort column (an untyped string
    /// literal in COALESCE beside a number, say) must still sort: an order
    /// that is not total makes `sort_by` panic.
    #[test]
    fn mixed_kinds_sort_without_panicking() {
        let mut rows: Vec<Tuple> = (0..200)
            .map(|i| {
                let value = match i % 4 {
                    0 => DataValue::Int(i),
                    1 => DataValue::Varchar(format!("{}", i / 3)),
                    2 => DataValue::Varchar(format!("{}a", i)),
                    _ => {
                        DataValue::DoublePrecision(crate::types::value::OrderedF64(i as f64 / 7.0))
                    }
                };
                Tuple::new(vec![Some(value)])
            })
            .collect();
        rows.sort_by(|a, b| compare_rows(&[(0, false)], a, b));
        // Numbers first, then strings, each group in order.
        let kinds: Vec<u8> = rows
            .iter()
            .map(|t| value_kind(t.values[0].as_ref().unwrap()))
            .collect();
        assert!(kinds.windows(2).all(|w| w[0] <= w[1]));
    }
}
