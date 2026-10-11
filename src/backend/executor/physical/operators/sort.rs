use std::cmp::Ordering;

use super::super::external_sort::{ExternalSortConfig, ExternalSortOperator};
use super::super::tuple::{ColumnInfo, Tuple};
use super::filter_project::NullOperator;
use super::trait_::PhysicalOperator;
use crate::backend::error::RookResult;

use crate::types::comparison::compare_nullable;

// ── Sort Operator ─────────────────────────────────────────────────────────────

/// Sorts all tuples from the child by the specified columns.
///
/// Uses in-memory sort for small datasets and transparently switches to
/// external merge sort (spilling to temp files) for larger datasets.
pub struct SortOperator {
    child: Box<dyn PhysicalOperator>,
    /// Sort columns: (column_index, descending).
    sort_keys: Vec<(usize, bool)>,
    /// Whether to use external merge sort.
    use_external: bool,
    /// External sort operator (lazily initialised).
    external: Option<ExternalSortOperator>,
    /// Whether the child has been fully consumed (in-memory mode only).
    loaded: bool,
    /// Sorted buffer of tuples (in-memory mode).
    buffer: Vec<Tuple>,
    /// Current position in buffer.
    pos: usize,
    /// Threshold for switching to external sort (0 = always in-memory).
    external_threshold: usize,
    /// Cached schema (survives child replacement during external init).
    cached_schema: Vec<ColumnInfo>,
    /// Only the first `limit` rows of the sorted output are needed (top-N).
    limit: Option<usize>,
}

/// A physical operator that yields tuples from an in-memory buffer first,
/// then continues draining from an underlying child operator.
struct BufferedChildOperator {
    buffer: Vec<Tuple>,
    pos: usize,
    child: Box<dyn PhysicalOperator>,
    schema: Vec<ColumnInfo>,
}

impl PhysicalOperator for BufferedChildOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if self.pos < self.buffer.len() {
            let tuple = self.buffer[self.pos].clone();
            self.pos += 1;
            Ok(Some(tuple))
        } else {
            self.child.next()
        }
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        batch.clear();
        let available = self.buffer.len().saturating_sub(self.pos);
        if available > 0 {
            let take = available.min(super::trait_::DEFAULT_BATCH_SIZE);
            batch.reserve(take);
            for i in 0..take {
                batch.push(self.buffer[self.pos + i].clone());
            }
            self.pos += take;
            Ok(take)
        } else {
            self.child.next_batch(batch)
        }
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.pos = 0;
        self.child.reset()
    }

    fn estimate_cardinality(&self) -> usize {
        self.buffer.len() + self.child.estimate_cardinality()
    }

    fn name(&self) -> &'static str {
        "BufferedChild"
    }
}

impl SortOperator {
    /// Create a new in-memory-only sort operator.
    pub fn new(child: Box<dyn PhysicalOperator>, sort_keys: Vec<(usize, bool)>) -> Self {
        let cached_schema = child.schema().to_vec();
        Self {
            child,
            sort_keys,
            use_external: false,
            external: None,
            loaded: false,
            buffer: Vec::new(),
            pos: 0,
            external_threshold: 0,
            cached_schema,
            limit: None,
        }
    }

    /// Create an adaptive sort operator that switches to external merge sort
    /// when the estimated cardinality exceeds `threshold`.
    pub fn new_adaptive(
        child: Box<dyn PhysicalOperator>,
        sort_keys: Vec<(usize, bool)>,
        threshold: usize,
    ) -> Self {
        let cached_schema = child.schema().to_vec();
        let estimated = child.estimate_cardinality();
        let use_external = threshold > 0 && estimated > threshold;
        log::info!(
            "[Sort] Adaptive mode: estimated={}, threshold={}, use_external={}",
            estimated,
            threshold,
            use_external
        );
        Self {
            child,
            sort_keys,
            use_external,
            external: None,
            loaded: false,
            buffer: Vec::new(),
            pos: 0,
            external_threshold: threshold,
            cached_schema,
            limit: None,
        }
    }

    /// Only the first `limit` rows of the sorted output will be read. The
    /// in-memory sort then keeps a bounded set of candidates instead of every
    /// input row.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Get or initialise the external sort operator.
    fn init_external(&mut self) -> RookResult<&mut ExternalSortOperator> {
        if self.external.is_none() {
            log::info!("[Sort] Switching to external merge sort");
            let config = ExternalSortConfig {
                max_tuples_per_run: if self.external_threshold > 0 {
                    self.external_threshold
                } else {
                    100
                },
                ..Default::default()
            };
            let child = std::mem::replace(&mut self.child, Box::new(NullOperator::new(Vec::new())));
            let source: Box<dyn PhysicalOperator> = if self.buffer.is_empty() {
                child
            } else {
                let buffer = std::mem::take(&mut self.buffer);
                Box::new(BufferedChildOperator {
                    buffer,
                    pos: 0,
                    child,
                    schema: self.cached_schema.clone(),
                })
            };
            self.external = Some(ExternalSortOperator::new(
                source,
                self.sort_keys.clone(),
                config,
            ));
        }
        Ok(self.external.as_mut().unwrap())
    }

    /// Ensure the child has been consumed and sorted in memory (or flagged for external).
    fn load_if_needed(&mut self) -> RookResult<()> {
        if self.loaded {
            return Ok(());
        }

        if let Some(limit) = self.limit
            && self.external_threshold == 0
        {
            self.buffer = self.top_n(limit)?;
            self.loaded = true;
            return Ok(());
        }

        // Consume tuples from child in batches
        let mut child_batch = Vec::with_capacity(super::trait_::DEFAULT_BATCH_SIZE);
        while self.child.next_batch(&mut child_batch)? > 0 {
            self.buffer.append(&mut child_batch);

            // Check if we should switch to external sort
            if self.external_threshold > 0 && self.buffer.len() > self.external_threshold {
                log::info!(
                    "[Sort] In-memory sort exceeded threshold ({} > {}), switching to external",
                    self.buffer.len(),
                    self.external_threshold
                );
                self.use_external = true;
                return Ok(());
            }
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
fn compare_rows(keys: &[(usize, bool)], a: &Tuple, b: &Tuple) -> Ordering {
    for &(key_idx, descending) in keys {
        let av = a.values.get(key_idx).and_then(|v| v.as_ref());
        let bv = b.values.get(key_idx).and_then(|v| v.as_ref());
        let ordering = match (av, bv) {
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
        if self.use_external {
            let external = self.init_external()?;
            return external.next();
        }

        // ── In-memory path ──
        if !self.loaded {
            self.load_if_needed()?;
            if self.use_external {
                let external = self.init_external()?;
                return external.next();
            }
        }

        if self.pos < self.buffer.len() {
            let tuple = self.buffer[self.pos].clone();
            self.pos += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        if self.use_external {
            let external = self.init_external()?;
            return external.next_batch(batch);
        }

        if !self.loaded {
            self.load_if_needed()?;
            if self.use_external {
                let external = self.init_external()?;
                return external.next_batch(batch);
            }
        }

        batch.clear();
        let available = self.buffer.len().saturating_sub(self.pos);
        if available == 0 {
            return Ok(0);
        }
        let take = available.min(super::trait_::DEFAULT_BATCH_SIZE);
        batch.reserve(take);
        for i in 0..take {
            batch.push(self.buffer[self.pos + i].clone());
        }
        self.pos += take;
        Ok(take)
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.cached_schema
    }

    fn reset(&mut self) -> RookResult<()> {
        if self.use_external
            && let Some(ref mut ext) = self.external
        {
            return ext.reset();
        }
        self.child.reset()?;
        self.loaded = false;
        self.buffer.clear();
        self.pos = 0;
        Ok(())
    }

    fn estimate_cardinality(&self) -> usize {
        self.child.estimate_cardinality()
    }

    fn name(&self) -> &'static str {
        "Sort"
    }

    fn ordering(&self) -> Option<Vec<(usize, bool)>> {
        Some(self.sort_keys.clone())
    }
}
