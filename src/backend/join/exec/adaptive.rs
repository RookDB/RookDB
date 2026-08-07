//! Adaptive join.
//!
//! Reads both inputs in alternation and builds from whichever ends first, so
//! the choice is a measurement rather than an estimate. With no equality to
//! hash it runs a nested loop instead. Reversing the sides never changes the
//! output.

use std::rc::Rc;
use std::sync::Arc;

use super::super::algorithm::{JoinType, ValidatedJoinSpec};
use super::super::config::JoinTuning;
use super::super::error::JoinError;
use super::super::memory::MemoryAccountant;
use super::super::schema::OutputSchema;
use super::super::source::RowSource;
use super::super::spill::SpillScope;
use super::hash::HashJoin;
use super::nested_loop::NestedLoopJoin;
use super::{ExecStats, MatchEvaluator, RowStream};

/// Which input ends first, and therefore which is smaller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SmallerSide {
    Left,
    Right,
    /// Neither ended within the sample, so the measurement is inconclusive.
    Unknown,
}

/// Read both inputs in alternation until one ends.
fn smaller_side(
    left: &dyn RowSource,
    right: &dyn RowSource,
    sample_rows: u64,
) -> Result<SmallerSide, JoinError> {
    let mut left_stream = left.open()?;
    let mut right_stream = right.open()?;

    for _ in 0..sample_rows {
        let left_row = left_stream.next().transpose()?;
        let right_row = right_stream.next().transpose()?;

        match (left_row.is_none(), right_row.is_none()) {
            // Both ended on the same step: they are the same size, and the
            // declared orientation is as good as any.
            (true, true) => return Ok(SmallerSide::Right),
            (true, false) => return Ok(SmallerSide::Left),
            (false, true) => return Ok(SmallerSide::Right),
            (false, false) => {}
        }
    }

    Ok(SmallerSide::Unknown)
}

pub struct AdaptiveJoin {
    inner: Box<dyn RowStream>,
    budget: Rc<MemoryAccountant>,
    tuning: JoinTuning,
    role_reversed: bool,
    rows_seen: u64,
    pressure_reductions: u64,
    /// System memory at construction, used to interpret later readings.
    total_memory: u64,
}

impl AdaptiveJoin {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        spec: &ValidatedJoinSpec,
        evaluator: MatchEvaluator,
        left: Box<dyn RowSource>,
        right: Box<dyn RowSource>,
        schema: Arc<OutputSchema>,
        budget: Rc<MemoryAccountant>,
        scope: Arc<SpillScope>,
        tuning: JoinTuning,
    ) -> Result<Self, JoinError> {
        let (inner, role_reversed) =
            Self::choose(spec, evaluator, left, right, schema, &budget, scope, tuning)?;

        Ok(Self {
            inner,
            budget,
            tuning,
            role_reversed,
            rows_seen: 0,
            pressure_reductions: 0,
            total_memory: system_total_memory(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn choose(
        spec: &ValidatedJoinSpec,
        evaluator: MatchEvaluator,
        left: Box<dyn RowSource>,
        right: Box<dyn RowSource>,
        schema: Arc<OutputSchema>,
        budget: &Rc<MemoryAccountant>,
        scope: Arc<SpillScope>,
        tuning: JoinTuning,
    ) -> Result<(Box<dyn RowStream>, bool), JoinError> {
        // No equality means no key, and a hash table needs one.
        if spec.keys().is_empty() {
            let outer = left.open()?;
            return Ok((
                Box::new(NestedLoopJoin::new(
                    spec,
                    evaluator,
                    outer,
                    right,
                    schema,
                    tuning.block_rows,
                )),
                false,
            ));
        }

        // SEMI and ANTI are defined in terms of left rows, so their roles are
        // not interchangeable; everything else is.
        let reversible = !spec.join_type().emits_left_only() && spec.join_type() != JoinType::Cross;
        let smaller = if reversible {
            smaller_side(left.as_ref(), right.as_ref(), tuning.adaptive_sample_rows)?
        } else {
            SmallerSide::Unknown
        };

        if smaller == SmallerSide::Left {
            // Build from the left relation and probe with the right.
            let probe = right.open()?;
            return Ok((
                Box::new(HashJoin::with_roles(
                    spec,
                    evaluator,
                    probe,
                    left,
                    schema,
                    Rc::clone(budget),
                    scope,
                    tuning,
                    false,
                )),
                true,
            ));
        }

        let probe = left.open()?;
        Ok((
            Box::new(HashJoin::with_roles(
                spec,
                evaluator,
                probe,
                right,
                schema,
                Rc::clone(budget),
                scope,
                tuning,
                true,
            )),
            false,
        ))
    }

    /// Shrink the budget if the machine is genuinely short of memory.
    fn check_pressure(&mut self) {
        if self.total_memory == 0 {
            return;
        }
        let available = system_available_memory();
        if available == 0 {
            return;
        }

        let fraction = available as f64 / self.total_memory as f64;
        if fraction >= self.tuning.pressure_threshold {
            return;
        }

        let reduced = self.budget.budget() / 2;
        let before = self.budget.budget();
        self.budget.shrink_to(reduced);
        if self.budget.budget() < before {
            self.pressure_reductions += 1;
            log::info!(
                "[join] system memory at {:.1}%; reduced join budget to {} bytes",
                fraction * 100.0,
                self.budget.budget()
            );
        }
    }
}

impl Iterator for AdaptiveJoin {
    type Item = Result<Vec<u8>, JoinError>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.inner.next()?;

        self.rows_seen += 1;
        if self
            .rows_seen
            .is_multiple_of(self.tuning.pressure_check_rows)
        {
            self.check_pressure();
        }

        Some(row)
    }
}

impl RowStream for AdaptiveJoin {
    fn schema(&self) -> &Arc<OutputSchema> {
        self.inner.schema()
    }

    fn stats(&self) -> ExecStats {
        let mut stats = self.inner.stats();
        stats.role_reversed = self.role_reversed;
        stats.strategy_switches = self.pressure_reductions;
        stats
    }
}

fn system_total_memory() -> u64 {
    use sysinfo::{MemoryRefreshKind, System};
    let mut system = System::new();
    system.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
    system.total_memory()
}

fn system_available_memory() -> u64 {
    use sysinfo::{MemoryRefreshKind, System};
    let mut system = System::new();
    system.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
    system.available_memory()
}
