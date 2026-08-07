//! Adaptive join.
//!
//! Two decisions are made from what the data turns out to be, rather than from
//! what the statistics predicted:
//!
//! **Which side to build from.** Both inputs are read a little way in
//! alternation; whichever reaches its end first is *provably* the smaller, and
//! becomes the hash table. This does not depend on distinct-value estimates,
//! row counts, or an ANALYZE having been run - it is a measurement. Reversing
//! the roles never changes the output: the operator fills each half of an
//! output row from whichever input the declared schema says belongs there.
//!
//! **Whether hashing applies at all.** With no equality between the relations
//! there is no key to hash, and the operator runs a block nested loop instead
//! of failing.
//!
//! Beneath that, the hash join it delegates to already degrades on its own -
//! resident, to hybrid, to Grace, to a nested loop over one partition that a
//! single dominant key will not let it split. The adaptive layer adds the
//! decisions that have to be made *before* building starts, plus a periodic
//! check of real system memory so a budget set when the machine was idle does
//! not stand while it is under pressure.

use std::rc::Rc;
use std::sync::Arc;

use super::super::algorithm::{JoinType, ValidatedJoinSpec};
use super::super::error::JoinError;
use super::super::memory::MemoryAccountant;
use super::super::schema::OutputSchema;
use super::super::source::RowSource;
use super::super::spill::SpillScope;
use super::hash::HashJoin;
use super::nested_loop::{DEFAULT_BLOCK_ROWS, NestedLoopJoin};
use super::{ExecStats, MatchEvaluator, RowStream};

/// Rows read from each side while deciding which is smaller.
const SAMPLE_ROWS: u64 = 8_192;

/// Rows between checks of real system memory. Polling is not free, and memory
/// pressure does not change per row.
const PRESSURE_INTERVAL: u64 = 65_536;

/// Below this fraction of system memory still available, the budget is halved.
const PRESSURE_THRESHOLD: f64 = 0.10;

/// Which input ends first, and therefore which is smaller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SmallerSide {
    Left,
    Right,
    /// Neither ended within the sample, so the measurement is inconclusive.
    Unknown,
}

/// Read both inputs in alternation until one ends.
///
/// The sampled rows are discarded. Both inputs are re-openable relations, so
/// reading a bounded prefix twice is cheaper - and far simpler - than the
/// machinery needed to splice a buffered prefix back onto a stream.
fn smaller_side(left: &dyn RowSource, right: &dyn RowSource) -> Result<SmallerSide, JoinError> {
    let mut left_stream = left.open()?;
    let mut right_stream = right.open()?;

    for _ in 0..SAMPLE_ROWS {
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
        block_rows: usize,
    ) -> Result<Self, JoinError> {
        let (inner, role_reversed) = Self::choose(
            spec, evaluator, left, right, schema, &budget, scope, block_rows,
        )?;

        Ok(Self {
            inner,
            budget,
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
        block_rows: usize,
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
                    block_rows.max(DEFAULT_BLOCK_ROWS),
                )),
                false,
            ));
        }

        // SEMI and ANTI are defined in terms of left rows, so their roles are
        // not interchangeable; everything else is.
        let reversible = !spec.join_type().emits_left_only() && spec.join_type() != JoinType::Cross;
        let smaller = if reversible {
            smaller_side(left.as_ref(), right.as_ref())?
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
                true,
            )),
            false,
        ))
    }

    /// Shrink the budget if the machine is genuinely short of memory.
    ///
    /// A budget chosen when the system was idle should not stand while it is
    /// under pressure. Reducing it makes the next charge fail, which the hash
    /// join answers by spilling - so this reaches the right behaviour through
    /// the mechanism that already exists, rather than a second one.
    fn check_pressure(&mut self) {
        if self.total_memory == 0 {
            return;
        }
        let available = system_available_memory();
        if available == 0 {
            return;
        }

        let fraction = available as f64 / self.total_memory as f64;
        if fraction >= PRESSURE_THRESHOLD {
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
        if self.rows_seen.is_multiple_of(PRESSURE_INTERVAL) {
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
