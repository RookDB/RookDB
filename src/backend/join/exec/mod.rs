//! The execution layer: what a join operator is, and what it means for two
//! rows to join.
//!
//! Operators are plain `Iterator`s over serialized rows. That choice buys
//! composition with the engine's existing `filter_iter`, and it makes cleanup
//! `Drop`'s job rather than a `close()` a caller can forget - which matters
//! once operators own spill directories.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::executor::selection::TriValue;
use crate::types::value::DataValue;

use super::error::JoinError;
use super::key::KeySpec;
use super::predicate::JoinPredicate;
use super::schema::OutputSchema;

pub mod hash;
pub mod index_nested_loop;
pub mod nested_loop;
pub mod sort_merge;
pub mod symmetric_hash;

/// Counters describing what an operator actually did.
///
/// These are what EXPLAIN ANALYZE reports, and what tests assert on to show a
/// path was genuinely exercised rather than merely not crashing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecStats {
    pub rows_out: u64,
    /// Rows pulled from the outer (or probe) input.
    pub outer_rows: u64,
    /// Rows pulled from the inner (or build) input, summed over every scan.
    pub inner_rows: u64,
    /// Row pairs the match evaluator was asked about.
    pub candidate_pairs: u64,
    /// Times the inner input was reopened.
    pub inner_rescans: u64,

    /// Row bytes written to spill files. Non-zero proves a spilling path was
    /// genuinely taken, which is what makes those paths testable.
    pub spilled_bytes: u64,
    /// Partition pairs created by a hash join, including repartitioned ones.
    pub partitions: u64,
    /// Deepest level of recursive repartitioning reached.
    pub repartition_depth: u32,
    /// Partitions that still did not fit after repartitioning, because a
    /// single key dominates them.
    pub oversized_partitions: u64,
    /// Sorted runs written by an external sort.
    pub sort_runs: u64,
    /// Passes made over those runs.
    pub merge_passes: u64,
    /// Duplicate groups a sort-merge join had to spill.
    pub spilled_groups: u64,
}

pub type StatsHandle = Rc<RefCell<ExecStats>>;

pub fn new_stats() -> StatsHandle {
    Rc::new(RefCell::new(ExecStats::default()))
}

/// A stream of serialized rows conforming to a known schema.
pub trait RowStream: Iterator<Item = Result<Vec<u8>, JoinError>> {
    fn schema(&self) -> &Arc<OutputSchema>;
    /// A snapshot of this stream's counters, valid at any point.
    fn stats(&self) -> ExecStats;
}

/// The single definition of "these two rows join".
///
/// Every operator uses this, so a hash join, a sort-merge join and a nested
/// loop cannot disagree about what a match is. Equality comes from the key
/// encoding - the same bytes the hash table and the merge comparator use - so
/// a NULL key never matches, in any algorithm, including after a row has been
/// written to and read back from a spill file.
#[derive(Debug, Clone)]
pub struct MatchEvaluator {
    keys: KeySpec,
    residual: Option<JoinPredicate>,
}

impl MatchEvaluator {
    pub fn new(keys: KeySpec, residual: Option<JoinPredicate>) -> Self {
        Self { keys, residual }
    }

    pub fn keys(&self) -> &KeySpec {
        &self.keys
    }

    pub fn has_residual(&self) -> bool {
        self.residual.is_some()
    }

    /// Whether the residual alone accepts this pair. Used by operators that
    /// have already established key equality by other means - a hash bucket
    /// hit, or a merge-join group - so they do not re-encode the keys.
    pub fn residual_matches(
        &self,
        left: &[Option<DataValue>],
        right: &[Option<DataValue>],
    ) -> Result<bool, JoinError> {
        match &self.residual {
            None => Ok(true),
            Some(predicate) => Ok(predicate.evaluate(left, right)? == TriValue::True),
        }
    }

    /// Whether the full condition - keys and residual - accepts this pair.
    pub fn matches(
        &self,
        left: &[Option<DataValue>],
        right: &[Option<DataValue>],
    ) -> Result<bool, JoinError> {
        if !self.keys.is_empty() {
            let (Some(left_key), Some(right_key)) =
                (self.keys.left_key(left)?, self.keys.right_key(right)?)
            else {
                // A NULL in any key component. There is no key, so there is
                // no match - this is where SQL's "NULL never equals NULL"
                // is enforced for every algorithm at once.
                return Ok(false);
            };
            if left_key != right_key {
                return Ok(false);
            }
        }

        self.residual_matches(left, right)
    }
}
