//! Nested-loop join.
//!
//! One operator for both the simple and blocked variants - the simple one is a
//! block of a single row. The only family that can run every join type,
//! including CROSS and arbitrary non-equi conditions.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::types::value::DataValue;

use super::super::algorithm::{JoinType, ValidatedJoinSpec};
use super::super::error::JoinError;
use super::super::row::{RowBuilder, RowCodec};
use super::super::schema::OutputSchema;
use super::super::source::RowSource;
use super::{ExecStats, MatchEvaluator, RowStream, StatsHandle, new_stats};

pub struct NestedLoopJoin {
    join_type: JoinType,
    evaluator: MatchEvaluator,
    outer: Box<dyn RowStream>,
    inner: Box<dyn RowSource>,
    outer_codec: RowCodec,
    inner_codec: RowCodec,
    builder: RowBuilder,
    schema: Arc<OutputSchema>,
    block_rows: usize,

    /// Rows produced by the current block, waiting to be handed out.
    pending: VecDeque<Vec<u8>>,
    /// Which inner rows have matched something, by scan position. Requires the
    /// inner source to yield rows in a stable order across scans, which is the
    /// [`RowSource`] contract.
    inner_matched: Vec<bool>,
    outer_done: bool,
    inner_drained: bool,
    stats: StatsHandle,
}

impl NestedLoopJoin {
    /// `spec` is the proof that this algorithm supports this join type; it can
    /// only have come from `AlgorithmSpec::validate`.
    pub fn new(
        spec: &ValidatedJoinSpec,
        evaluator: MatchEvaluator,
        outer: Box<dyn RowStream>,
        inner: Box<dyn RowSource>,
        schema: Arc<OutputSchema>,
        block_rows: usize,
    ) -> Self {
        let outer_codec = RowCodec::new(outer.schema().types.clone());
        let inner_codec = RowCodec::new(inner.schema().types.clone());
        let builder = RowBuilder::new(&schema);

        Self {
            join_type: spec.join_type(),
            evaluator,
            outer,
            inner,
            outer_codec,
            inner_codec,
            builder,
            schema,
            block_rows: block_rows.max(1),
            pending: VecDeque::new(),
            inner_matched: Vec::new(),
            outer_done: false,
            inner_drained: false,
            stats: new_stats(),
        }
    }

    fn tracks_inner_matches(&self) -> bool {
        self.join_type.keeps_unmatched_right()
    }

    /// Pull the next block of outer rows, decoded.
    fn next_block(&mut self) -> Result<Vec<Vec<Option<DataValue>>>, JoinError> {
        let mut block = Vec::with_capacity(self.block_rows);

        while block.len() < self.block_rows {
            match self.outer.next() {
                None => {
                    self.outer_done = true;
                    break;
                }
                Some(Err(e)) => return Err(e),
                Some(Ok(bytes)) => {
                    self.stats.borrow_mut().outer_rows += 1;
                    block.push(self.outer_codec.decode(&bytes)?);
                }
            }
        }

        Ok(block)
    }

    /// Join one block of outer rows against a full pass of the inner input.
    fn run_block(&mut self, block: &[Vec<Option<DataValue>>]) -> Result<(), JoinError> {
        let mut outer_matched = vec![false; block.len()];
        let emits_pairs = !self.join_type.emits_left_only();

        let mut inner_stream = self.inner.open()?;
        self.stats.borrow_mut().inner_rescans += 1;
        let mut position = 0usize;

        while let Some(row) = inner_stream.next() {
            let bytes = row?;
            self.stats.borrow_mut().inner_rows += 1;
            let inner_row = self.inner_codec.decode(&bytes)?;

            if self.tracks_inner_matches() && self.inner_matched.len() <= position {
                self.inner_matched.resize(position + 1, false);
            }

            for (index, outer_row) in block.iter().enumerate() {
                self.stats.borrow_mut().candidate_pairs += 1;
                if !self.evaluator.matches(outer_row, &inner_row)? {
                    continue;
                }

                outer_matched[index] = true;
                if self.tracks_inner_matches() {
                    self.inner_matched[position] = true;
                }
                if emits_pairs {
                    self.pending
                        .push_back(self.builder.build(Some(outer_row), Some(&inner_row))?);
                }
            }

            position += 1;
        }

        // Rows the inner pass did not resolve: NULL-extended outer rows for an
        // outer join, or the bare outer row for SEMI and ANTI.
        for (index, outer_row) in block.iter().enumerate() {
            let matched = outer_matched[index];
            let emit = match self.join_type {
                JoinType::Semi => matched,
                JoinType::Anti => !matched,
                _ => !matched && self.join_type.keeps_unmatched_left(),
            };
            if !emit {
                continue;
            }

            // SEMI and ANTI have a left-only output schema, so the same call
            // produces a bare left row for them and a NULL-extended one for
            // the outer joins.
            self.pending
                .push_back(self.builder.build(Some(outer_row), None)?);
        }

        Ok(())
    }

    /// Emit inner rows that never matched, NULL-extended on the outer side.
    /// Only RIGHT and FULL outer joins reach this.
    fn drain_unmatched_inner(&mut self) -> Result<(), JoinError> {
        let mut inner_stream = self.inner.open()?;
        self.stats.borrow_mut().inner_rescans += 1;
        let mut position = 0usize;

        while let Some(row) = inner_stream.next() {
            let bytes = row?;
            let matched = self.inner_matched.get(position).copied().unwrap_or(false);
            if !matched {
                let inner_row = self.inner_codec.decode(&bytes)?;
                self.pending
                    .push_back(self.builder.build(None, Some(&inner_row))?);
            }
            position += 1;
        }

        Ok(())
    }

    fn advance(&mut self) -> Result<bool, JoinError> {
        if !self.outer_done {
            let block = self.next_block()?;
            if !block.is_empty() {
                self.run_block(&block)?;
                return Ok(true);
            }
        }

        if self.tracks_inner_matches() && !self.inner_drained {
            self.inner_drained = true;
            self.drain_unmatched_inner()?;
            return Ok(true);
        }

        Ok(false)
    }
}

impl Iterator for NestedLoopJoin {
    type Item = Result<Vec<u8>, JoinError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.pending.pop_front() {
                self.stats.borrow_mut().rows_out += 1;
                return Some(Ok(row));
            }

            match self.advance() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl RowStream for NestedLoopJoin {
    fn schema(&self) -> &Arc<OutputSchema> {
        &self.schema
    }

    fn stats(&self) -> ExecStats {
        self.stats.borrow().clone()
    }
}
