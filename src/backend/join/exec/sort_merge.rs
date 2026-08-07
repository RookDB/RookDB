//! Sort-merge join.
//!
//! Both sides are sorted, then walked in step. The right-hand group of a
//! duplicate key is buffered and replayed per left row; that buffer spills if
//! it outgrows the budget, so one hot key cannot exhaust memory.

use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;

use crate::types::value::DataValue;

use super::super::algorithm::{JoinType, ValidatedJoinSpec};
use super::super::config::JoinTuning;
use super::super::error::JoinError;
use super::super::key::JoinKey;
use super::super::memory::MemoryAccountant;
use super::super::row::{RowBuilder, RowCodec};
use super::super::schema::OutputSchema;
use super::super::sort::{KeySide, SortedRows, sort_rows};
use super::super::source::RowSource;
use super::super::spill::{RowBuffer, RowBufferBuilder, SpillScope};
use super::{ExecStats, MatchEvaluator, RowStream, StatsHandle, new_stats};

/// One side's sorted stream plus the row currently at its head.
struct Cursor {
    rows: SortedRows,
    head: Option<(JoinKey, Vec<u8>)>,
}

impl Cursor {
    fn new(mut rows: SortedRows) -> Result<Self, JoinError> {
        let head = rows.next().transpose()?;
        Ok(Self { rows, head })
    }

    fn advance(&mut self) -> Result<Option<(JoinKey, Vec<u8>)>, JoinError> {
        let next = self.rows.next().transpose()?;
        Ok(std::mem::replace(&mut self.head, next))
    }

    fn key(&self) -> Option<&JoinKey> {
        self.head.as_ref().map(|(key, _)| key)
    }
}

enum Stage {
    NotStarted,
    Merging,
    /// Left rows past the end of the right input.
    DrainLeft,
    /// Right rows past the end of the left input.
    DrainRight,
    /// Rows set aside by the sort because their key was NULL.
    DrainNulls,
    Finished,
}

pub struct SortMergeJoin {
    join_type: JoinType,
    evaluator: MatchEvaluator,
    builder: RowBuilder,
    schema: Arc<OutputSchema>,
    left_codec: RowCodec,
    right_codec: RowCodec,
    budget: Rc<MemoryAccountant>,
    scope: Arc<SpillScope>,
    tuning: JoinTuning,

    left_input: Option<Box<dyn RowStream>>,
    right_input: Option<Box<dyn RowSource>>,

    left: Option<Cursor>,
    right: Option<Cursor>,
    left_nulls: Option<RowBuffer>,
    right_nulls: Option<RowBuffer>,

    stage: Stage,
    pending: VecDeque<Vec<u8>>,
    stats: StatsHandle,
    left_values: Vec<Option<DataValue>>,
    right_values: Vec<Option<DataValue>>,
}

impl SortMergeJoin {
    pub fn new(
        spec: &ValidatedJoinSpec,
        evaluator: MatchEvaluator,
        left: Box<dyn RowStream>,
        right: Box<dyn RowSource>,
        schema: Arc<OutputSchema>,
        budget: Rc<MemoryAccountant>,
        scope: Arc<SpillScope>,
        tuning: JoinTuning,
    ) -> Self {
        let left_codec = RowCodec::new(left.schema().types.clone());
        let right_codec = RowCodec::new(right.schema().types.clone());

        Self {
            join_type: spec.join_type(),
            evaluator,
            builder: RowBuilder::new(&schema),
            schema,
            left_codec,
            right_codec,
            budget,
            scope,
            tuning,
            left_input: Some(left),
            right_input: Some(right),
            left: None,
            right: None,
            left_nulls: None,
            right_nulls: None,
            stage: Stage::NotStarted,
            pending: VecDeque::new(),
            stats: new_stats(),
            left_values: Vec::new(),
            right_values: Vec::new(),
        }
    }

    // ── Sorting both inputs ──────────────────────────────────────────────────

    fn start(&mut self) -> Result<Stage, JoinError> {
        let (Some(left_input), Some(right_source)) =
            (self.left_input.take(), self.right_input.take())
        else {
            return Ok(Stage::Finished);
        };

        let left_fingerprint = left_input.schema().fingerprint;
        let right_fingerprint = right_source.schema().fingerprint;

        let mut left_iter = left_input;
        let left_sorted = sort_rows(
            &mut left_iter,
            &self.left_codec,
            self.evaluator.keys(),
            KeySide::Left,
            &self.budget,
            &self.scope,
            "smj-left",
            left_fingerprint,
            self.tuning.merge_buffer_bytes,
        )?;

        let mut right_iter = right_source.open()?;
        let right_sorted = sort_rows(
            &mut right_iter,
            &self.right_codec,
            self.evaluator.keys(),
            KeySide::Right,
            &self.budget,
            &self.scope,
            "smj-right",
            right_fingerprint,
            self.tuning.merge_buffer_bytes,
        )?;

        {
            let mut stats = self.stats.borrow_mut();
            stats.outer_rows = left_sorted.stats.sorted_rows + left_sorted.stats.null_keyed_rows;
            stats.inner_rows = right_sorted.stats.sorted_rows + right_sorted.stats.null_keyed_rows;
            stats.sort_runs = left_sorted.stats.runs + right_sorted.stats.runs;
            stats.merge_passes = left_sorted.stats.merge_passes + right_sorted.stats.merge_passes;
            stats.spilled_bytes +=
                left_sorted.stats.spilled_bytes + right_sorted.stats.spilled_bytes;
        }

        self.left_nulls = Some(left_sorted.null_keyed);
        self.right_nulls = Some(right_sorted.null_keyed);
        self.left = Some(Cursor::new(left_sorted.rows)?);
        self.right = Some(Cursor::new(right_sorted.rows)?);

        Ok(Stage::Merging)
    }

    // ── Emitting ─────────────────────────────────────────────────────────────

    fn emit_unmatched_left(&mut self, row: &[u8]) -> Result<(), JoinError> {
        let emit = match self.join_type {
            JoinType::Anti => true,
            JoinType::Semi => false,
            other => other.keeps_unmatched_left(),
        };
        if !emit {
            return Ok(());
        }
        self.left_codec.decode_into(row, &mut self.left_values)?;
        let built = self.builder.build(Some(&self.left_values), None)?;
        self.pending.push_back(built);
        Ok(())
    }

    fn emit_unmatched_right(&mut self, row: &[u8]) -> Result<(), JoinError> {
        if !self.join_type.keeps_unmatched_right() {
            return Ok(());
        }
        self.right_codec.decode_into(row, &mut self.right_values)?;
        let built = self.builder.build(None, Some(&self.right_values))?;
        self.pending.push_back(built);
        Ok(())
    }

    // ── The merge ────────────────────────────────────────────────────────────

    fn merge_step(&mut self) -> Result<Stage, JoinError> {
        let (Some(left), Some(right)) = (self.left.as_ref(), self.right.as_ref()) else {
            return Ok(Stage::DrainNulls);
        };

        let (Some(left_key), Some(right_key)) = (left.key(), right.key()) else {
            return Ok(if left.head.is_some() {
                Stage::DrainLeft
            } else if right.head.is_some() {
                Stage::DrainRight
            } else {
                Stage::DrainNulls
            });
        };

        match left_key.cmp(right_key) {
            std::cmp::Ordering::Less => {
                // No right row can carry this key: the right side is sorted.
                if let Some((_, row)) = self.take_left()? {
                    self.emit_unmatched_left(&row)?;
                }
                Ok(Stage::Merging)
            }
            std::cmp::Ordering::Greater => {
                if let Some((_, row)) = self.take_right()? {
                    self.emit_unmatched_right(&row)?;
                }
                Ok(Stage::Merging)
            }
            std::cmp::Ordering::Equal => {
                let key = left_key.clone();
                self.join_group(&key)?;
                Ok(Stage::Merging)
            }
        }
    }

    fn take_left(&mut self) -> Result<Option<(JoinKey, Vec<u8>)>, JoinError> {
        match self.left.as_mut() {
            Some(cursor) => cursor.advance(),
            None => Ok(None),
        }
    }

    fn take_right(&mut self) -> Result<Option<(JoinKey, Vec<u8>)>, JoinError> {
        match self.right.as_mut() {
            Some(cursor) => cursor.advance(),
            None => Ok(None),
        }
    }

    /// Join every left row carrying `key` against every right row carrying it.
    fn join_group(&mut self, key: &JoinKey) -> Result<(), JoinError> {
        // Buffer the right group once. It spills if it outgrows the budget,
        // which is what bounds a single hot key.
        let mut group = RowBufferBuilder::new(&self.scope, "smj-group", self.schema.fingerprint);
        while self.right.as_ref().and_then(Cursor::key) == Some(key) {
            let Some((_, row)) = self.take_right()? else {
                break;
            };
            group.push(&row, &self.budget)?;
        }
        let spilled = group.spilled();
        let group = group.finish(&self.budget)?;
        if spilled {
            self.stats.borrow_mut().spilled_groups += 1;
            self.stats.borrow_mut().spilled_bytes += match &group {
                RowBuffer::Disk(handle) => handle.bytes(),
                RowBuffer::Memory(_) => 0,
            };
        }

        // Which right rows of the group matched something, for RIGHT and FULL.
        let mut group_matched = vec![false; group.len() as usize];

        while self.left.as_ref().and_then(Cursor::key) == Some(key) {
            let Some((_, left_row)) = self.take_left()? else {
                break;
            };
            self.left_codec
                .decode_into(&left_row, &mut self.left_values)?;

            let mut matched = false;
            for (index, right_row) in group.reader()?.enumerate() {
                let right_row = right_row?;
                self.right_codec
                    .decode_into(&right_row, &mut self.right_values)?;
                self.stats.borrow_mut().candidate_pairs += 1;

                // The group establishes key equality; the residual is
                // everything the condition asked for beyond it.
                if !self
                    .evaluator
                    .residual_matches(&self.left_values, &self.right_values)?
                {
                    continue;
                }

                matched = true;
                if let Some(flag) = group_matched.get_mut(index) {
                    *flag = true;
                }

                if self.join_type.emits_left_only() {
                    // One match settles a SEMI or ANTI row; emitting per match
                    // would duplicate it.
                    break;
                }

                let built = self
                    .builder
                    .build(Some(&self.left_values), Some(&self.right_values))?;
                self.pending.push_back(built);
            }

            if matched {
                if self.join_type == JoinType::Semi {
                    let built = self.builder.build(Some(&self.left_values), None)?;
                    self.pending.push_back(built);
                }
            } else {
                self.emit_unmatched_left(&left_row)?;
            }
        }

        if self.join_type.keeps_unmatched_right() {
            for (index, right_row) in group.reader()?.enumerate() {
                if group_matched.get(index).copied().unwrap_or(false) {
                    continue;
                }
                self.emit_unmatched_right(&right_row?)?;
            }
        }

        Ok(())
    }

    fn drain_left(&mut self) -> Result<Stage, JoinError> {
        while let Some((_, row)) = self.take_left()? {
            self.emit_unmatched_left(&row)?;
            if !self.pending.is_empty() {
                return Ok(Stage::DrainLeft);
            }
        }
        Ok(Stage::DrainNulls)
    }

    fn drain_right(&mut self) -> Result<Stage, JoinError> {
        while let Some((_, row)) = self.take_right()? {
            self.emit_unmatched_right(&row)?;
            if !self.pending.is_empty() {
                return Ok(Stage::DrainRight);
            }
        }
        Ok(Stage::DrainNulls)
    }

    /// NULL-keyed rows match nothing, so they are unmatched by definition.
    fn drain_nulls(&mut self) -> Result<Stage, JoinError> {
        if let Some(buffer) = self.left_nulls.take() {
            let rows: Vec<Vec<u8>> = buffer.reader()?.collect::<Result<Vec<_>, JoinError>>()?;
            for row in rows {
                self.emit_unmatched_left(&row)?;
            }
        }
        if let Some(buffer) = self.right_nulls.take() {
            let rows: Vec<Vec<u8>> = buffer.reader()?.collect::<Result<Vec<_>, JoinError>>()?;
            for row in rows {
                self.emit_unmatched_right(&row)?;
            }
        }
        Ok(Stage::Finished)
    }

    fn advance(&mut self) -> Result<bool, JoinError> {
        self.stage = match std::mem::replace(&mut self.stage, Stage::Finished) {
            Stage::NotStarted => self.start()?,
            Stage::Merging => self.merge_step()?,
            Stage::DrainLeft => self.drain_left()?,
            Stage::DrainRight => self.drain_right()?,
            Stage::DrainNulls => self.drain_nulls()?,
            Stage::Finished => return Ok(false),
        };
        Ok(true)
    }
}

impl Iterator for SortMergeJoin {
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

impl RowStream for SortMergeJoin {
    fn schema(&self) -> &Arc<OutputSchema> {
        &self.schema
    }

    fn stats(&self) -> ExecStats {
        self.stats.borrow().clone()
    }
}
