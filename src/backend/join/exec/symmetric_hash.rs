//! Symmetric hash join.
//!
//! Both inputs are consumed together, each row probing the other side's table
//! before joining its own. A match is emitted as soon as the second of its two
//! rows arrives, so output starts before either input is exhausted - which is
//! what the previous implementation claimed to do while actually buffering
//! everything into a `Vec` and returning nothing until both scans finished.
//!
//! It holds both inputs in memory and cannot spill. That is inherent: it has
//! no build phase to partition. When the budget runs out it says so rather
//! than growing without bound, and the adaptive operator responds by choosing
//! an algorithm that can spill.

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::sync::Arc;

use crate::types::value::DataValue;

use super::super::algorithm::{JoinType, ValidatedJoinSpec};
use super::super::error::JoinError;
use super::super::key::JoinKey;
use super::super::memory::{HASH_ENTRY_OVERHEAD, MemoryAccountant, row_footprint};
use super::super::row::{RowBuilder, RowCodec};
use super::super::schema::OutputSchema;
use super::super::source::RowSource;
use super::{ExecStats, MatchEvaluator, RowStream, StatsHandle, new_stats};

/// Rows seen so far on one side.
#[derive(Default)]
struct SideTable {
    buckets: HashMap<JoinKey, Vec<u32>>,
    rows: Vec<Vec<u8>>,
    matched: Vec<bool>,
}

impl SideTable {
    fn insert(
        &mut self,
        key: JoinKey,
        row: Vec<u8>,
        budget: &MemoryAccountant,
        side: &str,
    ) -> Result<u32, JoinError> {
        let footprint = row_footprint(row.len()) + key.byte_len() as u64 + HASH_ENTRY_OVERHEAD;
        budget.charge(footprint).map_err(|over| {
            JoinError::OutOfMemory(format!(
                "a symmetric hash join cannot spill, and its {side} side no longer fits ({over}); \
                 use a hash or sort-merge join for inputs this size"
            ))
        })?;

        let index = self.rows.len() as u32;
        self.rows.push(row);
        self.matched.push(false);
        self.buckets.entry(key).or_default().push(index);
        Ok(index)
    }

    fn candidates(&self, key: &JoinKey) -> Vec<u32> {
        self.buckets.get(key).cloned().unwrap_or_default()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

pub struct SymmetricHashJoin {
    join_type: JoinType,
    evaluator: MatchEvaluator,
    builder: RowBuilder,
    schema: Arc<OutputSchema>,
    left_codec: RowCodec,
    right_codec: RowCodec,
    budget: Rc<MemoryAccountant>,

    left_input: Option<Box<dyn RowStream>>,
    right_source: Option<Box<dyn RowSource>>,
    right_input: Option<Box<dyn Iterator<Item = Result<Vec<u8>, JoinError>>>>,

    left_table: SideTable,
    right_table: SideTable,
    left_done: bool,
    right_done: bool,
    turn: Side,
    drained: bool,

    pending: VecDeque<Vec<u8>>,
    stats: StatsHandle,
    left_values: Vec<Option<DataValue>>,
    right_values: Vec<Option<DataValue>>,
}

impl SymmetricHashJoin {
    pub fn new(
        spec: &ValidatedJoinSpec,
        evaluator: MatchEvaluator,
        left: Box<dyn RowStream>,
        right: Box<dyn RowSource>,
        schema: Arc<OutputSchema>,
        budget: Rc<MemoryAccountant>,
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
            left_input: Some(left),
            right_source: Some(right),
            right_input: None,
            left_table: SideTable::default(),
            right_table: SideTable::default(),
            left_done: false,
            right_done: false,
            turn: Side::Left,
            drained: false,
            pending: VecDeque::new(),
            stats: new_stats(),
            left_values: Vec::new(),
            right_values: Vec::new(),
        }
    }

    fn ensure_right_open(&mut self) -> Result<(), JoinError> {
        if self.right_input.is_none() {
            if let Some(source) = self.right_source.take() {
                self.right_input = Some(Box::new(source.open()?));
            }
        }
        Ok(())
    }

    /// Take one row from `side`, if it has one left.
    fn pull(&mut self, side: Side) -> Result<Option<Vec<u8>>, JoinError> {
        match side {
            Side::Left => match self.left_input.as_mut().and_then(Iterator::next) {
                Some(row) => {
                    self.stats.borrow_mut().outer_rows += 1;
                    Ok(Some(row?))
                }
                None => {
                    self.left_done = true;
                    Ok(None)
                }
            },
            Side::Right => {
                self.ensure_right_open()?;
                match self.right_input.as_mut().and_then(Iterator::next) {
                    Some(row) => {
                        self.stats.borrow_mut().inner_rows += 1;
                        Ok(Some(row?))
                    }
                    None => {
                        self.right_done = true;
                        Ok(None)
                    }
                }
            }
        }
    }

    /// A row arriving on the left: probe the right table, then store it.
    fn accept_left(&mut self, row: Vec<u8>) -> Result<(), JoinError> {
        self.left_codec.decode_into(&row, &mut self.left_values)?;
        let Some(key) = self.evaluator.keys().left_key(&self.left_values)? else {
            // A NULL key can never match, however much input follows.
            return self.emit_unmatched_left();
        };

        let index = self
            .left_table
            .insert(key.clone(), row, &self.budget, "left")?;

        let mut matched = false;
        for candidate in self.right_table.candidates(&key) {
            let right_row = self.right_table.rows[candidate as usize].clone();
            self.right_codec
                .decode_into(&right_row, &mut self.right_values)?;
            self.stats.borrow_mut().candidate_pairs += 1;

            if !self
                .evaluator
                .residual_matches(&self.left_values, &self.right_values)?
            {
                continue;
            }

            matched = true;
            self.right_table.matched[candidate as usize] = true;
            if self.join_type.emits_left_only() {
                break;
            }
            let built = self
                .builder
                .build(Some(&self.left_values), Some(&self.right_values))?;
            self.pending.push_back(built);
        }

        if matched {
            self.left_table.matched[index as usize] = true;
            if self.join_type == JoinType::Semi {
                let built = self.builder.build(Some(&self.left_values), None)?;
                self.pending.push_back(built);
            }
        }
        Ok(())
    }

    /// A row arriving on the right: probe the left table, then store it.
    fn accept_right(&mut self, row: Vec<u8>) -> Result<(), JoinError> {
        self.right_codec.decode_into(&row, &mut self.right_values)?;
        let Some(key) = self.evaluator.keys().right_key(&self.right_values)? else {
            return self.emit_unmatched_right();
        };

        let index = self
            .right_table
            .insert(key.clone(), row, &self.budget, "right")?;

        for candidate in self.left_table.candidates(&key) {
            let left_row = self.left_table.rows[candidate as usize].clone();
            self.left_codec
                .decode_into(&left_row, &mut self.left_values)?;
            self.stats.borrow_mut().candidate_pairs += 1;

            if !self
                .evaluator
                .residual_matches(&self.left_values, &self.right_values)?
            {
                continue;
            }

            self.right_table.matched[index as usize] = true;

            // A SEMI row is owed exactly once, on the transition to matched.
            let first_match = !self.left_table.matched[candidate as usize];
            self.left_table.matched[candidate as usize] = true;

            if self.join_type.emits_left_only() {
                if self.join_type == JoinType::Semi && first_match {
                    let built = self.builder.build(Some(&self.left_values), None)?;
                    self.pending.push_back(built);
                }
                continue;
            }

            let built = self
                .builder
                .build(Some(&self.left_values), Some(&self.right_values))?;
            self.pending.push_back(built);
        }

        Ok(())
    }

    fn emit_unmatched_left(&mut self) -> Result<(), JoinError> {
        let emit = match self.join_type {
            JoinType::Anti => true,
            JoinType::Semi => false,
            other => other.keeps_unmatched_left(),
        };
        if emit {
            let built = self.builder.build(Some(&self.left_values), None)?;
            self.pending.push_back(built);
        }
        Ok(())
    }

    fn emit_unmatched_right(&mut self) -> Result<(), JoinError> {
        if self.join_type.keeps_unmatched_right() {
            let built = self.builder.build(None, Some(&self.right_values))?;
            self.pending.push_back(built);
        }
        Ok(())
    }

    /// Both inputs are exhausted, so anything still unmatched is final.
    fn drain(&mut self) -> Result<(), JoinError> {
        let left_rows = std::mem::take(&mut self.left_table.rows);
        for (index, row) in left_rows.iter().enumerate() {
            if self.left_table.matched[index] {
                continue;
            }
            self.left_codec.decode_into(row, &mut self.left_values)?;
            self.emit_unmatched_left()?;
        }

        let right_rows = std::mem::take(&mut self.right_table.rows);
        for (index, row) in right_rows.iter().enumerate() {
            if self.right_table.matched[index] {
                continue;
            }
            self.right_codec.decode_into(row, &mut self.right_values)?;
            self.emit_unmatched_right()?;
        }

        Ok(())
    }

    fn advance(&mut self) -> Result<bool, JoinError> {
        if self.left_done && self.right_done {
            if self.drained {
                return Ok(false);
            }
            self.drained = true;
            self.drain()?;
            return Ok(true);
        }

        // Alternate while both sides have rows, so neither table grows
        // needlessly ahead of the other.
        let side = match (self.left_done, self.right_done) {
            (false, true) => Side::Left,
            (true, false) => Side::Right,
            _ => self.turn,
        };
        self.turn = match side {
            Side::Left => Side::Right,
            Side::Right => Side::Left,
        };

        match self.pull(side)? {
            Some(row) => match side {
                Side::Left => self.accept_left(row)?,
                Side::Right => self.accept_right(row)?,
            },
            None => {}
        }
        Ok(true)
    }
}

impl Iterator for SymmetricHashJoin {
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

impl RowStream for SymmetricHashJoin {
    fn schema(&self) -> &Arc<OutputSchema> {
        &self.schema
    }

    fn stats(&self) -> ExecStats {
        self.stats.borrow().clone()
    }
}
