//! Index nested-loop join.
//!
//! The outer side streams and each key is looked up in an index on the inner
//! side. Every candidate the index returns is fetched and re-checked against
//! the whole condition - an index may return a superset.
//!
//! RIGHT and FULL are excluded: enumerating unmatched inner rows would need a
//! set the size of the inner relation.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::heap::HeapManager;
use crate::types::value::DataValue;

use super::super::algorithm::{JoinType, ValidatedJoinSpec};
use super::super::error::JoinError;
use super::super::index::JoinIndex;
use super::super::key::KeySpec;
use super::super::row::{RowBuilder, RowCodec};
use super::super::schema::OutputSchema;
use super::super::source::TableRef;
use super::{ExecStats, MatchEvaluator, RowStream, StatsHandle, new_stats};
use std::rc::Rc;

pub struct IndexNestedLoopJoin {
    join_type: JoinType,
    evaluator: MatchEvaluator,
    /// The key the *index* can answer, which may be narrower than the join's.
    probe_keys: KeySpec,
    index: Rc<dyn JoinIndex>,
    inner: HeapManager,
    builder: RowBuilder,
    schema: Arc<OutputSchema>,
    outer_codec: RowCodec,
    inner_codec: RowCodec,

    outer: Box<dyn RowStream>,
    pending: VecDeque<Vec<u8>>,
    finished: bool,
    stats: StatsHandle,
    outer_values: Vec<Option<DataValue>>,
    inner_values: Vec<Option<DataValue>>,
}

impl IndexNestedLoopJoin {
    pub fn new(
        spec: &ValidatedJoinSpec,
        evaluator: MatchEvaluator,
        probe_keys: KeySpec,
        index: Rc<dyn JoinIndex>,
        inner_table: &TableRef,
        outer: Box<dyn RowStream>,
        schema: Arc<OutputSchema>,
    ) -> Result<Self, JoinError> {
        let inner = HeapManager::open(inner_table.path.clone()).map_err(|e| {
            JoinError::Io(format!(
                "cannot open '{}' at {}: {e}",
                inner_table.alias,
                inner_table.path.display()
            ))
        })?;

        let outer_codec = RowCodec::new(outer.schema().types.clone());
        let inner_codec = RowCodec::new(
            inner_table
                .columns
                .iter()
                .map(|column| column.data_type.clone())
                .collect(),
        );

        Ok(Self {
            join_type: spec.join_type(),
            evaluator,
            probe_keys,
            index,
            inner,
            builder: RowBuilder::new(&schema),
            schema,
            outer_codec,
            inner_codec,
            outer,
            pending: VecDeque::new(),
            finished: false,
            stats: new_stats(),
            outer_values: Vec::new(),
            inner_values: Vec::new(),
        })
    }

    fn emit_unmatched_outer(&mut self) -> Result<(), JoinError> {
        let emit = match self.join_type {
            JoinType::Anti => true,
            JoinType::Semi => false,
            other => other.keeps_unmatched_left(),
        };
        if emit {
            let built = self.builder.build(Some(&self.outer_values), None)?;
            self.pending.push_back(built);
        }
        Ok(())
    }

    fn process(&mut self, row: Vec<u8>) -> Result<(), JoinError> {
        self.stats.borrow_mut().outer_rows += 1;
        self.outer_codec.decode_into(&row, &mut self.outer_values)?;

        let Some(key) = self.probe_keys.left_key(&self.outer_values)? else {
            // No key means nothing to look up, and nothing can match.
            return self.emit_unmatched_outer();
        };

        let candidates = self.index.probe(&key)?;
        let mut matched = false;

        for locator in candidates {
            let bytes = match self.inner.get_tuple(locator.page_id, locator.slot_id) {
                Ok(bytes) => bytes,
                // The index's validity stamp rules out missed inserts, so a
                // row the index points at but the heap no longer holds was
                // deleted. Skipping it is correct.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(JoinError::Io(format!(
                        "cannot fetch row at page {} slot {}: {e}",
                        locator.page_id, locator.slot_id
                    )));
                }
            };

            self.stats.borrow_mut().inner_rows += 1;
            self.inner_codec
                .decode_into(&bytes, &mut self.inner_values)?;
            self.stats.borrow_mut().candidate_pairs += 1;

            // The index promises candidates, not answers: re-check the whole
            // condition, key included.
            if !self
                .evaluator
                .matches(&self.outer_values, &self.inner_values)?
            {
                continue;
            }

            matched = true;
            if self.join_type.emits_left_only() {
                break;
            }

            let built = self
                .builder
                .build(Some(&self.outer_values), Some(&self.inner_values))?;
            self.pending.push_back(built);
        }

        if matched {
            if self.join_type == JoinType::Semi {
                let built = self.builder.build(Some(&self.outer_values), None)?;
                self.pending.push_back(built);
            }
            Ok(())
        } else {
            self.emit_unmatched_outer()
        }
    }
}

impl Iterator for IndexNestedLoopJoin {
    type Item = Result<Vec<u8>, JoinError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(row) = self.pending.pop_front() {
                self.stats.borrow_mut().rows_out += 1;
                return Some(Ok(row));
            }
            if self.finished {
                return None;
            }

            match self.outer.next() {
                Some(Ok(row)) => {
                    if let Err(e) = self.process(row) {
                        return Some(Err(e));
                    }
                }
                Some(Err(e)) => return Some(Err(e)),
                None => self.finished = true,
            }
        }
    }
}

impl RowStream for IndexNestedLoopJoin {
    fn schema(&self) -> &Arc<OutputSchema> {
        &self.schema
    }

    fn stats(&self) -> ExecStats {
        self.stats.borrow().clone()
    }
}
