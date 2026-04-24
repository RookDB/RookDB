//! Nested Loop Join executor (Simple and Block modes).

use std::collections::HashSet;
use std::io;

use crate::catalog::types::Catalog;

use super::condition::evaluate_conditions;
use super::result::JoinResult;
use super::scanner::TupleScanner;
use super::tuple::Tuple;
use super::{JoinCondition, JoinType, NLJMode};

/// Nested Loop Join executor.
///
/// - **Simple mode**: one inner-table scan per outer row.
/// - **Block mode** : loads a chunk of outer rows, then scans inner once per chunk.
pub struct NLJExecutor {
    pub outer_table: String,
    pub inner_table: String,
    pub conditions: Vec<JoinCondition>,
    pub join_type: JoinType,
    pub block_size: usize,
    pub mode: NLJMode,
}

impl NLJExecutor {
    /// Dispatch to the appropriate execution strategy.
    pub fn execute(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        match self.mode {
            NLJMode::Simple  => self.execute_simple(db, catalog),
            NLJMode::Block   => self.execute_block(db, catalog),
            NLJMode::Indexed => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Indexed NLJ mode not yet implemented",
            )),
        }
    }

    // ── Simple NLJ ───────────────────────────────────────────────────

    fn execute_simple(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        let mut outer = TupleScanner::new(db, &self.outer_table, catalog)?;
        let mut inner = TupleScanner::new(db, &self.inner_table, catalog)?;
        let left_schema  = outer.schema.clone();
        let right_schema = inner.schema.clone();
        let mut result = JoinResult::new(&left_schema, &right_schema, &self.outer_table, &self.inner_table);

        match self.join_type {
            JoinType::Cross => {
                while let Some(o) = outer.next_tuple() {
                    inner.reset();
                    while let Some(i) = inner.next_tuple() {
                        result.add(Tuple::merge(&o, &i));
                    }
                }
            }
            JoinType::Inner => {
                while let Some(o) = outer.next_tuple() {
                    inner.reset();
                    while let Some(i) = inner.next_tuple() {
                        if evaluate_conditions(&self.conditions, &o, &i) {
                            result.add(Tuple::merge(&o, &i));
                        }
                    }
                }
            }
            JoinType::LeftOuter => {
                while let Some(o) = outer.next_tuple() {
                    inner.reset();
                    let mut matched = false;
                    while let Some(i) = inner.next_tuple() {
                        if evaluate_conditions(&self.conditions, &o, &i) {
                            result.add(Tuple::merge(&o, &i));
                            matched = true;
                        }
                    }
                    if !matched {
                        result.add(Tuple::merge(&o, &Tuple::null_tuple(&right_schema)));
                    }
                }
            }
            JoinType::RightOuter => {
                while let Some(i) = inner.next_tuple() {
                    outer.reset();
                    let mut matched = false;
                    while let Some(o) = outer.next_tuple() {
                        if evaluate_conditions(&self.conditions, &o, &i) {
                            result.add(Tuple::merge(&o, &i));
                            matched = true;
                        }
                    }
                    if !matched {
                        result.add(Tuple::merge(&Tuple::null_tuple(&left_schema), &i));
                    }
                }
            }
            JoinType::FullOuter => {
                let mut right_matched: HashSet<usize> = HashSet::new();
                while let Some(o) = outer.next_tuple() {
                    inner.reset();
                    let mut left_matched = false;
                    let mut j = 0;
                    while let Some(i) = inner.next_tuple() {
                        if evaluate_conditions(&self.conditions, &o, &i) {
                            result.add(Tuple::merge(&o, &i));
                            left_matched = true;
                            right_matched.insert(j);
                        }
                        j += 1;
                    }
                    if !left_matched {
                        result.add(Tuple::merge(&o, &Tuple::null_tuple(&right_schema)));
                    }
                }
                inner.reset();
                let mut j = 0;
                while let Some(i) = inner.next_tuple() {
                    if !right_matched.contains(&j) {
                        result.add(Tuple::merge(&Tuple::null_tuple(&left_schema), &i));
                    }
                    j += 1;
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("{} not implemented in NLJ", self.join_type),
                ));
            }
        }

        Ok(result)
    }

    // ── Block NLJ ────────────────────────────────────────────────────

    fn execute_block(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        let mut outer = TupleScanner::new(db, &self.outer_table, catalog)?;
        let mut inner = TupleScanner::new(db, &self.inner_table, catalog)?;
        let left_schema  = outer.schema.clone();
        let right_schema = inner.schema.clone();
        let mut result = JoinResult::new(&left_schema, &right_schema, &self.outer_table, &self.inner_table);

        let chunk_capacity = self.block_size * 100;

        match self.join_type {
            JoinType::Inner | JoinType::Cross
            | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                let mut right_matched: HashSet<usize> = HashSet::new();

                loop {
                    // Load one chunk of outer tuples.
                    let mut chunk = Vec::with_capacity(chunk_capacity);
                    for _ in 0..chunk_capacity {
                        match outer.next_tuple() {
                            Some(t) => chunk.push(t),
                            None    => break,
                        }
                    }
                    if chunk.is_empty() {
                        break;
                    }

                    inner.reset();
                    let mut outer_matched = vec![false; chunk.len()];
                    let mut inner_idx = 0usize;

                    while let Some(i) = inner.next_tuple() {
                        for (idx, o) in chunk.iter().enumerate() {
                            if self.join_type == JoinType::Cross {
                                result.add(Tuple::merge(o, &i));
                            } else if evaluate_conditions(&self.conditions, o, &i) {
                                result.add(Tuple::merge(o, &i));
                                outer_matched[idx] = true;
                                right_matched.insert(inner_idx);
                            }
                        }
                        inner_idx += 1;
                    }

                    // Emit unmatched outer rows for LEFT / FULL.
                    if self.join_type == JoinType::LeftOuter || self.join_type == JoinType::FullOuter {
                        for (idx, o) in chunk.iter().enumerate() {
                            if !outer_matched[idx] {
                                result.add(Tuple::merge(o, &Tuple::null_tuple(&right_schema)));
                            }
                        }
                    }
                }

                // Emit unmatched inner rows for RIGHT / FULL.
                if self.join_type == JoinType::RightOuter || self.join_type == JoinType::FullOuter {
                    inner.reset();
                    let mut inner_idx = 0usize;
                    while let Some(i) = inner.next_tuple() {
                        if !right_matched.contains(&inner_idx) {
                            result.add(Tuple::merge(&Tuple::null_tuple(&left_schema), &i));
                        }
                        inner_idx += 1;
                    }
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("{} not implemented in Block NLJ", self.join_type),
                ));
            }
        }

        Ok(result)
    }
}
