// Nested Loop Join executor (Simple + Block modes).
use std::io;

use crate::catalog::types::Catalog;

use super::{JoinType, NLJMode};
use super::condition::{JoinCondition, evaluate_conditions};
use super::scanner::TupleScanner;
use super::result::JoinResult;
use super::tuple::Tuple;

/// Nested Loop Join executor.
pub struct NLJExecutor {
    pub outer_table: String,
    pub inner_table: String,
    pub conditions: Vec<JoinCondition>,
    pub join_type: JoinType,
    pub block_size: usize,
    pub mode: NLJMode,
}

impl NLJExecutor {
    pub fn execute(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        match self.mode {
            NLJMode::Simple => self.execute_simple(db, catalog),
            NLJMode::Block => self.execute_block(db, catalog),
        }
    }

    fn execute_simple(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        let mut outer_scanner = TupleScanner::new(db, &self.outer_table, catalog)?;
        let mut inner_scanner = TupleScanner::new(db, &self.inner_table, catalog)?;

        let left_schema = outer_scanner.schema.clone();
        let right_schema = inner_scanner.schema.clone();

        let mut result = JoinResult::new(&left_schema, &right_schema, &self.outer_table, &self.inner_table);

        match self.join_type {
            JoinType::Cross => {
                // Cross join: every combination, no condition check
                let outer_tuples = outer_scanner.collect_all();
                for o in &outer_tuples {
                    inner_scanner.reset();
                    while let Some(i) = inner_scanner.next_tuple() {
                        result.add(Tuple::merge(o, &i));
                    }
                }
            }
            JoinType::Inner => {
                let outer_tuples = outer_scanner.collect_all();
                for o in &outer_tuples {
                    inner_scanner.reset();
                    while let Some(i) = inner_scanner.next_tuple() {
                        if evaluate_conditions(&self.conditions, o, &i) {
                            result.add(Tuple::merge(o, &i));
                        }
                    }
                }
            }
            JoinType::LeftOuter => {
                let outer_tuples = outer_scanner.collect_all();
                for o in &outer_tuples {
                    inner_scanner.reset();
                    let mut matched = false;
                    while let Some(i) = inner_scanner.next_tuple() {
                        if evaluate_conditions(&self.conditions, o, &i) {
                            result.add(Tuple::merge(o, &i));
                            matched = true;
                        }
                    }
                    if !matched {
                        let null_right = Tuple::null_tuple(&right_schema);
                        result.add(Tuple::merge(o, &null_right));
                    }
                }
            }
            JoinType::RightOuter => {
                // Gather all inner tuples, then for each inner check if any outer matches
                let outer_tuples = outer_scanner.collect_all();
                let inner_tuples = inner_scanner.collect_all();
                for i in &inner_tuples {
                    let mut matched = false;
                    for o in &outer_tuples {
                        if evaluate_conditions(&self.conditions, o, i) {
                            result.add(Tuple::merge(o, i));
                            matched = true;
                        }
                    }
                    if !matched {
                        let null_left = Tuple::null_tuple(&left_schema);
                        result.add(Tuple::merge(&null_left, i));
                    }
                }
            }
            JoinType::FullOuter => {
                let outer_tuples = outer_scanner.collect_all();
                let inner_tuples = inner_scanner.collect_all();
                let mut right_matched = vec![false; inner_tuples.len()];

                for o in &outer_tuples {
                    let mut left_matched = false;
                    for (j, i) in inner_tuples.iter().enumerate() {
                        if evaluate_conditions(&self.conditions, o, i) {
                            result.add(Tuple::merge(o, i));
                            left_matched = true;
                            right_matched[j] = true;
                        }
                    }
                    if !left_matched {
                        let null_right = Tuple::null_tuple(&right_schema);
                        result.add(Tuple::merge(o, &null_right));
                    }
                }
                // Emit unmatched right tuples
                for (j, i) in inner_tuples.iter().enumerate() {
                    if !right_matched[j] {
                        let null_left = Tuple::null_tuple(&left_schema);
                        result.add(Tuple::merge(&null_left, i));
                    }
                }
            }
        }

        Ok(result)
    }

    fn execute_block(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        let mut outer_scanner = TupleScanner::new(db, &self.outer_table, catalog)?;
        let mut inner_scanner = TupleScanner::new(db, &self.inner_table, catalog)?;

        let left_schema = outer_scanner.schema.clone();
        let right_schema = inner_scanner.schema.clone();

        let mut result = JoinResult::new(&left_schema, &right_schema, &self.outer_table, &self.inner_table);

        // Collect outer tuples in blocks
        let all_outer = outer_scanner.collect_all();
        let block_size_tuples = self.block_size * 100; // approximate tuples per block

        for chunk in all_outer.chunks(block_size_tuples.max(1)) {
            inner_scanner.reset();
            while let Some(i) = inner_scanner.next_tuple() {
                for o in chunk {
                    match self.join_type {
                        JoinType::Cross => {
                            result.add(Tuple::merge(o, &i));
                        }
                        _ => {
                            if evaluate_conditions(&self.conditions, o, &i) {
                                result.add(Tuple::merge(o, &i));
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }
}