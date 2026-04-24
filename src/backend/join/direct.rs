//! Direct Join executor.
//!
//! Loads both tables completely into memory and performs an in-memory
//! nested loop join. Extremely fast for small datasets, but crashes
//! on out-of-memory for large datasets.

use std::io;

use crate::catalog::types::Catalog;
use super::condition::{JoinCondition, evaluate_conditions};
use super::result::JoinResult;
use super::scanner::TupleScanner;
use super::tuple::Tuple;
use super::JoinType;

pub struct DirectJoinExecutor {
    pub outer_table: String,
    pub inner_table: String,
    pub conditions: Vec<JoinCondition>,
    pub join_type: JoinType,
}

impl DirectJoinExecutor {
    pub fn execute(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        let mut oscan = TupleScanner::new(db, &self.outer_table, catalog)?;
        let mut iscan = TupleScanner::new(db, &self.inner_table, catalog)?;

        let os = oscan.schema.clone();
        let is = iscan.schema.clone();

        let mut outer_tuples = Vec::new();
        while let Some(t) = oscan.next_tuple() {
            outer_tuples.push(t);
        }

        let mut inner_tuples = Vec::new();
        while let Some(t) = iscan.next_tuple() {
            inner_tuples.push(t);
        }

        let mut result = JoinResult::new(&os, &is, &self.outer_table, &self.inner_table);

        let mut outer_matched = vec![false; outer_tuples.len()];
        let mut inner_matched = vec![false; inner_tuples.len()];

        for (o_idx, o) in outer_tuples.iter().enumerate() {
            for (i_idx, i) in inner_tuples.iter().enumerate() {
                if self.join_type == JoinType::Cross {
                    result.add(Tuple::merge(o, i));
                } else if evaluate_conditions(&self.conditions, o, i) {
                    result.add(Tuple::merge(o, i));
                    outer_matched[o_idx] = true;
                    inner_matched[i_idx] = true;
                }
            }
        }

        // Handle outer joins
        if matches!(self.join_type, JoinType::LeftOuter | JoinType::FullOuter) {
            for (o_idx, o) in outer_tuples.iter().enumerate() {
                if !outer_matched[o_idx] {
                    result.add(Tuple::merge(o, &Tuple::null_tuple(&is)));
                }
            }
        }

        if matches!(self.join_type, JoinType::RightOuter | JoinType::FullOuter) {
            for (i_idx, i) in inner_tuples.iter().enumerate() {
                if !inner_matched[i_idx] {
                    result.add(Tuple::merge(&Tuple::null_tuple(&os), i));
                }
            }
        }

        Ok(result)
    }
}
