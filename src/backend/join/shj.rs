//! Symmetric Hash Join executor.
//!
//! Maintains in-memory hash tables for both relations concurrently.
//! Reads tuples from both sides alternately, probing the opposite
//! hash table and then inserting into its own hash table.
//! Excellent for pipelining and streaming results.

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::io;

use crate::catalog::types::{Catalog, Column};
use super::condition::{JoinCondition, evaluate_conditions};
use super::result::JoinResult;
use super::scanner::TupleScanner;
use super::tuple::{ColumnValue, Tuple};
use super::JoinType;

pub struct SymmetricHashJoinExecutor {
    pub left_table: String,
    pub right_table: String,
    pub conditions: Vec<JoinCondition>,
    pub join_type: JoinType,
}

impl SymmetricHashJoinExecutor {
    pub fn execute(&self, db: &str, catalog: &Catalog) -> io::Result<JoinResult> {
        let mut lscan = TupleScanner::new(db, &self.left_table, catalog)?;
        let mut rscan = TupleScanner::new(db, &self.right_table, catalog)?;

        let ls = lscan.schema.clone();
        let rs = rscan.schema.clone();

        let (lcol, rcol) = self.resolve_columns();

        let mut result = JoinResult::new(&ls, &rs, &self.left_table, &self.right_table);

        // We only support Equi-joins natively for hash joins
        if self.conditions.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Symmetric Hash Join requires at least one condition"));
        }

        let mut left_ht: HashMap<u64, Vec<Tuple>> = HashMap::new();
        let mut right_ht: HashMap<u64, Vec<Tuple>> = HashMap::new();

        let mut left_matched: HashSet<Vec<u8>> = HashSet::new();
        let mut right_matched: HashSet<Vec<u8>> = HashSet::new();

        let mut left_tuples_stored = Vec::new();
        let mut right_tuples_stored = Vec::new();

        let mut left_done = false;
        let mut right_done = false;

        while !left_done || !right_done {
            // Process one tuple from left
            if !left_done {
                if let Some(l_tup) = lscan.next_tuple() {
                    let key = self.hash_value(l_tup.get_field(&lcol));
                    
                    // Probe right
                    let mut _matched = false;
                    if let Some(matches) = right_ht.get(&key) {
                        for r_tup in matches {
                            if evaluate_conditions(&self.conditions, &l_tup, r_tup) {
                                result.add(Tuple::merge(&l_tup, r_tup));
                                _matched = true;
                                left_matched.insert(self.ser(&l_tup, &ls));
                                right_matched.insert(self.ser(r_tup, &rs));
                            }
                        }
                    }
                    
                    left_ht.entry(key).or_default().push(l_tup.clone());
                    left_tuples_stored.push(l_tup);
                } else {
                    left_done = true;
                }
            }

            // Process one tuple from right
            if !right_done {
                if let Some(r_tup) = rscan.next_tuple() {
                    let key = self.hash_value(r_tup.get_field(&rcol));
                    
                    // Probe left
                    let mut _matched = false;
                    if let Some(matches) = left_ht.get(&key) {
                        for l_tup in matches {
                            if evaluate_conditions(&self.conditions, l_tup, &r_tup) {
                                result.add(Tuple::merge(l_tup, &r_tup));
                                _matched = true;
                                left_matched.insert(self.ser(l_tup, &ls));
                                right_matched.insert(self.ser(&r_tup, &rs));
                            }
                        }
                    }
                    
                    right_ht.entry(key).or_default().push(r_tup.clone());
                    right_tuples_stored.push(r_tup);
                } else {
                    right_done = true;
                }
            }
        }

        // Handle outer joins at the end, since we now know everything that didn't match
        if matches!(self.join_type, JoinType::LeftOuter | JoinType::FullOuter) {
            for l_tup in &left_tuples_stored {
                if !left_matched.contains(&self.ser(l_tup, &ls)) {
                    result.add(Tuple::merge(l_tup, &Tuple::null_tuple(&rs)));
                }
            }
        }

        if matches!(self.join_type, JoinType::RightOuter | JoinType::FullOuter) {
            for r_tup in &right_tuples_stored {
                if !right_matched.contains(&self.ser(r_tup, &rs)) {
                    result.add(Tuple::merge(&Tuple::null_tuple(&ls), r_tup));
                }
            }
        }

        Ok(result)
    }

    fn resolve_columns(&self) -> (String, String) {
        let cond = &self.conditions[0];
        if cond.left_table == self.left_table {
            (cond.left_col.clone(), cond.right_col.clone())
        } else {
            (cond.right_col.clone(), cond.left_col.clone())
        }
    }

    fn hash_value(&self, val: Option<&ColumnValue>) -> u64 {
        let mut hasher = DefaultHasher::new();
        if let Some(v) = val {
            match v {
                ColumnValue::Int(i) => i.hash(&mut hasher),
                ColumnValue::Text(s) => s.hash(&mut hasher),
                ColumnValue::Null => 0.hash(&mut hasher),
            }
        } else {
            0.hash(&mut hasher);
        }
        hasher.finish()
    }

    fn ser(&self, t: &Tuple, schema: &[Column]) -> Vec<u8> {
        let mut out = Vec::new();
        for col in schema {
            match t.get_field(&col.name) {
                Some(ColumnValue::Int(i)) => out.extend_from_slice(&i.to_le_bytes()),
                Some(ColumnValue::Text(s)) => {
                    let mut b = s.as_bytes().to_vec();
                    if b.len() > 10 { b.truncate(10); }
                    else { b.extend(vec![b' '; 10 - b.len()]); }
                    out.extend_from_slice(&b);
                }
                _ => out.extend(vec![0; 10]),
            }
        }
        out
    }
}
