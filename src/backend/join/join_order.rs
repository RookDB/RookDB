//! Multi-way join ordering using Selinger dynamic programming.
//!
//! Given N tables and a set of join predicates, finds the cheapest
//! left-deep or bushy join tree by bottom-up enumeration.

use std::collections::HashMap;
use std::fmt;

use super::condition::JoinCondition;
use super::{JoinAlgorithmType, JoinType};
use crate::catalog::types::Catalog;

// ── Relation Set ─────────────────────────────────────────────────────

/// Sorted, deduplicated set of table names (hashable).
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct RelationSet {
    pub relations: Vec<String>,
}

impl RelationSet {
    pub fn new(tables: &[&str]) -> Self {
        let mut v: Vec<String> = tables.iter().map(|s| s.to_string()).collect();
        v.sort(); v.dedup();
        RelationSet { relations: v }
    }

    pub fn single(name: &str) -> Self {
        RelationSet { relations: vec![name.to_string()] }
    }

    pub fn size(&self) -> usize { self.relations.len() }

    pub fn union(&self, other: &RelationSet) -> RelationSet {
        let mut v = self.relations.clone();
        v.extend(other.relations.clone());
        v.sort(); v.dedup();
        RelationSet { relations: v }
    }

    pub fn contains(&self, name: &str) -> bool {
        self.relations.contains(&name.to_string())
    }

    pub fn is_disjoint(&self, other: &RelationSet) -> bool {
        self.relations.iter().all(|r| !other.contains(r))
    }

    /// Elements in `self` but not in `other`.
    pub fn minus(&self, other: &RelationSet) -> RelationSet {
        let v: Vec<String> = self.relations.iter().filter(|r| !other.relations.contains(r)).cloned().collect();
        RelationSet { relations: v }
    }
}

impl fmt::Display for RelationSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{{}}}", self.relations.join(", "))
    }
}

// ── Join Tree ────────────────────────────────────────────────────────

/// A node in the join tree (leaf = table, inner = join of two subtrees).
#[derive(Debug, Clone)]
pub enum JoinTreeNode {
    Table { name: String },
    Join { left: Box<JoinTreeNode>, right: Box<JoinTreeNode>, algorithm: JoinAlgorithmType, cost: f64 },
}

impl JoinTreeNode {
    pub fn relations(&self) -> RelationSet {
        match self {
            JoinTreeNode::Table { name } => RelationSet::single(name),
            JoinTreeNode::Join { left, right, .. } => left.relations().union(&right.relations()),
        }
    }

    pub fn total_cost(&self) -> f64 {
        match self {
            JoinTreeNode::Table { .. } => 0.0,
            JoinTreeNode::Join { left, right, cost, .. } => left.total_cost() + right.total_cost() + cost,
        }
    }
}

impl fmt::Display for JoinTreeNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinTreeNode::Table { name } => write!(f, "{}", name),
            JoinTreeNode::Join { left, right, algorithm, cost } =>
                write!(f, "({} {} {} [cost:{:.0}])", left, algorithm, right, cost),
        }
    }
}

// ── DP Entry ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct JoinPlanEntry {
    pub relations: RelationSet,
    pub tree: JoinTreeNode,
    pub cost: f64,
    pub rows: u64,
}

// ── Optimizer ────────────────────────────────────────────────────────

/// Selinger-style DP optimizer for multi-way joins.
pub struct MultiJoinOptimizer {
    dp: HashMap<RelationSet, JoinPlanEntry>,
    pub join_conditions: Vec<JoinCondition>,
    pub join_type: JoinType,
    pub catalog: Catalog,
    pub memory_pages: usize,
}

impl MultiJoinOptimizer {
    pub fn new(
        _tables: &[&str], conditions: Vec<JoinCondition>,
        join_type: JoinType, catalog: Catalog, memory_pages: usize,
    ) -> Self {
        MultiJoinOptimizer { dp: HashMap::new(), join_conditions: conditions, join_type, catalog, memory_pages }
    }

    /// Run the DP algorithm and return the cheapest join tree.
    pub fn optimize(&mut self, tables: &[&str]) -> Option<JoinTreeNode> {
        if tables.is_empty() { return None; }
        if tables.len() == 1 {
            return Some(JoinTreeNode::Table { name: tables[0].to_string() });
        }

        // Base case: single-table entries.
        for t in tables {
            let rs = RelationSet::single(t);
            self.dp.insert(rs.clone(), JoinPlanEntry {
                relations: rs, tree: JoinTreeNode::Table { name: t.to_string() },
                cost: 0.0, rows: self.estimate_rows(t),
            });
        }

        // Bottom-up: build plans for size 2..N.
        for size in 2..=tables.len() {
            self.build_for_size(size, tables);
        }

        let final_set = RelationSet::new(tables);
        self.dp.get(&final_set).map(|e| e.tree.clone())
    }

    fn build_for_size(&mut self, size: usize, all: &[&str]) {
        let subsets = Self::subsets(size, all);
        for current in &subsets {
            let tabs: Vec<&str> = current.relations.iter().map(|s| s.as_str()).collect();
            for split in 1..size {
                let lefts = Self::subsets(split, &tabs);
                for left_rel in &lefts {
                    let right_rel = current.minus(left_rel);
                    if right_rel.size() == 0 { continue; }
                    let (lp, rp) = match (self.dp.get(left_rel).cloned(), self.dp.get(&right_rel).cloned()) {
                        (Some(l), Some(r)) => (l, r),
                        _ => continue,
                    };
                    let joined = left_rel.union(&right_rel);
                    let (jcost, orows) = self.cost(&lp, &rp);
                    let total = lp.cost + rp.cost + jcost;

                    let dominated = self.dp.get(&joined).map_or(false, |e| total >= e.cost);
                    if dominated { continue; }

                    let tree = JoinTreeNode::Join {
                        left: Box::new(lp.tree), right: Box::new(rp.tree),
                        algorithm: JoinAlgorithmType::BlockNLJ, cost: jcost,
                    };
                    self.dp.insert(joined.clone(), JoinPlanEntry { relations: joined, tree, cost: total, rows: orows });
                }
            }
        }
    }

    fn subsets(size: usize, tables: &[&str]) -> Vec<RelationSet> {
        let mut out = Vec::new();
        Self::subsets_rec(size, tables, 0, Vec::new(), &mut out);
        out
    }

    fn subsets_rec(size: usize, tables: &[&str], start: usize, cur: Vec<&str>, out: &mut Vec<RelationSet>) {
        if cur.len() == size { out.push(RelationSet::new(&cur)); return; }
        for i in start..tables.len() {
            let mut next = cur.clone();
            next.push(tables[i]);
            Self::subsets_rec(size, tables, i + 1, next, out);
        }
    }

    fn cost(&self, left: &JoinPlanEntry, right: &JoinPlanEntry) -> (f64, u64) {
        let lp = self.pages(left.rows);
        let rp = self.pages(right.rows);
        let m = (self.memory_pages as f64).max(2.0);
        let buf = (m - 2.0).max(1.0);
        let chunks = (lp / buf).ceil();
        let cost = lp + chunks * rp;
        let rows = ((left.rows as f64 * right.rows as f64) * 0.01).ceil() as u64;
        (cost, rows)
    }

    fn pages(&self, rows: u64) -> f64 {
        let avg = self.avg_row_size().unwrap_or(128);
        ((rows as usize * avg + 4095) / 4096) as f64
    }

    fn avg_row_size(&self) -> Option<usize> {
        for (_, db) in &self.catalog.databases {
            for (_, t) in &db.tables {
                if t.avg_row_size > 0 { return Some(t.avg_row_size); }
            }
        }
        None
    }

    fn estimate_rows(&self, table: &str) -> u64 {
        for (_, db) in &self.catalog.databases {
            if let Some(t) = db.tables.get(table) {
                if t.row_count > 0 { return t.row_count; }
            }
        }
        1000
    }
}

// ── Tests ────────────────────────────────────────────────────────────


