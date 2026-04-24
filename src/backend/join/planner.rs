//! Cost-based join planner.
//!
//! Selects the cheapest physical join algorithm by computing I/O cost
//! estimates for all eligible candidates and returning the minimum.

use std::fmt;
use crate::catalog::types::Catalog;
use super::condition::JoinCondition;
use super::cost_model::{CostModel, JoinCost};
use super::join_order::MultiJoinOptimizer;
use super::{JoinAlgorithmType, JoinType};

/// A complete physical plan: algorithm + cost + output estimate.
#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    pub algorithm: JoinAlgorithmType,
    pub left_table: String,
    pub right_table: String,
    pub join_type: JoinType,
    pub estimated_cost: JoinCost,
    pub estimated_output_rows: u64,
    pub config: JoinPlannerConfig,
}

impl fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} JOIN {} ∣ {} ∣ {} ∣ ~{} rows",
            self.left_table, self.right_table,
            self.join_type, self.algorithm, self.estimated_output_rows)
    }
}

/// Planner configuration knobs.
#[derive(Debug, Clone)]
pub struct JoinPlannerConfig {
    pub available_memory_pages: usize,
    pub page_size: usize,
    pub enable_hash_join: bool,
    pub enable_sort_merge: bool,
    pub enable_index_scan: bool,
    pub use_bloom_filter: bool,
    pub bloom_filter_bits: usize,
    pub num_hash_partitions: usize,
    pub force_algorithm: Option<JoinAlgorithmType>,
    pub index_clustering_factor: f64,
    pub index_data_page_fetch: f64,
    pub num_distributed_nodes: usize,
}

impl Default for JoinPlannerConfig {
    fn default() -> Self {
        JoinPlannerConfig {
            available_memory_pages: 100,
            page_size: 4096,
            enable_hash_join: true,
            enable_sort_merge: true,
            enable_index_scan: true,
            use_bloom_filter: false,
            bloom_filter_bits: 1024 * 1024 * 8,
            num_hash_partitions: 8,
            force_algorithm: None,
            index_clustering_factor: 0.8,
            index_data_page_fetch: 1.0,
            num_distributed_nodes: 4,
        }
    }
}

/// Stateless cost-based join planner.
pub struct JoinPlanner;

impl JoinPlanner {
    /// Select the cheapest join algorithm for a two-table join.
    pub fn select_best_join(
        left: &str, right: &str,
        conditions: &[JoinCondition],
        join_type: JoinType,
        catalog: &Catalog,
        config: &JoinPlannerConfig,
    ) -> Result<PhysicalPlan, String> {
        if let Some(algo) = &config.force_algorithm {
            return Self::make_plan(left, right, conditions, join_type, *algo, catalog, config);
        }

        // Lateral joins must use NLJ.
        if join_type == JoinType::Lateral {
            return Self::make_plan(left, right, conditions, join_type, JoinAlgorithmType::BlockNLJ, catalog, config);
        }

        let is_equi     = Self::has_equi(conditions);
        let is_non_equi = Self::has_non_equi(conditions);
        let mut candidates = Vec::new();

        // BNLJ — always eligible.
        Self::try_add(&mut candidates, left, right, conditions, join_type, JoinAlgorithmType::BlockNLJ, catalog, config);

        if is_non_equi {
            if config.enable_sort_merge {
                Self::try_add(&mut candidates, left, right, conditions, join_type, JoinAlgorithmType::SortMergeJoin, catalog, config);
            }
        }

        if is_equi {
            if config.enable_index_scan && Self::has_index(right, conditions, catalog) {
                Self::try_add(&mut candidates, left, right, conditions, join_type, JoinAlgorithmType::IndexedNLJ, catalog, config);
            }
            if config.enable_hash_join {
                Self::try_add(&mut candidates, left, right, conditions, join_type, JoinAlgorithmType::InMemoryHashJoin, catalog, config);
                Self::try_add(&mut candidates, left, right, conditions, join_type, JoinAlgorithmType::GraceHashJoin, catalog, config);
                Self::try_add(&mut candidates, left, right, conditions, join_type, JoinAlgorithmType::HybridHashJoin, catalog, config);
            }
            if config.enable_sort_merge {
                Self::try_add(&mut candidates, left, right, conditions, join_type, JoinAlgorithmType::SortMergeJoin, catalog, config);
            }
            Self::try_add(&mut candidates, left, right, conditions, join_type, JoinAlgorithmType::SymmetricHashJoin, catalog, config);
        }

        if candidates.is_empty() {
            return Err("No viable join algorithms".into());
        }
        candidates.sort_by(|a, b| a.estimated_cost.total_cost.partial_cmp(&b.estimated_cost.total_cost).unwrap_or(std::cmp::Ordering::Equal));
        Ok(candidates.remove(0))
    }

    /// Multi-table join optimisation (Selinger DP for 3+ tables).
    pub fn select_best_multijoin(
        tables: &[&str], conditions: &[JoinCondition],
        join_type: JoinType, catalog: &Catalog, config: &JoinPlannerConfig,
    ) -> Result<super::join_order::JoinTreeNode, String> {
        if tables.len() < 2 { return Err("Need ≥2 tables".into()); }
        if tables.len() == 2 {
            let plan = Self::select_best_join(tables[0], tables[1], conditions, join_type, catalog, config)?;
            return Ok(super::join_order::JoinTreeNode::Join {
                left: Box::new(super::join_order::JoinTreeNode::Table { name: plan.left_table }),
                right: Box::new(super::join_order::JoinTreeNode::Table { name: plan.right_table }),
                algorithm: plan.algorithm, cost: plan.estimated_cost.total_cost,
            });
        }
        let mut opt = MultiJoinOptimizer::new(tables, conditions.to_vec(), join_type, catalog.clone(), config.available_memory_pages);
        opt.optimize(tables).ok_or_else(|| "DP optimisation failed".into())
    }

    // ── Internal helpers ─────────────────────────────────────────────

    fn try_add(
        out: &mut Vec<PhysicalPlan>, l: &str, r: &str,
        conds: &[JoinCondition], jt: JoinType, algo: JoinAlgorithmType,
        cat: &Catalog, cfg: &JoinPlannerConfig,
    ) {
        if let Ok(p) = Self::make_plan(l, r, conds, jt, algo, cat, cfg) { out.push(p); }
    }

    fn make_plan(
        left: &str, right: &str,
        conditions: &[JoinCondition], join_type: JoinType,
        algorithm: JoinAlgorithmType,
        catalog: &Catalog, config: &JoinPlannerConfig,
    ) -> Result<PhysicalPlan, String> {
        let (lp, lr) = Self::table_size(left, catalog)?;
        let (rp, rr) = Self::table_size(right, catalog)?;

        let sel = match join_type {
            JoinType::Cross => 1.0,
            JoinType::SemiJoin | JoinType::AntiJoin => (lr.min(rr) as f64 / lr.max(1) as f64).min(1.0),
            _ => 0.01,
        };
        let output = ((lr as f64 * rr as f64) * sel).ceil() as u64;

        let cost = match algorithm {
            JoinAlgorithmType::SimpleNLJ          => CostModel::simple_nlj_cost(lp, rp, lr, rr),
            JoinAlgorithmType::BlockNLJ           => CostModel::bnlj_cost(lp, rp, config.available_memory_pages, lr, rr),
            JoinAlgorithmType::IndexedNLJ         => {
                if !Self::has_index(right, conditions, catalog) { return Err("No index".into()); }
                CostModel::inlj_cost(lp, 4.0, lr, config.index_clustering_factor, config.index_data_page_fetch)
            }
            JoinAlgorithmType::SortMergeJoin      => {
                let ls = Self::is_sorted(left, conditions, catalog);
                let rs = Self::is_sorted(right, conditions, catalog);
                CostModel::smj_cost(lp, rp, config.available_memory_pages, ls, rs)
            }
            JoinAlgorithmType::InMemoryHashJoin   => CostModel::in_memory_hash_cost(lp, rp, config.available_memory_pages).ok_or("OOM")?,
            JoinAlgorithmType::GraceHashJoin      => CostModel::grace_hash_cost(lp, rp, config.num_hash_partitions),
            JoinAlgorithmType::HybridHashJoin     => CostModel::hybrid_hash_cost(lp, rp, config.available_memory_pages, config.num_hash_partitions),
            JoinAlgorithmType::SymmetricHashJoin  => CostModel::symmetric_hash_cost(lp, rp, config.available_memory_pages, lr, rr),
            JoinAlgorithmType::BroadcastHashJoin  => {
                let (s, b) = if lp < rp { (lp, rp) } else { (rp, lp) };
                CostModel::broadcast_hash_cost(s, b, config.num_distributed_nodes)
            }
            JoinAlgorithmType::ShuffleSortMergeJoin => {
                let n = config.num_distributed_nodes as f64;
                CostModel::smj_cost(lp / n, rp / n, config.available_memory_pages, false, false)
            }
            JoinAlgorithmType::AdaptiveJoin       => CostModel::bnlj_cost(lp, rp, config.available_memory_pages, lr, rr),
            JoinAlgorithmType::DirectJoin          => CostModel::in_memory_hash_cost(lp, rp, config.available_memory_pages)
                .unwrap_or_else(|| CostModel::grace_hash_cost(lp, rp, config.num_hash_partitions)),
        };

        Ok(PhysicalPlan {
            algorithm, left_table: left.into(), right_table: right.into(),
            join_type, estimated_cost: cost, estimated_output_rows: output, config: config.clone(),
        })
    }

    fn table_size(name: &str, catalog: &Catalog) -> Result<(f64, u64), String> {
        for (_db, db_meta) in &catalog.databases {
            if let Some(t) = db_meta.tables.get(name) {
                if t.page_count > 0 && t.row_count > 0 {
                    return Ok((t.page_count as f64, t.row_count));
                }
            }
        }
        Ok((100.0, 10_000))
    }

    fn has_equi(c: &[JoinCondition]) -> bool { c.iter().any(|c| c.is_equality()) }
    fn has_non_equi(c: &[JoinCondition]) -> bool { c.iter().any(|c| !c.is_equality()) }
    fn has_index(_t: &str, _c: &[JoinCondition], _cat: &Catalog) -> bool { false }
    fn is_sorted(_t: &str, _c: &[JoinCondition], _cat: &Catalog) -> bool { false }
}


