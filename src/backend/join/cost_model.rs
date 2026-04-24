//! Cost model for join algorithm selection.
//!
//! Implements I/O-cost formulas for every physical join strategy:
//! SimpleNLJ, BNLJ, INLJ, SMJ, In-Memory Hash, Grace Hash, Hybrid Hash,
//! Symmetric Hash, and Broadcast Hash.

use std::fmt;

// ── Cardinality Estimator ────────────────────────────────────────────

/// Estimates the output cardinality of a join.
#[derive(Debug, Clone)]
pub struct CardinalityEstimator {
    pub left_cardinality:  u64,
    pub right_cardinality: u64,
    pub join_selectivity:  f64,
}

impl CardinalityEstimator {
    pub fn new(left: u64, right: u64, selectivity: f64) -> Self {
        CardinalityEstimator {
            left_cardinality:  left,
            right_cardinality: right,
            join_selectivity:  selectivity.clamp(0.0, 1.0),
        }
    }

    /// Output rows ≈ |L| × |R| × selectivity (clamped to u64::MAX).
    pub fn estimated_output_rows(&self) -> u64 {
        let cross = (self.left_cardinality as f64) * (self.right_cardinality as f64);
        (cross * self.join_selectivity).min(u64::MAX as f64) as u64
    }
}

// ── Join Cost ────────────────────────────────────────────────────────

/// Itemised I/O cost breakdown for a single join plan.
#[derive(Debug, Clone)]
pub struct JoinCost {
    pub algorithm: String,
    pub cost_component_scan: f64,
    pub cost_component_build: f64,
    pub cost_component_probe: f64,
    pub cost_component_sort: f64,
    pub cost_component_partition: f64,
    pub total_cost: f64,
    pub memory_required: usize,
    pub io_passes: u32,
    pub can_pipeline: bool,
    pub notes: String,
}

impl fmt::Display for JoinCost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {:.0} I/Os (scan:{:.0} build:{:.0} probe:{:.0} sort:{:.0} part:{:.0}) | {} mem pages",
            self.algorithm, self.total_cost,
            self.cost_component_scan, self.cost_component_build,
            self.cost_component_probe, self.cost_component_sort,
            self.cost_component_partition, self.memory_required,
        )
    }
}

// ── Skew Estimate ────────────────────────────────────────────────────

/// Skew estimate for hash-join partitions.
#[derive(Debug, Clone)]
pub struct SkewEstimate {
    pub max_partition_ratio: f64,
    pub is_skewed: bool,
    pub fallback_algorithm: Option<String>,
    pub skew_factor: f64,
}

impl SkewEstimate {
    pub fn new() -> Self {
        SkewEstimate { max_partition_ratio: 1.0, is_skewed: false, fallback_algorithm: None, skew_factor: 1.0 }
    }

    /// Estimate skew from the ratio of max-partition to average-partition size.
    pub fn estimate_skew(total_rows: u64, num_partitions: usize, max_partition_estimate: u64) -> Self {
        if num_partitions == 0 {
            return Self::new();
        }
        let avg = (total_rows as f64) / (num_partitions as f64);
        let ratio = if avg > 0.0 { (max_partition_estimate as f64) / avg } else { 1.0 };
        let skewed = ratio > 1.5;
        SkewEstimate {
            max_partition_ratio: ratio,
            is_skewed: skewed,
            fallback_algorithm: if skewed { Some("Hybrid Hash Join".into()) } else { None },
            skew_factor: ratio,
        }
    }
}

// ── Cost Model ───────────────────────────────────────────────────────

/// Static cost-model calculator (stateless).
pub struct CostModel;

impl CostModel {
    /// Simple NLJ: `B_outer + N_outer × B_inner`.
    pub fn simple_nlj_cost(outer_pages: f64, inner_pages: f64, outer_rows: u64, _inner_rows: u64) -> JoinCost {
        let scan  = outer_pages;
        let probe = (outer_rows as f64) * inner_pages;
        JoinCost {
            algorithm: "Simple NLJ".into(),
            cost_component_scan: scan,
            cost_component_build: 0.0,
            cost_component_probe: probe,
            cost_component_sort: 0.0,
            cost_component_partition: 0.0,
            total_cost: scan + probe,
            memory_required: 3,
            io_passes: outer_rows as u32,
            can_pipeline: false,
            notes: format!("Outer: {:.0} pages ({} rows)", outer_pages, outer_rows),
        }
    }

    /// BNLJ: `B_outer + ⌈B_outer / (M−2)⌉ × B_inner`.
    pub fn bnlj_cost(outer_pages: f64, inner_pages: f64, memory_pages: usize, _outer_rows: u64, _inner_rows: u64) -> JoinCost {
        let m = (memory_pages as f64).max(2.0);
        let buffer = (m - 2.0).max(1.0);
        let chunks = (outer_pages / buffer).ceil();
        let scan  = outer_pages;
        let probe = chunks * inner_pages;
        JoinCost {
            algorithm: "Block NLJ".into(),
            cost_component_scan: scan,
            cost_component_build: 0.0,
            cost_component_probe: probe,
            cost_component_sort: 0.0,
            cost_component_partition: 0.0,
            total_cost: scan + probe,
            memory_required: memory_pages,
            io_passes: (chunks as u32) + 1,
            can_pipeline: false,
            notes: format!("{:.0} outer chunks, {:.0} inner rescans", chunks, chunks),
        }
    }

    /// INLJ: `B_outer + N_outer × (depth + CF × data_pages)`.
    pub fn inlj_cost(outer_pages: f64, index_depth: f64, outer_rows: u64, clustering_factor: f64, data_page_fetch: f64) -> JoinCost {
        let scan  = outer_pages;
        let per   = index_depth + (clustering_factor * data_page_fetch);
        let probe = (outer_rows as f64) * per;
        let mem = if clustering_factor < 0.3 { 4 } else { 8 };
        JoinCost {
            algorithm: "Indexed NLJ".into(),
            cost_component_scan: scan,
            cost_component_build: 0.0,
            cost_component_probe: probe,
            cost_component_sort: 0.0,
            cost_component_partition: 0.0,
            total_cost: scan + probe,
            memory_required: mem,
            io_passes: outer_rows as u32,
            can_pipeline: false,
            notes: format!("depth={:.0} CF={:.2} fetch={:.1}", index_depth, clustering_factor, data_page_fetch),
        }
    }

    /// SMJ: `sort(outer) + sort(inner) + B_outer + B_inner`.
    pub fn smj_cost(outer_pages: f64, inner_pages: f64, memory_pages: usize, outer_presorted: bool, inner_presorted: bool) -> JoinCost {
        let sort_l = if outer_presorted { 0.0 } else { Self::external_sort_cost(outer_pages, memory_pages as f64) };
        let sort_r = if inner_presorted { 0.0 } else { Self::external_sort_cost(inner_pages, memory_pages as f64) };
        let merge  = outer_pages + inner_pages;
        JoinCost {
            algorithm: "Sort-Merge Join".into(),
            cost_component_scan: 0.0,
            cost_component_build: 0.0,
            cost_component_probe: merge,
            cost_component_sort: sort_l + sort_r,
            cost_component_partition: 0.0,
            total_cost: merge + sort_l + sort_r,
            memory_required: memory_pages,
            io_passes: if outer_presorted && inner_presorted { 1 } else { 3 },
            can_pipeline: true,
            notes: format!("L:{} R:{}", if outer_presorted {"sorted"} else {"unsorted"}, if inner_presorted {"sorted"} else {"unsorted"}),
        }
    }

    /// In-memory hash join: `B_outer + B_inner` if both fit in memory, else `None`.
    pub fn in_memory_hash_cost(outer_pages: f64, inner_pages: f64, memory_pages: usize) -> Option<JoinCost> {
        let needed = (outer_pages + inner_pages).ceil() as usize;
        if needed > memory_pages.saturating_sub(2) {
            return None;
        }
        Some(JoinCost {
            algorithm: "In-Memory Hash Join".into(),
            cost_component_scan: outer_pages + inner_pages,
            cost_component_build: outer_pages,
            cost_component_probe: inner_pages,
            cost_component_sort: 0.0,
            cost_component_partition: 0.0,
            total_cost: outer_pages + inner_pages,
            memory_required: needed,
            io_passes: 2,
            can_pipeline: false,
            notes: format!("{}/{} pages used", needed, memory_pages),
        })
    }

    /// Grace hash join: `3 × (B_outer + B_inner)`.
    pub fn grace_hash_cost(outer_pages: f64, inner_pages: f64, num_partitions: usize) -> JoinCost {
        let base = outer_pages + inner_pages;
        JoinCost {
            algorithm: "Grace Hash Join".into(),
            cost_component_scan: base,
            cost_component_build: base,
            cost_component_probe: base,
            cost_component_sort: 0.0,
            cost_component_partition: base,
            total_cost: 3.0 * base,
            memory_required: num_partitions.max(10),
            io_passes: 3,
            can_pipeline: false,
            notes: format!("{} partitions", num_partitions),
        }
    }

    /// Hybrid hash join: `≈ 2.1 × (B_outer + B_inner)` (partition 0 stays in memory).
    pub fn hybrid_hash_cost(outer_pages: f64, inner_pages: f64, memory_pages: usize, num_partitions: usize) -> JoinCost {
        let base = outer_pages + inner_pages;
        let total = 2.1 * base;
        JoinCost {
            algorithm: "Hybrid Hash Join".into(),
            cost_component_scan: base * 0.5,
            cost_component_build: base * 0.7,
            cost_component_probe: base * 0.7,
            cost_component_sort: 0.0,
            cost_component_partition: base * 0.5,
            total_cost: total,
            memory_required: memory_pages,
            io_passes: 2,
            can_pipeline: true,
            notes: format!("P0 in-memory, {} partitions to disk", num_partitions),
        }
    }

    /// Symmetric hash join: `B_outer + B_inner` (streaming, pipelined).
    pub fn symmetric_hash_cost(outer_pages: f64, inner_pages: f64, memory_pages: usize, outer_rows: u64, inner_rows: u64) -> JoinCost {
        let total = outer_pages + inner_pages;
        JoinCost {
            algorithm: "Symmetric Hash Join".into(),
            cost_component_scan: total,
            cost_component_build: 0.0,
            cost_component_probe: 0.0,
            cost_component_sort: 0.0,
            cost_component_partition: 0.0,
            total_cost: total,
            memory_required: memory_pages,
            io_passes: 1,
            can_pipeline: true,
            notes: format!("streaming {} + {} rows", outer_rows, inner_rows),
        }
    }

    /// Broadcast hash join (distributed): small table replicated to all nodes.
    pub fn broadcast_hash_cost(small_pages: f64, large_pages: f64, num_nodes: usize) -> JoinCost {
        let broadcast = small_pages * (num_nodes as f64);
        let total = broadcast + large_pages;
        JoinCost {
            algorithm: "Broadcast Hash Join".into(),
            cost_component_scan: broadcast,
            cost_component_build: broadcast,
            cost_component_probe: large_pages,
            cost_component_sort: 0.0,
            cost_component_partition: 0.0,
            total_cost: total,
            memory_required: (small_pages.ceil() as usize) * num_nodes,
            io_passes: 2,
            can_pipeline: true,
            notes: format!("broadcast to {} nodes", num_nodes),
        }
    }

    /// External merge sort cost: `2 × B × ⌈log_{M−1}(B/M)⌉`.
    fn external_sort_cost(pages: f64, memory_pages: f64) -> f64 {
        if pages <= memory_pages {
            return 0.0;
        }
        let m = memory_pages.max(2.0);
        let fan_in = (m - 1.0).max(2.0);
        let passes = ((pages / m).ln() / fan_in.ln()).ceil().max(1.0);
        2.0 * pages * passes
    }

    /// Convert tuple count to page count.
    pub fn estimate_pages(tuple_count: u64, bytes_per_tuple: usize, page_size: usize) -> f64 {
        ((tuple_count as usize * bytes_per_tuple + page_size - 1) / page_size) as f64
    }
}
