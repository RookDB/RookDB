//! Cost model for Cost-Based Optimization (CBO).
//!
//! Provides mathematical cost estimation for access paths:
//! - Sequential Scan: $C_{seq} = N_{pages} \cdot PAGE\_READ\_COST + N_{tuples} \cdot CPU\_TUPLE\_COST$
//! - Index Scan (unclustered): $C_{idx} = (tree\_depth + \min(E, N_{pages})) \cdot RANDOM\_PAGE\_COST + E \cdot (CPU\_INDEX\_TUPLE\_COST + CPU\_TUPLE\_COST)$
//!
//! Standard database optimizer constants:
//! - `PAGE_READ_COST = 1.0` (sequential disk read)
//! - `RANDOM_PAGE_COST = 4.0` (random disk seek/read)
//! - `CPU_TUPLE_COST = 0.01` (tuple decoding and expression evaluation)
//! - `CPU_INDEX_TUPLE_COST = 0.005` (B+tree search / key comparison)

#[derive(Debug, Clone, Copy)]
pub struct CostModel;

impl CostModel {
    pub const PAGE_READ_COST: f64 = 1.0;
    pub const RANDOM_PAGE_COST: f64 = 4.0;
    pub const CPU_TUPLE_COST: f64 = 0.01;
    pub const CPU_INDEX_TUPLE_COST: f64 = 0.005;

    /// Cost of a full table sequential scan.
    pub fn cost_seq_scan(data_pages: u32, total_tuples: u64) -> f64 {
        let pages = data_pages.max(1) as f64;
        let tuples = total_tuples as f64;
        pages * Self::PAGE_READ_COST + tuples * Self::CPU_TUPLE_COST
    }

    /// Cost of an unclustered B+Tree index scan given selectivity (0.0 to 1.0).
    pub fn cost_index_scan(data_pages: u32, total_tuples: u64, selectivity: f64) -> f64 {
        let pages = data_pages.max(1) as f64;
        let total_tuples_f = total_tuples as f64;
        let matching_tuples = (selectivity.clamp(0.0, 1.0) * total_tuples_f).max(1.0);

        // B+ tree root-to-leaf traversal depth
        let tree_depth = if total_tuples <= 100 {
            1.0
        } else if total_tuples <= 10_000 {
            2.0
        } else if total_tuples <= 1_000_000 {
            3.0
        } else {
            4.0
        };
        let btree_cost = tree_depth * Self::RANDOM_PAGE_COST;

        // Unclustered heap page fetches: each matching index leaf entry
        // causes a random heap fetch, capped by the total data pages in the relation.
        let heap_pages_fetched = matching_tuples.min(pages);
        let heap_cost = heap_pages_fetched * Self::RANDOM_PAGE_COST;

        let cpu_cost = matching_tuples * (Self::CPU_INDEX_TUPLE_COST + Self::CPU_TUPLE_COST);

        btree_cost + heap_cost + cpu_cost
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_lookup_cheaper_than_seq_scan() {
        let pages = 100u32;
        let tuples = 10_000u64;
        let c_seq = CostModel::cost_seq_scan(pages, tuples);
        // Selectivity for 1 row: 1/10000 = 0.0001
        let c_idx = CostModel::cost_index_scan(pages, tuples, 0.0001);

        assert!(c_idx < c_seq, "Index point lookup should be cheaper: idx={}, seq={}", c_idx, c_seq);
    }

    #[test]
    fn test_high_selectivity_seq_scan_cheaper_than_index_scan() {
        let pages = 100u32;
        let tuples = 10_000u64;
        let c_seq = CostModel::cost_seq_scan(pages, tuples);
        // Selectivity for 35% of table: 0.35
        let c_idx = CostModel::cost_index_scan(pages, tuples, 0.35);

        assert!(c_seq < c_idx, "Seq scan should be cheaper at 35% selectivity: seq={}, idx={}", c_seq, c_idx);
    }
}
