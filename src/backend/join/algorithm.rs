//! Algorithm metadata and physical execution trait.

use std::fmt;

/// Static metadata describing a join algorithm's capabilities.
#[derive(Debug, Clone)]
pub struct AlgorithmMetadata {
    pub name: String,
    pub supports_equi_join: bool,
    pub supports_non_equi_join: bool,
    pub supports_outer_join: bool,
    pub supports_semi_join: bool,
    pub supports_anti_join: bool,
    pub supports_cross_join: bool,
    pub min_memory_pages: usize,
    pub is_blocking: bool,
    pub is_pipelined: bool,
    pub requires_sorting: bool,
    pub can_use_index: bool,
}

impl AlgorithmMetadata {
    pub fn for_bnlj() -> Self {
        AlgorithmMetadata {
            name: "Block Nested Loop Join".into(),
            supports_equi_join: true,
            supports_non_equi_join: true,
            supports_outer_join: true,
            supports_semi_join: true,
            supports_anti_join: true,
            supports_cross_join: true,
            min_memory_pages: 3,
            is_blocking: false,
            is_pipelined: false,
            requires_sorting: false, can_use_index: false,
        }
    }

    pub fn for_inlj() -> Self {
        AlgorithmMetadata {
            name: "Indexed Nested Loop Join".into(),
            supports_equi_join: true,
            supports_non_equi_join: false,
            supports_outer_join: true,
            supports_semi_join: true,
            supports_anti_join: true,
            supports_cross_join: false,
            min_memory_pages: 2,
            is_blocking: false,
            is_pipelined: true,
            requires_sorting: false,
            can_use_index: true,
        }
    }

    pub fn for_smj() -> Self {
        AlgorithmMetadata {
            name: "Sort-Merge Join".into(),
            supports_equi_join: true,
            supports_non_equi_join: false,
            supports_outer_join: true,
            supports_semi_join: true,
            supports_anti_join: true,
            supports_cross_join: false,
            min_memory_pages: 10,
            is_blocking: true,
            is_pipelined: true,
            requires_sorting: true,
            can_use_index: false,
        }
    }

    pub fn for_hash_join() -> Self {
        AlgorithmMetadata {
            name: "Hash Join (Family)".into(),
            supports_equi_join: true,
            supports_non_equi_join: false,
            supports_outer_join: true,
            supports_semi_join: true,
            supports_anti_join: true,
            supports_cross_join: false,
            min_memory_pages: 5,
            is_blocking: true,
            is_pipelined: false,
            requires_sorting: false,
            can_use_index: false,
        }
    }

    pub fn for_symmetric_hash_join() -> Self {
        AlgorithmMetadata {
            name: "Symmetric Hash Join".into(),
            supports_equi_join: true,
            supports_non_equi_join: false,
            supports_outer_join: true,
            supports_semi_join: true,
            supports_anti_join: true,
            supports_cross_join: false,
            min_memory_pages: 10,
            is_blocking: false,
            is_pipelined: true,
            requires_sorting: false,
            can_use_index: false,
        }
    }

    /// Check whether this algorithm supports the given join type and condition mix.
    pub fn can_execute(&self, join_type: super::JoinType, _has_equi: bool, has_non_equi: bool) -> bool {
        match join_type {
            super::JoinType::Inner | super::JoinType::LeftOuter
            | super::JoinType::RightOuter | super::JoinType::FullOuter => {
                if has_non_equi { self.supports_non_equi_join }
                else { self.supports_equi_join || self.supports_non_equi_join }
            }
            super::JoinType::Cross    => self.supports_cross_join,
            super::JoinType::SemiJoin => self.supports_semi_join,
            super::JoinType::AntiJoin => self.supports_anti_join,
            super::JoinType::Natural  => self.supports_equi_join,
            super::JoinType::Lateral  => self.name.contains("Nested Loop"),
        }
    }
}

/// Trait for a physical join executor.
pub trait JoinAlgorithm: Send + Sync {
    fn metadata(&self) -> &AlgorithmMetadata;
    fn execute(&self) -> Result<super::JoinResult, std::io::Error>;
    fn estimated_cost(&self) -> f64;
    fn estimated_output_rows(&self) -> u64;
}

impl fmt::Debug for dyn JoinAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JoinAlgorithm: {}", self.metadata().name)
    }
}

/// Post-execution statistics for a join.
#[derive(Debug, Clone)]
pub struct JoinExecutionStats {
    pub algorithm_name: String,
    pub actual_cost_io: u64,
    pub actual_cost_cpu_us: u64,
    pub output_rows: u64,
    pub input_left_rows: u64,
    pub input_right_rows: u64,
    pub memory_used_pages: usize,
    pub estimated_vs_actual_cost_ratio: f64,
}

impl fmt::Display for JoinExecutionStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f, "{}: {} I/Os, {} CPU μs, {} rows (est/actual: {:.2}x)",
            self.algorithm_name, self.actual_cost_io, self.actual_cost_cpu_us,
            self.output_rows, self.estimated_vs_actual_cost_ratio
        )
    }
}
