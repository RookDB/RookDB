//! Building an executable join from two relations and a condition.
//!
//! Everything that can be rejected is rejected here, before a single row is
//! read: unresolvable or ambiguous columns, incomparable key types, and
//! algorithms that do not implement the requested join type. An executor that
//! exists is an executor that will produce the join it was asked for.

use std::sync::Arc;

use crate::catalog::Table;
use crate::executor::selection::{Predicate, SelectionExecutor};

use super::algorithm::{JoinAlgorithm, JoinRequest, JoinType, spec_for};
use super::config::JoinConfig;
use super::error::JoinError;
use super::exec::hash::HashJoin;
use super::exec::nested_loop::{DEFAULT_BLOCK_ROWS, NestedLoopJoin};
use super::exec::sort_merge::SortMergeJoin;
use super::exec::symmetric_hash::SymmetricHashJoin;
use super::exec::{MatchEvaluator, RowStream};
use super::memory::MemoryAccountant;
use super::predicate::{JoinPredicate, SideResolver, split_conjuncts};
use super::schema::OutputSchema;
use super::source::{RowSource, TableRef, TableSource};
use super::spill::SpillScope;

/// Describes one join, and turns it into a running operator.
pub struct JoinBuilder {
    left: TableRef,
    right: TableRef,
    join_type: JoinType,
    condition: Option<Predicate>,
    block_rows: usize,
    algorithm: Option<JoinAlgorithm>,
    config: JoinConfig,
}

impl JoinBuilder {
    pub fn new(left: TableRef, right: TableRef, join_type: JoinType) -> Self {
        Self {
            left,
            right,
            join_type,
            condition: None,
            block_rows: DEFAULT_BLOCK_ROWS,
            algorithm: None,
            config: JoinConfig::resolve(),
        }
    }

    /// Force a specific algorithm instead of letting the builder choose.
    /// Validation still applies, so an algorithm that cannot serve the join
    /// type is refused rather than silently substituted.
    pub fn with_algorithm(mut self, algorithm: JoinAlgorithm) -> Self {
        self.algorithm = Some(algorithm);
        self
    }

    pub fn with_config(mut self, config: JoinConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_condition(mut self, condition: Predicate) -> Self {
        self.condition = Some(condition);
        self
    }

    /// Outer rows buffered per pass over the inner relation. One row makes
    /// this a simple nested-loop join; more makes it a blocked one.
    pub fn with_block_rows(mut self, rows: usize) -> Self {
        self.block_rows = rows.max(1);
        self
    }

    fn algorithm(&self) -> JoinAlgorithm {
        if let Some(algorithm) = self.algorithm {
            return algorithm;
        }
        if self.block_rows == 1 {
            JoinAlgorithm::SimpleNestedLoop
        } else {
            JoinAlgorithm::BlockNestedLoop
        }
    }

    /// The shape of the rows this join will produce.
    ///
    /// An outer join makes the *opposite* side's columns nullable: it is
    /// unmatched right rows that force NULLs into the left columns.
    pub fn output_schema(&self) -> Result<OutputSchema, JoinError> {
        let left = self.left.relation_schema();
        let right = self.right.relation_schema();

        if self.join_type.emits_left_only() {
            return Ok(OutputSchema::left_only(&left));
        }

        Ok(OutputSchema::concat(
            &left,
            &right,
            self.join_type.keeps_unmatched_right(),
            self.join_type.keeps_unmatched_left(),
        ))
    }

    /// Validate and start the join.
    pub fn execute(&self) -> Result<Box<dyn RowStream>, JoinError> {
        let left_relation = self.left.relation_schema();
        let right_relation = self.right.relation_schema();
        let resolver = SideResolver::new(&left_relation, &right_relation)?;
        let split = split_conjuncts(self.condition.as_ref(), &resolver, self.join_type)?;

        let request = JoinRequest {
            join_type: self.join_type,
            keys: &split.keys,
            has_residual: split.residual.is_some(),
            has_inner_index: false,
        };
        let spec = spec_for(self.algorithm()).validate(&request)?;

        let schema = Arc::new(self.output_schema()?);

        let left_source = TableSource::with_filter(
            &self.left,
            compile_filter(split.left_local.clone(), &self.left)?,
        )?;
        let right_source = TableSource::with_filter(
            &self.right,
            compile_filter(split.right_local.clone(), &self.right)?,
        )?;

        let residual = split
            .residual
            .clone()
            .map(|predicate| JoinPredicate::new(predicate, left_relation.len()));
        let evaluator = MatchEvaluator::new(split.keys.clone(), residual);

        // The left relation is the outer (probe) side and the right is the
        // inner (build) side, uniformly across every algorithm. That is what
        // makes unmatched-left rows streamable and unmatched-right rows a
        // post-pass, in all of them.
        let outer = left_source.open()?;

        match spec.algorithm() {
            JoinAlgorithm::SimpleNestedLoop | JoinAlgorithm::BlockNestedLoop => {
                // Asking for the simple variant means a block of one row;
                // otherwise the operator would claim to be simple while
                // blocking.
                let block_rows = if spec.algorithm() == JoinAlgorithm::SimpleNestedLoop {
                    1
                } else {
                    self.block_rows.max(2)
                };
                Ok(Box::new(NestedLoopJoin::new(
                    &spec,
                    evaluator,
                    outer,
                    Box::new(right_source),
                    schema,
                    block_rows,
                )))
            }
            JoinAlgorithm::Hash => {
                let budget = MemoryAccountant::new(self.config.work_memory_bytes);
                let scope = self.spill_scope()?;
                Ok(Box::new(HashJoin::new(
                    &spec,
                    evaluator,
                    outer,
                    Box::new(right_source),
                    schema,
                    budget,
                    scope,
                )))
            }
            JoinAlgorithm::SortMerge => {
                let budget = MemoryAccountant::new(self.config.work_memory_bytes);
                let scope = self.spill_scope()?;
                Ok(Box::new(SortMergeJoin::new(
                    &spec,
                    evaluator,
                    outer,
                    Box::new(right_source),
                    schema,
                    budget,
                    scope,
                )))
            }
            JoinAlgorithm::SymmetricHash => {
                let budget = MemoryAccountant::new(self.config.work_memory_bytes);
                Ok(Box::new(SymmetricHashJoin::new(
                    &spec,
                    evaluator,
                    outer,
                    Box::new(right_source),
                    schema,
                    budget,
                )))
            }
            other => Err(JoinError::plan(format!(
                "{} join is not available yet",
                other.name()
            ))),
        }
    }

    fn spill_scope(&self) -> Result<Arc<SpillScope>, JoinError> {
        SpillScope::create(&self.config.spill_root)
    }
}

/// Compile a single-relation conjunct into a scan filter.
fn compile_filter(
    predicate: Option<Predicate>,
    table: &TableRef,
) -> Result<Option<SelectionExecutor>, JoinError> {
    let Some(predicate) = predicate else {
        return Ok(None);
    };

    let schema = Table {
        columns: table.columns.clone(),
    };
    SelectionExecutor::new(predicate, schema)
        .map(Some)
        .map_err(|e| JoinError::plan(format!("cannot push a filter into '{}': {e}", table.alias)))
}
