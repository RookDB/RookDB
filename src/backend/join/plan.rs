//! Planning a join: choosing an algorithm, estimating what it will produce,
//! and rendering the result.
//!
//! Everything that can be rejected is rejected here, before a single row is
//! read: unresolvable or ambiguous columns, incomparable key types, and
//! algorithms that do not implement the requested join type. An executor that
//! exists is an executor that will produce the join it was asked for.
//!
//! Plans carry the confidence of the statistics they were built from, and
//! EXPLAIN prints it. An estimate derived from a measured distinct-value count
//! and one derived from a fallback guess should not look alike.

use std::fmt::Write as _;
use std::rc::Rc;
use std::sync::Arc;

use crate::catalog::Table;
use crate::executor::selection::{ComparisonOp, Constant, Expr, Predicate, SelectionExecutor};

use super::algorithm::{AlgorithmSpec, JoinAlgorithm, JoinRequest, JoinType, ValidatedJoinSpec};
use super::config::JoinConfig;
use super::cost::{
    CostModel, JoinCost, JoinEstimate, SideEstimate, estimate_join, key_columns_of,
    residual_selectivity,
};
use super::error::JoinError;
use super::exec::adaptive::AdaptiveJoin;
use super::exec::hash::HashJoin;
use super::exec::index_nested_loop::IndexNestedLoopJoin;
use super::exec::nested_loop::{DEFAULT_BLOCK_ROWS, NestedLoopJoin};
use super::exec::sort_merge::SortMergeJoin;
use super::exec::symmetric_hash::SymmetricHashJoin;
use super::exec::{MatchEvaluator, RowStream};
use super::index::{JoinIndex, find_usable};
use super::key::KeySpec;
use super::memory::MemoryAccountant;
use super::predicate::{JoinPredicate, PredicateSplit, SideResolver, split_conjuncts};
use super::schema::OutputSchema;
use super::source::{RowSource, TableRef, TableSource};
use super::spill::SpillScope;
use super::stats::{StatsConfidence, TableStatsCache};

/// Algorithms this module can currently construct.
///
/// The capability matrix in `algorithm.rs` describes what each one *supports*;
/// this is what has an executor behind it. The planner never proposes
/// something it cannot build.
const AVAILABLE: [JoinAlgorithm; 7] = [
    JoinAlgorithm::SimpleNestedLoop,
    JoinAlgorithm::BlockNestedLoop,
    JoinAlgorithm::IndexNestedLoop,
    JoinAlgorithm::SortMerge,
    JoinAlgorithm::Hash,
    JoinAlgorithm::SymmetricHash,
    JoinAlgorithm::Adaptive,
];

/// An index on the inner relation, with the key it can answer.
pub type InnerIndex = (std::rc::Rc<dyn JoinIndex>, KeySpec);

/// One input of a planned join, as EXPLAIN describes it.
#[derive(Debug, Clone)]
pub struct PlanSide {
    pub alias: String,
    pub rows: u64,
    pub pages: u64,
    pub filter: Option<String>,
}

/// A chosen algorithm with its estimates.
#[derive(Debug, Clone)]
pub struct PhysicalPlan {
    pub algorithm: JoinAlgorithm,
    pub join_type: JoinType,
    pub schema: Arc<OutputSchema>,
    pub estimate: JoinEstimate,
    pub cost: JoinCost,
    pub confidence: StatsConfidence,
    pub left: PlanSide,
    pub right: PlanSide,
    /// Equijoin components, rendered as `l.a = r.b`.
    pub key_conditions: Vec<String>,
    /// Whatever the keys could not express.
    pub residual: Option<String>,
    /// Entries in the index the plan will probe, when it uses one.
    pub index_entries: Option<u64>,
    /// Alternatives that were costed and not chosen, cheapest first.
    pub rejected: Vec<(JoinAlgorithm, f64)>,
}

impl PhysicalPlan {
    /// Render the plan the way EXPLAIN shows it.
    pub fn render(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "{} Join ({})  [rows={}  cost={:.2}ms  stats={}]",
            self.algorithm.name(),
            self.join_type.name(),
            self.estimate.output_rows,
            self.cost.total(),
            self.confidence.label()
        );

        if !self.key_conditions.is_empty() {
            let _ = writeln!(out, "  Join Cond: {}", self.key_conditions.join(" AND "));
        }
        if let Some(residual) = &self.residual {
            let _ = writeln!(out, "  Residual:  {residual}");
        }
        if let Some(entries) = self.index_entries {
            let _ = writeln!(
                out,
                "  Index:     {entries} entries on {}",
                self.right.alias
            );
        }
        let _ = writeln!(
            out,
            "  Cost:      io={:.2}ms  cpu={:.2}ms  extra passes={:.0}",
            self.cost.io, self.cost.cpu, self.cost.spill_passes
        );

        for side in [&self.left, &self.right] {
            let _ = writeln!(
                out,
                "  -> Scan on {}  [rows={}  pages={}]",
                side.alias, side.rows, side.pages
            );
            if let Some(filter) = &side.filter {
                let _ = writeln!(out, "       Filter: {filter}");
            }
        }

        if !self.rejected.is_empty() {
            let alternatives: Vec<String> = self
                .rejected
                .iter()
                .map(|(algorithm, cost)| format!("{} {cost:.2}ms", algorithm.name()))
                .collect();
            let _ = writeln!(out, "  Considered: {}", alternatives.join(", "));
        }

        out
    }
}

/// Describes one join, and turns it into a running operator.
pub struct JoinBuilder {
    left: TableRef,
    right: TableRef,
    join_type: JoinType,
    condition: Option<Predicate>,
    block_rows: usize,
    algorithm: Option<JoinAlgorithm>,
    config: JoinConfig,
    stats: Rc<TableStatsCache>,
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
            stats: Rc::new(TableStatsCache::new()),
        }
    }

    /// Force an algorithm instead of letting the planner choose. Validation
    /// still applies, so one that cannot serve the join type is refused rather
    /// than silently substituted.
    pub fn with_algorithm(mut self, algorithm: JoinAlgorithm) -> Self {
        self.algorithm = Some(algorithm);
        self
    }

    pub fn with_config(mut self, config: JoinConfig) -> Self {
        self.config = config;
        self
    }

    /// Share a statistics cache across several plans, so a relation's pages
    /// are read once rather than once per candidate.
    pub fn with_stats_cache(mut self, stats: Rc<TableStatsCache>) -> Self {
        self.stats = stats;
        self
    }

    pub fn with_condition(mut self, condition: Predicate) -> Self {
        self.condition = Some(condition);
        self
    }

    /// Outer rows a nested-loop join buffers per pass over the inner
    /// relation. Only meaningful when a nested loop is chosen or forced; it
    /// does not itself select one.
    pub fn with_block_rows(mut self, rows: usize) -> Self {
        self.block_rows = rows.max(1);
        self
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

    fn split(&self) -> Result<PredicateSplit, JoinError> {
        let left_relation = self.left.relation_schema();
        let right_relation = self.right.relation_schema();
        let resolver = SideResolver::new(&left_relation, &right_relation)?;
        split_conjuncts(self.condition.as_ref(), &resolver, self.join_type)
    }

    /// An index on the inner relation that can serve this join's keys.
    ///
    /// Absence is not an error: it simply removes index nested loop from the
    /// candidates. A sidecar that no longer matches the table is treated as
    /// absent rather than trusted.
    fn inner_index(&self, keys: &KeySpec) -> Option<InnerIndex> {
        find_usable(&self.right, keys)
    }

    /// Choose an algorithm and estimate what it will cost.
    pub fn plan(&self) -> Result<PhysicalPlan, JoinError> {
        let split = self.split()?;
        let index = self.inner_index(&split.keys);
        let request = JoinRequest {
            join_type: self.join_type,
            keys: &split.keys,
            has_residual: split.residual.is_some(),
            has_inner_index: index.is_some(),
        };

        let (left_stats, left_confidence) = self.stats.stats_for(&self.left);
        let (right_stats, right_confidence) = self.stats.stats_for(&self.right);
        // A plan is only as trustworthy as its least-known input.
        let confidence = weakest(left_confidence, right_confidence);

        let left_estimate =
            SideEstimate::from_stats(&left_stats, &key_columns_of(&split.keys, true));
        let right_estimate =
            SideEstimate::from_stats(&right_stats, &key_columns_of(&split.keys, false));

        let left_width = self.left.columns.len() as u64;
        let selectivity = residual_selectivity(
            split.residual.as_ref(),
            &left_stats,
            &right_stats,
            left_width,
        );
        let estimate = estimate_join(
            self.join_type,
            &left_estimate,
            &right_estimate,
            !split.keys.is_empty(),
            selectivity,
        );

        let model = CostModel::new(self.config.work_memory_bytes);
        let (algorithm, cost, rejected) = self.choose(
            &request,
            &model,
            &left_estimate,
            &right_estimate,
            &estimate,
            confidence,
        )?;

        Ok(PhysicalPlan {
            algorithm,
            join_type: self.join_type,
            schema: Arc::new(self.output_schema()?),
            estimate,
            cost,
            confidence,
            left: PlanSide {
                alias: self.left.alias.clone(),
                rows: left_estimate.rows,
                pages: left_estimate.pages,
                filter: split.left_local.as_ref().map(render_predicate),
            },
            right: PlanSide {
                alias: self.right.alias.clone(),
                rows: right_estimate.rows,
                pages: right_estimate.pages,
                filter: split.right_local.as_ref().map(render_predicate),
            },
            key_conditions: self.render_keys(&split),
            residual: split.residual.as_ref().map(render_predicate),
            index_entries: index.as_ref().map(|(index, _)| index.entry_count()),
            rejected,
        })
    }

    /// Cost every applicable algorithm and take the cheapest.
    fn choose(
        &self,
        request: &JoinRequest,
        model: &CostModel,
        left: &SideEstimate,
        right: &SideEstimate,
        estimate: &JoinEstimate,
        confidence: StatsConfidence,
    ) -> Result<(JoinAlgorithm, JoinCost, Vec<(JoinAlgorithm, f64)>), JoinError> {
        let has_keys = !request.keys.is_empty();
        let analyzed = confidence == StatsConfidence::Analyzed;

        // An explicit choice still has to pass validation.
        if let Some(forced) = self.algorithm {
            spec_of(forced).validate(request)?;
            let cost = model.cost(
                forced,
                left,
                right,
                estimate.output_rows,
                self.block_rows as u64,
                has_keys,
            );
            return Ok((forced, cost, Vec::new()));
        }

        let mut candidates: Vec<(JoinAlgorithm, JoinCost)> = Vec::new();
        for algorithm in AVAILABLE {
            if spec_of(algorithm).validate(request).is_err() {
                continue;
            }
            // A symmetric hash join holds both inputs at once and cannot
            // spill, so it is not offered when they do not fit. Modelling a
            // case that cannot happen would be worse than excluding it.
            if algorithm == JoinAlgorithm::SymmetricHash
                && left.bytes().saturating_add(right.bytes()) > self.config.work_memory_bytes
            {
                continue;
            }
            // Without an equality the adaptive operator is a block nested loop
            // wearing a different name; offering both only adds noise.
            if algorithm == JoinAlgorithm::Adaptive && !has_keys {
                continue;
            }

            let mut cost = model.cost(
                algorithm,
                left,
                right,
                estimate.output_rows,
                self.block_rows as u64,
                has_keys,
            );
            if algorithm == JoinAlgorithm::Adaptive {
                let factor = model.adaptive_factor(analyzed);
                cost.io *= factor;
                cost.cpu *= factor;
            }
            candidates.push((algorithm, cost));
        }

        candidates.sort_by(|a, b| {
            a.1.total()
                .partial_cmp(&b.1.total())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let Some((best, cost)) = candidates.first().copied() else {
            return Err(JoinError::plan(format!(
                "no available algorithm can execute a {} join with this condition",
                request.join_type.name()
            )));
        };

        let rejected = candidates
            .iter()
            .skip(1)
            .map(|(algorithm, cost)| (*algorithm, cost.total()))
            .collect();

        Ok((best, cost, rejected))
    }

    fn render_keys(&self, split: &PredicateSplit) -> Vec<String> {
        split
            .keys
            .columns
            .iter()
            .map(|column| {
                let left = self
                    .left
                    .columns
                    .get(column.left_index)
                    .map(|c| c.name.as_str())
                    .unwrap_or("?");
                let right = self
                    .right
                    .columns
                    .get(column.right_index)
                    .map(|c| c.name.as_str())
                    .unwrap_or("?");
                format!(
                    "{}.{} = {}.{}",
                    self.left.alias, left, self.right.alias, right
                )
            })
            .collect()
    }

    /// Plan the join and render it, without running it.
    pub fn explain(&self) -> Result<String, JoinError> {
        Ok(self.plan()?.render())
    }

    /// Validate, plan, and start the join.
    pub fn execute(&self) -> Result<Box<dyn RowStream>, JoinError> {
        let plan = self.plan()?;
        let split = self.split()?;
        let index = self.inner_index(&split.keys);
        let left_relation = self.left.relation_schema();

        let request = JoinRequest {
            join_type: self.join_type,
            keys: &split.keys,
            has_residual: split.residual.is_some(),
            has_inner_index: index.is_some(),
        };
        let spec = spec_of(plan.algorithm).validate(&request)?;

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
        if spec.algorithm() == JoinAlgorithm::Adaptive {
            // The adaptive operator decides which side to build from, so it
            // needs both inputs re-openable rather than one already streaming.
            return Ok(Box::new(AdaptiveJoin::new(
                &spec,
                evaluator,
                Box::new(left_source),
                Box::new(right_source),
                plan.schema,
                MemoryAccountant::new(self.config.work_memory_bytes),
                spill_scope_in(&self.config)?,
                self.block_rows,
            )?));
        }

        let outer = left_source.open()?;
        self.build_operator(
            &spec,
            evaluator,
            outer,
            Box::new(right_source),
            plan.schema,
            index,
        )
    }

    fn build_operator(
        &self,
        spec: &ValidatedJoinSpec,
        evaluator: MatchEvaluator,
        outer: Box<dyn RowStream>,
        inner: Box<dyn RowSource>,
        schema: Arc<OutputSchema>,
        index: Option<InnerIndex>,
    ) -> Result<Box<dyn RowStream>, JoinError> {
        build_operator_with(
            spec,
            evaluator,
            outer,
            inner,
            schema,
            index,
            &self.config,
            self.block_rows,
            Some(&self.right),
        )
    }
}

/// Construct an operator from inputs that are already streams and sources.
///
/// `JoinBuilder` works from two table references; this is the same dispatch
/// for callers whose inputs are themselves joins. `inner_table` is only needed
/// by the index nested loop, which fetches rows by their location in a heap
/// file - an intermediate result has none, so it is `None` there and the
/// algorithm is not offered.
#[allow(clippy::too_many_arguments)]
pub fn build_operator_with(
    spec: &ValidatedJoinSpec,
    evaluator: MatchEvaluator,
    outer: Box<dyn RowStream>,
    inner: Box<dyn RowSource>,
    schema: Arc<OutputSchema>,
    index: Option<InnerIndex>,
    config: &JoinConfig,
    block_rows: usize,
    inner_table: Option<&TableRef>,
) -> Result<Box<dyn RowStream>, JoinError> {
    {
        let self_block_rows = block_rows;
        let self_config = config;
        match spec.algorithm() {
            JoinAlgorithm::IndexNestedLoop => {
                // Validation already established that an index exists; this
                // only unpacks it.
                let (index, probe_keys) = index.ok_or_else(|| {
                    JoinError::plan(
                        "index nested loop was planned without a usable index".to_string(),
                    )
                })?;
                let table = inner_table.ok_or_else(|| {
                    JoinError::plan(
                        "index nested loop needs a base relation on its inner side".to_string(),
                    )
                })?;
                Ok(Box::new(IndexNestedLoopJoin::new(
                    spec, evaluator, probe_keys, index, table, outer, schema,
                )?))
            }
            JoinAlgorithm::SimpleNestedLoop | JoinAlgorithm::BlockNestedLoop => {
                // Asking for the simple variant means a block of one row;
                // otherwise the operator would claim to be simple while
                // blocking.
                let block_rows = if spec.algorithm() == JoinAlgorithm::SimpleNestedLoop {
                    1
                } else {
                    self_block_rows.max(2)
                };
                Ok(Box::new(NestedLoopJoin::new(
                    spec, evaluator, outer, inner, schema, block_rows,
                )))
            }
            JoinAlgorithm::Hash => Ok(Box::new(HashJoin::new(
                spec,
                evaluator,
                outer,
                inner,
                schema,
                MemoryAccountant::new(self_config.work_memory_bytes),
                spill_scope_in(self_config)?,
            ))),
            JoinAlgorithm::SortMerge => Ok(Box::new(SortMergeJoin::new(
                spec,
                evaluator,
                outer,
                inner,
                schema,
                MemoryAccountant::new(self_config.work_memory_bytes),
                spill_scope_in(self_config)?,
            ))),
            JoinAlgorithm::SymmetricHash => Ok(Box::new(SymmetricHashJoin::new(
                spec,
                evaluator,
                outer,
                inner,
                schema,
                MemoryAccountant::new(self_config.work_memory_bytes),
            ))),
            // Every other algorithm has an executor. The adaptive one needs
            // both inputs re-openable, so `execute` builds it before reaching
            // here; arriving with it means the two have drifted apart.
            JoinAlgorithm::Adaptive => Err(JoinError::plan(
                "internal: the adaptive operator is built by execute(), not here".to_string(),
            )),
        }
    }
}

/// A fresh spill directory for one operator.
pub fn spill_scope_in(config: &JoinConfig) -> Result<Arc<SpillScope>, JoinError> {
    SpillScope::create(&config.spill_root)
}

fn spec_of(algorithm: JoinAlgorithm) -> &'static AlgorithmSpec {
    super::algorithm::spec_for(algorithm)
}

fn weakest(a: StatsConfidence, b: StatsConfidence) -> StatsConfidence {
    let rank = |confidence: StatsConfidence| match confidence {
        StatsConfidence::Analyzed => 2,
        StatsConfidence::HeaderOnly => 1,
        StatsConfidence::Defaults => 0,
    };
    if rank(a) <= rank(b) { a } else { b }
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

// ── Rendering predicates ─────────────────────────────────────────────────────

/// Render a predicate back into something close to what was written.
///
/// Approximate by design: this is for EXPLAIN, not for round-tripping.
pub fn render_predicate(predicate: &Predicate) -> String {
    match predicate {
        Predicate::Compare(left, op, right) => format!(
            "{} {} {}",
            render_expr(left),
            render_op(*op),
            render_expr(right)
        ),
        Predicate::IsNull(expr) => format!("{} IS NULL", render_expr(expr)),
        Predicate::IsNotNull(expr) => format!("{} IS NOT NULL", render_expr(expr)),
        Predicate::Not(inner) => format!("NOT ({})", render_predicate(inner)),
        Predicate::Exists(inner) => format!("EXISTS ({})", render_predicate(inner)),
        Predicate::And(a, b) => {
            format!("({}) AND ({})", render_predicate(a), render_predicate(b))
        }
        Predicate::Or(a, b) => format!("({}) OR ({})", render_predicate(a), render_predicate(b)),
        Predicate::Between(value, low, high) => format!(
            "{} BETWEEN {} AND {}",
            render_expr(value),
            render_expr(low),
            render_expr(high)
        ),
        Predicate::In(value, list) => {
            let items: Vec<String> = list.iter().map(render_expr).collect();
            format!("{} IN ({})", render_expr(value), items.join(", "))
        }
        Predicate::Like(value, pattern, _) => {
            format!("{} LIKE '{pattern}'", render_expr(value))
        }
    }
}

fn render_expr(expr: &Expr) -> String {
    match expr {
        Expr::Column(reference) => reference.column_name.clone(),
        Expr::Constant(constant) => render_constant(constant),
        Expr::Add(a, b) => format!("({} + {})", render_expr(a), render_expr(b)),
        Expr::Sub(a, b) => format!("({} - {})", render_expr(a), render_expr(b)),
        Expr::Mul(a, b) => format!("({} * {})", render_expr(a), render_expr(b)),
        Expr::Div(a, b) => format!("({} / {})", render_expr(a), render_expr(b)),
    }
}

fn render_constant(constant: &Constant) -> String {
    match constant {
        Constant::Int(value) => value.to_string(),
        Constant::Float(value) => value.to_string(),
        Constant::Date(value) | Constant::Text(value) => format!("'{value}'"),
        Constant::Null => "NULL".to_string(),
    }
}

fn render_op(op: ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::Equals => "=",
        ComparisonOp::NotEquals => "<>",
        ComparisonOp::LessThan => "<",
        ComparisonOp::LessOrEqual => "<=",
        ComparisonOp::GreaterThan => ">",
        ComparisonOp::GreaterOrEqual => ">=",
    }
}
