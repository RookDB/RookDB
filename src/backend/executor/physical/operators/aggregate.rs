use std::collections::{HashMap, HashSet};
use std::cmp::Ordering;

use crate::backend::error::{RookError, RookResult};
use super::super::tuple::{Tuple, ColumnInfo};
use super::super::expr::{Expr, Predicate, evaluate_predicate};
use super::trait_::PhysicalOperator;

use crate::types::value::DataValue;
use crate::types::datatype::DataType;
use crate::types::comparison::compare_nullable;

// ── Aggregate Function Enum ────────────────────────────────────────────────────

/// The type of aggregate function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

// ── Aggregate Info Struct ──────────────────────────────────────────────────────

/// Metadata for one aggregate function in the SELECT clause.
#[derive(Debug, Clone)]
pub struct AggregateInfo {
    pub function: AggregateFunction,
    pub input: Option<Expr>,
    pub output_name: String,
    pub output_type: DataType,
    /// Whether this aggregate uses DISTINCT (e.g. COUNT(DISTINCT col)).
    pub distinct: bool,
}

// ── PerGroupState ─────────────────────────────────────────────────────────────

/// Per-group partial aggregate state.
#[derive(Debug, Clone)]
pub struct PerGroupState {
    pub(super) row_count: u64,
    pub(super) non_null_count: u64,
    pub(super) distinct_seen: HashSet<DataValue>,
    pub(super) has_sum: bool,
    pub(super) sum_int: i128,
    pub(super) sum_float: f64,
    pub(super) sum_is_float: bool,
    pub(super) min: Option<DataValue>,
    pub(super) max: Option<DataValue>,
}

impl Default for PerGroupState {
    fn default() -> Self {
        Self::new()
    }
}

impl PerGroupState {
    pub fn new() -> Self {
        Self {
            row_count: 0,
            non_null_count: 0,
            distinct_seen: HashSet::new(),
            has_sum: false,
            sum_int: 0,
            sum_float: 0.0,
            sum_is_float: false,
            min: None,
            max: None,
        }
    }

    /// Update state with a new value from one row.
    pub fn update(&mut self, function: AggregateFunction, value: Option<&DataValue>, distinct: bool) -> Result<(), String> {
        self.row_count += 1;

        let val = match value {
            Some(v) => v,
            None => return Ok(()),
        };

        if distinct
            && !self.distinct_seen.insert(val.clone()) {
                return Ok(());
            }

        self.non_null_count += 1;

        if matches!(function, AggregateFunction::Sum | AggregateFunction::Avg) {
            match val {
                DataValue::SmallInt(v) => { self.sum_int += *v as i128; self.has_sum = true; }
                DataValue::Int(v) => { self.sum_int += *v as i128; self.has_sum = true; }
                DataValue::BigInt(v) => { self.sum_int += *v as i128; self.has_sum = true; }
                DataValue::Real(v) => { self.sum_float += v.0 as f64; self.sum_is_float = true; self.has_sum = true; }
                DataValue::DoublePrecision(v) => { self.sum_float += v.0; self.sum_is_float = true; self.has_sum = true; }
                DataValue::Numeric(v) => {
                    self.sum_float += v.unscaled as f64 / 10f64.powi(v.scale as i32);
                    self.sum_is_float = true;
                    self.has_sum = true;
                }
                _ => return Err(format!("Cannot SUM/AVG non-numeric type: {:?}", val)),
            }
        }

        if matches!(function, AggregateFunction::Min) {
            match &self.min {
                None => self.min = Some(val.clone()),
                Some(current) => {
                    let ordering = compare_nullable(Some(val), Some(current))
                        .map_err(|e| format!("Comparison error in MIN: {}", e))?
                        .unwrap_or(Ordering::Equal);
                    if ordering == Ordering::Less {
                        self.min = Some(val.clone());
                    }
                }
            }
        }

        if matches!(function, AggregateFunction::Max) {
            match &self.max {
                None => self.max = Some(val.clone()),
                Some(current) => {
                    let ordering = compare_nullable(Some(val), Some(current))
                        .map_err(|e| format!("Comparison error in MAX: {}", e))?
                        .unwrap_or(Ordering::Equal);
                    if ordering == Ordering::Greater {
                        self.max = Some(val.clone());
                    }
                }
            }
        }

        Ok(())
    }

    /// Produce the final aggregate value for a given function.
    pub fn finalize(&self, info: &AggregateInfo) -> Option<DataValue> {
        match info.function {
            AggregateFunction::Count => {
                if info.input.is_none() {
                    Some(DataValue::BigInt(self.row_count as i64))
                } else {
                    Some(DataValue::BigInt(self.non_null_count as i64))
                }
            }
            AggregateFunction::Sum => {
                if !self.has_sum { return None; }
                if self.sum_is_float {
                    Some(DataValue::DoublePrecision(crate::types::value::OrderedF64(self.sum_float)))
                } else {
                    let v = if self.sum_int > i64::MAX as i128 { i64::MAX }
                        else if self.sum_int < i64::MIN as i128 { i64::MIN }
                        else { self.sum_int as i64 };
                    Some(DataValue::BigInt(v))
                }
            }
            AggregateFunction::Avg => {
                if self.non_null_count == 0 || !self.has_sum { return None; }
                let total = if self.sum_is_float { self.sum_float } else { self.sum_int as f64 };
                Some(DataValue::DoublePrecision(
                    crate::types::value::OrderedF64(total / self.non_null_count as f64)
                ))
            }
            AggregateFunction::Min => self.min.clone(),
            AggregateFunction::Max => self.max.clone(),
        }
    }
}

/// Infer the output DataType for an aggregate function based on input expression type.
pub fn infer_aggregate_output_type(
    function: AggregateFunction,
    input_type: Option<&DataType>,
) -> DataType {
    match function {
        AggregateFunction::Count => DataType::BigInt,
        AggregateFunction::Sum => match input_type {
            Some(DataType::SmallInt) | Some(DataType::Int) | Some(DataType::BigInt) => DataType::BigInt,
            Some(DataType::Real) => DataType::Real,
            Some(DataType::DoublePrecision) => DataType::DoublePrecision,
            Some(DataType::Numeric { .. }) | Some(DataType::Decimal { .. }) => DataType::DoublePrecision,
            _ => DataType::BigInt,
        },
        AggregateFunction::Avg => DataType::DoublePrecision,
        AggregateFunction::Min | AggregateFunction::Max => {
            input_type.cloned().unwrap_or(DataType::Int)
        }
    }
}

// ── GroupEntry ────────────────────────────────────────────────────────────────

/// Stores the actual GROUP BY values and aggregate states for one group.
struct GroupEntry {
    /// The group-by column values (used for output tuple reconstruction).
    key_values: Vec<Option<DataValue>>,
    /// Per-aggregate states (one per aggregate function in the plan).
    agg_states: Vec<PerGroupState>,
}

// ── AggregateOperator ─────────────────────────────────────────────────────────

/// A hash-based aggregation operator implementing GROUP BY.
#[allow(dead_code)]
pub struct AggregateOperator {
    child: Box<dyn PhysicalOperator>,
    group_by_exprs: Vec<Expr>,
    group_by_names: Vec<String>,
    group_by_types: Vec<DataType>,
    aggregates: Vec<AggregateInfo>,
    having: Option<Predicate>,
    output_schema: Vec<ColumnInfo>,

    group_map: HashMap<Vec<Option<DataValue>>, usize>,
    groups: Vec<GroupEntry>,
    output_buffer: Vec<Tuple>,
    output_pos: usize,
    consumed: bool,
}

impl AggregateOperator {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        group_by_exprs: Vec<Expr>,
        group_by_names: Vec<String>,
        group_by_types: Vec<DataType>,
        aggregates: Vec<AggregateInfo>,
        having: Option<Predicate>,
    ) -> Self {
        // Propagate the child's table qualifier for group-by columns so
        // projections above the aggregate can still use qualified references
        // (e.g. `SELECT d.dept, COUNT(*) FROM t d GROUP BY d.dept`).
        let child_schema = child.schema();
        let mut output_schema: Vec<ColumnInfo> = group_by_names.iter().zip(group_by_types.iter())
            .map(|(name, dt)| ColumnInfo {
                name: name.clone(),
                data_type: dt.clone(),
                table: child_schema
                    .iter()
                    .find(|ci| ci.name.eq_ignore_ascii_case(name))
                    .and_then(|ci| ci.table.clone()),
            })
            .collect();
        for agg in &aggregates {
            output_schema.push(ColumnInfo {
                name: agg.output_name.clone(),
                data_type: agg.output_type.clone(), table: None });
        }

        Self {
            child,
            group_by_exprs,
            group_by_names,
            group_by_types,
            aggregates,
            having,
            output_schema,
            group_map: HashMap::new(),
            groups: Vec::new(),
            output_buffer: Vec::new(),
            output_pos: 0,
            consumed: false,
        }
    }

    fn build_output_tuple(&self, entry: &GroupEntry) -> RookResult<Tuple> {
        let mut values = Vec::new();
        for val in &entry.key_values {
            values.push(val.clone());
        }
        for (agg_idx, agg) in self.aggregates.iter().enumerate() {
            let val = entry.agg_states[agg_idx].finalize(agg);
            values.push(val);
        }
        Ok(Tuple::new(values))
    }

    fn materialise(&mut self) -> RookResult<()> {
        for entry in &self.groups {
            let tuple = self.build_output_tuple(entry)?;
            if let Some(ref having) = self.having {
                if let Some(true) = evaluate_predicate(having, &tuple, &self.output_schema)? { self.output_buffer.push(tuple) }
            } else {
                self.output_buffer.push(tuple);
            }
        }
        Ok(())
    }
    fn consume_if_needed(&mut self) -> RookResult<()> {
        if self.consumed {
            return Ok(());
        }

        let child_schema = self.child.schema().to_vec();
        let mut batch = Vec::with_capacity(super::trait_::DEFAULT_BATCH_SIZE);

        // Fast path for non-grouped global COUNT(*)
        let is_simple_count_star = self.group_by_exprs.is_empty()
            && self.aggregates.len() == 1
            && self.aggregates[0].function == AggregateFunction::Count
            && self.aggregates[0].input.is_none()
            && !self.aggregates[0].distinct;

        if is_simple_count_star {
            let mut total = 0usize;
            loop {
                let n = self.child.next_batch(&mut batch)?;
                if n == 0 {
                    break;
                }
                total += n;
                batch.clear();
            }
            let mut state = PerGroupState::new();
            state.row_count = total as u64;
            self.groups.push(GroupEntry {
                key_values: Vec::new(),
                agg_states: vec![state],
            });
        } else {
            while self.child.next_batch(&mut batch)? > 0 {
                for tuple in batch.drain(..) {
                    let key_values: Vec<Option<DataValue>> = if self.group_by_exprs.is_empty() {
                        Vec::new()
                    } else {
                        self.group_by_exprs.iter()
                            .map(|expr| expr.evaluate(&tuple, &child_schema))
                            .collect::<Result<Vec<_>, String>>()
                            .map_err(|e| RookError::Internal(e))?
                    };

                    let group_idx = match self.group_map.entry(key_values.clone()) {
                        std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            let idx = self.groups.len();
                            entry.insert(idx);
                            let agg_states = (0..self.aggregates.len())
                                .map(|_| PerGroupState::new())
                                .collect();
                            self.groups.push(GroupEntry { key_values, agg_states });
                            idx
                        }
                    };

                    let group = &mut self.groups[group_idx];
                    for (agg_idx, agg) in self.aggregates.iter().enumerate() {
                        let value = match &agg.input {
                            Some(expr) => expr.evaluate(&tuple, &child_schema)?,
                            None => None,
                        };
                        group.agg_states[agg_idx].update(agg.function, value.as_ref(), agg.distinct)
                            .map_err(|e| RookError::Internal(format!("Aggregate error: {}", e)))?;
                    }
                }
            }
        }

        self.consumed = true;
        self.materialise()?;
        Ok(())
    }
}

impl PhysicalOperator for AggregateOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if !self.consumed {
            self.consume_if_needed()?;
        }

        if self.output_pos < self.output_buffer.len() {
            let tuple = self.output_buffer[self.output_pos].clone();
            self.output_pos += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        batch.clear();
        if !self.consumed {
            self.consume_if_needed()?;
        }

        let available = self.output_buffer.len().saturating_sub(self.output_pos);
        if available == 0 {
            return Ok(0);
        }
        let take = available.min(super::trait_::DEFAULT_BATCH_SIZE);
        batch.reserve(take);
        for i in 0..take {
            batch.push(self.output_buffer[self.output_pos + i].clone());
        }
        self.output_pos += take;
        Ok(take)
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.child.reset()?;
        self.group_map.clear();
        self.groups.clear();
        self.output_buffer.clear();
        self.output_pos = 0;
        self.consumed = false;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "Aggregate"
    }
}
