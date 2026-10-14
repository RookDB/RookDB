use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use super::super::explain::PlanNode;
use super::super::expr::{Expr, Predicate, evaluate_predicate};
use super::super::tuple::{ColumnInfo, Tuple};
use super::trait_::PhysicalOperator;
use crate::backend::error::{RookError, RookResult};

use crate::types::comparison::compare_nullable;
use crate::types::datatype::DataType;
use crate::types::value::{DataValue, NumericValue, OrderedF32, OrderedF64};

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

fn add_numeric(a: NumericValue, b: NumericValue) -> Result<NumericValue, String> {
    let target_scale = a.scale.max(b.scale);
    let scale_val = |unscaled: i128, cur_scale: u8| -> Result<i128, String> {
        if cur_scale == target_scale {
            Ok(unscaled)
        } else {
            let diff = (target_scale - cur_scale) as u32;
            let factor = 10_i128
                .checked_pow(diff)
                .ok_or_else(|| "Numeric scale overflow".to_string())?;
            unscaled
                .checked_mul(factor)
                .ok_or_else(|| "Numeric scale overflow".to_string())
        }
    };
    let a_scaled = scale_val(a.unscaled, a.scale)?;
    let b_scaled = scale_val(b.unscaled, b.scale)?;
    let sum = a_scaled
        .checked_add(b_scaled)
        .ok_or_else(|| "Numeric addition overflow".to_string())?;
    Ok(NumericValue {
        unscaled: sum,
        scale: target_scale,
    })
}

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
    pub(super) sum_numeric: Option<NumericValue>,
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
            sum_numeric: None,
            min: None,
            max: None,
        }
    }

    /// Update state with a new value from one row.
    pub fn update(
        &mut self,
        function: AggregateFunction,
        value: Option<&DataValue>,
        distinct: bool,
    ) -> Result<(), String> {
        self.row_count += 1;

        let val = match value {
            Some(v) => v,
            None => return Ok(()),
        };

        if distinct && !self.distinct_seen.insert(val.clone()) {
            return Ok(());
        }

        self.non_null_count += 1;

        if matches!(function, AggregateFunction::Sum | AggregateFunction::Avg) {
            match val {
                DataValue::SmallInt(v) => {
                    if self.sum_is_float {
                        self.sum_float += *v as f64;
                    } else if let Some(num) = self.sum_numeric.take() {
                        let v_num = NumericValue {
                            unscaled: *v as i128,
                            scale: 0,
                        };
                        self.sum_numeric = Some(add_numeric(num, v_num)?);
                    } else {
                        self.sum_int += *v as i128;
                    }
                    self.has_sum = true;
                }
                DataValue::Int(v) => {
                    if self.sum_is_float {
                        self.sum_float += *v as f64;
                    } else if let Some(num) = self.sum_numeric.take() {
                        let v_num = NumericValue {
                            unscaled: *v as i128,
                            scale: 0,
                        };
                        self.sum_numeric = Some(add_numeric(num, v_num)?);
                    } else {
                        self.sum_int += *v as i128;
                    }
                    self.has_sum = true;
                }
                DataValue::BigInt(v) => {
                    if self.sum_is_float {
                        self.sum_float += *v as f64;
                    } else if let Some(num) = self.sum_numeric.take() {
                        let v_num = NumericValue {
                            unscaled: *v as i128,
                            scale: 0,
                        };
                        self.sum_numeric = Some(add_numeric(num, v_num)?);
                    } else {
                        self.sum_int += *v as i128;
                    }
                    self.has_sum = true;
                }
                DataValue::Real(v) => {
                    if !self.sum_is_float {
                        self.sum_is_float = true;
                        self.sum_float += self.sum_int as f64;
                        self.sum_int = 0;
                        if let Some(num) = self.sum_numeric.take() {
                            self.sum_float += num.unscaled as f64 / 10f64.powi(num.scale as i32);
                        }
                    }
                    self.sum_float += v.0 as f64;
                    self.has_sum = true;
                }
                DataValue::DoublePrecision(v) => {
                    if !self.sum_is_float {
                        self.sum_is_float = true;
                        self.sum_float += self.sum_int as f64;
                        self.sum_int = 0;
                        if let Some(num) = self.sum_numeric.take() {
                            self.sum_float += num.unscaled as f64 / 10f64.powi(num.scale as i32);
                        }
                    }
                    self.sum_float += v.0;
                    self.has_sum = true;
                }
                DataValue::Numeric(v) => {
                    if self.sum_is_float {
                        self.sum_float += v.unscaled as f64 / 10f64.powi(v.scale as i32);
                    } else {
                        let current = self.sum_numeric.take().unwrap_or_else(|| {
                            let n = NumericValue {
                                unscaled: self.sum_int,
                                scale: 0,
                            };
                            self.sum_int = 0;
                            n
                        });
                        self.sum_numeric = Some(add_numeric(current, v.clone())?);
                    }
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
    ///
    /// A SUM comes out in `info.output_type`, the type the column was declared
    /// with: see [`sum_as_declared`].
    pub fn finalize(&self, info: &AggregateInfo) -> Result<Option<DataValue>, String> {
        Ok(match info.function {
            AggregateFunction::Count => {
                if info.input.is_none() {
                    Some(DataValue::BigInt(self.row_count as i64))
                } else {
                    Some(DataValue::BigInt(self.non_null_count as i64))
                }
            }
            AggregateFunction::Sum => {
                if !self.has_sum {
                    return Ok(None);
                }
                let sum = if self.sum_is_float {
                    SumValue::Float(self.sum_float + (self.sum_int as f64))
                } else if let Some(num) = &self.sum_numeric {
                    SumValue::Numeric(num.clone())
                } else {
                    SumValue::Int(self.sum_int)
                };
                Some(sum_as_declared(sum, &info.output_type)?)
            }
            AggregateFunction::Avg => {
                if self.non_null_count == 0 || !self.has_sum {
                    return Ok(None);
                }
                let total = if self.sum_is_float {
                    self.sum_float + (self.sum_int as f64)
                } else if let Some(num) = &self.sum_numeric {
                    num.unscaled as f64 / 10f64.powi(num.scale as i32)
                } else {
                    self.sum_int as f64
                };
                Some(DataValue::DoublePrecision(OrderedF64(
                    total / self.non_null_count as f64,
                )))
            }
            AggregateFunction::Min => self.min.clone(),
            AggregateFunction::Max => self.max.clone(),
        })
    }
}

/// A finished SUM, in the representation it was accumulated in.
enum SumValue {
    Int(i128),
    Float(f64),
    Numeric(NumericValue),
}

/// Render a finished SUM as its declared output type, so the column header,
/// the schema handed to the operators above, and the value all agree.
///
/// An integer sum declared BIGINT that no longer fits 64 bits is an error
/// ("BIGINT out of range", as in PostgreSQL) rather than a NUMERIC value in a
/// BIGINT column. A sum whose inputs were not of the type the planner inferred
/// for them (an integer-typed CASE with a fractional branch, say) keeps the
/// representation it was accumulated in: narrowing it would lose data.
fn sum_as_declared(sum: SumValue, declared: &DataType) -> Result<DataValue, String> {
    Ok(match (sum, declared) {
        (SumValue::Int(v), DataType::Numeric { .. } | DataType::Decimal { .. }) => {
            DataValue::Numeric(NumericValue {
                unscaled: v,
                scale: 0,
            })
        }
        (SumValue::Int(v), DataType::Real) => DataValue::Real(OrderedF32(v as f32)),
        (SumValue::Int(v), DataType::DoublePrecision) => {
            DataValue::DoublePrecision(OrderedF64(v as f64))
        }
        (SumValue::Int(v), _) => DataValue::BigInt(
            i64::try_from(v).map_err(|_| "SUM result is out of range for BIGINT".to_string())?,
        ),
        (SumValue::Float(v), DataType::Real) => {
            let narrowed = v as f32;
            if narrowed.is_infinite() && v.is_finite() {
                return Err("SUM result is out of range for REAL".to_string());
            }
            DataValue::Real(OrderedF32(narrowed))
        }
        (SumValue::Float(v), _) => DataValue::DoublePrecision(OrderedF64(v)),
        (SumValue::Numeric(v), _) => DataValue::Numeric(v),
    })
}

/// Widest NUMERIC the engine has; a SUM is declared with it because the total
/// of a column outgrows the column's own precision.
const SUM_NUMERIC_PRECISION: u8 = 38;

/// Infer the output DataType for an aggregate function based on input expression type.
///
/// SUM follows PostgreSQL: SMALLINT/INT sum to BIGINT, BIGINT to NUMERIC (two
/// BIGINTs already overflow 64 bits), REAL to REAL, DOUBLE to DOUBLE, and
/// NUMERIC to NUMERIC at the input's scale. AVG is DOUBLE PRECISION for every
/// input, which is what [`PerGroupState::finalize`] computes.
pub fn infer_aggregate_output_type(
    function: AggregateFunction,
    input_type: Option<&DataType>,
) -> DataType {
    match function {
        AggregateFunction::Count => DataType::BigInt,
        AggregateFunction::Sum => match input_type {
            Some(DataType::SmallInt) | Some(DataType::Int) => DataType::BigInt,
            Some(DataType::BigInt) => DataType::Numeric {
                precision: SUM_NUMERIC_PRECISION,
                scale: 0,
            },
            Some(DataType::Real) => DataType::Real,
            Some(DataType::DoublePrecision) => DataType::DoublePrecision,
            Some(DataType::Numeric { scale, .. }) => DataType::Numeric {
                precision: SUM_NUMERIC_PRECISION,
                scale: *scale,
            },
            Some(DataType::Decimal { scale, .. }) => DataType::Decimal {
                precision: SUM_NUMERIC_PRECISION,
                scale: *scale,
            },
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
    /// Groups in first-seen order, which is the output order.
    groups: Vec<GroupEntry>,
    /// Next group to emit.
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
        // A qualified key (`GROUP BY m.name` under a join) takes the
        // qualifier of the column it reads, not of the first `name`.
        let mut output_schema: Vec<ColumnInfo> = group_by_names
            .iter()
            .zip(group_by_types.iter())
            .zip(group_by_exprs.iter())
            .map(|((name, dt), expr)| ColumnInfo {
                merged: false,
                name: name.clone(),
                data_type: dt.clone(),
                table: match expr {
                    Expr::Column { table, column } => {
                        super::super::expr::column_position(child_schema, table.as_deref(), column)
                            .map(|i| &child_schema[i])
                    }
                    _ => None,
                }
                .or_else(|| {
                    child_schema
                        .iter()
                        .find(|ci| ci.name.eq_ignore_ascii_case(name))
                })
                .and_then(|ci| ci.table.clone()),
            })
            .collect();
        for agg in &aggregates {
            output_schema.push(ColumnInfo {
                merged: false,
                name: agg.output_name.clone(),
                data_type: agg.output_type.clone(),
                table: None,
            });
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
            output_pos: 0,
            consumed: false,
        }
    }

    /// Output row for a group. Each group is emitted once, so its key values
    /// are moved out rather than cloned.
    fn take_output_tuple(&mut self, idx: usize) -> RookResult<Tuple> {
        let entry = &mut self.groups[idx];
        let mut values = std::mem::take(&mut entry.key_values);
        values.reserve(self.aggregates.len());
        for (agg_idx, agg) in self.aggregates.iter().enumerate() {
            let val = entry.agg_states[agg_idx]
                .finalize(agg)
                .map_err(|e| RookError::Internal(format!("Aggregate error: {}", e)))?;
            values.push(val);
        }
        Ok(Tuple::new(values))
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
                        self.group_by_exprs
                            .iter()
                            .map(|expr| expr.evaluate(&tuple, &child_schema))
                            .collect::<Result<Vec<_>, String>>()
                            .map_err(RookError::Internal)?
                    };

                    // The key is cloned only when it starts a new group.
                    let group_idx = match self.group_map.get(&key_values) {
                        Some(&idx) => idx,
                        None => {
                            let idx = self.groups.len();
                            self.group_map.insert(key_values.clone(), idx);
                            let agg_states = (0..self.aggregates.len())
                                .map(|_| PerGroupState::new())
                                .collect();
                            self.groups.push(GroupEntry {
                                key_values,
                                agg_states,
                            });
                            idx
                        }
                    };

                    let group = &mut self.groups[group_idx];
                    for (agg_idx, agg) in self.aggregates.iter().enumerate() {
                        let value = match &agg.input {
                            Some(expr) => expr.evaluate(&tuple, &child_schema)?,
                            None => None,
                        };
                        group.agg_states[agg_idx]
                            .update(agg.function, value.as_ref(), agg.distinct)
                            .map_err(|e| RookError::Internal(format!("Aggregate error: {}", e)))?;
                    }
                }
            }
        }

        // Global aggregate over empty input: SQL standard requires exactly 1 row
        if self.group_by_exprs.is_empty() && self.groups.is_empty() {
            let agg_states = (0..self.aggregates.len())
                .map(|_| PerGroupState::new())
                .collect();
            self.groups.push(GroupEntry {
                key_values: Vec::new(),
                agg_states,
            });
        }

        // Every group is known; the lookup table is no longer needed.
        self.group_map = HashMap::new();
        self.consumed = true;
        Ok(())
    }
}

impl PhysicalOperator for AggregateOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        self.consume_if_needed()?;
        while self.output_pos < self.groups.len() {
            let tuple = self.take_output_tuple(self.output_pos)?;
            self.output_pos += 1;
            match &self.having {
                Some(having) => {
                    if let Some(true) = evaluate_predicate(having, &tuple, &self.output_schema)? {
                        return Ok(Some(tuple));
                    }
                }
                None => return Ok(Some(tuple)),
            }
        }
        Ok(None)
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.child.reset()?;
        self.group_map.clear();
        self.groups.clear();
        self.output_pos = 0;
        self.consumed = false;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "Aggregate"
    }

    fn explain(&self) -> PlanNode {
        let mut node = PlanNode::new("Aggregate");
        if !self.group_by_names.is_empty() {
            node = node.with("group_by", self.group_by_names.join(", "));
        }
        let aggregates: Vec<&str> = self
            .aggregates
            .iter()
            .map(|a| a.output_name.as_str())
            .collect();
        if !aggregates.is_empty() {
            node = node.with("aggregates", aggregates.join(", "));
        }
        if self.having.is_some() {
            node = node.with("having", "yes");
        }
        node.child(self.child.explain())
    }
}
