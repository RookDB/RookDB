use std::collections::{HashMap, HashSet};

use super::super::explain::PlanNode;
use super::super::tuple::{ColumnInfo, Tuple};
use super::trait_::PhysicalOperator;
use super::utils::{KeyClass, canonical_row_key};
use crate::backend::error::{ResultContext, RookResult};
use crate::types::datatype::DataType;
use crate::types::value::DataValue;

/// How one side's values of a column are converted to the column's type.
#[derive(Clone, Debug, PartialEq)]
enum Convert {
    Keep,
    /// CHAR to VARCHAR: the padding is dropped.
    Unpad,
    Cast(DataType),
}

/// The output type of a column whose sides have types `left` and `right`,
/// and how to convert each side's values to it. Afterwards every value of
/// the column has one [`KeyClass`], so hashing agrees with comparison:
/// numbers of different classes meet as DOUBLE PRECISION, CHAR and VARCHAR
/// as VARCHAR, and any other mismatch takes the left side's type.
fn resolve_column(left: &DataType, right: &DataType) -> (DataType, Convert, Convert) {
    use KeyClass::*;
    let (lc, rc) = (KeyClass::of_type(left), KeyClass::of_type(right));
    let cast = |class: KeyClass, to: &DataType| {
        if class == KeyClass::of_type(to) {
            Convert::Keep
        } else {
            Convert::Cast(to.clone())
        }
    };
    match (lc, rc) {
        _ if lc == rc => (left.clone(), Convert::Keep, Convert::Keep),
        (Exact | Real | Double, Exact | Real | Double) => {
            let to = DataType::DoublePrecision;
            (to.clone(), cast(lc, &to), cast(rc, &to))
        }
        (Char, Varchar) | (Varchar, Char) => {
            let width = |t: &DataType| match t {
                DataType::Char(n) | DataType::Character(n) | DataType::Varchar(n) => *n,
                _ => 0,
            };
            let unpad = |c: KeyClass| {
                if c == Char {
                    Convert::Unpad
                } else {
                    Convert::Keep
                }
            };
            (
                DataType::Varchar(width(left).max(width(right))),
                unpad(lc),
                unpad(rc),
            )
        }
        _ => (left.clone(), Convert::Keep, Convert::Cast(left.clone())),
    }
}

/// Convert each value of `tuple` as `converts` says.
fn convert_row(tuple: &mut Tuple, converts: &[Convert]) -> RookResult<()> {
    for (value, convert) in tuple.values.iter_mut().zip(converts) {
        let Some(v) = value else { continue };
        match convert {
            Convert::Keep => {}
            Convert::Unpad => {
                if let DataValue::Char(s) = v {
                    *v = DataValue::Varchar(s.trim_end().to_string());
                }
            }
            Convert::Cast(to) => {
                *v = crate::types::functions::cast(v, to).context("set operation column type")?;
            }
        }
    }
    Ok(())
}

// ── SetOp Type ────────────────────────────────────────────────────────────────

/// Set operation type matching SQL semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpType {
    Union,
    Intersect,
    Except,
}

// ── SetOp Operator ────────────────────────────────────────────────────────────

/// A set-operation operator (UNION, INTERSECT, EXCEPT).
///
/// Implements standard SQL set operations between two child operators that
/// produce tuples with identical schemas.
pub struct SetOpOperator {
    left: Box<dyn PhysicalOperator>,
    right: Box<dyn PhysicalOperator>,
    op_type: SetOpType,
    all: bool,
    /// Output schema: the left child's names, with each column's type
    /// resolved from both children (see [`resolve_column`]).
    output_schema: Vec<ColumnInfo>,
    /// Per column, how each child's values are converted to its type.
    left_converts: Vec<Convert>,
    right_converts: Vec<Convert>,

    // ── Runtime state ──
    left_tuples: Vec<Tuple>,
    right_tuples: Vec<Tuple>,
    output_buffer: Vec<Tuple>,
    output_pos: usize,
    consumed: bool,
}

impl SetOpOperator {
    /// Create a new set operation operator.
    pub fn new(
        left: Box<dyn PhysicalOperator>,
        right: Box<dyn PhysicalOperator>,
        op_type: SetOpType,
        all: bool,
    ) -> Self {
        let mut output_schema = left.schema().to_vec();
        let mut left_converts = Vec::with_capacity(output_schema.len());
        let mut right_converts = Vec::with_capacity(output_schema.len());
        for (col, right_col) in output_schema.iter_mut().zip(right.schema()) {
            let (ty, l, r) = resolve_column(&col.data_type, &right_col.data_type);
            col.data_type = ty;
            left_converts.push(l);
            right_converts.push(r);
        }
        Self {
            left,
            right,
            op_type,
            all,
            output_schema,
            left_converts,
            right_converts,
            left_tuples: Vec::new(),
            right_tuples: Vec::new(),
            output_buffer: Vec::new(),
            output_pos: 0,
            consumed: false,
        }
    }

    /// Materialise both children and compute the set operation result.
    fn materialise(&mut self) -> RookResult<()> {
        self.left_tuples.clear();
        self.right_tuples.clear();
        let mut child_batch = Vec::with_capacity(super::trait_::DEFAULT_BATCH_SIZE);
        let keep = |c: &[Convert]| c.iter().all(|c| *c == Convert::Keep);
        let (keep_left, keep_right) = (keep(&self.left_converts), keep(&self.right_converts));
        while self.left.next_batch(&mut child_batch)? > 0 {
            if !keep_left {
                for tuple in &mut child_batch {
                    convert_row(tuple, &self.left_converts)?;
                }
            }
            self.left_tuples.append(&mut child_batch);
        }
        while self.right.next_batch(&mut child_batch)? > 0 {
            if !keep_right {
                for tuple in &mut child_batch {
                    convert_row(tuple, &self.right_converts)?;
                }
            }
            self.right_tuples.append(&mut child_batch);
        }

        match (self.op_type, self.all) {
            (SetOpType::Union, true) => {
                self.output_buffer = std::mem::take(&mut self.left_tuples);
                self.output_buffer.append(&mut self.right_tuples);
            }

            (SetOpType::Union, false) => {
                let mut seen = HashSet::new();
                for tuple in self.left_tuples.drain(..) {
                    if seen.insert(canonical_row_key(&tuple.values)) {
                        self.output_buffer.push(tuple);
                    }
                }
                for tuple in self.right_tuples.drain(..) {
                    if seen.insert(canonical_row_key(&tuple.values)) {
                        self.output_buffer.push(tuple);
                    }
                }
            }

            (SetOpType::Intersect, true) => {
                let mut left_counts: HashMap<Vec<Option<DataValue>>, (usize, Tuple)> =
                    HashMap::new();
                let mut order: Vec<Vec<Option<DataValue>>> = Vec::new();
                for tuple in self.left_tuples.drain(..) {
                    let key = canonical_row_key(&tuple.values);
                    let entry = left_counts.entry(key.clone()).or_insert((0, tuple));
                    entry.0 += 1;
                    if entry.0 == 1 {
                        order.push(key);
                    }
                }

                let mut results: HashMap<Vec<Option<DataValue>>, Vec<Tuple>> = HashMap::new();
                for tuple in self.right_tuples.drain(..) {
                    if let Some((count, _)) = left_counts.get_mut(&canonical_row_key(&tuple.values))
                        && *count > 0
                    {
                        *count -= 1;
                        results
                            .entry(canonical_row_key(&tuple.values))
                            .or_default()
                            .push(tuple);
                    }
                }

                for key in order {
                    if let Some(tuples) = results.remove(&key) {
                        self.output_buffer.extend(tuples);
                    }
                }
            }

            (SetOpType::Intersect, false) => {
                let left_set: HashSet<Vec<Option<DataValue>>> = self
                    .left_tuples
                    .iter()
                    .map(|t| canonical_row_key(&t.values))
                    .collect();
                let mut seen = HashSet::new();
                for tuple in self.right_tuples.drain(..) {
                    if left_set.contains(&canonical_row_key(&tuple.values))
                        && seen.insert(canonical_row_key(&tuple.values))
                    {
                        self.output_buffer.push(tuple);
                    }
                }
            }

            (SetOpType::Except, true) => {
                let mut left_counts: HashMap<Vec<Option<DataValue>>, (usize, Vec<Tuple>)> =
                    HashMap::new();
                let mut order: Vec<Vec<Option<DataValue>>> = Vec::new();
                for tuple in self.left_tuples.drain(..) {
                    let key = canonical_row_key(&tuple.values);
                    let entry = left_counts.entry(key.clone()).or_insert((0, Vec::new()));
                    entry.0 += 1;
                    entry.1.push(tuple);
                    if entry.0 == 1 {
                        order.push(key);
                    }
                }

                for tuple in self.right_tuples.drain(..) {
                    if let Some((count, _)) = left_counts.get_mut(&canonical_row_key(&tuple.values))
                        && *count > 0
                    {
                        *count -= 1;
                    }
                }

                for key in order {
                    if let Some((count, tuples)) = left_counts.remove(&key) {
                        for tuple in tuples.into_iter().take(count) {
                            self.output_buffer.push(tuple);
                        }
                    }
                }
            }

            (SetOpType::Except, false) => {
                let right_set: HashSet<Vec<Option<DataValue>>> = self
                    .right_tuples
                    .iter()
                    .map(|t| canonical_row_key(&t.values))
                    .collect();
                let mut seen = HashSet::new();
                for tuple in self.left_tuples.drain(..) {
                    if !right_set.contains(&canonical_row_key(&tuple.values))
                        && seen.insert(canonical_row_key(&tuple.values))
                    {
                        self.output_buffer.push(tuple);
                    }
                }
            }
        }

        self.consumed = true;
        Ok(())
    }
}

impl PhysicalOperator for SetOpOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if !self.consumed {
            self.materialise()?;
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
            self.materialise()?;
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
        self.left.reset()?;
        self.right.reset()?;
        self.left_tuples.clear();
        self.right_tuples.clear();
        self.output_buffer.clear();
        self.output_pos = 0;
        self.consumed = false;
        Ok(())
    }

    fn estimate_cardinality(&self) -> usize {
        let (left, right) = (
            self.left.estimate_cardinality(),
            self.right.estimate_cardinality(),
        );
        match self.op_type {
            SetOpType::Union => left + right,
            _ => left.min(right),
        }
    }

    fn name(&self) -> &'static str {
        match (self.op_type, self.all) {
            (SetOpType::Union, true) => "UnionAll",
            (SetOpType::Union, false) => "Union",
            (SetOpType::Intersect, true) => "IntersectAll",
            (SetOpType::Intersect, false) => "Intersect",
            (SetOpType::Except, true) => "ExceptAll",
            (SetOpType::Except, false) => "Except",
        }
    }

    fn explain(&self) -> PlanNode {
        PlanNode::new(self.name())
            .child(self.left.explain())
            .child(self.right.explain())
    }
}
