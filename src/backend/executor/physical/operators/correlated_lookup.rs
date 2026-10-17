//! Hashed input for a correlated subquery evaluated once per outer row.
//!
//! A subquery such as `EXISTS (SELECT 1 FROM t WHERE t.id = outer.id)` is
//! planned as `Filter(t.id = $param)` over a scan of `t`, reset and re-run
//! for every outer row: a full scan of `t` per outer row. This operator sits
//! where that scan was. On first use it reads its input once and groups the
//! rows by the columns compared for equality with outer values; after each
//! `reset` it yields only the group matching the current parameter values.
//! The filter above still checks every condition, so a row this operator
//! passes through needlessly is harmless.
//!
//! If the input does not fit [`BUDGET_FACTOR`] times the hash-join memory
//! budget, the operator stops building and streams its input on every run,
//! as the plain scan did. It may use more than a hash join because it cannot
//! spill, and running out means a full scan per outer row.
//!
//! When the single key column has an index, the operator first looks each
//! outer value up in it instead: a probe costs per outer row, the hash
//! table per input row. A probe costs about two rows of building (B-tree
//! descent and a heap page per row), so once there have been half as many
//! probes as the table has rows, or an outer value cannot be looked up
//! exactly (it does not convert to the key type without loss), it builds
//! the hash table and uses that from then on.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use super::super::explain::PlanNode;
use super::super::planner::PhysicalPlanner;
use super::super::tuple::{ColumnInfo, Tuple};
use super::scans::IndexScanOperator;
use super::trait_::PhysicalOperator;
use super::utils::canonical_key;
use crate::backend::error::RookResult;
use crate::types::datatype::DataType;
use crate::types::value::DataValue;

/// Multiple of the hash-join budget the grouped input may occupy.
const BUDGET_FACTOR: usize = 4;
/// Charge for a new group: its key vector and hash bucket.
const GROUP_OVERHEAD: usize = 48;
/// What one index probe costs in rows of hash-table building.
const PROBE_COST: usize = 2;
/// End of a group's row chain.
const END: u32 = u32::MAX;

/// Memory held by one stored row: the tuple, its values and string payloads.
fn row_bytes(tuple: &Tuple) -> usize {
    let mut bytes = std::mem::size_of::<Tuple>()
        + tuple.values.len() * std::mem::size_of::<Option<DataValue>>();
    for value in tuple.values.iter().flatten() {
        if let DataValue::Char(s) | DataValue::Varchar(s) | DataValue::Bit(s) = value {
            bytes += s.len();
        }
    }
    bytes + std::mem::size_of::<u32>()
}

/// The index lookup used while there are fewer outer rows than input rows.
struct Probe {
    scan: IndexScanOperator,
    key_type: DataType,
    /// Probes left before the hash table is cheaper.
    runs_left: usize,
}

/// `value` as a key of `key_type` that the index matches exactly the rows
/// `key_column = value` holds for; `None` when the conversion could lose
/// something (a CHAR padded, a NUMERIC rescaled, an integer made a float).
fn probe_key(value: &DataValue, key_type: &DataType) -> Option<DataValue> {
    let integer = |v: &DataValue| {
        matches!(
            v,
            DataValue::SmallInt(_) | DataValue::Int(_) | DataValue::BigInt(_)
        )
    };
    let key = PhysicalPlanner::coerce_to_key_type(value.clone(), key_type)?;
    (key == *value || (integer(value) && integer(&key))).then_some(key)
}

enum State {
    /// The input has not been read yet.
    Unbuilt,
    /// Looking each run's key up in the index; `None` until the run's first
    /// `next`, then whether the run has rows to return.
    Probing(Option<bool>),
    /// The input rows in arrival order. Each group is a chain through
    /// `next`, from its first row to its last; `cursor` is the next row of
    /// the current run's group, `None` until the run's first `next`.
    Built {
        rows: Vec<Tuple>,
        next: Vec<u32>,
        groups: HashMap<Vec<DataValue>, (u32, u32)>,
        cursor: Option<u32>,
    },
    /// Over budget: every run reads the input again.
    Streaming,
}

pub struct CorrelatedLookupOperator {
    child: Box<dyn PhysicalOperator>,
    /// Positions in the child's schema compared for equality with `params`.
    key_columns: Vec<usize>,
    /// The outer values for the current run, set before each `reset`.
    params: Vec<Rc<RefCell<Option<DataValue>>>>,
    budget: usize,
    state: State,
    /// Reused for each run's lookup key.
    key_buf: Vec<DataValue>,
    probe: Option<Probe>,
}

impl CorrelatedLookupOperator {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        key_columns: Vec<usize>,
        params: Vec<Rc<RefCell<Option<DataValue>>>>,
    ) -> Self {
        debug_assert_eq!(key_columns.len(), params.len());
        Self {
            child,
            key_columns,
            params,
            budget: super::joins::spill_budget().saturating_mul(BUDGET_FACTOR),
            state: State::Unbuilt,
            key_buf: Vec::new(),
            probe: None,
        }
    }

    /// Look keys up through `scan`, an index on the single key column of
    /// type `key_type`, until probing costs more than the hash table.
    pub fn with_index_probe(mut self, scan: IndexScanOperator, key_type: DataType) -> Self {
        if self.key_columns.len() == 1 {
            let runs_left = scan.table_rows() / PROBE_COST;
            self.probe = Some(Probe {
                scan,
                key_type,
                runs_left,
            });
        }
        self
    }

    /// Point the probe at this run's outer value. `Some(has_rows)` when the
    /// index can answer the run; `None` when the hash table must take over.
    fn start_probe(&mut self) -> Option<bool> {
        let probe = self.probe.as_mut()?;
        if probe.runs_left == 0 {
            return None;
        }
        probe.runs_left -= 1;
        // `col = NULL` holds for no row.
        let Some(value) = self.params[0].borrow().clone() else {
            return Some(false);
        };
        let key = probe_key(&value, &probe.key_type)?;
        probe.scan.repoint(key);
        Some(true)
    }

    /// `values` at the key columns as a hash key in `buf`; false when one
    /// is NULL, since `col = NULL` holds for no row.
    fn fill_key<'a>(
        values: impl Iterator<Item = &'a Option<DataValue>>,
        buf: &mut Vec<DataValue>,
    ) -> bool {
        buf.clear();
        for value in values {
            let Some(value) = value else { return false };
            buf.push(canonical_key(value.clone()));
        }
        true
    }

    /// Read the whole input into groups, or switch to streaming when it
    /// outgrows the budget.
    fn build(&mut self) -> RookResult<()> {
        let mut rows: Vec<Tuple> = Vec::new();
        let mut next: Vec<u32> = Vec::new();
        let mut groups: HashMap<Vec<DataValue>, (u32, u32)> = HashMap::new();
        let mut bytes = 0usize;
        let mut batch = Vec::with_capacity(super::trait_::DEFAULT_BATCH_SIZE);
        while self.child.next_batch(&mut batch)? > 0 {
            for tuple in batch.drain(..) {
                let key_values = self.key_columns.iter().map(|&i| &tuple.values[i]);
                if !Self::fill_key(key_values, &mut self.key_buf) {
                    continue;
                }
                bytes += row_bytes(&tuple);
                let idx = rows.len() as u32;
                match groups.get_mut(self.key_buf.as_slice()) {
                    Some((_, last)) => {
                        next[*last as usize] = idx;
                        *last = idx;
                    }
                    None => {
                        bytes +=
                            GROUP_OVERHEAD + self.key_buf.len() * std::mem::size_of::<DataValue>();
                        groups.insert(self.key_buf.clone(), (idx, idx));
                    }
                }
                if bytes > self.budget {
                    self.child.reset()?;
                    self.state = State::Streaming;
                    return Ok(());
                }
                rows.push(tuple);
                next.push(END);
            }
        }
        self.state = State::Built {
            rows,
            next,
            groups,
            cursor: None,
        };
        Ok(())
    }
}

impl PhysicalOperator for CorrelatedLookupOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if matches!(self.state, State::Unbuilt) {
            if self.probe.is_some() {
                self.state = State::Probing(None);
            } else {
                self.build()?;
            }
        }
        if let State::Probing(run) = self.state {
            let has_rows = match run {
                Some(has_rows) => has_rows,
                None => match self.start_probe() {
                    Some(has_rows) => {
                        self.state = State::Probing(Some(has_rows));
                        has_rows
                    }
                    None => {
                        self.probe = None;
                        self.build()?;
                        return self.next();
                    }
                },
            };
            return match &mut self.probe {
                Some(probe) if has_rows => probe.scan.next(),
                _ => Ok(None),
            };
        }
        match &mut self.state {
            State::Unbuilt | State::Probing(_) => unreachable!("handled above"),
            State::Streaming => self.child.next(),
            State::Built {
                rows,
                next,
                groups,
                cursor,
            } => {
                let idx = match *cursor {
                    Some(idx) => idx,
                    None => {
                        let mut keyed = true;
                        self.key_buf.clear();
                        for param in &self.params {
                            match &*param.borrow() {
                                Some(v) => self.key_buf.push(canonical_key(v.clone())),
                                None => {
                                    keyed = false;
                                    break;
                                }
                            }
                        }
                        match groups.get(self.key_buf.as_slice()) {
                            Some(&(first, _)) if keyed => first,
                            _ => END,
                        }
                    }
                };
                if idx == END {
                    *cursor = Some(END);
                    return Ok(None);
                }
                *cursor = Some(next[idx as usize]);
                Ok(Some(rows[idx as usize].clone()))
            }
        }
    }

    fn schema(&self) -> &[ColumnInfo] {
        self.child.schema()
    }

    fn reset(&mut self) -> RookResult<()> {
        match &mut self.state {
            State::Unbuilt => Ok(()),
            State::Probing(run) => {
                *run = None;
                Ok(())
            }
            State::Streaming => self.child.reset(),
            State::Built { cursor, .. } => {
                *cursor = None;
                Ok(())
            }
        }
    }

    fn estimate_cardinality(&self) -> usize {
        self.child.estimate_cardinality()
    }

    fn name(&self) -> &'static str {
        "CorrelatedLookup"
    }

    fn explain(&self) -> PlanNode {
        let mut node = PlanNode::new("CorrelatedLookup").with("keys", self.key_columns.len());
        match &self.state {
            State::Unbuilt => {}
            State::Probing(_) => {}
            State::Built { groups, .. } => node = node.with("groups", groups.len()),
            State::Streaming => node = node.with("build", "over budget, streamed"),
        }
        if let Some(probe) = &self.probe {
            node = node.with("probe", "index").child(probe.scan.explain());
        }
        node.child(self.child.explain())
    }
}
