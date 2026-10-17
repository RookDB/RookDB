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
//!
//! Hashing finds every matching row only when the outer value shares a
//! [`KeyClass`] with the column's values (see there). A run whose outer
//! value does not, such as text against a DATE column, returns every row
//! and leaves the comparison to the filter.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use super::super::explain::PlanNode;
use super::super::planner::PhysicalPlanner;
use super::super::tuple::{ColumnInfo, Tuple};
use super::scans::IndexScanOperator;
use super::trait_::PhysicalOperator;
use super::utils::{KeyClass, canonical_key};
use crate::backend::error::RookResult;
use crate::types::datatype::DataType;
use crate::types::value::{DataValue, NumericValue};

/// Multiple of the hash-join budget the grouped input may occupy.
const BUDGET_FACTOR: usize = 4;
/// Charge for a new group: its key vector and hash bucket.
const GROUP_OVERHEAD: usize = 48;
/// What one index probe costs in rows of hash-table building.
const PROBE_COST: usize = 2;
/// End of a group's row chain.
const END: u32 = u32::MAX;

/// Memory held by one stored row: the tuple, its values and their heap
/// payloads.
fn row_bytes(tuple: &Tuple) -> usize {
    let mut bytes = std::mem::size_of::<Tuple>()
        + tuple.values.len() * std::mem::size_of::<Option<DataValue>>();
    for value in tuple.values.iter().flatten() {
        match value {
            DataValue::Char(s) | DataValue::Varchar(s) | DataValue::Bit(s) => bytes += s.len(),
            DataValue::Numeric(_) => bytes += std::mem::size_of::<NumericValue>(),
            _ => {}
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

/// The key classes a key column's stored values have.
#[derive(Clone, Copy, PartialEq)]
enum Classes {
    Nothing,
    One(KeyClass),
    Mixed,
}

impl Classes {
    fn add(&mut self, class: KeyClass) {
        *self = match *self {
            Classes::Nothing => Classes::One(class),
            Classes::One(c) if c == class => Classes::One(c),
            _ => Classes::Mixed,
        };
    }

    /// Whether hashing a value of `class` finds every stored row that
    /// compares equal to it.
    fn hashes_alike(self, class: KeyClass) -> bool {
        self == Classes::Nothing || self == Classes::One(class)
    }
}

/// Where a run of the built table is.
#[derive(Clone, Copy)]
enum Cursor {
    /// The next row of the run's group, or [`END`].
    Chain(u32),
    /// The next row of all of them, for a key that cannot be hashed.
    All(u32),
}

enum State {
    /// The input has not been read yet.
    Unbuilt,
    /// Looking each run's key up in the index; `None` until the run's first
    /// `next`, then whether the run has rows to return.
    Probing(Option<bool>),
    /// The input rows in arrival order, less those with a NULL key. Each
    /// group is a chain through `next`, from its first row to its last;
    /// `classes` holds each key column's classes; `cursor` is `None` until
    /// the run's first `next`.
    Built {
        rows: Vec<Tuple>,
        next: Vec<u32>,
        groups: HashMap<Vec<DataValue>, (u32, u32)>,
        classes: Vec<Classes>,
        cursor: Option<Cursor>,
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
    /// Reused for each run's lookup key and its values' classes.
    key_buf: Vec<DataValue>,
    class_buf: Vec<KeyClass>,
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
            class_buf: Vec::new(),
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

    /// `values` at the key columns as a hash key in `buf`, with their
    /// classes in `classes`; false when one is NULL, since `col = NULL`
    /// holds for no row.
    fn fill_key(
        values: impl Iterator<Item = Option<DataValue>>,
        buf: &mut Vec<DataValue>,
        classes: &mut Vec<KeyClass>,
    ) -> bool {
        buf.clear();
        classes.clear();
        for value in values {
            let Some(value) = value else { return false };
            classes.push(KeyClass::of_value(&value));
            buf.push(canonical_key(value));
        }
        true
    }

    /// Read the whole input into groups, or switch to streaming when it
    /// outgrows the budget.
    fn build(&mut self) -> RookResult<()> {
        let mut rows: Vec<Tuple> = Vec::new();
        let mut next: Vec<u32> = Vec::new();
        let mut groups: HashMap<Vec<DataValue>, (u32, u32)> = HashMap::new();
        let mut classes = vec![Classes::Nothing; self.key_columns.len()];
        let mut bytes = 0usize;
        let mut batch = Vec::with_capacity(super::trait_::DEFAULT_BATCH_SIZE);
        while self.child.next_batch(&mut batch)? > 0 {
            for tuple in batch.drain(..) {
                let key_values = self.key_columns.iter().map(|&i| tuple.values[i].clone());
                if !Self::fill_key(key_values, &mut self.key_buf, &mut self.class_buf) {
                    continue;
                }
                for (seen, &class) in classes.iter_mut().zip(&self.class_buf) {
                    seen.add(class);
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
            classes,
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
                classes,
                cursor,
            } => {
                let run = match *cursor {
                    Some(run) => run,
                    None => {
                        let params = self.params.iter().map(|p| p.borrow().clone());
                        if !Self::fill_key(params, &mut self.key_buf, &mut self.class_buf) {
                            Cursor::Chain(END)
                        } else if classes
                            .iter()
                            .zip(&self.class_buf)
                            .all(|(seen, &class)| seen.hashes_alike(class))
                        {
                            match groups.get(self.key_buf.as_slice()) {
                                Some(&(first, _)) => Cursor::Chain(first),
                                None => Cursor::Chain(END),
                            }
                        } else {
                            Cursor::All(0)
                        }
                    }
                };
                let (idx, after) = match run {
                    Cursor::Chain(idx) if idx != END => (idx, Cursor::Chain(next[idx as usize])),
                    Cursor::All(idx) if (idx as usize) < rows.len() => (idx, Cursor::All(idx + 1)),
                    done => {
                        *cursor = Some(done);
                        return Ok(None);
                    }
                };
                *cursor = Some(after);
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
