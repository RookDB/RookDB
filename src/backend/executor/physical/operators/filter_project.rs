use std::collections::HashSet;

use super::super::explain::PlanNode;
use super::super::expr::{Expr, Predicate, evaluate_predicate};
use super::super::tuple::{ColumnInfo, Tuple};
use super::trait_::PhysicalOperator;
use crate::backend::error::RookResult;

use crate::types::datatype::DataType;
use crate::types::value::DataValue;

// ── SingleRowOperator ────────────────────────────────────────────────────────

/// An operator that produces exactly one empty tuple (zero-column row).
pub struct SingleRowOperator {
    schema: Vec<ColumnInfo>,
    emitted: bool,
}

impl Default for SingleRowOperator {
    fn default() -> Self {
        Self::new()
    }
}

impl SingleRowOperator {
    pub fn new() -> Self {
        Self {
            schema: Vec::new(),
            emitted: false,
        }
    }
}

impl PhysicalOperator for SingleRowOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(Tuple::new(Vec::new())))
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.emitted = false;
        Ok(())
    }

    fn estimate_cardinality(&self) -> usize {
        1
    }

    fn name(&self) -> &'static str {
        "SingleRow"
    }
}

// ── Null Source ───────────────────────────────────────────────────────────────

/// An operator that produces no tuples — used as a placeholder.
pub struct NullOperator {
    schema: Vec<ColumnInfo>,
}

impl NullOperator {
    pub fn new(schema: Vec<ColumnInfo>) -> Self {
        Self { schema }
    }
}

impl PhysicalOperator for NullOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        Ok(None)
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn estimate_cardinality(&self) -> usize {
        0
    }

    fn name(&self) -> &'static str {
        "Null"
    }
}

// ── CteScan Operator ──────────────────────────────────────────────────────────

/// Scans over a pre-materialised CTE result set stored in memory.
pub struct CteScanOperator {
    /// All tuples produced by the CTE, in evaluation order.
    tuples: Vec<Tuple>,
    /// Current read position.
    pos: usize,
    /// Schema forwarded from the CTE's output.
    schema: Vec<ColumnInfo>,
}

impl CteScanOperator {
    /// Create a new CTE scan operator.
    pub fn new(tuples: Vec<Tuple>, schema: Vec<ColumnInfo>) -> Self {
        Self {
            tuples,
            pos: 0,
            schema,
        }
    }
}

impl PhysicalOperator for CteScanOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if self.pos >= self.tuples.len() {
            return Ok(None);
        }
        let tuple = self.tuples[self.pos].clone();
        self.pos += 1;
        Ok(Some(tuple))
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.pos = 0;
        Ok(())
    }

    fn estimate_cardinality(&self) -> usize {
        self.tuples.len()
    }

    fn name(&self) -> &'static str {
        "CteScan"
    }

    fn explain(&self) -> PlanNode {
        let mut node = PlanNode::new("CteScan");
        if let Some(name) = self.schema.first().and_then(|c| c.table.as_ref()) {
            node = node.with("cte", name);
        }
        node.with("rows", self.tuples.len())
    }
}

// ── Filter Operator ───────────────────────────────────────────────────────────

/// Filters tuples from a child operator by evaluating a predicate.
pub struct FilterOperator {
    child: Box<dyn PhysicalOperator>,
    predicate: Predicate,
    schema: Vec<ColumnInfo>,
    child_buffer: Vec<Tuple>,
    buffer_pos: usize,
}

impl FilterOperator {
    pub fn new(child: Box<dyn PhysicalOperator>, predicate: Predicate) -> Self {
        let schema = child.schema().to_vec();
        Self {
            child,
            predicate,
            schema,
            child_buffer: Vec::new(),
            buffer_pos: 0,
        }
    }
}

impl PhysicalOperator for FilterOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        let child_schema = &self.schema;
        loop {
            if self.buffer_pos < self.child_buffer.len() {
                let tuple = &self.child_buffer[self.buffer_pos];
                self.buffer_pos += 1;
                match evaluate_predicate(&self.predicate, tuple, child_schema)? {
                    Some(true) => return Ok(Some(tuple.clone())),
                    _ => continue,
                }
            }

            self.child_buffer.clear();
            self.buffer_pos = 0;
            let count = self.child.next_batch(&mut self.child_buffer)?;
            if count == 0 {
                return Ok(None);
            }
        }
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        batch.clear();
        let child_schema = &self.schema;
        while batch.len() < super::trait_::DEFAULT_BATCH_SIZE {
            if self.buffer_pos >= self.child_buffer.len() {
                self.child_buffer.clear();
                self.buffer_pos = 0;
                let count = self.child.next_batch(&mut self.child_buffer)?;
                if count == 0 {
                    break;
                }
            }

            while self.buffer_pos < self.child_buffer.len()
                && batch.len() < super::trait_::DEFAULT_BATCH_SIZE
            {
                let tuple = &self.child_buffer[self.buffer_pos];
                self.buffer_pos += 1;
                match evaluate_predicate(&self.predicate, tuple, child_schema)? {
                    Some(true) => batch.push(tuple.clone()),
                    _ => continue,
                }
            }
        }
        Ok(batch.len())
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.child_buffer.clear();
        self.buffer_pos = 0;
        self.child.reset()
    }

    fn estimate_cardinality(&self) -> usize {
        // A tenth, the factor used where nothing is known of the data.
        (self.child.estimate_cardinality() / 10).max(1)
    }

    fn name(&self) -> &'static str {
        "Filter"
    }

    fn explain(&self) -> PlanNode {
        PlanNode::new("Filter").child(self.child.explain())
    }

    fn ordering(&self) -> Option<Vec<(usize, bool)>> {
        self.child.ordering()
    }
}

// ── Projection Operator ───────────────────────────────────────────────────────

/// Evaluates expressions to produce projected output tuples.
pub struct ProjectionOperator {
    child: Box<dyn PhysicalOperator>,
    projections: Vec<(Expr, String, DataType)>,
    child_schema: Vec<ColumnInfo>,
    output_schema: Vec<ColumnInfo>,
    child_buffer: Vec<Tuple>,
    buffer_pos: usize,
    /// When every output is a plain child column: its position, and whether
    /// this is the column's last use (its value is then moved, not cloned).
    columns: Option<Vec<(usize, bool)>>,
}

/// `(position, last use)` for each of `positions`.
fn bind_columns(positions: &[usize]) -> Vec<(usize, bool)> {
    positions
        .iter()
        .enumerate()
        .map(|(i, &p)| (p, !positions[i + 1..].contains(&p)))
        .collect()
}

/// Build one output row from `child`, moving values out of it where the
/// projection allows.
fn project_row(
    columns: &Option<Vec<(usize, bool)>>,
    projections: &[(Expr, String, DataType)],
    schema: &[ColumnInfo],
    child: &mut Tuple,
) -> RookResult<Tuple> {
    let values = match columns {
        Some(cols) => cols
            .iter()
            .map(|&(i, last)| {
                if last {
                    std::mem::take(&mut child.values[i])
                } else {
                    child.values[i].clone()
                }
            })
            .collect(),
        None => {
            let mut values = Vec::with_capacity(projections.len());
            for (expr, _, _) in projections {
                values.push(expr.evaluate(child, schema)?);
            }
            values
        }
    };
    Ok(Tuple::new(values).with_location_from(child))
}

impl ProjectionOperator {
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        projections: Vec<(Expr, String, DataType)>,
    ) -> Self {
        let child_schema = child.schema().to_vec();
        let output_schema: Vec<ColumnInfo> = projections
            .iter()
            .map(|(_, name, dt)| ColumnInfo {
                merged: false,
                name: name.clone(),
                data_type: dt.clone(),
                table: None,
            })
            .collect();
        // Plain column references are resolved once, here, not per row.
        let positions: Option<Vec<usize>> = projections
            .iter()
            .map(|(expr, _, _)| match expr {
                Expr::Column { table, column } => {
                    crate::backend::executor::physical::expr::column_position(
                        &child_schema,
                        table.as_deref(),
                        column,
                    )
                }
                _ => None,
            })
            .collect();
        Self {
            child,
            projections,
            child_schema,
            output_schema,
            child_buffer: Vec::new(),
            buffer_pos: 0,
            columns: positions.as_deref().map(bind_columns),
        }
    }

    pub fn new_with_mapping(
        child: Box<dyn PhysicalOperator>,
        indices: &[usize],
        names: &[String],
    ) -> Result<Self, String> {
        let child_schema = child.schema();
        let mut projections = Vec::with_capacity(indices.len());
        for (i, &idx) in indices.iter().enumerate() {
            let name = names.get(i).cloned().unwrap_or_else(|| {
                child_schema
                    .get(idx)
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| format!("col{}", idx))
            });
            let dt = child_schema
                .get(idx)
                .map(|c| c.data_type.clone())
                .ok_or_else(|| format!("Column index {} out of bounds", idx))?;
            projections.push((
                Expr::Column {
                    table: None,
                    column: name.clone(),
                },
                name,
                dt,
            ));
        }
        // Bind to the given positions: a name could match an earlier column
        // of the same name (after a join).
        let mut op = Self::new(child, projections);
        op.columns = Some(bind_columns(indices));
        Ok(op)
    }

    pub fn from_indices(
        child: Box<dyn PhysicalOperator>,
        indices: &[usize],
        names: &[String],
    ) -> Result<Self, String> {
        Self::new_with_mapping(child, indices, names)
    }

    pub fn star(child: Box<dyn PhysicalOperator>) -> Self {
        let child_schema = child.schema().to_vec();
        let projections: Vec<(Expr, String, DataType)> = child_schema
            .iter()
            .map(|ci| {
                (
                    Expr::Column {
                        table: ci.table.clone(),
                        column: ci.name.clone(),
                    },
                    ci.name.clone(),
                    ci.data_type.clone(),
                )
            })
            .collect();
        let output_schema = child_schema.clone();
        let all: Vec<usize> = (0..child_schema.len()).collect();
        Self {
            child,
            projections,
            child_schema,
            output_schema,
            child_buffer: Vec::new(),
            buffer_pos: 0,
            columns: Some(bind_columns(&all)),
        }
    }
}

impl PhysicalOperator for ProjectionOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        let child_schema = &self.child_schema;
        if self.buffer_pos >= self.child_buffer.len() {
            self.child_buffer.clear();
            self.buffer_pos = 0;
            let count = self.child.next_batch(&mut self.child_buffer)?;
            if count == 0 {
                return Ok(None);
            }
        }

        let child_tuple = &mut self.child_buffer[self.buffer_pos];
        self.buffer_pos += 1;
        Ok(Some(project_row(
            &self.columns,
            &self.projections,
            child_schema,
            child_tuple,
        )?))
    }

    fn next_batch(&mut self, batch: &mut Vec<Tuple>) -> RookResult<usize> {
        batch.clear();
        let child_schema = &self.child_schema;

        // Drain any remaining items in child_buffer
        while self.buffer_pos < self.child_buffer.len()
            && batch.len() < super::trait_::DEFAULT_BATCH_SIZE
        {
            let child_tuple = &mut self.child_buffer[self.buffer_pos];
            self.buffer_pos += 1;
            batch.push(project_row(
                &self.columns,
                &self.projections,
                child_schema,
                child_tuple,
            )?);
        }

        if batch.len() >= super::trait_::DEFAULT_BATCH_SIZE {
            return Ok(batch.len());
        }

        // Pull new batch from child
        self.child_buffer.clear();
        self.buffer_pos = 0;
        let count = self.child.next_batch(&mut self.child_buffer)?;
        if count == 0 {
            return Ok(batch.len());
        }

        while self.buffer_pos < self.child_buffer.len()
            && batch.len() < super::trait_::DEFAULT_BATCH_SIZE
        {
            let child_tuple = &mut self.child_buffer[self.buffer_pos];
            self.buffer_pos += 1;
            batch.push(project_row(
                &self.columns,
                &self.projections,
                child_schema,
                child_tuple,
            )?);
        }

        Ok(batch.len())
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.child_buffer.clear();
        self.buffer_pos = 0;
        self.child.reset()
    }

    fn estimate_cardinality(&self) -> usize {
        self.child.estimate_cardinality()
    }

    fn name(&self) -> &'static str {
        "Project"
    }

    fn explain(&self) -> PlanNode {
        let columns: Vec<&str> = self.output_schema.iter().map(|c| c.name.as_str()).collect();
        PlanNode::new("Project")
            .with("columns", columns.join(", "))
            .child(self.child.explain())
    }

    fn ordering(&self) -> Option<Vec<(usize, bool)>> {
        if let Some(child_order) = self.child.ordering() {
            let mut mapped_order = Vec::new();
            for (child_idx, desc) in child_order {
                let child_col_name = self.child_schema.get(child_idx).map(|c| &c.name)?;
                if let Some(pos) = self.projections.iter().position(|(expr, _, _)| {
                    matches!(expr, Expr::Column { column, .. } if column == child_col_name)
                }) {
                    mapped_order.push((pos, desc));
                } else {
                    break;
                }
            }
            if !mapped_order.is_empty() {
                return Some(mapped_order);
            }
        }
        None
    }
}

// ── Limit Operator ────────────────────────────────────────────────────────────

/// Limits the number of tuples produced (SQL `LIMIT`).
pub struct LimitOperator {
    child: Box<dyn PhysicalOperator>,
    limit: usize,
    offset: usize,
    emitted: usize,
    skipped: usize,
}

impl LimitOperator {
    pub fn new(child: Box<dyn PhysicalOperator>, limit: usize, offset: usize) -> Self {
        Self {
            child,
            limit,
            offset,
            emitted: 0,
            skipped: 0,
        }
    }
}

impl PhysicalOperator for LimitOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        while self.skipped < self.offset {
            match self.child.next()? {
                Some(_) => self.skipped += 1,
                None => return Ok(None),
            }
        }

        if self.emitted >= self.limit {
            return Ok(None);
        }

        match self.child.next()? {
            Some(tuple) => {
                self.emitted += 1;
                Ok(Some(tuple))
            }
            None => Ok(None),
        }
    }

    fn schema(&self) -> &[ColumnInfo] {
        self.child.schema()
    }

    fn reset(&mut self) -> RookResult<()> {
        self.child.reset()?;
        self.emitted = 0;
        self.skipped = 0;
        Ok(())
    }

    fn estimate_cardinality(&self) -> usize {
        self.child
            .estimate_cardinality()
            .saturating_sub(self.offset)
            .min(self.limit)
    }

    fn name(&self) -> &'static str {
        "Limit"
    }

    fn explain(&self) -> PlanNode {
        let mut node = PlanNode::new("Limit").with("limit", self.limit);
        if self.offset > 0 {
            node = node.with("offset", self.offset);
        }
        node.child(self.child.explain())
    }

    fn ordering(&self) -> Option<Vec<(usize, bool)>> {
        self.child.ordering()
    }
}

// ── Distinct Operator ─────────────────────────────────────────────────────────

/// Removes duplicate tuples (SQL `DISTINCT`).
pub struct DistinctOperator {
    child: Box<dyn PhysicalOperator>,
    seen: HashSet<Vec<Option<DataValue>>>,
    loaded: bool,
    buffer: Vec<Tuple>,
    pos: usize,
}

impl DistinctOperator {
    pub fn new(child: Box<dyn PhysicalOperator>) -> Self {
        Self {
            child,
            seen: HashSet::new(),
            loaded: false,
            buffer: Vec::new(),
            pos: 0,
        }
    }
}

impl PhysicalOperator for DistinctOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if !self.loaded {
            while let Some(tuple) = self.child.next()? {
                if self.seen.insert(tuple.values.clone()) {
                    self.buffer.push(tuple);
                }
            }
            self.loaded = true;
        }

        if self.pos < self.buffer.len() {
            let tuple = self.buffer[self.pos].clone();
            self.pos += 1;
            Ok(Some(tuple))
        } else {
            Ok(None)
        }
    }

    fn schema(&self) -> &[ColumnInfo] {
        self.child.schema()
    }

    fn reset(&mut self) -> RookResult<()> {
        self.child.reset()?;
        self.seen.clear();
        self.buffer.clear();
        self.pos = 0;
        self.loaded = false;
        Ok(())
    }

    fn estimate_cardinality(&self) -> usize {
        self.child.estimate_cardinality() / 5 * 4
    }

    fn name(&self) -> &'static str {
        "Distinct"
    }

    fn explain(&self) -> PlanNode {
        PlanNode::new("Distinct").child(self.child.explain())
    }
}
