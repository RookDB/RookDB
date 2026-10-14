use super::super::explain::PlanNode;
use super::super::tuple::{ColumnInfo, Tuple};
use super::trait_::PhysicalOperator;
use crate::backend::error::RookResult;
use crate::types::datatype::DataType;
use crate::types::value::DataValue;

// ── SubqueryType ─────────────────────────────────────────────────────────────

/// Mode of subquery execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubqueryType {
    /// Scalar subquery: must return exactly one row/column.
    Scalar,
    /// EXISTS subquery: returns true if subquery has any rows.
    Exists,
}

// ── SubqueryExec Operator ─────────────────────────────────────────────────────

/// Executes a child operator as a subquery (scalar or EXISTS).
///
/// # Scalar mode
/// Materialises the child plan, asserts exactly one row with one column,
/// and yields that single tuple.
///
/// # EXISTS mode
/// Executes the child plan and yields a single boolean tuple indicating
/// whether any rows exist.
pub struct SubqueryExecOperator {
    child: Box<dyn PhysicalOperator>,
    subquery_type: SubqueryType,
    output_schema: Vec<ColumnInfo>,
    evaluated: bool,
    result: Option<Tuple>,
}

impl SubqueryExecOperator {
    /// Create a new subquery execution operator.
    pub fn new(
        child: Box<dyn PhysicalOperator>,
        subquery_type: SubqueryType,
        _alias: Option<&str>,
    ) -> Self {
        let output_schema = match subquery_type {
            SubqueryType::Scalar => child.schema().to_vec(),
            SubqueryType::Exists => {
                vec![ColumnInfo {
                    merged: false,
                    name: "exists".to_string(),
                    data_type: DataType::Bool,
                    table: None,
                }]
            }
        };

        Self {
            child,
            subquery_type,
            output_schema,
            evaluated: false,
            result: None,
        }
    }

    /// Evaluate the subquery and store the result.
    fn evaluate(&mut self) -> RookResult<()> {
        match self.subquery_type {
            SubqueryType::Scalar => {
                let mut tuples: Vec<Tuple> = Vec::new();
                while let Some(t) = self.child.next()? {
                    tuples.push(t);
                }

                if tuples.len() > 1 {
                    return Err(format!(
                        "Scalar subquery returned more than one row (got {})",
                        tuples.len()
                    )
                    .into());
                }

                if let Some(tuple) = tuples.into_iter().next() {
                    self.result = Some(tuple);
                }
            }
            SubqueryType::Exists => {
                let exists = self.child.next()?.is_some();

                let bool_val = DataValue::Bool(exists);
                self.result = Some(Tuple::new(vec![Some(bool_val)]));
            }
        }

        self.evaluated = true;
        Ok(())
    }
}

impl PhysicalOperator for SubqueryExecOperator {
    fn next(&mut self) -> RookResult<Option<Tuple>> {
        if !self.evaluated {
            self.evaluate()?;
        }

        match self.result.take() {
            Some(tuple) => Ok(Some(tuple)),
            None => Ok(None),
        }
    }

    fn schema(&self) -> &[ColumnInfo] {
        &self.output_schema
    }

    fn reset(&mut self) -> RookResult<()> {
        self.child.reset()?;
        self.evaluated = false;
        self.result = None;
        Ok(())
    }

    fn estimate_cardinality(&self) -> usize {
        1
    }

    fn name(&self) -> &'static str {
        match self.subquery_type {
            SubqueryType::Scalar => "SubqueryExec(Scalar)",
            SubqueryType::Exists => "SubqueryExec(Exists)",
        }
    }

    fn explain(&self) -> PlanNode {
        PlanNode::new(self.name()).child(self.child.explain())
    }
}
