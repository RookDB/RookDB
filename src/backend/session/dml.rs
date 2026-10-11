//! INSERT, UPDATE, DELETE and queries.

use rook_ast::{DeletePlan, ExplainPlan, QueryPlan, UpdatePlan};

use super::StatementResult;
use crate::backend::error::{RookError, RookResult};
use std::sync::Arc;

use crate::backend::executor::physical::engine::{
    execute_shared, explain_analyze_shared, explain_shared,
};
use crate::backend::executor::physical::explain::PlanNode;
use crate::backend::executor::physical::tuple::{ColumnInfo, Tuple};
use crate::backend::executor::row_select::{matching_rows_plan, select_matching_pointers};
use crate::backend::executor::{delete_by_pointers, update_by_pointers};
use crate::catalog::Catalog;
use crate::types::{DataType, DataValue};

/// INSERT ... VALUES and INSERT ... SELECT, both through the planner.
pub(super) fn insert(
    catalog: &Arc<Catalog>,
    db: &str,
    plan: &QueryPlan,
) -> RookResult<StatementResult> {
    let logical = crate::planner::plan_query(plan, catalog, db)
        .map_err(|e| RookError::Internal(e.to_string()).with_context("Insert planning error"))?;
    // Inserting reports a count, not the written rows.
    let n = execute_shared(&logical, Arc::clone(catalog), db)
        .map_err(|e| e.with_context("Insert failed"))?
        .0
        .len();
    Ok(StatementResult::message(if n == 1 {
        "1 row inserted.\n".to_string()
    } else {
        format!("{} rows inserted.\n", n)
    }))
}

/// SELECT and set operations.
pub(super) fn query(
    catalog: &Arc<Catalog>,
    db: &str,
    plan: &QueryPlan,
) -> RookResult<StatementResult> {
    let logical = crate::planner::plan_query(plan, catalog, db)
        .map_err(|e| RookError::Internal(e.to_string()).with_context("Planning error"))?;
    let (rows, schema) = execute_shared(&logical, Arc::clone(catalog), db)
        .map_err(|e| e.with_context("Execution error"))?;
    let count = format!("{} row(s) returned.\n", rows.len());
    Ok(StatementResult {
        rows: Some((schema, rows)),
        messages: vec![count],
        plan: None,
    })
}

/// UPDATE: the planner finds the target rows, then they are rewritten in place.
pub(super) fn update(catalog: &Catalog, db: &str, upd: &UpdatePlan) -> RookResult<StatementResult> {
    let pointers = select_matching_pointers(catalog, db, &upd.table, upd.selection.clone())
        .map_err(|e| e.with_context("Update failed"))?;
    let result = update_by_pointers(catalog, db, &upd.table, &pointers, &upd.assignments)
        .map_err(|e| e.with_context("Update failed"))?;
    Ok(StatementResult::message(format!(
        "\nUpdated {} row(s).",
        result.updated_count
    )))
}

/// DELETE: the planner finds the target rows, then they are deleted.
pub(super) fn delete(catalog: &Catalog, db: &str, del: &DeletePlan) -> RookResult<StatementResult> {
    let pointers = select_matching_pointers(catalog, db, &del.table, del.selection.clone())
        .map_err(|e| e.with_context("Delete failed"))?;
    let result = delete_by_pointers(catalog, db, &del.table, &pointers)
        .map_err(|e| e.with_context("Delete failed"))?;
    Ok(StatementResult::message(format!(
        "\nDeleted {} row(s).",
        result.deleted_count
    )))
}

/// EXPLAIN [ANALYZE]: the physical plan of a statement, one operator per row.
///
/// Plain EXPLAIN plans the statement without running it. For UPDATE and
/// DELETE the plan is the one that finds the target rows. EXPLAIN ANALYZE
/// runs a query and adds what running it showed; it is refused for
/// statements that write.
pub(super) fn explain(
    catalog: &Arc<Catalog>,
    db: &str,
    explain: &ExplainPlan,
) -> RookResult<StatementResult> {
    let planning = |e: crate::planner::PlanError| {
        RookError::Internal(e.to_string()).with_context("Planning error")
    };
    // The plan to describe, and the writing operator it feeds, if any.
    let (logical, writer) = match explain.statement.as_ref() {
        statement @ (QueryPlan::Select(_) | QueryPlan::SetOperation(_) | QueryPlan::Insert(_)) => (
            crate::planner::plan_query(statement, catalog, db).map_err(planning)?,
            None,
        ),
        QueryPlan::Update(upd) => (
            matching_rows_plan(catalog, db, &upd.table, upd.selection.clone())
                .map_err(|e| e.with_context("Planning error"))?,
            Some(
                PlanNode::new("Update")
                    .with("table", &upd.table)
                    .with("assignments", upd.assignments.len()),
            ),
        ),
        QueryPlan::Delete(del) => (
            matching_rows_plan(catalog, db, &del.table, del.selection.clone())
                .map_err(|e| e.with_context("Planning error"))?,
            Some(PlanNode::new("Delete").with("table", &del.table)),
        ),
        other => {
            return Err(RookError::Internal(format!(
                "EXPLAIN supports SELECT, INSERT, UPDATE and DELETE, not {}",
                other.statement_type()
            )));
        }
    };

    let writes = writer.is_some() || matches!(*explain.statement, QueryPlan::Insert(_));
    let mut description = if !explain.analyze {
        explain_shared(&logical, Arc::clone(catalog), db)
            .map_err(|e| e.with_context("Planning error"))?
    } else if writes {
        return Err(RookError::Internal(
            "EXPLAIN ANALYZE runs the statement and supports only queries".to_string(),
        ));
    } else {
        let (mut description, rows) = explain_analyze_shared(&logical, Arc::clone(catalog), db)
            .map_err(|e| e.with_context("Execution error"))?;
        description
            .trace
            .notes
            .push(format!("Executed: {} row(s) returned", rows));
        description
    };
    if let Some(writer) = writer {
        description.root = writer.child(description.root);
    }

    let schema = vec![ColumnInfo {
        name: "plan".to_string(),
        data_type: DataType::Varchar(u16::MAX),
        table: None,
    }];
    let rows = description
        .lines()
        .into_iter()
        .map(|line| Tuple::new(vec![Some(DataValue::Varchar(line))]))
        .collect();
    Ok(StatementResult {
        rows: Some((schema, rows)),
        messages: Vec::new(),
        plan: Some(description),
    })
}
