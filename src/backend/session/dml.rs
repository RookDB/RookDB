//! INSERT, UPDATE, DELETE and queries.

use rook_ast::{DeletePlan, QueryPlan, UpdatePlan};

use super::StatementResult;
use crate::backend::error::{RookError, RookResult};
use crate::backend::executor::physical::engine::{
    execute_plan_collect, execute_plan_collect_with_schema,
};
use crate::backend::executor::row_select::select_matching_pointers;
use crate::backend::executor::{delete_by_pointers, update_by_pointers};
use crate::catalog::Catalog;

/// INSERT ... VALUES and INSERT ... SELECT, both through the planner.
pub(super) fn insert(catalog: &Catalog, db: &str, plan: &QueryPlan) -> RookResult<StatementResult> {
    let logical = crate::planner::plan_query(plan, catalog, db)
        .map_err(|e| RookError::Internal(e.to_string()).with_context("Insert planning error"))?;
    // Inserting reports a count, not the written rows.
    let n = execute_plan_collect(&logical, catalog, db)
        .map_err(|e| e.with_context("Insert failed"))?
        .len();
    Ok(StatementResult::message(if n == 1 {
        "1 row inserted.\n".to_string()
    } else {
        format!("{} rows inserted.\n", n)
    }))
}

/// SELECT and set operations.
pub(super) fn query(catalog: &Catalog, db: &str, plan: &QueryPlan) -> RookResult<StatementResult> {
    let logical = crate::planner::plan_query(plan, catalog, db)
        .map_err(|e| RookError::Internal(e.to_string()).with_context("Planning error"))?;
    let (rows, schema) = execute_plan_collect_with_schema(&logical, catalog, db)
        .map_err(|e| e.with_context("Execution error"))?;
    let count = format!("{} row(s) returned.\n", rows.len());
    Ok(StatementResult {
        rows: Some((schema, rows)),
        messages: vec![count],
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
