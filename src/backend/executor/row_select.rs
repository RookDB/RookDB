//! Volcano-backed row selection for mutation commands.
//!
//! The single source of "which rows match this WHERE clause" is now the
//! physical engine: build a wildcard `SELECT` over the target table, run it
//! through the logical/physical planners, and collect the heap locations of
//! the matching tuples. Mutation entry points (`delete_by_pointers`,
//! `update_by_pointers`) consume those pointers.
//!
//! This module replaced the retired `selection.rs` bytecode VM and the DNF
//! condition-group parser for all interactive/command paths.

use rook_ast::{PredicateNode, QueryPlan, SelectExpr, SelectPlan, TableRef};

use crate::backend::error::RookError;
use crate::catalog::Catalog;

/// Parse a raw WHERE-clause string into a `PredicateNode`.
///
/// `None` means "no predicate" (match every row).
pub fn parse_where_text(text: &str) -> crate::backend::error::RookResult<Option<PredicateNode>> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    rook_parser::parse_where_text(trimmed).map_err(RookError::Internal)
}

/// Execute a wildcard scan over `db.table` with an optional predicate and
/// return the heap locations `(page_id, slot_id)` of every matching tuple.
///
/// Every returned tuple must carry its location; a synthetic location-less
/// row indicates an operator bug and is rejected rather than silently
/// dropped.
pub fn select_matching_pointers(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    selection: Option<PredicateNode>,
) -> crate::backend::error::RookResult<Vec<(u32, u32)>> {
    let logical = matching_rows_plan(catalog, db_name, table_name, selection)?;
    let tuples = crate::backend::executor::physical::engine::execute_plan_collect(
        &logical, catalog, db_name,
    )?;

    let mut out = Vec::with_capacity(tuples.len());
    for t in tuples {
        match (t.page_id, t.slot_id) {
            (Some(page), Some(slot)) => out.push((page, slot)),
            _ => {
                return Err(RookError::Internal(
                    "engine returned rows without heap locations (operator bug)".to_string(),
                ));
            }
        }
    }
    Ok(out)
}

/// The plan [`select_matching_pointers`] runs to find the rows of `db.table`
/// that match `selection`: what an UPDATE or DELETE reads before it writes.
pub fn matching_rows_plan(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    selection: Option<PredicateNode>,
) -> crate::backend::error::RookResult<rook_ast::logical::LogicalPlan> {
    let select = SelectPlan {
        ctes: Vec::new(),
        projections: vec![SelectExpr::Wildcard],
        from: vec![TableRef {
            name: table_name.to_string(),
            alias: None,
        }],
        joins: Vec::new(),
        selection,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        limit: None,
        distinct: false,
    };

    crate::planner::plan_query(&QueryPlan::Select(select), catalog, db_name)
}
