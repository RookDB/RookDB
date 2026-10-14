//! Validation helpers for constraint checking.
//!
//! Provides the core validation logic for NOT NULL, UNIQUE, FOREIGN KEY,
//! and CHECK constraints, called by the public API functions.

use crate::catalog::types::Column;
use crate::types::{DataType, DataValue};

use super::value_lookup;
use super::{ConstraintKind, RookError};
use crate::backend::cache::TableMeta;

/// Check that no NOT NULL column receives a NULL value.
pub(crate) fn check_not_null(
    table_name: &str,
    columns: &[Column],
    values: &[Option<DataValue>],
) -> Result<(), RookError> {
    for (i, col) in columns.iter().enumerate() {
        if !col.nullable && values.get(i).is_none_or(Option::is_none) {
            return Err(RookError::constraint(
                ConstraintKind::NotNull,
                table_name,
                Some(&col.name),
                format!(
                    "NOT NULL constraint violated: column '{}' cannot be null",
                    col.name
                ),
            ));
        }
    }
    Ok(())
}

/// Metadata-driven UNIQUE check — the hot path used by row inserts/updates.
///
/// The unique-index list comes from the process-cached [`TableMeta`] and
/// B+ Tree handles are reused across rows (fsync batched in the cache layer).
/// Every UNIQUE key is checked through an index: a listed index whose file is
/// missing, or a UNIQUE column with no index at all, is built on first use.
pub(crate) fn check_unique_insert_meta(
    catalog: &crate::catalog::types::Catalog,
    db_name: &str,
    table_name: &str,
    columns: &[Column],
    values: &[Option<DataValue>],
    exclude_ptrs: &[(u32, u32)],
    meta: Option<&TableMeta>,
) -> Result<(), RookError> {
    let engine = catalog.engine();
    let unique_indexes: &[(String, Vec<String>)] = match meta {
        Some(m) => &m.unique_indexes,
        None => &[],
    };

    // 1. Check all UNIQUE indexes (composite or single-column)
    for (idx_name, idx_cols) in unique_indexes {
        if idx_cols.is_empty() {
            continue;
        }

        let mut key_values = Vec::with_capacity(idx_cols.len());
        let mut key_types = Vec::with_capacity(idx_cols.len());
        let mut has_null = false;

        for col_name in idx_cols {
            let col_pos = match columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(col_name))
            {
                Some(p) => p,
                None => continue,
            };

            let Some(Some(dv)) = values.get(col_pos) else {
                has_null = true;
                break;
            };
            key_values.push(dv.clone());
            key_types.push(columns[col_pos].data_type.clone());
        }

        // Under standard SQL UNIQUE constraint semantics, a composite key containing
        // any NULL component does not violate uniqueness.
        if has_null || key_values.len() != idx_cols.len() {
            continue;
        }

        let idx_path = engine.paths().index(db_name, table_name, idx_name);
        if !idx_path.exists() {
            crate::backend::executor::create_index::restore_index_file(
                catalog, db_name, table_name, idx_name,
            )
            .map_err(|e| unique_lookup_error(table_name, idx_name, e))?;
        }
        let kt1 = key_types.clone();
        let open_path = idx_path.clone();
        let tids = crate::backend::cache::with_btree(
            engine,
            &idx_path,
            move || -> std::io::Result<crate::backend::index::btree::BTree> {
                let mut bt = crate::backend::index::btree::BTree::open(open_path)?;
                bt.set_key_types(kt1);
                Ok(bt)
            },
            |bt| {
                bt.set_key_types(key_types);
                bt.search_all_keys(&key_values)
            },
        )
        .map_err(|e| unique_lookup_error(table_name, idx_name, e.to_string()))?;

        if tids.iter().any(|tid| !exclude_ptrs.contains(tid)) {
            let vals_str = idx_cols
                .iter()
                .zip(key_values.iter())
                .map(|(c, v)| format!("{}={}", c, super::fk_actions::key_text(v)))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(RookError::constraint(
                ConstraintKind::Unique,
                table_name,
                Some(&idx_cols.join(",")),
                format!(
                    "UNIQUE constraint violated on index '{}': duplicate key ({})",
                    idx_name, vals_str
                ),
            ));
        }
    }

    // 2. Column-level UNIQUE flags with no unique index of their own
    //    (tables from before CREATE TABLE built one).
    for (col_pos, col) in columns.iter().enumerate() {
        if !col.constraints.unique {
            continue;
        }

        // Already covered by a single-column unique index in step 1.
        if unique_indexes
            .iter()
            .any(|(_, cols)| cols.len() == 1 && cols[0].eq_ignore_ascii_case(&col.name))
        {
            continue;
        }

        let Some(Some(key_value)) = values.get(col_pos) else {
            continue;
        };
        let (index, _) = crate::backend::executor::create_index::ensure_columns_index(
            catalog,
            db_name,
            table_name,
            std::slice::from_ref(&col.name),
            &format!("uq_{}_{}", table_name, col.name),
            true,
        )
        .map_err(|e| unique_lookup_error(table_name, &col.name, e))?;
        let idx_path = engine.paths().index(db_name, table_name, &index);
        let col_type = col.data_type.clone();
        let open_path = idx_path.clone();
        let open_type = col_type.clone();
        let tids = crate::backend::cache::with_btree(
            engine,
            &idx_path,
            move || -> std::io::Result<crate::backend::index::btree::BTree> {
                let mut bt = crate::backend::index::btree::BTree::open(open_path)?;
                bt.set_key_type(open_type);
                Ok(bt)
            },
            |bt| {
                bt.set_key_type(col_type);
                bt.search_all(key_value)
            },
        )
        .map_err(|e| unique_lookup_error(table_name, &index, e.to_string()))?;

        if tids.iter().any(|tid| !exclude_ptrs.contains(tid)) {
            return Err(RookError::constraint(
                ConstraintKind::Unique,
                table_name,
                Some(&col.name),
                format!(
                    "UNIQUE constraint violated: value '{}' already exists for column '{}'",
                    super::fk_actions::key_text(key_value),
                    col.name
                ),
            ));
        }
    }

    Ok(())
}

fn unique_lookup_error(table_name: &str, what: &str, cause: String) -> RookError {
    RookError::Internal(format!(
        "cannot check UNIQUE on '{}' ({}): {}",
        table_name, what, cause
    ))
}

/// Metadata-driven FOREIGN KEY check (hot path).
///
/// FK definitions come from the process-cached [`TableMeta`] instead of
/// re-resolving table ids and rescanning `sys_constraints` on every row.
pub(crate) fn check_foreign_key_insert_meta(
    catalog: &crate::catalog::types::Catalog,
    db_name: &str,
    table_name: &str,
    columns: &[Column],
    values: &[Option<DataValue>],
    meta: Option<&TableMeta>,
) -> Result<(), RookError> {
    let Some(meta) = meta else {
        return Ok(());
    };

    for (child_cols, parent_table, parent_cols) in &meta.foreign_keys {
        // The child's values for the FK columns.
        let mut values_of_key = Vec::with_capacity(child_cols.len());
        for child_col in child_cols {
            let col_pos = columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(child_col))
                .ok_or_else(|| format!("FK column '{}' not found in table schema", child_col))?;
            values_of_key.push(values.get(col_pos).and_then(|v| v.as_ref()));
        }
        // MATCH SIMPLE: a row with a NULL in any FK column is not checked.
        let Some(child_values) = values_of_key.into_iter().collect::<Option<Vec<_>>>() else {
            continue;
        };

        let parent_columns = catalog
            .databases
            .get(db_name)
            .and_then(|db| db.tables.get(parent_table))
            .map(|t| t.columns.as_slice())
            .unwrap_or_default();

        // A value that cannot be a parent key cannot be found among them.
        let mut key = Vec::with_capacity(child_values.len());
        for (value, parent_col) in child_values.iter().zip(parent_cols) {
            let parent_col_type = parent_columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(parent_col))
                .map(|c| c.data_type.clone())
                .unwrap_or(DataType::Varchar(255));
            match crate::types::functions::assign_cast(value, &parent_col_type) {
                Ok(v) => key.push(v),
                Err(_) => break,
            }
        }
        let found = key.len() == child_values.len()
            && value_lookup::key_exists_in_table(
                catalog,
                db_name,
                parent_table,
                parent_cols,
                &key,
            )?;
        if !found {
            let shown = |names: &[String]| names.join(", ");
            return Err(RookError::constraint(
                ConstraintKind::ForeignKey,
                table_name,
                Some(&shown(child_cols)),
                format!(
                    "FOREIGN KEY constraint violated: value '{}' in column '{}' not found in parent table '{}' (column '{}')",
                    super::fk_actions::tuple_text(child_values.iter().copied()),
                    shown(child_cols),
                    parent_table,
                    shown(parent_cols)
                ),
            ));
        }
    }

    Ok(())
}

/// Check CHECK constraints loaded from `sys_constraints`.
///
/// Each CHECK expression (stored in the `columns` field of the constraint
/// row) is parsed with the real SQL grammar and evaluated against the new
/// row using the Volcano expression evaluator — full expression support
/// (arithmetic, AND/OR/NOT, IN, BETWEEN, LIKE, functions), replacing the
/// retired legacy DNF matcher.
///
/// Semantics follow SQL: the row passes when the predicate is TRUE;
/// NULL results are UNKNOWN and therefore pass.
/// Metadata-driven CHECK constraint check (hot path).
///
/// Fast path evaluates precompiled physical predicates stored in [`TableMeta`].
/// If the table metadata was loaded before parser registration, it falls back
/// to compiling via the registered hook.
pub(crate) fn check_constraints_meta(
    table_name: &str,
    columns: &[Column],
    values: &[Option<DataValue>],
    meta: Option<&TableMeta>,
) -> Result<(), RookError> {
    let meta = match meta {
        Some(m) => m,
        None => return Ok(()),
    };

    if meta.check_exprs.is_empty() {
        return Ok(());
    }

    let tuple = crate::backend::executor::physical::tuple::Tuple::new(values.to_vec());

    let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let schema: Vec<crate::backend::executor::physical::tuple::ColumnInfo> = columns
        .iter()
        .map(|c| crate::backend::executor::physical::tuple::ColumnInfo {
            merged: false,
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            table: None,
        })
        .collect();

    // Fast path: evaluate precompiled check AST nodes directly (no SQL parsing)
    if !meta.check_ast.is_empty() {
        for (expr, ast_node) in &meta.check_ast {
            let pred =
                crate::backend::executor::physical::expr::predicate_from_ast(ast_node, &col_names)
                    .map_err(|e| {
                        RookError::Internal(format!(
                            "Failed to compile CHECK constraint '{}': {}",
                            expr, e
                        ))
                    })?;
            match crate::backend::executor::physical::expr::evaluate_predicate(
                &pred, &tuple, &schema,
            ) {
                Ok(Some(true)) => {} // satisfied
                Ok(Some(false)) => {
                    return Err(RookError::constraint(
                        ConstraintKind::Check,
                        table_name,
                        None,
                        format!("CHECK constraint violated: '{}'", expr),
                    ));
                }
                Ok(None) => {} // UNKNOWN (NULL) → pass, per SQL
                Err(e) => {
                    return Err(RookError::Internal(format!(
                        "Failed to evaluate CHECK constraint '{}': {}",
                        expr, e
                    )));
                }
            }
        }
        return Ok(());
    }

    // Fallback path: parse and compile using registered hook if check_ast is empty
    if let Some(parser) = crate::backend::cache::get_check_parser() {
        for expr in &meta.check_exprs {
            let node = parser(expr).map_err(|e| {
                RookError::Internal(format!(
                    "Failed to parse CHECK constraint expression '{}': {}",
                    expr, e
                ))
            })?;
            let pred =
                crate::backend::executor::physical::expr::predicate_from_ast(&node, &col_names)
                    .map_err(|e| {
                        RookError::Internal(format!(
                            "Failed to compile CHECK constraint '{}': {}",
                            expr, e
                        ))
                    })?;
            match crate::backend::executor::physical::expr::evaluate_predicate(
                &pred, &tuple, &schema,
            ) {
                Ok(Some(true)) => {}
                Ok(Some(false)) => {
                    return Err(RookError::constraint(
                        ConstraintKind::Check,
                        table_name,
                        None,
                        format!("CHECK constraint violated: '{}'", expr),
                    ));
                }
                Ok(None) => {}
                Err(e) => {
                    return Err(RookError::Internal(format!(
                        "Failed to evaluate CHECK constraint '{}': {}",
                        expr, e
                    )));
                }
            }
        }
    }

    Ok(())
}
