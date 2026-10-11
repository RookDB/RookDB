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
/// Identical semantics to [`check_unique_insert`], but the unique-index list
/// comes from the process-cached [`TableMeta`] and B+ Tree handles are reused
/// across rows (fsync batched in the cache layer). When `meta` is `None`
/// (table absent from system tables) only column-level UNIQUE flags apply,
/// via the heap-scan fallback.
pub(crate) fn check_unique_insert_meta(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
    columns: &[Column],
    values: &[Option<DataValue>],
    exclude_ptrs: &[(u32, u32)],
    meta: Option<&TableMeta>,
) -> Result<(), RookError> {
    let unique_indexes: Vec<(String, Vec<String>)> = match meta {
        Some(m) => m.unique_indexes.clone(),
        None => Vec::new(),
    };

    // 1. Check all UNIQUE indexes (composite or single-column)
    for (idx_name, idx_cols) in &unique_indexes {
        if idx_cols.is_empty() {
            continue;
        }

        let mut key_values = Vec::with_capacity(idx_cols.len());
        let mut key_types = Vec::with_capacity(idx_cols.len());
        let mut col_positions = Vec::with_capacity(idx_cols.len());
        let mut has_null = false;

        for col_name in idx_cols {
            let col_pos = match columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(col_name))
            {
                Some(p) => p,
                None => continue,
            };
            col_positions.push(col_pos);

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

        let mut found_via_index = false;
        let mut checked_index = false;

        let idx_path =
            std::path::PathBuf::from(engine.paths().index(db_name, table_name, idx_name));
        if idx_path.exists() {
            let kt1 = key_types.clone();
            let kt2 = key_types.clone();
            let open_path = idx_path.clone();
            match crate::backend::cache::with_btree(
                &idx_path,
                move || -> std::io::Result<crate::backend::index::btree::BTree> {
                    let mut bt = crate::backend::index::btree::BTree::open(open_path)?;
                    bt.set_key_types(kt1);
                    Ok(bt)
                },
                |bt| {
                    bt.set_key_types(kt2);
                    bt.search_all_keys(&key_values)
                },
            ) {
                Ok(tids) => {
                    checked_index = true;
                    if tids.iter().any(|tid| !exclude_ptrs.contains(tid)) {
                        found_via_index = true;
                    }
                }
                Err(e) => {
                    log::warn!(
                        "[Constraint] BTree search error for UNIQUE check on '{}.{}' (index '{}'): {}",
                        db_name,
                        table_name,
                        idx_name,
                        e
                    );
                }
            }
        }

        if found_via_index {
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

        if checked_index {
            continue;
        }

        // Heap-scan fallback for this unique index if no index file exists
        let heap_path = std::path::PathBuf::from(engine.paths().heap(db_name, table_name));
        if heap_path.exists()
            && let Ok(heap) = crate::backend::heap::HeapManager::open(heap_path)
        {
            let schema_types: Vec<crate::types::DataType> =
                columns.iter().map(|c| c.data_type.clone()).collect();
            for result in heap.scan() {
                let (page_id, slot_id, raw_bytes) = match result {
                    Ok(triple) => triple,
                    Err(_) => continue,
                };
                if exclude_ptrs.contains(&(page_id, slot_id)) {
                    continue;
                }

                let decoded =
                    match crate::types::deserialize_nullable_row(&schema_types, &raw_bytes) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };

                let mut matches = true;
                use crate::types::Comparable;
                for (i, &pos) in col_positions.iter().enumerate() {
                    match decoded.get(pos) {
                        Some(Some(existing_val)) => {
                            if let Ok(cmp) = existing_val.compare(&key_values[i]) {
                                if cmp != std::cmp::Ordering::Equal {
                                    matches = false;
                                    break;
                                }
                            } else {
                                matches = false;
                                break;
                            }
                        }
                        _ => {
                            matches = false;
                            break;
                        }
                    }
                }

                if matches {
                    return Err(RookError::constraint(
                        ConstraintKind::Unique,
                        table_name,
                        Some(&idx_cols.join(",")),
                        format!(
                            "UNIQUE constraint violated on index '{}': duplicate key values in heap",
                            idx_name
                        ),
                    ));
                }
            }
        }
    }

    // 2. Check column-level UNIQUE constraints (`col.constraints.unique`)
    for (col_pos, col) in columns.iter().enumerate() {
        if !col.constraints.unique {
            continue;
        }

        // If this column is already covered by a single-column unique index, skip it
        if unique_indexes
            .iter()
            .any(|(_, cols)| cols.len() == 1 && cols[0].eq_ignore_ascii_case(&col.name))
        {
            continue;
        }

        let Some(Some(key_value)) = values.get(col_pos) else {
            continue;
        };
        let key_value = key_value.clone();
        let shown = super::fk_actions::key_text(&key_value);
        let col_type = columns[col_pos].data_type.clone();

        let mut found_via_index = false;
        let mut checked_index = false;

        // Legacy single-index file fallback
        let legacy_idx_path =
            std::path::PathBuf::from(engine.paths().legacy_index(db_name, table_name));
        if legacy_idx_path.exists() {
            match crate::backend::cache::with_btree(
                &legacy_idx_path,
                || -> std::io::Result<crate::backend::index::btree::BTree> {
                    let mut bt =
                        crate::backend::index::btree::BTree::open(legacy_idx_path.clone())?;
                    bt.set_key_type(col_type.clone());
                    Ok(bt)
                },
                |bt| {
                    bt.set_key_type(col_type.clone());
                    bt.search_all(&key_value)
                },
            ) {
                Ok(tids) => {
                    checked_index = true;
                    if tids.iter().any(|tid| !exclude_ptrs.contains(tid)) {
                        found_via_index = true;
                    }
                }
                Err(e) => {
                    log::warn!(
                        "[Constraint] Legacy BTree search error for UNIQUE check on '{}.{}': {}",
                        db_name,
                        table_name,
                        e
                    );
                }
            }
        }

        if found_via_index {
            return Err(RookError::constraint(
                ConstraintKind::Unique,
                table_name,
                Some(&col.name),
                format!(
                    "UNIQUE constraint violated: value '{}' already exists for column '{}'",
                    shown, col.name
                ),
            ));
        }

        if checked_index {
            continue;
        }

        // Heap-scan fallback for column-level unique
        let heap_path = std::path::PathBuf::from(engine.paths().heap(db_name, table_name));
        if heap_path.exists()
            && let Ok(heap) = crate::backend::heap::HeapManager::open(heap_path)
        {
            let schema_types: Vec<crate::types::DataType> =
                columns.iter().map(|c| c.data_type.clone()).collect();
            for result in heap.scan() {
                let (page_id, slot_id, raw_bytes) = match result {
                    Ok(triple) => triple,
                    Err(_) => continue,
                };
                if exclude_ptrs.contains(&(page_id, slot_id)) {
                    continue;
                }

                let decoded =
                    match crate::types::deserialize_nullable_row(&schema_types, &raw_bytes) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };

                if let Some(Some(existing_val)) = decoded.get(col_pos) {
                    use crate::types::Comparable;
                    if let Ok(cmp) = existing_val.compare(&key_value)
                        && cmp == std::cmp::Ordering::Equal
                    {
                        return Err(RookError::constraint(
                            ConstraintKind::Unique,
                            table_name,
                            Some(&col.name),
                            format!(
                                "UNIQUE constraint violated: value '{}' already exists for column '{}'",
                                shown, col.name
                            ),
                        ));
                    }
                }
            }
        }
    }

    Ok(())
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
    let foreign_keys: Vec<(String, String, String)> = match meta {
        Some(m) => m.foreign_keys.clone(),
        None => Vec::new(),
    };

    if foreign_keys.is_empty() {
        return Ok(());
    }

    for fk in &foreign_keys {
        let child_col = &fk.0;
        let parent_table = &fk.1;
        let parent_col = &fk.2;

        let col_pos = columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(child_col))
            .ok_or_else(|| format!("FK column '{}' not found in table schema", child_col))?;

        // NULL values in FK columns are allowed (SQL standard)
        let Some(Some(child_value)) = values.get(col_pos) else {
            continue;
        };

        let parent_col_type = catalog
            .databases
            .get(db_name)
            .and_then(|db| db.tables.get(parent_table))
            .and_then(|t| {
                t.columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(parent_col))
            })
            .map(|c| c.data_type.clone())
            .unwrap_or(DataType::Varchar(255));

        // A value that cannot be a parent key cannot be found among them.
        let found = match crate::types::functions::assign_cast(child_value, &parent_col_type) {
            Ok(key) => value_lookup::value_exists_in_table(
                catalog.engine(),
                db_name,
                parent_table,
                parent_col,
                &parent_col_type,
                &key,
            )?,
            Err(_) => false,
        };
        if !found {
            return Err(RookError::constraint(
                ConstraintKind::ForeignKey,
                table_name,
                Some(child_col),
                format!(
                    "FOREIGN KEY constraint violated: value '{}' in column '{}' not found in parent table '{}' (column '{}')",
                    super::fk_actions::key_text(child_value),
                    child_col,
                    parent_table,
                    parent_col
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
