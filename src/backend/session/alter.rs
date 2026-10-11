//! ALTER TABLE.

use std::io;
use std::path::{Path, PathBuf};

use rook_ast::{AlterTableAction, AlterTablePlan};

use super::StatementResult;
use super::ddl::{remove_index_files_for_table, rename_index_files_for_table};
use super::names::strip_outer_parens;
use crate::catalog::Catalog;
use crate::catalog::types::Column;
use crate::heap::HeapManager;
use crate::types::{DataType, DataValue, deserialize_nullable_row, serialize_nullable_typed_row};

pub(super) fn alter_table(
    catalog: &mut Catalog,
    db: &str,
    alter: &AlterTablePlan,
) -> io::Result<StatementResult> {
    let db_obj = catalog.databases.get(db).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Database '{}' not found", db),
        )
    })?;
    let mut updated_table = db_obj.tables.get(&alter.table).cloned().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Table '{}' not found", alter.table),
        )
    })?;
    let mut lines = Vec::new();
    let column_not_found = |column: &str| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Column '{}' not found in table '{}'", column, alter.table),
        )
    };
    let find = |columns: &[Column], name: &str| {
        columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| column_not_found(name))
    };

    match &alter.action {
        AlterTableAction::RenameTable { new_name } => {
            return rename_table(catalog, db, &alter.table, new_name, updated_table);
        }
        AlterTableAction::AddColumn { column_def } => {
            let mut col = Column::new(
                column_def.name.clone(),
                column_def.data_type.parse().map_err(|e: String| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Invalid data type: {}", e),
                    )
                })?,
            );
            // Apply the column's constraints; refuse the ones this path cannot
            // honour rather than dropping them.
            for constraint in &column_def.constraints {
                let upper = constraint.to_ascii_uppercase();
                if upper == "NOT NULL" {
                    col.nullable = false;
                    col.constraints.not_null = true;
                } else if upper == "NULL" {
                    // Explicitly nullable: the default.
                } else if upper.starts_with("DEFAULT ") || upper.starts_with("DEFAULT(") {
                    col.constraints.default =
                        parse_default(&col.data_type, &constraint["DEFAULT".len()..], &col.name)?;
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "ALTER TABLE ADD COLUMN does not support the constraint '{}'",
                            constraint
                        ),
                    ));
                }
            }
            if updated_table
                .columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(&column_def.name))
            {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "Column '{}' already exists in table '{}'",
                        column_def.name, alter.table
                    ),
                ));
            }

            // Existing rows take the column's DEFAULT (NULL without one).
            let fill = col.constraints.default.clone();
            let reject_rows = col.constraints.not_null && fill.is_none();
            let old_schema = schema(&updated_table.columns);
            updated_table.columns.push(col);
            rewrite_rows(
                db,
                &alter.table,
                &old_schema,
                &schema(&updated_table.columns),
                |mut row| {
                    if reject_rows {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "Cannot add NOT NULL column '{}' without a DEFAULT to a table that has rows",
                                column_def.name
                            ),
                        ));
                    }
                    row.push(fill.clone());
                    Ok(row)
                },
            )?;
        }
        AlterTableAction::DropColumn { column } => {
            let pos = find(&updated_table.columns, column)?;
            let old_schema = schema(&updated_table.columns);
            updated_table.columns.remove(pos);
            rewrite_rows(
                db,
                &alter.table,
                &old_schema,
                &schema(&updated_table.columns),
                |mut row| {
                    row.remove(pos);
                    Ok(row)
                },
            )?;
        }
        AlterTableAction::RenameColumn { old_name, new_name } => {
            let pos = find(&updated_table.columns, old_name)?;
            updated_table.columns[pos].name = new_name.clone();
            // CHECK expressions are re-emitted from the catalog on save.
            for c in updated_table.columns.iter_mut() {
                if let Some(expr) = c.constraints.check.take() {
                    c.constraints.check =
                        Some(crate::backend::system_table::rename_identifier_in_expr(
                            &expr, old_name, new_name,
                        ));
                }
            }
        }
        AlterTableAction::SetDefault {
            column,
            default_expr,
        } => {
            let pos = find(&updated_table.columns, column)?;
            let col = &mut updated_table.columns[pos];
            col.constraints.default = parse_default(&col.data_type, default_expr, column)?;
        }
        AlterTableAction::DropDefault { column } => {
            let pos = find(&updated_table.columns, column)?;
            updated_table.columns[pos].constraints.default = None;
        }
        AlterTableAction::SetNotNull { column } => {
            let col_idx = find(&updated_table.columns, column)?;
            // Existing rows must have a value; the null bitmap answers that
            // without decoding the row.
            let dat_path = crate::backend::paths::heap(db, &alter.table);
            if Path::new(&dat_path).exists() {
                let heap = HeapManager::open(PathBuf::from(&dat_path)).map_err(|e| {
                    io::Error::other(format!(
                        "Failed to open heap for SET NOT NULL validation: {}",
                        e
                    ))
                })?;
                for result in heap.scan() {
                    let (_, _, row_bytes) = result?;
                    if crate::types::null_bitmap::is_column_null_in_row(&row_bytes, col_idx)
                        .unwrap_or(false)
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "Table '{}' contains NULL values in column '{}'; cannot alter to NOT NULL",
                                alter.table, column
                            ),
                        ));
                    }
                }
            }
            let col = &mut updated_table.columns[col_idx];
            col.nullable = false;
            col.constraints.not_null = true;
        }
        AlterTableAction::DropNotNull { column } => {
            let pos = find(&updated_table.columns, column)?;
            let col = &mut updated_table.columns[pos];
            col.nullable = true;
            col.constraints.not_null = false;
        }
    }

    // Update sys_constraints metadata for column operations
    match &alter.action {
        AlterTableAction::DropColumn { column } => {
            if let Err(e) =
                crate::backend::system_table::delete_column_constraints(db, &alter.table, column)
            {
                lines.push(format!(
                    "[AlterTable] Warning: failed to clean up constraint metadata for column '{}': {}",
                    column, e
                ));
            }
        }
        AlterTableAction::RenameColumn { old_name, new_name } => {
            if let Err(e) = crate::backend::system_table::rename_column_in_constraints(
                db,
                &alter.table,
                old_name,
                new_name,
            ) {
                lines.push(format!(
                    "[AlterTable] Warning: failed to update constraint metadata for column rename '{}' → '{}': {}",
                    old_name, new_name, e
                ));
            }
        }
        _ => {}
    }

    if let Some(d) = catalog.databases.get_mut(db) {
        d.tables.insert(alter.table.clone(), updated_table);
    }
    if let Err(e) = crate::catalog::save_catalog(catalog) {
        lines.push(format!(
            "[AlterTable] Warning: failed to save catalog after altering table '{}': {}",
            alter.table, e
        ));
    }

    lines.push(format!(
        "Altered table '{}': {:?}.",
        alter.table, alter.action
    ));
    Ok(StatementResult::lines(lines))
}

fn rename_table(
    catalog: &mut Catalog,
    db: &str,
    old_name: &str,
    new_name: &str,
    table: crate::catalog::types::Table,
) -> io::Result<StatementResult> {
    if catalog.databases[db].tables.contains_key(new_name) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("Table '{}' already exists", new_name),
        ));
    }
    // Foreign keys store the parent's name: remember who references this
    // table so those rows can follow the rename.
    let referencing: Vec<String> = crate::backend::cache::referencing_fks(db, old_name)
        .iter()
        .map(|fk| fk.0.clone())
        .collect();
    let old_dat = crate::backend::paths::heap(db, old_name);
    let new_dat = crate::backend::paths::heap(db, new_name);
    let _ = std::fs::rename(&old_dat, &new_dat);
    let _ = std::fs::rename(format!("{}.fsm", old_dat), format!("{}.fsm", new_dat));
    rename_index_files_for_table(db, old_name, new_name);

    let mut lines = Vec::new();
    if let Some(d) = catalog.databases.get_mut(db) {
        d.tables.remove(old_name);
        d.tables.insert(new_name.to_string(), table);
    }
    if let Err(e) = crate::catalog::save_catalog(catalog) {
        lines.push(format!(
            "[AlterTable] Warning: failed to save catalog after renaming table '{}' → '{}': {}",
            old_name, new_name, e
        ));
    }
    // A self-referencing table is its own child under the new name.
    let children: Vec<String> = referencing
        .iter()
        .map(|c| {
            if c.eq_ignore_ascii_case(old_name) {
                new_name.to_string()
            } else {
                c.clone()
            }
        })
        .collect();
    crate::backend::system_table::rename_referenced_table_in_constraints(
        db, &children, old_name, new_name,
    )?;
    crate::backend::cache::invalidate_metadata();
    lines.push(format!("Renamed table '{}' to '{}'.", old_name, new_name));
    Ok(StatementResult::lines(lines))
}

fn schema(columns: &[Column]) -> Vec<DataType> {
    columns.iter().map(|c| c.data_type.clone()).collect()
}

/// A DEFAULT given as text; `None` for NULL.
fn parse_default(ty: &DataType, raw: &str, column: &str) -> io::Result<Option<DataValue>> {
    let raw = strip_outer_parens(raw);
    if raw.eq_ignore_ascii_case("NULL") {
        return Ok(None);
    }
    crate::backend::executor::create_index::parse_string_to_value(ty, raw)
        .map(Some)
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid DEFAULT {} for column '{}': {}", raw, column, e),
            )
        })
}

/// Rewrite every row of a table for a new schema: each row is decoded with
/// `old_schema`, changed by `change`, and encoded with `new_schema`.
///
/// The rows are written to a new heap file that then replaces the old one,
/// so a failure leaves the table as it was. Index files are removed, since
/// they point at the old row locations.
fn rewrite_rows(
    db: &str,
    table: &str,
    old_schema: &[DataType],
    new_schema: &[DataType],
    mut change: impl FnMut(Vec<Option<DataValue>>) -> io::Result<Vec<Option<DataValue>>>,
) -> io::Result<()> {
    let dat_path = crate::backend::paths::heap(db, table);
    if !Path::new(&dat_path).exists() {
        return Ok(());
    }

    let old_heap = HeapManager::open(PathBuf::from(&dat_path))
        .map_err(|e| io::Error::other(format!("Failed to open heap for backfill: {}", e)))?;
    let mut rows: Vec<Vec<u8>> = Vec::new();
    for result in old_heap.scan() {
        let (_, _, row_bytes) = result?;
        let old_row = deserialize_nullable_row(old_schema, &row_bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize row during backfill: {}", e),
            )
        })?;
        let new_row = change(old_row)?;
        rows.push(
            serialize_nullable_typed_row(new_schema, &new_row).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to serialize row during backfill: {}", e),
                )
            })?,
        );
    }
    drop(old_heap);

    let tmp_path = format!("{}.backfill", dat_path);
    {
        let mut tmp_heap = HeapManager::create(PathBuf::from(&tmp_path)).map_err(|e| {
            io::Error::other(format!("Failed to create temp heap for backfill: {}", e))
        })?;
        for row in &rows {
            tmp_heap.insert_tuple(row)?;
        }
        tmp_heap.flush()?;
    }

    let fsm_path = format!("{}.fsm", dat_path);
    std::fs::remove_file(&dat_path).map_err(|e| {
        io::Error::other(format!("Failed to remove old heap during backfill: {}", e))
    })?;
    let _ = std::fs::remove_file(&fsm_path);
    std::fs::rename(&tmp_path, &dat_path)
        .map_err(|e| io::Error::other(format!("Failed to rename backfill heap: {}", e)))?;
    let _ = std::fs::rename(format!("{}.fsm", tmp_path), &fsm_path);

    // The cached handles still describe the replaced file.
    crate::backend::buffer_manager::shared_pool::invalidate(Path::new(&dat_path));
    let _ = crate::backend::cache::evict_heap(Path::new(&dat_path));
    let _ = crate::backend::cache::evict_heap(Path::new(&tmp_path));
    crate::backend::buffer_manager::shared_pool::invalidate(Path::new(&tmp_path));

    remove_index_files_for_table(db, table, None);
    Ok(())
}
