//! ALTER TABLE.

use std::path::{Path, PathBuf};

use rook_ast::{AlterTableAction, AlterTablePlan, ColumnConstraint, ColumnDef, DefaultValue};

use super::StatementResult;
use super::ddl::{
    default_value, rebuild_indexes, remove_index_files_for_table, rename_index_files_for_table,
};
use crate::backend::error::{ConstraintKind, ResultContext, RookError, RookResult};
use crate::backend::system_table::SavedIndex;
use crate::catalog::Catalog;
use crate::catalog::types::{Column, Table};
use crate::heap::HeapManager;
use crate::types::{DataType, DataValue, deserialize_nullable_row, serialize_nullable_typed_row};

pub(super) fn alter_table(
    catalog: &mut Catalog,
    db: &str,
    alter: &AlterTablePlan,
) -> RookResult<StatementResult> {
    let db_obj = catalog
        .databases
        .get(db)
        .ok_or_else(|| RookError::NotFound {
            entity: "Database",
            name: db.to_string(),
        })?;
    let mut updated_table =
        db_obj
            .tables
            .get(&alter.table)
            .cloned()
            .ok_or_else(|| RookError::NotFound {
                entity: "Table",
                name: alter.table.to_string(),
            })?;
    let mut lines = Vec::new();
    // The table's indexes, rebuilt or renamed below when the change touches
    // their columns.
    let indexes = crate::backend::system_table::table_indexes(catalog.engine(), db, &alter.table);

    match &alter.action {
        AlterTableAction::RenameTable { new_name } => {
            return rename_table(catalog, db, &alter.table, new_name, updated_table);
        }
        AlterTableAction::AddColumn { column_def } => {
            add_column(catalog, db, &alter.table, &mut updated_table, column_def)?
        }
        AlterTableAction::DropColumn { column } => {
            drop_column(catalog, db, &alter.table, &mut updated_table, column)?
        }
        AlterTableAction::RenameColumn { old_name, new_name } => {
            rename_column(&mut updated_table, &alter.table, old_name, new_name)?
        }
        AlterTableAction::SetDefault {
            column,
            default_expr,
        } => set_default(&mut updated_table, &alter.table, column, default_expr)?,
        AlterTableAction::DropDefault { column } => {
            drop_default(&mut updated_table, &alter.table, column)?
        }
        AlterTableAction::SetNotNull { column } => {
            set_not_null(catalog, db, &alter.table, &mut updated_table, column)?
        }
        AlterTableAction::DropNotNull { column } => {
            drop_not_null(&mut updated_table, &alter.table, column)?
        }
    }

    update_constraint_metadata(catalog, db, alter, &mut lines);

    if let Some(d) = catalog.databases.get_mut(db) {
        d.tables.insert(alter.table.clone(), updated_table);
    }
    if let Err(e) = crate::catalog::save_catalog(catalog) {
        lines.push(format!(
            "[AlterTable] Warning: failed to save catalog after altering table '{}': {}",
            alter.table, e
        ));
    }

    reindex_after_alter(catalog, db, alter, &indexes, &mut lines)?;

    lines.push(format!(
        "Altered table '{}': {:?}.",
        alter.table, alter.action
    ));
    Ok(StatementResult::lines(lines))
}

/// Find a column by name (case-insensitive) in `table`'s column list.
fn find_column(columns: &[Column], table: &str, name: &str) -> RookResult<usize> {
    columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| RookError::NotFoundIn {
            entity: "Column",
            name: name.to_string(),
            container_kind: "table",
            container: table.to_string(),
        })
}

/// ADD COLUMN: build the column, then backfill existing rows with its DEFAULT.
fn add_column(
    catalog: &mut Catalog,
    db: &str,
    table: &str,
    updated_table: &mut Table,
    column_def: &ColumnDef,
) -> RookResult<()> {
    let mut col = Column::new(
        column_def.name.clone(),
        column_def
            .data_type
            .parse()
            .map_err(|e: String| RookError::TypeMismatch(format!("Invalid data type: {}", e)))?,
    );
    // Apply the column's constraints; refuse the ones this path cannot
    // honour rather than dropping them.
    for constraint in &column_def.constraints {
        match constraint {
            ColumnConstraint::NotNull => {
                col.nullable = false;
                col.constraints.not_null = true;
            }
            // Explicitly nullable: the default.
            ColumnConstraint::Null => {}
            ColumnConstraint::Default(expr) => {
                col.constraints.default = default_value(&col.data_type, expr)
                    .map_err(|e| invalid_default(&col.name, e))?;
            }
            unsupported => {
                let name = match unsupported {
                    ColumnConstraint::PrimaryKey => "PRIMARY KEY",
                    ColumnConstraint::Unique => "UNIQUE",
                    _ => "CHECK",
                };
                return Err(RookError::Internal(format!(
                    "ALTER TABLE ADD COLUMN does not support the constraint '{}'",
                    name
                )));
            }
        }
    }
    if updated_table
        .columns
        .iter()
        .any(|c| c.name.eq_ignore_ascii_case(&column_def.name))
    {
        return Err(RookError::AlreadyExists(format!(
            "Column '{}' already exists in table '{}'",
            column_def.name, table
        )));
    }

    // Existing rows take the column's DEFAULT (NULL without one).
    let fill = col.constraints.default.clone();
    let reject_rows = col.constraints.not_null && fill.is_none();
    let old_schema = schema(&updated_table.columns);
    updated_table.columns.push(col);
    rewrite_rows(
        catalog.engine(),
        db,
        table,
        &old_schema,
        &schema(&updated_table.columns),
        |mut row| {
            if reject_rows {
                return Err(RookError::constraint(
                    ConstraintKind::NotNull,
                    table,
                    None,
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
    Ok(())
}

/// DROP COLUMN: refuse a column another table references, then rewrite the rows.
fn drop_column(
    catalog: &mut Catalog,
    db: &str,
    table: &str,
    updated_table: &mut Table,
    column: &str,
) -> RookResult<()> {
    let pos = find_column(&updated_table.columns, table, column)?;
    // A key that another table references cannot lose a column.
    if let Some(fk) = crate::backend::cache::referencing_fks(catalog.engine(), db, table)
        .iter()
        .find(|fk| {
            !fk.child_table.eq_ignore_ascii_case(table)
                && fk
                    .parent_cols
                    .iter()
                    .any(|c| c.eq_ignore_ascii_case(column))
        })
    {
        return Err(RookError::constraint(
            ConstraintKind::ForeignKey,
            table,
            None,
            format!(
                "Cannot drop column '{}' of table '{}': it is referenced by a foreign key on '{}'",
                column, table, fk.child_table
            ),
        ));
    }
    let old_schema = schema(&updated_table.columns);
    updated_table.columns.remove(pos);
    rewrite_rows(
        catalog.engine(),
        db,
        table,
        &old_schema,
        &schema(&updated_table.columns),
        |mut row| {
            row.remove(pos);
            Ok(row)
        },
    )?;
    Ok(())
}

/// RENAME COLUMN: rename it and re-emit CHECK expressions that name it.
fn rename_column(
    updated_table: &mut Table,
    table: &str,
    old_name: &str,
    new_name: &str,
) -> RookResult<()> {
    let pos = find_column(&updated_table.columns, table, old_name)?;
    updated_table.columns[pos].name = new_name.to_string();
    // CHECK expressions are re-emitted from the catalog on save.
    for c in updated_table.columns.iter_mut() {
        if let Some(expr) = c.constraints.check.take() {
            c.constraints.check = Some(crate::backend::system_table::rename_identifier_in_expr(
                &expr, old_name, new_name,
            ));
        }
    }
    Ok(())
}

/// ALTER COLUMN SET DEFAULT.
fn set_default(
    updated_table: &mut Table,
    table: &str,
    column: &str,
    default_expr: &DefaultValue,
) -> RookResult<()> {
    let pos = find_column(&updated_table.columns, table, column)?;
    let col = &mut updated_table.columns[pos];
    col.constraints.default =
        default_value(&col.data_type, default_expr).map_err(|e| invalid_default(column, e))?;
    Ok(())
}

/// ALTER COLUMN DROP DEFAULT.
fn drop_default(updated_table: &mut Table, table: &str, column: &str) -> RookResult<()> {
    let pos = find_column(&updated_table.columns, table, column)?;
    updated_table.columns[pos].constraints.default = None;
    Ok(())
}

/// ALTER COLUMN SET NOT NULL: refuse when existing rows hold NULLs.
fn set_not_null(
    catalog: &mut Catalog,
    db: &str,
    table: &str,
    updated_table: &mut Table,
    column: &str,
) -> RookResult<()> {
    let col_idx = find_column(&updated_table.columns, table, column)?;
    // Existing rows must have a value; the null bitmap answers that
    // without decoding the row.
    let dat_path = catalog.engine().paths().heap(db, table);
    if Path::new(&dat_path).exists() {
        let heap = crate::backend::cache::lease_heap(catalog.engine(), Path::new(&dat_path))
            .context("Failed to open heap for SET NOT NULL validation")?;
        for result in heap.scan() {
            let (_, _, row_bytes) = result?;
            if crate::types::null_bitmap::is_column_null_in_row(&row_bytes, col_idx)
                .unwrap_or(false)
            {
                return Err(RookError::constraint(
                    ConstraintKind::NotNull,
                    table,
                    None,
                    format!(
                        "Table '{}' contains NULL values in column '{}'; cannot alter to NOT NULL",
                        table, column
                    ),
                ));
            }
        }
    }
    let col = &mut updated_table.columns[col_idx];
    col.nullable = false;
    col.constraints.not_null = true;
    Ok(())
}

/// ALTER COLUMN DROP NOT NULL.
fn drop_not_null(updated_table: &mut Table, table: &str, column: &str) -> RookResult<()> {
    let pos = find_column(&updated_table.columns, table, column)?;
    let col = &mut updated_table.columns[pos];
    col.nullable = true;
    col.constraints.not_null = false;
    Ok(())
}

/// Update sys_constraints metadata for column operations; failures become warnings.
fn update_constraint_metadata(
    catalog: &Catalog,
    db: &str,
    alter: &AlterTablePlan,
    lines: &mut Vec<String>,
) {
    match &alter.action {
        AlterTableAction::DropColumn { column } => {
            if let Err(e) = crate::backend::system_table::delete_column_constraints(
                catalog.engine(),
                db,
                &alter.table,
                column,
            ) {
                lines.push(format!(
                    "[AlterTable] Warning: failed to clean up constraint metadata for column '{}': {}",
                    column, e
                ));
            }
        }
        AlterTableAction::RenameColumn { old_name, new_name } => {
            if let Err(e) = crate::backend::system_table::rename_column_in_constraints(
                catalog.engine(),
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
            // Foreign keys of other tables name this table's column too.
            if let Err(e) = crate::backend::system_table::rename_referenced_column_in_constraints(
                catalog.engine(),
                db,
                &alter.table,
                old_name,
                new_name,
            ) {
                lines.push(format!(
                    "[AlterTable] Warning: failed to update referencing foreign keys for column rename '{}' → '{}': {}",
                    old_name, new_name, e
                ));
            }
        }
        _ => {}
    }
}

/// Re-index after the change: rebuild the indexes of moved rows, or rename
/// the key columns of the indexes that name a renamed column.
fn reindex_after_alter(
    catalog: &mut Catalog,
    db: &str,
    alter: &AlterTablePlan,
    indexes: &[SavedIndex],
    lines: &mut Vec<String>,
) -> RookResult<()> {
    match &alter.action {
        // The rows moved: index them again.
        AlterTableAction::AddColumn { .. } => {
            lines.extend(rebuild_indexes(catalog, db, &alter.table, indexes, None)?);
        }
        AlterTableAction::DropColumn { column } => {
            lines.extend(rebuild_indexes(
                catalog,
                db,
                &alter.table,
                indexes,
                Some(column),
            )?);
        }
        AlterTableAction::RenameColumn { old_name, new_name } => {
            reindex_renamed_column(catalog, db, &alter.table, indexes, old_name, new_name)?;
        }
        _ => {}
    }
    Ok(())
}

/// The rows stay put; only the recorded key columns change. An index named after
/// its columns by CREATE TABLE (`pk_t_a`, `fk_t_a`, `fkp_t_a`, ...) takes the
/// new column name too.
fn reindex_renamed_column(
    catalog: &mut Catalog,
    db: &str,
    table: &str,
    indexes: &[SavedIndex],
    old_name: &str,
    new_name: &str,
) -> RookResult<()> {
    for ix in indexes {
        let columns = ix.column_list();
        if !columns.iter().any(|c| c.eq_ignore_ascii_case(old_name)) {
            continue;
        }
        let renamed: Vec<String> = columns
            .iter()
            .map(|c| {
                if c.eq_ignore_ascii_case(old_name) {
                    new_name.to_string()
                } else {
                    c.clone()
                }
            })
            .collect();
        let new_index_name = ["pk", "uq", "fk", "fkp"]
            .iter()
            .find(|prefix| ix.name == format!("{}_{}_{}", prefix, table, columns.join("_")))
            .map(|prefix| format!("{}_{}_{}", prefix, table, renamed.join("_")))
            .filter(|name| !indexes.iter().any(|other| other.name == *name))
            .unwrap_or_else(|| ix.name.clone());
        crate::backend::system_table::delete_index_metadata(catalog.engine(), db, table, &ix.name)?;
        if new_index_name != ix.name {
            rename_index_file(catalog.engine(), db, table, &ix.name, &new_index_name);
        }
        crate::backend::system_table::insert_index_metadata(
            catalog.engine(),
            db,
            table,
            &new_index_name,
            &renamed.join(","),
            ix.is_unique,
            ix.is_primary,
        )?;
    }
    // Cached constraint metadata still names the old column.
    catalog.engine().invalidate_metadata();
    Ok(())
}

fn rename_table(
    catalog: &mut Catalog,
    db: &str,
    old_name: &str,
    new_name: &str,
    table: crate::catalog::types::Table,
) -> RookResult<StatementResult> {
    if catalog.databases[db].tables.contains_key(new_name) {
        return Err(RookError::AlreadyExists(format!(
            "Table '{}' already exists",
            new_name
        )));
    }
    // Foreign keys store the parent's name: remember who references this
    // table so those rows can follow the rename.
    let referencing: Vec<String> =
        crate::backend::cache::referencing_fks(catalog.engine(), db, old_name)
            .iter()
            .map(|fk| fk.child_table.clone())
            .collect();
    let old_dat = catalog.engine().paths().heap(db, old_name);
    let new_dat = catalog.engine().paths().heap(db, new_name);
    // The open-table handle is keyed by path: drop it before the file moves.
    crate::backend::cache::release_file(&old_dat)?;
    let _ = std::fs::rename(&old_dat, &new_dat);
    let _ = std::fs::rename(
        catalog.engine().paths().heap_fsm(db, old_name),
        catalog.engine().paths().heap_fsm(db, new_name),
    );
    rename_index_files_for_table(catalog.engine(), db, old_name, new_name);
    crate::backend::cache::drop_statistics(catalog.engine(), db, old_name);

    let mut lines = Vec::new();
    if let Err(e) = crate::backend::system_table::rename_table_in_system(
        catalog.engine(),
        db,
        old_name,
        new_name,
    ) {
        lines.push(format!(
            "[AlterTable] Warning: failed to rename '{}' in the system tables: {}",
            old_name, e
        ));
    }
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
        catalog.engine(),
        db,
        &children,
        old_name,
        new_name,
    )?;
    catalog.engine().invalidate_metadata();
    lines.push(format!("Renamed table '{}' to '{}'.", old_name, new_name));
    Ok(StatementResult::lines(lines))
}

/// Move the files of the index `old` of `table` to the name `new`.
fn rename_index_file(
    engine: &crate::backend::engine::Engine,
    db: &str,
    table: &str,
    old: &str,
    new: &str,
) {
    let old_path = engine.paths().index(db, table, old);
    crate::backend::cache::flush_and_evict_btree(engine, &old_path);
    let _ = std::fs::rename(&old_path, engine.paths().index(db, table, new));
    let _ = std::fs::rename(
        crate::backend::index::btree::root_sidecar_path(&old_path),
        crate::backend::index::btree::root_sidecar_path(&engine.paths().index(db, table, new)),
    );
}

fn schema(columns: &[Column]) -> Vec<DataType> {
    columns.iter().map(|c| c.data_type.clone()).collect()
}

fn invalid_default(column: &str, err: impl std::fmt::Display) -> RookError {
    RookError::TypeMismatch(format!("Invalid DEFAULT for column '{}': {}", column, err))
}

/// Rewrite every row of a table for a new schema: each row is decoded with
/// `old_schema`, changed by `change`, and encoded with `new_schema`.
///
/// The rows are written to a new heap file that then replaces the old one,
/// so a failure leaves the table as it was. Index files are removed, since
/// they point at the old row locations.
fn rewrite_rows(
    engine: &crate::backend::engine::Engine,
    db: &str,
    table: &str,
    old_schema: &[DataType],
    new_schema: &[DataType],
    mut change: impl FnMut(Vec<Option<DataValue>>) -> RookResult<Vec<Option<DataValue>>>,
) -> RookResult<()> {
    let dat_path = engine.paths().heap(db, table);
    if !Path::new(&dat_path).exists() {
        return Ok(());
    }

    let old_heap = crate::backend::cache::lease_heap(engine, Path::new(&dat_path))
        .context("Failed to open heap for backfill")?;
    let mut rows: Vec<Vec<u8>> = Vec::new();
    for result in old_heap.scan() {
        let (_, _, row_bytes) = result?;
        let old_row = deserialize_nullable_row(old_schema, &row_bytes)
            .context("Failed to deserialize row during backfill")?;
        let new_row = change(old_row)?;
        rows.push(
            serialize_nullable_typed_row(new_schema, &new_row)
                .context("Failed to serialize row during backfill")?,
        );
    }
    drop(old_heap);

    let tmp_path = crate::backend::paths::with_suffix(&dat_path, ".backfill");
    {
        let mut tmp_heap = HeapManager::create(PathBuf::from(&tmp_path))
            .context("Failed to create temp heap for backfill")?;
        for row in &rows {
            tmp_heap.insert_tuple(row)?;
        }
        tmp_heap.flush()?;
    }

    let fsm_path = engine.paths().heap_fsm(db, table);
    std::fs::remove_file(&dat_path).context("Failed to remove old heap during backfill")?;
    let _ = std::fs::remove_file(&fsm_path);
    std::fs::rename(&tmp_path, &dat_path).context("Failed to rename backfill heap")?;
    let _ = std::fs::rename(
        crate::backend::paths::with_suffix(&tmp_path, ".fsm"),
        &fsm_path,
    );

    // The cached handles still describe the replaced file.
    crate::backend::cache::release_file(Path::new(&dat_path))?;
    crate::backend::cache::release_file(Path::new(&tmp_path))?;

    remove_index_files_for_table(engine, db, table, None);
    Ok(())
}
