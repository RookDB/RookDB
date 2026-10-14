//! Database, table, view and index definition statements, and VACUUM.

use std::str::FromStr;

use rook_ast::{
    ColumnConstraint, CreateDatabasePlan, CreateIndexPlan, CreateTablePlan, DefaultValue,
    DropDatabasePlan, TableConstraint,
};

use super::names::{save_table_constraint, select_plan_references_table};
use super::{Session, StatementResult};
use crate::backend::error::{ConstraintKind, ResultContext, RookError, RookResult};
use crate::backend::executor::create_index::{
    create_index_with_flags, ensure_fk_parent_indexes, ensure_parent_column_index,
};
use crate::catalog::types::{Column, Constraints};
use crate::catalog::{Catalog, create_table as catalog_create_table};
use crate::types::{DataType, DataValue};

pub(super) fn show_databases(catalog: &Catalog) -> StatementResult {
    if catalog.databases.is_empty() {
        return StatementResult::message("No databases found.");
    }
    StatementResult::lines(
        catalog
            .databases
            .keys()
            .map(|name| format!("- {}", name))
            .collect(),
    )
}

pub(super) fn show_tables(catalog: &Catalog, db: &str) -> StatementResult {
    let mut lines = vec![
        "--------------------------".to_string(),
        format!("Tables in Database: {}", db),
        "--------------------------".to_string(),
    ];
    match catalog.databases.get(db) {
        Some(database) if database.tables.is_empty() => {
            lines.push(format!("No tables found in '{}'.", db));
        }
        Some(database) => lines.extend(database.tables.keys().map(|t| format!("- {}", t))),
        None => lines.push(format!("Database '{}' not found.", db)),
    }
    StatementResult::lines(lines)
}

pub(super) fn create_database(
    catalog: &mut Catalog,
    params: &CreateDatabasePlan,
) -> StatementResult {
    crate::catalog::create_database(catalog, &params.database);
    StatementResult::message(format!(
        "Database '{}' created successfully.",
        params.database
    ))
}

pub(super) fn drop_database(
    session: &mut Session,
    params: &DropDatabasePlan,
) -> RookResult<StatementResult> {
    let db_name = &params.database;
    // Defense-in-depth: the name is about to be used in `remove_dir_all`.
    crate::backend::name_validation::validate_database_name(db_name)?;

    let catalog = std::sync::Arc::make_mut(&mut session.catalog);
    if !catalog.databases.contains_key(db_name) {
        return Ok(StatementResult::message(if params.if_exists {
            format!(
                "Database '{}' does not exist (IF EXISTS specified, skipping).",
                db_name
            )
        } else {
            format!("Database '{}' does not exist.", db_name)
        }));
    }

    let mut lines = Vec::new();
    catalog.databases.remove(db_name);
    let engine = std::sync::Arc::clone(catalog.engine());
    // Every cached table handle of the database must not outlive its files.
    let _ = crate::backend::cache::flush_and_clear_heaps(&engine);
    let _ = std::fs::remove_dir_all(engine.paths().database_dir(db_name));
    if let Err(e) = crate::backend::system_table::delete_database_metadata(&engine, db_name) {
        lines.push(format!(
            "[DropDatabase] Warning: failed to clean up system table metadata: {}",
            e
        ));
    }
    if let Err(e) = crate::catalog::save_catalog(catalog) {
        lines.push(format!(
            "[DropDatabase] Warning: failed to save catalog: {}",
            e
        ));
    }
    if session.current_db.as_deref() == Some(db_name.as_str()) {
        session.current_db = None;
    }
    lines.push(format!("Database '{}' dropped successfully.", db_name));
    Ok(StatementResult::lines(lines))
}

pub(super) fn use_database(session: &mut Session, db_name: &str) -> StatementResult {
    StatementResult::message(if session.catalog.databases.is_empty() {
        "No databases found.".to_string()
    } else if session.catalog.databases.contains_key(db_name) {
        session.current_db = Some(db_name.to_string());
        format!("Database '{}' selected.", db_name)
    } else {
        format!("Database '{}' does not exist.", db_name)
    })
}

/// The value of a column DEFAULT; `None` for NULL. The literal is converted
/// from its text for the column's type, so no digits are lost on the way.
pub(super) fn default_value(ty: &DataType, value: &DefaultValue) -> RookResult<Option<DataValue>> {
    let text = match value {
        DefaultValue::Null => return Ok(None),
        DefaultValue::Number(n) | DefaultValue::Text(n) => n.clone(),
        DefaultValue::Boolean(b) => b.to_string(),
    };
    crate::backend::executor::create_index::parse_string_to_value(ty, &text).map(Some)
}

pub(super) fn create_table(
    catalog: &mut Catalog,
    db: &str,
    params: &CreateTablePlan,
) -> RookResult<StatementResult> {
    fn not_created<E: Into<RookError>>(table: &str, cause: E) -> RookError {
        let suffix = |message: String| format!("{}\nTable '{}' was not created.", message, table);
        match cause.into() {
            RookError::ConstraintViolation {
                kind,
                table: constrained,
                column,
                message,
            } => RookError::ConstraintViolation {
                kind,
                table: constrained,
                column,
                message: suffix(message),
            },
            other => RookError::Internal(suffix(other.to_string())),
        }
    }

    // Validate everything before touching the catalog or creating files: a
    // table must never be created with part of its definition missing.
    if catalog
        .databases
        .get(db)
        .is_some_and(|d| d.tables.contains_key(&params.table))
    {
        return Ok(StatementResult::message(format!(
            "Table '{}' already exists in database '{}'.",
            params.table, db
        )));
    }

    let mut columns = Vec::new();
    for col in &params.columns {
        let data_type = DataType::from_str(&col.data_type).map_err(|err| {
            not_created(
                &params.table,
                format!(
                "Unknown type '{}': {}. Supported: SMALLINT, INT, BIGINT, REAL, DOUBLE PRECISION, NUMERIC(p,s), DECIMAL(p,s), BOOLEAN, CHAR(n), CHARACTER(n), VARCHAR(n), DATE, TIME, TIMESTAMP, BIT(n)",
                col.data_type, err
            ))
        })?;

        let has = |c: &ColumnConstraint| col.constraints.contains(c);
        let primary_key = has(&ColumnConstraint::PrimaryKey);
        let not_null = has(&ColumnConstraint::NotNull) || primary_key;
        let unique = has(&ColumnConstraint::Unique) || primary_key;

        let mut default = None;
        if let Some(expr) = col.constraints.iter().find_map(|c| match c {
            ColumnConstraint::Default(e) => Some(e),
            _ => None,
        }) {
            default = default_value(&data_type, expr).map_err(|e| {
                not_created(
                    &params.table,
                    format!(
                        "Invalid DEFAULT for column '{}' of type {}: {}",
                        col.name, col.data_type, e
                    ),
                )
            })?;
        }

        let check = col.constraints.iter().find_map(|c| match c {
            ColumnConstraint::Check(expr) => Some(expr.clone()),
            _ => None,
        });

        columns.push(Column {
            name: col.name.clone(),
            data_type,
            nullable: !not_null,
            constraints: Constraints {
                not_null,
                unique,
                default,
                check,
            },
        });
    }

    for tc in &params.constraints {
        if let TableConstraint::ForeignKey {
            columns: fk_cols,
            ref_table,
            ref_columns,
            ..
        } = tc
        {
            check_foreign_key(catalog, db, params, fk_cols, ref_table, ref_columns)
                .map_err(|e| not_created(&params.table, e))?;
        }
    }

    check_forward_references(catalog, db, params).map_err(|e| not_created(&params.table, e))?;

    catalog_create_table(catalog, db, &params.table, columns);
    let mut lines = vec![format!("Table '{}' created successfully.", params.table)];

    for tc in &params.constraints {
        save_table_constraint(catalog.engine(), db, &params.table, tc);
    }

    // Auto-create B+ Tree indexes on PRIMARY KEY, UNIQUE, and REFERENCES columns.
    //
    // PK/UNIQUE indexes are marked UNIQUE (is_unique=true) so the UNIQUE
    // checker uses them as its fast path instead of an O(n) heap scan per
    // row; the FK child index stays non-unique (it accelerates parent-side
    // referencing checks, which are existence probes).
    // A name another index of the database already has is not reused.
    let index = |name: String, cols: &[String], unique: bool, primary: bool| {
        let name = crate::backend::system_table::free_index_name(catalog.engine(), db, &name);
        create_index_with_flags(catalog, db, &params.table, &name, cols, unique, primary)
    };
    for col in &params.columns {
        let has = |c: &ColumnConstraint| col.constraints.contains(c);
        let cols = std::slice::from_ref(&col.name);
        if has(&ColumnConstraint::PrimaryKey) {
            if let Err(e) = index(
                format!("pk_{}_{}", params.table, col.name),
                cols,
                true,
                true,
            ) {
                lines.push(format!(
                    "Warning: failed to auto-create PRIMARY KEY index: {}",
                    e
                ));
            }
        } else if has(&ColumnConstraint::Unique)
            && let Err(e) = index(
                format!("uq_{}_{}", params.table, col.name),
                cols,
                true,
                false,
            )
        {
            lines.push(format!(
                "Warning: failed to auto-create UNIQUE index: {}",
                e
            ));
        }
    }

    // Auto-create indexes for table-level constraints. An inline REFERENCES
    // arrives here as a table-level foreign key too.
    for tc in &params.constraints {
        let (prefix, cols, unique, primary) = match tc {
            TableConstraint::PrimaryKey { columns } => ("pk", columns, true, true),
            TableConstraint::Unique { columns } => ("uq", columns, true, false),
            TableConstraint::ForeignKey { columns, .. } => ("fk", columns, false, false),
            TableConstraint::Check(_) => continue,
        };
        if cols.is_empty() {
            continue;
        }
        // A column list that an index already covers (the same column
        // declared UNIQUE inline and at table level, an FK column that is
        // the PRIMARY KEY) needs no second one.
        let covered = crate::backend::executor::create_index::load_table_indexes_multi(
            catalog.engine(),
            db,
            &params.table,
        )
        .unwrap_or_default()
        .iter()
        .any(|(_, ix_cols, ix_unique)| {
            ix_cols.len() == cols.len()
                && ix_cols
                    .iter()
                    .zip(cols)
                    .all(|(a, b)| a.eq_ignore_ascii_case(b))
                && (*ix_unique || !unique)
        });
        if covered {
            continue;
        }
        let name = format!("{}_{}_{}", prefix, params.table, cols.join("_"));
        let _ = index(name, cols, unique, primary);
    }

    // Auto-create parent-side FK indexes.
    //
    // FK enforcement probes the PARENT table for every child insert
    // (`value_exists_in_table`); without an index on the referenced column
    // that probe is a full parent-table scan. Two passes:
    //
    // 1. Every `REFERENCES parent(col)` this table declares — covers the
    //    normal case where the parent already exists.
    // 2. Deferred: if the parent is created LATER, `ensure_fk_parent_indexes`
    //    (run after every CREATE TABLE) indexes its referenced columns then.
    for tc in &params.constraints {
        if let TableConstraint::ForeignKey {
            ref_table,
            ref_columns,
            ..
        } = tc
        {
            ensure_parent_column_index(catalog, db, ref_table, ref_columns);
        }
    }

    // Deferred parent-side indexing: if OTHER tables reference THIS table's
    // columns but it did not exist when they were created (forward
    // references), index those referenced columns now that it does.
    ensure_fk_parent_indexes(catalog, db, &params.table);

    Ok(StatementResult::lines(lines))
}

/// Whether `a` and `b` list the same columns, in any order.
fn same_columns(a: &[String], b: &[String]) -> bool {
    a.len() == b.len()
        && a.iter()
            .all(|x| b.iter().any(|y| x.eq_ignore_ascii_case(y)))
}

/// The column sets that are a PRIMARY KEY or UNIQUE key of the table being
/// created.
fn new_table_keys(params: &CreateTablePlan) -> Vec<Vec<String>> {
    let mut sets = Vec::new();
    for col in &params.columns {
        if col
            .constraints
            .iter()
            .any(|c| matches!(c, ColumnConstraint::PrimaryKey | ColumnConstraint::Unique))
        {
            sets.push(vec![col.name.clone()]);
        }
    }
    for tc in &params.constraints {
        if let TableConstraint::PrimaryKey { columns } | TableConstraint::Unique { columns } = tc {
            sets.push(columns.clone());
        }
    }
    sets
}

/// Check the foreign keys that earlier tables declared against the table
/// being created (forward references): what they reference must exist here
/// and be a PRIMARY KEY or UNIQUE key.
fn check_forward_references(
    catalog: &Catalog,
    db: &str,
    params: &CreateTablePlan,
) -> RookResult<()> {
    let referencing = crate::backend::constraint::loaders::load_referencing_foreign_keys(
        catalog.engine(),
        db,
        &params.table,
    )
    .unwrap_or_default();
    let keys = new_table_keys(params);
    for fk in referencing
        .iter()
        .filter(|fk| !fk.child_table.eq_ignore_ascii_case(&params.table))
    {
        let missing = fk.parent_cols.iter().find(|c| {
            !params
                .columns
                .iter()
                .any(|col| col.name.eq_ignore_ascii_case(c))
        });
        if let Some(col) = missing {
            return Err(RookError::constraint(
                ConstraintKind::ForeignKey,
                &params.table,
                None,
                format!(
                    "Column '{}' referenced by a foreign key of '{}' does not exist in table '{}'",
                    col, fk.child_table, params.table
                ),
            ));
        }
        if !keys.iter().any(|k| same_columns(k, &fk.parent_cols)) {
            return Err(RookError::constraint(
                ConstraintKind::ForeignKey,
                &params.table,
                None,
                format!(
                    "The foreign key of '{}' references '{}' ({}), which would not be a PRIMARY KEY or UNIQUE key of '{}'",
                    fk.child_table,
                    params.table,
                    fk.parent_cols.join(", "),
                    params.table
                ),
            ));
        }
    }
    Ok(())
}

/// Check a FOREIGN KEY of the table being created: its columns exist, pair up
/// with the referenced ones, and the referenced columns form a PRIMARY KEY or
/// UNIQUE key of the parent (so a child row matches at most one parent row).
///
/// A parent that does not exist yet (a forward reference) cannot be checked
/// and is accepted.
fn check_foreign_key(
    catalog: &Catalog,
    db: &str,
    params: &CreateTablePlan,
    fk_cols: &[String],
    ref_table: &str,
    ref_columns: &[String],
) -> RookResult<()> {
    let list = |cols: &[String]| cols.join(", ");
    if fk_cols.len() != ref_columns.len() {
        return Err(RookError::constraint(
            ConstraintKind::ForeignKey,
            &params.table,
            None,
            format!(
                "FOREIGN KEY ({}) has {} column(s) but REFERENCES {} ({}) lists {}",
                list(fk_cols),
                fk_cols.len(),
                ref_table,
                list(ref_columns),
                ref_columns.len()
            ),
        ));
    }
    for col in fk_cols {
        if !params
            .columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(col))
        {
            return Err(RookError::constraint(
                ConstraintKind::ForeignKey,
                &params.table,
                None,
                format!(
                    "FOREIGN KEY column '{}' does not exist in table '{}'",
                    col, params.table
                ),
            ));
        }
    }

    // The parent's columns and the column sets that are unique in it.
    let (parent_columns, unique_sets): (Vec<String>, Vec<Vec<String>>) =
        if ref_table.eq_ignore_ascii_case(&params.table) {
            let mut sets = Vec::new();
            for col in &params.columns {
                if col
                    .constraints
                    .iter()
                    .any(|c| matches!(c, ColumnConstraint::PrimaryKey | ColumnConstraint::Unique))
                {
                    sets.push(vec![col.name.clone()]);
                }
            }
            for tc in &params.constraints {
                if let TableConstraint::PrimaryKey { columns }
                | TableConstraint::Unique { columns } = tc
                {
                    sets.push(columns.clone());
                }
            }
            (
                params.columns.iter().map(|c| c.name.clone()).collect(),
                sets,
            )
        } else if let Some(parent) = catalog.databases[db].tables.get(ref_table) {
            let mut sets: Vec<Vec<String>> = parent
                .columns
                .iter()
                .filter(|c| c.constraints.unique)
                .map(|c| vec![c.name.clone()])
                .collect();
            if let Some(meta) = crate::backend::cache::metadata(catalog.engine(), db, ref_table) {
                sets.extend(meta.unique_indexes.iter().map(|(_, cols)| cols.clone()));
            }
            (
                parent.columns.iter().map(|c| c.name.clone()).collect(),
                sets,
            )
        } else {
            return Ok(());
        };

    for col in ref_columns {
        if !parent_columns.iter().any(|c| c.eq_ignore_ascii_case(col)) {
            return Err(RookError::constraint(
                ConstraintKind::ForeignKey,
                &params.table,
                None,
                format!(
                    "Column '{}' referenced by FOREIGN KEY ({}) does not exist in table '{}'",
                    col,
                    list(fk_cols),
                    ref_table
                ),
            ));
        }
    }
    if !unique_sets.iter().any(|s| same_columns(s, ref_columns)) {
        return Err(RookError::constraint(
            ConstraintKind::ForeignKey,
            &params.table,
            None,
            format!(
                "FOREIGN KEY ({}) references '{}' ({}), which is not a PRIMARY KEY or UNIQUE key of '{}'",
                list(fk_cols),
                ref_table,
                list(ref_columns),
                ref_table
            ),
        ));
    }
    Ok(())
}

/// DROP TABLE: removes the table's files, metadata and catalog entry.
pub(super) fn drop_table(
    catalog: &mut Catalog,
    db: &str,
    table: &str,
    if_exists: bool,
    cascade: bool,
) -> RookResult<StatementResult> {
    if !catalog.databases.contains_key(db) {
        return Err(RookError::NotFound {
            entity: "Database",
            name: db.to_string(),
        });
    }
    if !catalog.databases[db].tables.contains_key(table) {
        if if_exists {
            return Ok(StatementResult::message(format!(
                "Table '{}' does not exist (IF EXISTS specified, skipping).",
                table
            )));
        }
        return Err(RookError::NotFoundIn {
            entity: "Table",
            name: table.to_string(),
            container_kind: "database",
            container: db.to_string(),
        });
    }

    // Views that read the table
    let ref_views: Vec<String> = catalog.databases[db]
        .views
        .iter()
        .filter(|(_, def)| {
            serde_json::from_str::<rook_ast::SelectPlan>(&def.query_json)
                .is_ok_and(|plan| select_plan_references_table(&plan, table))
        })
        .map(|(name, _)| name.clone())
        .collect();
    if !ref_views.is_empty() && !cascade {
        return Err(RookError::Internal(format!(
            "Cannot drop table '{}' because it is referenced by views: {}",
            table,
            ref_views.join(", ")
        )));
    }

    // Foreign keys that reference the table (RESTRICT check)
    let ref_fks = crate::backend::constraint::loaders::load_referencing_foreign_keys(
        catalog.engine(),
        db,
        table,
    )
    .context("Failed to check referencing foreign keys")?;
    if !ref_fks.is_empty() && !cascade {
        let child_tbls: Vec<String> = ref_fks.iter().map(|fk| fk.child_table.clone()).collect();
        return Err(RookError::constraint(
            ConstraintKind::ForeignKey,
            table,
            None,
            format!(
                "Cannot drop table '{}' because it is referenced by foreign keys in: {}",
                table,
                child_tbls.join(", ")
            ),
        ));
    }

    let mut lines = Vec::new();
    if cascade && let Some(db_obj) = catalog.databases.get_mut(db) {
        for view_name in &ref_views {
            db_obj.views.remove(view_name);
            lines.push(format!("Cascaded drop of view '{}'.", view_name));
        }
    }
    if cascade && !ref_fks.is_empty() {
        // The child-side index was made for the key about to go.
        for fk in &ref_fks {
            drop_fk_child_index(catalog, db, &fk.child_table, &fk.child_cols);
        }
        let count = crate::backend::system_table::delete_referencing_foreign_keys(
            catalog.engine(),
            db,
            table,
        )?;
        lines.push(format!(
            "Cascaded drop of {} referencing foreign key constraints.",
            count
        ));
    }

    if let Some(db_obj) = catalog.databases.get_mut(db) {
        db_obj.tables.remove(table);
    }
    let dat_path = catalog.engine().paths().heap(db, table);
    // The engine's open-table handle must not outlive the file.
    let _ = crate::backend::cache::evict_heap(catalog.engine(), &dat_path);
    crate::backend::buffer_manager::shared_pool::invalidate(&dat_path);
    let _ = std::fs::remove_file(&dat_path);
    let _ = std::fs::remove_file(catalog.engine().paths().heap_fsm(db, table));
    crate::backend::cache::drop_statistics(catalog.engine(), db, table);
    remove_index_files_for_table(catalog.engine(), db, table, None);

    if let Err(e) = crate::backend::system_table::delete_table_metadata(catalog.engine(), db, table)
    {
        lines.push(format!(
            "[DropTable] Warning: failed to clean up system table metadata for '{}.{}': {}",
            db, table, e
        ));
    }
    if let Err(e) = crate::catalog::save_catalog(catalog) {
        lines.push(format!(
            "[DropTable] Warning: failed to save catalog after dropping table '{}.{}': {}",
            db, table, e
        ));
    }

    lines.push(format!("Dropped table '{}' from database '{}'.", table, db));
    Ok(StatementResult::lines(lines))
}

/// Drop the index CREATE TABLE made for a foreign key on `child.child_cols`,
/// unless another foreign key still uses the same columns.
fn drop_fk_child_index(catalog: &Catalog, db: &str, child: &str, child_cols: &[String]) {
    let engine = catalog.engine();
    let still_used = crate::backend::cache::metadata(engine, db, child).is_some_and(|m| {
        m.foreign_keys
            .iter()
            .filter(|(cols, ..)| same_columns(cols, child_cols))
            .count()
            > 1
    });
    let name = format!("fk_{}_{}", child, child_cols.join("_"));
    let listed = crate::backend::system_table::table_indexes(engine, db, child)
        .iter()
        .any(|ix| ix.name == name);
    if still_used || !listed {
        return;
    }
    remove_index_files_for_table(engine, db, child, Some(&name));
    let _ = crate::backend::system_table::delete_index_metadata(engine, db, child, &name);
}

/// TRUNCATE: replaces the table's heap with an empty one.
pub(super) fn truncate(catalog: &Catalog, db: &str, table: &str) -> RookResult<StatementResult> {
    let db_obj = catalog
        .databases
        .get(db)
        .ok_or_else(|| RookError::NotFound {
            entity: "Database",
            name: db.to_string(),
        })?;
    if !db_obj.tables.contains_key(table) {
        return Err(RookError::NotFoundIn {
            entity: "Table",
            name: table.to_string(),
            container_kind: "database",
            container: db.to_string(),
        });
    }

    // Emptying a referenced parent would orphan its children.
    if let Some(child) = crate::backend::cache::referencing_fks(catalog.engine(), db, table)
        .iter()
        .map(|fk| fk.child_table.clone())
        .find(|child| !child.eq_ignore_ascii_case(table))
    {
        return Err(RookError::constraint(
            ConstraintKind::ForeignKey,
            table,
            None,
            format!(
                "Cannot truncate table '{}': it is referenced by a foreign key on '{}'",
                table, child
            ),
        ));
    }

    let indexes = crate::backend::system_table::table_indexes(catalog.engine(), db, table);
    let dat_path = catalog.engine().paths().heap(db, table);
    let mut hm = crate::heap::HeapManager::create(std::path::PathBuf::from(&dat_path))
        .context("Failed to create table file")?;
    hm.flush().context("Failed to flush table file")?;
    drop(hm);

    // The index files point at the old rows: build them again, empty.
    rebuild_indexes(catalog, db, table, &indexes, None)?;

    Ok(StatementResult::message(format!(
        "Truncated table '{}' (0 rows).",
        table
    )))
}

/// CREATE VIEW: stores the view's query as JSON in the catalog.
pub(super) fn create_view(
    catalog: &mut Catalog,
    db: &str,
    view: &rook_ast::CreateViewPlan,
) -> RookResult<StatementResult> {
    let query_json =
        serde_json::to_string(&view.query).map_err(|e| RookError::Internal(e.to_string()))?;
    let db_obj = catalog
        .databases
        .get_mut(db)
        .ok_or_else(|| RookError::NotFound {
            entity: "Database",
            name: db.to_string(),
        })?;
    if db_obj.tables.contains_key(&view.name) {
        return Err(RookError::AlreadyExists(format!(
            "A table named '{}' already exists",
            view.name
        )));
    }
    if db_obj.views.contains_key(&view.name) && !view.or_replace {
        return Err(RookError::AlreadyExists(format!(
            "View '{}' already exists (use OR REPLACE to modify)",
            view.name
        )));
    }
    db_obj.views.insert(
        view.name.clone(),
        crate::catalog::types::ViewDef { query_json },
    );
    crate::catalog::save_catalog(catalog)?;
    Ok(StatementResult::message(format!(
        "View '{}' created successfully.",
        view.name
    )))
}

pub(super) fn drop_view(
    catalog: &mut Catalog,
    db: &str,
    view_name: &str,
    if_exists: bool,
) -> RookResult<StatementResult> {
    let db_obj = catalog
        .databases
        .get_mut(db)
        .ok_or_else(|| RookError::NotFound {
            entity: "Database",
            name: db.to_string(),
        })?;
    if db_obj.views.remove(view_name).is_none() {
        if if_exists {
            return Ok(StatementResult::message(format!(
                "View '{}' does not exist (IF EXISTS specified, skipping).",
                view_name
            )));
        }
        return Err(RookError::NotFoundIn {
            entity: "View",
            name: view_name.to_string(),
            container_kind: "database",
            container: db.to_string(),
        });
    }
    crate::catalog::save_catalog(catalog)?;
    Ok(StatementResult::message(format!(
        "View '{}' dropped.",
        view_name
    )))
}

/// CREATE TABLE ... AS SELECT: the table takes the query's output schema and
/// rows.
pub(super) fn create_table_as_select(
    catalog: &mut Catalog,
    db: &str,
    ctas: &rook_ast::CreateTableAsSelectPlan,
) -> RookResult<StatementResult> {
    if !catalog.databases.contains_key(db) {
        return Err(RookError::NotFound {
            entity: "Database",
            name: db.to_string(),
        });
    }
    if catalog.databases[db].tables.contains_key(&ctas.table) {
        return Err(RookError::AlreadyExists(format!(
            "Table '{}' already exists",
            ctas.table
        )));
    }

    let query_plan = rook_ast::QueryPlan::Select(*ctas.query.clone());
    let logical = crate::planner::plan_query(&query_plan, catalog, db)
        .map_err(|e| RookError::Internal(format!("Failed to plan SELECT query: {}", e)))?;
    let (tuples, output_schema) =
        crate::backend::executor::physical::engine::execute_plan_collect_with_schema(
            &logical, catalog, db,
        )
        .context("Failed to execute SELECT query")?;

    let columns: Vec<Column> = output_schema
        .iter()
        .map(|ci| Column {
            name: ci.name.clone(),
            data_type: ci.data_type.clone(),
            nullable: true,
            constraints: Constraints::default(),
        })
        .collect();
    if columns.is_empty() {
        return Err(RookError::Internal(
            "Cannot determine column types for CREATE TABLE AS SELECT".to_string(),
        ));
    }
    catalog_create_table(catalog, db, &ctas.table, columns);

    let mut lines = Vec::new();
    let mut inserted = 0usize;
    for tuple in &tuples {
        match crate::backend::executor::insert_row(catalog, db, &ctas.table, &tuple.values) {
            Ok(_) => inserted += 1,
            Err(RookError::Io(e)) => lines.push(format!("Insert error: {}", e)),
            Err(_) => lines.push("Insert failed for one row.".to_string()),
        }
    }
    lines.push(format!(
        "Table '{}' created successfully with {} row(s).",
        ctas.table, inserted
    ));
    Ok(StatementResult::lines(lines))
}

pub(super) fn create_index(
    catalog: &Catalog,
    db: &str,
    params: &CreateIndexPlan,
) -> RookResult<StatementResult> {
    if params.if_not_exists
        && !params.index_name.is_empty()
        && crate::backend::system_table::index_owner(catalog.engine(), db, &params.index_name)
            .is_some()
    {
        return Ok(StatementResult::message(format!(
            "Index '{}' already exists (IF NOT EXISTS specified, skipping).",
            params.index_name
        )));
    }
    let count = crate::backend::executor::create_index::create_index(
        catalog,
        db,
        &params.table_name,
        &params.index_name,
        &params.columns,
    )
    .map_err(|e| RookError::Internal(e.to_string()).with_context("Failed to create index"))?;
    Ok(StatementResult::message(format!(
        "Created index '{}' on {}.{}({}) with {} entries.",
        params.index_name,
        db,
        params.table_name,
        params.columns.join(","),
        count
    )))
}

/// VACUUM: reclaim space from soft-deleted rows and rebuild indexes.
pub(super) fn vacuum(catalog: &Catalog, db: &str, table: &str) -> RookResult<StatementResult> {
    let stats = crate::backend::executor::vacuum::vacuum_table(catalog, db, table)
        .map_err(|e| RookError::Internal(e).with_context("VACUUM failed"))?;
    let mut lines = vec![format!(
        "VACUUM '{}.{}' complete: {} page(s) compacted, {} dead tuple(s) reclaimed, {} index(es) rebuilt.",
        db, table, stats.pages_compacted, stats.dead_tuples_before, stats.indexes_rebuilt
    )];
    if stats.pages_compacted == 0 {
        lines.push("Table is already clean — nothing to do.".to_string());
    }
    // The rows moved; what the planner knows of them is gathered anew.
    crate::backend::cache::analyze_table(catalog, db, table)
        .map_err(|e| RookError::Io(e).with_context("VACUUM statistics failed"))?;
    Ok(StatementResult::lines(lines))
}

/// ANALYZE: gather the planner's statistics for one table, or every table of
/// the database.
pub(super) fn analyze(
    catalog: &Catalog,
    db: &str,
    table: Option<&str>,
) -> RookResult<StatementResult> {
    let db_obj = catalog
        .databases
        .get(db)
        .ok_or_else(|| RookError::Internal(format!("Database '{}' not found", db)))?;
    let mut tables: Vec<&str> = match table {
        Some(t) if db_obj.tables.contains_key(t) => vec![t],
        Some(t) => {
            return Err(RookError::NotFound {
                entity: "Table",
                name: format!("{}.{}", db, t),
            });
        }
        None => db_obj.tables.keys().map(String::as_str).collect(),
    };
    tables.sort_unstable();
    let mut lines = Vec::new();
    for t in tables {
        let stats = crate::backend::cache::analyze_table(catalog, db, t)
            .map_err(|e| RookError::Io(e).with_context(format!("ANALYZE '{}' failed", t)))?;
        lines.push(format!(
            "ANALYZE '{}.{}' complete: {} row(s), {} column(s).",
            db,
            t,
            stats.total_tuple_count,
            stats.columns.len()
        ));
    }
    Ok(StatementResult::lines(lines))
}

/// Remove index files (*.idx and *.idx.meta) for a given table.
///
/// When `index_name` is `Some(name)`, removes only the specific named index file
/// (`{table}.{name}.idx` and `{table}.{name}.idx.meta`).
///
/// When `index_name` is `None`, removes ALL index files for the table:
/// - Legacy `{table}.idx` and `{table}.idx.meta`
/// - All named `{table}.{any_name}.idx` and `{table}.{any_name}.idx.meta`
pub(super) fn remove_index_files_for_table(
    engine: &crate::backend::engine::Engine,
    db: &str,
    table: &str,
    index_name: Option<&str>,
) {
    use crate::backend::cache::evict_btree;

    let base_dir = engine.paths().database_dir(db);
    let base_path = std::path::Path::new(&base_dir);
    if !base_path.exists() {
        return;
    }
    let prefix = format!("{}.", table);

    // Drop any cached B+ Tree handle(s) for the files being removed so a
    // later CREATE INDEX on the same name cannot reuse the stale tree.
    if let Some(name) = index_name {
        evict_btree(
            engine,
            std::path::Path::new(&engine.paths().index(db, table, name)),
        );
    } else if let Ok(entries) = std::fs::read_dir(base_path) {
        for entry in entries.flatten() {
            let fname = entry.file_name().to_string_lossy().to_string();
            if fname.starts_with(&prefix) && fname.ends_with(".idx") {
                evict_btree(engine, &entry.path());
            }
        }
    }

    // Index set changed — forget memoised discovery/metadata.
    engine.invalidate_metadata();

    if let Some(name) = index_name {
        let idx_path = engine.paths().index(db, table, name);
        let _ = std::fs::remove_file(&idx_path);
    } else {
        if let Ok(entries) = std::fs::read_dir(base_path) {
            for entry in entries.flatten() {
                let fname = entry.file_name().to_string_lossy().to_string();
                if fname.starts_with(&prefix) && fname.ends_with(".idx") {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }
}

/// Build the indexes `indexes` of a table again from its rows, after a change
/// that rewrote or emptied it. An index on the column `dropped`, which no
/// longer exists, is dropped instead; the result has a line for each.
pub(crate) fn rebuild_indexes(
    catalog: &Catalog,
    db: &str,
    table: &str,
    indexes: &[crate::backend::system_table::SavedIndex],
    dropped: Option<&str>,
) -> RookResult<Vec<String>> {
    remove_index_files_for_table(catalog.engine(), db, table, None);
    let mut lines = Vec::new();
    for ix in indexes {
        // The row is written again by create_index_with_flags.
        let _ = crate::backend::system_table::delete_index_metadata(
            catalog.engine(),
            db,
            table,
            &ix.name,
        );
        let columns = ix.column_list();
        if let Some(col) = dropped
            && columns.iter().any(|c| c.eq_ignore_ascii_case(col))
        {
            lines.push(format!(
                "Dropped index '{}', which used column '{}'.",
                ix.name, col
            ));
            continue;
        }
        create_index_with_flags(
            catalog,
            db,
            table,
            &ix.name,
            &columns,
            ix.is_unique,
            ix.is_primary,
        )
        .map_err(|e| {
            RookError::Internal(format!("Failed to rebuild index '{}': {}", ix.name, e))
        })?;
    }
    Ok(lines)
}

/// Rename all index files for a table from `old_name` to `new_name`.
pub(super) fn rename_index_files_for_table(
    engine: &crate::backend::engine::Engine,
    db: &str,
    old_name: &str,
    new_name: &str,
) {
    let base_dir = engine.paths().database_dir(db);
    let base_path = std::path::Path::new(&base_dir);
    if !base_path.exists() {
        return;
    }

    // {old_name}.{index_name}.idx → {new_name}.{index_name}.idx
    if let Ok(entries) = std::fs::read_dir(base_path) {
        let prefix = format!("{}.", old_name);
        for entry in entries.flatten() {
            let fname = entry.file_name().to_string_lossy().to_string();
            if let Some(suffix) = fname.strip_prefix(&prefix)
                && (fname.ends_with(".idx") || fname.ends_with(".idx.root"))
            {
                if fname.ends_with(".idx") {
                    crate::backend::cache::flush_and_evict_btree(engine, &entry.path());
                }
                let new_fname = base_path.join(format!("{}.{}", new_name, suffix));
                let _ = std::fs::rename(entry.path(), &new_fname);
            }
        }
    }
}

/// DROP INDEX: deletes the index files and their `sys_indexes` row.
pub(super) fn drop_index(
    catalog: &Catalog,
    db: &str,
    index_name: &str,
    table_name: &str,
    if_exists: bool,
) -> RookResult<StatementResult> {
    if !catalog.databases.contains_key(db) {
        return Err(RookError::NotFound {
            entity: "Database",
            name: db.to_string(),
        });
    }
    let skipped = || {
        StatementResult::message(format!(
            "Index '{}' does not exist (IF EXISTS specified, skipping).",
            index_name
        ))
    };
    let mut lines = Vec::new();
    let mut forget = |table: &str| {
        if let Err(e) = crate::backend::system_table::delete_index_metadata(
            catalog.engine(),
            db,
            table,
            index_name,
        ) {
            lines.push(format!(
                "[DropIndex] Warning: failed to clean up sys_indexes for '{}': {}",
                index_name, e
            ));
        }
    };

    if !table_name.is_empty() {
        let named_idx_path = catalog.engine().paths().index(db, table_name, index_name);

        if std::path::Path::new(&named_idx_path).exists() {
            remove_index_files_for_table(catalog.engine(), db, table_name, Some(index_name));
        } else if if_exists {
            return Ok(skipped());
        } else {
            return Err(RookError::Internal(format!(
                "Index file not found for table '{}'",
                table_name
            )));
        }
        forget(table_name);
        lines.push(format!(
            "Dropped index '{}' from table '{}'.",
            index_name, table_name
        ));
        return Ok(StatementResult::lines(lines));
    }

    // No table name: find the index file by scanning the database directory.
    let base_dir = catalog.engine().paths().database_dir(db);
    let entries = std::fs::read_dir(&base_dir).map_err(|_| {
        RookError::Internal(format!(
            "Database directory '{}' not found",
            base_dir.display()
        ))
    })?;
    for entry in entries.flatten() {
        let fname = entry.file_name().to_string_lossy().to_string();
        let Some(without_suffix) = fname.strip_suffix(".idx") else {
            continue;
        };
        // Named index file `{table}.{index}.idx`.
        let tbl_name = match without_suffix.rfind('.') {
            Some(dot) if without_suffix[dot + 1..].eq_ignore_ascii_case(index_name) => {
                &without_suffix[..dot]
            }
            _ => continue,
        };
        std::fs::remove_file(entry.path())?;
        let tbl_name = tbl_name.to_string();
        forget(&tbl_name);
        lines.push(format!(
            "Dropped index '{}' from table '{}'.",
            index_name, tbl_name
        ));
        return Ok(StatementResult::lines(lines));
    }

    if if_exists {
        Ok(skipped())
    } else {
        Err(RookError::NotFoundIn {
            entity: "Index",
            name: index_name.to_string(),
            container_kind: "database",
            container: db.to_string(),
        })
    }
}
