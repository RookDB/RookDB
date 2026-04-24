//! Catalog manager – high-level operations on databases and tables.
//!
//! Catalog system: page-based storage under database/global/catalog_pages/
//!
//! On a fresh install init_catalog() calls bootstrap_catalog() which sets
//! up the page-based backend and pre-populates built-in types.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::buffer_manager::BufferManager;
use crate::catalog::constraints::{add_not_null_constraint, add_primary_key_constraint};
use crate::catalog::indexes::{drop_index, get_indexes_for_table};
use crate::catalog::oid::OidCounter;
use crate::catalog::page_manager::{
    CAT_COLUMN, CAT_CONSTRAINT, CAT_DATABASE, CAT_INDEX, CAT_TABLE, CAT_TYPE, CatalogPageManager,
};
use crate::catalog::serialize::{
    deserialize_column_tuple, deserialize_constraint_tuple, deserialize_database_tuple,
    deserialize_index_tuple, deserialize_table_tuple, deserialize_type_tuple,
    serialize_column_tuple, serialize_database_tuple, serialize_table_tuple, serialize_type_tuple,
};
use crate::catalog::types::{
    Catalog, CatalogError, Column, ColumnDefinition, ConstraintDefinition, CatalogDataType, Database,
    Encoding, Table, TableMetadata, TableStatistics, TableType,
};
use crate::heap::init_table;
use crate::layout::{
    CATALOG_PAGES_DIR, DATABASE_DIR, GLOBAL_DIR, SYSTEM_DB_OID, TABLE_DIR_TEMPLATE,
    TABLE_FILE_TEMPLATE,
};

// ─────────────────────────────────────────────────────────────
// Timestamp helper
// ─────────────────────────────────────────────────────────────

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ─────────────────────────────────────────────────────────────
// 3.1.1 – Catalog initialisation
// ─────────────────────────────────────────────────────────────

/// Dual-mode catalog initialisation (called at startup).
pub fn init_catalog(bm: &mut BufferManager) {
    let global = Path::new(GLOBAL_DIR);
    if !global.exists() {
        fs::create_dir_all(global).expect("Failed to create global dir");
    }
    let base = Path::new(DATABASE_DIR);
    if !base.exists() {
        fs::create_dir_all(base).expect("Failed to create base dir");
    }

    if Path::new(CATALOG_PAGES_DIR).exists() {
        println!("Page-based catalog backend detected.");
    } else {
        println!("No catalog found – bootstrapping ...");
        if let Err(e) = bootstrap_catalog(bm) {
            eprintln!("Bootstrap failed: {}", e);
        }
    }
}

/// Bootstrap the self-hosting catalog: creates system catalog .dat files,
/// inserts built-in types, and writes the system database record.
pub fn bootstrap_catalog(bm: &mut BufferManager) -> Result<(), CatalogError> {
    let global = Path::new(GLOBAL_DIR);
    if !global.exists() {
        fs::create_dir_all(global)?;
    }
    let base = Path::new(DATABASE_DIR);
    if !base.exists() {
        fs::create_dir_all(base)?;
    }

    OidCounter::initialize()?;

    let mut pm = CatalogPageManager::new();
    pm.initialize_files()?;

    // Register built-in types
    for dt in CatalogDataType::all_builtins() {
        let bytes = serialize_type_tuple(&dt);
        pm.insert_catalog_tuple(bm, CAT_TYPE, bytes)?;
    }

    // Insert system database record
    let sys_bytes = serialize_database_tuple(
        SYSTEM_DB_OID,
        "system",
        "rookdb",
        now_unix(),
        Encoding::UTF8.to_u8(),
    );
    pm.insert_catalog_tuple(bm, CAT_DATABASE, sys_bytes)?;

    println!("Bootstrap complete – page-based catalog initialized.");
    Ok(())
}

/// Create (or verify) the CatalogPageManager after bootstrap.
pub fn init_catalog_page_storage() -> Result<CatalogPageManager, CatalogError> {
    let pm = CatalogPageManager::new();
    pm.initialize_files()?;
    Ok(pm)
}

// ─────────────────────────────────────────────────────────────
// Load / save
// ─────────────────────────────────────────────────────────────

/// Load the Catalog from the active storage backend.
pub fn load_catalog(bm: &mut BufferManager) -> Catalog {
    if Path::new(CATALOG_PAGES_DIR).exists() {
        match load_catalog_from_pages(bm) {
            Ok(cat) => return cat,
            Err(e) => eprintln!("Page catalog load failed: {}", e),
        }
    }
    Catalog::new()
}

fn load_catalog_from_pages(bm: &mut BufferManager) -> Result<Catalog, CatalogError> {
    let pm = CatalogPageManager::new();

    let mut oid_ctr = OidCounter::new();
    let _ = oid_ctr.load();

    let mut catalog = Catalog::new();
    catalog.oid_counter = oid_ctr.next_oid;
    catalog.page_backend_active = true;

    // ── databases ────────────────────────────────────────────────
    for bytes in pm.scan_catalog(bm, CAT_DATABASE)? {
        let (oid, name, owner, created_at, enc) =
            deserialize_database_tuple(&bytes).map_err(CatalogError::IoError)?;
        if name == "system" {
            continue;
        }
        catalog.databases.insert(
            name.clone(),
            Database {
                db_oid: oid,
                db_name: name,
                tables: HashMap::new(),
                owner,
                encoding: Encoding::from_u8(enc),
                created_at,
            },
        );
    }

    // ── tables ───────────────────────────────────────────────────
    for bytes in pm.scan_catalog(bm, CAT_TABLE)? {
        let (toid, tname, db_oid, ttype_b, row_count, page_count, created_at) =
            deserialize_table_tuple(&bytes).map_err(CatalogError::IoError)?;
        if ttype_b == 1 {
            continue;
        } // skip system catalogs
        let table = Table {
            table_oid: toid,
            table_name: tname.clone(),
            db_oid,
            columns: Vec::new(),
            constraints: Vec::new(),
            indexes: Vec::new(),
            table_type: TableType::UserTable,
            statistics: TableStatistics {
                row_count,
                page_count,
                created_at,
                last_modified: 0,
            },
        };
        for db in catalog.databases.values_mut() {
            if db.db_oid == db_oid {
                db.tables.insert(tname, table);
                break;
            }
        }
    }

    // ── columns ──────────────────────────────────────────────────
    for bytes in pm.scan_catalog(bm, CAT_COLUMN)? {
        let (coid, toid, cname, cpos, dt, tm, is_nullable, default_val, constraint_oids) =
            deserialize_column_tuple(&bytes).map_err(CatalogError::IoError)?;
        let col = Column {
            column_oid: coid,
            name: cname,
            column_position: cpos,
            data_type: dt,
            type_modifier: tm,
            is_nullable,
            default_value: default_val,
            constraints: constraint_oids,
        };
        for db in catalog.databases.values_mut() {
            for table in db.tables.values_mut() {
                if table.table_oid == toid {
                    table.columns.push(col.clone());
                    break;
                }
            }
        }
    }
    for db in catalog.databases.values_mut() {
        for table in db.tables.values_mut() {
            table.columns.sort_by_key(|c| c.column_position);
        }
    }

    // ── constraints ───────────────────────────────────────────────
    for bytes in pm.scan_catalog(bm, CAT_CONSTRAINT)? {
        let c = deserialize_constraint_tuple(&bytes).map_err(CatalogError::IoError)?;
        for db in catalog.databases.values_mut() {
            for table in db.tables.values_mut() {
                if table.table_oid == c.table_oid {
                    table.constraints.push(c.clone());
                    break;
                }
            }
        }
    }

    // ── index OIDs ────────────────────────────────────────────────
    for bytes in pm.scan_catalog(bm, CAT_INDEX)? {
        let idx = deserialize_index_tuple(&bytes).map_err(CatalogError::IoError)?;
        for db in catalog.databases.values_mut() {
            for table in db.tables.values_mut() {
                if table.table_oid == idx.table_oid && !table.indexes.contains(&idx.index_oid) {
                    table.indexes.push(idx.index_oid);
                    break;
                }
            }
        }
    }

    Ok(catalog)
}

// ─────────────────────────────────────────────────────────────
// 3.1.2 – Type helpers
// ─────────────────────────────────────────────────────────────

pub fn register_builtin_types(
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
) -> Result<(), CatalogError> {
    let existing: Vec<String> = pm
        .scan_catalog(bm, CAT_TYPE)?
        .iter()
        .filter_map(|b| deserialize_type_tuple(b).ok().map(|dt| dt.type_name))
        .collect();
    for dt in CatalogDataType::all_builtins() {
        if !existing.contains(&dt.type_name) {
            pm.insert_catalog_tuple(bm, CAT_TYPE, serialize_type_tuple(&dt))?;
        }
    }
    Ok(())
}

pub fn lookup_type_by_name(
    pm: &CatalogPageManager,
    bm: &mut BufferManager,
    type_name: &str,
) -> Result<CatalogDataType, CatalogError> {
    if let Some(dt) = CatalogDataType::from_name(type_name) {
        return Ok(dt);
    }
    for bytes in pm.scan_catalog(bm, CAT_TYPE)? {
        let dt = deserialize_type_tuple(&bytes).map_err(CatalogError::IoError)?;
        if dt.type_name.eq_ignore_ascii_case(type_name) {
            return Ok(dt);
        }
    }
    Err(CatalogError::TypeNotFound(type_name.to_string()))
}

// ─────────────────────────────────────────────────────────────
// 3.1.5 – Database operations
// ─────────────────────────────────────────────────────────────

pub fn show_databases(_catalog: &Catalog, pm: &mut CatalogPageManager, bm: &mut BufferManager) {
    println!("---------------------------------------------------------------");
    println!(
        "{:<20} | {:<15} | {:<20}",
        "Database", "Owner", "Created At"
    );
    println!("---------------------------------------------------------------");

    // Fetch directly from page-based backend as required
    let records = pm.scan_catalog(bm, CAT_DATABASE).unwrap_or_default();
    if records.is_empty() {
        println!("No databases found.\n");
        return;
    }

    for bytes in records {
        if let Ok((_oid, name, owner, created_at, _enc)) = deserialize_database_tuple(&bytes) {
            println!("{:<20} | {:<15} | {}", name, owner, created_at);
        }
    }
    println!("---------------------------------------------------------------\n");
}

/// Enhanced database creation with page-based catalog persistence.
pub fn create_database(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    db_name: &str,
    owner: &str,
    encoding: Encoding,
) -> Result<u32, CatalogError> {
    if db_name.is_empty() {
        return Err(CatalogError::InvalidOperation("Empty db name".into()));
    }
    if catalog.databases.contains_key(db_name) {
        return Err(CatalogError::DatabaseAlreadyExists(db_name.into()));
    }

    let db_oid = catalog.alloc_oid();
    let created_at = now_unix();
    let db_path = TABLE_DIR_TEMPLATE.replace("{database}", db_name);
    if !Path::new(&db_path).exists() {
        fs::create_dir_all(&db_path)?;
    }

    let bytes = serialize_database_tuple(db_oid, db_name, owner, created_at, encoding.to_u8());
    pm.insert_catalog_tuple(bm, CAT_DATABASE, bytes)?;

    catalog.databases.insert(
        db_name.to_string(),
        Database {
            db_oid,
            db_name: db_name.to_string(),
            tables: HashMap::new(),
            owner: owner.to_string(),
            encoding,
            created_at,
        },
    );
    catalog.cache.invalidate_database(db_name);
    Ok(db_oid)
}

/// Drop a database and all its tables.
pub fn drop_database(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    db_name: &str,
) -> Result<(), CatalogError> {
    let db_oid = catalog
        .databases
        .get(db_name)
        .map(|d| d.db_oid)
        .ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;

    let table_oids: Vec<u32> = catalog
        .databases
        .get(db_name)
        .map(|db| db.tables.values().map(|t| t.table_oid).collect())
        .unwrap_or_default();

    for oid in table_oids {
        drop_table(catalog, pm, bm, oid)?;
    }

    let _ = pm
        .find_catalog_tuple(bm, CAT_DATABASE, |b| {
            deserialize_database_tuple(b)
                .map(|(oid, ..)| oid == db_oid)
                .unwrap_or(false)
        })
        .and_then(|res| {
            if let Some((pn, slot, _)) = res {
                pm.delete_catalog_tuple(bm, CAT_DATABASE, pn, slot)?;
            }
            Ok(())
        });

    let _ = fs::remove_dir_all(TABLE_DIR_TEMPLATE.replace("{database}", db_name));
    catalog.databases.remove(db_name);
    catalog.cache.invalidate_database(db_name);
    Ok(())
}

// ─────────────────────────────────────────────────────────────
// 3.1.6 – Table operations
// ─────────────────────────────────────────────────────────────

/// Enhanced table creation with page-based catalog and constraints.
pub fn create_table(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    db_name: &str,
    table_name: &str,
    col_defs: Vec<ColumnDefinition>,
    constraint_defs: Vec<ConstraintDefinition>,
) -> Result<u32, CatalogError> {
    let db = catalog
        .databases
        .get(db_name)
        .ok_or_else(|| CatalogError::DatabaseNotFound(db_name.to_string()))?;
    if db.tables.contains_key(table_name) {
        return Err(CatalogError::TableAlreadyExists(table_name.to_string()));
    }
    let db_oid = db.db_oid;
    let table_oid = catalog.alloc_oid();
    let created_at = now_unix();

    // Build column metadata
    let mut columns: Vec<Column> = Vec::new();
    for (pos, def) in col_defs.iter().enumerate() {
        let dt = CatalogDataType::from_name(&def.type_name)
            .ok_or_else(|| CatalogError::TypeNotFound(def.type_name.clone()))?;
        let col_oid = catalog.alloc_oid();
        let type_mod_val = col_defs[pos]
            .type_modifier
            .map(|n| crate::catalog::types::TypeModifier::VarcharLen(n));
        let col_bytes = serialize_column_tuple(
            col_oid,
            table_oid,
            &def.name,
            (pos + 1) as u16,
            &dt,
            type_mod_val.as_ref(),
            def.is_nullable,
            def.default_value.as_ref(),
            &[],
        );
        pm.insert_catalog_tuple(bm, CAT_COLUMN, col_bytes)?;
        columns.push(Column {
            column_oid: col_oid,
            name: def.name.clone(),
            column_position: (pos + 1) as u16,
            data_type: dt,
            type_modifier: type_mod_val,
            is_nullable: def.is_nullable,
            default_value: def.default_value.clone(),
            constraints: Vec::new(),
        });
    }

  // Insert table metadata into catalog
    let new_table = Table {
        columns,
        row_count: 0,      // Will be updated when table is populated
        page_count: 0,     // Initial table has no data pages (only header)
        avg_row_size: 0,   // Will be estimated from actual data
    };
    database.tables.insert(table_name.to_string(), new_table);

    // Persist catalog changes
    save_catalog(catalog);

    // Create table data file
    let file_path = TABLE_FILE_TEMPLATE
        .replace("{database}", db_name)
        .replace("{table}", table_name);
  
    if !Path::new(&file_path).exists() {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&file_path)?;
        init_table(&mut f)?;
    }

    // Persist to pg_table
    pm.insert_catalog_tuple(
        bm,
        CAT_TABLE,
        serialize_table_tuple(table_oid, table_name, db_oid, 0, 0, 1, created_at),
    )?;

    let table = Table {
        table_oid,
        table_name: table_name.to_string(),
        db_oid,
        columns,
        constraints: Vec::new(),
        indexes: Vec::new(),
        table_type: TableType::UserTable,
        statistics: TableStatistics {
            row_count: 0,
            page_count: 1,
            created_at,
            last_modified: 0,
        },
    };
    catalog
        .databases
        .get_mut(db_name)
        .unwrap()
        .tables
        .insert(table_name.to_string(), table);
    catalog.cache.invalidate_table(db_oid, table_name);

    // Apply constraints
    for cdef in constraint_defs {
        match cdef {
            ConstraintDefinition::PrimaryKey {
                columns: cols,
                name,
            } => {
                add_primary_key_constraint(catalog, pm, bm, table_oid, cols, name)?;
            }
            ConstraintDefinition::NotNull { column } => {
                let oid = catalog
                    .databases
                    .get(db_name)
                    .and_then(|db| db.tables.get(table_name))
                    .and_then(|t| t.columns.iter().find(|c| c.name == column))
                    .map(|c| c.column_oid)
                    .ok_or_else(|| CatalogError::ColumnNotFound(column.clone()))?;
                add_not_null_constraint(catalog, pm, bm, table_oid, oid)?;
            }
            ConstraintDefinition::ForeignKey {
                columns: cols,
                referenced_table,
                referenced_columns,
                on_delete,
                on_update,
                name,
            } => {
                let ref_oid = catalog
                    .databases
                    .get(db_name)
                    .and_then(|db| db.tables.get(&referenced_table))
                    .map(|t| t.table_oid)
                    .ok_or_else(|| CatalogError::TableNotFound(referenced_table.clone()))?;
                crate::catalog::constraints::add_foreign_key_constraint(
                    catalog,
                    pm,
                    bm,
                    table_oid,
                    cols,
                    ref_oid,
                    referenced_columns,
                    on_delete,
                    on_update,
                    name,
                )?;
            }
            ConstraintDefinition::Unique {
                columns: cols,
                name,
            } => {
                crate::catalog::constraints::add_unique_constraint(
                    catalog, pm, bm, table_oid, cols, name,
                )?;
            }
            _ => {}
        }
    }

    Ok(table_oid)
}

/// Drop a table and its dependent objects.
pub fn drop_table(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    table_oid: u32,
) -> Result<(), CatalogError> {
    // Check for FK dependencies from other tables
    for bytes in pm.scan_catalog(bm, CAT_CONSTRAINT)? {
        let c = deserialize_constraint_tuple(&bytes).map_err(CatalogError::IoError)?;
        if let crate::catalog::types::ConstraintMetadata::ForeignKey {
            referenced_table_oid,
            ..
        } = &c.metadata
        {
            if *referenced_table_oid == table_oid && c.table_oid != table_oid {
                return Err(CatalogError::ForeignKeyDependency(
                    c.constraint_name.clone(),
                ));
            }
        }
    }

    // Drop indexes
    for oid in get_indexes_for_table(pm, bm, table_oid)?
        .iter()
        .map(|i| i.index_oid)
        .collect::<Vec<_>>()
    {
        let _ = drop_index(catalog, pm, bm, oid);
    }

    let (db_name, table_name) = {
        let mut found = None;
        for db in catalog.databases.values() {
            for t in db.tables.values() {
                if t.table_oid == table_oid {
                    found = Some((db.db_name.clone(), t.table_name.clone()));
                    break;
                }
            }
        }
        found.ok_or_else(|| CatalogError::TableNotFound(table_oid.to_string()))?
    };

    let _ = fs::remove_file(
        TABLE_FILE_TEMPLATE
            .replace("{database}", &db_name)
            .replace("{table}", &table_name),
    );

    let _ = pm
        .find_catalog_tuple(bm, CAT_TABLE, |b| {
            deserialize_table_tuple(b)
                .map(|(oid, ..)| oid == table_oid)
                .unwrap_or(false)
        })
        .and_then(|res| {
            if let Some((pn, slot, _)) = res {
                pm.delete_catalog_tuple(bm, CAT_TABLE, pn, slot)?;
            }
            Ok(())
        });

    if let Some(db) = catalog.databases.get_mut(&db_name) {
        let db_oid = db.db_oid;
        db.tables.remove(&table_name);
        catalog.cache.invalidate_table(db_oid, &table_name);
        catalog.cache.invalidate_constraints(table_oid);
        catalog.cache.invalidate_indexes(table_oid);
    }
    Ok(())
}

/// Add a new column to an existing table.
pub fn alter_table_add_column(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    table_oid: u32,
    col_def: ColumnDefinition,
) -> Result<u32, CatalogError> {
    let dt = CatalogDataType::from_name(&col_def.type_name)
        .ok_or_else(|| CatalogError::TypeNotFound(col_def.type_name.clone()))?;

    if !col_def.is_nullable && col_def.default_value.is_none() {
        return Err(CatalogError::InvalidOperation(
            "Cannot add NOT NULL column without default to existing table".into(),
        ));
    }

    let mut col_pos = 1u16;
    for db in catalog.databases.values() {
        for t in db.tables.values() {
            if t.table_oid == table_oid {
                if t.columns.iter().any(|c| c.name == col_def.name) {
                    return Err(CatalogError::InvalidOperation(format!(
                        "Column '{}' already exists",
                        col_def.name
                    )));
                }
                col_pos = t.columns.len() as u16 + 1;
            }
        }
    }

    let col_oid = catalog.alloc_oid();
    let type_mod = col_def
        .type_modifier
        .map(|n| crate::catalog::types::TypeModifier::VarcharLen(n));
    let col_bytes = serialize_column_tuple(
        col_oid,
        table_oid,
        &col_def.name,
        col_pos,
        &dt,
        type_mod.as_ref(),
        col_def.is_nullable,
        col_def.default_value.as_ref(),
        &[],
    );
    pm.insert_catalog_tuple(bm, CAT_COLUMN, col_bytes)?;

    for db in catalog.databases.values_mut() {
        for table in db.tables.values_mut() {
            if table.table_oid == table_oid {
                table.columns.push(Column {
                    column_oid: col_oid,
                    name: col_def.name.clone(),
                    column_position: col_pos,
                    data_type: dt.clone(),
                    type_modifier: type_mod.clone(),
                    is_nullable: col_def.is_nullable,
                    default_value: col_def.default_value.clone(),
                    constraints: Vec::new(),
                });
                break;
            }
        }
    }
    catalog.cache.invalidate_constraints(table_oid);
    Ok(col_oid)
}

// ─────────────────────────────────────────────────────────────
// 3.1.7 – Catalog query functions
// ─────────────────────────────────────────────────────────────

pub fn get_table_metadata(
    catalog: &Catalog,
    pm: &CatalogPageManager,
    bm: &mut BufferManager,
    db_name: &str,
    table_name: &str,
) -> Result<TableMetadata, CatalogError> {
    let db = catalog
        .databases
        .get(db_name)
        .ok_or_else(|| CatalogError::DatabaseNotFound(db_name.into()))?;
    let table = db
        .tables
        .get(table_name)
        .ok_or_else(|| CatalogError::TableNotFound(table_name.into()))?;
    let constraints =
        crate::catalog::constraints::get_constraints_for_table(catalog, pm, bm, table.table_oid)?;
    let indexes = get_indexes_for_table(pm, bm, table.table_oid)?;
    Ok(TableMetadata {
        table_oid: table.table_oid,
        table_name: table.table_name.clone(),
        db_oid: table.db_oid,
        columns: table.columns.clone(),
        constraints,
        indexes,
        statistics: table.statistics.clone(),
    })
}

pub fn show_tables(
    catalog: &Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    db_name: &str,
) {
    println!("---------------------------------------------------------------");
    println!("Tables in Database: {}", db_name);
    println!("---------------------------------------------------------------");
    println!(
        "{:<20} | {:<10} | {:<10} | {:<15}",
        "Table Name", "Rows", "Pages", "Created At"
    );
    println!("---------------------------------------------------------------");

    let db = match catalog.databases.get(db_name) {
        Some(d) => d,
        None => {
            println!("Database '{}' not found.\n", db_name);
            return;
        }
    };

    let records = pm.scan_catalog(bm, CAT_TABLE).unwrap_or_default();
    let mut found = false;
    for bytes in records {
        if let Ok((_toid, tname, db_oid, ttype_b, row_count, page_count, created_at)) =
            deserialize_table_tuple(&bytes)
        {
            if db_oid == db.db_oid && ttype_b == 0 {
                found = true;
                println!(
                    "{:<20} | {:<10} | {:<10} | {}",
                    tname, row_count, page_count, created_at
                );
            }
        }
    }
    if !found {
        println!("No tables found in '{}'.\n", db_name);
    }
    println!("---------------------------------------------------------------\n");
}
