//! Catalog manager – high-level operations on databases and tables.
//!
//! Catalog system: page-based storage under database/global/catalog_pages/
//!
//! On a fresh install init_catalog() calls bootstrap_catalog() which sets
//! up the page-based backend and pre-populates built-in types.

use std::fs::{self, OpenOptions};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::buffer_manager::BufferManager;
use crate::catalog::constraints::{add_not_null_constraint, add_primary_key_constraint};
use crate::catalog::indexes::{drop_index, get_indexes_for_table};
use crate::catalog::oid::OidCounter;
use crate::catalog::page_manager::{
    CAT_COLUMN, CAT_CONSTRAINT, CAT_DATABASE, CAT_TABLE, CAT_TYPE, CatalogPageManager,
};
use crate::catalog::serialize::{
    deserialize_column_tuple, deserialize_constraint_tuple, deserialize_database_tuple,
    deserialize_table_tuple, deserialize_type_tuple,
    serialize_column_tuple, serialize_database_tuple, serialize_table_tuple, serialize_type_tuple,
};
use crate::catalog::types::{
    Catalog, CatalogError, Column, ColumnDefinition, ConstraintDefinition, DataType, Database,
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
    for dt in DataType::all_builtins() {
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
    // NOTE: The new architecture uses LAZY-LOADING of catalog entries on-demand via
    // get_database(), get_table(), get_columns(), etc. rather than eager-loading all
    // metadata into memory. This provides better scalability for large catalogs.
    //
    // The CatalogCache layer (cache.rs) handles in-memory caching with LRU eviction.
    // The page_backend_active flag is set to true to indicate all data is page-backed.
    //
    // For backward compatibility, if you need to warm the cache or pre-load specific
    // catalogs, use get_database(), get_table() etc. explicitly after calling this function.

    let mut oid_ctr = OidCounter::new();
    let _ = oid_ctr.load();

    let mut catalog = Catalog::new();
    catalog.oid_counter = oid_ctr.next_oid;
    catalog.page_backend_active = true;

    // The BufferManager is kept to potentially support future cache warming strategies
    // or catalog consistency checks on load
    let _ = bm; // Suppress unused warning

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
    for dt in DataType::all_builtins() {
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
) -> Result<DataType, CatalogError> {
    if let Some(dt) = DataType::from_name(type_name) {
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

pub fn get_database(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    db_name: &str,
) -> Result<Database, CatalogError> {
    if let Some(db) = catalog.cache.get_database(db_name) {
        return Ok(db.clone());
    }

    for bytes in pm.scan_catalog(bm, CAT_DATABASE)? {
        if let Ok((oid, name, owner, created_at, enc)) = deserialize_database_tuple(&bytes) {
            if name == db_name {
                let db = Database {
                    db_oid: oid,
                    db_name: name.clone(),
                    owner,
                    encoding: Encoding::from_u8(enc),
                    created_at,
                };
                catalog.cache.insert_database(name, db.clone());
                return Ok(db);
            }
        }
    }
    Err(CatalogError::DatabaseNotFound(db_name.to_string()))
}

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
    if get_database(catalog, pm, bm, db_name).is_ok() {
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

    let db = Database {
        db_oid,
        db_name: db_name.to_string(),
        owner: owner.to_string(),
        encoding,
        created_at,
    };
    catalog.cache.insert_database(db_name.to_string(), db);
    Ok(db_oid)
}

/// Drop a database and all its tables.
pub fn drop_database(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    db_name: &str,
) -> Result<(), CatalogError> {
    let db = get_database(catalog, pm, bm, db_name)?;
    let db_oid = db.db_oid;

    let mut table_oids = Vec::new();
    for bytes in pm.scan_catalog(bm, CAT_TABLE)? {
        if let Ok((toid, _tname, tdb_oid, ..)) = deserialize_table_tuple(&bytes) {
            if tdb_oid == db_oid {
                table_oids.push(toid);
            }
        }
    }

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
    catalog.cache.invalidate_database(db_name);
    Ok(())
}

// ─────────────────────────────────────────────────────────────
// 3.1.6 – Table operations
// ─────────────────────────────────────────────────────────────

pub fn get_table(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    db_oid: u32,
    table_name: &str,
) -> Result<Table, CatalogError> {
    if let Some(table) = catalog.cache.get_table(db_oid, table_name) {
        return Ok(table.clone());
    }

    // fallback: iterate pages
    for bytes in pm.scan_catalog(bm, CAT_TABLE)? {
        if let Ok((toid, tname, tdb_oid, ttype_b, row_count, page_count, created_at)) =
            deserialize_table_tuple(&bytes)
        {
            if tdb_oid == db_oid && tname == table_name {
                let table = Table {
                    table_oid: toid,
                    table_name: tname.clone(),
                    db_oid: tdb_oid,
                    table_type: if ttype_b == 1 {
                        TableType::SystemCatalog
                    } else {
                        TableType::UserTable
                    },
                    statistics: TableStatistics {
                        row_count,
                        page_count,
                        created_at,
                        last_modified: 0,
                    },
                };
                catalog.cache.insert_table(tdb_oid, tname, table.clone());
                return Ok(table);
            }
        }
    }
    Err(CatalogError::TableNotFound(table_name.to_string()))
}

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
    let db = get_database(catalog, pm, bm, db_name)?;
    if get_table(catalog, pm, bm, db.db_oid, table_name).is_ok() {
        return Err(CatalogError::TableAlreadyExists(table_name.to_string()));
    }
    let db_oid = db.db_oid;
    let table_oid = catalog.alloc_oid();
    let created_at = now_unix();

    // Build column metadata
    for (pos, def) in col_defs.iter().enumerate() {
        let dt = DataType::from_name(&def.type_name)
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
    }

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
        table_type: TableType::UserTable,
        statistics: TableStatistics {
            row_count: 0,
            page_count: 1,
            created_at,
            last_modified: 0,
        },
    };
    catalog.cache.insert_table(db_oid, table_name.to_string(), table);

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
                let mut col_oid_opt = None;
                for bytes in pm.scan_catalog(bm, CAT_COLUMN)? {
                    if let Ok((coid, toid, cname, ..)) = deserialize_column_tuple(&bytes) {
                        if toid == table_oid && cname == column {
                            col_oid_opt = Some(coid);
                            break;
                        }
                    }
                }
                let oid = col_oid_opt
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
                let ref_db = get_database(catalog, pm, bm, db_name)?;
                let ref_table = get_table(catalog, pm, bm, ref_db.db_oid, &referenced_table)?;
                let ref_oid = ref_table.table_oid;
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

    let mut found = None;
    for bytes in pm.scan_catalog(bm, CAT_TABLE)? {
        if let Ok((toid, tname, tdb_oid, ..)) = deserialize_table_tuple(&bytes) {
            if toid == table_oid {
                found = Some((tdb_oid, tname));
                break;
            }
        }
    }
    
    let (db_oid, table_name) = found.ok_or_else(|| CatalogError::TableNotFound(table_oid.to_string()))?;
    
    // Efficiently lookup database name only once
    let db_name = pm.scan_catalog(bm, CAT_DATABASE)?
        .iter()
        .find_map(|bytes| {
            deserialize_database_tuple(bytes)
                .ok()
                .and_then(|(doid, dname, ..)| if doid == db_oid { Some(dname) } else { None })
        })
        .ok_or_else(|| CatalogError::DatabaseNotFound(format!("db_oid: {}", db_oid)))?;

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

    catalog.cache.invalidate_table(db_oid, &table_name);
    catalog.cache.invalidate_constraints(table_oid);
    catalog.cache.invalidate_indexes(table_oid);
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
    let dt = DataType::from_name(&col_def.type_name)
        .ok_or_else(|| CatalogError::TypeNotFound(col_def.type_name.clone()))?;

    if !col_def.is_nullable && col_def.default_value.is_none() {
        return Err(CatalogError::InvalidOperation(
            "Cannot add NOT NULL column without default to existing table".into(),
        ));
    }

    // Fetch existing columns to check for duplicates and get next position
    let existing_cols = get_columns(pm, bm, table_oid)?;
    if existing_cols.iter().any(|c| c.name == col_def.name) {
        return Err(CatalogError::InvalidOperation(format!(
            "Column '{}' already exists",
            col_def.name
        )));
    }
    let col_pos = (existing_cols.len() + 1) as u16;

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

    catalog.cache.invalidate_constraints(table_oid);
    Ok(col_oid)
}

// ─────────────────────────────────────────────────────────────
// 3.1.7 – Catalog query functions
// ─────────────────────────────────────────────────────────────

pub fn get_columns(
    pm: &CatalogPageManager,
    bm: &mut BufferManager,
    table_oid: u32,
) -> Result<Vec<Column>, CatalogError> {
    let mut columns = Vec::new();
    for bytes in pm.scan_catalog(bm, CAT_COLUMN)? {
        if let Ok((coid, toid, cname, cpos, dt, tm, is_nullable, default_val, constraint_oids)) =
            deserialize_column_tuple(&bytes)
        {
            if toid == table_oid {
                columns.push(Column {
                    column_oid: coid,
                    name: cname,
                    column_position: cpos,
                    data_type: dt,
                    type_modifier: tm,
                    is_nullable,
                    default_value: default_val,
                    constraints: constraint_oids,
                });
            }
        }
    }
    columns.sort_by_key(|c| c.column_position);
    Ok(columns)
}

pub fn get_table_metadata(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    db_name: &str,
    table_name: &str,
) -> Result<TableMetadata, CatalogError> {
    let db = get_database(catalog, pm, bm, db_name)?;
    let table = get_table(catalog, pm, bm, db.db_oid, table_name)?;
    
    let columns = get_columns(pm, bm, table.table_oid)?;
    let constraints =
        crate::catalog::constraints::get_constraints_for_table(catalog, pm, bm, table.table_oid)?;
    let indexes = get_indexes_for_table(pm, bm, table.table_oid)?;
    
    Ok(TableMetadata {
        table_oid: table.table_oid,
        table_name: table.table_name.clone(),
        db_oid: table.db_oid,
        columns,
        constraints,
        indexes,
        statistics: table.statistics.clone(),
    })
}

pub fn show_tables(
    catalog: &mut Catalog,
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

    let db = match get_database(catalog, pm, bm, db_name) {
        Ok(d) => d,
        Err(_) => {
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

pub fn update_table_statistics(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    table_oid: u32,
    row_count: u64,
    page_count: u32,
) -> Result<(), CatalogError> {
    if let Some((page_num, slot, tuple_bytes)) = pm.find_catalog_tuple(bm, crate::catalog::page_manager::CAT_TABLE, |b| {
        deserialize_table_tuple(b)
            .map(|(oid, ..)| oid == table_oid)
            .unwrap_or(false)
    })? {
        let (_toid, tname, tdb_oid, ttype_b, _old_rows, _old_pages, created_at) =
            deserialize_table_tuple(&tuple_bytes).map_err(CatalogError::IoError)?;
            
        let new_bytes = serialize_table_tuple(
            table_oid,
            &tname,
            tdb_oid,
            ttype_b,
            row_count,
            page_count,
            created_at,
        );
        pm.update_catalog_tuple(bm, crate::catalog::page_manager::CAT_TABLE, page_num, slot, &new_bytes)?;
        
        // Invalidate table from cache so it will be reloaded with fresh stats
        catalog.cache.invalidate_table(tdb_oid, &tname);
        Ok(())
    } else {
        Err(CatalogError::TableNotFound(table_oid.to_string()))
    }
}
