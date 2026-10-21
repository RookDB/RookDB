use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::catalog::types::*;
use crate::heap::HeapManager;

/// Create the directories and system tables of the working directory's
/// data directory if they are missing.
pub fn init_catalog() -> crate::backend::error::RookResult<()> {
    init_catalog_in(&crate::backend::engine::Engine::current_dir())
}

/// [`init_catalog`] for the data directory of `engine`. A data directory from
/// before the system catalog (a `catalog.json` and no system tables) is an
/// error: it is no longer migrated.
pub fn init_catalog_in(
    engine: &std::sync::Arc<crate::backend::engine::Engine>,
) -> crate::backend::error::RookResult<()> {
    let old_catalog = engine
        .paths()
        .data_dir()
        .join("global")
        .join("catalog.json");
    if old_catalog.exists() && !engine.paths().system_dir().exists() {
        return Err(crate::backend::error::RookError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "{} is from an older RookDB version that kept its catalog in catalog.json; \
                 that format is no longer supported. Use a fresh data directory.",
                old_catalog.display()
            ),
        )));
    }

    engine.lock_data_dir()?;

    let base_dir = engine.paths().base_dir();
    fs::create_dir_all(&base_dir).map_err(|e| {
        crate::backend::error::RookError::Io(e).with_context(format!(
            "Failed to create the data directory {}",
            base_dir.display()
        ))
    })?;

    // Create any system table file that is missing (e.g. sys_views in a
    // catalog that predates views).
    crate::backend::system_table::ensure_system_tables(engine)
}

/// Loads the catalog from disk into memory. A data directory without system
/// tables loads as an empty catalog.
///
/// # Panics
/// If the system tables cannot be read; [`try_load_catalog_in`] returns the
/// error instead.
pub fn load_catalog() -> Catalog {
    load_catalog_in(&crate::backend::engine::Engine::current_dir())
}

/// [`load_catalog`] for the data directory of `engine`.
pub fn load_catalog_in(engine: &std::sync::Arc<crate::backend::engine::Engine>) -> Catalog {
    try_load_catalog_in(engine).unwrap_or_else(|e| panic!("failed to load the catalog: {}", e))
}

/// [`load_catalog_in`] that returns an unreadable catalog as an error.
///
/// Takes the data directory lock ([`Engine::lock_data_dir`]), which the
/// catalog's engine then holds for as long as the catalog lives.
///
/// [`Engine::lock_data_dir`]: crate::backend::engine::Engine::lock_data_dir
pub fn try_load_catalog_in(
    engine: &std::sync::Arc<crate::backend::engine::Engine>,
) -> crate::backend::error::RookResult<Catalog> {
    engine.lock_data_dir()?;
    debug_print_catalog("Loading catalog from system tables.");
    crate::backend::system_table::load_catalog_from_system(engine)
}

/// Persists the in-memory catalog state to disk.
/// Returns Ok(()) on success, or an error with a detailed message if something goes wrong.
pub fn save_catalog(catalog: &Catalog) -> crate::backend::error::RookResult<()> {
    debug_print_catalog("Saving catalog to system tables.");
    crate::backend::system_table::save_catalog_to_system(catalog)
}

/// Print debug information for catalog operations
fn debug_print_catalog(msg: &str) {
    if cfg!(debug_assertions) {
        log::debug!("[CATALOG] {}", msg);
    }
}

// Prints all databases present in the catalog.
pub fn show_databases(catalog: &Catalog) {
    log::debug!("--------------------------");
    log::info!("Databases in Catalog");
    log::debug!("--------------------------");

    if catalog.databases.is_empty() {
        log::info!("No databases found.\n");
        println!("No databases found.");
        return;
    }

    for db_name in catalog.databases.keys() {
        log::info!("- {}", db_name);
        println!("- {}", db_name);
    }

    log::info!("");
}

// Creates a new database entry in the catalog and its directory on disk.
pub fn create_database(catalog: &mut Catalog, db_name: &str) -> bool {
    debug_print_catalog(&format!("Creating database: '{}'", db_name));

    // Validate database name
    // Validate database name (path-safety: reject separators, '..', NUL, …)
    if let Err(e) = crate::backend::name_validation::validate_database_name(db_name) {
        log::info!("{}", e);
        debug_print_catalog(&e.to_string());
        return false;
    }

    if db_name.is_empty() {
        log::info!("Database name cannot be empty");
        debug_print_catalog("Database name is empty");
        return false;
    }

    if catalog.databases.contains_key(db_name) {
        log::info!("Database '{}' already exists", db_name);
        debug_print_catalog(&format!("Database '{}' already exists", db_name));
        return false;
    }

    // Insert database into in-memory catalog
    catalog.databases.insert(
        db_name.to_string(),
        Database {
            tables: HashMap::new(),
            views: HashMap::new(),
        },
    );

    debug_print_catalog(&format!(
        "Added database to in-memory catalog: '{}'",
        db_name
    ));

    // Persist updated catalog
    if let Err(e) = save_catalog(catalog) {
        log::info!("Failed to persist catalog: {}", e);
        debug_print_catalog(&format!("Persist error: {}", e));
        return false;
    }

    debug_print_catalog(&format!("Persisted database to catalog: '{}'", db_name));

    // Create database directory on disk
    let db_path_str = catalog.engine().paths().database_dir(db_name);
    let db_path = Path::new(&db_path_str);

    if !db_path.exists() {
        if let Err(e) = fs::create_dir_all(db_path) {
            log::info!("Failed to create database directory: {}", e);
            debug_print_catalog(&format!("Failed to create directory: {}", e));
            return false;
        }
        debug_print_catalog(&format!(
            "Created database directory: {}",
            db_path.display()
        ));
    } else {
        log::info!(
            " Database directory already exists at {}",
            db_path.display()
        );
        debug_print_catalog(&format!("Directory already exists: {}", db_path.display()));
    }

    log::info!("Database '{}' created successfully", db_name);
    debug_print_catalog(&format!("Database '{}' created successfully", db_name));
    true
}

// Creates a new table, updates the catalog, and initializes its data file.
#[allow(deprecated)]
pub fn create_table(catalog: &mut Catalog, db_name: &str, table_name: &str, columns: Vec<Column>) {
    // Step 0: Validate names (path-safety: reject separators, '..', NUL, …)
    if let Err(e) = crate::backend::name_validation::validate_table_name(table_name) {
        log::info!("{}", e);
        println!("{}", e);
        return;
    }
    if let Err(e) = crate::backend::name_validation::validate_database_name(db_name) {
        log::info!("{}", e);
        println!("{}", e);
        return;
    }

    // Step 1: Validate database existence
    if !catalog.databases.contains_key(db_name) {
        log::info!(
            "Database '{}' does not exist. Cannot create table '{}'.",
            db_name,
            table_name
        );

        println!(
            "Database '{}' does not exist. Cannot create table '{}'.",
            db_name, table_name
        );

        return;
    }

    let database = catalog.databases.get_mut(db_name).unwrap();

    // Prevent overwriting existing table
    if database.tables.contains_key(table_name) {
        log::info!(
            "Table '{}' already exists in database '{}'. Skipping creation.",
            table_name,
            db_name
        );

        println!(
            "Table '{}' already exists in database '{}'. Skipping creation.",
            table_name, db_name
        );

        return;
    }

    // Insert table metadata into catalog
    let new_table = Table { columns };
    database.tables.insert(table_name.to_string(), new_table);

    // Persist catalog changes
    if let Err(e) = save_catalog(catalog) {
        log::warn!(
            "Warning: Failed to save catalog immediately: {}. Table metadata may not be persisted.",
            e
        );

        println!(
            "Warning: Failed to save catalog immediately: {}. Table metadata may not be persisted.",
            e
        );

        log::warn!("Continuing with table creation. Please save manually if needed.");

        println!("Continuing with table creation. Please save manually if needed.");
    }

    // Construct table file path
    let table_file_path = catalog.engine().paths().heap(db_name, table_name);

    // Create and initialize table file
    let table_path = Path::new(&table_file_path);

    if !table_path.exists() {
        match HeapManager::create(table_path.to_path_buf()) {
            Ok(_) => {
                log::info!(
                    "Table data file created at '{}'.",
                    table_file_path.display()
                );
            }

            Err(e) => {
                log::error!(
                    "Failed to create table data file '{}': {}",
                    table_file_path.display(),
                    e
                );

                println!(
                    "Failed to create table data file '{}': {}",
                    table_file_path.display(),
                    e
                );

                return;
            }
        }
    } else {
        log::info!(
            "Table data file '{}' already exists.",
            table_file_path.display()
        );
    }

    log::info!(
        "Table '{}' created successfully in database '{}' and saved to catalog.",
        table_name,
        db_name
    );
}

/// Lists all tables in the specified database.
pub fn show_tables(catalog: &Catalog, db_name: &str) {
    log::debug!("--------------------------");
    log::info!("Tables in Database: {}", db_name);
    log::debug!("--------------------------");

    println!("--------------------------");
    println!("Tables in Database: {}", db_name);
    println!("--------------------------");

    if let Some(database) = catalog.databases.get(db_name) {
        if database.tables.is_empty() {
            log::info!("No tables found in '{}'.\n", db_name);
            println!("No tables found in '{}'.", db_name);
            return;
        }

        for table_name in database.tables.keys() {
            log::info!("- {}", table_name);
            println!("- {}", table_name);
        }

        log::info!("");
    } else {
        log::info!("Database '{}' not found.\n", db_name);
        println!("Database '{}' not found.", db_name);
    }
}
