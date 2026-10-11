use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::catalog::types::*;
use crate::heap::HeapManager;
use crate::layout::*;

pub fn init_catalog() {
    let catalog_path = Path::new(CATALOG_FILE);

    debug_print_catalog(&format!(
        "Initializing catalog at: {}",
        catalog_path.display()
    ));

    // Create directory if not exist
    if let Some(parent) = catalog_path.parent()
        && !parent.exists()
    {
        match fs::create_dir_all(parent) {
            Ok(_) => {
                debug_print_catalog(&format!(" Created catalog directory: {}", parent.display()));
            }
            Err(e) => {
                log::error!("Failed to create catalog directory: {}", e);
                log::error!("Please check directory permissions and disk space.");
                return;
            }
        }
    }

    // Ensure base database directory exists
    let base_dir = Path::new(DATABASE_DIR);
    if !base_dir.exists() {
        match fs::create_dir_all(base_dir) {
            Ok(_) => {
                debug_print_catalog(&format!(
                    "Created base database directory: {}",
                    base_dir.display()
                ));
            }
            Err(e) => {
                log::error!("Failed to create base data directory: {}", e);
                log::error!("Please check directory permissions and disk space.");
            }
        }
    }

    // Create system directory if not exist
    let system_dir = Path::new(SYSTEM_DIR);
    if !system_dir.exists()
        && let Err(e) = fs::create_dir_all(system_dir)
    {
        log::error!("Failed to create system directory: {}", e);
    }

    // Bootstrap: migrate catalog.json → system tables if needed
    let migrated = crate::backend::system_table::bootstrap_system_catalog();
    if migrated {
        debug_print_catalog("Catalog migrated from JSON to system tables.");
    }

    // Ensure all system table files exist (e.g. sys_views may be missing if
    // this is an existing system catalog that predates the views feature).
    crate::backend::system_table::ensure_system_tables();

    // NOTE: catalog.json is no longer created or maintained.
    // The system table heap files under database/system/ are the single
    // source of truth for catalog metadata. The bootstrap migration in
    // bootstrap_system_catalog() handles migrating existing catalog.json
    // into the system tables.
}

/// Loads the catalog from disk into memory.
/// Returns an empty catalog if the file is missing or invalid.
pub fn load_catalog() -> Catalog {
    // Always load from system tables (the single source of truth).
    if crate::backend::system_table::system_tables_exist() {
        debug_print_catalog("Loading catalog from system tables.");
        return crate::backend::system_table::load_catalog_from_system();
    }

    // Fallback: if system tables do not exist, attempt JSON load for backward
    // compatibility during migration. This path is exercised during the first
    // call before bootstrap_system_catalog() has created the heap files.
    let catalog_path = Path::new(CATALOG_FILE);
    if !catalog_path.exists() {
        debug_print_catalog("No catalog.json or system tables found. Returning empty catalog.");
        return Catalog::new(crate::backend::engine::Engine::current_dir());
    }

    match fs::read_to_string(catalog_path) {
        Ok(data) => serde_json::from_str::<Catalog>(&data).unwrap_or_else(|e| {
            log::error!("Failed to parse catalog JSON: {}", e);
            Catalog::new(crate::backend::engine::Engine::current_dir())
        }),
        Err(e) => {
            log::error!("Failed to read catalog.json: {}", e);
            Catalog::new(crate::backend::engine::Engine::current_dir())
        }
    }
}

/// Persists the in-memory catalog state to disk.
/// Returns Ok(()) on success, or an error with a detailed message if something goes wrong.
pub fn save_catalog(catalog: &Catalog) -> std::io::Result<()> {
    // Always save to system tables (the single source of truth).
    if crate::backend::system_table::system_tables_exist() {
        debug_print_catalog("Saving catalog to system tables.");
        return crate::backend::system_table::save_catalog_to_system(catalog);
    }

    // Fallback: save as JSON if system tables don't exist yet
    // (during initial bootstrap before migration completes).
    let catalog_path = Path::new(CATALOG_FILE);
    debug_print_catalog(&format!("Saving catalog to: {}", catalog_path.display()));
    let json = serde_json::to_string_pretty(catalog)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    fs::write(catalog_path, json)
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
        debug_print_catalog(&e);
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

    // Persist updated catalog (routes to system tables if migrated)
    if let Err(e) = save_catalog(catalog) {
        log::info!("Failed to persist catalog: {}", e);
        debug_print_catalog(&format!("Persist error: {}", e));
        return false;
    }

    debug_print_catalog(&format!("Persisted database to catalog: '{}'", db_name));

    // Create database directory on disk
    let db_path_str = crate::backend::paths::database_dir(db_name);
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
    let table_file_path = crate::backend::paths::heap(db_name, table_name);

    // Create and initialize table file
    let table_path = Path::new(&table_file_path);

    if !table_path.exists() {
        match HeapManager::create(table_path.to_path_buf()) {
            Ok(_) => {
                log::info!("Table data file created at '{}'.", table_file_path);
            }

            Err(e) => {
                log::error!(
                    "Failed to create table data file '{}': {}",
                    table_file_path,
                    e
                );

                println!(
                    "Failed to create table data file '{}': {}",
                    table_file_path, e
                );

                return;
            }
        }
    } else {
        log::info!("Table data file '{}' already exists.", table_file_path);
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
