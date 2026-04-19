use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::path::Path;

use crate::catalog::types::*;

#[allow(deprecated)]
use crate::heap::init_table;
use crate::layout::*;


pub fn init_catalog() {
    let catalog_path = Path::new(CATALOG_FILE);

    debug_print_catalog(&format!("Initializing catalog at: {}", catalog_path.display()));

    // Create directory if not exist
    if let Some(parent) = catalog_path.parent() {
        if !parent.exists() {
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
    }

    // Ensure base database directory exists
    let base_dir = Path::new(DATABASE_DIR);
    if !base_dir.exists() {
        match fs::create_dir_all(base_dir) {
            Ok(_) => {
                debug_print_catalog(&format!("Created base database directory: {}", base_dir.display()));
            }
            Err(e) => {
                log::error!("Failed to create base data directory: {}", e);
                log::error!("Please check directory permissions and disk space.");
                return;
            }
        }
    }

    // Create an empty catalog file if missing
    if !catalog_path.exists() {
        let empty_catalog = Catalog {
            databases: HashMap::new(),
        };
        let json = match serde_json::to_string_pretty(&empty_catalog) {
            Ok(j) => j,
            Err(e) => {
                log::error!("Failed to serialize empty catalog: {}", e);
                return;
            }
        };

        match fs::write(catalog_path, json) {
            Ok(_) => {
                debug_print_catalog(&format!("Created new catalog file: {}", catalog_path.display()));
                log::info!(
                    " Catalog file created at {}",
                    catalog_path.display()
                );
            }
            Err(e) => {
                log::error!("Failed to write catalog file: {}", e);
                log::error!("Please check disk space and file permissions.");
            }
        }
    } else {
        debug_print_catalog(&format!("Catalog file already exists: {}", catalog_path.display()));
        log::info!("Catalog file already exists at {}", catalog_path.display());
    }
}

/// Loads the catalog from disk into memory.
/// Returns an empty catalog if the file is missing or invalid.
pub fn load_catalog() -> Catalog {
    let catalog_path = Path::new(CATALOG_FILE);

    debug_print_catalog(&format!("Loading catalog from: {}", catalog_path.display()));

    // Check if catalog file exists
    if !catalog_path.exists() {
        debug_print_catalog("Catalog file does not exist. Returning empty catalog.");
        log::error!("Catalog file does not exist at {}.", catalog_path.display());
        return Catalog {
            databases: HashMap::new(),
        };
    }

    // Read the catalog file
    let data = match fs::read_to_string(catalog_path) {
        Ok(content) => {
            debug_print_catalog(&format!("Read catalog file ({} bytes)", content.len()));
            content
        }
        Err(err) => {
            debug_print_catalog(&format!("Error reading catalog file: {}", err));
            log::error!("Failed to read catalog file: {}", err);
            log::error!("Please check file permissions and disk space.");
            return Catalog {
                databases: HashMap::new(),
            };
        }
    };

    // Deserialize JSON into Catalog struct
    match serde_json::from_str::<Catalog>(&data) {
        Ok(catalog) => {
            debug_print_catalog(&format!(
                "Parsed catalog successfully ({} databases)",
                catalog.databases.len()
            ));
            catalog
        }
        Err(err) => {
            debug_print_catalog(&format!("✗ Error parsing catalog JSON: {}", err));
            log::error!("Failed to parse catalog JSON: {}", err);
            log::error!("The catalog file may be corrupted. Please back it up and delete it to create a new one.");
            Catalog {
                databases: HashMap::new(),
            }
        }
    }
}

/// Persists the in-memory catalog state to disk.
/// Returns Ok(()) on success, or an error with a detailed message if something goes wrong.
pub fn save_catalog(catalog: &Catalog) -> std::io::Result<()> {
    let catalog_path = Path::new(CATALOG_FILE);

    debug_print_catalog(&format!("Saving catalog to: {}", catalog_path.display()));

    // Convert catalog to formatted JSON
    let json = match serde_json::to_string_pretty(catalog) {
        Ok(j) => {
            debug_print_catalog(&format!("Serialized catalog ({} bytes)", j.len()));
            j
        }
        Err(e) => {
            let msg = format!("Failed to serialize catalog to JSON: {}", e);
            debug_print_catalog(&format!("{}", msg));
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
        }
    };

    // Write catalog to disk
    match fs::write(catalog_path, json) {
        Ok(_) => {
            debug_print_catalog(&format!("Catalog saved successfully to: {}", catalog_path.display()));
            log::info!(
                "Catalog File updated with In Memory Data at {}",
                catalog_path.display()
            );
            Ok(())
        }
        Err(e) => {
            let msg = format!("Failed to write catalog file to disk: {}", e);
            debug_print_catalog(&format!("{}", msg));
            Err(e)
        }
    }
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
        return;
    }

    for db_name in catalog.databases.keys() {
        log::info!("- {}", db_name);
    }

    log::info!("");
}

// Creates a new database entry in the catalog and its directory on disk.
pub fn create_database(catalog: &mut Catalog, db_name: &str) -> bool {
    debug_print_catalog(&format!("Creating database: '{}'", db_name));

    // Validate database name
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
        },
    );

    debug_print_catalog(&format!("Added database to in-memory catalog: '{}'", db_name));

    // Persist updated catalog
    let json = match serde_json::to_string_pretty(&catalog) {
        Ok(j) => j,
        Err(e) => {
            log::info!("Failed to serialize catalog: {}", e);
            debug_print_catalog(&format!("Serialization error: {}", e));
            return false;
        }
    };

    if let Err(e) = fs::write(CATALOG_FILE, json) {
        log::info!("Failed to write catalog file: {}", e);
        debug_print_catalog(&format!("Write error: {}", e));
        return false;
    }

    debug_print_catalog(&format!("Persisted database to catalog: '{}'", db_name));

    // Create database directory on disk
    let db_path_str = TABLE_DIR_TEMPLATE.replace("{database}", db_name);
    let db_path = Path::new(&db_path_str);

    if !db_path.exists() {
        if let Err(e) = fs::create_dir_all(db_path) {
            log::info!("Failed to create database directory: {}", e);
            debug_print_catalog(&format!("Failed to create directory: {}", e));
            return false;
        }
        debug_print_catalog(&format!("Created database directory: {}", db_path.display()));
    } else {
        log::info!(" Database directory already exists at {}", db_path.display());
        debug_print_catalog(&format!("Directory already exists: {}", db_path.display()));
    }

    log::info!("Database '{}' created successfully", db_name);
    debug_print_catalog(&format!("Database '{}' created successfully", db_name));
    true
}

// Creates a new table, updates the catalog, and initializes its data file.
#[allow(deprecated)]
pub fn create_table(catalog: &mut Catalog, db_name: &str, table_name: &str, columns: Vec<Column>) {
    // Step 1: Validate database existence
    if !catalog.databases.contains_key(db_name) {
        log::info!(
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
            table_name, db_name
        );
        return;
    }

    // Insert table metadata into catalog
    let new_table = Table { columns };
    database.tables.insert(table_name.to_string(), new_table);

    // Persist catalog changes
    if let Err(e) = save_catalog(catalog) {
        log::warn!("Warning: Failed to save catalog immediately: {}. Table metadata may not be persisted.", e);
        log::warn!("Continuing with table creation. Please save manually if needed.");
    }

    // Construct table file path
    let table_file_path = TABLE_FILE_TEMPLATE
        .replace("{database}", db_name)
        .replace("{table}", table_name);

    // Create and initialize table file
    let table_path = Path::new(&table_file_path);
    if !table_path.exists() {
        match OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&table_file_path)
        {
            Ok(mut file) => {
                log::info!("Table data file created at '{}'.", table_file_path);

                if let Err(e) = init_table(&mut file) {
                    log::error!("Failed to initialize table '{}': {}", table_name, e);
                } else {
                    log::info!("Table '{}' initialized successfully.", table_name);
                }
            }
            Err(e) => {
                log::error!(
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
        table_name, db_name
    );
}

/// Lists all tables in the specified database.
pub fn show_tables(catalog: &Catalog, db_name: &str) {
    log::debug!("--------------------------");
    log::info!("Tables in Database: {}", db_name);
    log::debug!("--------------------------");

    if let Some(database) = catalog.databases.get(db_name) {
        if database.tables.is_empty() {
            log::info!("No tables found in '{}'.\n", db_name);
            return;
        }

        for table_name in database.tables.keys() {
            log::info!("- {}", table_name);
        }

        log::info!("");
    } else {
        log::info!("Database '{}' not found.\n", db_name);
    }
}
