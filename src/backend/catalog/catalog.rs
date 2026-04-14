//! Manages catalog metadata including databases, tables, and columns.
//! Handles persistence of catalog state and creation of physical
//! database and table structures on disk.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::path::Path;

use crate::catalog::types::*;

use crate::heap::init_table;
use crate::layout::*;

/// Initializes the catalog and required directory structure on disk.
/// Creates the catalog file if it does not already exist.
pub fn init_catalog() {
    let catalog_path = Path::new(CATALOG_FILE);

    // Create directory if not exist
    if let Some(parent) = catalog_path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent).expect("Failed to create catalog directory");
        }
    }

    // Ensure base database directory exists
    let base_dir = Path::new(DATABASE_DIR);
    if !base_dir.exists() {
        fs::create_dir_all(base_dir).expect("Failed to create base data directory");
    }

    // Create an empty catalog file if missing
    if !catalog_path.exists() {
        let empty_catalog = Catalog {
            databases: HashMap::new(),
        };
        let json = serde_json::to_string_pretty(&empty_catalog)
            .expect("Failed to serialize empty catalog");
        fs::write(catalog_path, json).expect("Failed to write catalog file");
        println!(
            "Catalog file not found. Created new catalog file at {}",
            catalog_path.display()
        );
    } else {
        println!("Catalog file already exists at {}", catalog_path.display());
    }
}

/// Loads the catalog from disk into memory.
/// Rebuilds catalog from metadata files in database/base,
/// then syncs it back to catalog.json to keep them in sync.
pub fn load_catalog() -> Catalog {
    // Rebuild from metadata files on disk (source of truth)
    let catalog = rebuild_catalog_from_disk();
    
    // Sync the rebuilt catalog back to catalog.json
    save_catalog(&catalog);
    
    catalog
}

/// Rebuilds the catalog by scanning database/base directory
/// and loading metadata.json files from each table directory
fn rebuild_catalog_from_disk() -> Catalog {
    let mut catalog = Catalog {
        databases: HashMap::new(),
    };

    let base_dir = Path::new(DATABASE_DIR);
    if !base_dir.exists() {
        return catalog;
    }

    // Iterate through database directories
    match fs::read_dir(base_dir) {
        Ok(entries) => {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_dir() {
                        if let Some(db_name) = path.file_name() {
                            let db_name = db_name.to_string_lossy().to_string();

                            // Initialize database entry
                            let mut database = Database {
                                tables: HashMap::new(),
                            };

                            // Scan for table metadata files and .dat files
                            if let Ok(table_entries) = fs::read_dir(&path) {
                                for table_entry in table_entries {
                                    if let Ok(table_entry) = table_entry {
                                        let table_path = table_entry.path();
                                        if table_path.is_file() {
                                            if let Some(filename) = table_path.file_name() {
                                                let filename = filename.to_string_lossy();
                                                
                                                // Look for metadata files
                                                if filename.ends_with(".metadata.json") {
                                                    // Extract table name from metadata filename
                                                    let table_name = filename
                                                        .trim_end_matches(".metadata.json")
                                                        .to_string();

                                                    // Load metadata
                                                    if let Ok(metadata_content) = fs::read_to_string(&table_path) {
                                                        if let Ok(table) = serde_json::from_str::<Table>(&metadata_content) {
                                                            database.tables.insert(table_name, table);
                                                        }
                                                    }
                                                } 
                                                // Also look for .dat files without metadata (backward compatibility)
                                                else if filename.ends_with(".dat") {
                                                    let table_name = filename
                                                        .trim_end_matches(".dat")
                                                        .to_string();
                                                    
                                                    // Check if metadata already exists
                                                    let metadata_path = format!(
                                                        "{}/{}/{}.metadata.json",
                                                        DATABASE_DIR, db_name, table_name
                                                    );
                                                    
                                                    if !Path::new(&metadata_path).exists() {
                                                        // Create a default metadata entry for backward compatibility
                                                        let default_table = Table {
                                                            columns: vec![],
                                                            schema_version: Some(2),
                                                        };
                                                        
                                                        if let Ok(metadata_json) = serde_json::to_string_pretty(&default_table) {
                                                            let _ = fs::write(&metadata_path, metadata_json);
                                                        }
                                                        
                                                        database.tables.insert(table_name, default_table);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            if !database.tables.is_empty() {
                                catalog.databases.insert(db_name, database);
                            }
                        }
                    }
                }
            }
        }
        Err(e) => eprintln!("Failed to read database directory: {}", e),
    }

    catalog
}

// Persists the in-memory catalog state to disk.
pub fn save_catalog(catalog: &Catalog) {
    let catalog_path = Path::new(CATALOG_FILE);

    // Convert catalog to formatted JSON
    let json = serde_json::to_string_pretty(catalog).expect("Failed to serialize catalog to JSON");

    // Write catalog to disk
    fs::write(catalog_path, json).expect("Failed to write catalog file to disk");

    println!(
        "Catalog File updated with In Memory Data {}",
        catalog_path.display()
    );
}

// Prints all databases present in the catalog.
pub fn show_databases(catalog: &Catalog) {
    println!("--------------------------");
    println!("Databases in Catalog");
    println!("--------------------------");

    if catalog.databases.is_empty() {
        println!("No databases found.\n");
        return;
    }

    for db_name in catalog.databases.keys() {
        println!("- {}", db_name);
    }

    println!();
}

// Creates a new database entry in the catalog and its directory on disk.
pub fn create_database(catalog: &mut Catalog, db_name: &str) -> bool {
    // Validate database name
    if db_name.is_empty() {
        println!("Database name cannot be empty");
        return false;
    }

    if catalog.databases.contains_key(db_name) {
        println!("Database '{}' already exists", db_name);
        return false;
    }

    // Insert database into in-memory catalog
    catalog.databases.insert(
        db_name.to_string(),
        Database {
            tables: HashMap::new(),
        },
    );

    // Persist updated catalog
    let json = match serde_json::to_string_pretty(&catalog) {
        Ok(j) => j,
        Err(e) => {
            println!("Failed to serialize catalog: {}", e);
            return false;
        }
    };

    if let Err(e) = fs::write(CATALOG_FILE, json) {
        println!("Failed to write catalog file: {}", e);
        return false;
    }

    // Create database directory on disk
    let db_path_str = TABLE_DIR_TEMPLATE.replace("{database}", db_name);
    let db_path = Path::new(&db_path_str);

    if !db_path.exists() {
        if let Err(e) = fs::create_dir_all(db_path) {
            println!("Failed to create database directory: {}", e);
            return false;
        }
        // println!("Created new database directory at {}", db_path.display());
    } else {
        println!("Database directory already exists at {}", db_path.display());
    }

    // println!("Database '{}' created successfully", db_name);
    true
}

// Creates a new table, updates the catalog, and initializes its data file.
pub fn create_table(catalog: &mut Catalog, db_name: &str, table_name: &str, columns: Vec<Column>) {
    // Step 1: Validate database existence
    if !catalog.databases.contains_key(db_name) {
        println!(
            "Database '{}' does not exist. Cannot create table '{}'.",
            db_name, table_name
        );
        return;
    }

    let database = catalog.databases.get_mut(db_name).unwrap();

    // Prevent overwriting existing table
    if database.tables.contains_key(table_name) {
        println!(
            "Table '{}' already exists in database '{}'. Skipping creation.",
            table_name, db_name
        );
        return;
    }

    // Insert table metadata into catalog
    let new_table = Table {
        columns,
        schema_version: Some(2),
    };
    database.tables.insert(table_name.to_string(), new_table.clone());

    // Persist catalog changes
    save_catalog(catalog);

    // Save table metadata to a separate metadata.json file
    let metadata_path = format!(
        "{}/{}/{}.metadata.json",
        DATABASE_DIR, db_name, table_name
    );
    if let Ok(metadata_json) = serde_json::to_string_pretty(&new_table) {
        if let Err(e) = fs::write(&metadata_path, metadata_json) {
            eprintln!("Warning: Failed to write table metadata to {}: {}", metadata_path, e);
        }
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
                println!("Table data file created at '{}'.", table_file_path);

                if let Err(e) = init_table(&mut file) {
                    eprintln!("Failed to initialize table '{}': {}", table_name, e);
                } else {
                    println!("Table '{}' initialized successfully.", table_name);
                }
            }
            Err(e) => {
                eprintln!(
                    "Failed to create table data file '{}': {}",
                    table_file_path, e
                );
                return;
            }
        }
    } else {
        println!("Table data file '{}' already exists.", table_file_path);
    }

    println!(
        "Table '{}' created successfully in database '{}' and saved to catalog.",
        table_name, db_name
    );
}

/// Lists all tables in the specified database.
pub fn show_tables(catalog: &Catalog, db_name: &str) {
    println!("--------------------------");
    println!("Tables in Database: {}", db_name);
    println!("--------------------------");

    if let Some(database) = catalog.databases.get(db_name) {
        if database.tables.is_empty() {
            println!("No tables found in '{}'.\n", db_name);
            return;
        }

        for table_name in database.tables.keys() {
            println!("- {}", table_name);
        }

        println!();
    } else {
        println!("Database '{}' not found.\n", db_name);
    }
}

/// Clears the catalog file, removing all database metadata
pub fn clear_catalog() {
    let catalog_path = Path::new(CATALOG_FILE);
    
    let empty_catalog = Catalog {
        databases: HashMap::new(),
    };
    
    let json = serde_json::to_string_pretty(&empty_catalog)
        .expect("Failed to serialize empty catalog");
    
    fs::write(catalog_path, json)
        .expect("Failed to clear catalog file");
    
    println!("Catalog cleared successfully.");
}
