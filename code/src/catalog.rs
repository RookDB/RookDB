use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::path::Path;

pub const DATA_DIR: &str = "database"; // Root directory for all storage
pub const CATALOG_DIR: &str = "database/global"; // Catalog metadata directory
pub const CATALOG_FILE: &str = "database/global/catalog.json"; // Global catalog file
pub const DATABASE_DIR: &str = "database/base"; // Root directory for all databases
pub const TABLE_DIR_TEMPLATE: &str = "database/base/{database}"; // Directory for specific database
pub const TABLE_FILE_TEMPLATE: &str = "database/base/{database}/{table}.dat"; // File path for specific table

use crate::table::init_table;

#[derive(Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: String,
}

#[derive(Serialize, Deserialize)]
pub struct Table {
    pub columns: Vec<Column>,
}

#[derive(Serialize, Deserialize)]
pub struct Database {
    pub tables: HashMap<String, Table>,
}

#[derive(Serialize, Deserialize)]
pub struct Catalog {
    pub databases: HashMap<String, Database>,
}

pub fn init_catalog() {
    let catalog_path = Path::new(CATALOG_FILE);

    // Step 1: Create necessary directories if not exist
    if let Some(parent) = catalog_path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent).expect("Failed to create catalog directory");
        }
    }

    let base_dir = Path::new(DATABASE_DIR);
    if !base_dir.exists() {
        fs::create_dir_all(base_dir).expect("Failed to create base data directory");
    }

    // Step 2: Create catalog.json if not exist
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

pub fn load_catalog() -> Catalog {
    let catalog_path = Path::new(CATALOG_FILE);

    // Step 1: Check if catalog file exists
    if !catalog_path.exists() {
        eprintln!("Catalog file does not exist at {}.", catalog_path.display());
        return Catalog {
            databases: HashMap::new(),
        };
    }

    // Step 2: Read the catalog file
    let data = fs::read_to_string(catalog_path);
    let data = match data {
        Ok(content) => content,
        Err(err) => {
            eprintln!("Failed to read catalog file: {}", err);
            return Catalog {
                databases: HashMap::new(),
            };
        }
    };

    // Step 3: Deserialize JSON into Catalog struct
    match serde_json::from_str::<Catalog>(&data) {
        Ok(catalog) => {
            // println!(
            //     "Catalog loaded successfully from {}",
            //     catalog_path.display()
            // );
            catalog
        }
        Err(err) => {
            eprintln!("Failed to parse catalog JSON: {}", err);
            Catalog {
                databases: HashMap::new(),
            }
        }
    }
}

pub fn save_catalog(catalog: &Catalog) {
    let catalog_path = Path::new(CATALOG_FILE);

    // Serialize the Catalog struct to pretty JSON
    let json = serde_json::to_string_pretty(catalog).expect("Failed to serialize catalog to JSON");

    // Write JSON to the catalog file
    fs::write(catalog_path, json).expect("Failed to write catalog file to disk");

    println!(
        "Catalog File updated with In Memory Data {}",
        catalog_path.display()
    );
}

/// Displays all databases available in the catalog.
/// If no databases exist, it prints an appropriate message.
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

pub fn create_database(catalog: &mut Catalog, db_name: &str) -> bool {
    // Step 1: Validate input
    if db_name.is_empty() {
        println!("Database name cannot be empty");
        return false;
    }

    if catalog.databases.contains_key(db_name) {
        println!("Database '{}' already exists", db_name);
        return false;
    }

    // Step 2: Insert new database into in-memory catalog
    catalog.databases.insert(
        db_name.to_string(),
        Database {
            tables: HashMap::new(),
        },
    );

    // Step 3: Persist updated catalog to disk
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

    // Step 4: Create the physical database directory
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

/// Creates a new table under the specified database and updates the catalog on disk.
/// If the table already exists, it will not be overwritten.
pub fn create_table(catalog: &mut Catalog, db_name: &str, table_name: &str, columns: Vec<Column>) {
    // Step 1: Validate database existence
    if !catalog.databases.contains_key(db_name) {
        println!(
            "Database '{}' does not exist. Cannot create table '{}'.",
            db_name, table_name
        );
        return;
    }

    // Step 2: Get mutable reference to the database
    let database = catalog.databases.get_mut(db_name).unwrap();

    // Step 3: Check if the table already exists
    if database.tables.contains_key(table_name) {
        println!(
            "Table '{}' already exists in database '{}'. Skipping creation.",
            table_name, db_name
        );
        return;
    }

    // Step 4: Create and insert the new table into catalog
    let new_table = Table { columns };
    database.tables.insert(table_name.to_string(), new_table);

    // Step 5: Save updated catalog using your existing save_catalog() helper
    save_catalog(catalog);

    // Step 6: Define the table file path (multi-database layout)
    let table_file_path = TABLE_FILE_TEMPLATE
        .replace("{database}", db_name)
        .replace("{table}", table_name);

    // Step 7: Create and initialize the table file
    let table_path = Path::new(&table_file_path);
    if !table_path.exists() {
        match OpenOptions::new()
            .create(true)
            .write(true)
            .read(true) // 👈 THIS is the key fix
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

pub fn show_tables(catalog: &Catalog, db_name: &str) {
    println!("--------------------------");
    println!("Tables in Database: {}", db_name);
    println!("--------------------------");

    // Check if database exists
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
