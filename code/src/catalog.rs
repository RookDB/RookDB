use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use std::fs;
use std::path::Path;

pub const DATA_DIR: &str = "database";
pub const CATALOG_DIR: &str = "database/global";
pub const CATALOG_FILE: &str = "database/global/catalog.json";
pub const TABLE_DIR: &str = "database/base";
pub const TABLE_FILE_TEMPLATE: &str = "database/base/{table}.dat";

use crate::table::{init_table};
use crate::page::{page_count};


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
pub struct Catalog {
    pub tables: HashMap<String, Table>,
}

pub fn init_catalog() {
    let catalog_path = Path::new(CATALOG_FILE);

    // Step 1: Create directories if not exist
    if let Some(parent) = catalog_path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent).expect("Failed to create catalog directory");
        }
    }

    // Step 2: Create catalog.json if not exist
    if !catalog_path.exists() {
        let empty_catalog = Catalog { tables: HashMap::new() };
        let json = serde_json::to_string_pretty(&empty_catalog).expect("Failed to serialize empty catalog");
        fs::write(catalog_path, json).expect("Failed to write catalog file");
        println!("Catalog File Not exist. Created new catalog file at {}", catalog_path.display());
    } else {
        println!("Catalog file already exists at {}", catalog_path.display());
    }
}

pub fn load_catalog() -> Catalog {
    let catalog_path = Path::new(CATALOG_FILE);

    // Check if catalog file exists
    if !catalog_path.exists() {
        eprintln!("Catalog file does not exist at {}.", catalog_path.display());
        return Catalog { tables: HashMap::new() };
    }

    // Read the catalog file
    let data = fs::read_to_string(catalog_path);
    let data = match data {
        Ok(content) => content,
        Err(err) => {
            eprintln!("Failed to read catalog file: {}", err);
            return Catalog { tables: HashMap::new() };
        }
    };

    // Deserialize JSON into Catalog struct
    match serde_json::from_str::<Catalog>(&data) {
        Ok(catalog) => catalog,
        Err(err) => {
            eprintln!("Failed to parse catalog JSON: {}", err);
            Catalog { tables: HashMap::new() }
        }
    }
}


pub fn save_catalog(catalog: &Catalog) {
    let catalog_path = Path::new(CATALOG_FILE);

    // Serialize the Catalog struct to pretty JSON
    let json = serde_json::to_string_pretty(catalog)
        .expect("Failed to serialize catalog to JSON");

    // Write JSON to the catalog file
    fs::write(catalog_path, json)
        .expect("Failed to write catalog file to disk");

    println!("Catalog File updated with In Memory Data {}", catalog_path.display());
}

/// Creates a new table and updates the catalog on disk.
/// If a table with the same name already exists, it will not be overwritten.
pub fn create_table(catalog: &mut Catalog, table_name: &str, columns: Vec<Column>) {
    // Check if table already exists
    if catalog.tables.contains_key(table_name) {
        println!("Table '{}' already exists in the catalog. Skipping creation.", table_name);
        return;
    }

    // Create a new table
    let new_table = Table { columns };

    // Insert into catalog
    catalog.tables.insert(table_name.to_string(), new_table);

    // Save catalog to disk
    save_catalog(catalog);

    // Define table file path (replace {table} with actual table name)
    let table_file_path = format!("{}/{}.dat", TABLE_DIR, table_name);

    // Create and initialize the table file
    if !Path::new(&table_file_path).exists() {
        match fs::File::create(&table_file_path) {
            Ok(mut file) => {
                println!("Table data file created at '{}'.", table_file_path);

                // Initialize the table file with headers (zeroed out bytes)
                if let Err(e) = init_table(&mut file) {
                    eprintln!("Failed to initialize table '{}': {}", table_name, e);
                } else {
                    println!("Table '{}' initialized successfully.", table_name);
                }
            }
            Err(e) => {
                eprintln!("Failed to create table data file '{}': {}", table_file_path, e);
                return;
            }
        }

        // ✅ Reopen file in read mode before calling page_count
        match fs::File::open(&table_file_path) {
            Ok(mut file) => {
                match page_count(&mut file) {
                    Ok(count) => println!("Table '{}' has {} pages.", table_name, count),
                    Err(e) => eprintln!("Failed to read page count for '{}': {}", table_name, e),
                }
            }
            Err(e) => {
                eprintln!("Failed to reopen '{}' for reading: {}", table_file_path, e);
            }
        }
    } else {
        println!("Table data file '{}' already exists.", table_file_path);
    }

    println!("Table '{}' created successfully and saved to catalog.", table_name);
}
