use std::fs::OpenOptions;
// use std::io::{self, Read, Seek, SeekFrom};
use std::io::{self, Write};

// use storage_manager::disk::{create_page, read_page};
// use storage_manager::disk::create_page;
use storage_manager::catalog::{
    Column, create_database, create_table, init_catalog, load_catalog, show_databases, show_tables,
};
use storage_manager::page::{load_csv_and_insert, show_tuples};
// use storage_manager::table::init_table;

fn main() -> io::Result<()> {
    println!("--------------------------------------");
    println!("Welcome to RookDB");
    println!("--------------------------------------\n");

    // Initialize catalog if missing
    println!("Initializing Catalog File...\n");
    init_catalog();

    // Load catalog from disk
    println!("Loading Catalog...\n");
    let mut catalog = load_catalog();

    // Keep track of the currently selected database
    let mut current_db: Option<String> = None;

    loop {
        println!("\n=============================");
        println!("Choose an option:");
        println!("1. Show Databases");
        println!("2. Create Database");
        println!("3. Select Database");
        println!("4. Show Tables");
        println!("5. Create Table");
        println!("6. Load CSV");
        println!("7. Show Tuples");
        println!("8. Exit");
        println!("=============================");

        // if let Some(ref db) = current_db {
        //     println!("Current database: '{}'", db);
        // } else {
        //     println!("No database selected.");
        // }

        print!("Enter your choice: ");
        io::stdout().flush()?;

        let mut choice = String::new();
        io::stdin().read_line(&mut choice)?;
        let choice = choice.trim();

        match choice {
            "1" => {
                show_databases(&catalog);
            }
            // -----------------------
            // Option 1: Create Database
            // -----------------------
            "2" => {
                let mut db_name = String::new();
                print!("\nEnter new database name: ");
                io::stdout().flush()?;
                io::stdin().read_line(&mut db_name)?;
                let db_name = db_name.trim();

                if db_name.is_empty() {
                    println!("Database name cannot be empty.");
                } else if create_database(&mut catalog, db_name) {
                    println!("Database '{}' created successfully.", db_name);
                } else {
                    println!("Failed to create database '{}'.", db_name);
                }
            }

            // -----------------------
            // Option 3: Select Database
            // -----------------------
            "3" => {
                if catalog.databases.is_empty() {
                    println!("No databases found. Please create one first.");
                    continue;
                }

                println!("\nAvailable Databases:");
                for db in catalog.databases.keys() {
                    println!("- {}", db);
                }

                let mut db_name = String::new();
                print!("\nEnter database name to select: ");
                io::stdout().flush()?;
                io::stdin().read_line(&mut db_name)?;
                let db_name = db_name.trim().to_string();

                if catalog.databases.contains_key(&db_name) {
                    current_db = Some(db_name.clone());
                    println!("Database '{}' selected successfully.", db_name);
                } else {
                    println!("Database '{}' does not exist.", db_name);
                }
            }

            // Option 4: Show Tables
            "4" => {
                let db_name = match &current_db {
                    Some(name) => name.clone(),
                    None => {
                        println!("No database selected. Please select a database first.");
                        continue;
                    }
                };

                show_tables(&catalog, &db_name);
            }

            // -----------------------
            // Option 5: Create Table
            // -----------------------
            "5" => {
                // Check if a database is currently selected
                let db_name = match &current_db {
                    Some(name) => name.clone(),
                    None => {
                        println!("No database selected. Please select a database first.");
                        continue;
                    }
                };

                let mut table_name = String::new();
                print!("\nEnter new table name: ");
                io::stdout().flush()?;
                io::stdin().read_line(&mut table_name)?;
                let table_name = table_name.trim().to_string();

                println!("\nEnter columns in the format: column_name:data_type");
                println!("(Press Enter on an empty line to finish)\n");

                let mut columns: Vec<Column> = Vec::new();

                loop {
                    let mut input = String::new();
                    print!("Enter column (name:type): ");
                    io::stdout().flush()?;
                    io::stdin().read_line(&mut input)?;
                    let input = input.trim();

                    if input.is_empty() {
                        break;
                    }

                    let parts: Vec<&str> = input.split(':').collect();
                    if parts.len() != 2 {
                        println!("Invalid format. Please use name:type (e.g. id:INT)");
                        continue;
                    }

                    let column_name = parts[0].trim().to_string();
                    let column_type = parts[1].trim().to_string();

                    columns.push(Column {
                        name: column_name,
                        data_type: column_type,
                    });
                }

                if columns.is_empty() {
                    println!("No columns provided. Table not created.");
                    continue;
                }

                create_table(&mut catalog, &db_name, &table_name, columns);
            }

            // -----------------------
            // 6. Insert Tuple (JSON input)
            // -----------------------
            "6" => {
                let db_name = match &current_db {
                    Some(name) => name.clone(),
                    None => {
                        println!("No database selected. Please select a database first.");
                        continue;
                    }
                };

                println!("Enter table name: ");
                let mut table_name = String::new();
                io::stdin().read_line(&mut table_name)?;
                let table_name = table_name.trim();

                println!("Enter CSV file path: ");
                let mut csv_path = String::new();
                io::stdin().read_line(&mut csv_path)?;
                let csv_path = csv_path.trim();

                let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&table_path)?;

                let catalog = load_catalog();
                load_csv_and_insert(&catalog, &db_name, table_name, &mut file, csv_path)?;
            }

            // -----------------------
            // Option 7: Show Tuples
            // -----------------------
            "7" => {
                let db_name = match &current_db {
                    Some(name) => name.clone(),
                    None => {
                        println!("No database selected. Please select a database first.");
                        continue;
                    }
                };

                println!("Enter table name: ");
                let mut table_name = String::new();
                io::stdin().read_line(&mut table_name)?;
                let table_name = table_name.trim();

                let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&table_path)?;

                let catalog = load_catalog();
                show_tuples(&catalog, &db_name, table_name, &mut file)?;
            }

            // -----------------------
            // Exit
            // -----------------------
            "8" => {
                println!("\nExiting Storage Manager. Goodbye!");
                break;
            }

            // -----------------------
            // Invalid Option
            // -----------------------
            _ => {
                println!("Invalid choice. Please select 1, 2,3 ... numbers only");
            }
        }
    }
    Ok(())
}
