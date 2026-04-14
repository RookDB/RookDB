//! Handles database-related user commands such as listing, creating,
//! and selecting databases from the catalog.

use std::io::{self, Write};
use storage_manager::catalog::{Catalog, create_database, show_databases};
use storage_manager::storage::database_logger;

/// Displays all available databases
pub fn show_databases_cmd(catalog: &Catalog) {
    let count = catalog.databases.len();
    show_databases(catalog);
    database_logger::log_show_databases(count);
}

/// Creates a new database based on user input
pub fn create_database_cmd(catalog: &mut Catalog) -> io::Result<()> {
    let mut db_name = String::new();

    // Prompt for database name
    print!("Enter new database name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut db_name)?;

    // Validate and create database
    let db_name = db_name.trim();
    if db_name.is_empty() {
        println!("Database name cannot be empty.");
        database_logger::log_create_database_failed(db_name, "empty name");
    } else if create_database(catalog, db_name) {
        println!("Database '{}' created successfully.", db_name);
        database_logger::log_create_database(db_name);
    } else {
        println!("Failed to create database '{}'.", db_name);
        database_logger::log_create_database_failed(db_name, "already exists or creation error");
    }
    Ok(())
}

/// Selects an existing database and updates the current context
pub fn select_database_cmd(catalog: &Catalog, current_db: &mut Option<String>) -> io::Result<()> {
    // Check if any databases exist
    if catalog.databases.is_empty() {
        println!("No databases found.");
        database_logger::log_select_database_failed("", "no databases found");
        return Ok(());
    }

    // Display available databases
    println!("Available Databases:");
    for db in catalog.databases.keys() {
        println!("- {}", db);
    }

    // Read database name from user
    let mut db_name = String::new();
    print!("Enter database name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut db_name)?;

    let db_name = db_name.trim().to_string();
    // Update selected database
    if catalog.databases.contains_key(&db_name) {
        *current_db = Some(db_name.clone());
        println!("Database '{}' selected.", db_name);
        database_logger::log_select_database(&db_name);
    } else {
        println!("Database '{}' does not exist.", db_name);
        database_logger::log_select_database_failed(&db_name, "database not found");
    }

    Ok(())
}
