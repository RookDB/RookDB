//! Handles database-related user commands such as listing, creating,
//! and selecting databases from the catalog.

use std::io::{self, Write};
use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::{Catalog, init_catalog_page_storage, show_databases};

/// Displays all available databases
pub fn show_databases_cmd(catalog: &mut Catalog, bm: &mut BufferManager) -> io::Result<()> {
    let mut pm = init_catalog_page_storage()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    show_databases(catalog, &mut pm, bm);
    Ok(())
}

/// Creates a new database based on user input
pub fn create_database_cmd(catalog: &mut Catalog, bm: &mut BufferManager) -> io::Result<()> {
    let mut db_name = String::new();

    // Prompt for database name
    print!("Enter new database name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut db_name)?;

    // Validate and create database
    let db_name = db_name.trim();
    if db_name.is_empty() {
        println!("Database name cannot be empty.");
    } else {
        let mut pm = init_catalog_page_storage()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        match storage_manager::catalog::create_database(
            catalog,
            &mut pm,
            bm,
            db_name,
            "admin",
            storage_manager::catalog::types::Encoding::UTF8,
        ) {
            Ok(_) => println!("Database '{}' created successfully.", db_name),
            Err(e) => println!("Failed to create database '{}': {:?}", db_name, e),
        }
    }
    Ok(())
}

/// Selects an existing database and updates the current context
pub fn select_database_cmd(_catalog: &mut Catalog, bm: &mut BufferManager, current_db: &mut Option<String>) -> io::Result<()> {
    let pm = init_catalog_page_storage()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        
    let records = pm.scan_catalog(bm, storage_manager::catalog::page_manager::CAT_DATABASE).unwrap_or_default();
    if records.is_empty() {
        println!("No databases found.");
        return Ok(());
    }

    println!("Available Databases:");
    let mut db_names = Vec::new();
    for r in &records {
        if let Ok((_, name, ..)) = storage_manager::catalog::serialize::deserialize_database_tuple(r) {
            println!("- {}", name);
            db_names.push(name);
        }
    }

    let mut db_name = String::new();
    print!("Enter database name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut db_name)?;

    let db_name = db_name.trim().to_string();
    if db_names.contains(&db_name) {
        *current_db = Some(db_name.clone());
        println!("Database '{}' selected.", db_name);
    } else {
        println!("Database '{}' does not exist.", db_name);
    }

    Ok(())
}
