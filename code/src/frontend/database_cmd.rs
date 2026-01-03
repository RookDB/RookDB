use std::io::{self, Write};
use storage_manager::catalog::{Catalog, create_database, show_databases};

pub fn show_databases_cmd(catalog: &Catalog) {
    show_databases(catalog);
}

pub fn create_database_cmd(catalog: &mut Catalog) -> io::Result<()> {
    let mut db_name = String::new();
    print!("Enter new database name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut db_name)?;

    let db_name = db_name.trim();
    if db_name.is_empty() {
        println!("Database name cannot be empty.");
    } else if create_database(catalog, db_name) {
        println!("Database '{}' created successfully.", db_name);
    } else {
        println!("Failed to create database '{}'.", db_name);
    }
    Ok(())
}

pub fn select_database_cmd(
    catalog: &Catalog,
    current_db: &mut Option<String>,
) -> io::Result<()> {
    if catalog.databases.is_empty() {
        println!("No databases found.");
        return Ok(());
    }

    println!("Available Databases:");
    for db in catalog.databases.keys() {
        println!("- {}", db);
    }

    let mut db_name = String::new();
    print!("Enter database name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut db_name)?;

    let db_name = db_name.trim().to_string();
    if catalog.databases.contains_key(&db_name) {
        *current_db = Some(db_name.clone());
        println!("Database '{}' selected.", db_name);
    } else {
        println!("Database '{}' does not exist.", db_name);
    }

    Ok(())
}
