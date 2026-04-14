//! Handles table-related user commands such as listing tables,
//! creating tables, and displaying table statistics.

use std::io::{self, Write};

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::{Catalog, Column, create_table, show_tables};
use storage_manager::statistics::print_table_page_count;
use storage_manager::storage::database_logger;

/// Displays tables in the currently selected database
pub fn show_tables_cmd(catalog: &Catalog, current_db: &Option<String>) {
    let db = match current_db {
        Some(db) => db,
        None => {
            println!("No database selected. Please select a database first.");
            return;
        }
    };
    
    let table_count = catalog.databases.get(db).map(|d| d.tables.len()).unwrap_or(0);
    show_tables(catalog, db);
    database_logger::log_show_tables(db, table_count);
}

pub fn create_table_cmd(
    catalog: &mut Catalog,
    buffer_manager: &mut BufferManager,
    current_db: &Option<String>,
) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first.");
            return Ok(());
        }
    };

    let mut table_name = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    print!("\nEnter columns in the format:- column_name:data_type\n");
    print!("Press Enter on an empty line to finish\n");

    let mut columns = Vec::new();
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

        columns.push(Column {
            name: parts[0].to_string(),
            data_type: Some(parts[1].to_string()),
            nullable: false,
            schema_version: Some(2),
            toast_strategy: None,
        });
    }

    if columns.is_empty() {
        println!("Table must have at least one column.");
        database_logger::log_create_table_failed(&db, &table_name, "no columns defined");
        return Ok(());
    }

    let column_count = columns.len();
    create_table(catalog, &db, &table_name, columns);
    buffer_manager.load_table_from_disk(&db, &table_name)?;
    
    println!("Table '{}' created successfully with {} columns.", table_name, column_count);
    database_logger::log_create_table(&db, &table_name, column_count);

    Ok(())
}

pub fn show_table_statistics_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db_name = match current_db {
        Some(db) => db,
        None => {
            println!("No database selected.");
            return Ok(());
        }
    };

    print!("Enter table name: ");
    io::stdout().flush()?;

    let mut table_name = String::new();
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim();

    match print_table_page_count(db_name, table_name) {
        Ok(_) => {
            // Note: We'll need to get page_count and tuple_count from the statistics function
            // For now, just log the operation
            database_logger::log_show_table_statistics(db_name, table_name, 0, 0);
        }
        Err(e) => {
            database_logger::log_error(
                &format!("show_table_statistics: {}", table_name),
                &e.to_string(),
            );
        }
    }

    Ok(())
}
