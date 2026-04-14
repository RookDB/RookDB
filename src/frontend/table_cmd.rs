//! Handles table-related user commands such as listing tables,
//! creating tables, and displaying table statistics.

use std::io::{self, Write};

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::types::ColumnDefinition;
use storage_manager::catalog::{
    Catalog, create_table, init_catalog_page_storage, show_tables,
};
use storage_manager::statistics::print_table_page_count;

/// Displays tables in the currently selected database
pub fn show_tables_cmd(
    catalog: &Catalog,
    bm: &mut BufferManager,
    current_db: &Option<String>,
) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db,
        None => {
            println!("No database selected. Please select a database first.");
            return Ok(());
        }
    };
    let mut pm = init_catalog_page_storage()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    show_tables(catalog, &mut pm, bm, db);
    Ok(())
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

    let mut columns: Vec<ColumnDefinition> = Vec::new();
    loop {
        let mut input = String::new();
        print!("Enter column (name:type): ");
        io::stdout().flush()?;
        io::stdin().read_line(&mut input)?;
        let input = input.trim();
        if input.is_empty() {
            break;
        }

        let parts: Vec<&str> = input.splitn(2, ':').collect();
        if parts.len() != 2 {
            println!("Invalid format. Please use name:type (e.g. id:INT)");
            continue;
        }

        let col_name = parts[0].trim().to_string();
        let type_str = parts[1].trim().to_string();

        columns.push(ColumnDefinition {
            name: col_name,
            type_name: type_str,
            type_modifier: None,
            is_nullable: true,
            default_value: None,
        });
    }

    let mut pm = init_catalog_page_storage()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    match create_table(
        catalog,
        &mut pm,
        buffer_manager,
        &db,
        &table_name,
        columns,
        vec![],
    ) {
        Ok(_) => {
            println!("Table '{}' created in database '{}'.", table_name, db);
        }
        Err(e) => {
            println!("Failed to create table '{}': {:?}", table_name, e);
        }
    }

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

    print_table_page_count(db_name, table_name)?;

    Ok(())
}
