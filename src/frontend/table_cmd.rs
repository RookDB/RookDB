//! Handles table-related user commands such as listing tables,
//! creating tables, and displaying table statistics.

use std::io::{self, Write};

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::types::SortDirection;
use storage_manager::catalog::{Catalog, Column, SortKey, create_table, show_tables};
use storage_manager::statistics::print_table_page_count;

use crate::frontend::sort_cmd;

/// Displays tables in the currently selected database
pub fn show_tables_cmd(catalog: &Catalog, current_db: &Option<String>) {
    let db = match current_db {
        Some(db) => db,
        None => {
            println!("No database selected. Please select a database first.");
            return;
        }
    };
    show_tables(catalog, db);
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
            data_type: parts[1].to_string(),
        });
    }

    if columns.is_empty() {
        println!("No valid columns provided.");
        return Ok(());
    }

    println!("\nSelect table type:");
    println!("1. Sorted Table");
    println!("2. Unsorted Table");
    print!("Enter your choice (1/2): ");
    io::stdout().flush()?;

    let mut table_type = String::new();
    io::stdin().read_line(&mut table_type)?;
    let table_type = table_type.trim();

    let sort_keys: Option<Vec<SortKey>> = match table_type {
        "1" => {
            let mut sort_input = String::new();
            print!("Enter sort columns (format: col1:ASC,col2:DESC): ");
            io::stdout().flush()?;
            io::stdin().read_line(&mut sort_input)?;
            let sort_input = sort_input.trim();

            match sort_cmd::parse_sort_keys_from_columns(&columns, sort_input) {
                Ok(keys) => Some(keys),
                Err(e) => {
                    println!("Error parsing sort keys: {}", e);
                    return Ok(());
                }
            }
        }
        "2" => None,
        _ => {
            println!("Invalid table type. Please choose 1 or 2.");
            return Ok(());
        }
    };

    create_table(
        catalog,
        &db,
        &table_name,
        columns.clone(),
        sort_keys.clone(),
    );
    buffer_manager.load_table_from_disk(&db, &table_name)?;

    if let Some(keys) = sort_keys {
        let desc = format_sort_keys(&columns, &keys);
        println!(
            "Table '{}' created as sorted table with sort key [{}].",
            table_name, desc
        );
    } else {
        println!("Table '{}' created as unsorted heap table.", table_name);
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

fn format_sort_keys(columns: &[Column], sort_keys: &[SortKey]) -> String {
    sort_keys
        .iter()
        .map(|sk| {
            let col_name = columns
                .get(sk.column_index as usize)
                .map(|c| c.name.as_str())
                .unwrap_or("?");
            let dir = match sk.direction {
                SortDirection::Ascending => "ASC",
                SortDirection::Descending => "DESC",
            };
            format!("{} {}", col_name, dir)
        })
        .collect::<Vec<_>>()
        .join(", ")
}
