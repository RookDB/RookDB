//! CLI handlers for sort-related commands (options 10-13).
//!
//! - Option 10: Sort Table (convert heap to ordered)
//! - Option 11: Create Ordered Table
//! - Option 12: Range Scan
//! - Option 13: ORDER BY Query

use std::fs::OpenOptions;
use std::io::{self, Write};

use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::types::{Column, SortDirection, SortKey};
use storage_manager::catalog::{create_table, Catalog};
use storage_manager::executor::{create_ordered_file_from_heap, order_by_execute};
use storage_manager::ordered::range_scan;

/// Option 10: Sort an existing heap table into an ordered file.
pub fn sort_table_cmd(
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

    let mut sort_input = String::new();
    print!("Enter sort columns (format: col1:ASC,col2:DESC): ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut sort_input)?;
    let sort_input = sort_input.trim();

    let sort_keys = match parse_sort_keys(catalog, &db, &table_name, sort_input) {
        Ok(keys) => keys,
        Err(e) => {
            println!("Error parsing sort keys: {}", e);
            return Ok(());
        }
    };

    match create_ordered_file_from_heap(
        catalog,
        &db,
        &table_name,
        sort_keys.clone(),
        buffer_manager.pool_size,
    ) {
        Ok(()) => {
            // Format the sort key description
            let desc = format_sort_keys(catalog, &db, &table_name, &sort_keys);
            println!(
                "Table '{}' sorted by [{}]. File type changed to ordered.",
                table_name, desc
            );
        }
        Err(e) => {
            println!("Error sorting table: {}", e);
        }
    }

    Ok(())
}

/// Option 11: Create a new table that maintains sort order.
pub fn create_ordered_table_cmd(
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

    print!("\nEnter columns (format: col1:type,col2:type): ");
    io::stdout().flush()?;
    let mut col_input = String::new();
    io::stdin().read_line(&mut col_input)?;
    let col_input = col_input.trim();

    let columns: Vec<Column> = col_input
        .split(',')
        .filter_map(|part| {
            let parts: Vec<&str> = part.trim().split(':').collect();
            if parts.len() == 2 {
                Some(Column {
                    name: parts[0].trim().to_string(),
                    data_type: parts[1].trim().to_uppercase(),
                })
            } else {
                println!("Invalid column format: '{}'. Skipping.", part.trim());
                None
            }
        })
        .collect();

    if columns.is_empty() {
        println!("No valid columns provided.");
        return Ok(());
    }

    let mut sort_input = String::new();
    print!("Enter sort columns (format: col1:ASC,col2:DESC): ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut sort_input)?;
    let sort_input = sort_input.trim();

    // Parse sort keys against the column list we just built
    let sort_keys = match parse_sort_keys_from_columns(&columns, sort_input) {
        Ok(keys) => keys,
        Err(e) => {
            println!("Error parsing sort keys: {}", e);
            return Ok(());
        }
    };

    create_table(
        catalog,
        &db,
        &table_name,
        columns.clone(),
        Some(sort_keys.clone()),
    );
    buffer_manager.load_table_from_disk(&db, &table_name)?;

    let desc = sort_keys
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
        .join(", ");
    println!(
        "Ordered table '{}' created with sort key [{}].",
        table_name, desc
    );

    Ok(())
}

/// Option 12: Range scan on an ordered file.
pub fn range_scan_cmd(catalog: &mut Catalog, current_db: &Option<String>) -> io::Result<()> {
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

    let mut col_name = String::new();
    print!("Enter column name for range: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut col_name)?;
    let col_name = col_name.trim().to_string();

    let mut start_val = String::new();
    print!("Enter start value (or leave empty for unbounded): ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut start_val)?;
    let start_val = start_val.trim().to_string();
    let start_value: Option<&str> = if start_val.is_empty() {
        None
    } else {
        Some(&start_val)
    };

    let mut end_val = String::new();
    print!("Enter end value (or leave empty for unbounded): ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut end_val)?;
    let end_val = end_val.trim().to_string();
    let end_value: Option<&str> = if end_val.is_empty() {
        None
    } else {
        Some(&end_val)
    };

    let table_path = format!("database/base/{}/{}.dat", db, table_name);
    let mut file = match OpenOptions::new().read(true).write(true).open(&table_path) {
        Ok(f) => f,
        Err(e) => {
            println!("Error opening table file: {}", e);
            return Ok(());
        }
    };

    match range_scan(
        &mut file,
        catalog,
        &db,
        &table_name,
        &col_name,
        start_value,
        end_value,
    ) {
        Ok(tuples) => {
            // Get columns for decoding
            let columns = &catalog
                .databases
                .get(&db)
                .and_then(|d| d.tables.get(&table_name))
                .map(|t| &t.columns);

            match columns {
                Some(cols) => {
                    println!("\n=== Range Scan results ({} tuples) ===", tuples.len());
                    for (i, tuple) in tuples.iter().enumerate() {
                        print!("  {}: ", i + 1);
                        print_tuple(tuple, cols);
                        println!();
                    }
                    println!("=== End of range scan ===\n");
                }
                None => {
                    println!("Could not find table schema for decoding.");
                }
            }
        }
        Err(e) => {
            println!("Range scan error: {}", e);
        }
    }

    Ok(())
}

/// Option 13: ORDER BY query.
pub fn order_by_cmd(
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

    let mut sort_input = String::new();
    print!("Enter sort columns (format: col1:ASC,col2:DESC): ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut sort_input)?;
    let sort_input = sort_input.trim();

    let sort_keys = match parse_sort_keys(catalog, &db, &table_name, sort_input) {
        Ok(keys) => keys,
        Err(e) => {
            println!("Error parsing sort keys: {}", e);
            return Ok(());
        }
    };

    match order_by_execute(
        catalog,
        &db,
        &table_name,
        sort_keys,
        buffer_manager.pool_size,
    ) {
        Ok(()) => {}
        Err(e) => {
            println!("ORDER BY error: {}", e);
        }
    }

    Ok(())
}

// ---- Helper functions ----

/// Parse a sort key string like "col1:ASC,col2:DESC" against the catalog schema.
fn parse_sort_keys(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    input: &str,
) -> Result<Vec<SortKey>, String> {
    let db = catalog
        .databases
        .get(db_name)
        .ok_or_else(|| format!("Database '{}' not found", db_name))?;
    let table = db
        .tables
        .get(table_name)
        .ok_or_else(|| format!("Table '{}' not found", table_name))?;

    parse_sort_keys_from_columns(&table.columns, input)
}

/// Parse sort key string against a column list.
fn parse_sort_keys_from_columns(columns: &[Column], input: &str) -> Result<Vec<SortKey>, String> {
    let mut keys = Vec::new();

    for part in input.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let pieces: Vec<&str> = part.split(':').collect();
        if pieces.len() != 2 {
            return Err(format!(
                "Invalid sort key format '{}'. Expected 'column:ASC' or 'column:DESC'.",
                part
            ));
        }

        let col_name = pieces[0].trim();
        let direction_str = pieces[1].trim().to_uppercase();

        let col_idx = columns
            .iter()
            .position(|c| c.name == col_name)
            .ok_or_else(|| format!("Column '{}' not found in table schema", col_name))?;

        let direction = match direction_str.as_str() {
            "ASC" => SortDirection::Ascending,
            "DESC" => SortDirection::Descending,
            _ => {
                return Err(format!(
                    "Invalid direction '{}'. Use ASC or DESC.",
                    direction_str
                ))
            }
        };

        keys.push(SortKey {
            column_index: col_idx as u32,
            direction,
        });
    }

    if keys.is_empty() {
        return Err("No sort keys provided.".to_string());
    }

    Ok(keys)
}

/// Format sort keys into a human-readable description.
fn format_sort_keys(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    sort_keys: &[SortKey],
) -> String {
    let columns = catalog
        .databases
        .get(db_name)
        .and_then(|d| d.tables.get(table_name))
        .map(|t| &t.columns);

    sort_keys
        .iter()
        .map(|sk| {
            let col_name = columns
                .and_then(|cols| cols.get(sk.column_index as usize))
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

/// Print a single tuple decoded according to the schema.
fn print_tuple(tuple: &[u8], columns: &[Column]) {
    let mut cursor = 0usize;
    for col in columns {
        match col.data_type.as_str() {
            "INT" => {
                if cursor + 4 <= tuple.len() {
                    let val = i32::from_le_bytes(tuple[cursor..cursor + 4].try_into().unwrap());
                    print!("{}={} ", col.name, val);
                    cursor += 4;
                }
            }
            "TEXT" => {
                if cursor + 10 <= tuple.len() {
                    let text_bytes = &tuple[cursor..cursor + 10];
                    let text = String::from_utf8_lossy(text_bytes).trim().to_string();
                    print!("{}='{}' ", col.name, text);
                    cursor += 10;
                }
            }
            _ => {
                print!("{}=<unsupported> ", col.name);
            }
        }
    }
}
