//! ORDER BY execution and heap-to-ordered conversion.
//!
//! Provides:
//! - `order_by_execute()`: runs ORDER BY on a table, sorting first if needed
//! - `create_ordered_file_from_heap()`: converts a heap table to an ordered file

use std::fs::OpenOptions;
use std::io;

use crate::catalog::types::{Catalog, Column, SortKey};
use crate::ordered::scan::ordered_scan;
use crate::sorting::external_sort::external_sort;
use crate::sorting::in_memory_sort::in_memory_sort;
use crate::table::page_count;

/// Executes an ORDER BY query on a table.
///
/// If the table is already an ordered file sorted on the requested columns,
/// performs a direct ordered scan. Otherwise, performs an external sort
/// first, then scans.
pub fn order_by_execute(
    catalog: &mut Catalog,
    db_name: &str,
    table_name: &str,
    sort_keys: Vec<SortKey>,
    buffer_pool_size: usize,
) -> io::Result<()> {
    // 1. Check if already sorted on the correct keys
    let already_sorted = {
        let db = catalog.databases.get(db_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Database '{}' not found", db_name),
            )
        })?;
        let table = db.tables.get(table_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Table '{}' not found", table_name),
            )
        })?;

        match (&table.sort_keys, &table.file_type) {
            (Some(existing_keys), Some(ft)) if ft == "ordered" => {
                // Check if sort keys match exactly
                if existing_keys.len() == sort_keys.len() {
                    existing_keys.iter().zip(sort_keys.iter()).all(|(a, b)| {
                        a.column_index == b.column_index && a.direction == b.direction
                    })
                } else {
                    false
                }
            }
            _ => false,
        }
    };

    // 2. Sort if needed
    if !already_sorted {
        println!("Table is not sorted in the requested order. Sorting...");
        create_ordered_file_from_heap(catalog, db_name, table_name, sort_keys, buffer_pool_size)?;
    } else {
        println!("Table is already sorted in the requested order.");
    }

    // 3. Open the (now sorted) file and scan
    let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&table_path)?;

    let tuples = ordered_scan(&mut file, catalog, db_name, table_name)?;

    // 4. Decode and print tuples
    let db = catalog.databases.get(db_name).unwrap();
    let table = db.tables.get(table_name).unwrap();
    let columns = &table.columns;

    println!(
        "\n=== ORDER BY results for '{}.{}' ({} tuples) ===",
        db_name,
        table_name,
        tuples.len()
    );

    print_sort_order(columns, &table.sort_keys);

    for (i, tuple) in tuples.iter().enumerate() {
        print!("  {}: ", i + 1);
        print_tuple(tuple, columns);
        println!();
    }

    println!("=== End of ORDER BY results ===\n");

    Ok(())
}

/// Converts an existing heap table into an ordered file by sorting its contents.
///
/// Chooses between in-memory sort and external sort based on table size
/// relative to the buffer pool.
pub fn create_ordered_file_from_heap(
    catalog: &mut Catalog,
    db_name: &str,
    table_name: &str,
    sort_keys: Vec<SortKey>,
    buffer_pool_size: usize,
) -> io::Result<()> {
    // 1. Verify the table exists
    {
        let db = catalog.databases.get(db_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Database '{}' not found", db_name),
            )
        })?;
        let _ = db.tables.get(table_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Table '{}' not found", table_name),
            )
        })?;
    }

    // 2. Open the table file and read total pages
    let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&table_path)?;

    let total_pages = page_count(&mut file)?;
    let data_pages = if total_pages > 1 {
        (total_pages - 1) as usize
    } else {
        0
    };

    // 3. Choose sort strategy
    if data_pages <= buffer_pool_size {
        println!(
            "Table '{}' has {} data pages, fits in buffer pool ({}). Using in-memory sort.",
            table_name, data_pages, buffer_pool_size
        );
        in_memory_sort(catalog, db_name, table_name, sort_keys, &mut file)?;
    } else {
        println!(
            "Table '{}' has {} data pages, exceeds buffer pool ({}). Using external sort.",
            table_name, data_pages, buffer_pool_size
        );
        external_sort(catalog, db_name, table_name, sort_keys, buffer_pool_size)?;
    }

    Ok(())
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

/// Print the sort order header.
fn print_sort_order(columns: &[Column], sort_keys: &Option<Vec<SortKey>>) {
    if let Some(keys) = sort_keys {
        let parts: Vec<String> = keys
            .iter()
            .map(|sk| {
                let col_name = if (sk.column_index as usize) < columns.len() {
                    &columns[sk.column_index as usize].name
                } else {
                    "?"
                };
                let dir = match sk.direction {
                    crate::catalog::types::SortDirection::Ascending => "ASC",
                    crate::catalog::types::SortDirection::Descending => "DESC",
                };
                format!("{} {}", col_name, dir)
            })
            .collect();
        println!("[Ordered by: {}]", parts.join(", "));
    }
}
