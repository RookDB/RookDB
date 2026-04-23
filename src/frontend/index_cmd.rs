//! Frontend command handlers for index operations.
//!
//! Each function corresponds to a user menu choice and prompts the user for
//! input before delegating to the backend index subsystem.

use std::fs;
use std::io::{self, Write};

use storage_manager::catalog::{
    create_index, create_secondary_index, drop_index, drop_secondary_index,
    list_indexes, list_secondary_indices, load_catalog,
};
use storage_manager::catalog::types::{Column, IndexAlgorithm};
use storage_manager::executor::index_scan;
use storage_manager::index::{
    AnyIndex, cluster_table_by_index, index_file_path, index_key_from_values,
    rebuild_secondary_index, secondary_index_file_path, validate_index_consistency,
};

// ─── Create index ─────────────────────────────────────────────────────────────

/// Prompt the user for index details and build + register a new index.
pub fn create_index_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db_name = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Use 'Select Database' first.");
            return Ok(());
        }
    };

    print!("Table name: ");
    io::stdout().flush()?;
    let mut table_name = String::new();
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    print!("Column(s) to index (comma-separated): ");
    io::stdout().flush()?;
    let mut column_names_str = String::new();
    io::stdin().read_line(&mut column_names_str)?;
    let column_names: Vec<String> = column_names_str
        .trim()
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    print!("Index name: ");
    io::stdout().flush()?;
    let mut index_name = String::new();
    io::stdin().read_line(&mut index_name)?;
    let index_name = index_name.trim().to_string();

    println!("Algorithm options:");
    println!("  Hash-based : static_hash | chained_hash | extendible_hash | linear_hash");
    println!("  Tree-based : btree | bplus_tree | radix_tree | skip_list | lsm_tree");
    print!("Algorithm [bplus_tree]: ");
    io::stdout().flush()?;
    let mut algo_str = String::new();
    io::stdin().read_line(&mut algo_str)?;
    let algo_str = algo_str.trim();
    let algo_str = if algo_str.is_empty() { "bplus_tree" } else { algo_str };

    let algorithm = match IndexAlgorithm::from_str(algo_str) {
        Some(a) => a,
        None => {
            println!("Unknown algorithm '{}'. Defaulting to bplus_tree.", algo_str);
            IndexAlgorithm::BPlusTree
        }
    };

    print!("Clustered index? [y/N]: ");
    io::stdout().flush()?;
    let mut clustered_str = String::new();
    io::stdin().read_line(&mut clustered_str)?;
    let is_clustered = matches!(clustered_str.trim().to_lowercase().as_str(), "y" | "yes");

    print!("Include columns (comma-separated, optional): ");
    io::stdout().flush()?;
    let mut include_cols_str = String::new();
    io::stdin().read_line(&mut include_cols_str)?;
    let include_columns: Vec<String> = include_cols_str
        .trim()
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    let mut catalog = load_catalog();

    // Register in catalog first.
    let registered = create_index(
        &mut catalog,
        &db_name,
        &table_name,
        &index_name,
        &column_names,
        algorithm.clone(),
        is_clustered,
        include_columns,
    );
    if !registered {
        return Ok(());
    }

    // Build the index from existing table data.
    println!(
        "Building index '{}' on {}.{}.({})...",
        index_name,
        db_name,
        table_name,
        column_names.join(",")
    );
    match AnyIndex::build_from_table_columns(
        &catalog,
        &db_name,
        &table_name,
        &column_names,
        &algorithm,
    ) {
        Ok(idx) => {
            let path = if is_clustered {
                index_file_path(&db_name, &table_name, &index_name)
            } else {
                secondary_index_file_path(&db_name, &table_name, &index_name)
            };
            idx.save(&path)?;
            println!(
                "Index '{}' created ({} entries) saved to '{}'.",
                index_name,
                idx.entry_count(),
                path
            );

            if is_clustered {
                cluster_table_by_index(&catalog, &db_name, &table_name, &index_name)?;
                println!(
                    "Table '{}.{}' reordered by clustered index '{}'.",
                    db_name, table_name, index_name
                );
            }
        }
        Err(e) => {
            eprintln!("Failed to build index: {}", e);
            // Roll back the catalog registration.
            drop_index(&mut catalog, &db_name, &table_name, &index_name);
        }
    }

    Ok(())
}

/// Prompt the user for details and create a non-clustered secondary index.
pub fn create_secondary_index_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db_name = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Use 'Select Database' first.");
            return Ok(());
        }
    };

    print!("Table name: ");
    io::stdout().flush()?;
    let mut table_name = String::new();
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    print!("Column(s) to index (comma-separated): ");
    io::stdout().flush()?;
    let mut column_names_str = String::new();
    io::stdin().read_line(&mut column_names_str)?;
    let column_names: Vec<String> = column_names_str
        .trim()
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    print!("Index name: ");
    io::stdout().flush()?;
    let mut index_name = String::new();
    io::stdin().read_line(&mut index_name)?;
    let index_name = index_name.trim().to_string();

    println!("Algorithm options:");
    println!("  Hash-based : static_hash | chained_hash | extendible_hash | linear_hash");
    println!("  Tree-based : btree | bplus_tree | radix_tree | skip_list | lsm_tree");
    print!("Algorithm [bplus_tree]: ");
    io::stdout().flush()?;
    let mut algo_str = String::new();
    io::stdin().read_line(&mut algo_str)?;
    let algo_str = algo_str.trim();
    let algo_str = if algo_str.is_empty() { "bplus_tree" } else { algo_str };

    let algorithm = match IndexAlgorithm::from_str(algo_str) {
        Some(a) => a,
        None => {
            println!("Unknown algorithm '{}'. Defaulting to bplus_tree.", algo_str);
            IndexAlgorithm::BPlusTree
        }
    };

    let mut catalog = load_catalog();
    if let Err(e) = create_secondary_index(
        &mut catalog,
        &db_name,
        &table_name,
        &index_name,
        &column_names,
        algorithm.clone(),
    ) {
        println!("Failed to register secondary index: {}", e);
        return Ok(());
    }

    println!(
        "Building secondary index '{}' on {}.{}.({})...",
        index_name,
        db_name,
        table_name,
        column_names.join(",")
    );

    match AnyIndex::build_from_table_columns(
        &catalog,
        &db_name,
        &table_name,
        &column_names,
        &algorithm,
    ) {
        Ok(idx) => {
            let path = secondary_index_file_path(&db_name, &table_name, &index_name);
            idx.save(&path)?;
            println!(
                "Secondary index '{}' created ({} entries) saved to '{}'.",
                index_name,
                idx.entry_count(),
                path
            );
        }
        Err(e) => {
            eprintln!("Failed to build secondary index: {}", e);
            let _ = drop_secondary_index(&mut catalog, &db_name, &table_name, &index_name);
        }
    }

    Ok(())
}

// ─── Drop index ───────────────────────────────────────────────────────────────

/// Prompt for table and index name, then drop the index from catalog and disk.
pub fn drop_index_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db_name = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected.");
            return Ok(());
        }
    };

    print!("Table name: ");
    io::stdout().flush()?;
    let mut table_name = String::new();
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    print!("Index name: ");
    io::stdout().flush()?;
    let mut index_name = String::new();
    io::stdin().read_line(&mut index_name)?;
    let index_name = index_name.trim().to_string();

    let mut catalog = load_catalog();
    if drop_index(&mut catalog, &db_name, &table_name, &index_name) {
        // Also remove the index file from disk.
        let path = index_file_path(&db_name, &table_name, &index_name);
        let secondary_path = secondary_index_file_path(&db_name, &table_name, &index_name);
        if let Err(e) = fs::remove_file(&path) {
            // Not an error if the file never existed.
            eprintln!("Note: could not remove index file '{}': {}", path, e);
        }
        if secondary_path != path {
            let _ = fs::remove_file(&secondary_path);
        }
        println!("Index '{}' dropped from table '{}'.", index_name, table_name);
    }

    Ok(())
}

/// Prompt for table and index name, then drop only a secondary index.
pub fn drop_secondary_index_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db_name = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected.");
            return Ok(());
        }
    };

    print!("Table name: ");
    io::stdout().flush()?;
    let mut table_name = String::new();
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    print!("Secondary index name: ");
    io::stdout().flush()?;
    let mut index_name = String::new();
    io::stdin().read_line(&mut index_name)?;
    let index_name = index_name.trim().to_string();

    let mut catalog = load_catalog();
    if let Err(e) = drop_secondary_index(&mut catalog, &db_name, &table_name, &index_name) {
        println!("Failed to drop secondary index: {}", e);
        return Ok(());
    }

    let path = secondary_index_file_path(&db_name, &table_name, &index_name);
    if let Err(e) = fs::remove_file(&path) {
        eprintln!("Note: could not remove secondary index file '{}': {}", path, e);
    }

    println!(
        "Secondary index '{}' dropped from table '{}'.",
        index_name, table_name
    );
    Ok(())
}

// ─── List indexes ─────────────────────────────────────────────────────────────

/// Display all registered indices for a table.
pub fn list_indexes_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db_name = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected.");
            return Ok(());
        }
    };

    print!("Table name: ");
    io::stdout().flush()?;
    let mut table_name = String::new();
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    let catalog = load_catalog();
    match list_indexes(&catalog, &db_name, &table_name) {
        Some(indexes) if !indexes.is_empty() => {
            println!("\nIndexes on '{}.{}':", db_name, table_name);
            println!(
                "{:<20} {:<20} {:<12} {:<10} {}",
                "Index Name", "Columns", "Algorithm", "Clustered", "Include"
            );
            println!("{}", "-".repeat(90));
            for idx in indexes {
                println!(
                    "{:<20} {:<20} {:<12} {:<10} {}",
                    idx.index_name,
                    idx.column_name.join(","),
                    idx.algorithm.display_name(),
                    if idx.is_clustered { "yes" } else { "no" },
                    if idx.include_columns.is_empty() {
                        "-".to_string()
                    } else {
                        idx.include_columns.join(",")
                    }
                );
            }
            println!();
        }
        _ => {
            println!("No indexes found on '{}.{}'.", db_name, table_name);
        }
    }

    Ok(())
}

/// Display all non-clustered indices for a table.
pub fn list_secondary_indexes_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db_name = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected.");
            return Ok(());
        }
    };

    print!("Table name: ");
    io::stdout().flush()?;
    let mut table_name = String::new();
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    let catalog = load_catalog();
    let secondary = match list_secondary_indices(&catalog, &db_name, &table_name) {
        Ok(v) => v,
        Err(e) => {
            println!("Failed to list secondary indexes: {}", e);
            return Ok(());
        }
    };

    if secondary.is_empty() {
        println!("No secondary indexes found on '{}.{}'.", db_name, table_name);
        return Ok(());
    }

    println!("\nSecondary indexes on '{}.{}':", db_name, table_name);
    println!("{:<20} {:<24} {:<14}", "Index Name", "Columns", "Algorithm");
    println!("{}", "-".repeat(64));
    for idx in secondary {
        println!(
            "{:<20} {:<24} {:<14}",
            idx.index_name,
            idx.column_name.join(","),
            idx.algorithm.display_name(),
        );
    }
    println!();

    Ok(())
}

pub fn rebuild_secondary_index_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db_name = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected.");
            return Ok(());
        }
    };

    print!("Table name: ");
    io::stdout().flush()?;
    let mut table_name = String::new();
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    print!("Secondary index name: ");
    io::stdout().flush()?;
    let mut index_name = String::new();
    io::stdin().read_line(&mut index_name)?;
    let index_name = index_name.trim().to_string();

    let catalog = load_catalog();
    if let Err(e) = rebuild_secondary_index(&catalog, &db_name, &table_name, &index_name) {
        println!("Failed to rebuild secondary index: {}", e);
        return Ok(());
    }
    println!(
        "Secondary index '{}.{}.{}' rebuilt successfully.",
        db_name, table_name, index_name
    );
    Ok(())
}

pub fn validate_index_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db_name = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected.");
            return Ok(());
        }
    };

    print!("Table name: ");
    io::stdout().flush()?;
    let mut table_name = String::new();
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    print!("Index name: ");
    io::stdout().flush()?;
    let mut index_name = String::new();
    io::stdin().read_line(&mut index_name)?;
    let index_name = index_name.trim().to_string();

    let catalog = load_catalog();
    validate_index_consistency(&catalog, &db_name, &table_name, &index_name)?;
    println!("Index '{}.{}.{}' is consistent.", db_name, table_name, index_name);
    Ok(())
}

// ─── Search by index ──────────────────────────────────────────────────────────

/// Prompt for a point-lookup search value and display matching record IDs.
pub fn search_index_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db_name = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected.");
            return Ok(());
        }
    };

    print!("Table name: ");
    io::stdout().flush()?;
    let mut table_name = String::new();
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    print!("Index name: ");
    io::stdout().flush()?;
    let mut index_name = String::new();
    io::stdin().read_line(&mut index_name)?;
    let index_name = index_name.trim().to_string();

    print!("Search value: ");
    io::stdout().flush()?;
    let mut value_str = String::new();
    io::stdin().read_line(&mut value_str)?;
    let value_str = value_str.trim().to_string();

    let catalog = load_catalog();

    // Look up the index entry to know its column type and algorithm.
    let indexes = match list_indexes(&catalog, &db_name, &table_name) {
        Some(v) => v,
        None => {
            println!("Table '{}' not found.", table_name);
            return Ok(());
        }
    };
    let entry = match indexes.iter().find(|i| i.index_name == index_name) {
        Some(e) => e,
        None => {
            println!("Index '{}' not found.", index_name);
            return Ok(());
        }
    };

    let db = catalog.databases.get(&db_name).unwrap();
    let table = db.tables.get(&table_name).unwrap();

    let values = parse_index_input_values(&value_str);
    let search_key = match index_key_from_values(&table.columns, &entry.column_name, &values) {
        Ok(k) => k,
        Err(e) => {
            println!("Invalid search key: {}", e);
            return Ok(());
        }
    };

    let path = if entry.is_secondary() {
        secondary_index_file_path(&db_name, &table_name, &index_name)
    } else {
        index_file_path(&db_name, &table_name, &index_name)
    };
    let records = AnyIndex::search_on_disk(&path, &entry.algorithm, &search_key)?;

    if records.is_empty() {
        println!("No records found for key '{}'.", value_str);
    } else {
        println!("Found {} record(s):", records.len());
        for rid in &records {
            println!("  page={}, item={}", rid.page_no, rid.item_id);
        }
    }

    Ok(())
}

// ─── Range scan ───────────────────────────────────────────────────────────────

/// Prompt for a range [start, end] and perform a range scan on a tree index.
pub fn range_scan_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db_name = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected.");
            return Ok(());
        }
    };

    print!("Table name: ");
    io::stdout().flush()?;
    let mut table_name = String::new();
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    print!("Index name: ");
    io::stdout().flush()?;
    let mut index_name = String::new();
    io::stdin().read_line(&mut index_name)?;
    let index_name = index_name.trim().to_string();

    print!("Start value (inclusive): ");
    io::stdout().flush()?;
    let mut start_str = String::new();
    io::stdin().read_line(&mut start_str)?;
    let start_str = start_str.trim().to_string();

    print!("End value (inclusive): ");
    io::stdout().flush()?;
    let mut end_str = String::new();
    io::stdin().read_line(&mut end_str)?;
    let end_str = end_str.trim().to_string();

    let catalog = load_catalog();

    let indexes = match list_indexes(&catalog, &db_name, &table_name) {
        Some(v) => v,
        None => {
            println!("Table '{}' not found.", table_name);
            return Ok(());
        }
    };
    let entry = match indexes.iter().find(|i| i.index_name == index_name) {
        Some(e) => e,
        None => {
            println!("Index '{}' not found.", index_name);
            return Ok(());
        }
    };

    if entry.algorithm.is_hash() {
        println!(
            "Range scan is not supported by hash-based index '{}'.",
            index_name
        );
        return Ok(());
    }

    let db = catalog.databases.get(&db_name).unwrap();
    let table = db.tables.get(&table_name).unwrap();

    let start_values = parse_index_input_values(&start_str);
    let end_values = parse_index_input_values(&end_str);

    let start_key = match index_key_from_values(&table.columns, &entry.column_name, &start_values) {
        Ok(k) => k,
        Err(e) => {
            println!("Invalid range start key: {}", e);
            return Ok(());
        }
    };

    let end_key = match index_key_from_values(&table.columns, &entry.column_name, &end_values) {
        Ok(k) => k,
        Err(e) => {
            println!("Invalid range end key: {}", e);
            return Ok(());
        }
    };

    let path = if entry.is_secondary() {
        secondary_index_file_path(&db_name, &table_name, &index_name)
    } else {
        index_file_path(&db_name, &table_name, &index_name)
    };
    let index = AnyIndex::load(&path, &entry.algorithm)?;
    let records = index.range_scan(&start_key, &end_key)?;

    if records.is_empty() {
        println!("No records found in range ['{}', '{}'].", start_str, end_str);
    } else {
        println!("Found {} record(s) in range:", records.len());
        for rid in &records {
            println!("  page={}, item={}", rid.page_no, rid.item_id);
        }
    }

    Ok(())
}

// ─── Index scan ──────────────────────────────────────────────────────────────

/// Prompt for a point lookup and display the matching tuples.
pub fn index_scan_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db_name = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected.");
            return Ok(());
        }
    };

    print!("Table name: ");
    io::stdout().flush()?;
    let mut table_name = String::new();
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    print!("Index name: ");
    io::stdout().flush()?;
    let mut index_name = String::new();
    io::stdin().read_line(&mut index_name)?;
    let index_name = index_name.trim().to_string();

    print!("Search value: ");
    io::stdout().flush()?;
    let mut value_str = String::new();
    io::stdin().read_line(&mut value_str)?;
    let value_str = value_str.trim().to_string();

    let catalog = load_catalog();
    let indexes = match list_indexes(&catalog, &db_name, &table_name) {
        Some(v) => v,
        None => {
            println!("Table '{}' not found.", table_name);
            return Ok(());
        }
    };
    let entry = match indexes.iter().find(|i| i.index_name == index_name) {
        Some(e) => e,
        None => {
            println!("Index '{}' not found.", index_name);
            return Ok(());
        }
    };

    let db = catalog.databases.get(&db_name).unwrap();
    let table = db.tables.get(&table_name).unwrap();

    let values = parse_index_input_values(&value_str);
    let search_key = match index_key_from_values(&table.columns, &entry.column_name, &values) {
        Ok(k) => k,
        Err(e) => {
            println!("Invalid search key: {}", e);
            return Ok(());
        }
    };
    let tuples = index_scan(&catalog, &db_name, &table_name, &index_name, &search_key)?;

    if tuples.is_empty() {
        println!("No tuples found for key '{}'.", value_str);
        return Ok(());
    }

    println!("Found {} tuple(s):", tuples.len());
    for (i, tuple) in tuples.iter().enumerate() {
        let formatted = format_tuple(tuple, &table.columns);
        println!("  {}. {}", i + 1, formatted);
    }

    Ok(())
}

// ─── Helper ───────────────────────────────────────────────────────────────────

fn parse_index_input_values(input: &str) -> Vec<String> {
    input
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

fn format_tuple(tuple: &[u8], columns: &[Column]) -> String {
    let mut parts = Vec::new();
    let mut cursor = 0usize;

    for col in columns {
        match col.data_type.as_str() {
            "INT" => {
                if cursor + 4 <= tuple.len() {
                    let val = i32::from_le_bytes(tuple[cursor..cursor + 4].try_into().unwrap());
                    parts.push(format!("{}={}", col.name, val));
                } else {
                    parts.push(format!("{}=<out of bounds>", col.name));
                }
                cursor += 4;
            }
            "TEXT" => {
                if cursor + 10 <= tuple.len() {
                    let text_bytes = &tuple[cursor..cursor + 10];
                    let text = String::from_utf8_lossy(text_bytes).trim().to_string();
                    parts.push(format!("{}='{}'", col.name, text));
                } else {
                    parts.push(format!("{}=<out of bounds>", col.name));
                }
                cursor += 10;
            }
            "BOOL" | "BOOLEAN" => {
                if cursor + 1 <= tuple.len() {
                    let val = tuple[cursor] != 0;
                    parts.push(format!("{}={}", col.name, if val { "true" } else { "false" }));
                } else {
                    parts.push(format!("{}=<out of bounds>", col.name));
                }
                cursor += 1;
            }
            _ => {
                parts.push(format!("{}=<unsupported>", col.name));
            }
        }
    }

    parts.join(" ")
}
