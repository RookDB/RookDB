use std::fs::OpenOptions;
use std::io::{self, Write};

use storage_manager::backend::storage::literal_parser::parse_value_literal;
use storage_manager::backend::storage::row_layout::{ToastPointer, VarFieldEntry, TupleHeader};
use storage_manager::backend::storage::toast::ToastManager;
use storage_manager::backend::storage::tuple_codec::TupleCodec;
use storage_manager::backend::storage::database_logger;
use storage_manager::buffer_manager::BufferManager;
use storage_manager::catalog::load_catalog;
use storage_manager::catalog::data_type::DataType;
use storage_manager::executor::{show_tuples, scan_tuples_indexed};
use storage_manager::heap::{insert_tuple, delete_tuple, update_tuple};
use storage_manager::table::page_count;

pub fn load_csv_cmd(
    buffer_manager: &mut BufferManager,
    current_db: &Option<String>,
) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first");
            database_logger::log_error("load_csv_cmd", "no database selected");
            return Ok(());
        }
    };

    let mut table = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table)?;
    let table = table.trim();

    let mut csv_path = String::new();
    print!("Enter CSV path: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut csv_path)?;
    let csv_path = csv_path.trim();

    let catalog = load_catalog();
    
    match buffer_manager.load_csv_to_buffer(&catalog, &db, table, csv_path) {
        Ok(_) => {
            let path = format!("database/base/{}/{}.dat", db, table);
            match OpenOptions::new().read(true).write(true).open(&path) {
                Ok(mut file) => {
                    match page_count(&mut file) {
                        Ok(page_count_val) => {
                            println!("Page Count: {}", page_count_val);
                            // Log the CSV LOAD operation
                            // Note: We log total pages instead of row count for now
                            database_logger::log_csv_load(&db, table, csv_path, page_count_val);
                        }
                        Err(e) => {
                            database_logger::log_csv_load_failed(&db, table, csv_path, &format!("failed to read page count: {}", e));
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    database_logger::log_csv_load_failed(&db, table, csv_path, &format!("cannot open table file: {}", e));
                    return Err(e);
                }
            }
        }
        Err(e) => {
            database_logger::log_csv_load_failed(&db, table, csv_path, &e.to_string());
            return Err(e);
        }
    }

    Ok(())
}

pub fn show_tuples_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first");
            database_logger::log_error("show_tuples_cmd", "no database selected");
            return Ok(());
        }
    };

    let mut table = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table)?;
    let table = table.trim();

    let path = format!("database/base/{}/{}.dat", db, table);
    match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(mut file) => {
            let catalog = load_catalog();
            match show_tuples(&catalog, &db, table, &mut file) {
                Ok(_) => {
                    // Note: For detailed metrics, we'd need to track tuple count from show_tuples
                    database_logger::log_scan_tuples(&db, table, 0, 0);
                }
                Err(e) => {
                    database_logger::log_scan_tuples_failed(&db, table, &e.to_string());
                    return Err(e);
                }
            }
        }
        Err(e) => {
            database_logger::log_error(&format!("show_tuples_cmd: {}", table), &e.to_string());
            return Err(e);
        }
    }

    Ok(())
}

/// Interactive command to insert a single tuple into a table.
///
/// Reads the table schema from the catalog, prompts the user for each
/// column value, parses with `literal_parser`, encodes via `TupleCodec`
/// (with TOAST support for oversized BLOBs/ARRAYs), and writes the
/// resulting tuple bytes through the heap page layer to disk.
pub fn insert_tuple_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first.");
            return Ok(());
        }
    };

    // 1. Prompt for table name
    let mut table_name = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    // 2. Load catalog and look up the table schema
    let catalog = load_catalog();
    let database = match catalog.databases.get(&db) {
        Some(d) => d,
        None => {
            println!("Database '{}' not found in catalog.", db);
            database_logger::log_error(&format!("insert_tuple_cmd: {}", table_name), "database not found");
            return Ok(());
        }
    };
    let table = match database.tables.get(&table_name) {
        Some(t) => t,
        None => {
            println!("Table '{}' not found in database '{}'.", table_name, db);
            database_logger::log_error(&format!("insert_tuple_cmd: {}", table_name), "table not found");
            return Ok(());
        }
    };
    let columns = &table.columns;
    if columns.is_empty() {
        println!("Table '{}' has no columns.", table_name);
        database_logger::log_error(&format!("insert_tuple_cmd: {}", table_name), "table has no columns");
        return Ok(());
    }

    // 3. Build typed schema
    let schema: Vec<(String, DataType)> = columns
        .iter()
        .map(|col| {
            let data_type = col
                .data_type
                .as_ref()
                .and_then(|type_str| DataType::parse(type_str).ok())
                .unwrap_or(DataType::Text);
            (col.name.clone(), data_type)
        })
        .collect();

    // 4. Display column info and prompt for each value
    println!("\n--- Insert Tuple into '{}.{}' ---", db, table_name);
    println!("Columns:");
    for (name, dt) in &schema {
        println!("  {}: {}", name, dt.to_string());
    }
    println!("\nInput hints:");
    println!("  INT      → e.g. 42");
    println!("  BOOLEAN  → true / false / t / f / 1 / 0");
    println!("  TEXT     → must be quoted: \"hello\" or 'hello'");
    println!("  BLOB     → 0xDEADBEEF  or  @/path/to/file.bin");
    println!("  ARRAY<T> → [val1, val2, ...]   e.g. [1,2,3]");
    println!("  NULL     → type NULL for any column\n");

    let mut parsed_values = Vec::new();
    for (name, data_type) in &schema {
        loop {
            let mut input = String::new();
            print!("  {} ({}): ", name, data_type.to_string());
            io::stdout().flush()?;
            io::stdin().read_line(&mut input)?;
            let input = input.trim();

            match parse_value_literal(input, data_type) {
                Ok(value) => {
                    parsed_values.push(value);
                    break;
                }
                Err(e) => {
                    println!("    ✗ Invalid input: {}. Please try again.", e);
                }
            }
        }
    }

    // 5. Encode tuple via TupleCodec (with TOAST for oversized values)
    // Try to load existing TOAST state from disk, or create fresh
    let toast_path = format!("database/base/{}/{}.toast", db, table_name);
    let mut toast_manager = ToastManager::load_from_disk(&toast_path)
        .unwrap_or_else(|_| ToastManager::new());

    let tuple_bytes = match TupleCodec::encode_tuple(&parsed_values, &schema, &mut toast_manager) {
        Ok(bytes) => bytes,
        Err(e) => {
            println!("Failed to encode tuple: {}", e);
            database_logger::log_insert_failed(&db, &table_name, &format!("encoding failed: {}", e));
            return Ok(());
        }
    };

    // 6. Write tuple to heap page on disk
    let table_path = format!("database/base/{}/{}.dat", db, table_name);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&table_path)
        .map_err(|e| {
            let error_msg = format!("Cannot open table file '{}': {}", table_path, e);
            io::Error::new(e.kind(), error_msg)
        })?;

    if let Err(e) = insert_tuple(&mut file, &tuple_bytes) {
        database_logger::log_insert_failed(&db, &table_name, &format!("insertion failed: {}", e));
        return Err(e);
    }

    // 7. Persist TOAST chunks (even if none were added, keeps state consistent)
    if let Err(e) = toast_manager.save_to_disk(&toast_path) {
        eprintln!("Warning: Failed to save TOAST data: {}", e);
    }

    // 8. Confirmation
    let pages = page_count(&mut file)?;
    println!("\n✓ Tuple inserted into '{}.{}' ({} bytes, {} pages in table).",
        db, table_name, tuple_bytes.len(), pages);
    
    // Log the INSERT operation
    database_logger::log_insert(&db, &table_name, tuple_bytes.len(), pages);

    Ok(())
}

// ---------------------------------------------------------------------------
// Shared helper: print indexed tuple list and return user selection
// ---------------------------------------------------------------------------

/// Display all tuples with an index and return (page_num, slot_index, values)
/// for the one selected by the user.
fn prompt_tuple_selection(
    db: &str,
    table_name: &str,
) -> io::Result<Option<(u32, u32, Vec<storage_manager::catalog::data_type::Value>, Vec<(String, DataType)>)>> {
    // Open table file
    let table_path = format!("database/base/{}/{}.dat", db, table_name);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&table_path)
        .map_err(|e| io::Error::new(e.kind(), format!("Cannot open table '{}': {}", table_path, e)))?;

    let catalog = load_catalog();
    let database = match catalog.databases.get(db) {
        Some(d) => d,
        None => {
            println!("Database '{}' not found in catalog.", db);
            return Ok(None);
        }
    };
    let table = match database.tables.get(table_name) {
        Some(t) => t,
        None => {
            println!("Table '{}' not found.", table_name);
            return Ok(None);
        }
    };

    // Build schema
    let schema: Vec<(String, DataType)> = table.columns.iter().map(|col| {
        let dt = col.data_type.as_ref()
            .and_then(|s| DataType::parse(s).ok())
            .unwrap_or(DataType::Text);
        (col.name.clone(), dt)
    }).collect();

    // Scan all live tuples with their page/slot location
    let tuples = scan_tuples_indexed(&catalog, db, table_name, &mut file)?;

    if tuples.is_empty() {
        println!("No tuples found in '{}.{}'.", db, table_name);
        return Ok(None);
    }

    // Display
    println!("\n--- Tuples in '{}.{}' ---", db, table_name);
    for (i, (page_num, slot_index, values)) in tuples.iter().enumerate() {
        print!("[{}] (page={}, slot={})  ", i + 1, page_num, slot_index);
        for ((col_name, _), value) in schema.iter().zip(values.iter()) {
            print!("{}={} ", col_name, format_value_short(value));
        }
        println!();
    }

    // Prompt
    let mut choice = String::new();
    print!("\nEnter tuple number to select (1-{}): ", tuples.len());
    io::stdout().flush()?;
    io::stdin().read_line(&mut choice)?;
    let idx: usize = match choice.trim().parse::<usize>() {
        Ok(n) if n >= 1 && n <= tuples.len() => n - 1,
        _ => {
            println!("Invalid selection.");
            return Ok(None);
        }
    };

    let (page_num, slot_index, values) = tuples.into_iter().nth(idx).unwrap();
    Ok(Some((page_num, slot_index, values, schema)))
}

/// Short value display for the selection list
fn format_value_short(value: &storage_manager::catalog::data_type::Value) -> String {
    use storage_manager::catalog::data_type::Value;
    match value {
        Value::Null => "NULL".to_string(),
        Value::Int32(n) => n.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Text(s) => format!("'{}'", s),
        Value::Blob(b) => format!("<blob:{} bytes>", b.len()),
        Value::Array(elems) => format!("<array:{} elements>", elems.len()),
    }
}

/// Collect TOAST value_ids referenced by a raw tuple's variable-length fields.
///
/// This is **schema-aware**: it mirrors the layout produced by `TupleCodec::encode_tuple`:
///
/// ```text
/// [ TupleHeader | null_bitmap | var_dir[] | fixed_region | var_payload ]
/// ```
///
/// `VarFieldEntry.offset` values are relative to `var_payload`, which starts
/// *after* the fixed region. Without the schema we cannot compute `fixed_size`,
/// so this function requires the schema to be passed in.
fn collect_toast_ids(tuple_bytes: &[u8], schema: &[(String, DataType)]) -> Vec<u64> {
    let mut ids = Vec::new();

    if tuple_bytes.len() < TupleHeader::size() {
        return ids;
    }

    // 1. Parse header
    let header = match TupleHeader::from_bytes(&tuple_bytes[0..TupleHeader::size()]) {
        Ok(h) => h,
        Err(_) => return ids,
    };

    let null_bitmap_bytes = header.null_bitmap_bytes as usize;
    let var_field_count = header.var_field_count as usize;
    let mut cursor = TupleHeader::size() + null_bitmap_bytes;

    // 2. Parse variable-field directory
    let var_dir_size = var_field_count * VarFieldEntry::size();
    if cursor + var_dir_size > tuple_bytes.len() {
        return ids;
    }

    let mut var_entries = Vec::new();
    for i in 0..var_field_count {
        let start = cursor + i * VarFieldEntry::size();
        let end = start + VarFieldEntry::size();
        if let Ok(entry) = VarFieldEntry::from_bytes(&tuple_bytes[start..end]) {
            var_entries.push(entry);
        }
    }
    cursor += var_dir_size;

    // 3. Compute fixed-region size from schema (same logic as decode_tuple_internal)
    let fixed_size: usize = schema
        .iter()
        .filter(|(_, dt)| !dt.is_variable_length())
        .filter_map(|(_, dt)| dt.fixed_size())
        .sum();

    // 4. Advance cursor past the fixed region → cursor now points to var_payload
    if cursor + fixed_size > tuple_bytes.len() {
        return ids;
    }
    cursor += fixed_size;

    let var_payload = &tuple_bytes[cursor..];

    // 5. For every TOAST-flagged var entry, parse the ToastPointer from var_payload
    for entry in &var_entries {
        if !entry.is_toast() {
            continue;
        }
        let start = entry.offset as usize;
        let end = start + entry.length as usize;
        if end <= var_payload.len() {
            if let Ok(ptr) = ToastPointer::from_bytes(&var_payload[start..end]) {
                ids.push(ptr.value_id);
            }
        }
    }

    ids
}

// ---------------------------------------------------------------------------
// Delete Tuple Command
// ---------------------------------------------------------------------------

/// Interactive command: delete a single tuple from a table.
/// Finds and cleans up any TOAST-backed BLOB/ARRAY values automatically.
pub fn delete_tuple_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first.");
            database_logger::log_error("delete_tuple_cmd", "no database selected");
            return Ok(());
        }
    };

    let mut table_name = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    // Select a tuple
    let selection = match prompt_tuple_selection(&db, &table_name)? {
        Some(s) => s,
        None => return Ok(()),
    };
    let (page_num, slot_index, _values, schema) = selection;

    // Open file for read+write
    let table_path = format!("database/base/{}/{}.dat", db, table_name);
    let mut file = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(&table_path)
    {
        Ok(f) => f,
        Err(e) => {
            let error_msg = format!("Cannot open table '{}': {}", table_path, e);
            database_logger::log_delete_failed(&db, &table_name, &error_msg);
            return Err(io::Error::new(e.kind(), error_msg));
        }
    };

    // Delete from heap — returns old tuple bytes
    let old_bytes = match delete_tuple(&mut file, page_num, slot_index) {
        Ok(bytes) => bytes,
        Err(e) => {
            let error_msg = format!("Failed to delete tuple: {}", e);
            database_logger::log_delete_failed(&db, &table_name, &error_msg);
            return Err(io::Error::new(io::ErrorKind::Other, error_msg));
        }
    };

    // TOAST cleanup: find and free any out-of-line chunks
    let toast_path = format!("database/base/{}/{}.toast", db, table_name);
    let mut toast_manager = ToastManager::load_from_disk(&toast_path)
        .unwrap_or_else(|_| ToastManager::new());

    let toast_ids = collect_toast_ids(&old_bytes, &schema);
    for value_id in &toast_ids {
        if let Err(e) = toast_manager.delete_value(*value_id) {
            eprintln!("Warning: TOAST cleanup failed for value_id={}: {}", value_id, e);
            database_logger::log_error(&format!("delete_tuple_cmd: TOAST cleanup value_id={}", value_id), &e.to_string());
        }
    }
        
    if !toast_ids.is_empty() {
        if let Err(e) = toast_manager.save_to_disk(&toast_path) {
            eprintln!("Warning: Failed to persist TOAST after delete: {}", e);
            database_logger::log_error("delete_tuple_cmd: TOAST persistence", &e.to_string());
        }
        println!("  (Freed {} TOAST value(s))", toast_ids.len());
    }

    println!("\n✓ Tuple deleted from '{}.{}' (page={}, slot={}).",
        db, table_name, page_num, slot_index);
    
    // Log the DELETE operation
    database_logger::log_delete(&db, &table_name, page_num, slot_index, toast_ids.len());

    Ok(())
}

// ---------------------------------------------------------------------------
// Update Tuple Command
// ---------------------------------------------------------------------------

/// Interactive command: update a single tuple in a table.
/// Old TOAST-backed BLOB/ARRAY values are freed; new oversized values are re-TOASTed.
pub fn update_tuple_cmd(current_db: &Option<String>) -> io::Result<()> {
    let db = match current_db {
        Some(db) => db.clone(),
        None => {
            println!("No database selected. Please select a database first.");
            database_logger::log_error("update_tuple_cmd", "no database selected");
            return Ok(());
        }
    };

    let mut table_name = String::new();
    print!("Enter table name: ");
    io::stdout().flush()?;
    io::stdin().read_line(&mut table_name)?;
    let table_name = table_name.trim().to_string();

    // Select a tuple
    let selection = match prompt_tuple_selection(&db, &table_name)? {
        Some(s) => s,
        None => return Ok(()),
    };
    let (page_num, slot_index, current_values, schema) = selection;

    // Prompt user for new values (any column)
    println!("\n--- Update Tuple (press Enter to keep current value) ---");
    println!("Input hints:");
    println!("  INT      → e.g. 42");
    println!("  BOOLEAN  → true / false");
    println!("  TEXT     → must be quoted: \"hello\" or 'hello'");
    println!("  BLOB     → 0xDEADBEEF  or  @/path/to/file.bin");
    println!("  ARRAY<T> → [val1, val2, ...]");
    println!("  NULL     → type NULL for any column\n");

    let mut new_values = Vec::new();
    for (i, (col_name, data_type)) in schema.iter().enumerate() {
        let current = format_value_short(&current_values[i]);
        loop {
            let mut input = String::new();
            print!("  {} ({}) [{}]: ", col_name, data_type.to_string(), current);
            io::stdout().flush()?;
            io::stdin().read_line(&mut input)?;
            let input = input.trim();

            if input.is_empty() {
                // Keep current value
                new_values.push(current_values[i].clone());
                break;
            }

            match parse_value_literal(input, data_type) {
                Ok(value) => {
                    new_values.push(value);
                    break;
                }
                Err(e) => {
                    println!("    ✗ Invalid input: {}. Please try again.", e);
                }
            }
        }
    }

    // Load TOAST manager
    let toast_path = format!("database/base/{}/{}.toast", db, table_name);
    let mut toast_manager = ToastManager::load_from_disk(&toast_path)
        .unwrap_or_else(|_| ToastManager::new());

    // Encode the new tuple
    let new_tuple_bytes = match TupleCodec::encode_tuple(&new_values, &schema, &mut toast_manager) {
        Ok(bytes) => bytes,
        Err(e) => {
            println!("Failed to encode new tuple: {}", e);
            database_logger::log_update_failed(&db, &table_name, &format!("encoding failed: {}", e));
            return Ok(());
        }
    };

    // Open table file
    let table_path = format!("database/base/{}/{}.dat", db, table_name);
    let mut file = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(&table_path)
    {
        Ok(f) => f,
        Err(e) => {
            let error_msg = format!("Cannot open table '{}': {}", table_path, e);
            database_logger::log_update_failed(&db, &table_name, &error_msg);
            return Err(io::Error::new(e.kind(), error_msg));
        }
    };

    // Update heap: delete old + insert new, get old bytes back for TOAST cleanup
    let old_bytes = match update_tuple(&mut file, page_num, slot_index, &new_tuple_bytes) {
        Ok(bytes) => bytes,
        Err(e) => {
            let error_msg = format!("Failed to update tuple: {}", e);
            database_logger::log_update_failed(&db, &table_name, &error_msg);
            return Err(io::Error::new(io::ErrorKind::Other, error_msg));
        }
    };

    // TOAST cleanup for old values
    let old_toast_ids = collect_toast_ids(&old_bytes, &schema);
    for value_id in &old_toast_ids {
        // Skip IDs that were just re-created by encode_tuple for the new tuple
        if let Err(e) = toast_manager.delete_value(*value_id) {
            eprintln!("Warning: TOAST cleanup for old value_id={} failed: {}", value_id, e);
            database_logger::log_error(&format!("update_tuple_cmd: TOAST cleanup value_id={}", value_id), &e.to_string());
        }
    }

    // Persist TOAST state
    if let Err(e) = toast_manager.save_to_disk(&toast_path) {
        eprintln!("Warning: Failed to persist TOAST after update: {}", e);
        database_logger::log_error("update_tuple_cmd: TOAST persistence", &e.to_string());
    }

    let pages = page_count(&mut file)?;
    println!("\n✓ Tuple updated in '{}.{}' ({} bytes encoded, {} pages in table).",
        db, table_name, new_tuple_bytes.len(), pages);
    if !old_toast_ids.is_empty() {
        println!("  (Freed {} old TOAST value(s))", old_toast_ids.len());
    }
    
    // Log the UPDATE operation
    database_logger::log_update(&db, &table_name, page_num, slot_index, 
                                old_bytes.len(), new_tuple_bytes.len(), old_toast_ids.len());

    Ok(())
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use storage_manager::backend::catalog::data_type::{DataType, Value};
    use storage_manager::backend::storage::toast::{ToastManager, TOAST_THRESHOLD};
    use storage_manager::backend::storage::tuple_codec::TupleCodec;

    /// Verify that `collect_toast_ids` extracts the correct TOAST value_ids from a tuple
    /// that has **interleaved fixed-length and TOAST-backed variable-length columns**.
    ///
    /// Schema: [id:INT32, data1:BLOB, flag:BOOL, data2:BLOB]
    ///   - `id` and `flag` are fixed → fixed_region is non-zero (5 bytes: 4+1)
    ///   - `data1` and `data2` exceed TOAST_THRESHOLD → stored out-of-line
    ///
    /// The old schema-blind implementation applied var entry offsets to the wrong base
    /// (fixed_region was included in its "remaining" slice), producing garbage value_ids.
    /// The new schema-aware version correctly skips the fixed region first.
    #[test]
    fn test_collect_toast_ids_schema_aware() {
        let schema: Vec<(String, DataType)> = vec![
            ("id".to_string(),    DataType::Int32),         // fixed: 4 bytes
            ("data1".to_string(), DataType::Blob),          // variable + TOAST
            ("flag".to_string(),  DataType::Boolean),       // fixed: 1 byte
            ("data2".to_string(), DataType::Blob),          // variable + TOAST
        ];

        // Two large BLOBs that will be stored out-of-line by TupleCodec
        let blob1 = vec![0xAA_u8; TOAST_THRESHOLD + 1000]; // ~9 KB
        let blob2 = vec![0xBB_u8; TOAST_THRESHOLD + 2000]; // ~10 KB

        let values = vec![
            Value::Int32(42),
            Value::Blob(blob1),
            Value::Boolean(true),
            Value::Blob(blob2),
        ];

        let mut toast_manager = ToastManager::new();
        let encoded = TupleCodec::encode_tuple(&values, &schema, &mut toast_manager)
            .expect("encode_tuple should succeed");

        // The toast manager should hold exactly 2 out-of-line values
        assert_eq!(toast_manager.value_count(), 2, "expected 2 TOASTed values");

        // Collect their IDs directly from the manager (ground truth)
        let expected_ids: std::collections::HashSet<u64> = (1..=2).collect(); // next_value_id started at 1

        // Now extract them from the encoded tuple bytes using the schema-aware function
        let extracted = collect_toast_ids(&encoded, &schema);
        assert_eq!(
            extracted.len(), 2,
            "should extract exactly 2 TOAST value_ids, got {:?}", extracted
        );

        let extracted_set: std::collections::HashSet<u64> = extracted.into_iter().collect();
        assert_eq!(
            extracted_set, expected_ids,
            "extracted IDs should match the real TOAST value_ids"
        );

        // Verify that delete_value actually works with these IDs (no 'not found' error)
        for id in &expected_ids {
            assert!(
                toast_manager.delete_value(*id).is_ok(),
                "delete_value({}) should succeed", id
            );
        }
        assert_eq!(toast_manager.value_count(), 0, "all TOAST values should be freed");
    }
}
