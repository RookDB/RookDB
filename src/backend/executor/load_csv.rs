use std::fs::File;
use std::io::{self, BufRead, BufReader};

use crate::catalog::types::Catalog;
use crate::heap::insert_tuple;
use crate::backend::types_validator::DataType;

/// Load CSV file with full validation and error handling.
///
/// Before loading any data:
/// 1. Validates that all column data types are supported
/// 2. Checks that CSV file is readable
/// 3. Performs row-by-row validation and type checking
///
/// Returns count of successfully inserted rows on success.
pub fn load_csv(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    csv_path: &str,
) -> io::Result<u32> {
    println!("\n[CSV LOADER] Starting CSV load operation");
    println!("[CSV LOADER] Database: '{}', Table: '{}', CSV: '{}'", db_name, table_name, csv_path);

    // --- 1. Fetch table schema from catalog ---
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

    let columns = &table.columns;
    if columns.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Table has no columns",
        ));
    }

    println!("[CSV LOADER] Found table with {} columns", columns.len());

    // --- 2. VALIDATE ALL DATA TYPES BEFORE LOADING ---
    println!("[CSV LOADER] Validating schema data types...");
    for (idx, col) in columns.iter().enumerate() {
        println!("[CSV LOADER]   Column {}: '{}' → {}", idx + 1, col.name, col.data_type);
        
        match DataType::from_str(&col.data_type) {
            Ok(dt) => {
                println!("[CSV LOADER]   Supported data type: {:?}", dt);
            }
            Err(e) => {
                println!("[CSV LOADER]   VALIDATION FAILED: {}", e);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Column '{}' has unsupported type '{}'. Supported types: INT, TEXT",
                        col.name, col.data_type),
                ));
            }
        }
    }
    println!("[CSV LOADER] All data types validated successfully");

    // --- 3. Open and read the CSV file ---
    println!("[CSV LOADER] Opening CSV file: '{}'", csv_path);
    let csv_file = match File::open(csv_path) {
        Ok(f) => {
            println!("[CSV LOADER] CSV file opened successfully");
            f
        }
        Err(e) => {
            println!("[CSV LOADER] Failed to open CSV file: {}", e);
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Failed to open CSV file '{}': {}", csv_path, e),
            ));
        }
    };

    let reader = BufReader::new(csv_file);
    let mut lines = reader.lines();

    // Skip header line
    println!("[CSV LOADER] Reading CSV header...");
    if let Some(Ok(header)) = lines.next() {
        println!("[CSV LOADER] Header: {}", header);
    }

    // --- 4. Iterate through rows with detailed validation ---
    let mut inserted = 0u32;
    let mut skipped = 0u32;
    let mut failed = 0u32;

    for (line_num, line) in lines.enumerate() {
        let line_idx = line_num + 2; // +2 because we skipped header and start from line 2
        
        let row = match line {
            Ok(r) => r,
            Err(e) => {
                println!("[CSV LOADER] Line {}: Error reading line: {}", line_idx, e);
                failed += 1;
                continue;
            }
        };

        if row.trim().is_empty() {
            println!("[CSV LOADER] Line {}: Skipping empty row", line_idx);
            skipped += 1;
            continue;
        }

        // Split CSV fields by comma
        let values: Vec<&str> = row.split(',').map(|v| v.trim()).collect();

        // Validate number of columns
        if values.len() != columns.len() {
            println!(
                "[CSV LOADER] Line {}: Expected {} columns, found {}. Skipping row.",
                line_idx,
                columns.len(),
                values.len()
            );
            skipped += 1;
            continue;
        }

        // --- 5. Validate each value before serialization ---
        let mut validation_passed = true;
        for (col_idx, (val, col)) in values.iter().zip(columns.iter()).enumerate() {
            match DataType::from_str(&col.data_type) {
                Ok(data_type) => {
                    if let Err(validation_err) = data_type.validate_value(val) {
                        println!(
                            "[CSV LOADER] Line {}, Column {} ('{}'): {} Value: '{}'",
                            line_idx, col_idx + 1, col.name, validation_err, val
                        );
                        validation_passed = false;
                        break;
                    }
                }
                Err(_) => {
                    validation_passed = false;
                    break;
                }
            }
        }

        if !validation_passed {
            println!("[CSV LOADER] Line {}: Validation failed. Skipping row.", line_idx);
            failed += 1;
            continue;
        }

        // --- 6. Serialize row based on schema ---
        let mut tuple_bytes: Vec<u8> = Vec::new();

        for (val, col) in values.iter().zip(columns.iter()) {
            match DataType::from_str(&col.data_type) {
                Ok(data_type) => {
                    match data_type.serialize_value(val) {
                        Ok(bytes) => {
                            tuple_bytes.extend_from_slice(&bytes);
                        }
                        Err(e) => {
                            println!("[CSV LOADER] Line {}: Serialization error for column '{}': {}", 
                                line_idx, col.name, e);
                            validation_passed = false;
                            break;
                        }
                    }
                }
                Err(e) => {
                    println!("[CSV LOADER] Line {}: Invalid data type for column '{}': {}", 
                        line_idx, col.name, e);
                    validation_passed = false;
                    break;
                }
            }
        }

        if !validation_passed {
            failed += 1;
            continue;
        }

        // --- 7. Insert tuple into page system ---
        if let Err(e) = insert_tuple(file, &tuple_bytes) {
            println!("[CSV LOADER] Line {}: Failed to insert row: {}", line_idx, e);
            failed += 1;
        } else {
            inserted += 1;
            if inserted % 100 == 0 {
                println!("[CSV LOADER] Inserted {} rows so far...", inserted);
            }
        }
    }

    println!("\n[CSV LOADER] ═══════════════════════════════════");
    println!("[CSV LOADER] CSV Load Summary:");
    println!("[CSV LOADER] Successfully inserted: {}", inserted);
    println!("[CSV LOADER] Skipped (formatting): {}", skipped);
    println!("[CSV LOADER] Failed (validation/insert): {}", failed);
    println!("[CSV LOADER] Total rows processed: {}", inserted + skipped + failed);
    println!("[CSV LOADER] ═══════════════════════════════════\n");

    if inserted == 0 && (skipped > 0 || failed > 0) {
        println!("WARNING: No rows were inserted. Please check your CSV file format and data types.");
    }

    Ok(inserted)
}

/// Insert a single tuple directly (useful for manual data entry)
pub fn insert_single_tuple(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    values: &[&str],
) -> io::Result<bool> {
    println!("\n[TUPLE INSERT] Starting single tuple insertion");

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

    let columns = &table.columns;

    if values.len() != columns.len() {
        println!("[TUPLE INSERT] Expected {} values, got {}", columns.len(), values.len());
        return Ok(false);
    }

    // Validate all values
    for (val, col) in values.iter().zip(columns.iter()) {
        match DataType::from_str(&col.data_type) {
            Ok(data_type) => {
                if let Err(e) = data_type.validate_value(val) {
                    println!("[TUPLE INSERT] Column '{}': {}", col.name, e);
                    return Ok(false);
                }
            }
            Err(e) => {
                println!("[TUPLE INSERT] Column '{}' has invalid type: {}", col.name, e);
                return Ok(false);
            }
        }
    }

    // Serialize tuple
    let mut tuple_bytes: Vec<u8> = Vec::new();
    for (val, col) in values.iter().zip(columns.iter()) {
        match DataType::from_str(&col.data_type) {
            Ok(data_type) => {
                match data_type.serialize_value(val) {
                    Ok(bytes) => tuple_bytes.extend_from_slice(&bytes),
                    Err(e) => {
                        println!("[TUPLE INSERT] Failed to serialize column '{}': {}", col.name, e);
                        return Ok(false);
                    }
                }
            }
            Err(e) => {
                println!("[TUPLE INSERT] Invalid type for column '{}': {}", col.name, e);
                return Ok(false);
            }
        }
    }

    // Insert tuple
    match insert_tuple(file, &tuple_bytes) {
        Ok(_) => {
            println!("[TUPLE INSERT] Successfully inserted tuple");
            Ok(true)
        }
        Err(e) => {
            println!("[TUPLE INSERT] Failed to insert tuple: {}", e);
            Ok(false)
        }
    }
}

