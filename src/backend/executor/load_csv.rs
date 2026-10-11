use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;

use crate::backend::heap::HeapManager;
use crate::catalog::types::Catalog;
use crate::types::DataValue;
use crate::types::row::serialize_nullable_row;
use crate::types::validation::validate_value;

/// Load CSV file with full validation and error handling using HeapManager.
///
/// Before loading any data:
/// 1. Validates that all column data types are supported
/// 2. Checks that CSV file is readable
/// 3. Performs row-by-row validation and type checking
/// 4. Uses HeapManager for FSM-aware insertion
///
/// Returns count of successfully inserted rows on success.
pub fn load_csv(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    csv_path: &str,
) -> io::Result<u32> {
    log::info!(" Starting CSV load operation");
    log::info!(
        " Database: '{}', Table: '{}', CSV: '{}'",
        db_name,
        table_name,
        csv_path
    );

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

    log::info!(" Found table with {} columns", columns.len());

    // --- 2. VALIDATE ALL DATA TYPES BEFORE LOADING ---
    log::info!(" Validating schema data types...");
    for (idx, col) in columns.iter().enumerate() {
        log::info!("   Column {}: '{}' → {}", idx + 1, col.name, col.data_type);
        log::info!("   Supported data type: {:?}", col.data_type);
    }
    log::info!(" All data types validated successfully");

    // --- 3. Open and read the CSV file ---
    log::info!(" Opening CSV file: '{}'", csv_path);
    let csv_file = match File::open(csv_path) {
        Ok(f) => {
            log::info!(" CSV file opened successfully");
            f
        }
        Err(e) => {
            log::info!(" Failed to open CSV file: {}", e);
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Failed to open CSV file '{}': {}", csv_path, e),
            ));
        }
    };

    let reader = BufReader::new(csv_file);
    let mut lines = reader.lines();

    // Skip header line if present
    log::info!(" Reading CSV header...");
    let mut first_row: Option<String> = None;
    for line_res in lines.by_ref() {
        let line = match line_res {
            Ok(l) => l,
            Err(e) => {
                log::error!("Error reading CSV line: {}", e);
                return Err(e);
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let parsed = parse_csv_line(&line);
        let is_header = parsed.len() == columns.len()
            && parsed
                .iter()
                .zip(columns.iter())
                .all(|(v, col)| col.name.eq_ignore_ascii_case(v.trim().trim_matches('"')));
        if is_header {
            log::info!(" Header detected: {}", line);
        } else {
            first_row = Some(line);
        }
        break;
    }

    // --- 3a. Open HeapManager for FSM-aware insertion ---
    let table_path = PathBuf::from(format!("database/base/{}/{}.dat", db_name, table_name));
    let mut heap_manager = match HeapManager::open(table_path.clone()) {
        Ok(hm) => {
            log::info!(" Opened HeapManager (FSM will be created/updated)");
            hm
        }
        Err(e) => {
            log::info!(" CRITICAL: Failed to open HeapManager: {}", e);
            return Err(e);
        }
    };

    let has_header = first_row.is_none();
    let mut line_idx = if has_header { 1 } else { 0 };
    let rows_iter = first_row.into_iter().map(Ok).chain(lines);

    // --- 4. Iterate through rows with detailed validation ---
    let mut inserted = 0u32;
    let mut skipped = 0u32;
    let mut failed = 0u32;

    for line in rows_iter {
        line_idx += 1;

        let row = match line {
            Ok(r) => r,
            Err(e) => {
                log::info!(" Line {}: Error reading line: {}", line_idx, e);
                failed += 1;
                continue;
            }
        };

        if row.trim().is_empty() {
            log::debug!("Line {}: Skipping empty row", line_idx);
            skipped += 1;
            continue;
        }

        let values: Vec<String> = parse_csv_line(&row);

        if values.len() != columns.len() {
            log::warn!(
                "[CSV LOADER] Line {}: Expected {} columns, found {}. Skipping row.",
                line_idx,
                columns.len(),
                values.len()
            );
            skipped += 1;
            continue;
        }

        let values_ref: Vec<&str> = values.iter().map(|s| s.as_str()).collect();

        // --- 5. Validate each value before serialization ---
        let mut validation_passed = true;
        for (col_idx, (val, col)) in values_ref.iter().zip(columns.iter()).enumerate() {
            let data_type = &col.data_type;
            let trimmed = val.trim();
            if col.nullable && (trimmed.eq_ignore_ascii_case("null") || trimmed.is_empty()) {
                continue;
            }
            if let Err(validation_err) = validate_value(data_type, val) {
                log::warn!(
                    "[CSV LOADER] Line {}, Column {} ('{}'): {} Value: '{}'",
                    line_idx,
                    col_idx + 1,
                    col.name,
                    validation_err,
                    val
                );
                validation_passed = false;
                break;
            }
        }

        if !validation_passed {
            log::error!("Line {}: Validation failed. Skipping row.", line_idx);
            failed += 1;
            continue;
        }

        // Constraint validation: NOT NULL, UNIQUE, FK, CHECK
        if let Err(e) = crate::backend::constraint::validate_row_insert(
            catalog,
            db_name,
            table_name,
            &values_ref,
        ) {
            log::warn!("Line {}: Constraint violation: {}", line_idx, e);
            failed += 1;
            continue;
        }

        // --- 6. Serialize row based on schema ---
        let mut row_ok = true;

        for (val, col) in values_ref.iter().zip(columns.iter()) {
            let data_type = &col.data_type;
            let trimmed = val.trim();
            if col.nullable && (trimmed.eq_ignore_ascii_case("null") || trimmed.is_empty()) {
                continue;
            }
            match DataValue::parse_and_encode(data_type, val) {
                Ok(_) => {}
                Err(e) => {
                    println!("Skipping row {}: column '{}' — {}", line_idx, col.name, e);
                    row_ok = false;
                    break;
                }
            }
        }

        if !row_ok {
            failed += 1;
            continue;
        }

        // Build datatype list
        let data_types: Vec<_> = columns.iter().map(|c| c.data_type.clone()).collect();

        // Build nullable value list
        let nullable_values: Vec<Option<&str>> = values_ref
            .iter()
            .zip(columns.iter())
            .map(|(v, col)| {
                let trimmed = v.trim();
                if col.nullable && (trimmed.eq_ignore_ascii_case("null") || trimmed.is_empty()) {
                    None
                } else {
                    Some(*v)
                }
            })
            .collect();

        // Serialize using tuple layout serializer
        let tuple_bytes = match serialize_nullable_row(&data_types, &nullable_values) {
            Ok(bytes) => bytes,
            Err(e) => {
                log::error!("Line {}: Failed to serialize row: {}", line_idx, e);
                failed += 1;
                continue;
            }
        };

        // --- 7. Insert tuple using HeapManager (FSM-aware) ---
        match heap_manager.insert_tuple(&tuple_bytes) {
            Ok((page_id, slot_id)) => {
                // Update any existing B+ Tree index
                if let Err(e) = crate::backend::executor::create_index::update_index_on_insert(
                    db_name,
                    table_name,
                    &values_ref,
                    page_id,
                    slot_id,
                ) {
                    // Keep heap and index in step: drop the row again.
                    log::error!("Line {}: index update failed: {}", line_idx, e);
                    if let Err(undo) = heap_manager.delete_tuple(page_id, slot_id) {
                        log::error!("Line {}: failed to undo insert: {}", line_idx, undo);
                    }
                    failed += 1;
                    continue;
                }

                inserted += 1;
                if inserted.is_multiple_of(100) {
                    log::info!("Inserted {} rows so far...", inserted);
                }
            }
            Err(e) => {
                log::error!("Line {}: Failed to insert row: {}", line_idx, e);
                failed += 1;
            }
        }
    }

    log::info!("═══════════════════════════════════");
    log::info!(" CSV Load Summary:");
    log::info!(" Successfully inserted: {}", inserted);
    log::info!(" Skipped (formatting): {}", skipped);
    log::info!(" Failed (validation/insert): {}", failed);
    log::info!(" Total rows processed: {}", inserted + skipped + failed);
    log::info!(" ═══════════════════════════════════\n");

    if inserted == 0 && (skipped > 0 || failed > 0) {
        log::warn!(
            "WARNING: No rows were inserted. Please check your CSV file format and data types."
        );
    }

    Ok(inserted)
}

/// Insert a single tuple manually using HeapManager (FSM-aware)
///
/// Hot-path notes: the HeapManager for the table is process-cached
/// (`backend::cache::with_heap`) so repeated calls do not re-open files, and
/// index maintenance goes through the cached B+ Tree registry with batched
/// fsyncs. Callers that need data on disk should hit a checkpoint
/// (`backend::cache::checkpoint`) — heap scans do this automatically.
pub fn insert_single_tuple_with_location(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    values: &[&str],
) -> io::Result<Option<(u32, u32)>> {
    log::info!(" Starting single tuple insertion");

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
        log::info!(" Expected {} values, got {}", columns.len(), values.len());
        return Ok(None);
    }

    // Validate all values
    for (val, col) in values.iter().zip(columns.iter()) {
        let data_type = col.data_type.clone();
        let trimmed = val.trim();
        if col.nullable && (trimmed.eq_ignore_ascii_case("null") || trimmed.is_empty()) {
            continue;
        }

        if let Err(e) = validate_value(&data_type, val) {
            log::info!("Column '{}': {}", col.name, e);
            return Ok(None);
        }
    }

    // Constraint validation: NOT NULL, UNIQUE, FK, CHECK
    if let Err(e) =
        crate::backend::constraint::validate_row_insert(catalog, db_name, table_name, values)
    {
        log::info!("Constraint violation: {}", e);
        return Ok(None);
    }

    // Serialize tuple
    let mut row_ok = true;

    for (val, col) in values.iter().zip(columns.iter()) {
        let data_type = &col.data_type;
        let trimmed = val.trim();
        if col.nullable && (trimmed.eq_ignore_ascii_case("null") || trimmed.is_empty()) {
            continue;
        }

        match DataValue::parse_and_encode(data_type, val) {
            Ok(_) => {}
            Err(e) => {
                log::info!(" Failed to serialize column '{}': {}", col.name, e);
                row_ok = false;
                break;
            }
        }
    }

    if !row_ok {
        return Ok(None);
    }

    // Build datatype list
    let data_types: Vec<_> = columns.iter().map(|c| c.data_type.clone()).collect();

    // Build nullable value list
    let nullable_values: Vec<Option<&str>> = values
        .iter()
        .zip(columns.iter())
        .map(|(v, col)| {
            let trimmed = v.trim();
            if col.nullable && (trimmed.eq_ignore_ascii_case("null") || trimmed.is_empty()) {
                None
            } else {
                Some(*v)
            }
        })
        .collect();

    // Serialize using tuple layout serializer
    let tuple_bytes = match serialize_nullable_row(&data_types, &nullable_values) {
        Ok(bytes) => bytes,
        Err(e) => {
            log::info!(" Failed to serialize tuple: {}", e);
            return Ok(None);
        }
    };

    // Open HeapManager and insert tuple — via the process-cached manager
    // (no per-row file open / pool flush churn).
    let table_path = PathBuf::from(format!("database/base/{}/{}.dat", db_name, table_name));

    let inserted =
        crate::backend::cache::with_heap(&table_path, |hm| hm.insert_tuple(&tuple_bytes));

    match inserted {
        Ok((page_id, slot_id)) => {
            log::info!(
                " Successfully inserted at (page={}, slot={})",
                page_id,
                slot_id
            );

            // Update any existing B+ Tree index (cached handles + batched sync)
            if let Err(e) = crate::backend::executor::create_index::update_index_on_insert(
                db_name, table_name, values, page_id, slot_id,
            ) {
                // A row the index cannot see would vanish from every index
                // scan: take the row back out and fail the insert.
                if let Err(undo) = crate::backend::executor::delete::delete_by_pointers(
                    catalog,
                    db_name,
                    table_name,
                    &[(page_id, slot_id)],
                ) {
                    log::error!(" Failed to undo insert after index error: {}", undo);
                }
                return Err(io::Error::other(format!("Index update failed: {}", e)));
            }

            Ok(Some((page_id, slot_id)))
        }
        Err(e) => {
            log::info!(" Failed to insert tuple: {}", e);
            Ok(None)
        }
    }
}

pub fn insert_single_tuple(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    values: &[&str],
) -> io::Result<bool> {
    insert_single_tuple_with_location(catalog, db_name, table_name, values).map(|opt| opt.is_some())
}

/// RFC-4180 compliant CSV line parser.
/// Handles quoted fields, embedded commas, doubled quotes (`""`), and backslash-escaped quotes (`\"`).
pub fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let chars: Vec<char> = line.chars().collect();
    let n = chars.len();
    let mut i = 0;

    while i < n {
        // Skip leading whitespace before field
        while i < n && (chars[i] == ' ' || chars[i] == '\t') {
            i += 1;
        }
        if i >= n {
            break;
        }

        if chars[i] == '"' {
            i += 1; // consume opening quote
            let mut field = String::new();
            while i < n {
                if chars[i] == '"' {
                    if i + 1 < n && chars[i + 1] == '"' {
                        field.push('"');
                        i += 2;
                    } else {
                        // closing quote
                        i += 1;
                        break;
                    }
                } else if chars[i] == '\\' && i + 1 < n && chars[i + 1] == '"' {
                    field.push('"');
                    i += 2;
                } else {
                    field.push(chars[i]);
                    i += 1;
                }
            }
            // Skip trailing whitespace after closing quote up to comma or end
            while i < n && (chars[i] == ' ' || chars[i] == '\t') {
                i += 1;
            }
            fields.push(field);
            if i < n && chars[i] == ',' {
                i += 1; // consume delimiter
                if i == n {
                    fields.push(String::new());
                }
            }
        } else {
            let mut field = String::new();
            while i < n && chars[i] != ',' {
                field.push(chars[i]);
                i += 1;
            }
            fields.push(field.trim().to_string());
            if i < n && chars[i] == ',' {
                i += 1; // consume delimiter
                if i == n {
                    fields.push(String::new());
                }
            }
        }
    }

    fields
}
