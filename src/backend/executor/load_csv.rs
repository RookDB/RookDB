use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader};

use crate::buffer_manager::BufferManager;
use crate::catalog::indexes::get_indexes_for_table;
use crate::catalog::page_manager::CatalogPageManager;
use crate::catalog::types::Catalog;
use crate::heap::insert_tuple;

pub fn load_csv(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    csv_path: &str,
) -> io::Result<()> {
    // --- 1. Fetch table schema from catalog ---
    let table_meta = crate::catalog::catalog::get_table_metadata(catalog, pm, bm, db_name, table_name).map_err(|e| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Could not fetch metadata for '{}.{}': {}", db_name, table_name, e),
        )
    })?;

    let columns = table_meta.columns;
    if columns.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Table has no columns",
        ));
    }

    let indexes = get_indexes_for_table(pm, bm, table_meta.table_oid)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

    // --- 2. Open and read the CSV file ---
    let csv_file = File::open(csv_path)?;
    let reader = BufReader::new(csv_file);

    let mut lines = reader.lines();

    // Skip header line
    if let Some(Ok(_header)) = lines.next() {}

    // --- 3. Iterate through rows ---
    let mut inserted = 0;
    for (i, line) in lines.enumerate() {
        let row = line?;
        if row.trim().is_empty() {
            continue;
        }

        let values: Vec<&str> = row.split(',').map(|v| v.trim()).collect();

        if values.len() != columns.len() {
            println!(
                "Skipping row {}: expected {} columns, found {}",
                i + 1,
                columns.len(),
                values.len()
            );
            continue;
        }

        // --- 4. Serialize row and map fields for constraint checking ---
        let mut tuple_bytes: Vec<u8> = Vec::new();
        let mut tuple_map: HashMap<u32, Option<Vec<u8>>> = HashMap::new();

        for (val, col) in values.iter().zip(columns.iter()) {
            let mut field_bytes = Vec::new();
            if val.is_empty() && col.is_nullable {
                tuple_map.insert(col.column_oid, None);
                continue;
            }

            match col.data_type.type_name.to_uppercase().as_str() {
                "INT" | "INTEGER" => {
                    let num: i32 = val.parse().unwrap_or_default();
                    field_bytes.extend_from_slice(&num.to_le_bytes());
                }
                "BIGINT" => {
                    let num: i64 = val.parse().unwrap_or_default();
                    field_bytes.extend_from_slice(&num.to_le_bytes());
                }
                "FLOAT" | "REAL" => {
                    let num: f32 = val.parse().unwrap_or_default();
                    field_bytes.extend_from_slice(&num.to_le_bytes());
                }
                "DOUBLE" => {
                    let num: f64 = val.parse().unwrap_or_default();
                    field_bytes.extend_from_slice(&num.to_le_bytes());
                }
                "BOOL" | "BOOLEAN" => {
                    let b: u8 = match val.to_lowercase().as_str() {
                        "true" | "1" | "yes" => 1,
                        _ => 0,
                    };
                    field_bytes.push(b);
                }
                "TEXT" | "STRING" => {
                    let bytes = val.as_bytes().to_vec();
                    field_bytes.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                    field_bytes.extend_from_slice(&bytes);
                }
                t if t.starts_with("VARCHAR") => {
                    let max_len: usize = t
                        .strip_prefix("VARCHAR(")
                        .and_then(|s: &str| s.strip_suffix(')'))
                        .and_then(|s: &str| s.parse::<usize>().ok())
                        .unwrap_or(255);
                    let mut bytes = val.as_bytes().to_vec();
                    bytes.truncate(max_len);
                    field_bytes.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                    field_bytes.extend_from_slice(&bytes);
                }
                _ => {
                    field_bytes.extend_from_slice(&(0u16).to_le_bytes());
                }
            }
            tuple_map.insert(col.column_oid, Some(field_bytes.clone()));
            tuple_bytes.extend_from_slice(&field_bytes);
        }

        // --- 5. Validate Constraints ---
        if let Err(e) = crate::catalog::constraints::validate_constraints(
            catalog,
            pm,
            bm,
            table_meta.table_oid,
            &tuple_map,
        ) {
            println!("Skipping row {}: Constraint violation: {:?}", i + 1, e);
            continue;
        }

        // --- 6. Insert tuple into heap file ---
        match insert_tuple(file, &tuple_bytes) {
            Ok((page_num, slot_id)) => {
                inserted += 1;
                // --- 7. Update Indexes ---
                for idx in &indexes {
                    let mut key_bytes = Vec::new();
                    for col_oid in &idx.column_oids {
                        if let Some(Some(val)) = tuple_map.get(col_oid) {
                            key_bytes.extend_from_slice(val);
                        }
                    }
                    let _ = crate::catalog::indexes::insert_index_entry(
                        bm,
                        db_name,
                        &idx.index_name,
                        &key_bytes,
                        page_num,
                        slot_id,
                    );
                }
            }
            Err(e) => {
                println!("Failed to insert row {}: {}", i + 1, e);
            }
        }
    }
    let final_page_count = crate::table::page_count(file)?;
    
    let new_row_count = table_meta.statistics.row_count + inserted as u64;
    if let Err(e) = crate::catalog::update_table_statistics(
        catalog,
        pm,
        bm,
        table_meta.table_oid,
        new_row_count,
        final_page_count,
    ) {
        println!("Warning: Failed to update table statistics: {}", e);
    }
    
    println!("Total Number of rows inserted: {}", inserted);
    Ok(())
}
