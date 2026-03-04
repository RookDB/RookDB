use std::collections::HashMap;
use std::fs::File;
use std::io;

use crate::catalog::types::Catalog;
use crate::disk::read_page;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::table::page_count;

#[derive(Debug, Clone)]
pub enum ProjectionRequest {
    /// Equivalent to SELECT *
    All,
    /// Equivalent to SELECT col1, col2, ...
    List(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct ProjectionSpec {
    /// Column indices in the base schema, in the output order requested by user
    pub col_idxs: Vec<usize>,
    /// Output names in the same order (for printing)
    pub col_names: Vec<String>,
}

fn build_projection_spec(
    columns: &[crate::catalog::types::Column],
    req: ProjectionRequest,
) -> io::Result<ProjectionSpec> {
    match req {
        ProjectionRequest::All => {
            let mut col_idxs = Vec::with_capacity(columns.len());
            let mut col_names = Vec::with_capacity(columns.len());
            for (i, c) in columns.iter().enumerate() {
                col_idxs.push(i);
                col_names.push(c.name.clone());
            }
            Ok(ProjectionSpec { col_idxs, col_names })
        }
        ProjectionRequest::List(names) => {
            let mut name_to_idx: HashMap<&str, usize> = HashMap::new();
            for (i, c) in columns.iter().enumerate() {
                name_to_idx.insert(c.name.as_str(), i);
            }

            let mut col_idxs = Vec::with_capacity(names.len());
            let mut col_names = Vec::with_capacity(names.len());

            for raw in names {
                let name = raw.trim().to_string();
                if name.is_empty() {
                    continue;
                }
                let idx = match name_to_idx.get(name.as_str()) {
                    Some(i) => *i,
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("Unknown column '{}' in projection", name),
                        ))
                    }
                };
                col_idxs.push(idx);
                col_names.push(name);
            }

            if col_idxs.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Projection list is empty. Use * or provide at least one column.",
                ));
            }

            Ok(ProjectionSpec { col_idxs, col_names })
        }
    }
}

/// Decode ALL columns from tuple bytes into Vec<Option<String>> where index = schema column index.
/// Matches the existing fixed layout: INT=4 bytes, TEXT=10 bytes.
fn decode_full_tuple(
    tuple_data: &[u8],
    columns: &[crate::catalog::types::Column],
) -> Vec<Option<String>> {
    let mut out: Vec<Option<String>> = vec![None; columns.len()];

    let mut cursor = 0usize;
    for (i, col) in columns.iter().enumerate() {
        match col.data_type.as_str() {
            "INT" => {
                if cursor + 4 <= tuple_data.len() {
                    let val = i32::from_le_bytes(
                        tuple_data[cursor..cursor + 4].try_into().unwrap(),
                    );
                    out[i] = Some(val.to_string());
                    cursor += 4;
                }
            }
            "TEXT" => {
                if cursor + 10 <= tuple_data.len() {
                    let text_bytes = &tuple_data[cursor..cursor + 10];
                    let text = String::from_utf8_lossy(text_bytes).trim().to_string();
                    out[i] = Some(text);
                    cursor += 10;
                }
            }
            _ => {
                // Unsupported type: keep None
            }
        }
    }

    out
}

/// SELECT tuples with projection (attribute processing)
pub fn select_tuples(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    projection: ProjectionRequest,
) -> io::Result<()> {
    // 1) Schema from catalog
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

    // 2) Build projection spec
    let proj = build_projection_spec(columns, projection)?;

    // 3) Pages
    let total_pages = page_count(file)?;
    println!("\n=== Tuples in '{}.{}' ===", db_name, table_name);
    println!("Total pages: {}", total_pages);

    // Print projection header
    print!("Projection: ");
    for (i, n) in proj.col_names.iter().enumerate() {
        if i > 0 {
            print!(", ");
        }
        print!("{}", n);
    }
    println!("\n");

    // 4) Scan pages
    // Page 0 is header in your system; data pages start from 1.
    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(file, &mut page, page_num)?;

        println!("\n-- Page {} --", page_num);

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

        // Safety: skip non-slotted / invalid pages
        if lower < PAGE_HEADER_SIZE || lower > upper {
            println!("Skipping non-data page (lower={}, upper={})", lower, upper);
            continue;
        }

        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
        println!("Lower: {}, Upper: {}, Tuples: {}", lower, upper, num_items);

        // 5) For each tuple
        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap());

            // Bounds check
            let start = offset as usize;
            let end = (offset + length) as usize;
            if end > page.data.len() || start >= end {
                continue;
            }

            let tuple_data = &page.data[start..end];

            // Decode full tuple, then print only projected columns in requested order
            let decoded = decode_full_tuple(tuple_data, columns);

            print!("Tuple {}: ", i + 1);
            for (pos, col_idx) in proj.col_idxs.iter().enumerate() {
                let col_name = &proj.col_names[pos];
                let val_opt = decoded.get(*col_idx).cloned().unwrap_or(None);

                match columns[*col_idx].data_type.as_str() {
                    "TEXT" => {
                        let v = val_opt.unwrap_or_else(|| "".to_string());
                        print!("{}='{}' ", col_name, v);
                    }
                    "INT" => {
                        let v = val_opt.unwrap_or_else(|| "0".to_string());
                        print!("{}={} ", col_name, v);
                    }
                    _ => {
                        let v = val_opt.unwrap_or_else(|| "".to_string());
                        print!("{}={} ", col_name, v);
                    }
                }
            }
            println!();
        }
    }

    println!("\n=== End of tuples ===\n");
    Ok(())
}
