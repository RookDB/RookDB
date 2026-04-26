use std::fs::{File, OpenOptions};
use std::io::{self};

use crate::catalog::types::{Catalog, Column, Database};
use crate::disk::read_page;
use crate::executor::jsonb::JsonbSerializer;
use crate::executor::predicate::{Datum, Predicate, evaluate};
use crate::executor::udt::UdtSerializer;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::table::page_count;
use crate::toast::toast_reader::detoast_value;
use crate::toast::{TOAST_POINTER_TAG, ToastPointer};

/// Reads a variable-length value from a tuple byte slice at a given cursor position.
/// Returns (raw value bytes, new cursor position).
fn deserialize_variable_length(
    tuple_data: &[u8],
    cursor: usize,
    toast_file: &mut Option<File>,
) -> io::Result<(Vec<u8>, usize)> {
    let total_len = u32::from_le_bytes(tuple_data[cursor..cursor + 4].try_into().unwrap()) as usize;
    let tag = tuple_data[cursor + 4];

    if tag == TOAST_POINTER_TAG {
        // Toasted: parse the 18 byte pointer and detoast
        let pointer = ToastPointer::from_bytes(&tuple_data[cursor + 5..cursor + 5 + 18]);
        let data = detoast_value(
            toast_file
                .as_mut()
                .expect("Toast file required but not open"),
            &pointer,
        )?;
        Ok((data, cursor + 4 + total_len))
    } else {
        // Inline: copy the raw bytes
        let data = tuple_data[cursor + 5..cursor + 4 + total_len].to_vec();
        Ok((data, cursor + 4 + total_len))
    }
}

/// Decode every column of a single tuple into a `Vec<Datum>` so the predicate
/// evaluator can reason about typed values. JSONB columns are converted to
/// `serde_json::Value` once per row; JSON columns are kept as raw text and
/// parsed on demand by the evaluator.
fn materialize_tuple(
    columns: &[Column],
    tuple_data: &[u8],
    db: &Database,
    toast_file: &mut Option<File>,
) -> io::Result<Vec<Datum>> {
    let mut row = Vec::with_capacity(columns.len());
    let mut cursor = 0usize;

    for col in columns {
        let datum = match col.data_type.as_str() {
            "INT" => {
                if cursor + 4 > tuple_data.len() {
                    Datum::Null
                } else {
                    let val = i32::from_le_bytes(
                        tuple_data[cursor..cursor + 4].try_into().unwrap(),
                    );
                    cursor += 4;
                    Datum::Int(val)
                }
            }
            "TEXT" => {
                if cursor + 10 > tuple_data.len() {
                    Datum::Null
                } else {
                    let text = String::from_utf8_lossy(&tuple_data[cursor..cursor + 10])
                        .trim()
                        .to_string();
                    cursor += 10;
                    Datum::Text(text)
                }
            }
            "BOOLEAN" => {
                if cursor + 1 > tuple_data.len() {
                    Datum::Null
                } else {
                    let v = tuple_data[cursor] != 0;
                    cursor += 1;
                    Datum::Bool(v)
                }
            }
            "JSON" => {
                let (bytes, new_cursor) =
                    deserialize_variable_length(tuple_data, cursor, toast_file)?;
                cursor = new_cursor;
                Datum::JsonText(String::from_utf8_lossy(&bytes).into_owned())
            }
            "JSONB" => {
                let (bytes, new_cursor) =
                    deserialize_variable_length(tuple_data, cursor, toast_file)?;
                cursor = new_cursor;
                match JsonbSerializer::from_binary(&bytes) {
                    Ok((value, _)) => Datum::Json(JsonbSerializer::to_serde(&value)),
                    Err(_) => Datum::Null,
                }
            }
            "XML" => {
                let (bytes, new_cursor) =
                    deserialize_variable_length(tuple_data, cursor, toast_file)?;
                cursor = new_cursor;
                Datum::Text(String::from_utf8_lossy(&bytes).into_owned())
            }
            dt if dt.starts_with("UDT:") => {
                let udt_name = &dt[4..];
                let (bytes, new_cursor) =
                    deserialize_variable_length(tuple_data, cursor, toast_file)?;
                cursor = new_cursor;
                let display = db
                    .types
                    .get(udt_name)
                    .and_then(|def| UdtSerializer::to_display_string(def, &bytes).ok())
                    .unwrap_or_else(|| format!("<UDT '{}' not found>", udt_name));
                Datum::Text(display)
            }
            _ => Datum::Null,
        };
        row.push(datum);
    }

    Ok(row)
}

/// Format a row of `Datum`s for display, matching the legacy `show_tuples` output.
fn format_row(columns: &[Column], row: &[Datum]) -> String {
    let mut out = String::new();
    for (col, d) in columns.iter().zip(row.iter()) {
        match d {
            Datum::Null => out.push_str(&format!("{}=<null> ", col.name)),
            Datum::Int(v) => out.push_str(&format!("{}={} ", col.name, v)),
            Datum::Text(s) => {
                if col.data_type == "TEXT" {
                    out.push_str(&format!("{}='{}' ", col.name, s));
                } else {
                    out.push_str(&format!("{}={} ", col.name, s));
                }
            }
            Datum::Bool(b) => out.push_str(&format!("{}={} ", col.name, b)),
            Datum::Number(n) => out.push_str(&format!("{}={} ", col.name, n)),
            Datum::Json(v) => out.push_str(&format!("{}={} ", col.name, v)),
            Datum::JsonText(t) => out.push_str(&format!("{}={} ", col.name, t)),
        }
    }
    out
}

pub fn show_tuples(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    predicate: Option<&Predicate>,
) -> io::Result<()> {
    // 1. Get schema from catalog
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

    // Open toast file if the table has one
    let mut toast_file: Option<File> = if table.has_toast_table {
        let toast_path = format!("database/base/{}/{}_toast.dat", db_name, table_name);
        Some(OpenOptions::new().read(true).open(&toast_path)?)
    } else {
        None
    };

    // 2. Read total number of pages
    let total_pages = page_count(file)?;

    println!("\n=== Tuples in '{}.{}' ===", db_name, table_name);
    println!("Total pages: {}", total_pages);

    // 3. Loop through each page
    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(file, &mut page, page_num)?;
        println!("\n-- Page {} --", page_num);

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        println!("Lower: {}, Upper: {}, Tuples: {}", lower, upper, num_items);

        // 4. For each tuple
        let mut printed = 0u32;
        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap());
            let tuple_data = &page.data[offset as usize..(offset + length) as usize];

            let row = materialize_tuple(columns, tuple_data, db, &mut toast_file)?;

            // Filter: keep only rows whose predicate evaluates to TRUE.
            // UNKNOWN/NULL drop, matching SQL WHERE.
            if let Some(p) = predicate {
                if evaluate(p, &row) != Some(true) {
                    continue;
                }
            }

            print!("Tuple {}: ", i + 1);
            print!("{}", format_row(columns, &row));
            println!();
            printed += 1;
        }

        if predicate.is_some() {
            println!("Matched: {}", printed);
        }
    }

    println!("\n=== End of tuples ===\n");
    Ok(())
}
