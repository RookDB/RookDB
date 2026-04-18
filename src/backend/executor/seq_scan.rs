use std::fs::{File, OpenOptions};
use std::io::{self};

use crate::catalog::types::Catalog;
use crate::disk::read_page;
use crate::executor::jsonb::JsonbSerializer;
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

pub fn show_tuples(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
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
        println!("Lower: {}, Upper: {}", lower, upper);
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        println!("Lower: {}, Upper: {}, Tuples: {}", lower, upper, num_items);

        // 4. For each tuple
        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap());
            let tuple_data = &page.data[offset as usize..(offset + length) as usize];

            print!("Tuple {}: ", i + 1);

            // 5. Decode each column
            let mut cursor = 0usize;
            for col in columns {
                match col.data_type.as_str() {
                    "INT" => {
                        if cursor + 4 <= tuple_data.len() {
                            let val = i32::from_le_bytes(
                                tuple_data[cursor..cursor + 4].try_into().unwrap(),
                            );
                            print!("{}={} ", col.name, val);
                            cursor += 4;
                        }
                    }
                    "TEXT" => {
                        if cursor + 10 <= tuple_data.len() {
                            let text_bytes = &tuple_data[cursor..cursor + 10];
                            let text = String::from_utf8_lossy(text_bytes).trim().to_string();
                            print!("{}='{}' ", col.name, text);
                            cursor += 10;
                        }
                    }
                    "JSON" => {
                        let (json_bytes, new_cursor) =
                            deserialize_variable_length(tuple_data, cursor, &mut toast_file)?;
                        let json = String::from_utf8_lossy(&json_bytes).to_string();
                        print!("{}={} ", col.name, json);
                        cursor = new_cursor;
                    }
                    "JSONB" => {
                        let (jsonb_bytes, new_cursor) =
                            deserialize_variable_length(tuple_data, cursor, &mut toast_file)?;
                        match JsonbSerializer::from_binary(&jsonb_bytes) {
                            Ok((value, _)) => {
                                let display = JsonbSerializer::to_display_string(&value);
                                print!("{}={} ", col.name, display);
                            }
                            Err(e) => {
                                print!("{}=<JSONB error: {}> ", col.name, e);
                            }
                        }
                        cursor = new_cursor;
                    }
                    "XML" => {
                        let (xml_bytes, new_cursor) =
                            deserialize_variable_length(tuple_data, cursor, &mut toast_file)?;
                        let xml = String::from_utf8_lossy(&xml_bytes).to_string();
                        print!("{}={} ", col.name, xml);
                        cursor = new_cursor;
                    }
                    dt if dt.starts_with("UDT:") => {
                        let udt_name = &dt[4..];
                        let (udt_bytes, new_cursor) =
                            deserialize_variable_length(tuple_data, cursor, &mut toast_file)?;
                        if let Some(udt_def) = db.types.get(udt_name) {
                            match UdtSerializer::to_display_string(udt_def, &udt_bytes) {
                                Ok(display) => print!("{}={} ", col.name, display),
                                Err(e) => print!("{}=<UDT error: {}> ", col.name, e),
                            }
                        } else {
                            print!("{}=<UDT '{}' not found> ", col.name, udt_name);
                        }
                        cursor = new_cursor;
                    }
                    _ => {
                        print!("{}=<unsupported> ", col.name);
                    }
                }
            }
            println!();
        }
    }

    println!("\n=== End of tuples ===\n");
    Ok(())
}
