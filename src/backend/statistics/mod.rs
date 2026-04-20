use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

use crate::page::{Page, PAGE_SIZE, PAGE_HEADER_SIZE, ITEM_ID_SIZE, page_free_space, get_tuple_count, get_slot_entry};
use crate::catalog::types::Catalog;
use crate::backend::types_validator::DataType;

/// Print detailed statistics for each page in a table
pub fn print_table_page_count(catalog: &Catalog, db_name: &str, table_name: &str) -> io::Result<()> {
    let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut file = match File::open(&table_path) {
        Ok(f) => f,
        Err(e) => {
            println!("Could not open table file for {}.{}: {}", db_name, table_name, e);
            return Err(e);
        }
    };

    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name))
    })?;

    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name))
    })?;

    let columns = &table.columns;

    // Read header page (page 0)
    let mut header_page = vec![0u8; PAGE_SIZE];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header_page)?;

    // First 4 bytes store total page count
    let total_pages = u32::from_le_bytes(header_page[0..4].try_into().unwrap());

    println!("\n════════════════════════════════════════════");
    println!("   Table '{}' Statistics", table_name);
    println!("   Total pages: {}", total_pages);
    println!("════════════════════════════════════════════");

    for page_id in 1..total_pages {
        let mut page_data = vec![0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start((page_id as u64) * (PAGE_SIZE as u64)))?;
        file.read_exact(&mut page_data)?;

        let page = Page { data: page_data };

        let tuple_count = match get_tuple_count(&page) {
            Ok(c) => c,
            Err(e) => {
                println!("Page {}: Error reading tuple count: {}", page_id, e);
                continue;
            }
        };

        let largest_contiguous = match page_free_space(&page) {
            Ok(space) => space,
            Err(_) => 0,
        };

        let mut active_tuples = 0;
        let mut total_tuple_size = 0;
        let mut slot_details = Vec::new();

        for slot_id in 0..tuple_count {
            if let Ok((offset, length)) = get_slot_entry(&page, slot_id) {
                if length > 0 && offset > 0 { // active tuple
                    active_tuples += 1;
                    total_tuple_size += length;

                    let data_start = offset as usize;
                    let data_end = (offset + length) as usize;
                    
                    if data_end > PAGE_SIZE {
                        slot_details.push(format!("Slot {}: (invalid tuple bounds)", slot_id));
                        continue;
                    }

                    let tuple_data_slice = &page.data[data_start..data_end];
                    
                    // Decode columns
                    let mut cursor = 0usize;
                    let mut col_values = Vec::new();
                    
                    for col in columns.iter() {
                        let col_type = DataType::from_str(&col.data_type).unwrap_or(DataType::Text { max_length: 255 });
                        let byte_size = col_type.byte_size();
                        
                        if cursor + byte_size <= tuple_data_slice.len() {
                            let raw_bytes = &tuple_data_slice[cursor..cursor + byte_size];
                            match col_type.deserialize_value(raw_bytes) {
                                Ok(val) => col_values.push(format!("{}: {}", col.name, val)),
                                Err(_) => col_values.push(format!("{}: <error>", col.name)),
                            }
                            cursor += byte_size;
                        } else {
                            col_values.push(format!("{}: <incomplete>", col.name));
                        }
                    }

                    slot_details.push(format!("Slot {}: offset {}, length {}, tuple: {{ {} }}", 
                                              slot_id, offset, length, col_values.join(", ")));
                } else {
                    slot_details.push(format!("Slot {}: (deleted/dead)", slot_id));
                }
            } else {
                slot_details.push(format!("Slot {}: (invalid bounds)", slot_id));
            }
        }

        let slot_array_size = tuple_count * ITEM_ID_SIZE;
        let used_space = PAGE_HEADER_SIZE + slot_array_size + total_tuple_size;
        let total_free_space = if used_space <= PAGE_SIZE as u32 {
            PAGE_SIZE as u32 - used_space
        } else {
            0
        };

        println!("Page {}:", page_id);
        println!("  Total free space: {} bytes", total_free_space);
        println!("  Largest contiguous free space: {} bytes", largest_contiguous);
        println!("  Number of tuples (slots): {} (Active: {})", tuple_count, active_tuples);
        
        if slot_details.is_empty() {
            println!("  Tuples: (none)");
        } else {
            println!("  Tuples:");
            for detail in slot_details {
                println!("    {}", detail);
            }
        }
        println!("--------------------------------------------------");
    }

    Ok(())
}
