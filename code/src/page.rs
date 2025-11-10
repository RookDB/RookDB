use std::fs::{File};
use std::io::{self, Read, Seek, SeekFrom, BufRead, BufReader};
use crate::catalog::Catalog;

pub const PAGE_SIZE: usize = 8192; // Page size - storing as 8 bytes and as usize only because most pointers(file pointers and otheres requires it to be 8 bytes)
pub const PAGE_HEADER_SIZE: u32 = 8;
pub const ITEM_ID_SIZE: u32  = 8;

use crate::disk::{create_page, read_page, write_page};

// pub struct PageHeader {
//     pub lower: u32, // Offset to start of free space - 4 bytes
//     pub upper: u32, // Offset to end of free space - 4 bytes
// }

// pub struct ItemId {
//     pub offset: u32, // Offset of the item or Tuple - 4 bytes
//     pub length: u32, // Length of the item or Tuple - 4 bytes
// }

pub struct Page {
    pub data: Vec<u8>, // Fixed-size buffer holds the raw bytes of a page (PAGE_SIZE = 8KB)
}

impl Page {
    pub fn new() -> Self {
        Self {
            data: vec![0; PAGE_SIZE],
        }
    }
}

pub fn init_page(page: &mut Page) {
    // Lower offset in first 4 bytes
    let lower = PAGE_HEADER_SIZE.to_le_bytes();
    page.data[0..4].copy_from_slice(&lower);

    // Upper offset in next 4 bytes
    let upper = (PAGE_SIZE as u32).to_le_bytes();
    page.data[4..8].copy_from_slice(&upper);
}

pub fn page_count(file: &mut File) -> io::Result<u32> {
    // Move cursor to the start of the file header
    file.seek(SeekFrom::Start(0))?;

    // Read the first 4 bytes of the table header
    let mut buffer = [0u8; 4];
    file.read_exact(&mut buffer)?;

    // Convert bytes to little-endian u32
    let total_pages = u32::from_le_bytes(buffer);

    Ok(total_pages)
}



pub fn page_free_space(page: &Page) -> io::Result<u32> {
    // Get Lower and Upper of page
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
    // println!("UPPER: {}", upper);
    // println!("LOWER: {}", lower);
    // Freespace is equal to upper - lower
    Ok(upper - lower)
}

pub fn insert_tuple(file: &mut File, data: &[u8]) -> io::Result<()> {
    // Get total number of pages in the file
    let mut total_pages: u32 = page_count(file)?;
    let mut last_page_num: u32 = total_pages - 1;
    // println!("Inserting into page {}", last_page_num);

    // Read last page into memory
    let mut last_page: Page = Page::new();
    read_page(file, &mut last_page, last_page_num)?;

    // Calculate free space in the page
    let free_space: u32 = page_free_space(&last_page)?;
    // println!("Free space in last page: {} bytes", free_space);

    // Total bytes required = tuple data + item header (offset + length)
    let total_required = data.len() as u32 + ITEM_ID_SIZE;

    if total_required > free_space {
        // Not enough space — create a new page
        // println!("Not enough free space in last page. Creating a new page...");
        create_page(file)?;
        total_pages += 1;
        last_page_num = total_pages - 1;

        // Read the newly created page (it should be empty)
        read_page(file, &mut last_page, last_page_num)?;
        // println!("Inserting into newly created page {}", last_page_num);
    }

    // === Insert into last_page (either old or new) ===

    // Get current header offsets
    let mut lower = u32::from_le_bytes(last_page.data[0..4].try_into().unwrap());
    let mut upper = u32::from_le_bytes(last_page.data[4..8].try_into().unwrap());

    // Compute where to place data
    let start = upper - data.len() as u32;

    // Copy tuple data into the page
    last_page.data[start as usize..upper as usize].copy_from_slice(data);

    // Update upper pointer
    upper = start;
    last_page.data[4..8].copy_from_slice(&upper.to_le_bytes());

    // Write ItemId entry (offset + length)
    let item_id_pos = lower as usize;
    last_page.data[item_id_pos..item_id_pos + 4].copy_from_slice(&start.to_le_bytes());
    last_page.data[item_id_pos + 4..item_id_pos + 8]
        .copy_from_slice(&(data.len() as u32).to_le_bytes());

    // Update lower pointer
    lower += ITEM_ID_SIZE;
    last_page.data[0..4].copy_from_slice(&lower.to_le_bytes());

    // Write page back to disk
    write_page(file, &mut last_page, last_page_num)?;
    // println!("Tuple inserted successfully ({} bytes).", data.len());

    Ok(())
}



pub fn load_csv_and_insert(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
    csv_path: &str,
) -> io::Result<()> {
    // --- 1. Fetch table schema from catalog ---
    let db = catalog
        .databases
        .get(db_name)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name)))?;

    let table = db
        .tables
        .get(table_name)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name)))?;

    let columns = &table.columns;
    if columns.is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "Table has no columns"));
    }

    // println!(
    //     "Loading CSV '{}' into table '{}.{}' ({} columns)",
    //     csv_path,
    //     db_name,
    //     table_name,
    //     columns.len()
    // );

    // --- 2. Open and read the CSV file ---
    let csv_file = File::open(csv_path)?;
    let reader = BufReader::new(csv_file);

    let mut lines = reader.lines();

    // Skip header line
    if let Some(Ok(header)) = lines.next() {
        // println!("Header: {}", header);
    }

    // --- 3. Iterate through rows ---
    let mut inserted = 0;
    for (i, line) in lines.enumerate() {
        let row = line?;
        if row.trim().is_empty() {
            continue;
        }

        // Split CSV fields by comma
        let values: Vec<&str> = row.split(',').map(|v| v.trim()).collect();

        // Validate number of columns
        if values.len() != columns.len() {
            println!(
                "Skipping row {}: expected {} columns, found {}",
                i + 1,
                columns.len(),
                values.len()
            );
            continue;
        }

        // --- 4. Serialize row based on schema ---
        let mut tuple_bytes: Vec<u8> = Vec::new();

        for (val, col) in values.iter().zip(columns.iter()) {
            match col.data_type.as_str() {
                "INT" => {
                    let num: i32 = val.parse().unwrap_or_default();
                    tuple_bytes.extend_from_slice(&num.to_le_bytes());
                }
                "TEXT" => {
                    let mut text_bytes = val.as_bytes().to_vec();
                    if text_bytes.len() > 10 {
                        text_bytes.truncate(10);
                    } else if text_bytes.len() < 10 {
                        text_bytes.extend(vec![b' '; 10 - text_bytes.len()]);
                    }
                    tuple_bytes.extend_from_slice(&text_bytes);
                }
                _ => {
                    println!(
                        "Unsupported column type '{}' in column '{}'",
                        col.data_type, col.name
                    );
                    continue;
                }
            }
        }

        // --- 5. Insert tuple into page system ---
        if let Err(e) = insert_tuple(file, &tuple_bytes) {
            println!("Failed to insert row {}: {}", i + 1, e);
        } else {
            inserted += 1;
        }
    }

    println!("Successfully inserted {} rows into '{}.{}'", inserted, db_name, table_name);
     // --- 6️⃣. Inspect first page (for verification) ---
    // let mut page = Page::new();

    // Move to the first data page (page number 1)
    // read_page(file, &mut page, 1)?;

    // println!("\n--- First Data Page [1] Snapshot ---");
    // println!("First 32 bytes: {:?}", &page.data[0..32]);
    // println!("------------------------------------\n");
    Ok(())
}



pub fn show_tuples(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    file: &mut File,
) -> io::Result<()> {
    // 1. Get schema from catalog
    let db = catalog
        .databases
        .get(db_name)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name)))?;

    let table = db
        .tables
        .get(table_name)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name)))?;

    let columns = &table.columns;

    // 2. Read total number of pages
    file.seek(SeekFrom::Start(0))?;
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    let mut total_pages = u32::from_le_bytes(buf);

    println!("\n=== Tuples in '{}.{}' ===", db_name, table_name);
    println!("Total pages: {}", total_pages);
    total_pages = total_pages;
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
                            let val = i32::from_le_bytes(tuple_data[cursor..cursor + 4].try_into().unwrap());
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
                    _ => {
                        print!("{}=<unsupported> ", col.name);
                    }
                }
            }
            println!();
        }
        // break;
    }

    println!("\n=== End of tuples ===\n");
    Ok(())
}