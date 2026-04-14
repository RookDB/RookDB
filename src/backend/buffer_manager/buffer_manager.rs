use crate::catalog::types::Catalog;
use crate::backend::disk::{read_page, write_page};
use crate::backend::page::{ITEM_ID_SIZE, PAGE_SIZE, Page, init_page, page_free_space};
use crate::backend::storage::literal_parser::parse_value_literal;
use crate::backend::storage::tuple_codec::TupleCodec;
use crate::backend::storage::toast::ToastManager;
use crate::catalog::data_type::DataType;
use std::fs::File;
use std::io::{self, BufRead, BufReader, ErrorKind, Read, Seek, SeekFrom};

/// Parse a CSV line while respecting bracket-delimited arrays and quoted fields.
/// 
/// Standard CSV parsers treat every comma as a field delimiter, but this breaks
/// array fields like [item1,item2,item3]. This function correctly identifies
/// field boundaries while ignoring commas inside brackets and quotes.
/// 
/// # Examples
/// - `1,True,"text",0xABCD,[1,2,3]` → 5 fields
/// - `42,"hello, world",@file.bin` → 3 fields (comma in quoted string ignored)
/// - `[[1,2],[3,4]]` → 1 field (nested brackets handled)
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current_field = String::new();
    let mut in_brackets: i32 = 0;  // Track nested brackets
    let mut in_quotes = false;
    let mut quote_char = ' ';
    let mut escaped = false;

    for ch in line.chars() {
        if escaped {
            current_field.push(ch);
            escaped = false;
            continue;
        }

        match ch {
            '\\' if in_quotes => {
                escaped = true;
                current_field.push(ch);
            }
            '\"' | '\'' if !in_quotes => {
                // Enter quoted string
                in_quotes = true;
                quote_char = ch;
                current_field.push(ch);
            }
            c if in_quotes && c == quote_char => {
                // Exit quoted string
                in_quotes = false;
                current_field.push(ch);
            }
            '[' if !in_quotes => {
                in_brackets += 1;
                current_field.push(ch);
            }
            ']' if !in_quotes => {
                in_brackets = in_brackets.saturating_sub(1);
                current_field.push(ch);
            }
            ',' if !in_quotes && in_brackets == 0 => {
                // Field delimiter: only if not in quotes or brackets
                fields.push(current_field.trim().to_string());
                current_field.clear();
            }
            _ => current_field.push(ch),
        }
    }

    // Don't forget the last field
    if !current_field.is_empty() || !fields.is_empty() {
        fields.push(current_field.trim().to_string());
    }

    fields
}

pub struct BufferManager {
    pub pages: Vec<Page>, // In-memory pages (header + data)
}

impl BufferManager {
    pub fn new() -> Self {
        // Start with ONLY header page
        let mut pages = Vec::new();

        let mut header = Page::new();
        init_page(&mut header);
        pages.push(header);

        println!("Buffer Manager initialized with header page only.");

        Self { pages }
    }

    /// Allocate ONE new data page
    pub fn allocate_page(&mut self) {
        let mut page = Page::new();
        init_page(&mut page);
        self.pages.push(page);
    }

    /// Loads table from disk into buffer (opens an existing table)
    pub fn load_table_from_disk(&mut self, db_name: &str, table_name: &str) -> io::Result<()> {
        let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
        let mut file = File::open(&table_path)?;

        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let total_pages = (file_size as usize) / PAGE_SIZE;

        println!(
            "Loading table '{}' ({} bytes, {} pages)...",
            table_name, file_size, total_pages
        );

        // Reset in-memory buffer
        self.pages.clear();

        // Read header (page 0)
        let mut header_page = Page::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header_page.data)?;
        self.pages.push(header_page);

        // Read data pages
        for page_num in 1..total_pages {
            let mut page = Page::new();
            match read_page(&mut file, &mut page, page_num as u32) {
                Ok(_) => self.pages.push(page),
                Err(e) => {
                    if e.kind() == ErrorKind::UnexpectedEof {
                        break;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        println!(
            "Loaded {} pages (1 header + {} data).",
            self.pages.len(),
            self.pages.len().saturating_sub(1)
        );

        Ok(())
    }

    /// Load CSV into memory using page-based allocation
    pub fn load_csv_into_pages(
        &mut self,
        catalog: &Catalog,
        db_name: &str,
        table_name: &str,
        csv_path: &str,
    ) -> io::Result<usize> {
        // --- schema ---
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

        // --- read CSV ---
        let csv_file = File::open(csv_path)?;
        let reader = BufReader::new(csv_file);
        let mut lines = reader.lines();
        if let Some(Ok(_)) = lines.next() {} // skip header

        let mut inserted_rows = 0usize;
        let mut current_page_index = self.pages.len() - 1; // DATA pages start at index 1

        // Ensure first data page exists
        if self.pages.len() == 1 {
            self.allocate_page();
        }

        // Create ONE ToastManager for the entire CSV load (reused across all rows)
        let mut toast_manager = ToastManager::new();

        for (i, line) in lines.enumerate() {
            let row = line?;
            if row.trim().is_empty() {
                continue;
            }

            // Parse CSV fields, respecting bracket-delimited arrays and quoted fields
            let values = parse_csv_line(&row);
            let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
            
            if values_refs.len() != columns.len() {
                println!(
                    "Skipping row {}: expected {} columns, got {}",
                    i + 1,
                    columns.len(),
                    values_refs.len()
                );
                continue;
            }

            // Build schema for encoding
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

            // Parse and encode tuple using proper pipeline
            let mut parsed_values = Vec::new();
            let mut parse_error = false;
            
            for (val_str, (_, data_type)) in values_refs.iter().zip(schema.iter()) {
                match parse_value_literal(val_str, data_type) {
                    Ok(value) => parsed_values.push(value),
                    Err(e) => {
                        eprintln!("Error parsing value '{}' as {}: {}", val_str, data_type.to_string(), e);
                        parse_error = true;
                        break;
                    }
                }
            }
            
            if parse_error {
                continue;
            }

            // Encode tuple with proper structure (header, null bitmap, var fields, etc.)
            // Reuse the same toast_manager across all rows
            let tuple_bytes = match TupleCodec::encode_tuple(&parsed_values, &schema, &mut toast_manager) {
                Ok(bytes) => bytes,
                Err(e) => {
                    eprintln!("Error encoding tuple: {}", e);
                    continue;
                }
            };

            let tuple_len = tuple_bytes.len() as u32;
            let required = tuple_len + ITEM_ID_SIZE;

            loop {
                if current_page_index >= self.pages.len() {
                    self.allocate_page();
                }

                let page = &mut self.pages[current_page_index];
                let free = page_free_space(page)?;

                if free < required {
                    current_page_index += 1;
                    continue;
                }

                let mut lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
                let mut upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

                let start = upper - tuple_len;

                page.data[start as usize..upper as usize].copy_from_slice(&tuple_bytes);

                let item_id_pos = lower as usize;
                page.data[item_id_pos..item_id_pos + 4].copy_from_slice(&start.to_le_bytes());
                page.data[item_id_pos + 4..item_id_pos + 8]
                    .copy_from_slice(&tuple_len.to_le_bytes());

                lower += ITEM_ID_SIZE;
                upper = start;

                page.data[0..4].copy_from_slice(&lower.to_le_bytes());
                page.data[4..8].copy_from_slice(&upper.to_le_bytes());
                println!("Lower: {}", lower);
                inserted_rows += 1;
                break;
            }
        }

        let used_pages = self.pages.len();
        self.pages[0].data[0..4].copy_from_slice(&(used_pages as u32).to_le_bytes());

        println!(
            "Loaded {} rows into {} data pages.",
            inserted_rows,
            used_pages - 1
        );

        // Save TOAST chunks to disk
        let toast_path = format!("database/base/{}/{}.toast", db_name, table_name);
        if let Err(e) = toast_manager.save_to_disk(&toast_path) {
            eprintln!("Warning: Failed to save TOAST chunks to disk: {}", e);
        } else if inserted_rows > 0 {
            println!("Saved TOAST chunks to {}.", toast_path);
        }

        Ok(used_pages)
    }

    pub fn flush_to_disk(
        &mut self,
        db_name: &str,
        table_name: &str,
        used_pages: usize,
    ) -> io::Result<()> {
        let path = format!("database/base/{}/{}.dat", db_name, table_name);
        let mut file = File::options().write(true).open(&path)?;

        for (i, page) in self.pages.iter_mut().take(used_pages).enumerate() {
            write_page(&mut file, page, i as u32)?;
        }

        Ok(())
    }

    pub fn load_csv_to_buffer(
        &mut self,
        catalog: &Catalog,
        db_name: &str,
        table_name: &str,
        csv_path: &str,
    ) -> io::Result<()> {
        let used = self.load_csv_into_pages(catalog, db_name, table_name, csv_path)?;
        self.flush_to_disk(db_name, table_name, used)?;
        Ok(())
    }
}
