use crate::catalog::Catalog;
use crate::disk::{read_page, write_page};
use crate::page::{PAGE_SIZE, Page, init_page, page_free_space};
use crate::table::TABLE_HEADER_SIZE;

use std::fs::File;
use std::io::{self, BufRead, BufReader, ErrorKind, Read, Seek, SeekFrom, Write};

pub struct BufferManager {
    pub pages: Vec<Page>, // In-memory pages (header + data)
    pub header: Vec<u8>,  // Separate header copy (8192 bytes)
}

impl BufferManager {
    /// Initialize 100 pages (each page initialized)
    pub fn new() -> Self {
        let mut pages = Vec::with_capacity(100);
        for _ in 0..100 {
            let mut page = Page::new();
            init_page(&mut page);
            pages.push(page);
        }
        let header = vec![0u8; TABLE_HEADER_SIZE as usize];
        println!("Buffer Manager initialized with 100 pages (each page initialized).");
        Self { pages, header }
    }

    /// Load table into memory after create_table()
    pub fn load_table_on_create(&mut self, db_name: &str, table_name: &str) -> io::Result<()> {
        let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
        let mut file = File::open(&table_path)?;

        // Get file size and total pages
        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let total_pages = (file_size as usize) / PAGE_SIZE;

        println!(
            "Loading table '{}' ({} bytes, {} pages)...",
            table_name, file_size, total_pages
        );

        self.pages.clear();

        // 1️⃣ Read header page directly
        let mut header_page = Page::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header_page.data)?;
        self.pages.push(header_page);

        let total_pages_in_header =
            u32::from_le_bytes(self.pages[0].data[0..4].try_into().unwrap());
        println!("Header Page [0]: Total Pages = {}", total_pages_in_header);
        println!("Header First 16 bytes: {:?}", &self.pages[0].data[0..16]);

        // 2️⃣ Read remaining pages (1..total_pages)
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

        // 3️⃣ Show first data page info
        if self.pages.len() > 1 {
            let first_data = &self.pages[1];
            let lower = u32::from_le_bytes(first_data.data[0..4].try_into().unwrap());
            let upper = u32::from_le_bytes(first_data.data[4..8].try_into().unwrap());
            println!(
                "First Data Page [1]: Lower={}, Upper={}, Free={}",
                lower,
                upper,
                upper - lower
            );
        }

        // 4️⃣ Fill empty slots up to 100 pages
        while self.pages.len() < 100 {
            let mut new_page = Page::new();
            init_page(&mut new_page);
            self.pages.push(new_page);
        }

        println!(
            "Table '{}' loaded into buffer. {} data pages read, {} empty initialized.",
            table_name,
            total_pages.saturating_sub(1),
            100usize.saturating_sub(total_pages)
        );

        Ok(())
    }

    /// Load CSV into pages (schema-aware, skips header)
    pub fn load_csv_into_pages(
        &mut self,
        catalog: &Catalog,
        db_name: &str,
        table_name: &str,
        csv_path: &str,
    ) -> io::Result<usize> {
        use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, page_free_space};
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        // --- 1️⃣ Lookup schema ---
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

        // --- 2️⃣ Read CSV ---
        let csv_file = File::open(csv_path)?;
        let reader = BufReader::new(csv_file);
        let mut lines = reader.lines();
        if let Some(Ok(_)) = lines.next() {} // skip header

        let mut inserted_rows = 0usize;
        let mut current_page_index = 1; // start after header
        let mut used_pages = 1; // header + at least 1 data page

        // --- 3️⃣ Iterate CSV rows ---
        for (i, line) in lines.enumerate() {
            let row = line?;
            if row.trim().is_empty() {
                continue;
            }

            // --- 4️⃣ Parse & serialize tuple ---
            let values: Vec<&str> = row.split(',').map(|v| v.trim()).collect();
            if values.len() != columns.len() {
                println!(
                    "Skipping row {}: expected {} cols, got {}",
                    i + 1,
                    columns.len(),
                    values.len()
                );
                continue;
            }

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
                    _ => continue,
                }
            }

            let tuple_len = tuple_bytes.len() as u32;
            let total_required = tuple_len + ITEM_ID_SIZE;

            // --- 5️⃣ Insert into in-memory page (like insert_tuple) ---
            loop {
                if current_page_index >= self.pages.len() {
                    // allocate new in-memory page
                    let mut new_page = Page::new();
                    init_page(&mut new_page);
                    self.pages.push(new_page);
                    used_pages += 1;
                }

                let page = &mut self.pages[current_page_index];
                let free_space = page_free_space(page)?;

                if total_required > free_space {
                    // Not enough space → move to next page
                    current_page_index += 1;
                    used_pages = used_pages.max(current_page_index + 1);
                    continue;
                }

                // Offsets
                let mut lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
                let mut upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
                let start = upper - tuple_len;

                // Copy tuple data
                page.data[start as usize..upper as usize].copy_from_slice(&tuple_bytes);

                // Write ItemId (offset + length)
                let item_id_pos = lower as usize;
                page.data[item_id_pos..item_id_pos + 4].copy_from_slice(&start.to_le_bytes());
                page.data[item_id_pos + 4..item_id_pos + 8]
                    .copy_from_slice(&(tuple_len.to_le_bytes()));

                // Update lower/upper
                lower += ITEM_ID_SIZE;
                upper = start;
                page.data[0..4].copy_from_slice(&lower.to_le_bytes());
                page.data[4..8].copy_from_slice(&upper.to_le_bytes());

                inserted_rows += 1;
                break;
            }
        }

        // --- 6️⃣ Update header ---
        self.update_header_page_count(used_pages as u32);

        println!(
            "Successfully loaded {} rows into '{}.{}' using {} data pages (in-memory, directory-based).",
            inserted_rows,
            db_name,
            table_name,
            used_pages - 1
        );

        Ok(used_pages)
    }

    /// Update header page count
    fn update_header_page_count(&mut self, new_count: u32) {
       self.pages[0].data[0..4].copy_from_slice(&new_count.to_le_bytes());
        println!("Updated in-memory header page count to {}.", new_count);
    }

    /// Flush header + pages to disk
    fn flush_to_disk(
        &mut self,
        db_name: &str,
        table_name: &str,
        used_pages: usize,
    ) -> io::Result<()> {
        let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
        let mut file = File::options().write(true).open(&table_path)?;

        // ✅ Write all used pages starting from page 0 (header included)
        for (i, page) in self.pages.iter_mut().take(used_pages).enumerate() {
            write_page(&mut file, page, i as u32)?;
        }

        println!(
            "Flushed {} used pages (including header) of '{}' to disk.",
            used_pages, table_name
        );
        Ok(())
    }

    /// Full pipeline: load CSV -> update header -> flush to disk
    pub fn load_csv_to_buffer(
        &mut self,
        catalog: &Catalog,
        db_name: &str,
        table_name: &str,
        csv_path: &str,
    ) -> io::Result<()> {
        println!(
            "Starting buffered CSV load for '{}.{}'.",
            db_name, table_name
        );

        // Step 1️⃣: Load CSV → returns total pages used (including header)
        let used_pages = self.load_csv_into_pages(catalog, db_name, table_name, csv_path)?;

        // Step 2️⃣: Print first 10 in-memory pages (for debugging/inspection)
        println!("\n========== In-Memory Page Snapshot (first 10 pages) ==========");
        for (i, page) in self.pages.iter().enumerate().take(10) {
            let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
            let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
            let free = upper.saturating_sub(lower);

            if i == 0 {
                // Header Page
                let total_pages_in_header = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
                println!("Page [0] (Header Page):");
                println!(
                    "  Total Pages (stored in header): {}",
                    total_pages_in_header
                );
                println!("  First 32 bytes: {:?}", &page.data[0..32]);
            } else {
                println!(
                    "Page [{}]: lower={}, upper={}, free={} bytes",
                    i, lower, upper, free
                );
                println!("  First 32 bytes of data region: {:?}", &page.data[8..40]);
            }
            println!("---------------------------------------------------------------");
        }

        // Step 3️⃣: Flush only used pages to disk
        self.flush_to_disk(db_name, table_name, used_pages)?;

        Ok(())
    }
}
