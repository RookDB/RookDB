use crate::catalog::Catalog;
use crate::disk::{read_page, write_page};
use crate::page::{PAGE_SIZE, Page, init_page, page_free_space, ITEM_ID_SIZE, PAGE_HEADER_SIZE};
use crate::table::TABLE_HEADER_SIZE;

use std::fs::File;
use std::io::{self, BufRead, BufReader, ErrorKind, Read, Seek, SeekFrom, Write};

pub const EXTENT_SIZE: usize = 16;   // 16 data pages per extent

pub struct BufferManager {
    pub pages: Vec<Page>, // In-memory pages (header + data)
}

impl BufferManager {
    pub fn new() -> Self {
        // Start with ONLY header page + one extent? No — empty.
        let mut pages = Vec::new();

        // Create header page
        let mut header = Page::new();
        init_page(&mut header);
        pages.push(header);

        println!("Buffer Manager initialized with header page only.");

        Self { pages }
    }

    /// Add a full extent (16 initialized data pages) AFTER header page.
    pub fn allocate_extent(&mut self) {
        // println!("Allocating new EXTENT of {} pages...", EXTENT_SIZE);

        for _ in 0..EXTENT_SIZE {
            let mut page = Page::new();
            init_page(&mut page);
            self.pages.push(page); // push AFTER header
        }
    }

    /// Loads table from disk into buffer
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

        // Ensure buffer can grow when inserting
        println!(
            "Loaded {} pages (1 header + {} data).",
            self.pages.len(),
            self.pages.len().saturating_sub(1)
        );

        Ok(())
    }

    /// Load CSV into memory using extent-based allocation
    pub fn load_csv_into_pages(
        &mut self,
        catalog: &Catalog,
        db_name: &str,
        table_name: &str,
        csv_path: &str,
    ) -> io::Result<usize> {
        // --- schema ---
        let db = catalog.databases.get(db_name).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name))
        })?;
        let table = db.tables.get(table_name).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name))
        })?;
        let columns = &table.columns;

        if columns.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Table has no columns"));
        }

        // --- read CSV ---
        let csv_file = File::open(csv_path)?;
        let reader = BufReader::new(csv_file);
        let mut lines = reader.lines();
        if let Some(Ok(_)) = lines.next() {} // skip header

        let mut inserted_rows = 0usize;
        let mut current_page_index = 1; // DATA pages start at index 1 (page 0 is header)

        // Ensure first extent exists
        if self.pages.len() == 1 {
            self.allocate_extent(); // pages[1..17]
        }

        // --- iterate CSV ---
        for (i, line) in lines.enumerate() {
            let row = line?;
            if row.trim().is_empty() {
                continue;
            }

            let values: Vec<&str> = row.split(',').map(|v| v.trim()).collect();
            if values.len() != columns.len() {
                println!(
                    "Skipping row {}: expected {} columns, got {}",
                    i + 1,
                    columns.len(),
                    values.len()
                );
                continue;
            }

            // --- serialize row ---
            let mut tuple_bytes: Vec<u8> = Vec::new();
            for (val, col) in values.iter().zip(columns.iter()) {
                match col.data_type.as_str() {
                    "INT" => {
                        let num: i32 = val.parse().unwrap_or_default();
                        tuple_bytes.extend_from_slice(&num.to_le_bytes());
                    }
                    "TEXT" => {
                        let mut t = val.as_bytes().to_vec();
                        if t.len() > 10 {
                            t.truncate(10);
                        } else if t.len() < 10 {
                            t.extend(vec![b' '; 10 - t.len()]);
                        }
                        tuple_bytes.extend_from_slice(&t);
                    }
                    _ => continue,
                }
            }

            let tuple_len = tuple_bytes.len() as u32;
            let required = tuple_len + ITEM_ID_SIZE;

            // -------------------------
            // INSERT WITH EXTENT LOGIC
            // -------------------------
            loop {
                if current_page_index >= self.pages.len() {
                    self.allocate_extent();
                }

                let page = &mut self.pages[current_page_index];
                let free = page_free_space(page)?;

                if free < required {
                    // Move to next page
                    current_page_index += 1;

                    // If we just crossed an extent boundary, allocate next extent
                    if (current_page_index - 1) % EXTENT_SIZE == 0 {
                        self.allocate_extent();
                    }

                    continue;
                }

                // ---- Insert the tuple ----
                let mut lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
                let mut upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

                let start = upper - tuple_len;

                page.data[start as usize..upper as usize].copy_from_slice(&tuple_bytes);

                let item_id_pos = lower as usize;
                page.data[item_id_pos..item_id_pos + 4].copy_from_slice(&start.to_le_bytes());
                page.data[item_id_pos + 4..item_id_pos + 8]
                    .copy_from_slice(&(tuple_len.to_le_bytes()));

                lower += ITEM_ID_SIZE;
                upper = start;

                page.data[0..4].copy_from_slice(&lower.to_le_bytes());
                page.data[4..8].copy_from_slice(&upper.to_le_bytes());

                inserted_rows += 1;
                break;
            }
        }

        // number of used pages
        let used_pages = self.pages.len();

        // update header's page count
        self.pages[0].data[0..4].copy_from_slice(&(used_pages as u32).to_le_bytes());

        println!(
            "Loaded {} rows into {} data pages ({} extents).",
            inserted_rows,
            used_pages - 1,
            (used_pages - 1 + EXTENT_SIZE - 1) / EXTENT_SIZE
        );

        Ok(used_pages)
    }

    /// Write buffer to disk
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

    /// Full pipeline: load CSV into buffer → flush to disk
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
