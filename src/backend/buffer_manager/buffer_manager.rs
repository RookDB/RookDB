use crate::catalog::types::Catalog;
use crate::disk::{read_page, write_page};
use crate::ordered::ordered_file::{FileType, SortKeyEntry};
use crate::page::{init_page, page_free_space, Page, ITEM_ID_SIZE, PAGE_SIZE};
use crate::sorting::comparator::TupleComparator;

use std::fs::File;
use std::io::{self, BufRead, BufReader, ErrorKind, Read, Seek, SeekFrom};

pub struct BufferManager {
    pub pages: Vec<Page>, // In-memory pages (header + data)
    pub pool_size: usize, // Buffer pool size in pages (for external sort)
}

impl BufferManager {
    pub fn new() -> Self {
        // Start with ONLY header page
        let mut pages = Vec::new();

        let mut header = Page::new();
        init_page(&mut header);
        pages.push(header);

        println!("Buffer Manager initialized with header page only.");

        Self {
            pages,
            pool_size: 64, // default: 64 pages = 512 KB
        }
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

            // Serialize tuple
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

        // Check if table is ordered - if so, sort tuples before flushing
        let db = catalog.databases.get(db_name);
        let is_ordered = db
            .and_then(|d| d.tables.get(table_name))
            .and_then(|t| t.file_type.as_ref())
            .map(|ft| ft == "ordered")
            .unwrap_or(false);

        if is_ordered {
            if let Some(table) = db.and_then(|d| d.tables.get(table_name)) {
                if let Some(sort_keys) = &table.sort_keys {
                    let comparator = TupleComparator::new(table.columns.clone(), sort_keys.clone());

                    // Extract all tuples from data pages
                    let mut all_tuples: Vec<Vec<u8>> = Vec::new();
                    for page_idx in 1..self.pages.len() {
                        let page = &self.pages[page_idx];
                        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
                        let num_items = (lower - crate::page::PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

                        for i in 0..num_items {
                            let base = (crate::page::PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
                            let offset =
                                u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap())
                                    as usize;
                            let length = u32::from_le_bytes(
                                page.data[base + 4..base + 8].try_into().unwrap(),
                            ) as usize;
                            all_tuples.push(page.data[offset..offset + length].to_vec());
                        }
                    }

                    // Sort tuples
                    all_tuples.sort_by(|a, b| comparator.compare(a, b));

                    // Clear data pages and rewrite sorted tuples
                    self.pages.truncate(1); // keep header page

                    let mut current_page = Page::new();
                    init_page(&mut current_page);

                    for tuple in &all_tuples {
                        let tuple_len = tuple.len() as u32;
                        let required = tuple_len + ITEM_ID_SIZE;
                        let free = {
                            let lower =
                                u32::from_le_bytes(current_page.data[0..4].try_into().unwrap());
                            let upper =
                                u32::from_le_bytes(current_page.data[4..8].try_into().unwrap());
                            upper - lower
                        };

                        if required > free {
                            self.pages.push(current_page);
                            current_page = Page::new();
                            init_page(&mut current_page);
                        }

                        let mut lower =
                            u32::from_le_bytes(current_page.data[0..4].try_into().unwrap());
                        let mut upper =
                            u32::from_le_bytes(current_page.data[4..8].try_into().unwrap());

                        let start = upper - tuple_len;
                        current_page.data[start as usize..upper as usize].copy_from_slice(tuple);

                        current_page.data[lower as usize..lower as usize + 4]
                            .copy_from_slice(&start.to_le_bytes());
                        current_page.data[lower as usize + 4..lower as usize + 8]
                            .copy_from_slice(&tuple_len.to_le_bytes());

                        lower += ITEM_ID_SIZE;
                        upper = start;

                        current_page.data[0..4].copy_from_slice(&lower.to_le_bytes());
                        current_page.data[4..8].copy_from_slice(&upper.to_le_bytes());
                    }

                    self.pages.push(current_page);

                    // Write ordered file header into page 0
                    let total_page_count = self.pages.len() as u32;
                    let sort_key_entries: Vec<SortKeyEntry> = sort_keys
                        .iter()
                        .map(|sk| SortKeyEntry {
                            column_index: sk.column_index,
                            direction: match sk.direction {
                                crate::catalog::types::SortDirection::Ascending => 0,
                                crate::catalog::types::SortDirection::Descending => 1,
                            },
                        })
                        .collect();

                    // Build ordered header into page 0
                    let header_page = &mut self.pages[0];
                    header_page.data[0..4].copy_from_slice(&total_page_count.to_le_bytes());
                    header_page.data[4] = FileType::Ordered.to_u8();
                    header_page.data[5..9]
                        .copy_from_slice(&(sort_key_entries.len() as u32).to_le_bytes());
                    for (i, key) in sort_key_entries.iter().enumerate() {
                        let base = 9 + i * 5;
                        header_page.data[base..base + 4]
                            .copy_from_slice(&key.column_index.to_le_bytes());
                        header_page.data[base + 4] = key.direction;
                    }

                    println!(
                        "Sorted {} tuples for ordered table before flushing.",
                        all_tuples.len()
                    );

                    let used = self.pages.len();
                    self.flush_to_disk(db_name, table_name, used)?;
                    return Ok(());
                }
            }
        }

        self.flush_to_disk(db_name, table_name, used)?;
        Ok(())
    }
}
