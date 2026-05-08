use crate::disk::read_all_pages;
use crate::page::{PAGE_SIZE, Page, init_page};

use std::fs::File;
use std::io;

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

        log::info!("Buffer Manager initialized with header page only.");

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

        log::info!(
            "Loading table '{}' ({} bytes, {} pages)...",
            table_name, file_size, total_pages
        );

        // Load all pages from disk
        self.pages = read_all_pages(&mut file)?;

        log::info!(
            "Loaded {} pages (1 header + {} data).",
            self.pages.len(),
            self.pages.len().saturating_sub(1)
        );

        Ok(())
    }

}

