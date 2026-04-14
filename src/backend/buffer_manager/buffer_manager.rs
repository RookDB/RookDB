//! Page buffer manager with pin/unpin semantics and dirty-page tracking.
//!
//! The BufferManager serves two roles:
//!  1. **Legacy path** – table-specific bulk loading used by the CSV importer
//!     and the initial table open.  The `pages` Vec holds ALL pages of a single
//!     open table.
//!  2. **General path** – LRU multi-file buffer pool with pin/unpin, used by
//!     the catalog page manager and any future scan operators.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, ErrorKind, Read, Seek, SeekFrom};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::catalog::types::Catalog;
use crate::disk::{read_page, write_page};
use crate::page::{ITEM_ID_SIZE, PAGE_SIZE, Page, init_page, page_free_space};

// ─────────────────────────────────────────────────────────────
// New: PageId, PageMetadata, BufferPool
// ─────────────────────────────────────────────────────────────

/// Unique identifier for a page across all files.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PageId {
    pub file_path: String,
    pub page_num: u32,
}

impl PageId {
    pub fn new(file_path: impl Into<String>, page_num: u32) -> Self {
        PageId {
            file_path: file_path.into(),
            page_num,
        }
    }
    /// Canonical string key used in the page table hash map.
    fn key(&self) -> String {
        format!("{}:{}", self.file_path, self.page_num)
    }
}

/// Per-frame metadata for the general buffer pool.
#[derive(Debug, Clone)]
pub struct PageMetadata {
    pub page_id: PageId,
    /// Number of active holders.  Page cannot be evicted when > 0.
    pub pin_count: u32,
    /// True if the frame has been modified since it was loaded from disk.
    pub is_dirty: bool,
    /// Unix timestamp of last access (seconds).
    pub last_accessed: u64,
    /// Index into `BufferManager::frames`.
    pub frame_index: usize,
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ─────────────────────────────────────────────────────────────
// BufferManager
// ─────────────────────────────────────────────────────────────

pub struct BufferManager {
    // ── Legacy path (single-table) ────────────────────────────
    /// In-memory pages for the currently loaded table (header + data)
    pub pages: Vec<Page>,

    // ── General buffer pool ────────────────────────────────────
    /// Buffer frames (parallel to `page_metadata_list`)
    pub frames: Vec<Page>,
    /// page_id.key() → PageMetadata
    pub page_table: HashMap<String, PageMetadata>,
    /// LRU order: back = most recently used
    lru_order: Vec<String>,
    pub max_pages: usize,
}

impl BufferManager {
    pub fn new() -> Self {
        let mut pages = Vec::new();
        let mut header = Page::new();
        init_page(&mut header);
        pages.push(header);
        println!("Buffer Manager initialized with header page only.");
        Self {
            pages,
            frames: Vec::new(),
            page_table: HashMap::new(),
            lru_order: Vec::new(),
            max_pages: 256,
        }
    }

    // ──────────────────────────────────────────────────────────────
    // General buffer pool: pin / unpin / flush
    // ──────────────────────────────────────────────────────────────

    /// Pin a page into the buffer pool and return a mutable reference to the
    /// frame index.  Increments the pin count; loads from disk if not cached.
    pub fn pin_page(&mut self, page_id: PageId) -> io::Result<usize> {
        let key = page_id.key();

        if let Some(meta) = self.page_table.get_mut(&key) {
            meta.pin_count += 1;
            meta.last_accessed = now_secs();
            self.lru_order.retain(|k| k != &key);
            self.lru_order.push(key);
            return Ok(meta.frame_index);
        }

        // Not in buffer – load from disk
        let frame_index = self.allocate_frame(&key)?;

        let mut file = File::open(&page_id.file_path)?;
        read_page(&mut file, &mut self.frames[frame_index], page_id.page_num)?;

        let meta = PageMetadata {
            page_id: page_id.clone(),
            pin_count: 1,
            is_dirty: false,
            last_accessed: now_secs(),
            frame_index,
        };
        self.page_table.insert(key.clone(), meta);
        self.lru_order.push(key);
        Ok(frame_index)
    }

    /// Decrement the pin count for `page_id`.  If `is_dirty`, marks the frame
    /// so it will be written back on the next flush.
    pub fn unpin_page(&mut self, page_id: &PageId, is_dirty: bool) -> io::Result<()> {
        let key = page_id.key();
        if let Some(meta) = self.page_table.get_mut(&key) {
            if meta.pin_count > 0 {
                meta.pin_count -= 1;
            }
            if is_dirty {
                meta.is_dirty = true;
            }
        }
        Ok(())
    }

    /// Write all dirty frames back to their respective files.
    pub fn flush_pages(&mut self) -> io::Result<()> {
        // Collect dirty frames first to avoid borrow conflicts
        let dirty: Vec<(String, String, u32, usize)> = self
            .page_table
            .values()
            .filter(|m| m.is_dirty)
            .map(|m| {
                (
                    m.page_id.key(),
                    m.page_id.file_path.clone(),
                    m.page_id.page_num,
                    m.frame_index,
                )
            })
            .collect();

        for (key, path, page_num, fi) in dirty {
            let mut file = std::fs::OpenOptions::new().write(true).open(&path)?;
            write_page(&mut file, &mut self.frames[fi], page_num)?;
            if let Some(meta) = self.page_table.get_mut(&key) {
                meta.is_dirty = false;
            }
        }
        Ok(())
    }

    /// Evict the oldest unpinned page to free a buffer frame.
    fn evict_page(&mut self) -> io::Result<usize> {
        // Find oldest unpinned entry in LRU order
        let evict_key = self
            .lru_order
            .iter()
            .find(|k| {
                self.page_table
                    .get(*k)
                    .map(|m| m.pin_count == 0)
                    .unwrap_or(false)
            })
            .cloned();

        if let Some(key) = evict_key {
            let meta = self.page_table.remove(&key).unwrap();
            if meta.is_dirty {
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&meta.page_id.file_path)?;
                write_page(
                    &mut file,
                    &mut self.frames[meta.frame_index],
                    meta.page_id.page_num,
                )?;
            }
            self.lru_order.retain(|k| k != &key);
            return Ok(meta.frame_index);
        }
        Err(io::Error::new(
            io::ErrorKind::Other,
            "No unpinned frames available for eviction",
        ))
    }

    /// Return a free frame index, evicting if necessary.
    fn allocate_frame(&mut self, _key: &str) -> io::Result<usize> {
        if self.frames.len() < self.max_pages {
            let fi = self.frames.len();
            self.frames.push(Page::new());
            return Ok(fi);
        }
        self.evict_page()
    }

    // ──────────────────────────────────────────────────────────────
    // Legacy path (single-table)
    // ──────────────────────────────────────────────────────────────

    /// Allocate ONE new data page in the legacy single-table buffer.
    pub fn allocate_page(&mut self) {
        let mut page = Page::new();
        init_page(&mut page);
        self.pages.push(page);
    }

    /// Load a table from disk into the legacy single-table buffer.
    pub fn load_table_from_disk(&mut self, db_name: &str, table_name: &str) -> io::Result<()> {
        let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
        let mut file = File::open(&table_path)?;
        let file_size = file.metadata()?.len();
        let total_pages = (file_size as usize) / PAGE_SIZE;

        println!(
            "Loading table '{}' ({} bytes, {} pages)...",
            table_name, file_size, total_pages
        );
        self.pages.clear();

        let mut header = Page::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut header.data)?;
        self.pages.push(header);

        for page_num in 1..total_pages {
            let mut page = Page::new();
            match read_page(&mut file, &mut page, page_num as u32) {
                Ok(_) => self.pages.push(page),
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }
        println!(
            "Loaded {} pages (1 header + {} data).",
            self.pages.len(),
            self.pages.len().saturating_sub(1)
        );
        Ok(())
    }

    /// Load CSV rows into the legacy page buffer and return the number of pages used.
    pub fn load_csv_into_pages(
        &mut self,
        catalog: &Catalog,
        db_name: &str,
        table_name: &str,
        csv_path: &str,
    ) -> io::Result<usize> {
        // Fetch schema from catalog
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

        let csv_file = File::open(csv_path)?;
        let reader = BufReader::new(csv_file);
        let mut lines = reader.lines();
        if let Some(Ok(_)) = lines.next() {} // skip header

        let mut inserted_rows = 0usize;
        let mut current_page_ix = self.pages.len() - 1;
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

            let mut tuple_bytes: Vec<u8> = Vec::new();
            for (val, col) in values.iter().zip(columns.iter()) {
                let type_name = col.data_type.type_name.to_uppercase();
                match type_name.as_str() {
                    "INT" | "INTEGER" => {
                        let num: i32 = val.parse().unwrap_or_default();
                        tuple_bytes.extend_from_slice(&num.to_le_bytes());
                    }
                    "BIGINT" => {
                        let num: i64 = val.parse().unwrap_or_default();
                        tuple_bytes.extend_from_slice(&num.to_le_bytes());
                    }
                    "FLOAT" | "REAL" => {
                        let num: f32 = val.parse().unwrap_or_default();
                        tuple_bytes.extend_from_slice(&num.to_le_bytes());
                    }
                    "DOUBLE" => {
                        let num: f64 = val.parse().unwrap_or_default();
                        tuple_bytes.extend_from_slice(&num.to_le_bytes());
                    }
                    "BOOL" | "BOOLEAN" => {
                        let b: u8 = match val.to_lowercase().as_str() {
                            "true" | "1" | "yes" => 1,
                            _ => 0,
                        };
                        tuple_bytes.push(b);
                    }
                    t if t.starts_with("VARCHAR") => {
                        // Extract max length from VARCHAR(n), default 255
                        let max_len: usize = t
                            .strip_prefix("VARCHAR(")
                            .and_then(|s| s.strip_suffix(')'))
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(255);
                        let mut bytes = val.as_bytes().to_vec();
                        bytes.truncate(max_len);
                        // store as 2-byte length prefix + data
                        tuple_bytes.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                        tuple_bytes.extend_from_slice(&bytes);
                    }
                    _ => {
                        // Default TEXT: fixed 10-byte field
                        let mut t = val.as_bytes().to_vec();
                        if t.len() > 10 {
                            t.truncate(10);
                        } else {
                            t.extend(vec![b' '; 10 - t.len()]);
                        }
                        tuple_bytes.extend_from_slice(&t);
                    }
                }
            }

            let tuple_len = tuple_bytes.len() as u32;
            let required = tuple_len + ITEM_ID_SIZE;

            loop {
                if current_page_ix >= self.pages.len() {
                    self.allocate_page();
                }
                let page = &mut self.pages[current_page_ix];
                let free = page_free_space(page)?;
                if free < required {
                    current_page_ix += 1;
                    continue;
                }

                let mut lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
                let mut upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
                let start = upper - tuple_len;

                page.data[start as usize..upper as usize].copy_from_slice(&tuple_bytes);
                let ip = lower as usize;
                page.data[ip..ip + 4].copy_from_slice(&start.to_le_bytes());
                page.data[ip + 4..ip + 8].copy_from_slice(&tuple_len.to_le_bytes());
                lower += ITEM_ID_SIZE;
                upper = start;
                page.data[0..4].copy_from_slice(&lower.to_le_bytes());
                page.data[4..8].copy_from_slice(&upper.to_le_bytes());
                inserted_rows += 1;
                break;
            }
        }

        let used = self.pages.len();
        self.pages[0].data[0..4].copy_from_slice(&(used as u32).to_le_bytes());
        println!(
            "Loaded {} rows into {} data pages.",
            inserted_rows,
            used - 1
        );
        Ok(used)
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
        self.flush_to_disk(db_name, table_name, used)
    }
}
