//! CatalogPageManager – CRUD over system-catalog page files.
//!
//! Each system catalog (pg_database, pg_table, …) is stored in its own
//! `.dat` file that follows the same slotted-page format as user tables.
//!
//! Layout of every catalog file
//! ─────────────────────────────
//!   page 0  – table header  (8192 bytes; first 4 bytes = total page count)
//!   page 1… – slotted data pages  (8192 bytes each)
//!
//! Every CRUD operation opens the target file directly; higher-level caching
//! is the responsibility of CatalogCache (catalog/cache.rs).

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::Path;

use crate::disk::{read_page, write_page};
use crate::heap::{init_table, insert_tuple};
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::table::page_count;
use crate::layout::{
    CATALOG_PAGES_DIR,
    PG_DATABASE_FILE, PG_TABLE_FILE, PG_COLUMN_FILE,
    PG_CONSTRAINT_FILE, PG_INDEX_FILE, PG_TYPE_FILE,
};
use crate::catalog::types::CatalogError;

// ─────────────────────────────────────────────────────────────
// Catalog name constants
// ─────────────────────────────────────────────────────────────

pub const CAT_DATABASE   : &str = "pg_database";
pub const CAT_TABLE      : &str = "pg_table";
pub const CAT_COLUMN     : &str = "pg_column";
pub const CAT_CONSTRAINT : &str = "pg_constraint";
pub const CAT_INDEX      : &str = "pg_index";
pub const CAT_TYPE       : &str = "pg_type";

// ─────────────────────────────────────────────────────────────
// Page-manager struct
// ─────────────────────────────────────────────────────────────

/// Maps catalog names to their file paths
pub type CatalogFilePaths = HashMap<&'static str, &'static str>;

pub struct CatalogPageManager {
    pub file_paths: CatalogFilePaths,
}

impl CatalogPageManager {
    // ──────────────────────────────────────────────────────────────
    // Construction
    // ──────────────────────────────────────────────────────────────

    pub fn new() -> Self {
        let mut fp: CatalogFilePaths = HashMap::new();
        fp.insert(CAT_DATABASE,   PG_DATABASE_FILE);
        fp.insert(CAT_TABLE,      PG_TABLE_FILE);
        fp.insert(CAT_COLUMN,     PG_COLUMN_FILE);
        fp.insert(CAT_CONSTRAINT, PG_CONSTRAINT_FILE);
        fp.insert(CAT_INDEX,      PG_INDEX_FILE);
        fp.insert(CAT_TYPE,       PG_TYPE_FILE);
        CatalogPageManager { file_paths: fp }
    }

    // ──────────────────────────────────────────────────────────────
    // Initialization helpers
    // ──────────────────────────────────────────────────────────────

    /// Create the catalog_pages directory and all six system catalog files
    /// if they do not already exist.
    pub fn initialize_files(&self) -> Result<(), CatalogError> {
        let dir = Path::new(CATALOG_PAGES_DIR);
        if !dir.exists() {
            fs::create_dir_all(dir)?;
        }
        for (_, path) in &self.file_paths {
            if !Path::new(path).exists() {
                self.create_catalog_file(path)?;
            }
        }
        Ok(())
    }

    /// Create a brand-new catalog file: table header (page 0) + one empty data page.
    /// Uses the same layout as user-table files so read_page/write_page work correctly:
    ///   page 0 = 8192-byte header  (first 4 bytes = page count)
    ///   page 1 = first slotted data page
    fn create_catalog_file(&self, path: &str) -> Result<(), CatalogError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)?;

        // init_table writes an 8192-byte header page (page count = 1) and then
        // appends one empty slotted data page – identical to user-table layout.
        init_table(&mut file)?;
        Ok(())
    }

    /// Open the catalog file in read+write mode
    fn open_file(&self, catalog_name: &str) -> io::Result<File> {
        let path = self.file_paths
            .get(catalog_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("Unknown catalog: {}", catalog_name)))?;
        OpenOptions::new().read(true).write(true).open(path)
    }

    // ──────────────────────────────────────────────────────────────
    // CRUD operations
    // ──────────────────────────────────────────────────────────────

    /// Append a tuple to the named system catalog.
    /// Returns the exact `(page_num, slot_id)` where the tuple was stored.
    pub fn insert_catalog_tuple(
        &mut self,
        catalog_name: &str,
        data: Vec<u8>,
    ) -> Result<(u32, u32), CatalogError> {
        let mut file = self.open_file(catalog_name)?;
        insert_tuple(&mut file, &data)?;

        // After insert_tuple completes the tuple is the last slot on the last data page.
        // Compute slot_id from the updated `lower` pointer.
        let total    = page_count(&mut file)?;
        let page_num = total - 1;
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num)?;
        let lower   = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let slot_id = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE - 1;
        Ok((page_num, slot_id))
    }

    /// Read a specific tuple from a catalog page by slot index.
    pub fn read_catalog_tuple(
        &self,
        catalog_name: &str,
        page_num: u32,
        slot_id: u32,
    ) -> Result<Vec<u8>, CatalogError> {
        let mut file = self.open_file(catalog_name)?;
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num)?;

        let base = (PAGE_HEADER_SIZE + slot_id * ITEM_ID_SIZE) as usize;
        if base + 8 > page.data.len() {
            return Err(CatalogError::InvalidOperation("slot out of range".into()));
        }
        let offset = u32::from_le_bytes(page.data[base..base+4].try_into().unwrap()) as usize;
        let length = u32::from_le_bytes(page.data[base+4..base+8].try_into().unwrap()) as usize;
        Ok(page.data[offset..offset+length].to_vec())
    }

    /// Replace the tuple at (page_num, slot_id) with new_data.
    ///
    /// Because catalog tuples are variable-length, a simple in-place overwrite
    /// only works when the encoded size happens to be identical.  To handle the
    /// general case this method employs a **delete-then-reinsert** strategy:
    ///
    /// 1. Zero the old slot's length field (logical delete – preserves slot IDs
    ///    held by other in-flight callers and leaves the offset intact).
    /// 2. Append new_data as a fresh tuple at the end of the last data page.
    ///
    /// Returns the `(page_num, slot_id)` of the newly inserted tuple so callers
    /// can update any cached location information.
    pub fn update_catalog_tuple(
        &mut self,
        catalog_name: &str,
        page_num: u32,
        slot_id: u32,
        new_data: &[u8],
    ) -> Result<(u32, u32), CatalogError> {
        // Step 1 – logical delete of old slot.
        self.delete_catalog_tuple(catalog_name, page_num, slot_id)?;

        // Step 2 – append new tuple; return its exact location.
        let mut file = self.open_file(catalog_name)?;
        insert_tuple(&mut file, new_data)?;

        let total       = page_count(&mut file)?;
        let new_page    = total - 1;
        let mut page    = Page::new();
        read_page(&mut file, &mut page, new_page)?;
        let lower       = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let new_slot    = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE - 1;
        Ok((new_page, new_slot))
    }

    /// Scan all data pages of a catalog file and collect every tuple.
    pub fn scan_catalog(&self, catalog_name: &str) -> Result<Vec<Vec<u8>>, CatalogError> {
        let mut file = self.open_file(catalog_name)?;
        let total = page_count(&mut file)?;
        let mut results = Vec::new();

        for page_num in 1..total {
            let mut page = Page::new();
            match read_page(&mut file, &mut page, page_num) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(CatalogError::IoError(e)),
            }

            let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
            let num_slots = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

            for slot in 0..num_slots {
                let base   = (PAGE_HEADER_SIZE + slot * ITEM_ID_SIZE) as usize;
                let offset = u32::from_le_bytes(page.data[base..base+4].try_into().unwrap()) as usize;
                let length = u32::from_le_bytes(page.data[base+4..base+8].try_into().unwrap()) as usize;

                if length == 0 { continue; }  // deleted/empty slot
                if offset + length > page.data.len() { continue; }

                results.push(page.data[offset..offset+length].to_vec());
            }
        }
        Ok(results)
    }

    /// Delete a tuple by zero-ing its length in the slot directory (logical delete).
    pub fn delete_catalog_tuple(
        &self,
        catalog_name: &str,
        page_num: u32,
        slot_id: u32,
    ) -> Result<(), CatalogError> {
        let mut file = self.open_file(catalog_name)?;
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num)?;
        let base = (PAGE_HEADER_SIZE + slot_id * ITEM_ID_SIZE) as usize;
        // Zero the length field to mark as deleted
        page.data[base+4..base+8].copy_from_slice(&0u32.to_le_bytes());
        write_page(&mut file, &mut page, page_num)?;
        Ok(())
    }

    /// Find the (page_num, slot_id) of the first tuple that matches a predicate.
    pub fn find_catalog_tuple<F>(
        &self,
        catalog_name: &str,
        predicate: F,
    ) -> Result<Option<(u32, u32, Vec<u8>)>, CatalogError>
    where F: Fn(&[u8]) -> bool
    {
        let mut file = self.open_file(catalog_name)?;
        let total = page_count(&mut file)?;

        for page_num in 1..total {
            let mut page = Page::new();
            match read_page(&mut file, &mut page, page_num) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(CatalogError::IoError(e)),
            }
            let lower     = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
            let num_slots = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

            for slot in 0..num_slots {
                let base   = (PAGE_HEADER_SIZE + slot * ITEM_ID_SIZE) as usize;
                let offset = u32::from_le_bytes(page.data[base..base+4].try_into().unwrap()) as usize;
                let length = u32::from_le_bytes(page.data[base+4..base+8].try_into().unwrap()) as usize;
                if length == 0 { continue; }
                if offset + length > page.data.len() { continue; }
                let tuple_bytes = &page.data[offset..offset+length];
                if predicate(tuple_bytes) {
                    return Ok(Some((page_num, slot, tuple_bytes.to_vec())));
                }
            }
        }
        Ok(None)
    }
}
