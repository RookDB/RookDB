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
use std::fs::{self, OpenOptions};
use std::io;
use std::path::Path;

use crate::buffer_manager::{BufferManager, PageId};
use crate::catalog::types::CatalogError;
use crate::disk::create_page;
use crate::heap::init_table;
use crate::layout::{
    CATALOG_PAGES_DIR, PG_COLUMN_FILE, PG_CONSTRAINT_FILE, PG_DATABASE_FILE, PG_INDEX_FILE,
    PG_TABLE_FILE, PG_TYPE_FILE,
};
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page, page_free_space};

// ─────────────────────────────────────────────────────────────
// Page compaction helper
// ─────────────────────────────────────────────────────────────

/// Compact the tuple data area of a slotted page after a deletion.
///
/// All live slots (length > 0) have their tuple data repacked tightly at the
/// top of the data area.  Slot entries are updated with the new offsets and
/// the upper pointer is moved accordingly.  Deleted slots (length == 0) remain
/// as tombstones in the slot array; the lower pointer is unchanged.
fn compact_page_data(page: &mut Page) {
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap()) as usize;
    let num_slots = (lower - PAGE_HEADER_SIZE as usize) / ITEM_ID_SIZE as usize;
    let page_size = page.data.len();

    // Collect live tuples: (slot_index, owned_copy_of_data)
    let mut live: Vec<(usize, Vec<u8>)> = Vec::new();
    for slot in 0..num_slots {
        let base = PAGE_HEADER_SIZE as usize + slot * ITEM_ID_SIZE as usize;
        let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
        let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
        if length > 0 && offset + length <= page_size {
            live.push((slot, page.data[offset..offset + length].to_vec()));
        }
    }

    // Re-pack tuple data from page end downward (slot 0 lands nearest page end)
    let mut new_upper = page_size;
    for (slot, data) in live.iter() {
        new_upper -= data.len();
        page.data[new_upper..new_upper + data.len()].copy_from_slice(data);
        let base = PAGE_HEADER_SIZE as usize + slot * ITEM_ID_SIZE as usize;
        page.data[base..base + 4].copy_from_slice(&(new_upper as u32).to_le_bytes());
    }

    // Update upper pointer
    page.data[4..8].copy_from_slice(&(new_upper as u32).to_le_bytes());
}

// ─────────────────────────────────────────────────────────────
// Catalog name constants
// ─────────────────────────────────────────────────────────────

pub const CAT_DATABASE: &str = "pg_database";
pub const CAT_TABLE: &str = "pg_table";
pub const CAT_COLUMN: &str = "pg_column";
pub const CAT_CONSTRAINT: &str = "pg_constraint";
pub const CAT_INDEX: &str = "pg_index";
pub const CAT_TYPE: &str = "pg_type";

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
        fp.insert(CAT_DATABASE, PG_DATABASE_FILE);
        fp.insert(CAT_TABLE, PG_TABLE_FILE);
        fp.insert(CAT_COLUMN, PG_COLUMN_FILE);
        fp.insert(CAT_CONSTRAINT, PG_CONSTRAINT_FILE);
        fp.insert(CAT_INDEX, PG_INDEX_FILE);
        fp.insert(CAT_TYPE, PG_TYPE_FILE);
        CatalogPageManager { file_paths: fp }
    }

    // ──────────────────────────────────────────────────────────────
    // Initialization helpers
    // ──────────────────────────────────────────────────────────────

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

    fn create_catalog_file(&self, path: &str) -> Result<(), CatalogError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)?;

        init_table(&mut file)?;
        Ok(())
    }

    fn get_path(&self, catalog_name: &str) -> io::Result<String> {
        self.file_paths
            .get(catalog_name)
            .map(|s| s.to_string())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Unknown catalog: {}", catalog_name),
                )
            })
    }

    // ──────────────────────────────────────────────────────────────
    // CRUD operations
    // ──────────────────────────────────────────────────────────────

    pub fn insert_catalog_tuple(
        &mut self,
        bm: &mut BufferManager,
        catalog_name: &str,
        data: Vec<u8>,
    ) -> Result<(u32, u32), CatalogError> {
        let path = self.get_path(catalog_name)?;
        let header_fi = bm
            .pin_page(PageId::new(&path, 0))
            .map_err(CatalogError::IoError)?;
        let mut total_pages =
            u32::from_le_bytes(bm.frames[header_fi].data[0..4].try_into().unwrap());

        let mut last_page_num = total_pages - 1;
        let mut last_fi = bm
            .pin_page(PageId::new(&path, last_page_num))
            .map_err(CatalogError::IoError)?;

        let free_space = page_free_space(&bm.frames[last_fi]).map_err(CatalogError::IoError)?;
        let required = data.len() as u32 + ITEM_ID_SIZE;

        if required > free_space {
            bm.unpin_page(&PageId::new(&path, last_page_num), false)
                .map_err(CatalogError::IoError)?;

            let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
            create_page(&mut file)?;
            total_pages += 1;
            last_page_num = total_pages - 1;

            bm.frames[header_fi].data[0..4].copy_from_slice(&total_pages.to_le_bytes());
            bm.unpin_page(&PageId::new(&path, 0), true)
                .map_err(CatalogError::IoError)?;

            last_fi = bm
                .pin_page(PageId::new(&path, last_page_num))
                .map_err(CatalogError::IoError)?;
        } else {
            bm.unpin_page(&PageId::new(&path, 0), false)
                .map_err(CatalogError::IoError)?;
        }

        let page = &mut bm.frames[last_fi];
        let mut lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let mut upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

        let start = upper - data.len() as u32;
        page.data[start as usize..upper as usize].copy_from_slice(&data);

        upper = start;
        page.data[4..8].copy_from_slice(&upper.to_le_bytes());

        page.data[lower as usize..lower as usize + 4].copy_from_slice(&start.to_le_bytes());
        page.data[lower as usize + 4..lower as usize + 8]
            .copy_from_slice(&(data.len() as u32).to_le_bytes());

        let slot_id = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
        lower += ITEM_ID_SIZE;
        page.data[0..4].copy_from_slice(&lower.to_le_bytes());

        bm.unpin_page(&PageId::new(&path, last_page_num), true)
            .map_err(CatalogError::IoError)?;

        Ok((last_page_num, slot_id))
    }

    pub fn read_catalog_tuple(
        &self,
        bm: &mut BufferManager,
        catalog_name: &str,
        page_num: u32,
        slot_id: u32,
    ) -> Result<Vec<u8>, CatalogError> {
        let path = self.get_path(catalog_name)?;
        let fi = bm
            .pin_page(PageId::new(&path, page_num))
            .map_err(CatalogError::IoError)?;
        let page = &bm.frames[fi];

        let base = (PAGE_HEADER_SIZE + slot_id * ITEM_ID_SIZE) as usize;
        if base + 8 > page.data.len() {
            bm.unpin_page(&PageId::new(&path, page_num), false)
                .map_err(CatalogError::IoError)?;
            return Err(CatalogError::InvalidOperation("slot out of range".into()));
        }
        let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
        let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
        let data = page.data[offset..offset + length].to_vec();
        bm.unpin_page(&PageId::new(&path, page_num), false)
            .map_err(CatalogError::IoError)?;
        Ok(data)
    }

    pub fn update_catalog_tuple(
        &mut self,
        bm: &mut BufferManager,
        catalog_name: &str,
        page_num: u32,
        slot_id: u32,
        new_data: &[u8],
    ) -> Result<(u32, u32), CatalogError> {
        self.delete_catalog_tuple(bm, catalog_name, page_num, slot_id)?;
        self.insert_catalog_tuple(bm, catalog_name, new_data.to_vec())
    }

    pub fn scan_catalog(
        &self,
        bm: &mut BufferManager,
        catalog_name: &str,
    ) -> Result<Vec<Vec<u8>>, CatalogError> {
        let path = self.get_path(catalog_name)?;
        let header_fi = bm
            .pin_page(PageId::new(&path, 0))
            .map_err(CatalogError::IoError)?;
        let total = u32::from_le_bytes(bm.frames[header_fi].data[0..4].try_into().unwrap());
        bm.unpin_page(&PageId::new(&path, 0), false)
            .map_err(CatalogError::IoError)?;

        let mut results = Vec::new();

        for page_num in 1..total {
            let fi = match bm.pin_page(PageId::new(&path, page_num)) {
                Ok(ix) => ix,
                Err(_) => break,
            };
            let page = &bm.frames[fi];

            let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
            let num_slots = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

            for slot in 0..num_slots {
                let base = (PAGE_HEADER_SIZE + slot * ITEM_ID_SIZE) as usize;
                let offset =
                    u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
                let length =
                    u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;

                if length == 0 {
                    continue;
                }
                if offset + length > page.data.len() {
                    continue;
                }

                results.push(page.data[offset..offset + length].to_vec());
            }
            bm.unpin_page(&PageId::new(&path, page_num), false)
                .map_err(CatalogError::IoError)?;
        }
        Ok(results)
    }

    pub fn delete_catalog_tuple(
        &self,
        bm: &mut BufferManager,
        catalog_name: &str,
        page_num: u32,
        slot_id: u32,
    ) -> Result<(), CatalogError> {
        let path = self.get_path(catalog_name)?;
        let fi = bm
            .pin_page(PageId::new(&path, page_num))
            .map_err(CatalogError::IoError)?;
        {
            let page = &mut bm.frames[fi];
            let base = (PAGE_HEADER_SIZE + slot_id * ITEM_ID_SIZE) as usize;
            page.data[base + 4..base + 8].copy_from_slice(&0u32.to_le_bytes());
            compact_page_data(page);
        }
        bm.unpin_page(&PageId::new(&path, page_num), true)
            .map_err(CatalogError::IoError)?;
        Ok(())
    }

    pub fn find_catalog_tuple<F>(
        &self,
        bm: &mut BufferManager,
        catalog_name: &str,
        predicate: F,
    ) -> Result<Option<(u32, u32, Vec<u8>)>, CatalogError>
    where
        F: Fn(&[u8]) -> bool,
    {
        let path = self.get_path(catalog_name)?;
        let header_fi = bm
            .pin_page(PageId::new(&path, 0))
            .map_err(CatalogError::IoError)?;
        let total = u32::from_le_bytes(bm.frames[header_fi].data[0..4].try_into().unwrap());
        bm.unpin_page(&PageId::new(&path, 0), false)
            .map_err(CatalogError::IoError)?;

        for page_num in 1..total {
            let fi = match bm.pin_page(PageId::new(&path, page_num)) {
                Ok(ix) => ix,
                Err(_) => break,
            };

            let mut found = None;
            {
                let page = &bm.frames[fi];
                let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
                let num_slots = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

                for slot in 0..num_slots {
                    let base = (PAGE_HEADER_SIZE + slot * ITEM_ID_SIZE) as usize;
                    let offset =
                        u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
                    let length =
                        u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap())
                            as usize;
                    if length == 0 || offset + length > page.data.len() {
                        continue;
                    }
                    let tuple_bytes = &page.data[offset..offset + length];
                    if predicate(tuple_bytes) {
                        found = Some((page_num, slot, tuple_bytes.to_vec()));
                        break;
                    }
                }
            }
            bm.unpin_page(&PageId::new(&path, page_num), false)
                .map_err(CatalogError::IoError)?;
            if found.is_some() {
                return Ok(found);
            }
        }
        Ok(None)
    }
}
