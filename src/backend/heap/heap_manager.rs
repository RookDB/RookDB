/// HeapManager - High-level API for table operations.
/// 
/// This module provides a complete interface for:
/// - Inserting tuples using FSM-guided page selection (not append-only)
/// - Retrieving tuples by (page_id, slot_id)
/// - Sequential scans across all pages
/// - Automatic allocation of new pages with FSM registration
/// 
/// Key Design:
/// - Encapsulates FSM complexity; FSM-driven inserts spread load across pages
/// - Uses fp_next_slot for load-spreading hint
/// - Header persistence survives crashes; FSM fork is a hint (can be rebuilt)

use std::fs::{File, OpenOptions};
use std::io::{self, Write, Seek, SeekFrom};
use std::path::PathBuf;

use crate::backend::fsm::FSM;
use crate::backend::page::{Page, PAGE_SIZE, PAGE_HEADER_SIZE, ITEM_ID_SIZE, 
                            init_page, page_free_space, get_tuple_count, get_slot_entry};
use crate::backend::disk::{read_page, write_page, read_header_page, update_header_page};
use crate::heap::types::HeaderMetadata;

// ─────────────────────────────────────────────────────────────────────────
// HeapScanIterator - Sequential Scan
// ─────────────────────────────────────────────────────────────────────────

/// Memory-efficient sequential scan iterator.
/// Lazily loads pages as needed, yielding (page_id, slot_id, tuple_data).
pub struct HeapScanIterator {
    file_path: PathBuf,
    current_page: u32,
    current_slot: u32,
    total_pages: u32,
    cached_page: Option<(u32, Page)>, // (page_id, page_data)
}

impl HeapScanIterator {
    /// Create a new scan iterator starting at page 1 (Page 0 is header).
    fn new(file_path: PathBuf, total_pages: u32) -> Self {
        log::trace!(
            "[HeapScanIterator::new] Created iterator for {} pages",
            total_pages
        );
        Self {
            file_path,
            current_page: 1, // Skip header page
            current_slot: 0,
            total_pages,
            cached_page: None,
        }
    }

    /// Load a specific page into cache.
    fn load_page(&mut self, page_id: u32) -> io::Result<()> {
        log::trace!("[HeapScanIterator::load_page] Loading page {}", page_id);
        
        let mut file = OpenOptions::new()
            .read(true)
            .open(&self.file_path)?;
        
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_id)?;
        
        self.cached_page = Some((page_id, page));
        Ok(())
    }
}

impl Iterator for HeapScanIterator {
    type Item = io::Result<(u32, u32, Vec<u8>)>; // (page_id, slot_id, data)

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Check if we've reached the end
            if self.current_page >= self.total_pages {
                log::trace!("[HeapScanIterator::next] End of scan reached");
                return None;
            }

            // Load page if not cached or if we moved to a new page
            if self.cached_page.is_none() || self.cached_page.as_ref().unwrap().0 != self.current_page
            {
                if let Err(e) = self.load_page(self.current_page) {
                    return Some(Err(e));
                }
            }

            let (page_id, page) = self.cached_page.as_ref().unwrap();

            // Get tuple count for current page
            let tuple_count = match get_tuple_count(page) {
                Ok(count) => count,
                Err(e) => return Some(Err(e)),
            };

            // Check if we've exhausted tuples in this page
            if self.current_slot >= tuple_count {
                log::trace!(
                    "[HeapScanIterator::next] Page {} exhausted, moving to next",
                    self.current_page
                );
                self.current_page += 1;
                self.current_slot = 0;
                self.cached_page = None;
                continue;
            }

            // Extract the tuple
            let slot_id = self.current_slot;
            let (offset, length) = match get_slot_entry(page, slot_id) {
                Ok((o, l)) => (o, l),
                Err(e) => return Some(Err(e)),
            };

            // Validate bounds
            if offset as usize + length as usize > PAGE_SIZE {
                return Some(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Tuple bounds invalid: offset={}, length={}, page_size={}",
                        offset, length, PAGE_SIZE
                    ),
                )));
            }

            let data = page.data[offset as usize..(offset + length) as usize].to_vec();

            log::trace!(
                "[HeapScanIterator::next] Yielding page={}, slot={}, data_len={}",
                page_id, slot_id, data.len()
            );

            self.current_slot += 1;

            return Some(Ok((*page_id, slot_id, data)));
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// HeapManager - Main API
// ─────────────────────────────────────────────────────────────────────────

/// High-level heap file manager with FSM-guided insertion.
pub struct HeapManager {
    file_path: PathBuf,
    file_handle: File,
    fsm: FSM,
    #[doc(hidden)]
    pub header: HeaderMetadata,
}

impl HeapManager {
    /// Create a new heap file and initialize with empty pages.
    /// 
    /// # Arguments
    /// * `file_path` - Path where the new heap file will be created
    /// 
    /// # Returns
    /// HeapManager instance ready for inserts or error
    /// 
    /// # Steps
    /// 1. Create new file
    /// 2. Write header page with initial metadata
    /// 3. Create first data page (Page 1)
    /// 4. Create and initialize FSM fork
    pub fn create(file_path: PathBuf) -> io::Result<Self> {
        log::trace!("[HeapManager::create] Creating new heap file at {:?}", file_path);

        // Remove existing file if present
        if file_path.exists() {
            std::fs::remove_file(&file_path)?;
        }

        // Create new file
        let mut file_handle = File::create(&file_path)?;

        // Create initial header
        let header = HeaderMetadata::new();
        let header_bytes = header.serialize()?;

        // Write header page (20 bytes metadata + padding)
        let mut header_page = vec![0u8; PAGE_SIZE];
        header_page[0..20].copy_from_slice(&header_bytes);
        file_handle.write_all(&header_page)?;

        // Create first data page (Page 1)
        let mut data_page = Page::new();
        init_page(&mut data_page);
        file_handle.write_all(&data_page.data)?;

        file_handle.flush()?;
        file_handle.sync_all()?;

        log::trace!("[HeapManager::create] Heap file created, initializing FSM");

        // Derive FSM path
        let fsm_path = PathBuf::from(format!(
            "{}.fsm",
            file_path.to_string_lossy()
        ));

        // Create FSM fork
        let fsm = FSM::open(fsm_path.clone(), 2)?; // 2 pages initially
        
        let mut manager = Self {
            file_path,
            file_handle,
            fsm,
            header,
        };

        // Set heap page count to 2 (Page 0 + Page 1)
        manager.header.page_count = 2;
        manager.fsm.set_heap_page_count(2);

        // Register first data page with FSM
        let initial_free = PAGE_SIZE as u32 - PAGE_HEADER_SIZE;
        manager.fsm.fsm_set_avail(1, initial_free)?;

        // Persist changes
        manager.flush()?;

        log::trace!("[HeapManager::create] HeapManager successfully created");

        Ok(manager)
    }

    /// Open an existing heap file and initialize FSM fork.
    /// 
    /// # Arguments
    /// * `file_path` - Path to the heap file (table).dat)
    /// 
    /// # Returns
    /// HeapManager instance or error
    /// 
    /// # Steps
    /// 1. Open heap file
    /// 2. Read header metadata
    /// 3. Open/rebuild FSM fork
    pub fn open(file_path: PathBuf) -> io::Result<Self> {
        log::trace!("[HeapManager::open] Opening heap file {:?}", file_path);

        // Verify path exists
        if !file_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Heap file not found: {:?}", file_path),
            ));
        }

        // Open heap file
        let mut file_handle = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)?;

        // Read header
        let mut header = read_header_page(&mut file_handle)?;
        log::trace!(
            "[HeapManager::open] Read header: page_count={}, fsm_page_count={}, total_tuples={}",
            header.page_count, header.fsm_page_count, header.total_tuples
        );

        // Derive FSM path 
        // Note: We use .fsm suffix directly. 
        // If file_path is "table.dat", fsm_path is "table.dat.fsm"
        let mut fsm_path = file_path.to_string_lossy().into_owned();
        if !fsm_path.ends_with(".fsm") {
            fsm_path.push_str(".fsm");
        }
        let fsm_path = PathBuf::from(fsm_path);

        // Open/rebuild FSM
        let fsm = FSM::open(fsm_path.clone(), header.page_count)?;
        
        // ===== SYNC FSM PAGE COUNT TO HEADER =====
        // Calculate actual FSM page count based on current heap size
        let calculated_fsm_pages = FSM::calculate_fsm_page_count(header.page_count);
        
        // Only update if different
        if header.fsm_page_count != calculated_fsm_pages {
            log::trace!(
                "[HeapManager::open] FSM page count mismatch: header={}, calculated={}. Updating...",
                header.fsm_page_count, calculated_fsm_pages
            );
            header.fsm_page_count = calculated_fsm_pages;
            
            // Persist updated header to disk
            update_header_page(&mut file_handle, &header)?;
            log::trace!("[HeapManager::open] Updated and persisted fsm_page_count to {}", calculated_fsm_pages);
        }

        log::trace!("[HeapManager::open] Successfully opened HeapManager");

        Ok(Self {
            file_path,
            file_handle,
            fsm,
            header,
        })
    }

    /// Insert a tuple using FSM-guided page selection.
    /// 
    /// # Arguments
    /// * `tuple_data` - The tuple bytes to insert
    /// 
    /// # Returns
    /// (page_id, slot_id) of the inserted tuple or error
    /// 
    /// # Steps
    /// 1. Calculate min_category from tuple size
    /// 2. Call fsm_search_avail to find suitable page
    /// 3. If None returned, allocate new page
    /// 4. Insert tuple into slotted page
    /// 5. Update FSM with new free space
    /// 6. Increment total_tuples counter
    pub fn insert_tuple(&mut self, tuple_data: &[u8]) -> io::Result<(u32, u32)> {
        use crate::backend::instrumentation::HEAP_METRICS;
        HEAP_METRICS.insert_tuple_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        log::trace!(
            "[HeapManager::insert_tuple] Inserting tuple of {} bytes",
            tuple_data.len()
        );

        // Validate input
        if tuple_data.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Tuple data cannot be empty",
            ));
        }

        if tuple_data.len() > PAGE_SIZE - PAGE_HEADER_SIZE as usize - ITEM_ID_SIZE as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Tuple too large: {} > {}",
                    tuple_data.len(),
                    PAGE_SIZE - PAGE_HEADER_SIZE as usize
                ),
            ));
        }

        // Calculate required space (tuple data + slot entry)
        let required_bytes = (tuple_data.len() as u32) + ITEM_ID_SIZE;
        let min_category = Self::bytes_to_category(required_bytes);

        log::trace!(
            "[HeapManager::insert_tuple] min_category required: {}",
            min_category
        );

        // Track pages that failed insertion to avoid retrying them
        let mut failed_pages = Vec::new();

        // Try up to 3 times to find or allocate a suitable page
        for attempt in 0..3 {
            log::trace!("[HeapManager::insert_tuple] Attempt {}/3", attempt + 1);
            
            // Get a page to try
            let page_id = match self.fsm.fsm_search_avail(min_category)? {
                Some(pid) => {
                    // Check if this page failed before
                    if failed_pages.contains(&pid) {
                        log::trace!(
                            "[HeapManager::insert_tuple] Page {} previously failed for this insert, allocating new",
                            pid
                        );
                        // This page failed before, don't try it again
                        if attempt < 2 {
                            log::trace!("[HeapManager::insert_tuple] Retrying with fresh search");
                            continue;
                        } else {
                            log::trace!("[HeapManager::insert_tuple] Final attempt: allocating new page");
                            self.allocate_new_page()?
                        }
                    } else {
                        log::trace!(
                            "[HeapManager::insert_tuple] FSM search returned page: {}",
                            pid
                        );
                        pid
                    }
                }
                None => {
                    if attempt < 2 {
                        log::trace!("[HeapManager::insert_tuple] FSM returned None, will retry");
                        continue;
                    } else {
                        log::trace!("[HeapManager::insert_tuple] Final attempt: allocating new page");
                        self.allocate_new_page()?
                    }
                }
            };

            // Verify page_id is valid
            if page_id >= self.header.page_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Invalid page_id: {} >= {}",
                        page_id, self.header.page_count
                    ),
                ));
            }

            // Read the target page (reopen file to ensure clean state)
            let mut page = Page::new();
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&self.file_path)?;
            
            if let Err(e) = read_page(&mut file, &mut page, page_id) {
                eprintln!("[ERROR] Failed to read page {}: {}", page_id, e);
                return Err(e);
            }

            // Get current free space and verify
            let current_free = page_free_space(&page)?;
            if current_free < required_bytes {
                log::trace!(
                    "[HeapManager::insert_tuple] Page {} has only {} bytes free, needs {} - will retry with new page",
                    page_id, current_free, required_bytes
                );
                // Track that this page failed for this insert attempt
                failed_pages.push(page_id);
                // Page doesn't have space, loop will try next page on next iteration
                continue;
            }

            // Insert tuple into slotted page
            let slot_id = self.insert_into_page(&mut page, tuple_data)?;

            // Calculate new free space after insertion
            let new_free = page_free_space(&page)?;

            // Write page back using the same file handle
            write_page(&mut file, &mut page, page_id)?;

            // Update FSM with new free space
            self.fsm.fsm_set_avail(page_id, new_free)?;

            // Increment tuple counter
            self.header.total_tuples += 1;
            
            // Sync updated header to disk
            match update_header_page(&mut self.file_handle, &self.header) {
                Ok(_) => log::trace!("[HeapManager::insert_tuple] Header updated on disk"),
                Err(e) => eprintln!("[WARN] Failed to update header on disk: {}", e),
            }

            log::trace!(
                "[HeapManager::insert_tuple] Successfully inserted at (page={}, slot={}); total_tuples={}",
                page_id, slot_id, self.header.total_tuples
            );

            return Ok((page_id, slot_id));
        }

        // If we get here, we couldn't find space after 3 attempts
        Err(io::Error::new(
            io::ErrorKind::Other,
            "Could not find or allocate page with sufficient space after 3 attempts",
        ))
    }

    /// Retrieve a tuple by page and slot coordinates.
    /// 
    /// # Arguments
    /// * `page_id` - Page number
    /// * `slot_id` - Slot index within page
    /// 
    /// # Returns
    /// Tuple data or error
    pub fn get_tuple(&mut self, page_id: u32, slot_id: u32) -> io::Result<Vec<u8>> {
        log::trace!(
            "[HeapManager::get_tuple] Retrieving tuple (page={}, slot={})",
            page_id, slot_id
        );

        // Validate page_id
        if page_id >= self.header.page_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Page {} out of bounds (page_count={})", page_id, self.header.page_count),
            ));
        }

        // Read page (reopen file for clean state)
        let mut page = Page::new();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.file_path)?;
        read_page(&mut file, &mut page, page_id)?;

        // Validate slot_id
        let tuple_count = get_tuple_count(&page)?;
        if slot_id >= tuple_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Slot {} out of bounds (tuple_count={})", slot_id, tuple_count),
            ));
        }

        // Get slot entry
        let (offset, length) = get_slot_entry(&page, slot_id)?;

        // Validate bounds
        if offset as usize + length as usize > PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Tuple bounds exceed page: offset={}, length={}", offset, length),
            ));
        }

        // Extract and return tuple
        let tuple_data = page.data[offset as usize..(offset + length) as usize].to_vec();

        log::trace!(
            "[HeapManager::get_tuple] Retrieved {} bytes",
            tuple_data.len()
        );

        Ok(tuple_data)
    }

    /// Delete a tuple by marking it as deleted and updating FSM.
    /// 
    /// # Arguments
    /// * `page_id` - Page containing the tuple
    /// * `slot_id` - Slot within the page
    /// 
    /// # Returns
    /// Number of bytes freed or error
    pub fn delete_tuple(&mut self, page_id: u32, slot_id: u32) -> io::Result<u32> {
        use crate::backend::instrumentation::HEAP_METRICS;
        HEAP_METRICS.insert_tuple_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        log::trace!(
            "[HeapManager::delete_tuple] Deleting tuple (page={}, slot={})",
            page_id, slot_id
        );

        // Validate page_id
        if page_id >= self.header.page_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Page {} out of bounds (page_count={})", page_id, self.header.page_count),
            ));
        }

        // Read page
        let mut page = Page::new();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.file_path)?;
        read_page(&mut file, &mut page, page_id)?;

        // Get the tuple data to calculate freed bytes
        let (_offset, length) = get_slot_entry(&page, slot_id)?;
        let freed_bytes = (length + ITEM_ID_SIZE) as u32;

        log::trace!(
            "[HeapManager::delete_tuple] Marked slot {} as deleted, freed {} bytes",
            slot_id, freed_bytes
        );

        // Update the slot directory entry to mark as deleted (set length to 0)
        let slot_offset = PAGE_HEADER_SIZE as usize + (slot_id as usize) * ITEM_ID_SIZE as usize;
        page.data[slot_offset..slot_offset + 4].copy_from_slice(&0u32.to_le_bytes()); // offset = 0
        page.data[slot_offset + 4..slot_offset + 8].copy_from_slice(&0u32.to_le_bytes()); // length = 0

        // Write page back
        write_page(&mut file, &mut page, page_id)?;

        // Calculate new free space and update FSM
        let new_free = page_free_space(&page)?;
        self.fsm.fsm_set_avail(page_id, new_free)?;

        // Decrement tuple counter
        if self.header.total_tuples > 0 {
            self.header.total_tuples -= 1;
        }

        // Sync header to disk
        update_header_page(&mut self.file_handle, &self.header)?;

        Ok(freed_bytes)
    }

    /// Start a sequential scan iterator over all pages.
    pub fn scan(&self) -> HeapScanIterator {
        log::trace!("[HeapManager::scan] Creating scan iterator");
        HeapScanIterator::new(self.file_path.clone(), self.header.page_count)
    }

    /// Search for a page with available space (for testing/debugging).
    pub fn fsm_search_for_page(&mut self, min_category: u8) -> io::Result<Option<u32>> {
        self.fsm.fsm_search_avail(min_category)
    }

    /// Persist all changes to disk.
    /// - Updates header metadata
    /// - Syncs heap file and FSM fork
    pub fn flush(&mut self) -> io::Result<()> {
        log::trace!("[HeapManager::flush] Flushing all changes");

        // Write header
        update_header_page(&mut self.file_handle, &self.header)?;

        // Sync heap file
        self.file_handle.sync_all()?;

        // Sync FSM fork
        self.fsm.sync()?;

        log::trace!("[HeapManager::flush] Flush complete");

        Ok(())
    }

    // ─────────────────────────────────────────────────────────────────────
    // Private Helper Methods
    // ─────────────────────────────────────────────────────────────────────

    /// Allocate a new heap page and register with FSM.
    fn allocate_new_page(&mut self) -> io::Result<u32> {
        log::trace!("[HeapManager::allocate_new_page] Allocating new page");

        let new_page_id = self.header.page_count;

        // Create and initialize empty slotted page
        let mut page = Page::new();
        init_page(&mut page);

        // Append to heap file
        self.file_handle.seek(SeekFrom::End(0))?;
        self.file_handle.write_all(&page.data)?;

        // Update header
        self.header.page_count += 1;
        self.fsm.set_heap_page_count(self.header.page_count);

        // Calculate FSM pages needed
        let new_fsm_page_count = FSM::calculate_fsm_page_count(self.header.page_count);
        self.header.fsm_page_count = new_fsm_page_count;

        // Register new page with FSM (full free space)
        let initial_free = PAGE_SIZE as u32 - PAGE_HEADER_SIZE;
        self.fsm.fsm_set_avail(new_page_id, initial_free)?;

        log::trace!(
            "[HeapManager::allocate_new_page] New page_id={}, total_pages={}",
            new_page_id, self.header.page_count
        );

        Ok(new_page_id)
    }

    /// Insert a tuple into a specific page and return the slot_id.
    fn insert_into_page(&self, page: &mut Page, data: &[u8]) -> io::Result<u32> {
        log::trace!(
            "[HeapManager::insert_into_page] Inserting {} bytes into page",
            data.len()
        );

        // Get current lower and upper pointers
        let mut lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let mut upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

        // Verify space is available
        let required = data.len() as u32 + ITEM_ID_SIZE;
        if upper - lower < required {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Insufficient free space in page: {} < {}",
                    upper - lower, required
                ),
            ));
        }

        // Write tuple data at the upper end (moving backward)
        let data_start = upper - data.len() as u32;
        page.data[data_start as usize..upper as usize].copy_from_slice(data);

        // Update upper pointer
        upper = data_start;
        page.data[4..8].copy_from_slice(&upper.to_le_bytes());

        // Write slot entry at lower end
        let slot_offset_offset = lower as usize;
        page.data[slot_offset_offset..slot_offset_offset + 4] .copy_from_slice(&data_start.to_le_bytes());
        page.data[slot_offset_offset + 4..slot_offset_offset + 8]
            .copy_from_slice(&(data.len() as u32).to_le_bytes());

        // Update lower pointer (moved past the new slot)
        lower += ITEM_ID_SIZE;
        page.data[0..4].copy_from_slice(&lower.to_le_bytes());

        // Calculate slot_id
        let slot_id = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE - 1;

        log::trace!(
            "[HeapManager::insert_into_page] Inserted at slot_id={}",
            slot_id
        );

        Ok(slot_id)
    }

    /// Convert free bytes to free-space category (0-255).
    /// For tuple sizing (min_category): use CEILING to ensure pages with at least that much free space
    /// For page reporting (current free bytes): use FLOOR
    /// If sizing_for_tuple is true, rounds UP so a 22-byte tuple doesn't match a page with 0 bytes free
    fn bytes_to_category(free_bytes: u32) -> u8 {
        // Use ceiling division for better granularity at small sizes
        // This ensures that a request for 22 bytes gets min_category that only matches pages with real free space
        let category = if free_bytes == 0 {
            0
        } else {
            // Ceiling division: (a + b - 1) / b instead of a / b
            ((free_bytes as f64 * 255.0 + PAGE_SIZE as f64 - 1.0) / PAGE_SIZE as f64).floor() as u8
        };
        log::trace!(
            "[HeapManager::bytes_to_category] {} bytes → category {}",
            free_bytes, category
        );
        category
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_file(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        PathBuf::from(format!("{}_{}_{}.dat", prefix, std::process::id(), nanos))
    }

    fn cleanup_temp_heap(path: &PathBuf) {
        let _ = fs::remove_file(path);
        let fsm_path = PathBuf::from(format!("{}.fsm", path.to_string_lossy()));
        let _ = fs::remove_file(fsm_path);
    }

    fn setup_temp_heap(prefix: &str) -> (PathBuf, HeapManager) {
        let temp_file = unique_temp_file(prefix);
        if temp_file.exists() {
            fs::remove_file(&temp_file).ok();
        }

        let manager = HeapManager::create(temp_file.clone()).unwrap();
        (temp_file, manager)
    }

    #[test]
    fn test_open_heap() {
        let (path, _) = setup_temp_heap("test_open_heap");
        // If we get here, open succeeded
        assert!(path.exists());
        cleanup_temp_heap(&path);
    }

    #[test]
    fn test_bytes_to_category() {
        // Full page free space → 255
        let cat_full = HeapManager::bytes_to_category(PAGE_SIZE as u32);
        assert!(cat_full == 255 || cat_full == 254); // May be 254 due to rounding

        // Half free → ~127
        let cat_half = HeapManager::bytes_to_category((PAGE_SIZE / 2) as u32);
        assert!(cat_half >= 120 && cat_half <= 135);

        // No free → 0
        let cat_zero = HeapManager::bytes_to_category(0);
        assert_eq!(cat_zero, 0);
    }

    #[test]
    fn test_heap_scan_empty() {
        let (path, manager) = setup_temp_heap("test_heap_scan_empty");

        let mut count = 0;
        for result in manager.scan() {
            match result {
                Ok(_) => count += 1,
                Err(e) => panic!("Scan error: {}", e),
            }
        }

        assert_eq!(count, 0);
        cleanup_temp_heap(&path);
    }
}
