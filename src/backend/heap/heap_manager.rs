//! HeapManager - High-level API for table operations.
//!
//! This module provides a complete interface for:
//! - Inserting tuples using FSM-guided page selection
//! - Retrieving tuples by (page_id, slot_id)
//! - Sequential scans across all pages
//! - Automatic allocation of new pages with FSM registration
//!
//! Key Design:
//! - All page I/O goes through the CLOCK BufferPool for caching and eviction
//! - Encapsulates FSM complexity; FSM-driven inserts spread load across pages
//! - Header persistence survives crashes; FSM fork is a hint (can be rebuilt)

use std::fs::OpenOptions;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, MutexGuard};

use crate::backend::buffer_manager::buffer_pool::BufferPool;
use crate::backend::buffer_manager::shared_pool::{self, SharedPool};
use crate::backend::fsm::FSM;
use crate::backend::instrumentation::HEAP_METRICS;
use crate::backend::page::{
    ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE, Page, SLOT_FLAG_DELETED, get_slot_entry,
    get_tuple_count, init_page, page_free_space,
};
use crate::heap::types::HeaderMetadata;
use std::sync::atomic::Ordering;

// ─────────────────────────────────────────────────────────────────────────
// Helper: Header page operations via BufferPool
// ─────────────────────────────────────────────────────────────────────────

/// Read the header metadata from page 0 via the buffer pool.
fn read_header_via_pool(pool: &mut BufferPool) -> io::Result<HeaderMetadata> {
    let frame_id = pool.fetch_page(0)?;
    let page = pool.get_page(frame_id);
    let header = HeaderMetadata::deserialize(&page.data[..24])?;
    pool.unpin(frame_id, false);
    Ok(header)
}

/// Write the header metadata to page 0 via the buffer pool.
///
/// The page is only marked dirty; persistence happens through the normal
/// flush paths (checkpoint, pool eviction, `HeapManager::flush`). Forcing an
/// 8 KiB header write on every tuple was the dominant write-amplification
/// cost of bulk loads.
fn write_header_via_pool(pool: &mut BufferPool, header: &HeaderMetadata) -> io::Result<()> {
    let frame_id = pool.fetch_page(0)?;
    {
        let page = pool.get_page_mut(frame_id);
        let bytes = header.serialize()?;
        // Write only the header bytes (keep page count in sync with page data)
        page.data[..24].copy_from_slice(&bytes);
        // Update page count in bytes 0-3
        page.data[0..4].copy_from_slice(&header.page_count.to_le_bytes());
    }
    pool.unpin(frame_id, true);
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// HeapScanIterator - Sequential Scan
// ─────────────────────────────────────────────────────────────────────────

/// Memory-efficient sequential scan iterator.
/// Lazily loads pages as needed, yielding (page_id, slot_id, tuple_data).
///
/// Each page is copied out of the shared buffer pool once and its tuples are
/// served from that copy, so no pool lock or pin is held between calls.
pub struct HeapScanIterator {
    pool: SharedPool,
    current_page: u32,
    current_slot: u32,
    total_pages: u32,
    /// Copy of the page being scanned, reused from page to page.
    page: Page,
    /// Page id held in `page`, if any.
    loaded_page: Option<u32>,
}

impl HeapScanIterator {
    /// Create a new scan iterator starting at page 1 (Page 0 is header).
    fn new(pool: SharedPool, total_pages: u32) -> Self {
        log::trace!(
            "[HeapScanIterator::new] Created iterator for {} pages",
            total_pages
        );
        Self {
            pool,
            current_page: 1, // Skip header page
            current_slot: 0,
            total_pages,
            page: Page::new(),
            loaded_page: None,
        }
    }

    /// Copy a page out of the buffer pool.
    fn load_page(&mut self, page_id: u32) -> io::Result<()> {
        log::trace!("[HeapScanIterator::load_page] Loading page {}", page_id);

        self.loaded_page = None;
        let mut pool = self.pool.lock().unwrap_or_else(|p| p.into_inner());
        let frame_id = pool.fetch_page(page_id)?;
        self.page.data.copy_from_slice(&pool.get_page(frame_id).data);
        pool.unpin(frame_id, false);
        self.loaded_page = Some(page_id);
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

            // Load page if not cached or if we moved to a new page.
            //
            // If a page cannot be read (truncated file, garbage header, ...),
            // report the error ONCE and advance to the next page. Returning
            // without advancing would re-enter this branch forever, hanging
            // any consumer that skips errors (e.g. `filter_map(|r| r.ok())`).
            if self.loaded_page != Some(self.current_page)
                && let Err(e) = self.load_page(self.current_page)
            {
                self.current_page += 1;
                self.current_slot = 0;
                return Some(Err(e));
            }

            let page_id = self.current_page;
            let page = &self.page;

            // Get tuple count for current page
            let tuple_count = match get_tuple_count(page) {
                Ok(count) => count,
                Err(e) => {
                    // Undecodable page header: report once, move on.
                    self.current_page += 1;
                    self.current_slot = 0;
                    return Some(Err(e));
                }
            };

            // Check if we've exhausted tuples in this page
            if self.current_slot >= tuple_count {
                log::trace!(
                    "[HeapScanIterator::next] Page {} exhausted, moving to next",
                    self.current_page
                );
                self.current_page += 1;
                self.current_slot = 0;
                continue;
            }

            // Extract the tuple — get_slot_entry returns (0,0) for soft-deleted slots.
            let slot_id = self.current_slot;
            let (offset, length) = match get_slot_entry(page, slot_id) {
                Ok((o, l)) => (o, l),
                Err(e) => {
                    // Always advance past the bad slot to avoid an infinite loop
                    // (e.g. when filter_map silently swallows the error).
                    self.current_slot += 1;
                    return Some(Err(e));
                }
            };

            // (0, 0) is the sentinel for "deleted / empty slot" returned by get_slot_entry.
            // Skip it and move on.
            if offset == 0 && length == 0 {
                log::trace!(
                    "[HeapScanIterator::next] Skipping deleted/empty slot id={} on page={}",
                    slot_id,
                    page_id
                );
                self.current_slot += 1;
                continue;
            }

            // Validate bounds (guard against other corruption)
            if offset as usize + length as usize > PAGE_SIZE {
                // Advance to prevent infinite loop, then surface the error.
                self.current_slot += 1;
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
                page_id,
                slot_id,
                data.len()
            );

            self.current_slot += 1;

            return Some(Ok((page_id, slot_id, data)));
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// HeapManager - Main API
// ─────────────────────────────────────────────────────────────────────────

/// High-level heap file manager with FSM-guided insertion.
///
/// All page I/O is routed through a CLOCK BufferPool for efficient caching.
///
/// The pool is **shared process-wide per file** (see `buffer_manager::shared_pool`):
/// two `HeapManager`s opened on the same `.dat` file — e.g. a scan operator and
/// an insert operator in one query — see each other's dirty pages immediately
/// instead of holding independent caches over the same inode. The lock is held
/// only for individual page operations; pins keep frames stable between them.
///
/// The file header lives only in the pool's page 0 frame. A manager keeps no
/// copy of it, so every handle on the file reads the same counts.
pub struct HeapManager {
    /// Shared buffer pool (one per underlying file, process-wide).
    pub pool: SharedPool,
    fsm: FSM,
}

impl HeapManager {
    /// Lock the shared buffer pool for a page operation.
    ///
    /// Takes the pool by field reference so the returned guard borrows only
    /// `self.pool`, leaving `self.fsm` / `self.header` free for mutation
    /// while pages are accessed (disjoint field borrows).
    fn lock_pool(pool: &SharedPool) -> MutexGuard<'_, BufferPool> {
        pool.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Vacuum update to expose vacuuming logic internally
    pub fn vacuum_page(&mut self, page_id: u32, absolute_free_bytes: u32) -> io::Result<()> {
        self.fsm.fsm_vacuum_update(page_id, absolute_free_bytes)
    }

    /// Create a new heap file and initialize with empty pages.
    ///
    /// # Arguments
    /// * `file_path` - Path where the new heap file will be created
    ///
    /// # Returns
    /// HeapManager instance ready for inserts or error
    pub fn create(file_path: PathBuf) -> io::Result<Self> {
        log::trace!(
            "[HeapManager::create] Creating new heap file at {:?}",
            file_path
        );

        // Remove existing file if present — and its FSM sidecar: a stale
        // .fsm from a previous file at this path advertises free-space
        // categories for pages that no longer exist, which the search
        // would otherwise hand out as phantom targets (observed under
        // stress: "Invalid page_id 491 >= 2" on freshly created tables).
        if file_path.exists() {
            std::fs::remove_file(&file_path)?;
        }
        let fsm_sidecar = PathBuf::from(format!("{}.fsm", file_path.to_string_lossy()));
        if fsm_sidecar.exists() {
            std::fs::remove_file(&fsm_sidecar)?;
        }

        // A cached manager on this path would hold the OLD shared pool and
        // FSM handle; evict it so subsequent opens bind to the new file.
        crate::backend::cache::evict_heap(&file_path)?;

        // Create new file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&file_path)?;

        // Create initial header
        let header = HeaderMetadata::new();
        let header_bytes = header.serialize()?;

        // Write header page (24 bytes metadata + padding)
        let mut header_page = vec![0u8; PAGE_SIZE];
        header_page[0..24].copy_from_slice(&header_bytes);
        // Set initial page count: 2 (header + first data page)
        let init_page_count: u32 = 2;
        header_page[0..4].copy_from_slice(&init_page_count.to_le_bytes());
        file.write_all(&header_page)?;

        // Create first data page (Page 1)
        let mut data_page = Page::new();
        init_page(&mut data_page);
        file.write_all(&data_page.data)?;

        file.flush()?;
        file.sync_all()?;

        // Reset file cursor to start for the buffer pool
        file.seek(SeekFrom::Start(0))?;

        log::trace!("[HeapManager::create] Heap file created, initializing BufferPool");

        // Register a brand-new shared pool for this (re-created) file,
        // replacing any registry entry left over from a previous file.
        let pool =
            BufferPool::with_file(shared_pool::shared_pool_capacity(), file, file_path.clone())?;
        let pool = shared_pool::register_pool(&file_path, pool);

        // Derive FSM path
        let fsm_path = PathBuf::from(format!("{}.fsm", file_path.to_string_lossy()));

        // Create FSM fork
        let fsm = FSM::open(fsm_path.clone(), 2)?; // 2 pages initially

        let mut manager = Self {
            pool,
            fsm,
        };

        // Set heap page count to 2 (Page 0 + Page 1)
        manager.update_header(|h| {
            *h = header;
            h.page_count = 2;
        })?;
        manager.fsm.set_heap_page_count(2);

        // Register first data page with FSM
        let initial_free = PAGE_SIZE as u32 - PAGE_HEADER_SIZE;
        manager.fsm.fsm_set_avail(1, initial_free, None)?;

        // Persist changes via buffer pool
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
    pub fn open(file_path: PathBuf) -> io::Result<Self> {
        log::trace!("[HeapManager::open] Opening heap file {:?}", file_path);

        // Verify path exists
        if !file_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Heap file not found: {:?}", file_path),
            ));
        }

        // Get the process-wide shared pool for this file (opens it if this
        // is the first manager on the file).
        let pool = shared_pool::get_or_create(&file_path)?;

        // Read header via buffer pool
        let header = {
            let mut guard = pool.lock().unwrap_or_else(|p| p.into_inner());
            read_header_via_pool(&mut guard)?
        };
        log::trace!(
            "[HeapManager::open] Read header: page_count={}, fsm_page_count={}, total_tuples={}",
            header.page_count,
            header.fsm_page_count,
            header.total_tuples
        );

        let mut fsm_path = file_path.to_string_lossy().into_owned();
        if !fsm_path.ends_with(".fsm") {
            fsm_path.push_str(".fsm");
        }
        let fsm_path = PathBuf::from(fsm_path);

        // Open/rebuild FSM
        let fsm_is_missing = std::fs::metadata(&fsm_path).map_or(true, |meta| meta.len() == 0);
        let fsm = if fsm_is_missing {
            log::trace!("[HeapManager::open] FSM missing or empty, rebuilding...");
            // For FSM rebuild, we need the file handle. Use the pool's file.
            // Temporarily take ownership of the file for FSM rebuild.
            let mut guard = pool.lock().unwrap_or_else(|p| p.into_inner());
            let mut file = guard.file.take().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "BufferPool has no file")
            })?;
            file.seek(SeekFrom::Start(0))?;
            let fsm = FSM::build_from_heap(&mut file, fsm_path.clone())?;
            // Return file to the pool
            guard.file = Some(file);
            fsm
        } else {
            FSM::open(fsm_path.clone(), header.page_count)?
        };

        // ===== SYNC FSM PAGE COUNT TO HEADER =====
        let calculated_fsm_pages = FSM::calculate_fsm_page_count(header.page_count);

        let mut header = header;
        if header.fsm_page_count != calculated_fsm_pages {
            log::trace!(
                "[HeapManager::open] FSM page count mismatch: header={}, calculated={}. Updating...",
                header.fsm_page_count,
                calculated_fsm_pages
            );
            header.fsm_page_count = calculated_fsm_pages;
            {
                let mut guard = pool.lock().unwrap_or_else(|p| p.into_inner());
                write_header_via_pool(&mut guard, &header)?;
            }
            log::trace!(
                "[HeapManager::open] Updated and persisted fsm_page_count to {}",
                calculated_fsm_pages
            );
        }

        log::trace!("[HeapManager::open] Successfully opened HeapManager");

        Ok(Self {
            pool,
            fsm,
        })
    }

    /// Insert a tuple using FSM-guided page selection.
    ///
    /// All page I/O is routed through the CLOCK BufferPool.
    pub fn insert_tuple(&mut self, tuple_data: &[u8]) -> io::Result<(u32, u32)> {
        use crate::backend::instrumentation::HEAP_METRICS;
        HEAP_METRICS
            .insert_tuple_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

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

        // Three FSM-guided attempts, then a fresh page: stale FSM entries must
        // never make an insert fail while the file can still grow.
        for attempt in 0..4 {
            log::trace!("[HeapManager::insert_tuple] Attempt {}/4", attempt + 1);

            // Get a page to try
            let page_id = if attempt == 3 {
                self.allocate_new_page()?
            } else {
                match self.fsm.search_page(min_category)? {
                    Some(pid) => {
                        // Harden against phantom FSM hits: a stale/garbage FSM
                        // leaf can advertise a page beyond the heap's real page
                        // count (observed on freshly created tables under stress).
                        // Treat such hits as a miss so the normal retry logic
                        // allocates a genuine new page instead of erroring out.
                        if pid >= self.page_count() {
                            log::warn!(
                                "[HeapManager::insert_tuple] FSM returned phantom page {} (page_count={}); treating as miss",
                                pid,
                                self.page_count()
                            );
                            failed_pages.push(pid);
                            if attempt < 2 {
                                continue;
                            } else {
                                self.allocate_new_page()?
                            }
                        }
                        // Check if this page failed before
                        else if failed_pages.contains(&pid) {
                            log::trace!(
                                "[HeapManager::insert_tuple] Page {} previously failed for this insert, allocating new",
                                pid
                            );
                            if attempt < 2 {
                                log::trace!(
                                    "[HeapManager::insert_tuple] Retrying with fresh search"
                                );
                                continue;
                            } else {
                                log::trace!(
                                    "[HeapManager::insert_tuple] Final attempt: allocating new page"
                                );
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
                            log::trace!(
                                "[HeapManager::insert_tuple] FSM returned None, will retry"
                            );
                            continue;
                        } else {
                            log::trace!(
                                "[HeapManager::insert_tuple] Final attempt: allocating new page"
                            );
                            self.allocate_new_page()?
                        }
                    }
                }
            };

            // Verify page_id is valid
            if page_id >= self.page_count() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid page_id: {} >= {}", page_id, self.page_count()),
                ));
            }

            // Read the target page via buffer pool (lock held for the whole
            // page access; released before FSM/header updates below).
            let slot_id = {
                let mut pool = Self::lock_pool(&self.pool);
                let frame_id = match pool.fetch_page(page_id) {
                    Ok(fid) => fid,
                    Err(e) => {
                        log::error!("[ERROR] Failed to read page {}: {}", page_id, e);
                        return Err(e);
                    }
                };

                // Get current free space and verify
                let current_free = {
                    let page = pool.get_page(frame_id);
                    page_free_space(page)?
                };

                if current_free < required_bytes {
                    log::trace!(
                        "[HeapManager::insert_tuple] Page {} has only {} bytes free, needs {} - will retry",
                        page_id,
                        current_free,
                        required_bytes
                    );
                    // Unpin without dirty (we didn't modify)
                    pool.unpin(frame_id, false);
                    failed_pages.push(page_id);
                    self.fsm.fsm_set_avail(page_id, current_free, None)?;
                    continue;
                }

                // Insert tuple into slotted page (via buffer pool)
                let slot_id = {
                    let page = pool.get_page_mut(frame_id);
                    Self::insert_into_page(page, tuple_data)?
                };

                // Unpin page (dirty — modifications need to be written back)
                pool.unpin(frame_id, true);
                slot_id
            };

            let new_free = {
                let mut pool = Self::lock_pool(&self.pool);
                // Calculate new free space after insertion
                // (re-fetch: frame stays cached, pin is transient)
                let frame_id = pool.fetch_page(page_id)?;
                let page = pool.get_page(frame_id);
                let free = page_free_space(page)?;
                pool.unpin(frame_id, false);
                free
            };

            // Update FSM with new free space
            self.fsm.fsm_set_avail(page_id, new_free, None)?;

            // Increment tuple counter
            self.update_header(|h| h.total_tuples += 1)?;

            log::trace!(
                "[HeapManager::insert_tuple] Successfully inserted at (page={}, slot={})",
                page_id,
                slot_id
            );

            return Ok((page_id, slot_id));
        }

        Err(io::Error::other(
            "Could not find or allocate page with sufficient space",
        ))
    }

    /// Retrieve a tuple by page and slot coordinates.
    ///
    /// Uses the buffer pool for page access.
    pub fn get_tuple(&mut self, page_id: u32, slot_id: u32) -> io::Result<Vec<u8>> {
        HEAP_METRICS.get_tuple_calls.fetch_add(1, Ordering::Relaxed);
        log::trace!(
            "[HeapManager::get_tuple] Retrieving tuple (page={}, slot={})",
            page_id,
            slot_id
        );

        // Validate page_id
        if page_id >= self.page_count() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Page {} out of bounds (page_count={})",
                    page_id,
                    self.page_count()
                ),
            ));
        }

        // Fetch page via buffer pool
        let result = {
            let mut pool = Self::lock_pool(&self.pool);
            let frame_id = pool.fetch_page(page_id)?;

            // Validate slot_id and extract tuple
            let extract_res = (|| -> io::Result<Vec<u8>> {
                let page = pool.get_page(frame_id);
                let tuple_count = get_tuple_count(page)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

                if slot_id >= tuple_count {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Slot {} out of bounds (tuple_count={})",
                            slot_id, tuple_count
                        ),
                    ));
                }

                let (offset, length) = get_slot_entry(page, slot_id)?;

                if offset == 0 && length == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "Tuple at page {}, slot {} is deleted or slot is unused",
                            page_id, slot_id
                        ),
                    ));
                }

                if offset as usize + length as usize > PAGE_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Tuple bounds exceed page: offset={}, length={}",
                            offset, length
                        ),
                    ));
                }

                Ok(page.data[offset as usize..(offset + length) as usize].to_vec())
            })();

            // Always unpin (not dirty — we only read) before handling error/returning
            pool.unpin(frame_id, false);
            extract_res?
        };

        log::trace!("[HeapManager::get_tuple] Retrieved {} bytes", result.len());

        Ok(result)
    }

    /// Delete a tuple by marking it as deleted and updating FSM.
    ///
    /// Uses the buffer pool for page access.
    pub fn delete_tuple(&mut self, page_id: u32, slot_id: u32) -> io::Result<u32> {
        use crate::backend::instrumentation::HEAP_METRICS;
        HEAP_METRICS
            .insert_tuple_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        log::trace!(
            "[HeapManager::delete_tuple] Deleting tuple (page={}, slot={})",
            page_id,
            slot_id
        );

        // Validate page_id
        if page_id >= self.page_count() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Page {} out of bounds (page_count={})",
                    page_id,
                    self.page_count()
                ),
            ));
        }

        // Fetch page via buffer pool and mark the slot deleted
        let freed_bytes: u32;
        {
            let mut pool = Self::lock_pool(&self.pool);
            let frame_id = pool.fetch_page(page_id)?;

            let delete_res = (|| -> io::Result<u32> {
                let page = pool.get_page_mut(frame_id);

                // Get the tuple data to calculate freed bytes
                let (offset, length) = get_slot_entry(page, slot_id)?;
                if offset == 0 && length == 0 {
                    // Already deleted: nothing to free and nothing to count.
                    return Ok(0);
                }
                let freed = length + ITEM_ID_SIZE;

                log::trace!(
                    "[HeapManager::delete_tuple] Marked slot {} as deleted, freed {} bytes",
                    slot_id,
                    freed
                );

                // Update the slot directory entry to mark as deleted
                let slot_offset = PAGE_HEADER_SIZE + slot_id * ITEM_ID_SIZE;
                page.data[slot_offset as usize..slot_offset as usize + 4]
                    .copy_from_slice(&0u32.to_le_bytes()); // offset = 0
                page.data[slot_offset as usize + 4..slot_offset as usize + 8]
                    .copy_from_slice(&0u32.to_le_bytes()); // length = 0

                // Optimization: Roll back pointers if this is the last tuple
                let mut lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
                let mut upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

                let tuple_count = get_tuple_count(page)?;

                if offset == upper {
                    upper += length;
                    page.data[4..8].copy_from_slice(&upper.to_le_bytes());
                    log::trace!(
                        "[HeapManager::delete_tuple] Reclaimed data space, upper moved to {}",
                        upper
                    );
                }

                if slot_id == tuple_count - 1 && lower == slot_offset + ITEM_ID_SIZE {
                    lower -= ITEM_ID_SIZE;
                    page.data[0..4].copy_from_slice(&lower.to_le_bytes());
                    log::trace!(
                        "[HeapManager::delete_tuple] Reclaimed slot space, lower moved to {}",
                        lower
                    );
                }

                Ok(freed)
            })();

            match delete_res {
                Ok(freed) => {
                    // Unpin dirty (we modified the page)
                    pool.unpin(frame_id, true);

                    // We DO NOT update the FSM — dead slots are hidden until compaction

                    // Decrement tuple counter
                    if freed > 0 {
                        let mut header = read_header_via_pool(&mut pool)?;
                        header.total_tuples = header.total_tuples.saturating_sub(1);
                        write_header_via_pool(&mut pool, &header)?;
                    }

                    freed_bytes = freed;
                }
                Err(e) => {
                    // Unpin clean on error so the pin is not leaked
                    pool.unpin(frame_id, false);
                    return Err(e);
                }
            }
        }

        Ok(freed_bytes)
    }

    /// Start a sequential scan iterator over all pages.
    ///
    /// Pages are read through the shared buffer pool, so the scan sees every
    /// write made through any handle on this file without a flush. It covers
    /// the pages that exist when it starts.
    pub fn scan(&self) -> HeapScanIterator {
        log::trace!("[HeapManager::scan] Creating scan iterator");
        HeapScanIterator::new(Arc::clone(&self.pool), self.page_count())
    }

    /// Number of pages in the file, header page included.
    pub fn page_count(&self) -> u32 {
        Self::lock_pool(&self.pool).total_pages()
    }

    /// Read the file header from the pool's page 0.
    pub fn header(&self) -> io::Result<HeaderMetadata> {
        read_header_via_pool(&mut Self::lock_pool(&self.pool))
    }

    /// Change the file header in the pool's page 0.
    pub fn update_header(&self, f: impl FnOnce(&mut HeaderMetadata)) -> io::Result<()> {
        let mut pool = Self::lock_pool(&self.pool);
        let mut header = read_header_via_pool(&mut pool)?;
        f(&mut header);
        write_header_via_pool(&mut pool, &header)
    }

    /// Search for a page with available space (for testing/debugging).
    pub fn fsm_search_for_page(&mut self, min_category: u8) -> io::Result<Option<u32>> {
        self.fsm.search_page(min_category)
    }

    /// Persist all changes to disk.
    /// - Flushes all dirty pages in the buffer pool
    /// - Syncs FSM fork
    pub fn flush(&mut self) -> io::Result<()> {
        self.flush_inner(true)
    }

    /// Write all changes to the files without fsync.
    ///
    /// Enough for direct-I/O readers to see them and for a process crash;
    /// `flush` adds the fsync that covers a power loss.
    pub fn write_through(&mut self) -> io::Result<()> {
        self.flush_inner(false)
    }

    fn flush_inner(&mut self, durable: bool) -> io::Result<()> {
        log::trace!("[HeapManager::flush] Flushing all changes");

        let mut pool = Self::lock_pool(&self.pool);

        // Each call is a no-op when its target is clean, so repeated
        // checkpoints stay free.
        if durable {
            pool.flush_all()?;
            drop(pool);
            if self.fsm.has_pending() {
                self.fsm.sync()?;
            }
        } else {
            pool.write_dirty()?;
            drop(pool);
            self.fsm.write_through()?;
        }

        Ok(())
    }

    /// Check if the underlying heap file on disk was replaced (e.g. by rename/swap).
    pub fn is_file_stale(&self, path: &std::path::Path) -> bool {
        let pool = Self::lock_pool(&self.pool);
        pool.is_file_stale(path)
    }

    // ─────────────────────────────────────────────────────────────────────
    // Private Helper Methods
    // ─────────────────────────────────────────────────────────────────────

    /// Allocate a new heap page and register with FSM.
    /// Uses the buffer pool for page creation.
    fn allocate_new_page(&mut self) -> io::Result<u32> {
        HEAP_METRICS
            .allocate_page_calls
            .fetch_add(1, Ordering::Relaxed);
        let (new_page_id, page_count) = {
            let mut pool = Self::lock_pool(&self.pool);
            // Create new page via buffer pool (this writes it to disk and brings it into cache)
            let (page_id, frame_id) = pool.new_page()?;
            // Unpin immediately since we just need it registered, not kept in cache
            pool.unpin(frame_id, false);
            (page_id, pool.total_pages())
        };
        self.fsm.set_heap_page_count(page_count);

        // Calculate FSM pages needed
        let new_fsm_page_count = FSM::calculate_fsm_page_count(page_count);
        self.update_header(|h| {
            h.page_count = page_count;
            h.fsm_page_count = new_fsm_page_count;
        })?;

        // Register new page with FSM (full free space)
        let initial_free = PAGE_SIZE as u32 - PAGE_HEADER_SIZE;
        self.fsm.fsm_set_avail(new_page_id, initial_free, None)?;

        log::trace!(
            "[HeapManager::allocate_new_page] New page_id={}, total_pages={}",
            new_page_id,
            page_count
        );

        Ok(new_page_id)
    }

    /// Insert a tuple into a specific page and return the slot_id.
    fn insert_into_page(page: &mut Page, data: &[u8]) -> io::Result<u32> {
        log::trace!(
            "[HeapManager::insert_into_page] Inserting {} bytes into page",
            data.len()
        );

        // Get current lower and upper pointers
        let mut lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let mut upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

        // Step 1: Look for a dead slot in the slot directory to reuse
        let tuple_count = match get_tuple_count(page) {
            Ok(c) => c,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
        };

        let mut reused_slot_id = None;
        for i in 0..tuple_count {
            let base = PAGE_HEADER_SIZE as usize + i as usize * ITEM_ID_SIZE as usize;
            let slot_offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let slot_flags = u16::from_le_bytes(page.data[base + 6..base + 8].try_into().unwrap());

            // A slot is reusable if it is soft-deleted (SLOT_FLAG_DELETED set)
            // or is the legacy dead-slot marker (offset == 0).
            if slot_flags & SLOT_FLAG_DELETED != 0 || slot_offset == 0 {
                reused_slot_id = Some(i);
                break;
            }
        }

        // Verify space is available
        let required = if reused_slot_id.is_some() {
            data.len() as u32 // Reusing slot, only need data space
        } else {
            data.len() as u32 + ITEM_ID_SIZE // Expanding lower, need data + slot space
        };

        if upper - lower < required {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Insufficient free space in page: {} < {}",
                    upper - lower,
                    required
                ),
            ));
        }

        // Write tuple data at the upper end (moving backward)
        let data_start = upper - data.len() as u32;
        page.data[data_start as usize..upper as usize].copy_from_slice(data);

        // Update upper pointer
        upper = data_start;
        page.data[4..8].copy_from_slice(&upper.to_le_bytes());

        // Write slot entry
        let (slot_id, slot_offset_offset) = match reused_slot_id {
            Some(id) => {
                log::trace!(
                    "[HeapManager::insert_into_page] Reusing dead slot_id={}",
                    id
                );
                (
                    id,
                    PAGE_HEADER_SIZE as usize + id as usize * ITEM_ID_SIZE as usize,
                )
            }
            None => {
                let current_lower = lower as usize;

                // Update lower pointer (moved past the new slot)
                lower += ITEM_ID_SIZE;
                page.data[0..4].copy_from_slice(&lower.to_le_bytes());

                let id = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE - 1;
                (id, current_lower)
            }
        };

        // Canonical slot format: [offset: u32 (4B)][length: u16 (2B)][flags: u16 (2B)]
        page.data[slot_offset_offset..slot_offset_offset + 4]
            .copy_from_slice(&data_start.to_le_bytes());
        page.data[slot_offset_offset + 4..slot_offset_offset + 6]
            .copy_from_slice(&(data.len() as u16).to_le_bytes());
        page.data[slot_offset_offset + 6..slot_offset_offset + 8]
            .copy_from_slice(&0u16.to_le_bytes()); // flags = 0 (live slot)

        log::trace!(
            "[HeapManager::insert_into_page] Inserted at slot_id={}",
            slot_id
        );

        Ok(slot_id)
    }

    /// Convert free bytes to free-space category (0-255).
    fn bytes_to_category(required_bytes: u32) -> u8 {
        if required_bytes == 0 {
            return 0;
        }
        required_bytes.div_ceil(32).min(255) as u8
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
        assert!(path.exists());
        cleanup_temp_heap(&path);
    }

    #[test]
    fn test_bytes_to_category() {
        let cat_full = HeapManager::bytes_to_category(PAGE_SIZE as u32);
        assert!(cat_full == 255 || cat_full == 254);

        let cat_half = HeapManager::bytes_to_category((PAGE_SIZE / 2) as u32);
        assert!((120..=135).contains(&cat_half));

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

    #[test]
    fn test_heap_scan_sees_unflushed_rows_across_pages() {
        let (path, mut manager) = setup_temp_heap("test_heap_scan_reused");

        // Insert enough tuples to span multiple pages (each page has ~8 KB)
        // 200-byte tuples -> ~35 tuples per page. 100 tuples will span ~3 pages.
        let tuple_data = vec![42u8; 200];
        let mut inserted_ids = Vec::new();
        for _ in 0..100 {
            let (pid, sid) = manager.insert_tuple(&tuple_data).unwrap();
            inserted_ids.push((pid, sid));
        }

        // No flush: the scan reads the pool, so unflushed rows are visible.
        let mut scanned = Vec::new();
        for res in manager.scan() {
            let (pid, sid, data) = res.unwrap();
            assert_eq!(data, tuple_data);
            scanned.push((pid, sid));
        }

        assert_eq!(scanned.len(), 100);
        assert_eq!(scanned, inserted_ids);

        drop(manager);
        cleanup_temp_heap(&path);
    }
}
