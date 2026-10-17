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

use crate::backend::error::{RookError, RookResult};
use std::fs::OpenOptions;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, MutexGuard};

use crate::backend::buffer_manager::buffer_pool::BufferPool;
use crate::backend::buffer_manager::shared_pool::{self, SharedPool};
use crate::backend::fsm::FSM;
use crate::backend::instrumentation::HEAP_METRICS;
use crate::backend::page::{
    ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE, Page, Slot, get_slot_entry, get_tuple_count,
    init_page, page_free_space,
};
use crate::heap::types::HeaderMetadata;
use std::sync::atomic::Ordering;

/// The free-space map fork of the heap file at `heap_path`.
fn fsm_path(heap_path: &std::path::Path) -> PathBuf {
    crate::backend::paths::with_suffix(heap_path, ".fsm")
}

// ─────────────────────────────────────────────────────────────────────────
// Helper: Header page operations via BufferPool
// ─────────────────────────────────────────────────────────────────────────

/// Read the header metadata from page 0 via the buffer pool.
fn read_header_via_pool(pool: &mut BufferPool) -> RookResult<HeaderMetadata> {
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
fn write_header_via_pool(pool: &mut BufferPool, header: &HeaderMetadata) -> RookResult<()> {
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
/// [`HeapScanIterator::next_ref`] lends each row's bytes from that copy;
/// the `Iterator` impl returns owned copies.
pub struct HeapScanIterator {
    pool: SharedPool,
    current_page: u32,
    current_slot: u32,
    total_pages: u32,
    /// Copy of the page being scanned, reused from page to page.
    page: Page,
    /// Page id held in `page`, if any.
    loaded_page: Option<u32>,
    /// Slot count of the loaded page, validated when it was loaded.
    slots_on_page: u32,
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
            slots_on_page: 0,
        }
    }

    /// Copy a page out of the buffer pool and validate its header.
    fn load_page(&mut self, page_id: u32) -> RookResult<()> {
        self.loaded_page = None;
        {
            let mut pool = self.pool.lock().unwrap_or_else(|p| p.into_inner());
            let frame_id = pool.fetch_page(page_id)?;
            self.page
                .data
                .copy_from_slice(&pool.get_page(frame_id).data);
            pool.unpin(frame_id, false);
        }
        self.slots_on_page = get_tuple_count(&self.page)?;
        self.loaded_page = Some(page_id);
        Ok(())
    }

    /// Move to the next live row: its page, slot and byte range in `page`.
    ///
    /// A page that cannot be read or a slot pointing outside its page is
    /// reported once, and the scan moves past it.
    fn advance(&mut self) -> Option<RookResult<(u32, u32, usize, usize)>> {
        loop {
            if self.current_page >= self.total_pages {
                return None;
            }
            if self.loaded_page != Some(self.current_page)
                && let Err(e) = self.load_page(self.current_page)
            {
                self.current_page += 1;
                self.current_slot = 0;
                return Some(Err(e));
            }
            if self.current_slot >= self.slots_on_page {
                self.current_page += 1;
                self.current_slot = 0;
                continue;
            }

            let page_id = self.current_page;
            let slot_id = self.current_slot;
            self.current_slot += 1;
            let Some(slot) = self.page.slot(slot_id) else {
                continue;
            };
            if slot.is_dead() {
                continue;
            }
            let start = slot.offset as usize;
            let end = start + slot.length as usize;
            if end > PAGE_SIZE {
                return Some(Err(RookError::io(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Tuple bounds invalid: offset={}, length={}, page_size={}",
                        slot.offset, slot.length, PAGE_SIZE
                    ),
                )));
            }
            return Some(Ok((page_id, slot_id, start, end)));
        }
    }

    /// The next live row as `(page_id, slot_id, bytes)`, with the bytes
    /// borrowed from the scan's page copy (valid until the next call).
    pub fn next_ref(&mut self) -> Option<RookResult<(u32, u32, &[u8])>> {
        match self.advance()? {
            Ok((page_id, slot_id, start, end)) => {
                Some(Ok((page_id, slot_id, &self.page.data[start..end])))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl Iterator for HeapScanIterator {
    type Item = RookResult<(u32, u32, Vec<u8>)>; // (page_id, slot_id, data)

    fn next(&mut self) -> Option<Self::Item> {
        self.next_ref()
            .map(|r| r.map(|(page, slot, bytes)| (page, slot, bytes.to_vec())))
    }
}

// ─────────────────────────────────────────────────────────────────────────
// HeapManager - Main API
// ─────────────────────────────────────────────────────────────────────────

/// High-level heap file manager with FSM-guided insertion.
///
/// All page I/O is routed through a [`BufferPool`] view onto the global CLOCK
/// frame store, which holds the single memory budget for every open file.
///
/// The view is **shared per file** (see `buffer_manager::shared_pool`):
/// two `HeapManager`s opened on the same `.dat` file — e.g. a scan operator and
/// an insert operator in one query — see each other's dirty pages immediately
/// instead of holding independent caches over the same inode. The lock is held
/// only for individual page operations; pins keep frames stable between them.
///
/// The file header lives only in the pool's page 0 frame. A manager keeps no
/// copy of it, so every handle on the file reads the same counts.
pub struct HeapManager {
    /// Buffer pool view of this file (one per underlying file, shared by every manager on it).
    pub pool: SharedPool,
    fsm: FSM,
    /// Where the last insert landed: (page, slots before this one are all
    /// live, dead-slot epoch at that time). In memory only; see
    /// [`Page::insert_from`].
    insert_hint: Option<(u32, u32, u64)>,
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
    pub fn vacuum_page(&mut self, page_id: u32, absolute_free_bytes: u32) -> RookResult<()> {
        self.fsm.fsm_vacuum_update(page_id, absolute_free_bytes)
    }

    /// Create a new heap file and initialize with empty pages.
    ///
    /// # Arguments
    /// * `file_path` - Path where the new heap file will be created
    ///
    /// # Returns
    /// HeapManager instance ready for inserts or error
    pub fn create(file_path: PathBuf) -> RookResult<Self> {
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
        let fsm_sidecar = fsm_path(&file_path);
        if fsm_sidecar.exists() {
            std::fs::remove_file(&fsm_sidecar)?;
        }

        // A cached manager on this path would hold the OLD shared pool and
        // FSM handle; evict it so subsequent opens bind to the new file.
        crate::backend::cache::evict_heap_everywhere(&file_path)?;

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
        let pool = BufferPool::with_file(file, file_path.clone())?;
        let pool = shared_pool::register_pool(&file_path, pool);

        // Derive FSM path
        let fsm_path = fsm_path(&file_path);

        // Create FSM fork
        let fsm = FSM::open(fsm_path.clone())?;

        let mut manager = Self {
            pool,
            fsm,
            insert_hint: None,
        };

        // Set heap page count to 2 (Page 0 + Page 1)
        manager.update_header(|h| {
            *h = header;
            h.page_count = 2;
            h.fsm_page_count = FSM::calculate_fsm_page_count(2);
        })?;

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
    pub fn open(file_path: PathBuf) -> RookResult<Self> {
        log::trace!("[HeapManager::open] Opening heap file {:?}", file_path);

        // Verify path exists
        if !file_path.exists() {
            return Err(RookError::io(
                io::ErrorKind::NotFound,
                format!("Heap file not found: {:?}", file_path),
            ));
        }

        // Get the shared pool view for this file (opens it if this
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

        let fsm_path = fsm_path(&file_path);

        // Open the FSM, or rebuild it from the heap when it is missing or its
        // recorded size does not fit the heap: written in an older layout,
        // or left behind by a crash before the header was updated.
        let calculated_fsm_pages = FSM::calculate_fsm_page_count(header.page_count);
        let fsm_is_missing = std::fs::metadata(&fsm_path).map_or(true, |meta| meta.len() == 0);
        let fsm = if fsm_is_missing || header.fsm_page_count != calculated_fsm_pages {
            log::trace!("[HeapManager::open] FSM missing or out of date, rebuilding...");
            // For FSM rebuild, we need the file handle. Use the pool's file.
            // Temporarily take ownership of the file for FSM rebuild.
            let mut guard = pool.lock().unwrap_or_else(|p| p.into_inner());
            // The rebuild reads the file, so pages still in the pool go out first.
            guard.write_dirty()?;
            let mut file = guard.try_clone_file()?;
            file.seek(SeekFrom::Start(0))?;
            FSM::build_from_heap(&mut file, fsm_path.clone())?
        } else {
            FSM::open(fsm_path.clone())?
        };

        // ===== SYNC FSM PAGE COUNT TO HEADER =====

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
            insert_hint: None,
        })
    }

    /// Insert a tuple using FSM-guided page selection.
    ///
    /// All page I/O is routed through the CLOCK BufferPool.
    pub fn insert_tuple(&mut self, tuple_data: &[u8]) -> RookResult<(u32, u32)> {
        use crate::backend::instrumentation::HEAP_METRICS;
        HEAP_METRICS
            .insert_tuple_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Validate input
        if tuple_data.is_empty() {
            return Err(RookError::TypeMismatch(
                "Tuple data cannot be empty".to_string(),
            ));
        }

        if tuple_data.len() > PAGE_SIZE - PAGE_HEADER_SIZE as usize - ITEM_ID_SIZE as usize {
            return Err(RookError::TypeMismatch(format!(
                "Tuple too large: {} > {}",
                tuple_data.len(),
                PAGE_SIZE - PAGE_HEADER_SIZE as usize
            )));
        }

        // Calculate required space (tuple data + slot entry)
        let required_bytes = (tuple_data.len() as u32) + ITEM_ID_SIZE;
        let min_category = Self::bytes_to_category(required_bytes);

        // One FSM search. A hit can be stale: a page beyond the heap (an FSM
        // fork left from an earlier file at this path, which
        // `HeapManager::create` now removes) or a page that has less room
        // than advertised. Either entry is corrected so later searches skip
        // it, and the insert goes to a fresh page rather than searching
        // again, so a stale FSM never makes it fail while the file can grow.
        let candidate = match self.fsm.search_page(min_category)? {
            Some(pid) if pid >= self.page_count() => {
                log::warn!(
                    "[HeapManager::insert_tuple] FSM named page {} past the end of the heap (page_count={}); clearing it",
                    pid,
                    self.page_count()
                );
                self.fsm.fsm_set_avail(pid, 0, None)?;
                None
            }
            hit => hit,
        };
        if let Some(page_id) = candidate
            && let Some(slot_id) = self.insert_into_page(page_id, tuple_data, required_bytes)?
        {
            return Ok((page_id, slot_id));
        }

        let page_id = self.allocate_new_page()?;
        match self.insert_into_page(page_id, tuple_data, required_bytes)? {
            Some(slot_id) => Ok((page_id, slot_id)),
            None => Err(RookError::from(io::Error::other(
                "Could not find or allocate page with sufficient space",
            ))),
        }
    }

    /// Insert `tuple_data` into `page_id` if it has `required_bytes` free,
    /// then update the FSM and the tuple count. `None` when it has not; the
    /// FSM entry is corrected so later searches skip the page.
    fn insert_into_page(
        &mut self,
        page_id: u32,
        tuple_data: &[u8],
        required_bytes: u32,
    ) -> RookResult<Option<u32>> {
        // Read the target page via buffer pool (lock held for the whole
        // page access; released before FSM/header updates below).
        let (slot_id, new_free) = {
            let mut pool = Self::lock_pool(&self.pool);
            let frame_id = match pool.fetch_page(page_id) {
                Ok(fid) => fid,
                Err(e) => {
                    log::error!("[ERROR] Failed to read page {}: {}", page_id, e);
                    return Err(e);
                }
            };

            // Get current free space and verify
            let current_free = page_free_space(pool.get_page(frame_id))?;
            if current_free < required_bytes {
                // Unpin without dirty (we didn't modify)
                pool.unpin(frame_id, false);
                drop(pool);
                self.fsm.fsm_set_avail(page_id, current_free, None)?;
                return Ok(None);
            }

            // Insert tuple into slotted page (via buffer pool)
            let epoch = crate::page::dead_slot_epoch();
            let scan_from = match self.insert_hint {
                Some((p, clean, e)) if p == page_id && e == epoch => clean,
                _ => 0,
            };
            let inserted = pool
                .get_page_mut(frame_id)
                .insert_from(tuple_data, scan_from);
            if let Ok(slot) = &inserted {
                self.insert_hint = Some((page_id, slot + 1, epoch));
            }
            let new_free = inserted
                .is_ok()
                .then(|| page_free_space(pool.get_page(frame_id)));
            // Unpin page (dirty — modifications need to be written back)
            pool.unpin(frame_id, true);
            (inserted?, new_free.transpose()?.unwrap_or(0))
        };

        // Update FSM with new free space
        self.fsm.fsm_set_avail(page_id, new_free, None)?;

        // Increment tuple counter
        self.update_header(|h| h.total_tuples += 1)?;

        Ok(Some(slot_id))
    }

    /// Retrieve a tuple by page and slot coordinates.
    ///
    /// Uses the buffer pool for page access.
    pub fn get_tuple(&mut self, page_id: u32, slot_id: u32) -> RookResult<Vec<u8>> {
        let mut row = Vec::new();
        self.for_tuples_on_page(page_id, &[slot_id], |_, bytes| {
            row = bytes.to_vec();
            Ok(())
        })?;
        Ok(row)
    }

    /// Hand the rows at `slots` of page `page_id` to `f` as `(slot, bytes)`,
    /// in the order given, fetching the page from the pool once for all of
    /// them. A slot out of range, or holding no live row, is an error.
    pub fn for_tuples_on_page(
        &mut self,
        page_id: u32,
        slots: &[u32],
        mut f: impl FnMut(u32, &[u8]) -> RookResult<()>,
    ) -> RookResult<()> {
        HEAP_METRICS
            .get_tuple_calls
            .fetch_add(slots.len(), Ordering::Relaxed);

        if page_id >= self.page_count() {
            return Err(RookError::io(
                io::ErrorKind::InvalidData,
                format!(
                    "Page {} out of bounds (page_count={})",
                    page_id,
                    self.page_count()
                ),
            ));
        }

        let mut pool = Self::lock_pool(&self.pool);
        let frame_id = pool.fetch_page(page_id)?;
        let result = (|| -> RookResult<()> {
            let page = pool.get_page(frame_id);
            let tuple_count = get_tuple_count(page)?;
            for &slot_id in slots {
                if slot_id >= tuple_count {
                    return Err(RookError::io(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Slot {} out of bounds (tuple_count={})",
                            slot_id, tuple_count
                        ),
                    ));
                }
                let (offset, length) = get_slot_entry(page, slot_id)?;
                if offset == 0 && length == 0 {
                    return Err(RookError::io(
                        io::ErrorKind::NotFound,
                        format!(
                            "Tuple at page {}, slot {} is deleted or slot is unused",
                            page_id, slot_id
                        ),
                    ));
                }
                if offset as usize + length as usize > PAGE_SIZE {
                    return Err(RookError::io(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Tuple bounds exceed page: offset={}, length={}",
                            offset, length
                        ),
                    ));
                }
                f(
                    slot_id,
                    &page.data[offset as usize..(offset + length) as usize],
                )?;
            }
            Ok(())
        })();
        // Unpinned (clean: only read) whatever happened.
        pool.unpin(frame_id, false);
        result
    }

    /// Delete a tuple by marking it as deleted and updating FSM.
    ///
    /// Uses the buffer pool for page access.
    pub fn delete_tuple(&mut self, page_id: u32, slot_id: u32) -> RookResult<u32> {
        use crate::backend::instrumentation::HEAP_METRICS;
        HEAP_METRICS
            .insert_tuple_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Validate page_id
        if page_id >= self.page_count() {
            return Err(RookError::io(
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

            let delete_res = (|| -> RookResult<u32> {
                let page = pool.get_page_mut(frame_id);

                // Get the tuple data to calculate freed bytes
                let (offset, length) = get_slot_entry(page, slot_id)?;
                if offset == 0 && length == 0 {
                    // Already deleted: nothing to free and nothing to count.
                    return Ok(0);
                }
                let freed = length + ITEM_ID_SIZE;

                // Update the slot directory entry to mark as deleted
                let slot_offset = PAGE_HEADER_SIZE + slot_id * ITEM_ID_SIZE;
                page.set_slot(
                    slot_id,
                    Slot {
                        offset: 0,
                        length: 0,
                        flags: 0,
                    },
                );

                // Optimization: Roll back pointers if this is the last tuple
                let mut lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
                let mut upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

                let tuple_count = get_tuple_count(page)?;

                if offset == upper {
                    upper += length;
                    page.data[4..8].copy_from_slice(&upper.to_le_bytes());
                }

                if slot_id == tuple_count - 1 && lower == slot_offset + ITEM_ID_SIZE {
                    lower -= ITEM_ID_SIZE;
                    page.data[0..4].copy_from_slice(&lower.to_le_bytes());
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
    pub fn header(&self) -> RookResult<HeaderMetadata> {
        read_header_via_pool(&mut Self::lock_pool(&self.pool))
    }

    /// Change the file header in the pool's page 0.
    pub fn update_header(&self, f: impl FnOnce(&mut HeaderMetadata)) -> RookResult<()> {
        let mut pool = Self::lock_pool(&self.pool);
        let mut header = read_header_via_pool(&mut pool)?;
        f(&mut header);
        write_header_via_pool(&mut pool, &header)
    }

    /// Search for a page with available space (for testing/debugging).
    pub fn fsm_search_for_page(&mut self, min_category: u8) -> RookResult<Option<u32>> {
        self.fsm.search_page(min_category)
    }

    /// Persist all changes to disk.
    /// - Flushes all dirty pages in the buffer pool
    /// - Syncs FSM fork
    pub fn flush(&mut self) -> RookResult<()> {
        self.flush_inner(true)
    }

    /// Write all changes to the files without fsync.
    ///
    /// Enough for direct-I/O readers to see them and for a process crash;
    /// `flush` adds the fsync that covers a power loss.
    pub fn write_through(&mut self) -> RookResult<()> {
        self.flush_inner(false)
    }

    fn flush_inner(&mut self, durable: bool) -> RookResult<()> {
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

    // ─────────────────────────────────────────────────────────────────────
    // Private Helper Methods
    // ─────────────────────────────────────────────────────────────────────

    /// Allocate a new heap page and register with FSM.
    /// Uses the buffer pool for page creation.
    fn allocate_new_page(&mut self) -> RookResult<u32> {
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

        // Calculate FSM pages needed
        let new_fsm_page_count = FSM::calculate_fsm_page_count(page_count);
        self.update_header(|h| {
            h.page_count = page_count;
            h.fsm_page_count = new_fsm_page_count;
        })?;

        // Register new page with FSM (full free space)
        let initial_free = PAGE_SIZE as u32 - PAGE_HEADER_SIZE;
        self.fsm.fsm_set_avail(new_page_id, initial_free, None)?;

        Ok(new_page_id)
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
    fn test_fsm_entry_past_the_heap_is_cleared() {
        let (path, mut manager) = setup_temp_heap("test_fsm_phantom");
        // An entry for page 40 of a two-page heap, as a stale fork would
        // have: the insert goes to a real page and the entry is gone after.
        manager
            .fsm
            .fsm_set_avail(40, PAGE_SIZE as u32, None)
            .unwrap();
        manager.fsm.fsm_set_avail(1, 0, None).unwrap();
        let (page, _) = manager.insert_tuple(&[7u8; 100]).unwrap();
        assert!(page < manager.page_count());
        assert_ne!(manager.fsm.search_page(1).unwrap(), Some(40));
        cleanup_temp_heap(&path);
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

    /// After a stale FSM hit (a full page advertised as free, or a page
    /// beyond the file) the row goes to a fresh page, and a full page's
    /// entry is corrected.
    #[test]
    fn test_insert_after_stale_fsm_hit_uses_a_fresh_page() {
        let (path, mut manager) = setup_temp_heap("test_insert_stale_fsm");
        let tuple_data = vec![7u8; 200];
        let (first_page, _) = manager.insert_tuple(&tuple_data).unwrap();
        while manager.insert_tuple(&tuple_data).unwrap().0 == first_page {}
        let category = HeapManager::bytes_to_category(200 + ITEM_ID_SIZE);

        // Only the full first page advertises room.
        for page in 1..manager.page_count() {
            manager.fsm.fsm_set_avail(page, 0, None).unwrap();
        }
        manager
            .fsm
            .fsm_set_avail(first_page, PAGE_SIZE as u32 / 2, None)
            .unwrap();
        let pages = manager.page_count();
        let (page, _) = manager.insert_tuple(&tuple_data).unwrap();
        assert_eq!(page, pages, "row goes to a fresh page");
        assert_ne!(
            manager.fsm_search_for_page(category).unwrap(),
            Some(first_page)
        );

        // A page beyond the file.
        for page in 1..manager.page_count() {
            manager.fsm.fsm_set_avail(page, 0, None).unwrap();
        }
        let phantom = manager.page_count() + 5;
        manager
            .fsm
            .fsm_set_avail(phantom, PAGE_SIZE as u32 / 2, None)
            .unwrap();
        let pages = manager.page_count();
        let (page, _) = manager.insert_tuple(&tuple_data).unwrap();
        assert_eq!(page, pages);

        drop(manager);
        cleanup_temp_heap(&path);
    }
}
