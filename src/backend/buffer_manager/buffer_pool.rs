//! CLOCK Buffer Pool — A fixed-capacity page cache with CLOCK-sweep eviction.
//!
//! # Design
//!
//! The buffer pool manages a fixed number of **frames**, each holding one cached
//! `Page`. Frames are indexed by `FrameId`. A `HashMap<u32, FrameId>` provides
//! O(1) lookup from page number to frame.
//!
//! ## CLOCK-sweep eviction
//!
//! Each frame has a `clock_bit`. The CLOCK hand sweeps through frames cyclically:
//! - If the frame at the hand is **pinned** (pin_count > 0), skip it.
//! - If `clock_bit` is 1, clear it to 0 and advance.
//! - If `clock_bit` is 0 and the frame is unpinned, **evict** it (write back if
//!   dirty) and reuse for the new page.
//!
//! ## FrameGuard (RAII)
//!
//! `FrameGuard` wraps a pinned frame and automatically unpins it on drop. It
//! provides `get_page()` and `get_page_mut()` for accessing the underlying page.
//! Callers should hold a `FrameGuard` for the minimum time needed.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::page::{PAGE_SIZE, Page, init_page};

/// Opaque identifier for a frame within the buffer pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameId(usize);

impl FrameId {
    pub fn index(&self) -> usize {
        self.0
    }
}

/// RAII guard that holds a pinned frame and auto-unpins on drop.
///
/// Obtained from `BufferPool::fetch_page_guarded()`. Provides access to the
/// underlying page and automatically unpins (optionally dirty) when dropped.
///
/// # Example
///
/// ```ignore
/// let guard = pool.fetch_page_guarded(1)?;
/// {
///     let page = guard.get_page();
///     // read from page
/// }
/// let page_mut = guard.get_page_mut();
/// page_mut.data[..4].copy_from_slice(&val.to_le_bytes());
/// // guard drops here → auto-unpins with is_dirty=true
/// ```
pub struct FrameGuard<'a> {
    pool: &'a mut BufferPool,
    frame_id: FrameId,
    /// Set to true if the page was modified; used on drop to mark dirty.
    is_dirty: bool,
}

impl<'a> FrameGuard<'a> {
    /// Create a new frame guard.
    pub(crate) fn new(pool: &'a mut BufferPool, frame_id: FrameId) -> Self {
        Self {
            pool,
            frame_id,
            is_dirty: false,
        }
    }

    /// Access the underlying page (read-only).
    pub fn get_page(&self) -> &Page {
        &self.pool.frames[self.frame_id.0].page
    }

    /// Access the underlying page (mutable). Marks the page as dirty on drop.
    pub fn get_page_mut(&mut self) -> &mut Page {
        self.is_dirty = true;
        self.pool.frames[self.frame_id.0].clock_bit = true;
        &mut self.pool.frames[self.frame_id.0].page
    }

    /// The frame ID held by this guard.
    pub fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    /// Explicitly mark the page as dirty (or not).
    pub fn mark_dirty(&mut self, dirty: bool) {
        self.is_dirty = dirty;
    }
}

impl<'a> Drop for FrameGuard<'a> {
    fn drop(&mut self) {
        self.pool.unpin(self.frame_id, self.is_dirty);
    }
}

/// `(device, inode)` of an open file, where the platform exposes one.
#[cfg(unix)]
fn file_ident(file: &File) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    file.metadata().ok().map(|m| (m.dev(), m.ino()))
}

#[cfg(not(unix))]
fn file_ident(_file: &File) -> Option<(u64, u64)> {
    None
}

/// A single frame in the buffer pool.
struct Frame {
    /// The page number this frame holds (`u32::MAX` if empty/free).
    page_id: u32,
    /// The cached page data.
    page: Page,
    /// Whether the page has been modified since last written to disk.
    is_dirty: bool,
    /// Number of outstanding pins. A pinned frame cannot be evicted.
    pin_count: usize,
    /// CLOCK-sweep reference bit. Set on each access, cleared by the hand.
    clock_bit: bool,
    /// Whether this frame is occupied (holds a valid page).
    occupied: bool,
}

impl Frame {
    fn new() -> Self {
        Self {
            page_id: u32::MAX,
            // No buffer until the frame is first filled: every load path
            // (`fetch_page`, `new_page`, `allocate_raw_page`) moves a complete
            // page in, and a `FrameId` is only handed out for a filled frame.
            // Allocating `PAGE_SIZE` per frame up front made opening a pool
            // cost ~2 ms per 256 frames, paid on every index scan.
            page: Page { data: Vec::new() },
            is_dirty: false,
            pin_count: 0,
            clock_bit: false,
            occupied: false,
        }
    }
}

/// The CLOCK buffer pool.
///
/// Manages a fixed set of frames. Automatically loads pages from disk on
/// cache misses and evicts unpinned pages when capacity is reached.
pub struct BufferPool {
    /// Fixed-size array of frames.
    frames: Vec<Frame>,
    /// Fast page_id → frame_index lookup.
    frame_table: HashMap<u32, usize>,
    /// Current CLOCK hand position.
    clock_hand: usize,
    /// Maximum number of frames.
    capacity: usize,
    /// Path to the managed file (lazily opened).
    file_path: Option<std::path::PathBuf>,
    /// Cached file handle for I/O.
    pub(crate) file: Option<File>,
    /// `(device, inode)` of `file`, read once when the handle is attached.
    file_ident: Option<(u64, u64)>,
    /// Total number of pages in the managed file (from header).
    total_pages: u32,
    /// Pages were written to the file since the last fsync.
    unsynced: bool,
}

impl BufferPool {
    /// Create a new buffer pool with the given capacity (in frames).
    ///
    /// A reasonable default capacity for small databases is 64 frames (512 KB).
    /// Larger databases should use 1024+ frames (8 MB+).
    pub fn new(capacity: usize) -> Self {
        let mut frames = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            frames.push(Frame::new());
        }

        Self {
            frames,
            frame_table: HashMap::new(),
            clock_hand: 0,
            capacity,
            file_path: None,
            file: None,
            unsynced: false,
            file_ident: None,
            total_pages: 0,
        }
    }

    /// Create a buffer pool with an already-open file handle.
    ///
    /// Reads the header page to determine the total number of pages.
    /// This is used by HeapManager to share its opened file with the pool.
    pub fn with_file(capacity: usize, mut file: File, file_path: PathBuf) -> io::Result<Self> {
        let mut pool = Self::new(capacity);

        // Read header page to get page count
        let file_size = file.metadata()?.len();
        if file_size >= PAGE_SIZE as u64 {
            let mut header = Page::new();
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut header.data)?;
            pool.total_pages = u32::from_le_bytes(header.data[0..4].try_into().unwrap());
        }

        pool.file_path = Some(file_path);
        pool.file_ident = file_ident(&file);
        pool.file = Some(file);
        log::info!(
            "BufferPool created with capacity={}, file has {} pages",
            pool.capacity,
            pool.total_pages
        );

        Ok(pool)
    }

    /// Create a buffer pool for a raw page file (such as a B+ Tree .idx file)
    /// where total pages is derived directly from file size, not a heap header.
    pub fn with_file_raw(
        capacity: usize,
        file: File,
        file_path: PathBuf,
        total_pages: u32,
    ) -> Self {
        let mut pool = Self::new(capacity);
        pool.file_path = Some(file_path);
        pool.file_ident = file_ident(&file);
        pool.file = Some(file);
        pool.total_pages = total_pages;
        pool
    }

    /// Open a file for the buffer pool to manage.
    ///
    /// Reads the header page to determine the total number of pages.
    pub fn open(&mut self, path: &Path) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        // Read header page to get page count
        let file_size = file.metadata()?.len();
        if file_size >= PAGE_SIZE as u64 {
            let mut header = Page::new();
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut header.data)?;
            // Page count is stored in first 4 bytes of the header
            self.total_pages = u32::from_le_bytes(header.data[0..4].try_into().unwrap());
        } else {
            // File is empty or too small — treat as new
            self.total_pages = 0;
        }

        self.file_path = Some(path.to_path_buf());
        self.file_ident = file_ident(&file);
        self.file = Some(file);
        log::info!(
            "BufferPool opened with capacity={}, file has {} pages",
            self.capacity,
            self.total_pages
        );

        Ok(())
    }

    /// Check if the pool has a file open.
    pub fn is_open(&self) -> bool {
        self.file.is_some()
    }

    /// Check if the underlying open file handle is stale compared to the file
    /// currently at `path` on disk (e.g. if the file was replaced by rename/swap).
    pub fn is_file_stale(&self, path: &Path) -> bool {
        if let Some(open_file) = &self.file {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                // The open handle's identity never changes, so it is read
                // once when the file is attached; only the path is re-checked.
                let ident = self.file_ident.or_else(|| file_ident(open_file));
                if let (Some((dev, ino)), Ok(disk_meta)) = (ident, std::fs::metadata(path)) {
                    return ino != disk_meta.ino() || dev != disk_meta.dev();
                }
            }
            #[cfg(windows)]
            {
                use std::os::windows::fs::MetadataExt;
                if let (Ok(open_meta), Ok(disk_meta)) =
                    (open_file.metadata(), std::fs::metadata(path))
                {
                    return open_meta.file_index() != disk_meta.file_index()
                        || open_meta.volume_serial_number() != disk_meta.volume_serial_number();
                }
            }
        }
        false
    }

    /// Return the total number of pages tracked by this pool.
    pub fn total_pages(&self) -> u32 {
        self.total_pages
    }

    // ─── Core Page Fetch API ─────────────────────────────────────────────

    /// Fetch a page from the buffer pool, pinning it in memory.
    ///
    /// If the page is not already cached, it will be loaded from disk,
    /// potentially evicting an unpinned page.
    ///
    /// Returns a `FrameId` that can be used with `get_page()` / `get_page_mut()`.
    /// Call `unpin()` when done to allow eviction.
    pub fn fetch_page(&mut self, page_id: u32) -> io::Result<FrameId> {
        // 1. Check if already cached
        if let Some(&frame_idx) = self.frame_table.get(&page_id) {
            let frame = &mut self.frames[frame_idx];
            frame.pin_count += 1;
            frame.clock_bit = true;
            return Ok(FrameId(frame_idx));
        }

        // 2. Cache miss — find or evict a frame
        let frame_idx = self.evict_frame()?;

        // 3. Load page data from disk into a temporary buffer first
        //    (resolves borrow conflict: frames is borrowed after file I/O completes)
        let mut temp_page = Page::new();
        self.read_page_from_disk(&mut temp_page, page_id)?;

        // 4. Store the loaded page in the frame
        {
            let frame = &mut self.frames[frame_idx];
            frame.page_id = page_id;
            frame.page = temp_page;
            frame.is_dirty = false;
            frame.pin_count = 1;
            frame.clock_bit = true;
            frame.occupied = true;
        }

        // 5. Update lookup table
        self.frame_table.insert(page_id, frame_idx);

        log::trace!(
            "BufferPool: fetched page {} into frame {}",
            page_id,
            frame_idx
        );
        Ok(FrameId(frame_idx))
    }

    /// Allocate a new page (extend the file).
    ///
    /// Returns `(page_id, frame_id)` for the newly created page.
    pub fn new_page(&mut self) -> io::Result<(u32, FrameId)> {
        let page_id = self.total_pages;

        // 1. Evict first, BEFORE modifying the file (avoids borrow conflict with file)
        let frame_idx = self.evict_frame()?;

        // 2. Get file handle AFTER eviction (separate mutable borrow)
        let file: &mut File = self.file.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "BufferPool has no open file")
        })?;

        // 3. Create initialized page
        let mut page = Page::new();
        init_page(&mut page);

        // 4. Append to file
        file.seek(SeekFrom::End(0))?;
        file.write_all(&page.data)?;
        file.flush()?;

        // 5. Update header page count on disk
        self.total_pages = page_id + 1;
        let mut header_buf = [0u8; 8];
        header_buf[0..4].copy_from_slice(&self.total_pages.to_le_bytes());
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header_buf[0..4])?;
        file.flush()?;

        // A resident header page must agree with the file, or flushing it
        // later would write the old count back.
        if let Some(&header_idx) = self.frame_table.get(&0) {
            self.frames[header_idx].page.data[0..4]
                .copy_from_slice(&self.total_pages.to_le_bytes());
        }

        // 6. Store in pool (not dirty — already written to disk)
        let frame = &mut self.frames[frame_idx];
        frame.page_id = page_id;
        frame.page = page;
        frame.is_dirty = false;
        frame.pin_count = 1;
        frame.clock_bit = true;
        frame.occupied = true;

        self.frame_table.insert(page_id, frame_idx);

        log::trace!("BufferPool: allocated new page {}", page_id);
        Ok((page_id, FrameId(frame_idx)))
    }

    /// Allocate a new raw page (without initializing heap headers or rewriting page 0).
    /// Used for index files where page 0 is a B+ Tree node.
    pub fn allocate_raw_page(&mut self, data: &[u8]) -> io::Result<u32> {
        let page_id = self.total_pages;

        // 1. Evict first if needed
        let frame_idx = self.evict_frame()?;

        // 2. Append to file
        let file: &mut File = self.file.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "BufferPool has no open file")
        })?;
        file.seek(SeekFrom::End(0))?;
        file.write_all(data)?;
        file.flush()?;

        self.total_pages = page_id + 1;

        // 3. Store in pool
        let mut page = Page::new();
        page.data[..data.len()].copy_from_slice(data);
        let frame = &mut self.frames[frame_idx];
        frame.page_id = page_id;
        frame.page = page;
        frame.is_dirty = false;
        frame.pin_count = 0;
        frame.clock_bit = true;
        frame.occupied = true;

        self.frame_table.insert(page_id, frame_idx);
        Ok(page_id)
    }

    /// Read raw page bytes using the buffer pool cache.
    /// Hits return without any disk I/O!
    pub fn read_page_bytes(&mut self, page_id: u32) -> io::Result<Vec<u8>> {
        let frame_id = self.fetch_page(page_id)?;
        let data = self.get_page(frame_id).data.clone();
        self.unpin(frame_id, false);
        Ok(data)
    }

    /// Write raw page bytes through the buffer pool (marks page dirty).
    pub fn write_page_bytes(&mut self, page_id: u32, data: &[u8]) -> io::Result<()> {
        let frame_id = self.fetch_page(page_id)?;
        let page = self.get_page_mut(frame_id);
        page.data[..data.len()].copy_from_slice(data);
        self.unpin(frame_id, true);
        Ok(())
    }

    /// Access a pinned page by frame ID.
    pub fn get_page(&self, frame_id: FrameId) -> &Page {
        &self.frames[frame_id.0].page
    }

    /// Mutably access a pinned page by frame ID.
    pub fn get_page_mut(&mut self, frame_id: FrameId) -> &mut Page {
        let frame = &mut self.frames[frame_id.0];
        frame.clock_bit = true;
        &mut frame.page
    }

    /// Unpin a frame, optionally marking it as dirty.
    ///
    /// If `is_dirty` is true and the frame was not already dirty, it will be
    /// marked so the eviction algorithm knows to write it back.
    pub fn unpin(&mut self, frame_id: FrameId, is_dirty: bool) {
        let frame = &mut self.frames[frame_id.0];
        if frame.pin_count > 0 {
            frame.pin_count -= 1;
        }
        if is_dirty {
            frame.is_dirty = true;
        }
    }

    // ─── Flush API ───────────────────────────────────────────────────────

    /// Flush a specific page to disk if it is dirty.
    pub fn flush_page(&mut self, page_id: u32) -> io::Result<()> {
        if let Some(&frame_idx) = self.frame_table.get(&page_id)
            && self.frames[frame_idx].is_dirty
        {
            self.write_page_to_disk(frame_idx)?;
        }
        Ok(())
    }

    /// Whether any frame holds unflushed modifications.
    ///
    /// Lets callers skip flush cycles entirely when the pool is clean —
    /// repeated checkpoints during read workloads stay free.
    pub fn has_dirty(&self) -> bool {
        self.frames.iter().any(|f| f.occupied && f.is_dirty)
    }

    /// Write all dirty pages to the file without fsync.
    ///
    /// The pages become visible to direct-I/O readers and survive a process
    /// crash; `flush_all` makes them survive a power loss.
    pub fn write_dirty(&mut self) -> io::Result<()> {
        for i in 0..self.capacity {
            if self.frames[i].occupied && self.frames[i].is_dirty {
                self.write_page_to_disk(i)?;
                self.unsynced = true;
            }
        }
        Ok(())
    }

    /// Flush all dirty pages to disk and fsync.
    pub fn flush_all(&mut self) -> io::Result<()> {
        self.write_dirty()?;
        if self.unsynced
            && let Some(ref mut file) = self.file
        {
            file.flush()?;
            file.sync_all()?;
        }
        self.unsynced = false;
        Ok(())
    }

    /// Fetch a page and return a `FrameGuard` that auto-unpins on drop.
    ///
    /// This is the recommended way to fetch pages as it prevents forgetting
    /// to call `unpin()`. Mark the frame dirty via `guard.mark_dirty(true)`
    /// or by calling `guard.get_page_mut()` (which auto-marks dirty).
    pub fn fetch_page_guarded(&mut self, page_id: u32) -> io::Result<FrameGuard<'_>> {
        let frame_id = self.fetch_page(page_id)?;
        Ok(FrameGuard::new(self, frame_id))
    }

    // ─── Statistics ──────────────────────────────────────────────────────

    /// Number of frames currently occupied.
    pub fn occupied_count(&self) -> usize {
        self.frames.iter().filter(|f| f.occupied).count()
    }

    /// Number of frames currently pinned.
    pub fn pinned_count(&self) -> usize {
        self.frames.iter().filter(|f| f.pin_count > 0).count()
    }

    /// Number of dirty frames.
    pub fn dirty_count(&self) -> usize {
        self.frames
            .iter()
            .filter(|f| f.occupied && f.is_dirty)
            .count()
    }

    /// Total capacity of the pool.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Current cache hit rate metrics.
    pub fn utilization(&self) -> f64 {
        self.occupied_count() as f64 / self.capacity as f64
    }

    // ─── Internal Helpers ────────────────────────────────────────────────

    /// CLOCK-sweep eviction: find a frame to reuse.
    ///
    /// Algorithm:
    /// 1. Start at `clock_hand` and cycle through frames.
    /// 2. Skip pinned frames (pin_count > 0).
    /// 3. If clock_bit is 1, clear it to 0 and advance.
    /// 4. If clock_bit is 0, evict this frame (write back if dirty).
    ///
    /// Returns the index of the evicted (now available) frame.
    fn evict_frame(&mut self) -> io::Result<usize> {
        loop {
            for _ in 0..self.capacity {
                let idx = self.clock_hand;
                self.clock_hand = (self.clock_hand + 1) % self.capacity;

                if !self.frames[idx].occupied {
                    // Free slot — reuse immediately
                    return Ok(idx);
                }

                if self.frames[idx].pin_count > 0 {
                    // Pinned — skip
                    continue;
                }

                if self.frames[idx].clock_bit {
                    // Recently used — clear bit and keep looking
                    self.frames[idx].clock_bit = false;
                    continue;
                }

                // Evict this frame
                if self.frames[idx].is_dirty {
                    self.write_page_to_disk(idx)?;
                }

                // Remove from lookup table
                let page_id = self.frames[idx].page_id;
                self.frame_table.remove(&page_id);

                // Reset the frame
                self.frames[idx].occupied = false;
                self.frames[idx].is_dirty = false;
                self.frames[idx].pin_count = 0;
                self.frames[idx].clock_bit = false;
                self.frames[idx].page_id = u32::MAX;

                log::trace!("BufferPool: evicted frame {} (page {})", idx, page_id);
                return Ok(idx);
            }

            // If we cycled through all frames without finding a victim,
            // all unpinned frames were recently used and got their bits cleared.
            // Try one more full sweep — this time we'll find victims.
            // If still nothing, all frames are pinned (deadlock).
            let all_pinned = self.frames.iter().all(|f| f.pin_count > 0);
            if all_pinned {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "BufferPool: all frames are pinned — cannot evict",
                ));
            }
        }
    }

    /// Read a page from disk into the given Page struct.
    fn read_page_from_disk(&mut self, page: &mut Page, page_id: u32) -> io::Result<()> {
        let file = self.file.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "BufferPool has no open file")
        })?;

        let offset = page_id as u64 * PAGE_SIZE as u64;
        let file_size = file.metadata()?.len();

        if offset >= file_size {
            // An allocated page that has not reached disk yet reads as an
            // empty page. A page id that was never allocated is a stale
            // pointer (from the FSM or an index): fail instead of inventing
            // a blank page that could later be written past end of file.
            if page_id >= self.total_pages {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "page {} does not exist (file has {} pages)",
                        page_id, self.total_pages
                    ),
                ));
            }
            init_page(page);
            return Ok(());
        }

        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut page.data)?;

        Ok(())
    }

    /// Write a frame's page to disk.
    fn write_page_to_disk(&mut self, frame_idx: usize) -> io::Result<()> {
        let file = self.file.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "BufferPool has no open file")
        })?;

        let page_id = self.frames[frame_idx].page_id;
        let offset = page_id as u64 * PAGE_SIZE as u64;

        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&self.frames[frame_idx].page.data)?;

        self.frames[frame_idx].is_dirty = false;

        log::trace!("BufferPool: wrote page {} to disk", page_id);
        Ok(())
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        if let Err(e) = self.flush_all() {
            log::error!("BufferPool: error flushing on drop: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_path() -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("rookdb_buffer_pool_test_{}.dat", nanos))
    }

    fn create_test_file(path: &std::path::Path, num_pages: u32) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        // Write header page (page 0) with page count
        let mut header = vec![0u8; PAGE_SIZE];
        header[0..4].copy_from_slice(&num_pages.to_le_bytes());
        file.write_all(&header)?;

        // Write initialized data pages
        for _ in 1..num_pages {
            let mut page = Page::new();
            init_page(&mut page);
            file.write_all(&page.data)?;
        }

        file.flush()?;
        file.sync_all()?;
        Ok(())
    }

    #[test]
    fn test_new_pool_empty() {
        let pool = BufferPool::new(64);
        assert_eq!(pool.capacity(), 64);
        assert_eq!(pool.occupied_count(), 0);
        assert_eq!(pool.pinned_count(), 0);
        assert_eq!(pool.dirty_count(), 0);
    }

    #[test]
    fn test_open_and_fetch_page() {
        let path = unique_temp_path();
        create_test_file(&path, 10).expect("create test file");

        let mut pool = BufferPool::new(16);
        pool.open(&path).expect("open pool");

        assert!(pool.is_open());
        assert_eq!(pool.total_pages(), 10);

        // Fetch page 1 (first data page)
        let frame_id = pool.fetch_page(1).expect("fetch page");
        let page = pool.get_page(frame_id);
        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        assert_eq!(
            lower, 8,
            "initialized page should have lower=8 (PAGE_HEADER_SIZE)"
        );

        pool.unpin(frame_id, false);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn test_fetch_same_page_twice() {
        let path = unique_temp_path();
        create_test_file(&path, 5).expect("create test file");

        let mut pool = BufferPool::new(16);
        pool.open(&path).expect("open pool");

        let f1 = pool.fetch_page(1).expect("first fetch");
        let f2 = pool.fetch_page(1).expect("second fetch (cache hit)");

        // Same frame should be returned
        assert_eq!(f1.index(), f2.index());

        // Pin count should be 2
        pool.unpin(f1, false);
        pool.unpin(f2, false);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn test_dirty_page_flush() {
        let path = unique_temp_path();
        create_test_file(&path, 5).expect("create test file");

        let mut pool = BufferPool::new(16);
        pool.open(&path).expect("open pool");

        let frame_id = pool.fetch_page(1).expect("fetch page");
        {
            let page = pool.get_page_mut(frame_id);
            // Write some data
            page.data[100..104].copy_from_slice(&42u32.to_le_bytes());
        }
        pool.unpin(frame_id, true); // mark as dirty

        // Flush should write to disk
        pool.flush_page(1).expect("flush page");

        // Verify no dirty pages remain
        assert_eq!(pool.dirty_count(), 0);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn test_flush_all_dirty_pages() {
        let path = unique_temp_path();
        create_test_file(&path, 10).expect("create test file");

        let mut pool = BufferPool::new(16);
        pool.open(&path).expect("open pool");

        let f1 = pool.fetch_page(1).expect("fetch page 1");
        let f2 = pool.fetch_page(2).expect("fetch page 2");
        pool.unpin(f1, true);
        pool.unpin(f2, true);

        assert_eq!(pool.dirty_count(), 2);

        pool.flush_all().expect("flush all");
        assert_eq!(pool.dirty_count(), 0);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn test_write_dirty_reaches_the_file_without_flush_all() {
        let path = unique_temp_path();
        create_test_file(&path, 3).expect("create test file");

        let mut pool = BufferPool::new(16);
        pool.open(&path).expect("open pool");

        let frame_id = pool.fetch_page(1).expect("fetch page 1");
        pool.get_page_mut(frame_id).data[100..104].copy_from_slice(&42u32.to_le_bytes());
        pool.unpin(frame_id, true);

        pool.write_dirty().expect("write dirty");
        assert_eq!(pool.dirty_count(), 0);

        // A second handle on the file sees the page while the pool is alive.
        let bytes = fs::read(&path).expect("read file");
        let offset = PAGE_SIZE + 100;
        assert_eq!(&bytes[offset..offset + 4], &42u32.to_le_bytes());

        fs::remove_file(&path).ok();
    }

    #[test]
    fn test_new_page_allocation() {
        let path = unique_temp_path();
        create_test_file(&path, 2).expect("create test file"); // header + 1 data page

        let mut pool = BufferPool::new(16);
        pool.open(&path).expect("open pool");

        assert_eq!(pool.total_pages(), 2);

        let (page_id, frame_id) = pool.new_page().expect("new page");
        assert_eq!(page_id, 2); // third page (0=header, 1=data, 2=new)
        assert_eq!(pool.total_pages(), 3);

        let page = pool.get_page(frame_id);
        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        assert_eq!(lower, 8, "new page should be initialized with lower=8");

        pool.unpin(frame_id, false);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn test_clock_eviction_basic() {
        let path = unique_temp_path();
        create_test_file(&path, 100).expect("create test file");

        // Pool with only 4 frames
        let mut pool = BufferPool::new(4);
        pool.open(&path).expect("open pool");

        // Fetch 4 pages (filling the pool), unpin each so they're evictable
        for i in 1..=4 {
            let f = pool.fetch_page(i).expect("fetch page");
            pool.unpin(f, false);
        }

        // Fetch page 5 — this should trigger eviction of one of the first 4
        let f5 = pool
            .fetch_page(5)
            .expect("fetch page 5 should trigger eviction");
        pool.unpin(f5, false);

        // The 5th fetch succeeded — eviction worked
        assert_eq!(
            pool.occupied_count(),
            4,
            "pool should have 4 frames occupied after eviction"
        );

        // We should be able to re-fetch a page that may have been evicted
        let re_fetch = pool.fetch_page(1).expect("re-fetch page 1");
        pool.unpin(re_fetch, false);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn test_pinned_page_not_evicted() {
        let path = unique_temp_path();
        create_test_file(&path, 100).expect("create test file");

        let mut pool = BufferPool::new(4);
        pool.open(&path).expect("open pool");

        // Fetch and keep page 1 pinned
        let pinned = pool.fetch_page(1).expect("fetch pinned page");

        // Fetch enough pages to fill the pool
        for i in 2..=4 {
            let f = pool.fetch_page(i).expect("fetch page");
            pool.unpin(f, false);
        }

        // Now try to fetch page 5 — this should NOT evict page 1 (it's pinned)
        let f5 = pool.fetch_page(5).expect("fetch page 5");
        pool.unpin(f5, false);

        // Page 1 should still be cached
        let f1_again = pool.fetch_page(1).expect("re-fetch page 1");
        pool.unpin(f1_again, false);

        // Unpin the original
        pool.unpin(pinned, false);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn test_drop_flushes_dirty_pages() {
        let path = unique_temp_path();
        create_test_file(&path, 5).expect("create test file");

        let data_at_index: u32 = 42;
        let check_offset = 100;

        // Modify a page, mark dirty, then drop
        {
            let mut pool = BufferPool::new(16);
            pool.open(&path).expect("open pool");

            let f = pool.fetch_page(1).expect("fetch");
            {
                let page = pool.get_page_mut(f);
                page.data[check_offset..check_offset + 4]
                    .copy_from_slice(&data_at_index.to_le_bytes());
            }
            pool.unpin(f, true);
            // Pool drops here — should flush dirty pages
        }

        // Re-open the file and verify the data persisted
        let mut file = File::open(&path).expect("open file");
        let mut page = Page::new();
        file.seek(SeekFrom::Start(PAGE_SIZE as u64)).expect("seek");
        file.read_exact(&mut page.data).expect("read");
        let value = u32::from_le_bytes(
            page.data[check_offset..check_offset + 4]
                .try_into()
                .unwrap(),
        );
        assert_eq!(
            value, data_at_index,
            "dirty page should have been flushed on drop"
        );

        fs::remove_file(&path).ok();
    }

    #[test]
    fn test_utilization() {
        let mut pool = BufferPool::new(10);
        assert_eq!(pool.utilization(), 0.0);

        let path = unique_temp_path();
        create_test_file(&path, 5).expect("create test file");
        pool.open(&path).expect("open");

        // Fetch 3 pages
        for i in 1..=3 {
            let f = pool.fetch_page(i).expect("fetch");
            pool.unpin(f, false);
        }

        assert!((pool.utilization() - 0.3).abs() < 0.01);

        fs::remove_file(&path).ok();
    }

    #[test]
    fn test_cache_hit_preserves_data() {
        let path = unique_temp_path();
        create_test_file(&path, 5).expect("create test file");

        let mut pool = BufferPool::new(16);
        pool.open(&path).expect("open");

        // Write some data, flush, re-fetch, and verify
        let f1 = pool.fetch_page(1).expect("fetch");
        {
            let page = pool.get_page_mut(f1);
            page.data[200..204].copy_from_slice(&99u32.to_le_bytes());
        }
        pool.unpin(f1, true);
        pool.flush_page(1).expect("flush");

        // Re-fetch — should read from disk (not cached after flush? Actually it IS cached)
        let f2 = pool.fetch_page(1).expect("re-fetch");
        let page = pool.get_page(f2);
        let value = u32::from_le_bytes(page.data[200..204].try_into().unwrap());
        assert_eq!(value, 99, "data should persist through cache");
        pool.unpin(f2, false);

        fs::remove_file(&path).ok();
    }
}
