//! Global CLOCK buffer pool.
//!
//! # Design
//!
//! All page caching in the process goes through **one** frame store (the
//! global [`Store`]) with a single memory budget, [`shared_pool_capacity`]
//! frames of `PAGE_SIZE` bytes in total, however many tables and indexes are
//! open. A frame is addressed by `(file_id, page_id)`: every attached file
//! gets a fresh `file_id` from a counter that is never reused, so a file
//! replaced on disk (TRUNCATE, rewrite, DROP, index rebuild) gets a new
//! identity and can never see frames cached for its predecessor.
//!
//! [`BufferPool`] is a lightweight per-file *view* onto the store. It keeps
//! only per-file state (page count, path, file identity); the frames, the
//! `(file_id, page_id) -> frame` table, the CLOCK hand and the file handles
//! used to write victims back live in the store.
//!
//! ## CLOCK-sweep eviction
//!
//! Each frame has a `clock_bit`. The hand sweeps all frames, whichever file
//! they belong to:
//! - a **pinned** frame (pin_count > 0) is skipped;
//! - a frame with `clock_bit == 1` gets the bit cleared and a second chance;
//! - a frame with `clock_bit == 0` is the victim: it is written back to *its*
//!   file if dirty, and its buffer is reused for the incoming page (a miss
//!   reads straight into it, with no allocation).
//!
//! Frames are created lazily up to the budget, so an idle process holds no
//! page memory.
//!
//! ## Borrowing
//!
//! `page_bytes` / `page_bytes_mut` return [`PageRef`] / [`PageMut`], guards
//! that keep the frame pinned for as long as the borrow lives. `get_page`
//! on a `FrameId` relies on the pin taken by `fetch_page`.
//!
//! ## Locking
//!
//! The store is behind one mutex that is taken for each pool operation (and
//! held across the disk read/write of a miss or an eviction). The engine is
//! single-user, so this costs an uncontended lock per page access.

use crate::backend::error::{RookError, RookResult};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::hash::{BuildHasherDefault, Hasher};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use crate::page::{PAGE_SIZE, Page, init_page};

// ─── Fx-style hasher ─────────────────────────────────────────────────────

/// A tiny multiplicative hasher (the FxHash scheme) for the frame table.
/// Keys are small integers chosen by the engine, so SipHash's DoS resistance
/// buys nothing here and costs ~20 ns per lookup.
#[derive(Default, Clone, Copy)]
pub(crate) struct FxHasher {
    hash: u64,
}

const FX_SEED: u64 = 0x517c_c1b7_2722_0a95;

impl FxHasher {
    #[inline]
    fn add(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(FX_SEED);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut buf = [0u8; 8];
            buf[..chunk.len()].copy_from_slice(chunk);
            self.add(u64::from_le_bytes(buf));
        }
    }
    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.add(i as u64);
    }
    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.add(i);
    }
    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.add(i as u64);
    }
    #[inline]
    fn finish(&self) -> u64 {
        // hashbrown takes the bucket from the low bits and a tag from the
        // high bits; the multiply leaves the low bits weak, so fold.
        self.hash.rotate_left(26)
    }
}

type FxMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

// ─── Positional file I/O ─────────────────────────────────────────────────

fn read_full_at(file: &File, buf: &mut [u8], offset: u64) -> RookResult<usize> {
    let mut done = 0;
    while done < buf.len() {
        #[cfg(unix)]
        let res = std::os::unix::fs::FileExt::read_at(file, &mut buf[done..], offset + done as u64);
        #[cfg(windows)]
        let res =
            std::os::windows::fs::FileExt::seek_read(file, &mut buf[done..], offset + done as u64);
        match res {
            Ok(0) => break,
            Ok(n) => done += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(RookError::from(e)),
        }
    }
    Ok(done)
}

fn write_all_at(file: &File, buf: &[u8], offset: u64) -> RookResult<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::write_all_at(file, buf, offset).map_err(RookError::from)
    }
    #[cfg(windows)]
    {
        let mut done = 0;
        while done < buf.len() {
            match std::os::windows::fs::FileExt::seek_write(
                file,
                &buf[done..],
                offset + done as u64,
            ) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => done += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

// ─── Frame store ─────────────────────────────────────────────────────────

/// Opaque identifier for a frame within the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FrameId(usize);

impl FrameId {
    pub fn index(&self) -> usize {
        self.0
    }
}

#[inline]
fn frame_key(file_id: u32, page_id: u32) -> u64 {
    ((file_id as u64) << 32) | page_id as u64
}

/// A single frame.
struct Frame {
    /// `frame_key(file_id, page_id)` of the cached page (valid if occupied).
    key: u64,
    /// The page buffer. Boxed so its address is stable while the frame
    /// vector grows; the buffer is kept across evictions and reused.
    page: Box<Page>,
    is_dirty: bool,
    /// Number of outstanding pins. A pinned frame cannot be evicted.
    pin_count: usize,
    /// CLOCK reference bit. Set on each access, cleared by the hand.
    clock_bit: bool,
    occupied: bool,
}

/// Per-file bookkeeping inside the store.
struct FileEntry {
    /// Handle used to read misses and write victims back.
    file: Arc<File>,
    /// Number of dirty frames of this file.
    dirty: usize,
    /// Frames that became dirty since the last flush. May hold stale
    /// indices (evicted or already written); consumers re-check the frame.
    dirty_list: Vec<usize>,
    /// Number of frames of this file currently in the table.
    resident: usize,
    /// Pages were written since the last fsync.
    unsynced: bool,
}

struct Inner {
    frames: Vec<Frame>,
    /// Indices of unoccupied frames (their buffers are kept for reuse).
    free: Vec<usize>,
    table: FxMap<u64, usize>,
    files: FxMap<u32, FileEntry>,
    hand: usize,
    capacity: usize,
    next_file_id: u32,
}

/// A frame store with one memory budget. The process has a single global
/// instance; `BufferPool::new` builds private ones for isolated use.
pub struct Store {
    inner: Mutex<Inner>,
}

impl Store {
    fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                frames: Vec::new(),
                free: Vec::new(),
                table: FxMap::default(),
                files: FxMap::default(),
                hand: 0,
                capacity: capacity.max(1),
                next_file_id: 1,
            }),
        }
    }

    /// The process-wide store, sized by [`shared_pool_capacity`].
    pub fn global() -> &'static Arc<Store> {
        static GLOBAL: OnceLock<Arc<Store>> = OnceLock::new();
        GLOBAL.get_or_init(|| {
            Arc::new(Store::new(
                crate::backend::buffer_manager::shared_pool::shared_pool_capacity(),
            ))
        })
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl Inner {
    fn attach(&mut self, file: Arc<File>) -> u32 {
        let id = self.next_file_id;
        self.next_file_id += 1;
        self.files.insert(
            id,
            FileEntry {
                file,
                dirty: 0,
                dirty_list: Vec::new(),
                resident: 0,
                unsynced: false,
            },
        );
        id
    }

    /// Drop every frame of `file_id` (dirty ones are discarded: callers
    /// flush first) and forget the file.
    fn detach(&mut self, file_id: u32) {
        if let Some(entry) = self.files.remove(&file_id)
            && entry.resident > 0
        {
            for idx in 0..self.frames.len() {
                let f = &mut self.frames[idx];
                if f.occupied && (f.key >> 32) as u32 == file_id {
                    self.table.remove(&f.key);
                    f.occupied = false;
                    f.is_dirty = false;
                    f.pin_count = 0;
                    f.clock_bit = false;
                    self.free.push(idx);
                }
            }
        }
    }

    fn mark_dirty(&mut self, idx: usize) {
        let f = &mut self.frames[idx];
        if !f.is_dirty {
            f.is_dirty = true;
            if let Some(e) = self.files.get_mut(&((f.key >> 32) as u32)) {
                e.dirty += 1;
                e.dirty_list.push(idx);
            }
        }
    }

    /// Write one dirty frame to its file and mark it clean.
    fn write_frame(&mut self, idx: usize) -> RookResult<()> {
        let f = &mut self.frames[idx];
        let file_id = (f.key >> 32) as u32;
        let page_id = f.key as u32;
        if let Some(e) = self.files.get_mut(&file_id) {
            write_all_at(&e.file, &f.page.data, page_id as u64 * PAGE_SIZE as u64)?;
            e.unsynced = true;
            if f.is_dirty {
                e.dirty = e.dirty.saturating_sub(1);
            }
        }
        f.is_dirty = false;
        Ok(())
    }

    /// Write all dirty frames of a file.
    fn write_dirty(&mut self, file_id: u32) -> RookResult<()> {
        let list = match self.files.get_mut(&file_id) {
            Some(e) if e.dirty > 0 => std::mem::take(&mut e.dirty_list),
            _ => return Ok(()),
        };
        for (i, &idx) in list.iter().enumerate() {
            let f = &self.frames[idx];
            if !(f.occupied && f.is_dirty && (f.key >> 32) as u32 == file_id) {
                continue;
            }
            if let Err(e) = self.write_frame(idx) {
                if let Some(entry) = self.files.get_mut(&file_id) {
                    entry.dirty_list.extend_from_slice(&list[i..]);
                }
                return Err(e);
            }
        }
        Ok(())
    }

    /// Find a frame that can take a new page: a free one, a new one while
    /// under budget, or a CLOCK victim (written back first if dirty).
    /// The returned frame is unoccupied and not in the free list.
    fn acquire_frame(&mut self) -> RookResult<usize> {
        if let Some(idx) = self.free.pop() {
            return Ok(idx);
        }
        if self.frames.len() < self.capacity {
            self.frames.push(Frame {
                key: 0,
                page: Box::new(Page::new()),
                is_dirty: false,
                pin_count: 0,
                clock_bit: false,
                occupied: false,
            });
            return Ok(self.frames.len() - 1);
        }

        let n = self.frames.len();
        for _ in 0..2 * n {
            let idx = self.hand;
            self.hand = (self.hand + 1) % n;
            let f = &mut self.frames[idx];
            if !f.occupied || f.pin_count > 0 {
                continue;
            }
            if f.clock_bit {
                f.clock_bit = false;
                continue;
            }
            if f.is_dirty {
                self.write_frame(idx)?;
            }
            let f = &mut self.frames[idx];
            self.table.remove(&f.key);
            if let Some(e) = self.files.get_mut(&((f.key >> 32) as u32)) {
                e.resident = e.resident.saturating_sub(1);
            }
            f.occupied = false;
            f.clock_bit = false;
            return Ok(idx);
        }
        Err(RookError::io(
            io::ErrorKind::WouldBlock,
            "BufferPool: all frames are pinned — cannot evict",
        ))
    }

    /// Put a filled, acquired frame into the table.
    fn install(&mut self, idx: usize, file_id: u32, page_id: u32, pins: usize) {
        let key = frame_key(file_id, page_id);
        let f = &mut self.frames[idx];
        f.key = key;
        f.is_dirty = false;
        f.pin_count = pins;
        f.clock_bit = true;
        f.occupied = true;
        self.table.insert(key, idx);
        if let Some(e) = self.files.get_mut(&file_id) {
            e.resident += 1;
        }
    }

    fn release_unused(&mut self, idx: usize) {
        self.free.push(idx);
    }

    /// Pin the frame holding `(file_id, page_id)`, loading it on a miss
    /// straight into the buffer of the frame that was evicted for it.
    fn fetch(&mut self, file_id: u32, page_id: u32, total_pages: u32) -> RookResult<usize> {
        if let Some(&idx) = self.table.get(&frame_key(file_id, page_id)) {
            let f = &mut self.frames[idx];
            f.pin_count += 1;
            f.clock_bit = true;
            return Ok(idx);
        }
        let idx = self.acquire_frame()?;
        let res = self.load(idx, file_id, page_id, total_pages);
        match res {
            Ok(()) => {
                self.install(idx, file_id, page_id, 1);
                Ok(idx)
            }
            Err(e) => {
                self.release_unused(idx);
                Err(e)
            }
        }
    }

    fn load(&mut self, idx: usize, file_id: u32, page_id: u32, total_pages: u32) -> RookResult<()> {
        let entry = self.files.get(&file_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "BufferPool has no open file")
        })?;
        let page = &mut self.frames[idx].page;
        let offset = page_id as u64 * PAGE_SIZE as u64;
        let n = read_full_at(&entry.file, &mut page.data, offset)?;
        if n == page.data.len() {
            return Ok(());
        }
        if n > 0 {
            return Err(RookError::io(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        // Past the end of the file. An allocated page that has not reached
        // disk yet reads as an empty page. A page id that was never
        // allocated is a stale pointer (from the FSM or an index): fail
        // instead of inventing a blank page that could later be written
        // past end of file.
        if page_id >= total_pages {
            return Err(RookError::io(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "page {} does not exist (file has {} pages)",
                    page_id, total_pages
                ),
            ));
        }
        // The frame's bytes are what a short read left, or an evicted page.
        page.data.fill(0);
        init_page(page);
        Ok(())
    }

    fn unpin(&mut self, idx: usize, dirty: bool) {
        let f = &mut self.frames[idx];
        if f.pin_count > 0 {
            f.pin_count -= 1;
        }
        if dirty {
            self.mark_dirty(idx);
        }
    }
}

// ─── Guards ──────────────────────────────────────────────────────────────

/// A page's bytes borrowed in place; the frame stays pinned until drop.
pub struct PageRef<'a> {
    store: &'a Store,
    idx: usize,
    ptr: *const u8,
    len: usize,
}

impl Deref for PageRef<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        // SAFETY: the frame is pinned, so it is neither evicted nor reused;
        // its buffer is never reallocated, and the owning view is mutably
        // borrowed for `'a`, so nothing else touches the bytes.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for PageRef<'_> {
    fn drop(&mut self) {
        self.store.lock().unpin(self.idx, false);
    }
}

/// A page's bytes borrowed for editing; marks the page dirty on drop.
pub struct PageMut<'a> {
    store: &'a Store,
    idx: usize,
    ptr: *mut u8,
    len: usize,
}

impl Deref for PageMut<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        // SAFETY: see `PageRef`.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl DerefMut for PageMut<'_> {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: see `PageRef`; the guard is the only accessor.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for PageMut<'_> {
    fn drop(&mut self) {
        self.store.lock().unpin(self.idx, true);
    }
}

/// RAII guard that holds a pinned frame and auto-unpins on drop.
///
/// Obtained from `BufferPool::fetch_page_guarded()`. Provides access to the
/// underlying page and unpins (optionally dirty) when dropped.
pub struct FrameGuard<'a> {
    pool: &'a mut BufferPool,
    frame_id: FrameId,
    /// Set to true if the page was modified; used on drop to mark dirty.
    is_dirty: bool,
}

impl<'a> FrameGuard<'a> {
    pub(crate) fn new(pool: &'a mut BufferPool, frame_id: FrameId) -> Self {
        Self {
            pool,
            frame_id,
            is_dirty: false,
        }
    }

    /// Access the underlying page (read-only).
    pub fn get_page(&self) -> &Page {
        self.pool.get_page(self.frame_id)
    }

    /// Access the underlying page (mutable). Marks the page as dirty on drop.
    pub fn get_page_mut(&mut self) -> &mut Page {
        self.is_dirty = true;
        self.pool.get_page_mut(self.frame_id)
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

impl Drop for FrameGuard<'_> {
    fn drop(&mut self) {
        self.pool.unpin(self.frame_id, self.is_dirty);
    }
}

// ─── Per-file view ───────────────────────────────────────────────────────

/// One file's window onto a [`Store`].
pub struct BufferPool {
    store: Arc<Store>,
    /// Id of the attached file in the store (`None` until a file is attached).
    file_id: Option<u32>,
    file_path: Option<PathBuf>,
    file: Option<Arc<File>>,
    /// Total number of pages in the managed file (from header).
    total_pages: u32,
    /// Whether detaching (and so dropping) fsyncs the file; see
    /// [`BufferPool::set_sync_on_detach`].
    sync_on_detach: bool,
}

fn no_file() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "BufferPool has no open file")
}

impl BufferPool {
    /// Create a view on a private store with its own budget of `capacity`
    /// frames (isolated use and tests). Files used by the engine attach to
    /// the global store via [`BufferPool::with_file`].
    pub fn new(capacity: usize) -> Self {
        Self::on_store(Arc::new(Store::new(capacity)))
    }

    fn on_store(store: Arc<Store>) -> Self {
        Self {
            store,
            file_id: None,
            file_path: None,
            file: None,
            total_pages: 0,
            sync_on_detach: true,
        }
    }

    /// Whether detaching the file fsyncs it after writing its dirty pages.
    /// Off for a file whose owner fsyncs it later, in a batch with others,
    /// or deletes it.
    pub fn set_sync_on_detach(&mut self, on: bool) {
        self.sync_on_detach = on;
    }

    /// A second, empty view on the same store as `self`.
    #[cfg(test)]
    fn sibling(&self) -> Self {
        Self::on_store(self.store.clone())
    }

    fn attach_file(&mut self, file: File, path: PathBuf) {
        self.detach_file();
        let file = Arc::new(file);
        self.file_id = Some(self.store.lock().attach(file.clone()));
        self.file = Some(file);
        self.file_path = Some(path);
    }

    /// Flush and forget the attached file and all its frames.
    fn detach_file(&mut self) {
        if self.file_id.is_some() {
            let written = if self.sync_on_detach {
                self.flush_all()
            } else {
                self.write_dirty()
            };
            if let Err(e) = written {
                log::error!("BufferPool: error flushing on detach: {}", e);
            }
            let id = self.file_id.take().unwrap();
            self.store.lock().detach(id);
        }
        self.file = None;
    }

    /// Attach an already-open heap file to the global store.
    ///
    /// Reads the header page to determine the total number of pages.
    pub fn with_file(mut file: File, file_path: PathBuf) -> RookResult<Self> {
        let mut pool = Self::on_store(Store::global().clone());
        pool.total_pages = read_header_pages(&mut file)?.unwrap_or(0);
        pool.attach_file(file, file_path);
        log::info!(
            "BufferPool attached to global store, file has {} pages",
            pool.total_pages
        );
        Ok(pool)
    }

    /// Attach a raw page file (such as a B+ Tree .idx file) to the global
    /// store; the page count comes from the caller, not a heap header.
    pub fn with_file_raw(file: File, file_path: PathBuf, total_pages: u32) -> Self {
        let mut pool = Self::on_store(Store::global().clone());
        pool.total_pages = total_pages;
        pool.attach_file(file, file_path);
        pool
    }

    /// Open a file for the buffer pool to manage.
    ///
    /// Reads the header page to determine the total number of pages.
    pub fn open(&mut self, path: &Path) -> RookResult<()> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        // File is empty or too small — treat as new
        self.total_pages = read_header_pages(&mut file)?.unwrap_or(0);
        self.attach_file(file, path.to_path_buf());
        Ok(())
    }

    /// Check if the pool has a file open.
    pub fn is_open(&self) -> bool {
        self.file.is_some()
    }

    /// A duplicate of the underlying file handle (shares the file offset).
    /// Used for external scans such as the FSM rebuild.
    pub(crate) fn try_clone_file(&self) -> RookResult<File> {
        self.file
            .as_ref()
            .ok_or_else(no_file)?
            .try_clone()
            .map_err(RookError::from)
    }

    /// Return the total number of pages tracked by this pool.
    pub fn total_pages(&self) -> u32 {
        self.total_pages
    }

    fn id(&self) -> RookResult<u32> {
        self.file_id.ok_or_else(no_file).map_err(RookError::from)
    }

    // ─── Core Page Fetch API ─────────────────────────────────────────────

    /// Fetch a page from the buffer pool, pinning it in memory.
    ///
    /// If the page is not already cached, it will be loaded from disk,
    /// potentially evicting an unpinned page (of any file).
    ///
    /// Returns a `FrameId` that can be used with `get_page()` / `get_page_mut()`.
    /// Call `unpin()` when done to allow eviction.
    pub fn fetch_page(&mut self, page_id: u32) -> RookResult<FrameId> {
        let id = self.id()?;
        let idx = self.store.lock().fetch(id, page_id, self.total_pages)?;
        Ok(FrameId(idx))
    }

    /// Allocate a new page (extend the file) and header count.
    ///
    /// Returns `(page_id, frame_id)` for the newly created page.
    pub fn new_page(&mut self) -> RookResult<(u32, FrameId)> {
        let fid = self.id()?;
        let page_id = self.total_pages;
        let mut g = self.store.lock();

        // Evict first, BEFORE modifying the file.
        let idx = g.acquire_frame()?;
        let res = (|| -> RookResult<()> {
            let file = g.files.get(&fid).ok_or_else(no_file)?.file.clone();
            // The frame may hold an evicted page: start from zeros, as
            // `allocate_raw_page` does, so none of it reaches the new page.
            let page = &mut g.frames[idx].page;
            page.data.fill(0);
            init_page(page);

            // Append to file
            let mut f: &File = &file;
            f.seek(SeekFrom::End(0))?;
            f.write_all(&page.data)?;
            f.flush()?;

            // Update header page count on disk
            let header_buf = (page_id + 1).to_le_bytes();
            f.seek(SeekFrom::Start(0))?;
            f.write_all(&header_buf)?;
            f.flush()?;

            // A resident header page must agree with the file, or flushing
            // it later would write the old count back.
            if let Some(&h) = g.table.get(&frame_key(fid, 0)) {
                g.frames[h].page.data[0..4].copy_from_slice(&header_buf);
            }
            Ok(())
        })();
        if let Err(e) = res {
            g.release_unused(idx);
            return Err(e);
        }
        self.total_pages = page_id + 1;
        // Not dirty — already written to disk.
        g.install(idx, fid, page_id, 1);
        Ok((page_id, FrameId(idx)))
    }

    /// Allocate a new raw page (without initializing heap headers or rewriting page 0).
    /// Used for index files where page 0 is a B+ Tree node.
    pub fn allocate_raw_page(&mut self, data: &[u8]) -> RookResult<u32> {
        let fid = self.id()?;
        let page_id = self.total_pages;
        let mut g = self.store.lock();

        let idx = g.acquire_frame()?;
        let res = (|| -> RookResult<()> {
            let file = g.files.get(&fid).ok_or_else(no_file)?.file.clone();
            let mut f: &File = &file;
            f.seek(SeekFrom::End(0))?;
            f.write_all(data)?;
            f.flush()?;
            let page = &mut g.frames[idx].page;
            page.data.fill(0);
            page.data[..data.len()].copy_from_slice(data);
            Ok(())
        })();
        if let Err(e) = res {
            g.release_unused(idx);
            return Err(e);
        }
        self.total_pages = page_id + 1;
        g.install(idx, fid, page_id, 0);
        Ok(page_id)
    }

    /// Read raw page bytes using the buffer pool cache.
    /// Hits return without any disk I/O!
    pub fn read_page_bytes(&mut self, page_id: u32) -> RookResult<Vec<u8>> {
        Ok(self.page_bytes(page_id)?.to_vec())
    }

    /// Write raw page bytes through the buffer pool (marks page dirty).
    pub fn write_page_bytes(&mut self, page_id: u32, data: &[u8]) -> RookResult<()> {
        self.page_bytes_mut(page_id)?[..data.len()].copy_from_slice(data);
        Ok(())
    }

    /// Borrow a page's bytes in place, loading the page on a miss.
    ///
    /// Nothing is copied out of the frame. The frame stays pinned until the
    /// returned guard is dropped.
    pub fn page_bytes(&mut self, page_id: u32) -> RookResult<PageRef<'_>> {
        let id = self.id()?;
        let mut g = self.store.lock();
        let idx = g.fetch(id, page_id, self.total_pages)?;
        let data = &g.frames[idx].page.data;
        let (ptr, len) = (data.as_ptr(), data.len());
        drop(g);
        Ok(PageRef {
            store: &self.store,
            idx,
            ptr,
            len,
        })
    }

    /// Borrow a page's bytes for editing in place; the page is marked dirty
    /// when the guard drops.
    pub fn page_bytes_mut(&mut self, page_id: u32) -> RookResult<PageMut<'_>> {
        let id = self.id()?;
        let mut g = self.store.lock();
        let idx = g.fetch(id, page_id, self.total_pages)?;
        let data = &mut g.frames[idx].page.data;
        let (ptr, len) = (data.as_mut_ptr(), data.len());
        drop(g);
        Ok(PageMut {
            store: &self.store,
            idx,
            ptr,
            len,
        })
    }

    /// Access a pinned page by frame ID.
    pub fn get_page(&self, frame_id: FrameId) -> &Page {
        let ptr: *const Page = &*self.store.lock().frames[frame_id.0].page;
        // SAFETY: the caller holds a pin from `fetch_page`/`new_page`, so
        // the frame is not evicted; the boxed page never moves.
        unsafe { &*ptr }
    }

    /// Mutably access a pinned page by frame ID.
    pub fn get_page_mut(&mut self, frame_id: FrameId) -> &mut Page {
        let ptr: *mut Page = {
            let mut g = self.store.lock();
            let f = &mut g.frames[frame_id.0];
            f.clock_bit = true;
            &mut *f.page
        };
        // SAFETY: as for `get_page`; `&mut self` makes this view the only
        // accessor of its own frames.
        unsafe { &mut *ptr }
    }

    /// Unpin a frame, optionally marking it as dirty.
    pub fn unpin(&mut self, frame_id: FrameId, is_dirty: bool) {
        self.store.lock().unpin(frame_id.0, is_dirty);
    }

    // ─── Flush API ───────────────────────────────────────────────────────

    /// Flush a specific page to disk if it is dirty.
    pub fn flush_page(&mut self, page_id: u32) -> RookResult<()> {
        let id = self.id()?;
        let mut g = self.store.lock();
        if let Some(&idx) = g.table.get(&frame_key(id, page_id))
            && g.frames[idx].is_dirty
        {
            g.write_frame(idx)?;
        }
        Ok(())
    }

    /// Whether any frame of this file holds unflushed modifications.
    pub fn has_dirty(&self) -> bool {
        self.file_id.is_some_and(|id| {
            self.store
                .lock()
                .files
                .get(&id)
                .is_some_and(|e| e.dirty > 0)
        })
    }

    /// Write all dirty pages of this file without fsync.
    ///
    /// The pages become visible to direct-I/O readers and survive a process
    /// crash; `flush_all` makes them survive a power loss.
    pub fn write_dirty(&mut self) -> RookResult<()> {
        match self.file_id {
            Some(id) => self.store.lock().write_dirty(id),
            None => Ok(()),
        }
    }

    /// Flush all dirty pages to disk and fsync (if anything was written
    /// since the last fsync).
    pub fn flush_all(&mut self) -> RookResult<()> {
        let Some(id) = self.file_id else {
            return Ok(());
        };
        let to_sync = {
            let mut g = self.store.lock();
            g.write_dirty(id)?;
            match g.files.get_mut(&id) {
                Some(e) if e.unsynced => {
                    e.unsynced = false;
                    Some(e.file.clone())
                }
                _ => None,
            }
        };
        if let Some(file) = to_sync {
            let mut f: &File = &file;
            f.flush()?;
            file.sync_all()?;
        }
        Ok(())
    }

    /// Fetch a page and return a `FrameGuard` that auto-unpins on drop.
    pub fn fetch_page_guarded(&mut self, page_id: u32) -> RookResult<FrameGuard<'_>> {
        let frame_id = self.fetch_page(page_id)?;
        Ok(FrameGuard::new(self, frame_id))
    }

    // ─── Statistics ──────────────────────────────────────────────────────

    fn count_frames(&self, pred: impl Fn(&Frame) -> bool) -> usize {
        let Some(id) = self.file_id else { return 0 };
        let g = self.store.lock();
        g.frames
            .iter()
            .filter(|f| f.occupied && (f.key >> 32) as u32 == id && pred(f))
            .count()
    }

    /// Number of frames of this file currently resident.
    pub fn occupied_count(&self) -> usize {
        self.count_frames(|_| true)
    }

    /// Number of this file's frames currently pinned.
    pub fn pinned_count(&self) -> usize {
        self.count_frames(|f| f.pin_count > 0)
    }

    /// Number of this file's dirty frames.
    pub fn dirty_count(&self) -> usize {
        self.count_frames(|f| f.is_dirty)
    }

    /// Frame budget of the store this view belongs to.
    pub fn capacity(&self) -> usize {
        self.store.lock().capacity
    }

    /// Fraction of the store's budget this file occupies.
    pub fn utilization(&self) -> f64 {
        self.occupied_count() as f64 / self.capacity() as f64
    }
}

/// Page count from a heap file's header page, `None` if the file is too
/// small to have one.
fn read_header_pages(file: &mut File) -> RookResult<Option<u32>> {
    let file_size = file.metadata()?.len();
    if file_size < PAGE_SIZE as u64 {
        return Ok(None);
    }
    let mut header = [0u8; 4];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header)?;
    Ok(Some(u32::from_le_bytes(header)))
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        // Flushes, then releases this file's frames to the shared budget.
        self.detach_file();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_path() -> std::path::PathBuf {
        // Parallel tests can read the same clock value, so add a counter.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "rookdb_buffer_pool_test_{}_{}_{}.dat",
            std::process::id(),
            nanos,
            seq
        ))
    }

    fn create_test_file(path: &std::path::Path, num_pages: u32) -> RookResult<()> {
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
    fn test_page_bytes_borrow_the_frame_without_pinning() {
        let path = unique_temp_path();
        create_test_file(&path, 4).expect("create test file");
        let mut pool = BufferPool::new(2);
        pool.open(&path).expect("open pool");

        // A read leaves the page clean and unpinned.
        let lower = pool.page_bytes(1).expect("page bytes")[0..4].to_vec();
        assert_eq!(pool.pinned_count(), 0);
        assert_eq!(pool.dirty_count(), 0);

        // An edit lands in the frame itself and marks it dirty.
        pool.page_bytes_mut(1).expect("page bytes")[100] = 0xAB;
        assert_eq!(pool.pinned_count(), 0);
        assert_eq!(pool.dirty_count(), 1);
        assert_eq!(pool.page_bytes(1).unwrap()[100], 0xAB);
        assert_eq!(pool.page_bytes(1).unwrap()[0..4], lower[..]);

        // Evicting the page writes the edit back; reading it again reloads it.
        pool.page_bytes(2).unwrap();
        pool.page_bytes(3).unwrap();
        pool.page_bytes(2).unwrap();
        assert_eq!(pool.dirty_count(), 0);
        assert_eq!(pool.page_bytes(1).unwrap()[100], 0xAB);

        // A page that was never allocated is still an error.
        assert!(pool.page_bytes(9).is_err());
        assert!(pool.page_bytes_mut(9).is_err());

        drop(pool);
        fs::remove_file(&path).ok();
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
    fn test_new_page_does_not_inherit_an_evicted_page() {
        let path = unique_temp_path();
        create_test_file(&path, 2).expect("create test file");
        // One frame, so the new page reuses the frame of page 1.
        let mut pool = BufferPool::new(1);
        pool.open(&path).expect("open pool");
        let f = pool.fetch_page(1).expect("fetch page 1");
        pool.get_page_mut(f).data[100..108].copy_from_slice(b"leftover");
        pool.unpin(f, true);

        let (page_id, frame_id) = pool.new_page().expect("new page");
        assert!(pool.get_page(frame_id).data[8..].iter().all(|&b| b == 0));
        pool.unpin(frame_id, false);
        pool.flush_all().expect("flush");
        let on_disk = fs::read(&path).unwrap();
        let start = page_id as usize * PAGE_SIZE;
        assert!(
            on_disk[start + 8..start + PAGE_SIZE]
                .iter()
                .all(|&b| b == 0)
        );

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

    #[test]
    fn test_budget_is_shared_between_files() {
        let (pa, pb) = (unique_temp_path(), unique_temp_path());
        create_test_file(&pa, 8).unwrap();
        create_test_file(&pb, 8).unwrap();
        let mut a = BufferPool::new(4);
        a.open(&pa).unwrap();
        let mut b = a.sibling();
        b.open(&pb).unwrap();

        // File A dirties two pages and fills three of the four frames.
        a.page_bytes_mut(1).unwrap()[100] = 0xA1;
        a.page_bytes_mut(2).unwrap()[100] = 0xA2;
        a.page_bytes(3).unwrap();
        assert_eq!(a.occupied_count() + b.occupied_count(), 3);

        // File B reads five pages: the total never exceeds the budget and
        // A's dirty pages are written back to A's file when evicted.
        for page in 1..6 {
            b.page_bytes(page).unwrap();
            assert!(a.occupied_count() + b.occupied_count() <= 4);
        }
        assert_eq!(a.dirty_count(), 0);
        assert!(!a.has_dirty());
        assert_eq!(a.page_bytes(1).unwrap()[100], 0xA1);
        assert_eq!(a.page_bytes(2).unwrap()[100], 0xA2);

        // Frames of the same page id in two files do not collide.
        assert_ne!(a.page_bytes(1).unwrap()[100], b.page_bytes(1).unwrap()[100]);

        drop(a);
        drop(b);
        fs::remove_file(&pa).ok();
        fs::remove_file(&pb).ok();
    }

    #[test]
    fn test_drop_releases_frames_and_new_identity_sees_no_stale_pages() {
        let path = unique_temp_path();
        create_test_file(&path, 4).unwrap();
        let mut a = BufferPool::new(4);
        a.open(&path).unwrap();
        let mut b = a.sibling();
        a.page_bytes_mut(1).unwrap()[200] = 0x77;
        a.flush_all().unwrap();
        drop(a);

        // Dropping the view released its frames to the budget.
        {
            let g = b.store.lock();
            assert!(g.table.is_empty());
        }

        // Replace the file on disk; a view attached afterwards reads the
        // new contents, never a frame cached for the old file.
        fs::remove_file(&path).unwrap();
        create_test_file(&path, 4).unwrap();
        b.open(&path).unwrap();
        assert_eq!(b.page_bytes(1).unwrap()[200], 0);
        drop(b);
        fs::remove_file(&path).ok();
    }

    #[test]
    fn test_pinned_guard_blocks_eviction_across_files() {
        let (pa, pb) = (unique_temp_path(), unique_temp_path());
        create_test_file(&pa, 4).unwrap();
        create_test_file(&pb, 4).unwrap();
        let mut a = BufferPool::new(1);
        a.open(&pa).unwrap();
        let mut b = a.sibling();
        b.open(&pb).unwrap();
        let held = a.fetch_page(1).unwrap();
        // The only frame is pinned by file A: B cannot get one.
        assert!(b.page_bytes(1).is_err());
        a.unpin(held, false);
        assert!(b.page_bytes(1).is_ok());
        drop(a);
        drop(b);
        fs::remove_file(&pa).ok();
        fs::remove_file(&pb).ok();
    }

    #[test]
    fn test_fx_hasher_distinguishes_keys() {
        use std::hash::BuildHasher;
        let bh = BuildHasherDefault::<FxHasher>::default();
        let hashes: std::collections::HashSet<u64> = (0..10_000u64)
            .map(|i| bh.hash_one(frame_key((i % 7) as u32, (i / 7) as u32)))
            .collect();
        assert_eq!(hashes.len(), 10_000);
    }
}
