//! Page buffer manager with pin/unpin semantics and dirty-page tracking.
//!
//! Internal general-purpose LRU multi-file buffer pool with pin/unpin, used by
//! the catalog page manager and scan operators.

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::disk::{read_page, write_page};
use crate::page::Page;

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
        println!("Buffer Manager initialized.");
        Self {
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


}
