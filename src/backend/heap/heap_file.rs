//! Page-level access to a heap file through its shared buffer pool.
//!
//! UPDATE, DELETE, foreign-key actions and compaction work on whole page
//! images: read a page, change slots, write it back. `HeapFile` gives them
//! that interface over the same pool that inserts and scans use, so every
//! reader and writer of a table sees one copy of each page.

use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::backend::buffer_manager::shared_pool::SharedPool;
use crate::backend::heap::types::HeaderMetadata;
use crate::page::Page;

pub struct HeapFile {
    pool: SharedPool,
    identity: u64,
}

impl HeapFile {
    /// Open the heap file at `path`.
    ///
    /// The pool is taken from the cached `HeapManager`, which keeps it
    /// resident between statements and has it written out at checkpoints.
    pub fn open(engine: &crate::backend::engine::Engine, path: &Path) -> io::Result<Self> {
        let pool = crate::backend::cache::with_heap(engine, path, |hm| Ok(Arc::clone(&hm.pool)))?;
        let identity = {
            let guard = pool.lock().unwrap_or_else(|p| p.into_inner());
            let file = guard.file.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "BufferPool has no file")
            })?;
            crate::backend::disk::file_identity_from_file(file)?
        };
        Ok(Self { pool, identity })
    }

    /// Identity of the underlying file, for `PageWriteLock`.
    pub fn identity(&self) -> u64 {
        self.identity
    }

    /// Number of pages in the file, header page included.
    pub fn page_count(&self) -> u32 {
        self.lock().total_pages()
    }

    /// Copy page `page_num` into `page`.
    pub fn read_page(&self, page: &mut Page, page_num: u32) -> io::Result<()> {
        let mut pool = self.lock();
        let frame_id = pool.fetch_page(page_num)?;
        page.data.copy_from_slice(&pool.get_page(frame_id).data);
        pool.unpin(frame_id, false);
        Ok(())
    }

    /// Replace page `page_num` with `page`.
    pub fn write_page(&self, page: &Page, page_num: u32) -> io::Result<()> {
        let mut pool = self.lock();
        let frame_id = pool.fetch_page(page_num)?;
        pool.get_page_mut(frame_id).data.copy_from_slice(&page.data);
        pool.unpin(frame_id, true);
        Ok(())
    }

    /// Read the file header.
    pub fn header(&self) -> io::Result<HeaderMetadata> {
        let mut pool = self.lock();
        let frame_id = pool.fetch_page(0)?;
        let header = HeaderMetadata::deserialize(&pool.get_page(frame_id).data[..24]);
        pool.unpin(frame_id, false);
        header
    }

    /// Record that `count` live rows were soft-deleted.
    pub fn record_dead_tuples(&self, count: u32) -> io::Result<()> {
        self.update_header(|h| {
            h.dead_tuple_count = h.dead_tuple_count.saturating_add(count);
            h.total_tuples = h.total_tuples.saturating_sub(count as u64);
        })
    }

    /// Reset the dead-tuple counter after compaction removed them.
    pub fn reset_dead_tuples(&self) -> io::Result<()> {
        self.update_header(|h| h.dead_tuple_count = 0)
    }

    fn update_header(&self, f: impl FnOnce(&mut HeaderMetadata)) -> io::Result<()> {
        let mut pool = self.lock();
        let frame_id = pool.fetch_page(0)?;
        let updated = HeaderMetadata::deserialize(&pool.get_page(frame_id).data[..24]).and_then(
            |mut header| {
                f(&mut header);
                header.serialize()
            },
        );
        match updated {
            Ok(bytes) => {
                pool.get_page_mut(frame_id).data[..24].copy_from_slice(&bytes);
                pool.unpin(frame_id, true);
                Ok(())
            }
            Err(e) => {
                pool.unpin(frame_id, false);
                Err(e)
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, crate::backend::buffer_manager::BufferPool> {
        self.pool.lock().unwrap_or_else(|p| p.into_inner())
    }
}
