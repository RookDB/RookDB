use std::collections::HashMap;

use super::frame::{BufferFrame, FrameMetadata, PageId};
use super::policy::ReplacementPolicy;
use super::stats::BufferStats;

use crate::backend::page::Page;

pub struct BufferPool {
    pub frames: Vec<BufferFrame>,
    pub page_table: HashMap<PageId, usize>,
    pub pool_size: usize,
    pub policy: Box<dyn ReplacementPolicy>,
    pub stats: BufferStats,
}

impl BufferPool {

    pub fn new(pool_size: usize, policy: Box<dyn ReplacementPolicy>) -> Self {

        let mut frames = Vec::with_capacity(pool_size);

        for _ in 0..pool_size {
            frames.push(BufferFrame {
                page: Page::new(),
                metadata: FrameMetadata::new(),
            });
        }

        Self {
            frames,
            page_table: HashMap::new(),
            pool_size,
            policy,
            stats: BufferStats::new(),
        }
    }

    pub fn fetch_page(
        &mut self,
        table_name: String,
        page_number: u32,
    ) -> Result<&mut Page, String> {

        let page_id = PageId {
            table_name,
            page_number,
        };

        // -----------------------------
        // 1. BUFFER HIT
        // -----------------------------
        if let Some(&frame_index) = self.page_table.get(&page_id) {

            let frame = &mut self.frames[frame_index];

            frame.metadata.pin_count += 1;

            frame.metadata.usage_count = 1;

            self.policy.record_access(frame_index);

            self.stats.record_hit();

            return Ok(&mut frame.page);
        }

        // -----------------------------
        // 2. BUFFER MISS
        // -----------------------------
        self.stats.record_miss();

        // -----------------------------
        // 3. FIND FREE FRAME
        // -----------------------------
        let mut frame_index = None;

        for (i, frame) in self.frames.iter().enumerate() {
            if frame.metadata.page_id.is_none() {
                frame_index = Some(i);
                break;
            }
        }

        let frame_index = match frame_index {

            Some(index) => index,

            None => {

                // -----------------------------
                // 4. EVICTION
                // -----------------------------
                let victim = self.policy.victim(&self.frames);

                if victim.is_none() {
                    return Err("All frames are pinned".to_string());
                }

                let victim_index = victim.unwrap();

                let victim_frame = &mut self.frames[victim_index];

                // Flush dirty page
                if victim_frame.metadata.dirty {

                    // Disk write here (to be implemented with write_page API)
                    self.stats.record_dirty_flush();
                }

                // Remove old page mapping
                if let Some(old_page) = &victim_frame.metadata.page_id {
                    self.page_table.remove(old_page);
                }

                self.stats.record_eviction();

                victim_index
            }
        };

        // -----------------------------
        // 5. LOAD PAGE FROM DISK
        // -----------------------------
        let frame = &mut self.frames[frame_index];

        // Disk read here (to be implemented with read_page API)

        frame.metadata.page_id = Some(page_id.clone());
        frame.metadata.pin_count = 1;
        frame.metadata.dirty = false;
        frame.metadata.usage_count = 1;

        self.policy.record_access(frame_index);

        self.page_table.insert(page_id, frame_index);

        Ok(&mut frame.page)
    }

    pub fn unpin_page(
        &mut self,
        page_id: &PageId,
        is_dirty: bool,
    ) -> Result<(), String> {

        // -----------------------------
        // 1. Find frame
        // -----------------------------
        let frame_index = match self.page_table.get(page_id) {
            Some(&idx) => idx,
            None => return Err("InvalidPageError: Page not found in buffer".to_string()),
        };

        let frame = &mut self.frames[frame_index];

        // -----------------------------
        // 2. Validate pin count
        // -----------------------------
        if frame.metadata.pin_count == 0 {
            return Err("DuplicateUnpinError: Page already unpinned".to_string());
        }

        // -----------------------------
        // 3. Decrement pin count
        // -----------------------------
        frame.metadata.pin_count -= 1;

        // -----------------------------
        // 4. Mark dirty if needed
        // -----------------------------
        if is_dirty {
            frame.metadata.dirty = true;
        }

        Ok(())
    }

    pub fn flush_page(&mut self, page_id: &PageId) -> Result<(), String> {

        // -----------------------------
        // 1. Locate frame
        // -----------------------------
        let frame_index = match self.page_table.get(page_id) {
            Some(&idx) => idx,
            None => return Err("InvalidPageError: Page not found".to_string()),
        };

        let frame = &mut self.frames[frame_index];

        // -----------------------------
        // 2. Check dirty flag
        // -----------------------------
        if !frame.metadata.dirty {
            return Ok(());
        }

        // -----------------------------
        // 3. Write page to disk
        // -----------------------------
        // TODO: call write_page() API here

        frame.metadata.dirty = false;

        self.stats.record_dirty_flush();

        Ok(())
    }

    pub fn flush_all_pages(&mut self) -> Result<(), String> {

        for frame in &mut self.frames {

            if frame.metadata.dirty {

                // TODO: call write_page() API here

                frame.metadata.dirty = false;

                self.stats.record_dirty_flush();
            }
        }

        Ok(())
    }

    pub fn new_page(
        &mut self,
        table_name: String,
    ) -> Result<(PageId, &mut Page), String> {

        // --------------------------------
        // 1. Create page on disk
        // --------------------------------
        // TODO: integrate create_page() API

        // For now we assume the next page number is returned
        let new_page_number: u32 = 0; // placeholder

        // --------------------------------
        // 2. Fetch page into buffer
        // --------------------------------
        let page = self.fetch_page(table_name.clone(), new_page_number)?;

        // --------------------------------
        // 3. Mark page dirty
        // --------------------------------
        let page_id = PageId {
            table_name,
            page_number: new_page_number,
        };

        if let Some(&frame_index) = self.page_table.get(&page_id) {
            self.frames[frame_index].metadata.dirty = true;
        }

        // --------------------------------
        // 4. Return PageId and Page
        // --------------------------------
        Ok((page_id, page))
    }

    pub fn delete_page(&mut self, page_id: &PageId) -> Result<(), String> {

        // --------------------------------
        // 1. Check if page exists in buffer
        // --------------------------------
        if let Some(&frame_index) = self.page_table.get(page_id) {

            let frame = &mut self.frames[frame_index];

            // --------------------------------
            // 2. Check if pinned
            // --------------------------------
            if frame.metadata.pin_count > 0 {
                return Err("PagePinnedError: Cannot delete pinned page".to_string());
            }

            // --------------------------------
            // 3. Remove from page table
            // --------------------------------
            self.page_table.remove(page_id);

            // --------------------------------
            // 4. Reset frame metadata
            // --------------------------------
            frame.metadata.page_id = None;
            frame.metadata.dirty = false;
            frame.metadata.pin_count = 0;
            frame.metadata.usage_count = 0;
            frame.metadata.last_used = 0;
        }

        // --------------------------------
        // 5. Delete page from disk
        // --------------------------------
        // TODO: integrate disk delete logic

        Ok(())
    }
    
}