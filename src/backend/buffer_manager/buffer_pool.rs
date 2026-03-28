use std::collections::HashMap;
use std::fs::File;
use std::io;

use super::frame::{BufferFrame, FrameMetadata, PageId};
use super::policy::ReplacementPolicy;
use super::stats::BufferStats;

use crate::backend::page::Page;
use crate::disk::{read_page, write_page, create_page};

pub struct BufferPool {
    pub frames: Vec<BufferFrame>,
    pub page_table: HashMap<PageId, usize>,
    pub pool_size: usize,
    pub policy: Box<dyn ReplacementPolicy>,
    pub stats: BufferStats,
    pub file: File,
}

impl BufferPool {

    pub fn new(
        pool_size: usize,
        policy: Box<dyn ReplacementPolicy>,
        file: File,
    ) -> Self {

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
            file,
        }
    }

    pub fn fetch_page(
        &mut self,
        table_name: String,
        page_number: u32,
    ) -> io::Result<&mut Page> {

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
            frame.metadata.usage_count += 1;

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
                let victim = self.policy.victim(&mut self.frames);

                if victim.is_none() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "All frames are pinned",
                    ));
                }

                let victim_index = victim.unwrap();
                let victim_frame = &mut self.frames[victim_index];

                // Flush dirty page
                if victim_frame.metadata.dirty {
                    let victim_page_id = victim_frame.metadata.page_id.as_ref().unwrap();

                    write_page(
                        &mut self.file,
                        &mut victim_frame.page,
                        victim_page_id.page_number,
                    )?;

                    self.stats.record_dirty_flush();
                }

                // Remove old mapping
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

        read_page(&mut self.file, &mut frame.page, page_number)?;

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
    ) -> io::Result<()> {

        let frame_index = match self.page_table.get(page_id) {
            Some(&idx) => idx,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Page not found in buffer",
                ))
            }
        };

        let frame = &mut self.frames[frame_index];

        if frame.metadata.pin_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Page already unpinned",
            ));
        }

        frame.metadata.pin_count -= 1;

        if is_dirty {
            frame.metadata.dirty = true;
        }

        Ok(())
    }

    pub fn flush_page(&mut self, page_id: &PageId) -> io::Result<()> {

        let frame_index = match self.page_table.get(page_id) {
            Some(&idx) => idx,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Page not found",
                ))
            }
        };

        let frame = &mut self.frames[frame_index];

        if !frame.metadata.dirty {
            return Ok(());
        }

        write_page(
            &mut self.file,
            &mut frame.page,
            page_id.page_number,
        )?;

        frame.metadata.dirty = false;
        self.stats.record_dirty_flush();

        Ok(())
    }

    pub fn flush_all_pages(&mut self) -> io::Result<()> {

        for frame in &mut self.frames {

            if frame.metadata.dirty {

                if let Some(page_id) = &frame.metadata.page_id {
                    write_page(
                        &mut self.file,
                        &mut frame.page,
                        page_id.page_number,
                    )?;

                    frame.metadata.dirty = false;
                    self.stats.record_dirty_flush();
                }
            }
        }

        Ok(())
    }

    pub fn new_page(
    &mut self,
    table_name: String,
) -> io::Result<(PageId, &mut Page)> {

    // 1. Create page on disk
    let new_page_number = create_page(&mut self.file)?;

    let page_id = PageId {
        table_name: table_name.clone(),
        page_number: new_page_number,
    };

    // 2. Fetch page (this inserts into page_table)
    self.fetch_page(table_name, new_page_number)?;

    // 3. Now safely get frame index (no borrow active now)
    let frame_index = *self
        .page_table
        .get(&page_id)
        .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Page not found after fetch"))?;

    // 4. Mark dirty
    let frame = &mut self.frames[frame_index];
    frame.metadata.dirty = true;

    // 5. Return page reference
    Ok((page_id, &mut frame.page))
}
    pub fn delete_page(&mut self, page_id: &PageId) -> io::Result<()> {

        if let Some(&frame_index) = self.page_table.get(page_id) {

            let frame = &mut self.frames[frame_index];

            if frame.metadata.pin_count > 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Cannot delete pinned page",
                ));
            }

            self.page_table.remove(page_id);

            frame.metadata.page_id = None;
            frame.metadata.dirty = false;
            frame.metadata.pin_count = 0;
            frame.metadata.usage_count = 0;
            frame.metadata.last_used = 0;
        }

        // TODO: delete from disk if needed

        Ok(())
    }
}