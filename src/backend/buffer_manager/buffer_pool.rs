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
    pub num_frames: usize,
    pub policy: Box<dyn ReplacementPolicy>,
    pub stats: BufferStats,
    pub file: File,
}

impl BufferPool {

    pub fn new(
    policy: Box<dyn ReplacementPolicy>,
    file: File,
) -> Self {
    let num_frames = BUFFER_SIZE / PAGE_SIZE;

    assert!(
        num_frames > RESERVED_FRAMES,
        "Buffer too small for reserved frames"
    );

    let mut frames = Vec::with_capacity(num_frames);

    for _ in 0..num_frames {
        frames.push(BufferFrame {
            page: Page::new(),
            metadata: FrameMetadata::new(),
        });
    }

    Self {
        frames,
        page_table: HashMap::new(),
        num_frames,
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

            if frame_index >= RESERVED_FRAMES {
    self.policy.record_access(frame_index - RESERVED_FRAMES);
}
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

        for i in RESERVED_FRAMES..self.num_frames {
    if self.frames[i].metadata.page_id.is_none() {
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

let victim_index = match victim {
    Some(idx) if idx >= RESERVED_FRAMES => idx,

    Some(_) => {
        // Policy picked from reserved region → ignore and fallback
        let mut candidate = None;

        for i in RESERVED_FRAMES..self.num_frames {
            let frame = &self.frames[i];

            if frame.metadata.pin_count == 0 {
                candidate = Some(i);
                break;
            }
        }

        match candidate {
            Some(i) => i,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "All frames are pinned",
                ));
            }
        }
    }

    None => {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "All frames are pinned",
        ));
    }
};

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

        if frame_index >= RESERVED_FRAMES {
    self.policy.record_access(frame_index - RESERVED_FRAMES);
}
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

    pub fn reset(&mut self) {
    for frame in &mut self.frames {
        frame.metadata = FrameMetadata::new();
    }

    self.page_table.clear();
    self.stats = BufferStats::new();
}

use std::fs;
use std::path::Path;

pub fn preload_database(
    &mut self,
    db_name: &str,
) -> io::Result<()> {

    self.flush_all_pages()?;
    self.reset();

    let base_path = format!(
        "/home/pratham-omkar-pattanayak/SEM 8/Data Systems/Project/RookDB/database/base/{}",
        db_name
    );

    let mut frame_index = RESERVED_FRAMES;

    for entry in fs::read_dir(base_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) != Some("dat") {
            continue;
        }

        let table_name = path.file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let mut file = File::open(&path)?;

        let total_pages = crate::disk::page_count(&mut file)?; // existing API

        // Skip page 0 (header), start from page 1
        for page_number in 1..total_pages {

            if frame_index >= self.num_frames {
                return Ok(()); // buffer full
            }

            let page_id = PageId {
                table_name: table_name.clone(),
                page_number,
            };

            let frame = &mut self.frames[frame_index];

            read_page(&mut file, &mut frame.page, page_number)?;

            frame.metadata.page_id = Some(page_id.clone());
            frame.metadata.pin_count = 0;
            frame.metadata.dirty = false;
            frame.metadata.usage_count = 1;

            self.page_table.insert(page_id, frame_index);

            // optional: register with policy
            self.policy.record_access(frame_index - RESERVED_FRAMES);

            frame_index += 1;
        }
    }

    Ok(())
}
}