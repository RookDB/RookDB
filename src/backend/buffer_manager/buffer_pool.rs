use std::collections::HashMap;
use std::fs::{self, File};
use std::io;
use std::path::Path;

use super::frame::{BufferFrame, FrameMetadata, PageId};
use super::policy::ReplacementPolicy;
use super::stats::BufferStats;

use crate::backend::page::Page;
use crate::disk::{read_page, write_page, create_page};
use crate::table::page_count;
use crate::buffer_manager::{PAGE_SIZE, BUFFER_SIZE, RESERVED_FRAMES};

use crate::backend::layout::{
    PG_DATABASE_FILE,
    PG_TABLE_FILE,
    PG_COLUMN_FILE,
    PG_CONSTRAINT_FILE,
    PG_INDEX_FILE,
    PG_TYPE_FILE,
};

pub struct BufferPool {
    pub frames: Vec<BufferFrame>,
    pub page_table: HashMap<PageId, usize>,
    pub files: HashMap<String, File>, //  MULTI-FILE SUPPORT
    pub num_frames: usize,
    pub policy: Box<dyn ReplacementPolicy>,
    pub stats: BufferStats,
}

impl BufferPool {

    pub fn new(policy: Box<dyn ReplacementPolicy>) -> Self {
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
            files: HashMap::new(),
            num_frames,
            policy,
            stats: BufferStats::new(),
        }
    }

    // =========================================================
    // FETCH PAGE
    // =========================================================
    pub fn fetch_page(
        &mut self,
        table_name: String,
        page_number: u32,
    ) -> io::Result<&mut Page> {

        let page_id = PageId {
            table_name: table_name.clone(),
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

                    _ => {
                        let mut candidate = None;

                        for i in RESERVED_FRAMES..self.num_frames {
                            if self.frames[i].metadata.pin_count == 0 {
                                candidate = Some(i);
                                break;
                            }
                        }

                        candidate.ok_or_else(|| {
                            io::Error::new(io::ErrorKind::Other, "All frames are pinned")
                        })?
                    }
                };

                let victim_frame = &mut self.frames[victim_index];

                // Flush if dirty
                if victim_frame.metadata.dirty {
                    let victim_page_id = victim_frame.metadata.page_id.as_ref().unwrap();

                    let file = self.files.get_mut(&victim_page_id.table_name)
                        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

                    write_page(
                        file,
                        &mut victim_frame.page,
                        victim_page_id.page_number,
                    )?;

                    self.stats.record_dirty_flush();
                }

                // Remove mapping
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

        let file = self.files.get_mut(&table_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

        read_page(file, &mut frame.page, page_number)?;

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

    // =========================================================
    // UNPIN
    // =========================================================
    pub fn unpin_page(
        &mut self,
        page_id: &PageId,
        is_dirty: bool,
    ) -> io::Result<()> {

        let frame_index = *self.page_table.get(page_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Page not found"))?;

        let frame = &mut self.frames[frame_index];

        if frame.metadata.pin_count == 0 {
            return Err(io::Error::new(io::ErrorKind::Other, "Already unpinned"));
        }

        frame.metadata.pin_count -= 1;

        if is_dirty {
            frame.metadata.dirty = true;
        }

        Ok(())
    }

    // =========================================================
    // FLUSH PAGE
    // =========================================================
    pub fn flush_page(&mut self, page_id: &PageId) -> io::Result<()> {

        let frame_index = *self.page_table.get(page_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Page not found"))?;

        let frame = &mut self.frames[frame_index];

        if !frame.metadata.dirty {
            return Ok(());
        }

        let file = self.files.get_mut(&page_id.table_name).unwrap();

        write_page(file, &mut frame.page, page_id.page_number)?;

        frame.metadata.dirty = false;
        self.stats.record_dirty_flush();

        Ok(())
    }

    // =========================================================
    // FLUSH ALL
    // =========================================================
    pub fn flush_all_pages(&mut self) -> io::Result<()> {

        for frame in &mut self.frames {
            if frame.metadata.dirty {
                if let Some(page_id) = &frame.metadata.page_id {

                    let file = self.files.get_mut(&page_id.table_name).unwrap();

                    write_page(file, &mut frame.page, page_id.page_number)?;

                    frame.metadata.dirty = false;
                    self.stats.record_dirty_flush();
                }
            }
        }

        Ok(())
    }

    // =========================================================
    // NEW PAGE
    // =========================================================
    pub fn new_page(
        &mut self,
        table_name: String,
    ) -> io::Result<(PageId, &mut Page)> {

        let file = self.files.get_mut(&table_name)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "File not found"))?;

        let new_page_number = create_page(file)?;

        let page_id = PageId {
            table_name: table_name.clone(),
            page_number: new_page_number,
        };

        self.fetch_page(table_name, new_page_number)?;

        let frame_index = *self.page_table.get(&page_id).unwrap();

        let frame = &mut self.frames[frame_index];
        frame.metadata.dirty = true;

        Ok((page_id, &mut frame.page))
    }

    // =========================================================
    // RESET
    // =========================================================
    pub fn reset(&mut self) {
        for frame in &mut self.frames {
            frame.metadata = FrameMetadata::new();
        }

        self.page_table.clear();
        self.files.clear();
        self.stats = BufferStats::new();
    }

    // =========================================================
    // PRELOAD DATABASE (MULTI-FILE LOADING)
    // =========================================================
    pub fn preload_database(&mut self, db_name: &str) -> io::Result<()> {

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

            let total_pages = page_count(&mut file)?;

            // Store file handle
            self.files.insert(table_name.clone(), file);

            let file = self.files.get_mut(&table_name).unwrap();

            for page_number in 1..total_pages {

                if frame_index >= self.num_frames {
                    return Ok(()); // buffer full
                }

                let page_id = PageId {
                    table_name: table_name.clone(),
                    page_number,
                };

                let frame = &mut self.frames[frame_index];

                read_page(file, &mut frame.page, page_number)?;

                frame.metadata.page_id = Some(page_id.clone());
                frame.metadata.pin_count = 0;
                frame.metadata.dirty = false;
                frame.metadata.usage_count = 1;

                self.page_table.insert(page_id, frame_index);

                self.policy.record_access(frame_index - RESERVED_FRAMES);

                frame_index += 1;
            }
        }

        Ok(())
    }


    pub fn preload_catalog_pages(&mut self) -> io::Result<()> {

    use std::fs::File;

    let catalog_files = vec![
        ("pg_database", PG_DATABASE_FILE),
        ("pg_table", PG_TABLE_FILE),
        ("pg_column", PG_COLUMN_FILE),
        ("pg_constraint", PG_CONSTRAINT_FILE),
        ("pg_index", PG_INDEX_FILE),
        ("pg_type", PG_TYPE_FILE),
    ];

    let mut frame_index = 0;

    for (table_name, path) in catalog_files {

        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => continue, // skip if file doesn't exist
        };

        let total_pages = page_count(&mut file)?;

        // register file in buffer pool
        self.files.insert(table_name.to_string(), file);

        let file = self.files.get_mut(table_name).unwrap();

        // load only first 2 pages (0 and 1)
        let pages_to_load = std::cmp::min(2, total_pages);

        for page_number in 0..pages_to_load {

            if frame_index >= RESERVED_FRAMES {
                return Ok(()); // no more reserved space
            }

            let page_id = PageId {
                table_name: table_name.to_string(),
                page_number,
            };

            let frame = &mut self.frames[frame_index];

            read_page(file, &mut frame.page, page_number)?;

            frame.metadata.page_id = Some(page_id.clone());
            frame.metadata.pin_count = 0;
            frame.metadata.dirty = false;
            frame.metadata.usage_count = 1;

            self.page_table.insert(page_id, frame_index);

            frame_index += 1;
        }
    }

    Ok(())
}

}

