//! What the planner knows about a table: sizes from the heap file and, per
//! column, the number of distinct values, NULLs and the value range.
//!
//! [`collect_heap_statistics`] reads every page once; with the table's
//! columns it also gathers [`ColumnStatistics`] ([`column`]). The
//! [`estimator`] turns them into row estimates. Where they are kept and
//! when they are refreshed is `cache::table_statistics`.

pub mod column;
pub mod estimator;

use std::io::{self};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::backend::heap::HeapFile;
use crate::catalog::types::Column;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE, Page, page_free_space};

use column::ColumnCollector;
pub use column::ColumnStatistics;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageStatistics {
    pub page_id: u32,
    pub tuple_count: u32,
    pub tuple_bytes: u64,
    pub slot_bytes: u64,
    pub header_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub min_tuple_bytes: u32,
    pub max_tuple_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TableStatistics {
    pub total_pages: u32,
    pub data_pages: u32,
    pub file_size_bytes: u64,
    pub total_tuple_count: u64,
    pub total_tuple_bytes: u64,
    pub total_slot_bytes: u64,
    pub total_header_bytes: u64,
    pub total_free_bytes: u64,
    pub pages_with_tuples: u32,
    pub min_page_free_bytes: u32,
    pub max_page_free_bytes: u32,
    pub min_tuple_bytes: u32,
    pub max_tuple_bytes: u32,
    /// Not stored with the statistics: a table's pages are numerous and only
    /// reports read them.
    #[serde(skip)]
    pub page_breakdown: Vec<PageStatistics>,
    /// Per-column statistics, empty when the pass did not know the columns.
    pub columns: Vec<ColumnStatistics>,
    /// The live-row count the heap header held when these were gathered. A
    /// header that has moved since means rows came or went.
    pub header_tuples: u64,
}

impl TableStatistics {
    /// The statistics of the column called `name`, if gathered.
    pub fn column(&self, name: &str) -> Option<&ColumnStatistics> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn data_capacity_bytes(&self) -> u64 {
        self.data_pages as u64 * PAGE_SIZE as u64
    }

    pub fn used_bytes(&self) -> u64 {
        self.data_capacity_bytes()
            .saturating_sub(self.total_free_bytes)
    }

    pub fn fill_percent(&self) -> f64 {
        let capacity = self.data_capacity_bytes();
        if capacity == 0 {
            0.0
        } else {
            (self.used_bytes() as f64 / capacity as f64) * 100.0
        }
    }

    pub fn avg_tuple_bytes(&self) -> f64 {
        if self.total_tuple_count == 0 {
            0.0
        } else {
            self.total_tuple_bytes as f64 / self.total_tuple_count as f64
        }
    }

    pub fn avg_tuples_per_page(&self) -> f64 {
        if self.data_pages == 0 {
            0.0
        } else {
            self.total_tuple_count as f64 / self.data_pages as f64
        }
    }

    pub fn avg_page_free_bytes(&self) -> f64 {
        if self.data_pages == 0 {
            0.0
        } else {
            self.total_free_bytes as f64 / self.data_pages as f64
        }
    }
}

pub fn collect_table_statistics(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
) -> io::Result<TableStatistics> {
    let table_path = engine.paths().heap(db_name, table_name);
    collect_heap_statistics(engine, Path::new(&table_path))
}

/// Statistics for the heap file at `path`, read through its buffer pool.
/// Sizes only; see [`collect_heap_statistics_for`] for column statistics.
pub fn collect_heap_statistics(
    engine: &crate::backend::engine::Engine,
    path: &Path,
) -> io::Result<TableStatistics> {
    collect_heap_statistics_for(engine, path, &[])
}

/// [`collect_heap_statistics`], also gathering the statistics of `columns`
/// (the table's columns, in order) in the same pass.
pub fn collect_heap_statistics_for(
    engine: &crate::backend::engine::Engine,
    path: &Path,
    columns: &[Column],
) -> io::Result<TableStatistics> {
    let file = HeapFile::open(engine, path)?;
    let mut collector = (!columns.is_empty()).then(|| ColumnCollector::new(columns));
    let total_pages = file.page_count();
    let file_size_bytes = total_pages as u64 * PAGE_SIZE as u64;
    let data_pages = total_pages.saturating_sub(1);

    let mut stats = TableStatistics {
        total_pages,
        data_pages,
        file_size_bytes,
        total_tuple_count: 0,
        total_tuple_bytes: 0,
        total_slot_bytes: 0,
        total_header_bytes: data_pages as u64 * PAGE_HEADER_SIZE as u64,
        total_free_bytes: 0,
        pages_with_tuples: 0,
        min_page_free_bytes: u32::MAX,
        max_page_free_bytes: 0,
        min_tuple_bytes: u32::MAX,
        max_tuple_bytes: 0,
        page_breakdown: Vec::new(),
        columns: Vec::new(),
        header_tuples: file.header().map(|h| h.total_tuples).unwrap_or(0),
    };

    for page_num in 1..total_pages {
        let mut page = Page::new();
        file.read_page(&mut page, page_num)?;

        let slot_count = page.slot_count();
        let tuple_count_on_page = page.tuples().count() as u32;
        if tuple_count_on_page > 0 {
            stats.pages_with_tuples += 1;
        }

        let mut page_tuple_bytes = 0u64;
        let mut page_min_tuple_bytes = u32::MAX;
        let mut page_max_tuple_bytes = 0u32;

        let page_free_bytes = page_free_space(&page)?;
        stats.total_free_bytes += page_free_bytes as u64;
        stats.min_page_free_bytes = stats.min_page_free_bytes.min(page_free_bytes);
        stats.max_page_free_bytes = stats.max_page_free_bytes.max(page_free_bytes);

        stats.total_tuple_count += tuple_count_on_page as u64;
        stats.total_slot_bytes += slot_count as u64 * ITEM_ID_SIZE as u64;

        for (_, tuple) in page.tuples() {
            let length = tuple.len() as u32;
            stats.total_tuple_bytes += length as u64;
            stats.min_tuple_bytes = stats.min_tuple_bytes.min(length);
            stats.max_tuple_bytes = stats.max_tuple_bytes.max(length);

            page_tuple_bytes += length as u64;
            page_min_tuple_bytes = page_min_tuple_bytes.min(length);
            page_max_tuple_bytes = page_max_tuple_bytes.max(length);
            if let Some(collector) = collector.as_mut() {
                collector.add(tuple);
            }
        }

        if tuple_count_on_page == 0 {
            page_min_tuple_bytes = 0;
        }

        let used_bytes = PAGE_SIZE as u64 - page_free_bytes as u64;
        let page_slot_bytes = slot_count as u64 * ITEM_ID_SIZE as u64;

        stats.page_breakdown.push(PageStatistics {
            page_id: page_num,
            tuple_count: tuple_count_on_page,
            tuple_bytes: page_tuple_bytes,
            slot_bytes: page_slot_bytes,
            header_bytes: PAGE_HEADER_SIZE as u64,
            used_bytes,
            free_bytes: page_free_bytes as u64,
            min_tuple_bytes: page_min_tuple_bytes,
            max_tuple_bytes: page_max_tuple_bytes,
        });
    }

    if let Some(collector) = collector {
        stats.columns = collector.finish();
    }

    if stats.total_tuple_count == 0 {
        stats.min_tuple_bytes = 0;
    }

    if stats.data_pages == 0 {
        stats.min_page_free_bytes = 0;
    }

    Ok(stats)
}
