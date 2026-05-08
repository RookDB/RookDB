use std::fs::File;
use std::io::{self};

use crate::catalog::types::Table;
use crate::catalog::Catalog;
use crate::disk::read_page;
use crate::page::{page_free_space, ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE, Page};
use crate::table::page_count;

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub page_breakdown: Vec<PageStatistics>,
}

impl TableStatistics {
    pub fn data_capacity_bytes(&self) -> u64 {
        self.data_pages as u64 * PAGE_SIZE as u64
    }

    pub fn used_bytes(&self) -> u64 {
        self.data_capacity_bytes().saturating_sub(self.total_free_bytes)
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

pub fn collect_table_statistics(db_name: &str, table_name: &str) -> io::Result<TableStatistics> {
    let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut file = File::open(&table_path)?;
    collect_table_statistics_from_file(&mut file)
}

pub fn collect_table_statistics_from_file(file: &mut File) -> io::Result<TableStatistics> {
    let metadata = file.metadata()?;
    let file_size_bytes = metadata.len();
    let total_pages = page_count(file)?;
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
    };

    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let tuple_count_on_page = lower.saturating_sub(PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
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
        stats.total_slot_bytes += tuple_count_on_page as u64 * ITEM_ID_SIZE as u64;

        for item_index in 0..tuple_count_on_page {
            let base = (PAGE_HEADER_SIZE + item_index * ITEM_ID_SIZE) as usize;
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap());
            stats.total_tuple_bytes += length as u64;
            stats.min_tuple_bytes = stats.min_tuple_bytes.min(length);
            stats.max_tuple_bytes = stats.max_tuple_bytes.max(length);

            page_tuple_bytes += length as u64;
            page_min_tuple_bytes = page_min_tuple_bytes.min(length);
            page_max_tuple_bytes = page_max_tuple_bytes.max(length);
        }

        if tuple_count_on_page == 0 {
            page_min_tuple_bytes = 0;
        }

        let used_bytes = PAGE_SIZE as u64 - page_free_bytes as u64;
        let page_slot_bytes = tuple_count_on_page as u64 * ITEM_ID_SIZE as u64;

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

    if stats.total_tuple_count == 0 {
        stats.min_tuple_bytes = 0;
    }

    if stats.data_pages == 0 {
        stats.min_page_free_bytes = 0;
    }

    Ok(stats)
}

pub fn print_table_page_count(db_name: &str, table_name: &str) -> io::Result<()> {
    let stats = collect_table_statistics(db_name, table_name)?;
    println!("Table '{}' has {} total pages.", table_name, stats.total_pages);
    Ok(())
}

pub fn print_table_statistics(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
) -> io::Result<()> {
    let table = catalog
        .databases
        .get(db_name)
        .and_then(|db| db.tables.get(table_name))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Table '{}.{}' not found", db_name, table_name),
            )
        })?;

    let stats = collect_table_statistics(db_name, table_name)?;
    print_table_statistics_report(db_name, table_name, table, &stats);
    Ok(())
}

fn print_table_statistics_report(
    db_name: &str,
    table_name: &str,
    table: &Table,
    stats: &TableStatistics,
) {
    println!("\n=== Table Statistics: {}.{} ===", db_name, table_name);
    println!("Relation size: {} bytes", stats.file_size_bytes);
    println!("Total pages: {}", stats.total_pages);
    println!("Data pages: {}", stats.data_pages);
    println!("Tuples: {}", stats.total_tuple_count);
    println!("Pages with tuples: {}", stats.pages_with_tuples);
    println!("Tuples per data page (avg): {:.2}", stats.avg_tuples_per_page());
    println!("Tuple width (avg): {:.2} bytes", stats.avg_tuple_bytes());
    println!(
        "Tuple width (min/max): {} / {} bytes",
        stats.min_tuple_bytes, stats.max_tuple_bytes
    );
    println!("Total tuple bytes: {}", stats.total_tuple_bytes);
    println!("Slot directory bytes: {}", stats.total_slot_bytes);
    println!("Page header bytes: {}", stats.total_header_bytes);
    println!("Free space: {} bytes", stats.total_free_bytes);
    println!("Average free space per data page: {:.2} bytes", stats.avg_page_free_bytes());
    println!("Page fill factor: {:.2}%", stats.fill_percent());
    println!(
        "Free space per data page (min/max): {} / {} bytes",
        stats.min_page_free_bytes, stats.max_page_free_bytes
    );

    println!("\nPage breakdown:");
    for page in &stats.page_breakdown {
        println!(
            "  - page {}: tuples={}, used={} bytes, free={} bytes, tuple_bytes={}, tuple_width(min/max)={}/{}",
            page.page_id,
            page.tuple_count,
            page.used_bytes,
            page.free_bytes,
            page.tuple_bytes,
            page.min_tuple_bytes,
            page.max_tuple_bytes
        );
    }

    println!("\nColumns ({}):", table.columns.len());
    for (index, column) in table.columns.iter().enumerate() {
        let mut constraints = Vec::new();
        if column.constraints.not_null || !column.nullable {
            constraints.push("NOT NULL".to_string());
        }
        if column.constraints.unique {
            constraints.push("UNIQUE".to_string());
        }
        if let Some(default_value) = &column.constraints.default {
            constraints.push(format!("DEFAULT {}", default_value));
        }
        if let Some(check_expr) = &column.constraints.check {
            constraints.push(format!("CHECK ({})", check_expr));
        }

        let constraint_text = if constraints.is_empty() {
            String::from("-")
        } else {
            constraints.join(" | ")
        };

        println!(
            "  {}. {} {} [{}]",
            index + 1,
            column.name,
            column.data_type,
            constraint_text
        );
    }

    println!();
}
