//! Duplicate detection and management.
//!
//! Creates a `.dup` sidecar file next to each `.dat` file that records
//! which tuple slot numbers are exact duplicates of an earlier slot.
//! This lets callers choose: include duplicates (raw scan) or exclude them.
//!
//! Sidecar format  database/base/{db}/{table}.dup :
//!   [ 8-byte header: u64 le = number of duplicate entries ]
//!   [ repeated: (page_num: u32 le)(slot_num: u32 le) ]  ← duplicate slots
//!
//! A "duplicate" is a tuple whose full byte content is identical to any
//! earlier tuple in the table.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::catalog::types::{Catalog, Column};
use crate::disk::read_page;
use crate::executor::tuple_codec::decode_tuple;
use crate::executor::value::Value;
use crate::layout::TABLE_FILE_TEMPLATE;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::table::page_count;

/// Identifies a single tuple by its location.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TupleLocation {
    pub page_num: u32,
    pub slot_num: u32,
}

/// Result of a duplicate scan.
#[derive(Debug)]
pub struct DuplicateReport {
    /// Total tuples scanned.
    pub total_tuples: u64,
    /// Number of duplicate tuple locations found.
    pub duplicate_count: u64,
    /// The duplicate locations (page, slot).
    pub duplicates: Vec<TupleLocation>,
    /// How many distinct content groups had duplicates.
    pub duplicate_groups: usize,
}

impl DuplicateReport {
    pub fn print(&self) {
        println!("  Total tuples scanned : {}", self.total_tuples);
        println!("  Duplicate tuples     : {}", self.duplicate_count);
        println!("  Unique duplicate groups: {}", self.duplicate_groups);
        if self.duplicate_count == 0 {
            println!("  ✓ No duplicates found.");
        } else {
            println!("  First 20 duplicate locations:");
            for loc in self.duplicates.iter().take(20) {
                println!("    page={} slot={}", loc.page_num, loc.slot_num);
            }
            if self.duplicate_count > 20 {
                println!("    ... and {} more", self.duplicate_count - 20);
            }
        }
    }
}

// ── sidecar path ─────────────────────────────────────────────────────────────

fn dup_path(db_name: &str, table_name: &str) -> String {
    format!("database/base/{}/{}.dup", db_name, table_name)
}

// ── scan for duplicates ───────────────────────────────────────────────────────

/// Scan the table, find all duplicate tuples, write a sidecar .dup file,
/// and return a DuplicateReport.
///
/// Two tuples are duplicates when their decoded Values are all equal.
/// The FIRST occurrence is kept; every subsequent identical tuple is marked.
pub fn build_duplicate_index(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
) -> io::Result<DuplicateReport> {
    let (schema, path) = resolve(catalog, db_name, table_name)?;
    let mut file = open_read(&path)?;
    let total_pages = page_count(&mut file)?;

    // content key → first-seen location
    let mut seen: HashMap<Vec<u8>, TupleLocation> = HashMap::new();
    let mut duplicates: Vec<TupleLocation> = Vec::new();
    let mut total_tuples: u64 = 0;
    let mut group_set: HashSet<Vec<u8>> = HashSet::new();

    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for slot_num in 0..num_items {
            let base = (PAGE_HEADER_SIZE + slot_num * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(
                page.data[base..base + 4].try_into().unwrap()
            ) as usize;
            let length = u32::from_le_bytes(
                page.data[base + 4..base + 8].try_into().unwrap()
            ) as usize;

            let raw = page.data[offset..offset + length].to_vec();
            let row = decode_tuple(&raw, &schema);

            // Build a canonical content key: serialize decoded values as strings
            let content_key = row_key(&row);
            total_tuples += 1;

            if seen.contains_key(&content_key) {
                // This is a duplicate of an earlier tuple
                duplicates.push(TupleLocation { page_num, slot_num });
                group_set.insert(content_key);
            } else {
                seen.insert(content_key, TupleLocation { page_num, slot_num });
            }
        }
    }

    let duplicate_groups = group_set.len();
    let duplicate_count = duplicates.len() as u64;

    // Write sidecar file
    write_dup_sidecar(db_name, table_name, &duplicates)?;

    Ok(DuplicateReport {
        total_tuples,
        duplicate_count,
        duplicates,
        duplicate_groups,
    })
}

/// Load previously saved duplicate index from the sidecar file.
/// Returns empty vec if no sidecar exists yet.
pub fn load_duplicate_index(db_name: &str, table_name: &str) -> io::Result<Vec<TupleLocation>> {
    let path = dup_path(db_name, table_name);
    let mut file = match File::open(&path) {
        Ok(f) => f,
        Err(_) => return Ok(vec![]),
    };

    let mut header = [0u8; 8];
    file.read_exact(&mut header)?;
    let count = u64::from_le_bytes(header) as usize;

    let mut locs = Vec::with_capacity(count);
    for _ in 0..count {
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf)?;
        let page_num = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let slot_num = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        locs.push(TupleLocation { page_num, slot_num });
    }
    Ok(locs)
}

/// Check whether a specific (page, slot) is a duplicate according to the sidecar.
pub fn is_duplicate_slot(
    db_name: &str,
    table_name: &str,
    page_num: u32,
    slot_num: u32,
) -> io::Result<bool> {
    let locs = load_duplicate_index(db_name, table_name)?;
    Ok(locs.iter().any(|l| l.page_num == page_num && l.slot_num == slot_num))
}

// ── copy table excluding / including duplicates ──────────────────────────────

/// Write a new `.dat` file that contains only non-duplicate rows.
/// The new file is written to `database/base/{db}/{table}_dedup.dat`.
/// Returns the count of rows written.
pub fn copy_deduped(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
) -> io::Result<u64> {
    let (_schema, path) = resolve(catalog, db_name, table_name)?;
    let mut src = open_read(&path)?;
    let total_pages = page_count(&mut src)?;

    let dup_set: HashSet<(u32, u32)> = load_duplicate_index(db_name, table_name)?
        .into_iter()
        .map(|l| (l.page_num, l.slot_num))
        .collect();

    let dest_path = format!("database/base/{}/{}_dedup.dat", db_name, table_name);
    let mut dest = OpenOptions::new()
        .create(true).write(true).read(true).truncate(true)
        .open(&dest_path)?;

    // Write header page (page count = 1, will be updated)
    let mut header = vec![0u8; 8192];
    header[0..4].copy_from_slice(&1u32.to_le_bytes());
    dest.write_all(&header)?;

    // Write first data page header
    let mut data_page = vec![0u8; 8192];
    data_page[0..4].copy_from_slice(&8u32.to_le_bytes()); // lower = PAGE_HEADER_SIZE
    data_page[4..8].copy_from_slice(&8192u32.to_le_bytes()); // upper = PAGE_SIZE
    dest.write_all(&data_page)?;
    let mut current_page_count: u32 = 2; // header + first data page

    let mut rows_written: u64 = 0;

    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(&mut src, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for slot_num in 0..num_items {
            if dup_set.contains(&(page_num, slot_num)) {
                continue; // skip duplicate
            }

            let base = (PAGE_HEADER_SIZE + slot_num * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(
                page.data[base..base + 4].try_into().unwrap()
            ) as usize;
            let length = u32::from_le_bytes(
                page.data[base + 4..base + 8].try_into().unwrap()
            ) as usize;
            let raw = &page.data[offset..offset + length];

            // Append to dest using insert logic
            dest_insert_tuple(&mut dest, &mut current_page_count, raw)?;
            rows_written += 1;
        }
    }

    // Update page count in dest header
    dest.seek(SeekFrom::Start(0))?;
    dest.write_all(&current_page_count.to_le_bytes())?;

    println!("  Deduped file: {}", dest_path);
    println!("  Rows written: {}", rows_written);

    Ok(rows_written)
}

/// Write a new `.dat` file that contains ONLY duplicate rows.
/// Written to `database/base/{db}/{table}_dups_only.dat`.
pub fn copy_duplicates_only(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
) -> io::Result<u64> {
    let (_, path) = resolve(catalog, db_name, table_name)?;
    let mut src = open_read(&path)?;
    let total_pages = page_count(&mut src)?;

    let dup_set: HashSet<(u32, u32)> = load_duplicate_index(db_name, table_name)?
        .into_iter()
        .map(|l| (l.page_num, l.slot_num))
        .collect();

    if dup_set.is_empty() {
        println!("  No duplicates found. Nothing to copy.");
        return Ok(0);
    }

    let dest_path = format!("database/base/{}/{}_dups_only.dat", db_name, table_name);
    let mut dest = OpenOptions::new()
        .create(true).write(true).read(true).truncate(true)
        .open(&dest_path)?;

    let mut hdr = vec![0u8; 8192];
    hdr[0..4].copy_from_slice(&1u32.to_le_bytes());
    dest.write_all(&hdr)?;
    let mut data_p = vec![0u8; 8192];
    data_p[0..4].copy_from_slice(&8u32.to_le_bytes());
    data_p[4..8].copy_from_slice(&8192u32.to_le_bytes());
    dest.write_all(&data_p)?;
    let mut current_page_count: u32 = 2;
    let mut rows_written: u64 = 0;

    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(&mut src, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for slot_num in 0..num_items {
            if !dup_set.contains(&(page_num, slot_num)) {
                continue; // skip non-duplicates
            }

            let base = (PAGE_HEADER_SIZE + slot_num * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(
                page.data[base..base + 4].try_into().unwrap()
            ) as usize;
            let length = u32::from_le_bytes(
                page.data[base + 4..base + 8].try_into().unwrap()
            ) as usize;
            let raw = &page.data[offset..offset + length];

            dest_insert_tuple(&mut dest, &mut current_page_count, raw)?;
            rows_written += 1;
        }
    }

    dest.seek(SeekFrom::Start(0))?;
    dest.write_all(&current_page_count.to_le_bytes())?;

    println!("  Duplicates-only file: {}", dest_path);
    println!("  Rows written: {}", rows_written);

    Ok(rows_written)
}

// ── private helpers ──────────────────────────────────────────────────────────

fn row_key(row: &[Value]) -> Vec<u8> {
    let parts: Vec<String> = row.iter().map(|v| format!("{:?}", v)).collect();
    parts.join("\x00").into_bytes()
}

fn write_dup_sidecar(db_name: &str, table_name: &str, dups: &[TupleLocation]) -> io::Result<()> {
    let path = dup_path(db_name, table_name);
    let mut f = File::create(&path)?;
    f.write_all(&(dups.len() as u64).to_le_bytes())?;
    for loc in dups {
        f.write_all(&loc.page_num.to_le_bytes())?;
        f.write_all(&loc.slot_num.to_le_bytes())?;
    }
    Ok(())
}

fn dest_insert_tuple(dest: &mut File, page_count: &mut u32, data: &[u8]) -> io::Result<()> {
    // Read last data page
    let last_page_num = *page_count - 1;
    let offset_in_file = (last_page_num as u64) * 8192;
    dest.seek(SeekFrom::Start(offset_in_file))?;
    let mut page_buf = vec![0u8; 8192];
    dest.read_exact(&mut page_buf)?;

    let lower = u32::from_le_bytes(page_buf[0..4].try_into().unwrap());
    let upper = u32::from_le_bytes(page_buf[4..8].try_into().unwrap());
    let free = upper - lower;
    let needed = data.len() as u32 + 8; // +8 for item id slot

    if free < needed {
        // Allocate a new page
        let mut new_page = vec![0u8; 8192];
        new_page[0..4].copy_from_slice(&8u32.to_le_bytes());
        new_page[4..8].copy_from_slice(&8192u32.to_le_bytes());
        dest.seek(SeekFrom::End(0))?;
        dest.write_all(&new_page)?;
        *page_count += 1;
        return dest_insert_tuple(dest, page_count, data);
    }

    let new_upper = upper - data.len() as u32;
    page_buf[new_upper as usize..upper as usize].copy_from_slice(data);
    page_buf[lower as usize..lower as usize + 4].copy_from_slice(&new_upper.to_le_bytes());
    page_buf[lower as usize + 4..lower as usize + 8]
        .copy_from_slice(&(data.len() as u32).to_le_bytes());
    page_buf[0..4].copy_from_slice(&(lower + 8).to_le_bytes());
    page_buf[4..8].copy_from_slice(&new_upper.to_le_bytes());

    dest.seek(SeekFrom::Start(offset_in_file))?;
    dest.write_all(&page_buf)?;
    Ok(())
}

fn resolve(catalog: &Catalog, db_name: &str, table_name: &str) -> io::Result<(Vec<Column>, String)> {
    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name))
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name))
    })?;
    let path = TABLE_FILE_TEMPLATE
        .replace("{database}", db_name)
        .replace("{table}", table_name);
    Ok((table.columns.clone(), path))
}

fn open_read(path: &str) -> io::Result<File> {
    OpenOptions::new().read(true).open(path).map_err(|e| {
        io::Error::new(e.kind(), format!("Cannot open '{}': {}", path, e))
    })
}
