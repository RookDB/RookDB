// TupleScanner: sequential iterator over all tuples in a table file.
use std::fs::{File, OpenOptions};
use std::io::{self};

use crate::catalog::types::{Catalog, Column};
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::disk::read_page;
use crate::table::page_count;

use super::tuple::{Tuple, deserialize_tuple};

/// Sequential iterator over all tuples in a table file.
pub struct TupleScanner {
    pub file: File,
    pub schema: Vec<Column>,
    pub current_page: u32,
    pub current_slot: u32,
    pub total_pages: u32,
}

impl TupleScanner {
    /// Open a table file and initialize the scanner.
    pub fn new(db: &str, table: &str, catalog: &Catalog) -> io::Result<TupleScanner> {
        let path = format!("database/base/{}/{}.dat", db, table);

        // Get schema from catalog
        let database = catalog.databases.get(db).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db))
        })?;
        let tbl = database.tables.get(table).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table))
        })?;
        let schema = tbl.columns.clone();

        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        let total_pages = page_count(&mut file)?;

        Ok(TupleScanner {
            file,
            schema,
            current_page: 1, // data starts at page 1
            current_slot: 0,
            total_pages,
        })
    }

    /// Create a scanner from an already-open file and known schema (for temp files).
    pub fn from_file(mut file: File, schema: Vec<Column>) -> io::Result<TupleScanner> {
        let total_pages = page_count(&mut file)?;
        Ok(TupleScanner {
            file,
            schema,
            current_page: 1,
            current_slot: 0,
            total_pages,
        })
    }

    /// Return the next tuple, advancing page/slot as needed.
    pub fn next_tuple(&mut self) -> Option<Tuple> {
        loop {
            if self.current_page >= self.total_pages {
                return None;
            }

            let mut page = Page::new();
            if read_page(&mut self.file, &mut page, self.current_page).is_err() {
                return None;
            }

            let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
            let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

            if self.current_slot >= num_items {
                // Move to next page
                self.current_page += 1;
                self.current_slot = 0;
                continue;
            }

            // Read ItemId
            let base = (PAGE_HEADER_SIZE + self.current_slot * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap());
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap());

            let tuple_data = &page.data[offset as usize..(offset + length) as usize];
            let tuple = deserialize_tuple(tuple_data, &self.schema);

            self.current_slot += 1;
            return Some(tuple);
        }
    }

    /// Rewind scanner to the first data page (for NLJ inner-table rescans).
    pub fn reset(&mut self) {
        self.current_page = 1;
        self.current_slot = 0;
    }

    /// Collect all tuples into a Vec.
    pub fn collect_all(&mut self) -> Vec<Tuple> {
        let mut tuples = Vec::new();
        while let Some(t) = self.next_tuple() {
            tuples.push(t);
        }
        tuples
    }
}