//! External multi-way merge sort for large datasets.
//!
//! When a table exceeds available buffer pool capacity, external sort
//! proceeds in phases:
//! 1. Run generation: read B pages at a time, sort in memory, write to temp files
//! 2. Multi-way merge: merge up to (B-1) runs at a time using a min-heap
//! 3. Write the final sorted output as an ordered file

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write as IoWrite};

use crate::catalog::save_catalog;
use crate::catalog::types::{Catalog, Column, SortDirection, SortKey};
use crate::disk::{read_page, write_page};
use crate::ordered::ordered_file::{
    FileType, OrderedFileHeader, SortKeyEntry, write_ordered_file_header,
};
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE, Page, init_page};
use crate::sorting::comparator::TupleComparator;
use crate::table::page_count;

/// Represents a sorted run file produced during external merge sort.
pub struct SortedRun {
    /// Path to the temporary run file on disk
    pub file_path: String,
    /// Number of data pages in this run (excluding header)
    pub page_count: u32,
    /// Current page being read (for merge iteration); starts at 1 (first data page)
    pub current_page: u32,
    /// Current tuple slot within the current page
    pub current_slot: u32,
    /// Number of tuples on the current page
    pub current_page_tuples: u32,
    /// True when all tuples have been consumed
    pub is_exhausted: bool,
    /// The currently loaded page data
    pub page_buffer: Page,
}

impl SortedRun {
    /// Create a new sorted run from a temp file path.
    pub fn new(file_path: String) -> io::Result<Self> {
        let mut file = File::open(&file_path)?;
        let total = page_count(&mut file)?;
        let data_pages = if total > 1 { total - 1 } else { 0 };

        let mut run = Self {
            file_path,
            page_count: data_pages,
            current_page: 1,
            current_slot: 0,
            current_page_tuples: 0,
            is_exhausted: data_pages == 0,
            page_buffer: Page::new(),
        };

        if !run.is_exhausted {
            run.load_current_page()?;
        }

        Ok(run)
    }

    /// Load the current page into the page buffer.
    fn load_current_page(&mut self) -> io::Result<()> {
        let mut file = File::open(&self.file_path)?;
        read_page(&mut file, &mut self.page_buffer, self.current_page)?;

        let lower = u32::from_le_bytes(self.page_buffer.data[0..4].try_into().unwrap());
        self.current_page_tuples = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
        self.current_slot = 0;

        Ok(())
    }

    /// Peek at the current tuple without advancing.
    pub fn peek_tuple(&self) -> Option<Vec<u8>> {
        if self.is_exhausted {
            return None;
        }

        let base = (PAGE_HEADER_SIZE + self.current_slot * ITEM_ID_SIZE) as usize;
        let offset =
            u32::from_le_bytes(self.page_buffer.data[base..base + 4].try_into().unwrap()) as usize;
        let length = u32::from_le_bytes(
            self.page_buffer.data[base + 4..base + 8]
                .try_into()
                .unwrap(),
        ) as usize;

        Some(self.page_buffer.data[offset..offset + length].to_vec())
    }

    /// Advance to the next tuple. Returns Ok(true) if there is a next tuple.
    pub fn advance(&mut self) -> io::Result<bool> {
        if self.is_exhausted {
            return Ok(false);
        }

        self.current_slot += 1;

        if self.current_slot >= self.current_page_tuples {
            // Move to next page
            self.current_page += 1;
            if self.current_page > self.page_count {
                self.is_exhausted = true;
                return Ok(false);
            }
            self.load_current_page()?;
        }

        Ok(true)
    }
}

/// Entry in the min-heap used during k-way merge.
pub struct MergeEntry {
    /// The tuple bytes
    pub tuple_data: Vec<u8>,
    /// Which run this tuple came from
    pub run_index: usize,
}

/// Wrapper for MergeEntry that implements Ord for use in BinaryHeap (max-heap).
/// We reverse the comparison to get min-heap behavior.
struct HeapEntry {
    entry: MergeEntry,
    comparator_ptr: *const TupleComparator,
}

// Safety: HeapEntry is only used within a single-threaded context
unsafe impl Send for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        let comparator = unsafe { &*self.comparator_ptr };
        comparator.compare(&self.entry.tuple_data, &other.entry.tuple_data) == Ordering::Equal
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior (BinaryHeap is a max-heap)
        let comparator = unsafe { &*self.comparator_ptr };
        comparator.compare(&other.entry.tuple_data, &self.entry.tuple_data)
    }
}

/// Manages the overall state of an external merge sort operation.
pub struct ExternalSortState {
    pub source_db: String,
    pub source_table: String,
    pub sort_keys: Vec<SortKey>,
    pub buffer_pool_size: usize,
    pub runs: Vec<SortedRun>,
    pub comparator: TupleComparator,
    pub temp_file_counter: u32,
    pub temp_file_paths: Vec<String>,
}

impl ExternalSortState {
    /// Initialize a new external sort operation.
    pub fn new(
        db_name: &str,
        table_name: &str,
        sort_keys: Vec<SortKey>,
        columns: Vec<Column>,
        buffer_pool_size: usize,
    ) -> Self {
        let comparator = TupleComparator::new(columns, sort_keys.clone());
        Self {
            source_db: db_name.to_string(),
            source_table: table_name.to_string(),
            sort_keys,
            buffer_pool_size,
            runs: Vec::new(),
            comparator,
            temp_file_counter: 0,
            temp_file_paths: Vec::new(),
        }
    }

    /// Generate the path for a new temporary run file.
    pub fn next_temp_file_path(&mut self) -> String {
        let path = format!(
            "database/base/{}/.sort_tmp_{}_run_{}.dat",
            self.source_db, self.source_table, self.temp_file_counter
        );
        self.temp_file_counter += 1;
        self.temp_file_paths.push(path.clone());
        path
    }

    /// Clean up all temporary run files.
    pub fn cleanup(&self) -> io::Result<()> {
        for path in &self.temp_file_paths {
            if std::path::Path::new(path).exists() {
                fs::remove_file(path)?;
            }
        }
        Ok(())
    }
}

/// Extract all tuples from a set of pages.
fn extract_tuples_from_pages(pages: &[Page]) -> Vec<Vec<u8>> {
    let mut tuples = Vec::new();
    for page in pages {
        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
            let length =
                u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
            tuples.push(page.data[offset..offset + length].to_vec());
        }
    }
    tuples
}

/// Write sorted tuples into a new temp file. Returns the file path.
fn write_tuples_to_temp_file(tuples: &[Vec<u8>], file_path: &str) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(true)
        .open(file_path)?;

    // Write header page (page 0)
    let mut header_buf = vec![0u8; PAGE_SIZE];
    header_buf[0..4].copy_from_slice(&1u32.to_le_bytes()); // page_count = 1 initially
    file.write_all(&header_buf)?;
    file.flush()?;

    // Write tuples into data pages
    let mut current_page = Page::new();
    init_page(&mut current_page);
    let mut page_count_val: u32 = 1; // header page

    for tuple in tuples {
        let tuple_len = tuple.len() as u32;
        let required = tuple_len + ITEM_ID_SIZE;

        let lower = u32::from_le_bytes(current_page.data[0..4].try_into().unwrap());
        let upper = u32::from_le_bytes(current_page.data[4..8].try_into().unwrap());
        let free = upper - lower;

        if required > free {
            // Write current page to file
            page_count_val += 1;
            let offset = (page_count_val - 1) as u64 * PAGE_SIZE as u64;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&current_page.data)?;

            // Start a new page
            current_page = Page::new();
            init_page(&mut current_page);
        }

        // Insert tuple into current page
        let mut lower = u32::from_le_bytes(current_page.data[0..4].try_into().unwrap());
        let mut upper = u32::from_le_bytes(current_page.data[4..8].try_into().unwrap());

        let start = upper - tuple_len;
        current_page.data[start as usize..upper as usize].copy_from_slice(tuple);

        current_page.data[lower as usize..lower as usize + 4].copy_from_slice(&start.to_le_bytes());
        current_page.data[lower as usize + 4..lower as usize + 8]
            .copy_from_slice(&tuple_len.to_le_bytes());

        lower += ITEM_ID_SIZE;
        upper = start;

        current_page.data[0..4].copy_from_slice(&lower.to_le_bytes());
        current_page.data[4..8].copy_from_slice(&upper.to_le_bytes());
    }

    // Write the last page
    page_count_val += 1;
    let offset = (page_count_val - 1) as u64 * PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&current_page.data)?;

    // Update page count in header
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&page_count_val.to_le_bytes())?;
    file.flush()?;

    Ok(())
}

/// Phase 1: Generate sorted runs from the source table.
///
/// Reads B pages at a time, sorts tuples in memory, writes each batch
/// as a sorted run to a temporary file.
pub fn generate_sorted_runs(file: &mut File, state: &mut ExternalSortState) -> io::Result<()> {
    let total_pages = page_count(file)?;
    let mut current_page_num: u32 = 1; // skip header

    while current_page_num < total_pages {
        // Determine batch size
        let remaining = total_pages - current_page_num;
        let batch_size = std::cmp::min(state.buffer_pool_size as u32, remaining);

        // Read batch pages
        let mut batch_pages = Vec::with_capacity(batch_size as usize);
        for i in 0..batch_size {
            let mut page = Page::new();
            read_page(file, &mut page, current_page_num + i)?;
            batch_pages.push(page);
        }

        // Extract and sort tuples
        let mut tuples = extract_tuples_from_pages(&batch_pages);
        tuples.sort_by(|a, b| state.comparator.compare(a, b));

        // Write sorted run to temp file
        let temp_path = state.next_temp_file_path();
        write_tuples_to_temp_file(&tuples, &temp_path)?;

        // Create SortedRun
        let run = SortedRun::new(temp_path)?;
        state.runs.push(run);

        current_page_num += batch_size;
    }

    println!(
        "Generated {} sorted runs from {} data pages.",
        state.runs.len(),
        total_pages - 1
    );

    Ok(())
}

/// Phase 2: Multi-way merge of sorted runs.
///
/// Merges up to (B-1) runs at a time using a min-heap.
/// Repeats merge passes until a single run remains.
pub fn merge_runs(state: &mut ExternalSortState) -> io::Result<String> {
    let k = if state.buffer_pool_size > 1 {
        state.buffer_pool_size - 1
    } else {
        1
    };

    while state.runs.len() > 1 {
        let mut new_runs: Vec<SortedRun> = Vec::new();

        // Process runs in batches of k
        while !state.runs.is_empty() {
            let batch_size = std::cmp::min(k, state.runs.len());
            let mut batch: Vec<SortedRun> = state.runs.drain(..batch_size).collect();

            if batch.len() == 1 {
                // Only one run left in this batch, no merge needed
                new_runs.push(batch.pop().unwrap());
                continue;
            }

            // Create output temp file
            let output_path = state.next_temp_file_path();

            // Initialize min-heap
            let comparator_ptr: *const TupleComparator = &state.comparator;
            let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();

            // Seed heap with first tuple from each run
            for (idx, run) in batch.iter().enumerate() {
                if let Some(tuple) = run.peek_tuple() {
                    heap.push(HeapEntry {
                        entry: MergeEntry {
                            tuple_data: tuple,
                            run_index: idx,
                        },
                        comparator_ptr,
                    });
                }
            }

            // Merge into output — write incrementally to disk (page at a time)
            let mut out_file = OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .truncate(true)
                .open(&output_path)?;

            // Write placeholder header page
            let mut header_buf = vec![0u8; PAGE_SIZE];
            header_buf[0..4].copy_from_slice(&1u32.to_le_bytes());
            out_file.write_all(&header_buf)?;
            out_file.flush()?;

            let mut current_out_page = Page::new();
            init_page(&mut current_out_page);
            let mut out_page_count: u32 = 1; // header page

            while let Some(heap_entry) = heap.pop() {
                let tuple = &heap_entry.entry.tuple_data;
                let run_idx = heap_entry.entry.run_index;

                // Write tuple to output page, flushing if full
                let tuple_len = tuple.len() as u32;
                let required = tuple_len + ITEM_ID_SIZE;
                let lower = u32::from_le_bytes(current_out_page.data[0..4].try_into().unwrap());
                let upper = u32::from_le_bytes(current_out_page.data[4..8].try_into().unwrap());
                let free = upper - lower;

                if required > free {
                    // Flush current page to disk
                    out_page_count += 1;
                    let offset = (out_page_count - 1) as u64 * PAGE_SIZE as u64;
                    out_file.seek(SeekFrom::Start(offset))?;
                    out_file.write_all(&current_out_page.data)?;

                    current_out_page = Page::new();
                    init_page(&mut current_out_page);
                }

                // Insert tuple into current output page
                {
                    let mut lo =
                        u32::from_le_bytes(current_out_page.data[0..4].try_into().unwrap());
                    let mut up =
                        u32::from_le_bytes(current_out_page.data[4..8].try_into().unwrap());
                    let start = up - tuple_len;
                    current_out_page.data[start as usize..up as usize].copy_from_slice(tuple);
                    current_out_page.data[lo as usize..lo as usize + 4]
                        .copy_from_slice(&start.to_le_bytes());
                    current_out_page.data[lo as usize + 4..lo as usize + 8]
                        .copy_from_slice(&tuple_len.to_le_bytes());
                    lo += ITEM_ID_SIZE;
                    up = start;
                    current_out_page.data[0..4].copy_from_slice(&lo.to_le_bytes());
                    current_out_page.data[4..8].copy_from_slice(&up.to_le_bytes());
                }

                // Advance the run and push next tuple if available
                if batch[run_idx].advance()? {
                    if let Some(next_tuple) = batch[run_idx].peek_tuple() {
                        heap.push(HeapEntry {
                            entry: MergeEntry {
                                tuple_data: next_tuple,
                                run_index: run_idx,
                            },
                            comparator_ptr,
                        });
                    }
                }
            }

            // Flush last page
            out_page_count += 1;
            let offset = (out_page_count - 1) as u64 * PAGE_SIZE as u64;
            out_file.seek(SeekFrom::Start(offset))?;
            out_file.write_all(&current_out_page.data)?;

            // Update page count in header
            out_file.seek(SeekFrom::Start(0))?;
            out_file.write_all(&out_page_count.to_le_bytes())?;
            out_file.flush()?;

            // Clean up consumed run files
            for run in &batch {
                if std::path::Path::new(&run.file_path).exists() {
                    fs::remove_file(&run.file_path)?;
                }
            }

            let merged_run = SortedRun::new(output_path)?;
            new_runs.push(merged_run);
        }

        state.runs = new_runs;
    }

    // Return the path of the final run
    let final_path = state.runs[0].file_path.clone();
    println!("Merge complete. Final run: {}", final_path);
    Ok(final_path)
}

/// Orchestrates the complete external merge sort process.
///
/// 1. Generate sorted runs from the source table
/// 2. Merge runs until a single sorted run remains
/// 3. Write the final sorted output as an ordered file
pub fn external_sort(
    catalog: &mut Catalog,
    db_name: &str,
    table_name: &str,
    sort_keys: Vec<SortKey>,
    buffer_pool_size: usize,
) -> io::Result<()> {
    // 1. Load schema
    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Database '{}' not found", db_name),
        )
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Table '{}' not found", table_name),
        )
    })?;
    let columns: Vec<Column> = table.columns.clone();

    // 2. Create state
    let mut state = ExternalSortState::new(
        db_name,
        table_name,
        sort_keys.clone(),
        columns,
        buffer_pool_size,
    );

    // 3. Open source table file
    let table_path = format!("database/base/{}/{}.dat", db_name, table_name);
    let mut source_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&table_path)?;

    // 4. Generate sorted runs
    generate_sorted_runs(&mut source_file, &mut state)?;

    // 5. If only 1 run, skip merge
    let final_run_path = if state.runs.len() == 1 {
        state.runs[0].file_path.clone()
    } else {
        merge_runs(&mut state)?
    };

    // 6. Copy final run to the original table file
    let mut final_file = File::open(&final_run_path)?;
    let final_total_pages = page_count(&mut final_file)?;

    // Write ordered file header
    let sort_key_entries: Vec<SortKeyEntry> = sort_keys
        .iter()
        .map(|sk| SortKeyEntry {
            column_index: sk.column_index,
            direction: match sk.direction {
                SortDirection::Ascending => 0,
                SortDirection::Descending => 1,
            },
        })
        .collect();

    let header = OrderedFileHeader {
        page_count: final_total_pages,
        file_type: FileType::Ordered,
        sort_key_count: sort_key_entries.len() as u32,
        sort_keys: sort_key_entries,
    };

    // Truncate original file to the right size
    source_file.set_len(final_total_pages as u64 * PAGE_SIZE as u64)?;
    write_ordered_file_header(&mut source_file, &header)?;

    // Copy data pages from final run to source file
    for page_num in 1..final_total_pages {
        let mut page = Page::new();
        read_page(&mut final_file, &mut page, page_num)?;
        write_page(&mut source_file, &mut page, page_num)?;
    }

    // 7. Cleanup temp files
    state.cleanup()?;

    // 8. Update catalog
    let db = catalog.databases.get_mut(db_name).unwrap();
    let table = db.tables.get_mut(table_name).unwrap();
    table.sort_keys = Some(sort_keys);
    table.file_type = Some("ordered".to_string());
    save_catalog(catalog);

    println!(
        "External sort complete for table '{}'. {} pages written.",
        table_name,
        final_total_pages - 1
    );

    Ok(())
}
