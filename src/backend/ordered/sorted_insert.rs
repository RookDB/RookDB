//! Sorted insertion into ordered files with page splitting.
//!
//! Maintains the cross-page sort invariant: all tuples on page P
//! are <= all tuples on page P+1.

use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write as IoWrite};

use crate::disk::{read_page, write_page};
use crate::ordered::ordered_file::{read_ordered_file_header, write_ordered_file_header};
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE, Page, init_page};
use crate::sorting::comparator::TupleComparator;

/// Binary search across pages of an ordered file to find the correct page
/// for inserting a new tuple.
///
/// Returns the page number (1-based) where the tuple should be inserted.
pub fn find_insert_page(
    file: &mut File,
    total_pages: u32,
    tuple_data: &[u8],
    comparator: &TupleComparator,
) -> io::Result<u32> {
    if total_pages <= 2 {
        // Only header + one data page
        return Ok(1);
    }

    let mut low: u32 = 1;
    let mut high: u32 = total_pages - 1;

    while low < high {
        let mid = low + (high - low) / 2;
        let mut page = Page::new();
        read_page(file, &mut page, mid)?;

        // Get the last tuple on this page
        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        if num_items == 0 {
            high = mid;
            continue;
        }

        // Read last tuple
        let last_base = (PAGE_HEADER_SIZE + (num_items - 1) * ITEM_ID_SIZE) as usize;
        let last_offset =
            u32::from_le_bytes(page.data[last_base..last_base + 4].try_into().unwrap()) as usize;
        let last_length =
            u32::from_le_bytes(page.data[last_base + 4..last_base + 8].try_into().unwrap())
                as usize;
        let last_tuple = &page.data[last_offset..last_offset + last_length];

        match comparator.compare(tuple_data, last_tuple) {
            std::cmp::Ordering::Greater => {
                low = mid + 1;
            }
            _ => {
                high = mid;
            }
        }
    }

    // Clamp to valid range
    if low >= total_pages {
        low = total_pages - 1;
    }

    Ok(low)
}

/// Binary search within a page's ItemId array to find the correct slot
/// position for a new tuple.
///
/// Returns the slot index where the new ItemId should be inserted
/// (shifting existing entries right).
pub fn find_insert_slot(page: &Page, tuple_data: &[u8], comparator: &TupleComparator) -> u32 {
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let num_tuples = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

    if num_tuples == 0 {
        return 0;
    }

    let mut lo: u32 = 0;
    let mut hi: u32 = num_tuples;

    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let base = (PAGE_HEADER_SIZE + mid * ITEM_ID_SIZE) as usize;
        let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
        let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
        let existing_tuple = &page.data[offset..offset + length];

        match comparator.compare(tuple_data, existing_tuple) {
            std::cmp::Ordering::Less => {
                hi = mid;
            }
            _ => {
                lo = mid + 1;
            }
        }
    }

    lo
}

/// Extract all tuples from a page as Vec<Vec<u8>>.
fn extract_all_tuples(page: &Page) -> Vec<Vec<u8>> {
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
    let mut tuples = Vec::with_capacity(num_items as usize);

    for i in 0..num_items {
        let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
        let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
        let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
        tuples.push(page.data[offset..offset + length].to_vec());
    }

    tuples
}

/// Write a list of tuples into a page, assuming the page is freshly initialized.
fn write_tuples_to_page(page: &mut Page, tuples: &[Vec<u8>]) {
    let mut lower = PAGE_HEADER_SIZE;
    let mut upper = PAGE_SIZE as u32;

    for tuple in tuples {
        let tuple_len = tuple.len() as u32;
        let start = upper - tuple_len;
        page.data[start as usize..upper as usize].copy_from_slice(tuple);

        page.data[lower as usize..lower as usize + 4].copy_from_slice(&start.to_le_bytes());
        page.data[lower as usize + 4..lower as usize + 8].copy_from_slice(&tuple_len.to_le_bytes());

        lower += ITEM_ID_SIZE;
        upper = start;
    }

    page.data[0..4].copy_from_slice(&lower.to_le_bytes());
    page.data[4..8].copy_from_slice(&upper.to_le_bytes());
}

/// Splits a full page into two pages, distributing tuples evenly, and
/// inserts the new tuple into the correct page.
pub fn split_page(
    file: &mut File,
    page_num: u32,
    page: &Page,
    tuple_data: &[u8],
    comparator: &TupleComparator,
    total_pages: u32,
) -> io::Result<()> {
    // 1. Extract all tuples and add the new one
    let mut all_tuples = extract_all_tuples(page);
    all_tuples.push(tuple_data.to_vec());

    // 2. Sort all tuples
    all_tuples.sort_by(|a, b| comparator.compare(a, b));

    // 3. Split at midpoint
    let mid = all_tuples.len() / 2;
    let left_tuples = &all_tuples[..mid];
    let right_tuples = &all_tuples[mid..];

    // 4. Create left page (overwrites original)
    let mut left_page = Page::new();
    init_page(&mut left_page);
    write_tuples_to_page(&mut left_page, left_tuples);

    // 5. Create right page (new page)
    let mut right_page = Page::new();
    init_page(&mut right_page);
    write_tuples_to_page(&mut right_page, right_tuples);

    // 6. Shift subsequent pages forward by one position
    // Read from last page backwards to page_num + 1, write each one position later
    for p in (page_num + 1..total_pages).rev() {
        let mut temp_page = Page::new();
        read_page(file, &mut temp_page, p)?;
        // Write it one position later
        let new_offset = (p + 1) as u64 * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(new_offset))?;
        file.write_all(&temp_page.data)?;
    }

    // 7. Write the left page at original position
    write_page(file, &mut left_page, page_num)?;

    // 8. Write the right page at page_num + 1
    let right_offset = (page_num + 1) as u64 * PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(right_offset))?;
    file.write_all(&right_page.data)?;

    Ok(())
}

/// Inserts a tuple into an ordered file, maintaining the sort invariant
/// across and within pages. Triggers page split if the target page is full.
pub fn sorted_insert(
    file: &mut File,
    tuple_data: &[u8],
    comparator: &TupleComparator,
) -> io::Result<()> {
    // 1. Read ordered file header
    let mut header = read_ordered_file_header(file)?;
    let total_pages = header.page_count;

    // 2. Find target page
    let target_page_num = find_insert_page(file, total_pages, tuple_data, comparator)?;

    // 3. Read target page
    let mut page = Page::new();
    read_page(file, &mut page, target_page_num)?;

    // 4. Check free space
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
    let free_space = upper - lower;
    let required = tuple_data.len() as u32 + ITEM_ID_SIZE;

    if required <= free_space {
        // 5a. Enough space - insert in sorted position
        let slot_index = find_insert_slot(&page, tuple_data, comparator);
        let num_tuples = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        // Write tuple data at position upper - tuple_len
        let tuple_len = tuple_data.len() as u32;
        let start = upper - tuple_len;
        page.data[start as usize..upper as usize].copy_from_slice(tuple_data);

        // Shift ItemId entries from slot_index..num_tuples right by ITEM_ID_SIZE
        if slot_index < num_tuples {
            let src_start = (PAGE_HEADER_SIZE + slot_index * ITEM_ID_SIZE) as usize;
            let src_end = (PAGE_HEADER_SIZE + num_tuples * ITEM_ID_SIZE) as usize;
            let shift = ITEM_ID_SIZE as usize;

            // Copy from end to avoid overlap issues
            for i in (src_start..src_end).rev() {
                page.data[i + shift] = page.data[i];
            }
        }

        // Write new ItemId at slot_index
        let item_pos = (PAGE_HEADER_SIZE + slot_index * ITEM_ID_SIZE) as usize;
        page.data[item_pos..item_pos + 4].copy_from_slice(&start.to_le_bytes());
        page.data[item_pos + 4..item_pos + 8].copy_from_slice(&tuple_len.to_le_bytes());

        // Update lower and upper
        let new_lower = lower + ITEM_ID_SIZE;
        let new_upper = start;
        page.data[0..4].copy_from_slice(&new_lower.to_le_bytes());
        page.data[4..8].copy_from_slice(&new_upper.to_le_bytes());

        // Write page back
        write_page(file, &mut page, target_page_num)?;
    } else {
        // 5b. Not enough space - page split needed
        // First, extend the file to accommodate the new page
        let new_file_size = (total_pages + 1) as u64 * PAGE_SIZE as u64;
        file.set_len(new_file_size)?;

        split_page(
            file,
            target_page_num,
            &page,
            tuple_data,
            comparator,
            total_pages,
        )?;

        // Update page count
        header.page_count += 1;
        write_ordered_file_header(file, &header)?;
    }

    Ok(())
}
