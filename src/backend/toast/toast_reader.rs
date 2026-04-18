use std::fs::File;
use std::io;

use crate::disk::read_page;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::table::page_count;
use crate::toast::ToastPointer;
use crate::toast::compression as toast_compress;

/// Reads a toasted value from the toast table.
///
pub fn detoast_value(toast_file: &mut File, pointer: &ToastPointer) -> io::Result<Vec<u8>> {
    let total_pages = page_count(toast_file)?;

    // collect matching chunks
    let mut chunks: Vec<(u32, Vec<u8>)> = Vec::with_capacity(pointer.num_chunks as usize);

    // scan data pages
    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(toast_file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
            let length =
                u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
            let tuple_data = &page.data[offset..offset + length];

            // First 4 bytes of the chunk tuple are toast value id
            if tuple_data.len() < 12 {
                continue;
            }
            let chunk_value_id = u32::from_le_bytes(tuple_data[0..4].try_into().unwrap());
            if chunk_value_id != pointer.toast_value_id {
                continue;
            }

            let chunk_seq = u32::from_le_bytes(tuple_data[4..8].try_into().unwrap());
            let chunk_data_len = u32::from_le_bytes(tuple_data[8..12].try_into().unwrap()) as usize;
            let chunk_data = tuple_data[12..12 + chunk_data_len].to_vec();
            chunks.push((chunk_seq, chunk_data));

            // we have all chunks
            if chunks.len() == pointer.num_chunks as usize {
                break;
            }
        }

        if chunks.len() == pointer.num_chunks as usize {
            break;
        }
    }

    // Sort by chunk_seq and concatenate
    chunks.sort_by_key(|(seq, _)| *seq);
    let mut stored_data = Vec::with_capacity(pointer.stored_size as usize);
    for (_, data) in chunks {
        stored_data.extend_from_slice(&data);
    }

    // Decompress if needed
    if pointer.compression == 0x01 {
        toast_compress::decompress(&stored_data)
    } else {
        Ok(stored_data)
    }
}
