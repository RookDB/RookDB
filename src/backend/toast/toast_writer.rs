use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::heap::insert_tuple;
use crate::toast::compression as toast_compression;
use crate::toast::{TOAST_CHUNK_SIZE, TOAST_POINTER_TAG, ToastPointer};

// Writes a large value into the TOAST table as chunks, returning a ToastPointer.
//
// Steps:
// 1. Allocate a unique toast_value_id from the toast table header
// 2. Optionally LZ4 compress the value
// 3. Split into chunks
// 4. Write chunks to the toast table using `insert_tuple`
// 5. Return a ToastPointer to store in main tuple

pub fn toast_value(
    toast_file: &mut File,
    value: &[u8],
    compression: bool,
) -> io::Result<ToastPointer> {
    //1. Allocate a unique toast_value_id from the toast table header
    let toast_value_id = allocate_toast_id(toast_file)?;

    // check if compression is enabled
    let original_size = value.len() as u32;
    let (stored_data, compression_flag) = if compression {
        let compressed = toast_compression::compress(value);
        (compressed, 0x01u8)
    } else {
        (value.to_vec(), 0x00u8)
    };

    let stored_size = stored_data.len() as u32;

    //3. split into chunks
    let mut chunk_seq: u32 = 0;
    let mut offset = 0;
    while offset < stored_data.len() {
        let end = std::cmp::min(offset + TOAST_CHUNK_SIZE, stored_data.len());
        let chunk_data = &stored_data[offset..end];
        let chunk_data_len = chunk_data.len() as u32;

        // serialize chunk tuple
        let mut chunk_tuple = Vec::with_capacity(12 + chunk_data.len());
        chunk_tuple.extend_from_slice(&toast_value_id.to_le_bytes());
        chunk_tuple.extend_from_slice(&chunk_seq.to_le_bytes());
        chunk_tuple.extend_from_slice(&chunk_data_len.to_le_bytes());
        chunk_tuple.extend_from_slice(chunk_data);

        // 4. insert the chunk tuple
        insert_tuple(toast_file, &chunk_tuple)?;

        chunk_seq += 1;
        offset = end;
    }

    //5. return the toast pointer
    Ok(ToastPointer {
        tag: TOAST_POINTER_TAG,
        compression: compression_flag,
        toast_value_id,
        original_size,
        stored_size,
        num_chunks: chunk_seq,
    })
}

fn allocate_toast_id(toast_file: &mut File) -> io::Result<u32> {
    // Read current ID
    toast_file.seek(SeekFrom::Start(4))?;
    let mut buf = [0u8; 4];
    toast_file.read_exact(&mut buf)?;
    let current_id = u32::from_le_bytes(buf);

    // Increment and write back
    toast_file.seek(SeekFrom::Start(4))?;
    toast_file.write_all(&(current_id + 1).to_le_bytes())?;

    Ok(current_id)
}
