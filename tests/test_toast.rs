use std::env;
use std::fs::{File, OpenOptions, remove_file};
use std::path::PathBuf;

use storage_manager::heap::init_table;
use storage_manager::toast::compression::{compress, decompress};
use storage_manager::toast::toast_reader::detoast_value;
use storage_manager::toast::toast_writer::toast_value;
use storage_manager::toast::{TOAST_CHUNK_SIZE, TOAST_POINTER_SIZE, TOAST_POINTER_TAG, ToastPointer};

fn open_toast_file(name: &str) -> (PathBuf, File) {
    let mut path = env::temp_dir();
    path.push(name);
    let _ = remove_file(&path);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .expect("failed to create toast file");

    init_table(&mut file).expect("failed to init toast table");
    (path, file)
}

#[test]
fn test_toast_pointer_roundtrip() {
    let pointer = ToastPointer {
        tag: TOAST_POINTER_TAG,
        compression: 0x01,
        toast_value_id: 0xdeadbeef,
        original_size: 4096,
        stored_size: 1234,
        num_chunks: 3,
    };

    let bytes = pointer.to_bytes();
    assert_eq!(bytes.len(), TOAST_POINTER_SIZE);
    assert_eq!(bytes[0], TOAST_POINTER_TAG);
    assert_eq!(bytes[1], 0x01);

    let decoded = ToastPointer::from_bytes(&bytes);
    assert_eq!(decoded, pointer);
}

#[test]
fn test_compression_roundtrip() {
    // Highly compressible payload so we exercise the LZ4 path meaningfully.
    let original = vec![b'A'; 4096];
    let compressed = compress(&original);

    assert!(
        compressed.len() < original.len(),
        "expected LZ4 to shrink a 4KB run of 'A'; got {} >= {}",
        compressed.len(),
        original.len()
    );

    let decompressed = decompress(&compressed).expect("decompress should succeed");
    assert_eq!(decompressed, original);
}

#[test]
fn test_decompress_invalid_data_errors() {
    // Random bytes are not a valid LZ4-size-prepended frame.
    let garbage = vec![0xff_u8; 8];
    assert!(decompress(&garbage).is_err());
}

#[test]
fn test_toast_id_is_allocated_and_increments() {
    let (path, mut file) = open_toast_file("test_toast_id_alloc.tbl");

    let value = b"first".to_vec();
    let p1 = toast_value(&mut file, &value, false).expect("toast first value");
    let p2 = toast_value(&mut file, &value, false).expect("toast second value");

    // First id starts at 0 (init_table zeros bytes 4..8) and increments per call.
    assert_eq!(p1.toast_value_id, 0);
    assert_eq!(p2.toast_value_id, 1);
    assert_eq!(p1.tag, TOAST_POINTER_TAG);

    let _ = remove_file(path);
}

#[test]
fn test_toast_and_detoast_uncompressed_small() {
    let (path, mut file) = open_toast_file("test_toast_uncompressed_small.tbl");

    let value = b"hello, toast!".to_vec();
    let pointer = toast_value(&mut file, &value, false).expect("toast value");

    assert_eq!(pointer.compression, 0x00);
    assert_eq!(pointer.original_size as usize, value.len());
    assert_eq!(pointer.stored_size as usize, value.len());
    assert_eq!(pointer.num_chunks, 1);

    let detoasted = detoast_value(&mut file, &pointer).expect("detoast value");
    assert_eq!(detoasted, value);

    let _ = remove_file(path);
}

#[test]
fn test_toast_and_detoast_uncompressed_multi_chunk() {
    let (path, mut file) = open_toast_file("test_toast_uncompressed_multi_chunk.tbl");

    // 5000 bytes / 2000 byte chunks = 3 chunks (2000 + 2000 + 1000).
    let value: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
    let pointer = toast_value(&mut file, &value, false).expect("toast value");

    let expected_chunks = value.len().div_ceil(TOAST_CHUNK_SIZE);
    assert_eq!(pointer.num_chunks as usize, expected_chunks);
    assert_eq!(pointer.original_size as usize, value.len());
    assert_eq!(pointer.stored_size as usize, value.len());

    let detoasted = detoast_value(&mut file, &pointer).expect("detoast value");
    assert_eq!(detoasted.len(), value.len());
    assert_eq!(detoasted, value);

    let _ = remove_file(path);
}

#[test]
fn test_toast_and_detoast_compressed() {
    let (path, mut file) = open_toast_file("test_toast_compressed.tbl");

    // Repetitive payload so LZ4 can shrink it noticeably.
    let value: Vec<u8> = b"the quick brown fox jumps over the lazy dog. "
        .iter()
        .copied()
        .cycle()
        .take(8000)
        .collect();
    let pointer = toast_value(&mut file, &value, true).expect("toast value");

    assert_eq!(pointer.compression, 0x01);
    assert_eq!(pointer.original_size as usize, value.len());
    assert!(
        (pointer.stored_size as usize) < value.len(),
        "compressed size {} should be smaller than original {}",
        pointer.stored_size,
        value.len()
    );

    let detoasted = detoast_value(&mut file, &pointer).expect("detoast value");
    assert_eq!(detoasted, value);

    let _ = remove_file(path);
}

#[test]
fn test_two_toasted_values_remain_distinct() {
    let (path, mut file) = open_toast_file("test_toast_two_values.tbl");

    // Two distinct payloads, each spanning multiple chunks, written into the
    // same toast file. detoast_value must return the bytes for the requested
    // id only, even though the chunks share pages.
    let value_a: Vec<u8> = (0..3000u32).map(|i| (i % 200) as u8).collect();
    let value_b: Vec<u8> = (0..3500u32).map(|i| ((i * 7) % 200) as u8).collect();

    let p_a = toast_value(&mut file, &value_a, false).expect("toast A");
    let p_b = toast_value(&mut file, &value_b, false).expect("toast B");

    assert_ne!(p_a.toast_value_id, p_b.toast_value_id);

    let got_a = detoast_value(&mut file, &p_a).expect("detoast A");
    let got_b = detoast_value(&mut file, &p_b).expect("detoast B");

    assert_eq!(got_a, value_a);
    assert_eq!(got_b, value_b);

    let _ = remove_file(path);
}
