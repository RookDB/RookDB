use std::io;

/// Compress data using LZ4 block compression.
/// The compressed output is prepended with the original size for decompression.
pub fn compress(data: &[u8]) -> Vec<u8> {
    lz4_flex::compress_prepend_size(data)
}

/// Decompress LZ4 block-compressed data.
/// Expects the compressed data to be prepended with the original size.
pub fn decompress(data: &[u8]) -> io::Result<Vec<u8>> {
    lz4_flex::decompress_size_prepended(data)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
}
