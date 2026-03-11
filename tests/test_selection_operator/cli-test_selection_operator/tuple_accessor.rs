// Simulates how the Access Operator streams tuples.
// This is the bridge between storage and query execution.

use std::fs::File;
use std::io::{self, Read};

// Streams tuples one by one, like a sequential scan.
pub struct TupleStream {
    tuples: Vec<Vec<u8>>,
    index: usize,
}

impl TupleStream {
    // Create a stream from a bunch of tuples
    pub fn new(tuples: Vec<Vec<u8>>) -> Self {
        Self { tuples, index: 0 }
    }

    // Load tuples from a binary file.
    // File format is just: length, tuple bytes, length, tuple bytes, ...
    pub fn from_file(path: &str) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut tuples = Vec::new();

        loop {
            let mut length_buf = [0u8; 4];

            match file.read_exact(&mut length_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    break; // reached end of file
                }
                Err(e) => return Err(e),
            }

            let tuple_length = u32::from_le_bytes(length_buf) as usize;

            // Quick sanity check - if length looks wrong, file might be corrupted
            if tuple_length == 0 || tuple_length > 10_000 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid tuple length: {}", tuple_length),
                ));
            }

            let mut tuple = vec![0u8; tuple_length];
            file.read_exact(&mut tuple)?;

            tuples.push(tuple);
        }

        Ok(Self { tuples, index: 0 })
    }

    // Get the next tuple, or None if we've hit the end
    pub fn next_tuple(&mut self) -> Option<Vec<u8>> {
        if self.index >= self.tuples.len() {
            return None;
        }

        let tuple = self.tuples[self.index].clone();
        self.index += 1;

        Some(tuple)
    }

    /// Resets the stream to the beginning.
    pub fn reset(&mut self) {
        self.index = 0;
    }

    /// Returns the total number of tuples in the stream.
    pub fn count(&self) -> usize {
        self.tuples.len()
    }

    /// Returns whether more tuples are available.
    pub fn has_next(&self) -> bool {
        self.index < self.tuples.len()
    }
}