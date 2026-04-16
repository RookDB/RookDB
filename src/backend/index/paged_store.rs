use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::index::index_trait::{IndexKey, RecordId};
use crate::page::PAGE_SIZE;

const INDEX_MAGIC: [u8; 8] = *b"RDBIDXV1";
const INDEX_FORMAT_VERSION: u16 = 1;
const INDEX_HEADER_SIZE: usize = 64;

const DATA_PAGE_HEADER_SIZE: usize = 4;
const DATA_PAGE_PAYLOAD_CAPACITY: usize = PAGE_SIZE - DATA_PAGE_HEADER_SIZE;

/// Keep index files bounded to avoid accidental unbounded disk growth.
///
/// 64 MiB is enough for course-scale datasets while still preventing runaway
/// file growth that could stress memory and I/O.
// pub const MAX_INDEX_FILE_SIZE_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_INDEX_FILE_SIZE_BYTES: u64 = 512 * 1024;

fn io_invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn encode_entry(key: &IndexKey, rid: &RecordId) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();

    match key {
        IndexKey::Int(v) => {
            out.push(0);
            out.extend_from_slice(&v.to_le_bytes());
        }
        IndexKey::Float(v) => {
            out.push(1);
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        IndexKey::Text(s) => {
            out.push(2);
            let bytes = s.as_bytes();
            let len_u32 = u32::try_from(bytes.len())
                .map_err(|_| io_invalid_data("text key too large for index entry"))?;
            out.extend_from_slice(&len_u32.to_le_bytes());
            out.extend_from_slice(bytes);
        }
    }

    out.extend_from_slice(&rid.page_no.to_le_bytes());
    out.extend_from_slice(&rid.item_id.to_le_bytes());

    Ok(out)
}

fn decode_entry(bytes: &[u8]) -> io::Result<(IndexKey, RecordId)> {
    if bytes.is_empty() {
        return Err(io_invalid_data("empty entry payload"));
    }

    let tag = bytes[0];
    let mut cursor = 1usize;

    let key = match tag {
        0 => {
            if cursor + 8 > bytes.len() {
                return Err(io_invalid_data("invalid INT key payload length"));
            }
            let mut raw = [0u8; 8];
            raw.copy_from_slice(&bytes[cursor..cursor + 8]);
            cursor += 8;
            IndexKey::Int(i64::from_le_bytes(raw))
        }
        1 => {
            if cursor + 8 > bytes.len() {
                return Err(io_invalid_data("invalid FLOAT key payload length"));
            }
            let mut raw = [0u8; 8];
            raw.copy_from_slice(&bytes[cursor..cursor + 8]);
            cursor += 8;
            IndexKey::Float(f64::from_bits(u64::from_le_bytes(raw)))
        }
        2 => {
            if cursor + 4 > bytes.len() {
                return Err(io_invalid_data("invalid TEXT key length header"));
            }
            let mut len_raw = [0u8; 4];
            len_raw.copy_from_slice(&bytes[cursor..cursor + 4]);
            cursor += 4;
            let text_len = u32::from_le_bytes(len_raw) as usize;

            if cursor + text_len > bytes.len() {
                return Err(io_invalid_data("invalid TEXT key payload length"));
            }

            let text = String::from_utf8(bytes[cursor..cursor + text_len].to_vec())
                .map_err(|_| io_invalid_data("TEXT key contains invalid UTF-8"))?;
            cursor += text_len;
            IndexKey::Text(text)
        }
        _ => {
            return Err(io_invalid_data(format!(
                "unsupported index key tag {}",
                tag
            )));
        }
    };

    if cursor + 8 != bytes.len() {
        return Err(io_invalid_data("entry payload has trailing or missing bytes"));
    }

    let mut page_no_raw = [0u8; 4];
    page_no_raw.copy_from_slice(&bytes[cursor..cursor + 4]);
    cursor += 4;

    let mut item_id_raw = [0u8; 4];
    item_id_raw.copy_from_slice(&bytes[cursor..cursor + 4]);

    Ok((
        key,
        RecordId::new(u32::from_le_bytes(page_no_raw), u32::from_le_bytes(item_id_raw)),
    ))
}

struct PagedIndexWriter {
    file: File,
    page: Vec<u8>,
    page_used: usize,
    entry_count: u64,
    data_pages: u64,
}

impl PagedIndexWriter {
    fn new(path: &str) -> io::Result<Self> {
        if let Some(parent) = std::path::Path::new(path).parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        // Reserve page 0 for the file header.
        file.write_all(&vec![0u8; PAGE_SIZE])?;

        Ok(Self {
            file,
            page: vec![0u8; PAGE_SIZE],
            page_used: 0,
            entry_count: 0,
            data_pages: 0,
        })
    }

    fn current_size_bytes(&self) -> u64 {
        (1u64 + self.data_pages) * PAGE_SIZE as u64
    }

    fn flush_page_if_non_empty(&mut self) -> io::Result<()> {
        if self.page_used == 0 {
            return Ok(());
        }

        if self.current_size_bytes() + PAGE_SIZE as u64 > MAX_INDEX_FILE_SIZE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!(
                    "index file would exceed max size limit of {} bytes",
                    MAX_INDEX_FILE_SIZE_BYTES
                ),
            ));
        }

        let used_u16 = u16::try_from(self.page_used)
            .map_err(|_| io_invalid_data("data page usage overflow"))?;

        self.page[0..2].copy_from_slice(&used_u16.to_le_bytes());
        self.page[2..4].copy_from_slice(&0u16.to_le_bytes());

        self.file.write_all(&self.page)?;
        self.data_pages += 1;

        self.page.fill(0);
        self.page_used = 0;

        Ok(())
    }

    fn append(&mut self, key: &IndexKey, rid: &RecordId) -> io::Result<()> {
        let payload = encode_entry(key, rid)?;
        let payload_len = payload.len();
        let encoded_len = 4 + payload_len;

        if encoded_len > DATA_PAGE_PAYLOAD_CAPACITY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "single index entry too large ({} bytes), max per-entry is {} bytes",
                    encoded_len,
                    DATA_PAGE_PAYLOAD_CAPACITY
                ),
            ));
        }

        if self.page_used + encoded_len > DATA_PAGE_PAYLOAD_CAPACITY {
            self.flush_page_if_non_empty()?;
        }

        let entry_len_u32 = u32::try_from(payload_len)
            .map_err(|_| io_invalid_data("entry payload length overflow"))?;

        let start = DATA_PAGE_HEADER_SIZE + self.page_used;
        self.page[start..start + 4].copy_from_slice(&entry_len_u32.to_le_bytes());
        self.page[start + 4..start + 4 + payload_len].copy_from_slice(&payload);

        self.page_used += encoded_len;
        self.entry_count += 1;

        Ok(())
    }

    fn finish(mut self) -> io::Result<()> {
        self.flush_page_if_non_empty()?;

        let mut header = vec![0u8; PAGE_SIZE];
        header[0..8].copy_from_slice(&INDEX_MAGIC);
        header[8..10].copy_from_slice(&INDEX_FORMAT_VERSION.to_le_bytes());
        header[10..12].copy_from_slice(&(INDEX_HEADER_SIZE as u16).to_le_bytes());
        header[12..16].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
        header[16..24].copy_from_slice(&self.entry_count.to_le_bytes());
        header[24..32].copy_from_slice(&self.data_pages.to_le_bytes());
        header[32..40].copy_from_slice(&MAX_INDEX_FILE_SIZE_BYTES.to_le_bytes());

        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header)?;
        self.file.flush()?;

        Ok(())
    }
}

pub fn save_entries<I>(path: &str, entries: I) -> io::Result<()>
where
    I: IntoIterator<Item = (IndexKey, RecordId)>,
{
    let mut writer = PagedIndexWriter::new(path)?;
    for (key, rid) in entries {
        writer.append(&key, &rid)?;
    }
    writer.finish()
}

pub fn load_entries_stream<F>(path: &str, mut on_entry: F) -> io::Result<u64>
where
    F: FnMut(IndexKey, RecordId) -> io::Result<()>,
{
    let mut file = OpenOptions::new().read(true).open(path)?;

    let metadata = file.metadata()?;
    if metadata.len() < PAGE_SIZE as u64 {
        return Err(io_invalid_data("index file is smaller than a single page"));
    }
    if metadata.len() > MAX_INDEX_FILE_SIZE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!(
                "index file exceeds max size limit of {} bytes",
                MAX_INDEX_FILE_SIZE_BYTES
            ),
        ));
    }
    if metadata.len() % PAGE_SIZE as u64 != 0 {
        return Err(io_invalid_data(
            "index file length is not aligned to page size",
        ));
    }

    let mut header = vec![0u8; PAGE_SIZE];
    file.read_exact(&mut header)?;

    if header[0..8] != INDEX_MAGIC {
        return Err(io_invalid_data("invalid index file magic"));
    }

    let version = u16::from_le_bytes([header[8], header[9]]);
    if version != INDEX_FORMAT_VERSION {
        return Err(io_invalid_data(format!(
            "unsupported index format version {}",
            version
        )));
    }

    let header_size = u16::from_le_bytes([header[10], header[11]]) as usize;
    if header_size > PAGE_SIZE {
        return Err(io_invalid_data("invalid index header size"));
    }

    let stored_page_size = u32::from_le_bytes([header[12], header[13], header[14], header[15]]) as usize;
    if stored_page_size != PAGE_SIZE {
        return Err(io_invalid_data(format!(
            "index page size mismatch: expected {}, found {}",
            PAGE_SIZE, stored_page_size
        )));
    }

    let expected_entries = u64::from_le_bytes(
        header[16..24]
            .try_into()
            .map_err(|_| io_invalid_data("failed to decode expected entry count"))?,
    );
    let data_pages = u64::from_le_bytes(
        header[24..32]
            .try_into()
            .map_err(|_| io_invalid_data("failed to decode data page count"))?,
    );

    let total_pages_on_disk = metadata.len() / PAGE_SIZE as u64;
    if total_pages_on_disk != data_pages + 1 {
        return Err(io_invalid_data(
            "index data page count does not match file length",
        ));
    }

    let mut actual_entries = 0u64;
    let mut page = vec![0u8; PAGE_SIZE];

    for _ in 0..data_pages {
        file.read_exact(&mut page)?;

        let used = u16::from_le_bytes([page[0], page[1]]) as usize;
        if used > DATA_PAGE_PAYLOAD_CAPACITY {
            return Err(io_invalid_data("data page payload usage exceeds page capacity"));
        }

        let mut cursor = DATA_PAGE_HEADER_SIZE;
        let end = DATA_PAGE_HEADER_SIZE + used;

        while cursor < end {
            if cursor + 4 > end {
                return Err(io_invalid_data("truncated entry length in data page"));
            }

            let entry_len = u32::from_le_bytes(
                page[cursor..cursor + 4]
                    .try_into()
                    .map_err(|_| io_invalid_data("failed to decode entry length"))?,
            ) as usize;
            cursor += 4;

            if cursor + entry_len > end {
                return Err(io_invalid_data("truncated entry payload in data page"));
            }

            let (key, rid) = decode_entry(&page[cursor..cursor + entry_len])?;
            on_entry(key, rid)?;

            cursor += entry_len;
            actual_entries += 1;
        }
    }

    if actual_entries != expected_entries {
        return Err(io_invalid_data(format!(
            "entry count mismatch: header={}, parsed={}",
            expected_entries, actual_entries
        )));
    }

    Ok(actual_entries)
}