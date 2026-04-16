use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::index::index_trait::{IndexKey, RecordId};
use crate::page::PAGE_SIZE;

const INDEX_MAGIC: [u8; 8] = *b"RDBIDXV1";
const INDEX_FORMAT_VERSION_V1: u16 = 1;
const INDEX_FORMAT_VERSION_V2: u16 = 2;
const INDEX_HEADER_SIZE: usize = 64;

const DATA_PAGE_HEADER_SIZE: usize = 4;
const DATA_PAGE_PAYLOAD_CAPACITY: usize = PAGE_SIZE - DATA_PAGE_HEADER_SIZE;

const PARTITION_COUNT: usize = 256;
const DIRECTORY_ENTRY_SIZE: usize = 16;
const DIRECTORY_PAGE_HEADER_SIZE: usize = 4;
const DIRECTORY_PAGE_PAYLOAD_CAPACITY: usize = PAGE_SIZE - DIRECTORY_PAGE_HEADER_SIZE;

/// Keep index files bounded to avoid accidental unbounded disk growth.
///
/// 512 MiB is enough for course-scale datasets while still preventing runaway
/// file growth that could stress memory and I/O.
pub const MAX_INDEX_FILE_SIZE_BYTES: u64 = 512 * 1024 * 1024;

fn io_invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

#[derive(Debug, Clone, Copy, Default)]
struct PartitionMeta {
    start_page: u32,
    page_count: u32,
    entry_count: u64,
}

#[derive(Debug, Clone, Copy)]
struct V2Header {
    entry_count: u64,
    partition_count: u32,
    directory_pages: u32,
    data_start_page: u32,
    data_pages: u64,
}

fn directory_pages_for_partitions(partitions: usize) -> io::Result<usize> {
    if partitions == 0 {
        return Err(io_invalid_data("partition count must be > 0"));
    }

    let pages = partitions.div_ceil(DIRECTORY_PAGE_PAYLOAD_CAPACITY / DIRECTORY_ENTRY_SIZE);
    if pages == 0 {
        return Err(io_invalid_data("computed directory page count is zero"));
    }
    Ok(pages)
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

fn parse_common_header(header: &[u8]) -> io::Result<u16> {
    if header.len() != PAGE_SIZE {
        return Err(io_invalid_data("invalid index header page length"));
    }

    if header[0..8] != INDEX_MAGIC {
        return Err(io_invalid_data("invalid index file magic"));
    }

    let version = u16::from_le_bytes([header[8], header[9]]);
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

    Ok(version)
}

fn parse_v1_header(header: &[u8]) -> io::Result<(u64, u64)> {
    let expected_entries = u64::from_le_bytes(
        header[16..24]
            .try_into()
            .map_err(|_| io_invalid_data("failed to decode v1 entry count"))?,
    );
    let data_pages = u64::from_le_bytes(
        header[24..32]
            .try_into()
            .map_err(|_| io_invalid_data("failed to decode v1 data page count"))?,
    );

    Ok((expected_entries, data_pages))
}

fn parse_v2_header(header: &[u8]) -> io::Result<V2Header> {
    let entry_count = u64::from_le_bytes(
        header[16..24]
            .try_into()
            .map_err(|_| io_invalid_data("failed to decode v2 entry count"))?,
    );
    let partition_count = u32::from_le_bytes([header[24], header[25], header[26], header[27]]);
    let directory_pages = u32::from_le_bytes([header[28], header[29], header[30], header[31]]);
    let data_start_page = u32::from_le_bytes([header[32], header[33], header[34], header[35]]);
    let data_pages = u64::from_le_bytes(
        header[40..48]
            .try_into()
            .map_err(|_| io_invalid_data("failed to decode v2 data page count"))?,
    );

    if partition_count == 0 {
        return Err(io_invalid_data("v2 partition count must be > 0"));
    }
    if directory_pages == 0 {
        return Err(io_invalid_data("v2 directory page count must be > 0"));
    }

    let expected_data_start = 1u32
        .checked_add(directory_pages)
        .ok_or_else(|| io_invalid_data("v2 data_start_page overflow"))?;
    if data_start_page != expected_data_start {
        return Err(io_invalid_data(format!(
            "invalid v2 data_start_page: expected {}, found {}",
            expected_data_start, data_start_page
        )));
    }

    Ok(V2Header {
        entry_count,
        partition_count,
        directory_pages,
        data_start_page,
        data_pages,
    })
}

fn scan_data_page_entries<F>(page: &[u8], mut on_entry: F) -> io::Result<u64>
where
    F: FnMut(IndexKey, RecordId) -> io::Result<()>,
{
    let used = u16::from_le_bytes([page[0], page[1]]) as usize;
    if used > DATA_PAGE_PAYLOAD_CAPACITY {
        return Err(io_invalid_data("data page payload usage exceeds page capacity"));
    }

    let mut count = 0u64;
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
        count += 1;
    }

    Ok(count)
}

fn read_data_page(file: &mut File, page_no: u32, page_buf: &mut [u8]) -> io::Result<()> {
    let offset = page_no as u64 * PAGE_SIZE as u64;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(page_buf)
}

fn read_directory_entries(file: &mut File, header: &V2Header) -> io::Result<Vec<PartitionMeta>> {
    let partition_count = usize::try_from(header.partition_count)
        .map_err(|_| io_invalid_data("partition count conversion overflow"))?;
    let directory_pages = usize::try_from(header.directory_pages)
        .map_err(|_| io_invalid_data("directory page count conversion overflow"))?;

    let mut page = vec![0u8; PAGE_SIZE];
    let mut metas = Vec::with_capacity(partition_count);

    for page_idx in 0..directory_pages {
        let page_no = 1u32
            .checked_add(u32::try_from(page_idx).map_err(|_| io_invalid_data("directory page index overflow"))?)
            .ok_or_else(|| io_invalid_data("directory page number overflow"))?;
        read_data_page(file, page_no, &mut page)?;

        let used = u16::from_le_bytes([page[0], page[1]]) as usize;
        if used > DIRECTORY_PAGE_PAYLOAD_CAPACITY {
            return Err(io_invalid_data("directory page payload usage exceeds capacity"));
        }
        if used % DIRECTORY_ENTRY_SIZE != 0 {
            return Err(io_invalid_data("directory page payload is not entry-size aligned"));
        }

        let mut cursor = DIRECTORY_PAGE_HEADER_SIZE;
        let end = DIRECTORY_PAGE_HEADER_SIZE + used;

        while cursor < end && metas.len() < partition_count {
            let start_page = u32::from_le_bytes(
                page[cursor..cursor + 4]
                    .try_into()
                    .map_err(|_| io_invalid_data("failed to decode directory start_page"))?,
            );
            cursor += 4;

            let page_count = u32::from_le_bytes(
                page[cursor..cursor + 4]
                    .try_into()
                    .map_err(|_| io_invalid_data("failed to decode directory page_count"))?,
            );
            cursor += 4;

            let entry_count = u64::from_le_bytes(
                page[cursor..cursor + 8]
                    .try_into()
                    .map_err(|_| io_invalid_data("failed to decode directory entry_count"))?,
            );
            cursor += 8;

            metas.push(PartitionMeta {
                start_page,
                page_count,
                entry_count,
            });
        }
    }

    if metas.len() != partition_count {
        return Err(io_invalid_data(format!(
            "directory entry count mismatch: expected {}, got {}",
            partition_count,
            metas.len()
        )));
    }

    Ok(metas)
}

fn write_directory_pages(
    file: &mut File,
    metas: &[PartitionMeta],
    directory_pages: usize,
) -> io::Result<()> {
    file.seek(SeekFrom::Start(PAGE_SIZE as u64))?;

    let mut idx = 0usize;
    for _ in 0..directory_pages {
        let mut page = vec![0u8; PAGE_SIZE];
        let mut payload_cursor = 0usize;

        while idx < metas.len() && payload_cursor + DIRECTORY_ENTRY_SIZE <= DIRECTORY_PAGE_PAYLOAD_CAPACITY {
            let m = metas[idx];
            let base = DIRECTORY_PAGE_HEADER_SIZE + payload_cursor;
            page[base..base + 4].copy_from_slice(&m.start_page.to_le_bytes());
            page[base + 4..base + 8].copy_from_slice(&m.page_count.to_le_bytes());
            page[base + 8..base + 16].copy_from_slice(&m.entry_count.to_le_bytes());
            payload_cursor += DIRECTORY_ENTRY_SIZE;
            idx += 1;
        }

        let used_u16 = u16::try_from(payload_cursor)
            .map_err(|_| io_invalid_data("directory payload usage overflow"))?;
        page[0..2].copy_from_slice(&used_u16.to_le_bytes());
        page[2..4].copy_from_slice(&0u16.to_le_bytes());

        file.write_all(&page)?;
    }

    if idx != metas.len() {
        return Err(io_invalid_data("not all directory entries were written"));
    }

    Ok(())
}

struct DataPageAppender {
    file: File,
    page: Vec<u8>,
    page_used: usize,
    next_page_no: u32,
    data_pages_written: u64,
}

impl DataPageAppender {
    fn new(mut file: File, start_page_no: u32) -> io::Result<Self> {
        file.seek(SeekFrom::Start(start_page_no as u64 * PAGE_SIZE as u64))?;
        Ok(Self {
            file,
            page: vec![0u8; PAGE_SIZE],
            page_used: 0,
            next_page_no: start_page_no,
            data_pages_written: 0,
        })
    }

    fn flush_page_if_non_empty(&mut self) -> io::Result<()> {
        if self.page_used == 0 {
            return Ok(());
        }

        let size_after_write = (self.next_page_no as u64 + 1) * PAGE_SIZE as u64;
        if size_after_write > MAX_INDEX_FILE_SIZE_BYTES {
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
        self.page.fill(0);
        self.page_used = 0;
        self.next_page_no = self
            .next_page_no
            .checked_add(1)
            .ok_or_else(|| io_invalid_data("page number overflow while writing data pages"))?;
        self.data_pages_written += 1;

        Ok(())
    }

    fn append_entry(&mut self, key: &IndexKey, rid: &RecordId) -> io::Result<()> {
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

        Ok(())
    }

    fn append_partition(&mut self, entries: &[(IndexKey, RecordId)]) -> io::Result<PartitionMeta> {
        if entries.is_empty() {
            return Ok(PartitionMeta::default());
        }

        let start_page = self.next_page_no;
        let before_pages = self.data_pages_written;

        for (key, rid) in entries {
            self.append_entry(key, rid)?;
        }
        self.flush_page_if_non_empty()?;

        let page_count = u32::try_from(self.data_pages_written - before_pages)
            .map_err(|_| io_invalid_data("partition page count overflow"))?;
        let entry_count = u64::try_from(entries.len())
            .map_err(|_| io_invalid_data("partition entry count overflow"))?;

        Ok(PartitionMeta {
            start_page,
            page_count,
            entry_count,
        })
    }

    fn data_pages_written(&self) -> u64 {
        self.data_pages_written
    }

    fn into_file(self) -> File {
        self.file
    }
}

fn validate_common_file_properties(metadata_len: u64) -> io::Result<u64> {
    if metadata_len < PAGE_SIZE as u64 {
        return Err(io_invalid_data("index file is smaller than a single page"));
    }
    if metadata_len > MAX_INDEX_FILE_SIZE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!(
                "index file exceeds max size limit of {} bytes",
                MAX_INDEX_FILE_SIZE_BYTES
            ),
        ));
    }
    if metadata_len % PAGE_SIZE as u64 != 0 {
        return Err(io_invalid_data(
            "index file length is not aligned to page size",
        ));
    }

    Ok(metadata_len / PAGE_SIZE as u64)
}

fn load_entries_stream_v1<F>(
    file: &mut File,
    expected_entries: u64,
    data_pages: u64,
    mut on_entry: F,
) -> io::Result<u64>
where
    F: FnMut(IndexKey, RecordId) -> io::Result<()>,
{
    file.seek(SeekFrom::Start(PAGE_SIZE as u64))?;

    let mut actual_entries = 0u64;
    let mut page = vec![0u8; PAGE_SIZE];

    for _ in 0..data_pages {
        file.read_exact(&mut page)?;
        actual_entries += scan_data_page_entries(&page, &mut on_entry)?;
    }

    if actual_entries != expected_entries {
        return Err(io_invalid_data(format!(
            "entry count mismatch: header={}, parsed={}",
            expected_entries, actual_entries
        )));
    }

    Ok(actual_entries)
}

fn load_entries_stream_v2<F>(file: &mut File, header: &V2Header, mut on_entry: F) -> io::Result<u64>
where
    F: FnMut(IndexKey, RecordId) -> io::Result<()>,
{
    let directory = read_directory_entries(file, header)?;
    let mut page = vec![0u8; PAGE_SIZE];

    let mut actual_entries = 0u64;
    let mut actual_data_pages = 0u64;
    let data_end_page = header.data_start_page as u64 + header.data_pages;

    for meta in directory {
        if meta.page_count == 0 {
            if meta.start_page != 0 {
                return Err(io_invalid_data(
                    "empty partition must have start_page = 0",
                ));
            }
            if meta.entry_count != 0 {
                return Err(io_invalid_data(
                    "empty partition must have entry_count = 0",
                ));
            }
            continue;
        }

        if (meta.start_page as u64) < header.data_start_page as u64 {
            return Err(io_invalid_data("partition start_page is before data section"));
        }

        let partition_end = meta.start_page as u64 + meta.page_count as u64;
        if partition_end > data_end_page {
            return Err(io_invalid_data("partition pages exceed declared data section"));
        }

        let mut partition_entries = 0u64;
        for page_no in 0..meta.page_count {
            let absolute_page = meta
                .start_page
                .checked_add(page_no)
                .ok_or_else(|| io_invalid_data("partition page number overflow"))?;
            read_data_page(file, absolute_page, &mut page)?;
            partition_entries += scan_data_page_entries(&page, &mut on_entry)?;
            actual_data_pages += 1;
        }

        if partition_entries != meta.entry_count {
            return Err(io_invalid_data(format!(
                "partition entry count mismatch: header={}, parsed={}",
                meta.entry_count, partition_entries
            )));
        }

        actual_entries += partition_entries;
    }

    if actual_data_pages != header.data_pages {
        return Err(io_invalid_data(format!(
            "data page count mismatch: header={}, parsed={}",
            header.data_pages, actual_data_pages
        )));
    }

    if actual_entries != header.entry_count {
        return Err(io_invalid_data(format!(
            "entry count mismatch: header={}, parsed={}",
            header.entry_count, actual_entries
        )));
    }

    Ok(actual_entries)
}

fn search_key_v2(file: &mut File, header: &V2Header, key: &IndexKey) -> io::Result<Vec<RecordId>> {
    let directory = read_directory_entries(file, header)?;
    let partition_count = usize::try_from(header.partition_count)
        .map_err(|_| io_invalid_data("partition count conversion overflow"))?;

    let partition_idx = (key.hash_code() as usize) % partition_count;
    let meta = directory
        .get(partition_idx)
        .copied()
        .ok_or_else(|| io_invalid_data("partition index out of bounds in directory"))?;

    if meta.page_count == 0 {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    let mut page = vec![0u8; PAGE_SIZE];

    for page_no in 0..meta.page_count {
        let absolute_page = meta
            .start_page
            .checked_add(page_no)
            .ok_or_else(|| io_invalid_data("partition page number overflow"))?;
        read_data_page(file, absolute_page, &mut page)?;
        let _ = scan_data_page_entries(&page, |entry_key, rid| {
            if &entry_key == key {
                out.push(rid);
            }
            Ok(())
        })?;
    }

    Ok(out)
}

pub fn save_entries<I>(path: &str, entries: I) -> io::Result<()>
where
    I: IntoIterator<Item = (IndexKey, RecordId)>,
{
    let directory_pages = directory_pages_for_partitions(PARTITION_COUNT)?;
    let reserved_pages = 1usize
        .checked_add(directory_pages)
        .ok_or_else(|| io_invalid_data("reserved page count overflow"))?;

    let reserved_size = reserved_pages as u64 * PAGE_SIZE as u64;
    if reserved_size > MAX_INDEX_FILE_SIZE_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::OutOfMemory,
            format!(
                "reserved index pages exceed max file size of {} bytes",
                MAX_INDEX_FILE_SIZE_BYTES
            ),
        ));
    }

    if let Some(parent) = std::path::Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }

    let mut partitioned: Vec<Vec<(IndexKey, RecordId)>> = vec![Vec::new(); PARTITION_COUNT];
    let mut entry_count = 0u64;

    for (key, rid) in entries {
        let partition_idx = (key.hash_code() as usize) % PARTITION_COUNT;
        partitioned[partition_idx].push((key, rid));
        entry_count += 1;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;

    let zero_page = vec![0u8; PAGE_SIZE];
    for _ in 0..reserved_pages {
        file.write_all(&zero_page)?;
    }

    let data_start_page = u32::try_from(reserved_pages)
        .map_err(|_| io_invalid_data("data_start_page conversion overflow"))?;
    let mut appender = DataPageAppender::new(file, data_start_page)?;

    let mut metas = Vec::with_capacity(PARTITION_COUNT);
    for part_entries in &partitioned {
        metas.push(appender.append_partition(part_entries)?);
    }

    let data_pages = appender.data_pages_written();
    let mut file = appender.into_file();

    write_directory_pages(&mut file, &metas, directory_pages)?;

    let mut header = vec![0u8; PAGE_SIZE];
    header[0..8].copy_from_slice(&INDEX_MAGIC);
    header[8..10].copy_from_slice(&INDEX_FORMAT_VERSION_V2.to_le_bytes());
    header[10..12].copy_from_slice(&(INDEX_HEADER_SIZE as u16).to_le_bytes());
    header[12..16].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
    header[16..24].copy_from_slice(&entry_count.to_le_bytes());
    header[24..28].copy_from_slice(
        &u32::try_from(PARTITION_COUNT)
            .map_err(|_| io_invalid_data("partition count conversion overflow"))?
            .to_le_bytes(),
    );
    header[28..32].copy_from_slice(
        &u32::try_from(directory_pages)
            .map_err(|_| io_invalid_data("directory page count conversion overflow"))?
            .to_le_bytes(),
    );
    header[32..36].copy_from_slice(&data_start_page.to_le_bytes());
    header[40..48].copy_from_slice(&data_pages.to_le_bytes());
    header[48..56].copy_from_slice(&MAX_INDEX_FILE_SIZE_BYTES.to_le_bytes());

    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;
    file.flush()?;

    Ok(())
}

pub fn load_entries_stream<F>(path: &str, mut on_entry: F) -> io::Result<u64>
where
    F: FnMut(IndexKey, RecordId) -> io::Result<()>,
{
    let mut file = OpenOptions::new().read(true).open(path)?;
    let total_pages_on_disk = validate_common_file_properties(file.metadata()?.len())?;

    let mut header = vec![0u8; PAGE_SIZE];
    file.read_exact(&mut header)?;

    let version = parse_common_header(&header)?;

    match version {
        INDEX_FORMAT_VERSION_V1 => {
            let (expected_entries, data_pages) = parse_v1_header(&header)?;
            if total_pages_on_disk != data_pages + 1 {
                return Err(io_invalid_data(
                    "v1 index data page count does not match file length",
                ));
            }
            load_entries_stream_v1(&mut file, expected_entries, data_pages, &mut on_entry)
        }
        INDEX_FORMAT_VERSION_V2 => {
            let v2 = parse_v2_header(&header)?;
            let expected_total_pages = 1u64 + v2.directory_pages as u64 + v2.data_pages;
            if total_pages_on_disk != expected_total_pages {
                return Err(io_invalid_data(
                    "v2 index page counts do not match file length",
                ));
            }
            load_entries_stream_v2(&mut file, &v2, &mut on_entry)
        }
        other => Err(io_invalid_data(format!(
            "unsupported index format version {}",
            other
        ))),
    }
}

/// Point-search a key directly against the persisted index file.
///
/// For v2 files this reads only the key's partition pages; for legacy v1 files
/// it falls back to streaming all pages.
pub fn search_key(path: &str, key: &IndexKey) -> io::Result<Vec<RecordId>> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let _ = validate_common_file_properties(file.metadata()?.len())?;

    let mut header = vec![0u8; PAGE_SIZE];
    file.read_exact(&mut header)?;
    let version = parse_common_header(&header)?;

    match version {
        INDEX_FORMAT_VERSION_V1 => {
            drop(file);
            let mut out = Vec::new();
            let _ = load_entries_stream(path, |entry_key, rid| {
                if &entry_key == key {
                    out.push(rid);
                }
                Ok(())
            })?;
            Ok(out)
        }
        INDEX_FORMAT_VERSION_V2 => {
            let v2 = parse_v2_header(&header)?;
            search_key_v2(&mut file, &v2, key)
        }
        other => Err(io_invalid_data(format!(
            "unsupported index format version {}",
            other
        ))),
    }
}
