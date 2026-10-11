// Page size in bytes (8 KB)
pub const PAGE_SIZE: usize = 8192;

// Page header size: stores lower & upper pointers
pub const PAGE_HEADER_SIZE: u32 = 8;

// Size of one item slot
pub const ITEM_ID_SIZE: u32 = 8;

// Slot flag bit: tuple is soft-deleted
pub const SLOT_FLAG_DELETED: u16 = 0b0000_0000_0000_0001;

// Page size in bytes (8 KB)
pub mod page_lock;

// Represents a single database page
pub struct Page {
    // Raw page bytes
    pub data: Vec<u8>,
}

impl Default for Page {
    fn default() -> Self {
        Self::new()
    }
}

/// One slot directory entry: `[offset: u32][length: u16][flags: u16]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slot {
    pub offset: u32,
    pub length: u16,
    pub flags: u16,
}

impl Slot {
    /// The slot holds no row: soft-deleted, or zeroed by `HeapManager::delete_tuple`.
    pub fn is_dead(&self) -> bool {
        self.flags & SLOT_FLAG_DELETED != 0 || (self.offset == 0 && self.length == 0)
    }
}

impl Page {
    // Create an empty page
    pub fn new() -> Self {
        Self {
            data: vec![0; PAGE_SIZE],
        }
    }

    /// End of the slot directory.
    pub fn lower(&self) -> u32 {
        u32::from_le_bytes(self.data[0..4].try_into().unwrap())
    }

    /// Start of the tuple data area.
    pub fn upper(&self) -> u32 {
        u32::from_le_bytes(self.data[4..8].try_into().unwrap())
    }

    fn set_lower(&mut self, lower: u32) {
        self.data[0..4].copy_from_slice(&lower.to_le_bytes());
    }

    fn set_upper(&mut self, upper: u32) {
        self.data[4..8].copy_from_slice(&upper.to_le_bytes());
    }

    /// Number of slot directory entries, live or dead.
    pub fn slot_count(&self) -> u32 {
        self.lower()
            .min(PAGE_SIZE as u32)
            .saturating_sub(PAGE_HEADER_SIZE)
            / ITEM_ID_SIZE
    }

    /// Slot `slot_id`, or `None` past the end of the directory.
    pub fn slot(&self, slot_id: u32) -> Option<Slot> {
        if slot_id >= self.slot_count() {
            return None;
        }
        let base = (PAGE_HEADER_SIZE + slot_id * ITEM_ID_SIZE) as usize;
        Some(Slot {
            offset: u32::from_le_bytes(self.data[base..base + 4].try_into().unwrap()),
            length: u16::from_le_bytes(self.data[base + 4..base + 6].try_into().unwrap()),
            flags: u16::from_le_bytes(self.data[base + 6..base + 8].try_into().unwrap()),
        })
    }

    pub fn set_slot(&mut self, slot_id: u32, slot: Slot) {
        let base = (PAGE_HEADER_SIZE + slot_id * ITEM_ID_SIZE) as usize;
        self.data[base..base + 4].copy_from_slice(&slot.offset.to_le_bytes());
        self.data[base + 4..base + 6].copy_from_slice(&slot.length.to_le_bytes());
        self.data[base + 6..base + 8].copy_from_slice(&slot.flags.to_le_bytes());
    }

    /// Bytes of the live row in `slot_id`; `None` for a dead, missing or
    /// out-of-bounds slot.
    pub fn tuple(&self, slot_id: u32) -> Option<&[u8]> {
        let slot = self.slot(slot_id)?;
        if slot.is_dead() {
            return None;
        }
        let start = slot.offset as usize;
        self.data.get(start..start + slot.length as usize)
    }

    /// Every live row on the page with its slot id.
    pub fn tuples(&self) -> impl Iterator<Item = (u32, &[u8])> {
        (0..self.slot_count()).filter_map(|i| self.tuple(i).map(|t| (i, t)))
    }

    /// Soft-delete the row in `slot_id`. Returns false if it was not live.
    pub fn mark_deleted(&mut self, slot_id: u32) -> bool {
        match self.slot(slot_id) {
            Some(mut slot) if !slot.is_dead() => {
                slot.flags |= SLOT_FLAG_DELETED;
                self.set_slot(slot_id, slot);
                true
            }
            _ => false,
        }
    }

    /// Replace the live row in `slot_id` with `bytes` when they fit in its
    /// current space. Returns false, changing nothing, when they do not.
    pub fn overwrite(&mut self, slot_id: u32, bytes: &[u8]) -> bool {
        let Some(mut slot) = self.slot(slot_id) else {
            return false;
        };
        let start = slot.offset as usize;
        let end = start + slot.length as usize;
        if slot.is_dead() || bytes.len() > slot.length as usize || end > self.data.len() {
            return false;
        }
        self.data[start..start + bytes.len()].copy_from_slice(bytes);
        self.data[start + bytes.len()..end].fill(0);
        slot.length = bytes.len() as u16;
        self.set_slot(slot_id, slot);
        true
    }

    /// Store `data` as a new row, reusing the first dead slot if there is
    /// one, and return its slot id. A reused slot's old bytes stay on the
    /// page until compaction.
    pub fn insert(&mut self, data: &[u8]) -> std::io::Result<u32> {
        let free = page_free_space(self)?;
        let reused = (0..self.slot_count()).find(|&i| self.slot(i).is_some_and(|s| s.is_dead()));
        let required = data.len() as u32 + if reused.is_some() { 0 } else { ITEM_ID_SIZE };
        if free < required {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Insufficient free space in page: {} < {}", free, required),
            ));
        }

        let start = self.upper() - data.len() as u32;
        self.data[start as usize..start as usize + data.len()].copy_from_slice(data);
        self.set_upper(start);

        let slot_id = match reused {
            Some(id) => id,
            None => {
                let id = self.slot_count();
                self.set_lower(self.lower() + ITEM_ID_SIZE);
                id
            }
        };
        self.set_slot(
            slot_id,
            Slot {
                offset: start,
                length: data.len() as u16,
                flags: 0,
            },
        );
        Ok(slot_id)
    }

    /// Whether compaction would free space: the page has a dead slot, or
    /// its data area holds bytes no live row uses (left behind when a
    /// reused slot or a shrunken row gave up its old space).
    pub fn has_garbage(&self) -> bool {
        let mut live_rows = 0u32;
        let mut live_bytes = 0u32;
        for (_, t) in self.tuples() {
            live_rows += 1;
            live_bytes += t.len() as u32;
        }
        live_rows < self.slot_count()
            || (PAGE_SIZE as u32).saturating_sub(self.upper()) > live_bytes
    }

    /// Rewrite the page with only its live rows, packed and renumbered from
    /// slot 0 in their current order.
    pub fn compact(&mut self) {
        let live: Vec<Vec<u8>> = self.tuples().map(|(_, t)| t.to_vec()).collect();
        self.data.fill(0);
        init_page(self);
        for row in &live {
            self.insert(row)
                .expect("rows that fit before compaction fit after it");
        }
    }
}

// Initialize page header pointers
pub fn init_page(page: &mut Page) {
    // Lower starts after header
    page.data[0..4].copy_from_slice(&PAGE_HEADER_SIZE.to_le_bytes());

    // Upper starts at page end
    page.data[4..8].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
}

// Return available free space in the page
pub fn page_free_space(page: &Page) -> std::io::Result<u32> {
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

    if lower < PAGE_HEADER_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Invalid page lower pointer: {} < {}",
                lower, PAGE_HEADER_SIZE
            ),
        ));
    }

    if lower > PAGE_SIZE as u32 || upper > PAGE_SIZE as u32 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Page pointers out of bounds: lower={}, upper={}, page_size={}",
                lower, upper, PAGE_SIZE
            ),
        ));
    }

    if upper < lower {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Corrupted page pointers: upper ({}) < lower ({})",
                upper, lower
            ),
        ));
    }

    Ok(upper - lower)
}

/// Get the number of tuples currently stored in a page.
/// This is calculated as (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE.
///
/// # Errors
/// Returns error if reading the page header fails.
pub fn get_tuple_count(page: &Page) -> std::io::Result<u32> {
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());

    if lower < PAGE_HEADER_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Invalid page lower pointer: {} < {}",
                lower, PAGE_HEADER_SIZE
            ),
        ));
    }

    if lower > PAGE_SIZE as u32 || upper > PAGE_SIZE as u32 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Page pointers out of bounds: lower={}, upper={}, page_size={}",
                lower, upper, PAGE_SIZE
            ),
        ));
    }

    if upper < lower {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Corrupted page pointers: upper ({}) < lower ({})",
                upper, lower
            ),
        ));
    }

    if !(lower - PAGE_HEADER_SIZE).is_multiple_of(ITEM_ID_SIZE) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Invalid slot array alignment: lower={}, header={}, item_id_size={}",
                lower, PAGE_HEADER_SIZE, ITEM_ID_SIZE
            ),
        ));
    }

    let tuple_count = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;
    log::trace!(
        "[page::get_tuple_count] Computing tuple_count: ({} - {}) / {} = {}",
        lower,
        PAGE_HEADER_SIZE,
        ITEM_ID_SIZE,
        tuple_count
    );

    Ok(tuple_count)
}

/// Get slot entry (offset, length) for a given slot ID.
///
/// Uses the canonical 3-field slot format `[offset: u32, length: u16, flags: u16]`.
/// Returns `(0, 0)` for soft-deleted slots (those with `SLOT_FLAG_DELETED` set)
/// so that callers can uniformly treat `(0, 0)` as "skip this slot".
///
/// # Arguments
/// * `page`    - The page to read from
/// * `slot_id` - Zero-based slot index
///
/// # Returns
/// `(offset, length)` of the live tuple data, or `(0, 0)` for deleted slots.
///
/// # Errors
/// Returns an error only if `slot_id` is out of bounds or the page is structurally invalid.
pub fn get_slot_entry(page: &Page, slot_id: u32) -> std::io::Result<(u32, u32)> {
    let tuple_count = get_tuple_count(page)?;
    let slot = page.slot(slot_id).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Slot ID {} out of bounds (tuple_count={})",
                slot_id, tuple_count
            ),
        )
    })?;

    if slot.is_dead() {
        return Ok((0, 0));
    }

    let length = slot.length as u32;
    if slot.offset as u64 + length as u64 > PAGE_SIZE as u64 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Corrupted slot entry bounds: offset={}, length={}, page_size={}",
                slot.offset, length, PAGE_SIZE
            ),
        ));
    }

    Ok((slot.offset, length))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_free_space_detects_corrupted_pointers() {
        let mut page = Page::new();
        init_page(&mut page);

        page.data[0..4].copy_from_slice(&100u32.to_le_bytes());
        page.data[4..8].copy_from_slice(&50u32.to_le_bytes());

        let result = page_free_space(&page);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_tuple_count_detects_invalid_alignment() {
        let mut page = Page::new();
        init_page(&mut page);

        page.data[0..4].copy_from_slice(&9u32.to_le_bytes());

        let result = get_tuple_count(&page);
        assert!(result.is_err());
    }

    fn page_with(rows: &[&[u8]]) -> Page {
        let mut page = Page::new();
        init_page(&mut page);
        for row in rows {
            page.insert(row).unwrap();
        }
        page
    }

    #[test]
    fn test_insert_and_read_back() {
        let page = page_with(&[b"alpha", b"beta"]);
        assert_eq!(page.slot_count(), 2);
        assert_eq!(page.tuple(0), Some(&b"alpha"[..]));
        assert_eq!(page.tuple(1), Some(&b"beta"[..]));
        assert_eq!(page.tuple(2), None);
        assert_eq!(
            page_free_space(&page).unwrap(),
            PAGE_SIZE as u32 - PAGE_HEADER_SIZE - 2 * ITEM_ID_SIZE - 9
        );
    }

    #[test]
    fn test_mark_deleted_hides_row_once() {
        let mut page = page_with(&[b"a", b"b", b"c"]);
        assert!(page.mark_deleted(1));
        assert!(!page.mark_deleted(1));
        assert!(!page.mark_deleted(7));
        assert_eq!(page.tuple(1), None);
        let live: Vec<u32> = page.tuples().map(|(i, _)| i).collect();
        assert_eq!(live, vec![0, 2]);
    }

    #[test]
    fn test_insert_reuses_deleted_slot() {
        let mut page = page_with(&[b"a", b"b"]);
        page.mark_deleted(0);
        assert_eq!(page.insert(b"new").unwrap(), 0);
        assert_eq!(page.insert(b"more").unwrap(), 2);
        assert_eq!(page.tuple(0), Some(&b"new"[..]));
    }

    #[test]
    fn test_overwrite_only_when_it_fits() {
        let mut page = page_with(&[b"hello"]);
        assert!(page.overwrite(0, b"hi"));
        assert_eq!(page.tuple(0), Some(&b"hi"[..]));
        assert!(!page.overwrite(0, b"too long"));
        assert_eq!(page.tuple(0), Some(&b"hi"[..]));
        page.mark_deleted(0);
        assert!(!page.overwrite(0, b"x"));
    }

    #[test]
    fn test_has_garbage_after_slot_reuse() {
        let mut page = page_with(&[b"one", b"two"]);
        assert!(!page.has_garbage());
        page.mark_deleted(0);
        assert!(page.has_garbage());
        // The reused slot hides the deleted row, but its bytes remain.
        page.insert(b"new").unwrap();
        assert!(page.has_garbage());
        page.compact();
        assert!(!page.has_garbage());
    }

    #[test]
    fn test_compact_keeps_live_rows_in_order() {
        let mut page = page_with(&[b"one", b"two", b"three"]);
        page.mark_deleted(1);
        page.compact();
        let rows: Vec<(u32, Vec<u8>)> = page.tuples().map(|(i, t)| (i, t.to_vec())).collect();
        assert_eq!(rows, vec![(0, b"one".to_vec()), (1, b"three".to_vec())]);
        assert_eq!(
            page_free_space(&page).unwrap(),
            PAGE_SIZE as u32 - PAGE_HEADER_SIZE - 2 * ITEM_ID_SIZE - 8
        );
    }

    #[test]
    fn test_get_slot_entry_detects_out_of_bounds_tuple() {
        let mut page = Page::new();
        init_page(&mut page);

        let lower = PAGE_HEADER_SIZE + ITEM_ID_SIZE;
        page.data[0..4].copy_from_slice(&lower.to_le_bytes());

        let slot_offset = PAGE_HEADER_SIZE as usize;
        page.data[slot_offset..slot_offset + 4]
            .copy_from_slice(&(PAGE_SIZE as u32 - 4).to_le_bytes());
        page.data[slot_offset + 4..slot_offset + 8].copy_from_slice(&16u32.to_le_bytes());

        let result = get_slot_entry(&page, 0);
        assert!(result.is_err());
    }
}
