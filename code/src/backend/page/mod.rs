pub const PAGE_SIZE: usize = 8192;
pub const PAGE_HEADER_SIZE: u32 = 8;
pub const ITEM_ID_SIZE: u32 = 8;

pub struct Page {
    pub data: Vec<u8>,
}

impl Page {
    pub fn new() -> Self {
        Self {
            data: vec![0; PAGE_SIZE],
        }
    }
}

pub fn init_page(page: &mut Page) {
    let lower = PAGE_HEADER_SIZE.to_le_bytes();
    page.data[0..4].copy_from_slice(&lower);

    let upper = (PAGE_SIZE as u32).to_le_bytes();
    page.data[4..8].copy_from_slice(&upper);
}

pub fn page_free_space(page: &Page) -> std::io::Result<u32> {
    let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
    let upper = u32::from_le_bytes(page.data[4..8].try_into().unwrap());
    Ok(upper - lower)
}
