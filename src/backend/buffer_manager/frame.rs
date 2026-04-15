use crate::backend::page::Page;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PageId {
    pub table_name: String,
    pub page_number: u32,
}

pub struct FrameMetadata {
    pub page_id: Option<PageId>, // which page currently resides in this frame
    pub dirty: bool,             // whether page modified
    pub pin_count: u32,          // number of active users
    pub usage_count: u32,        // used by clock policy
    pub last_used: u64,          // timestamp for LRU
}

impl FrameMetadata {
    pub fn new() -> Self {
        Self {
            page_id: None,
            dirty: false,
            pin_count: 0,
            usage_count: 0,
            last_used: 0,
        }
    }
}

pub struct BufferFrame {
    pub page: Page,
    pub metadata: FrameMetadata,
}

impl BufferFrame {
    pub fn new(page: Page) -> Self {
        Self {
            page,
            metadata: FrameMetadata::new(),
        }
    }
}