pub mod buffer_pool;
pub mod buffer_manager;
pub mod frame;
pub mod policy;
pub mod lru;
pub mod clock;
pub mod stats;
pub const PAGE_SIZE: usize = 8192;
pub const BUFFER_SIZE: usize = 128 * 1024 * 1024; // example: 64MB
pub const RESERVED_FRAMES: usize = 129; // 0–128 reserved

pub use buffer_manager::BufferManager;
pub use buffer_pool::BufferPool;
pub use frame::PageId;
pub use policy::ReplacementPolicy;
pub use lru::LRUPolicy;
pub use clock::ClockPolicy;