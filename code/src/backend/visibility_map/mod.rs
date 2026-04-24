pub mod vm;

pub use vm::{vm_set_page, vm_clear_page, vm_is_visible, vm_visible_count, vm_evict};
