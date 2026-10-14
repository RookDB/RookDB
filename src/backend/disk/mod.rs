pub mod disk_manager;

pub use disk_manager::file_identity_from_file;
pub use disk_manager::{
    read_all_pages, read_header_page, read_page, update_header_page, write_page,
};
