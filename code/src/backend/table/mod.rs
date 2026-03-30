pub mod table_file;

pub use table_file::page_count;
pub use table_file::Table;
pub use table_file::TABLE_HEADER_SIZE;
pub use table_file::{read_dead_tuple_count, write_dead_tuple_count, increment_dead_tuple_count};
pub use table_file::file_identity_from_file;