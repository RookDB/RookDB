pub mod types;
pub use types::{Catalog, Column, Database, IndexAlgorithm, IndexEntry, Table};
pub mod catalog;

pub use catalog::{
    create_database,
    create_index,
    create_table,
    drop_index,
    init_catalog,
    list_indexes,
    load_catalog,
    save_catalog,
    show_databases,
    show_tables,
};