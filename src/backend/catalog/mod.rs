pub mod types;
pub use types::{Catalog, Column, Database, IndexAlgorithm, IndexEntry, Table};
pub mod catalog;

pub use catalog::{
    create_database,
    create_index,
    create_secondary_index,
    create_table,
    drop_secondary_index,
    drop_index,
    init_catalog,
    list_secondary_indices,
    list_indexes,
    load_catalog,
    save_catalog,
    show_databases,
    show_tables,
};