// ── Sub-modules ───────────────────────────────────────────────────────────────
pub mod types;
pub mod oid;
pub mod cache;
pub mod serialize;
pub mod page_manager;
pub mod constraints;
pub mod indexes;
pub mod catalog;

// ── Re-exports: core data types ──────────────────────────────────────────────
pub use types::{
    Catalog, CatalogError, Column, ColumnDefinition, Constraint, ConstraintDefinition,
    ConstraintMetadata, ConstraintType, ConstraintViolation, DataType, Database, DefaultValue,
    Encoding, Index, IndexType, ReferentialAction, Table, TableMetadata, TableStatistics,
    TableType, TypeCategory, TypeModifier,
};

// ── Re-exports: catalog operations ───────────────────────────────────────────
pub use catalog::{
    bootstrap_catalog, create_database, create_database_enhanced, create_table,
    create_table_enhanced, drop_database, drop_table, alter_table_add_column,
    get_table_metadata, init_catalog, init_catalog_page_storage, load_catalog,
    lookup_type_by_name, register_builtin_types, save_catalog, show_databases, show_tables,
};

// ── Re-exports: constraint operations ────────────────────────────────────────
pub use constraints::{
    add_foreign_key_constraint, add_not_null_constraint, add_primary_key_constraint,
    add_unique_constraint, get_constraints_for_table, validate_constraints,
};

// ── Re-exports: index operations ─────────────────────────────────────────────
pub use indexes::{create_index, drop_index, get_indexes_for_table};

// ── Re-exports: OID counter ───────────────────────────────────────────────────
pub use oid::OidCounter;

// ── Re-exports: page manager ─────────────────────────────────────────────────
pub use page_manager::CatalogPageManager;
