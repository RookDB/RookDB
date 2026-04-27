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
    Catalog, CatalogError, ColumnDefinition, Constraint, ConstraintDefinition,
    ConstraintMetadata, ConstraintType, ConstraintViolation, DataType, Database, DefaultValue,
    Encoding, Index, IndexType, ReferentialAction, TableMetadata, TableStatistics,
    TableType, TypeCategory, TypeModifier,
    // Full catalog entry types (pg_table / pg_column mirrors)
    CatalogTable, CatalogColumn,
    // Simple schema view types for the selection executor (exported as Column / Table
    // so test helpers can use the ergonomic names).
    ColumnSchema as Column, TableSchema as Table,
};

// ── Re-exports: catalog operations ───────────────────────────────────────────
pub use catalog::{
    bootstrap_catalog, alter_table_add_column, create_database, create_table,
    drop_database, drop_table, get_columns, get_database, get_table, get_table_metadata,
    init_catalog, init_catalog_page_storage, load_catalog, lookup_type_by_name,
    register_builtin_types, show_databases, show_tables, update_table_statistics,
};

// ── Re-exports: constraint operations ────────────────────────────────────────
pub use constraints::{
    add_foreign_key_constraint, add_not_null_constraint, add_primary_key_constraint,
    add_unique_constraint, get_constraints_for_table, validate_constraints,
};

// ── Re-exports: index operations ─────────────────────────────────────────────
pub use indexes::{
    create_index, drop_index, get_indexes_for_table, index_lookup, insert_index_entry,
};

// ── Re-exports: OID counter ───────────────────────────────────────────────────
pub use oid::OidCounter;

// ── Re-exports: page manager ─────────────────────────────────────────────────
pub use page_manager::CatalogPageManager;
