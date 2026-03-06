//! Defines the physical on-disk layout used by all backend components.

// Root directory for all storage
pub const DATA_DIR: &str = "database";

// Catalog metadata directory
pub const GLOBAL_DIR: &str = "database/global";

// Legacy global catalog file (JSON format)
pub const CATALOG_FILE: &str = "database/global/catalog.json";

// Root directory for all databases
pub const DATABASE_DIR: &str = "database/base";

// Directory for specific database
pub const TABLE_DIR_TEMPLATE: &str = "database/base/{database}";

// File path for specific table
pub const TABLE_FILE_TEMPLATE: &str = "database/base/{database}/{table}.dat";

// ──────────────────────────────────────────────────────────────────────────────
// Page-based catalog storage paths (new design)
// ──────────────────────────────────────────────────────────────────────────────

/// Directory that holds all system catalog .dat files
pub const CATALOG_PAGES_DIR: &str = "database/global/catalog_pages";

/// System catalog: databases
pub const PG_DATABASE_FILE: &str = "database/global/catalog_pages/pg_database.dat";
/// System catalog: tables
pub const PG_TABLE_FILE: &str = "database/global/catalog_pages/pg_table.dat";
/// System catalog: columns
pub const PG_COLUMN_FILE: &str = "database/global/catalog_pages/pg_column.dat";
/// System catalog: constraints
pub const PG_CONSTRAINT_FILE: &str = "database/global/catalog_pages/pg_constraint.dat";
/// System catalog: indexes
pub const PG_INDEX_FILE: &str = "database/global/catalog_pages/pg_index.dat";
/// System catalog: data types
pub const PG_TYPE_FILE: &str = "database/global/catalog_pages/pg_type.dat";

/// Persistent OID counter
pub const OID_COUNTER_FILE: &str = "database/global/pg_oid_counter.dat";

/// Directory for user-table index files inside a specific database
pub const INDEX_DIR_TEMPLATE: &str = "database/base/{database}/indexes";

/// File path for a specific index
pub const INDEX_FILE_TEMPLATE: &str = "database/base/{database}/indexes/{index}.idx";

// ──────────────────────────────────────────────────────────────────────────────
// Well-known system OID ranges
// ──────────────────────────────────────────────────────────────────────────────

/// First OID reserved for built-in types and system objects
pub const SYSTEM_OID_START: u32 = 1;
/// First OID available for user objects
pub const USER_OID_START: u32 = 10_000;

// ──────────────────────────────────────────────────────────────────────────────
// Built-in type OIDs (mirror PostgreSQL conventions loosely)
// ──────────────────────────────────────────────────────────────────────────────
pub const OID_TYPE_INT: u32 = 1;
pub const OID_TYPE_BIGINT: u32 = 2;
pub const OID_TYPE_FLOAT: u32 = 3;
pub const OID_TYPE_DOUBLE: u32 = 4;
pub const OID_TYPE_BOOL: u32 = 5;
pub const OID_TYPE_TEXT: u32 = 6;
pub const OID_TYPE_VARCHAR: u32 = 7;
pub const OID_TYPE_DATE: u32 = 8;
pub const OID_TYPE_TIMESTAMP: u32 = 9;
pub const OID_TYPE_BYTES: u32 = 10;

/// OID of the built-in "system" database
pub const SYSTEM_DB_OID: u32 = 1;
