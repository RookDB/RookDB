//! Defines the physical on-disk layout used by all backend components.

// Root directory for all storage
pub const DATA_DIR: &str = "database";

// Catalog metadata directory
pub const GLOBAL_DIR: &str = "database/global";

// Global catalog file
pub const CATALOG_FILE: &str = "database/global/catalog.json";

// Root directory for all databases
pub const DATABASE_DIR: &str = "database/base";

/// Directory for system catalog heap tables
pub const SYSTEM_DIR: &str = "database/system";

/// System table file names
pub const SYS_DATABASES_FILE: &str = "database/system/databases.dat";
pub const SYS_TABLES_FILE: &str = "database/system/tables.dat";
pub const SYS_COLUMNS_FILE: &str = "database/system/columns.dat";
pub const SYS_CONSTRAINTS_FILE: &str = "database/system/constraints.dat";
pub const SYS_INDEXES_FILE: &str = "database/system/indexes.dat";
