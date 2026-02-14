//! Defines the physical on-disk layout used by all backend components.

// Root directory for all storage
pub const DATA_DIR: &str = "database";

// Catalog metadata directory
pub const GLOBAL_DIR: &str = "database/global";

// Global catalog file
pub const CATALOG_FILE: &str = "database/global/catalog.json";

// Root directory for all databases
pub const DATABASE_DIR: &str = "database/base";

// Directory for specific database
pub const TABLE_DIR_TEMPLATE: &str = "database/base/{database}";

// File path for specific table
pub const TABLE_FILE_TEMPLATE: &str = "database/base/{database}/{table}.dat";
