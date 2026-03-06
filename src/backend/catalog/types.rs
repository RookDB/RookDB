//! Core catalog data structures for RookDB's self-hosting catalog system.
//! Mirrors PostgreSQL's system catalog architecture with page-based storage.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::catalog::cache::CatalogCache;

// ─────────────────────────────────────────────────────────────
// 1. TYPE SYSTEM
// ─────────────────────────────────────────────────────────────

/// Category of a data type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TypeCategory {
    Numeric,
    String,
    DateTime,
    Boolean,
    Binary,
}

/// Metadata about a single data type (mirrors pg_type)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataType {
    pub type_oid: u32,
    /// Canonical name, e.g. "INT", "VARCHAR", "BOOL"
    pub type_name: String,
    pub type_category: TypeCategory,
    /// Fixed byte length, or -1 for variable-length types
    pub type_length: i16,
    /// Alignment requirement in bytes (1, 2, 4, or 8)
    pub type_align: u8,
    pub is_builtin: bool,
}

impl DataType {
    // ── Convenience constructors for built-in types ──────────────

    pub fn int() -> Self {
        use crate::layout::OID_TYPE_INT;
        DataType { type_oid: OID_TYPE_INT, type_name: "INT".into(), type_category: TypeCategory::Numeric, type_length: 4, type_align: 4, is_builtin: true }
    }
    pub fn bigint() -> Self {
        use crate::layout::OID_TYPE_BIGINT;
        DataType { type_oid: OID_TYPE_BIGINT, type_name: "BIGINT".into(), type_category: TypeCategory::Numeric, type_length: 8, type_align: 8, is_builtin: true }
    }
    pub fn float() -> Self {
        use crate::layout::OID_TYPE_FLOAT;
        DataType { type_oid: OID_TYPE_FLOAT, type_name: "FLOAT".into(), type_category: TypeCategory::Numeric, type_length: 4, type_align: 4, is_builtin: true }
    }
    pub fn double() -> Self {
        use crate::layout::OID_TYPE_DOUBLE;
        DataType { type_oid: OID_TYPE_DOUBLE, type_name: "DOUBLE".into(), type_category: TypeCategory::Numeric, type_length: 8, type_align: 8, is_builtin: true }
    }
    pub fn bool_type() -> Self {
        use crate::layout::OID_TYPE_BOOL;
        DataType { type_oid: OID_TYPE_BOOL, type_name: "BOOL".into(), type_category: TypeCategory::Boolean, type_length: 1, type_align: 1, is_builtin: true }
    }
    pub fn text() -> Self {
        use crate::layout::OID_TYPE_TEXT;
        DataType { type_oid: OID_TYPE_TEXT, type_name: "TEXT".into(), type_category: TypeCategory::String, type_length: -1, type_align: 1, is_builtin: true }
    }
    pub fn varchar(max_len: u16) -> Self {
        use crate::layout::OID_TYPE_VARCHAR;
        DataType { type_oid: OID_TYPE_VARCHAR, type_name: format!("VARCHAR({})", max_len), type_category: TypeCategory::String, type_length: -1, type_align: 1, is_builtin: true }
    }
    pub fn date() -> Self {
        use crate::layout::OID_TYPE_DATE;
        DataType { type_oid: OID_TYPE_DATE, type_name: "DATE".into(), type_category: TypeCategory::DateTime, type_length: 4, type_align: 4, is_builtin: true }
    }
    pub fn timestamp() -> Self {
        use crate::layout::OID_TYPE_TIMESTAMP;
        DataType { type_oid: OID_TYPE_TIMESTAMP, type_name: "TIMESTAMP".into(), type_category: TypeCategory::DateTime, type_length: 8, type_align: 8, is_builtin: true }
    }
    pub fn bytes() -> Self {
        use crate::layout::OID_TYPE_BYTES;
        DataType { type_oid: OID_TYPE_BYTES, type_name: "BYTES".into(), type_category: TypeCategory::Binary, type_length: -1, type_align: 1, is_builtin: true }
    }

    /// Resolve a type name string (case-insensitive) to a DataType.
    /// Returns None when the name is not a known built-in.
    pub fn from_name(name: &str) -> Option<Self> {
        let upper = name.to_uppercase();
        // Handle VARCHAR(n) / VARCHAR
        if upper.starts_with("VARCHAR") {
            let max = if let Some(inner) = upper.strip_prefix("VARCHAR(").and_then(|s| s.strip_suffix(')')) {
                inner.parse::<u16>().unwrap_or(255)
            } else {
                255
            };
            return Some(DataType::varchar(max));
        }
        match upper.as_str() {
            "INT" | "INTEGER" | "INT32"  => Some(DataType::int()),
            "BIGINT" | "INT64"           => Some(DataType::bigint()),
            "FLOAT" | "REAL" | "FLOAT32" => Some(DataType::float()),
            "DOUBLE" | "FLOAT64"         => Some(DataType::double()),
            "BOOL" | "BOOLEAN"           => Some(DataType::bool_type()),
            "TEXT" | "STRING"            => Some(DataType::text()),
            "DATE"                        => Some(DataType::date()),
            "TIMESTAMP"                  => Some(DataType::timestamp()),
            "BYTES" | "BYTEA" | "BLOB"   => Some(DataType::bytes()),
            _                             => None,
        }
    }

    /// Returns all built-in types in a canonical list
    pub fn all_builtins() -> Vec<DataType> {
        vec![
            DataType::int(), DataType::bigint(), DataType::float(), DataType::double(),
            DataType::bool_type(), DataType::text(), DataType::varchar(255),
            DataType::date(), DataType::timestamp(), DataType::bytes(),
        ]
    }
}

/// Optional modifier that qualifies a type (e.g. VARCHAR length)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TypeModifier {
    VarcharLen(u16),
    Precision { precision: u8, scale: u8 },
}

/// Possible default expressions for a column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DefaultValue {
    Integer(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Str(String),
    Boolean(bool),
    Null,
    CurrentTimestamp,
}

// ─────────────────────────────────────────────────────────────
// 2. COLUMN
// ─────────────────────────────────────────────────────────────

/// A column within a table (mirrors pg_column)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub column_oid: u32,
    pub name: String,
    /// 1-based position within the table
    pub column_position: u16,
    pub data_type: DataType,
    pub type_modifier: Option<TypeModifier>,
    pub is_nullable: bool,
    pub default_value: Option<DefaultValue>,
    /// OIDs of constraints that apply to this column
    pub constraints: Vec<u32>,
}

/// Declarative column specification used in DDL (CREATE TABLE / ALTER TABLE)
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    /// Type name as written in the SQL, e.g. "INT", "VARCHAR(64)"
    pub type_name: String,
    pub type_modifier: Option<u16>,
    pub is_nullable: bool,
    pub default_value: Option<DefaultValue>,
}

// ─────────────────────────────────────────────────────────────
// 3. CONSTRAINT SYSTEM
// ─────────────────────────────────────────────────────────────

/// Referential action for FK ON DELETE / ON UPDATE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReferentialAction {
    NoAction,
    Cascade,
    SetNull,
    Restrict,
}

impl ReferentialAction {
    pub fn to_u8(&self) -> u8 {
        match self { Self::NoAction => 0, Self::Cascade => 1, Self::SetNull => 2, Self::Restrict => 3 }
    }
    pub fn from_u8(v: u8) -> Self {
        match v { 1 => Self::Cascade, 2 => Self::SetNull, 3 => Self::Restrict, _ => Self::NoAction }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstraintType {
    PrimaryKey,
    ForeignKey,
    Unique,
    NotNull,
    Check,
}

impl ConstraintType {
    pub fn to_u8(&self) -> u8 {
        match self { Self::PrimaryKey => 1, Self::ForeignKey => 2, Self::Unique => 3, Self::NotNull => 4, Self::Check => 5 }
    }
    pub fn from_u8(v: u8) -> Option<Self> {
        match v { 1 => Some(Self::PrimaryKey), 2 => Some(Self::ForeignKey), 3 => Some(Self::Unique), 4 => Some(Self::NotNull), 5 => Some(Self::Check), _ => None }
    }
}

/// Constraint-type-specific metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstraintMetadata {
    PrimaryKey { index_oid: u32 },
    ForeignKey {
        referenced_table_oid: u32,
        referenced_column_oids: Vec<u32>,
        on_delete: ReferentialAction,
        on_update: ReferentialAction,
    },
    Unique { index_oid: u32 },
    NotNull,
    Check { check_expression: String },
}

/// A constraint entry (mirrors pg_constraint)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Constraint {
    pub constraint_oid: u32,
    pub constraint_name: String,
    pub constraint_type: ConstraintType,
    pub table_oid: u32,
    pub column_oids: Vec<u32>,
    pub metadata: ConstraintMetadata,
    pub is_deferrable: bool,
}

/// Declarative constraint specification used in DDL
#[derive(Debug, Clone)]
pub enum ConstraintDefinition {
    PrimaryKey { columns: Vec<String>, name: Option<String> },
    ForeignKey {
        columns: Vec<String>,
        referenced_table: String,
        referenced_columns: Vec<String>,
        on_delete: ReferentialAction,
        on_update: ReferentialAction,
        name: Option<String>,
    },
    Unique { columns: Vec<String>, name: Option<String> },
    NotNull { column: String },
    Check { expression: String, name: Option<String> },
}

/// Violation produced when a constraint is broken during INSERT/UPDATE
#[derive(Debug)]
pub enum ConstraintViolation {
    NotNullViolation { column: String },
    UniqueViolation   { constraint: String },
    ForeignKeyViolation { constraint: String },
    CheckViolation    { constraint: String },
}

impl std::fmt::Display for ConstraintViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotNullViolation   { column }     => write!(f, "NOT NULL violation on column '{}'", column),
            Self::UniqueViolation    { constraint } => write!(f, "UNIQUE violation on constraint '{}'", constraint),
            Self::ForeignKeyViolation{ constraint } => write!(f, "FOREIGN KEY violation on constraint '{}'", constraint),
            Self::CheckViolation     { constraint } => write!(f, "CHECK violation on constraint '{}'", constraint),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// 4. INDEX METADATA
// ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
}

impl IndexType {
    pub fn to_u8(&self) -> u8 { match self { Self::BTree => 1, Self::Hash => 2 } }
    pub fn from_u8(v: u8) -> Self { if v == 2 { Self::Hash } else { Self::BTree } }
}

/// An index entry (mirrors pg_index)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Index {
    pub index_oid: u32,
    pub index_name: String,
    pub table_oid: u32,
    pub index_type: IndexType,
    pub column_oids: Vec<u32>,
    pub is_unique: bool,
    pub is_primary: bool,
    pub index_pages: u32,
}

// ─────────────────────────────────────────────────────────────
// 5. TABLE
// ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableType {
    UserTable,
    SystemCatalog,
}

/// Runtime statistics about a table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableStatistics {
    pub row_count: u64,
    pub page_count: u32,
    pub created_at: u64,
    pub last_modified: u64,
}

impl Default for TableStatistics {
    fn default() -> Self {
        TableStatistics { row_count: 0, page_count: 0, created_at: 0, last_modified: 0 }
    }
}

/// A table entry (mirrors pg_table)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub table_oid: u32,
    pub table_name: String,
    pub db_oid: u32,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
    /// OIDs of indexes that cover this table
    pub indexes: Vec<u32>,
    pub table_type: TableType,
    pub statistics: TableStatistics,
}

/// Flattened view returned by catalog queries
#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub table_oid: u32,
    pub table_name: String,
    pub db_oid: u32,
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
    pub indexes: Vec<Index>,
    pub statistics: TableStatistics,
}

// ─────────────────────────────────────────────────────────────
// 6. DATABASE
// ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Encoding {
    UTF8,
    ASCII,
}

impl Encoding {
    pub fn to_u8(&self) -> u8 { match self { Self::UTF8 => 1, Self::ASCII => 2 } }
    pub fn from_u8(v: u8) -> Self { if v == 2 { Self::ASCII } else { Self::UTF8 } }
}

/// A database entry (mirrors pg_database)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    pub db_oid: u32,
    pub db_name: String,
    pub tables: HashMap<String, Table>,
    pub owner: String,
    pub encoding: Encoding,
    pub created_at: u64,
}

// ─────────────────────────────────────────────────────────────
// 7. CATALOG
// ─────────────────────────────────────────────────────────────

/// Top-level catalog: databases in memory + infrastructure fields
/// (infrastructure fields are skipped during JSON serialisation for
///  the legacy catalog.json file – they are re-initialised at load time).
#[derive(Debug, Serialize, Deserialize)]
pub struct Catalog {
    pub databases: HashMap<String, Database>,

    #[serde(skip, default)]
    pub oid_counter: u32,

    #[serde(skip, default)]
    pub bootstrap_mode: bool,

    /// Set to true once the page backend has been enabled
    #[serde(skip, default)]
    pub page_backend_active: bool,

    /// In-memory LRU cache – invalidated on every DDL operation.
    /// Populated lazily on reads; never serialised.
    #[serde(skip, default = "CatalogCache::default_instance")]
    pub cache: CatalogCache,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            databases: HashMap::new(),
            oid_counter: crate::layout::USER_OID_START,
            bootstrap_mode: false,
            page_backend_active: false,
            cache: CatalogCache::default_instance(),
        }
    }

    /// Allocate a fresh OID.
    ///
    /// When the page backend is active the new `next_oid` is written directly
    /// to `pg_oid_counter.dat` so it survives a restart.  In legacy JSON mode
    /// the counter is captured as part of the catalog.json snapshot.
    pub fn alloc_oid(&mut self) -> u32 {
        let oid = self.oid_counter;
        self.oid_counter += 1;
        // Persist whenever the page backend is live so OIDs are never reused.
        if self.page_backend_active {
            let path = crate::layout::OID_COUNTER_FILE;
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true).write(true).truncate(false).open(path)
            {
                use std::io::{Seek, SeekFrom, Write};
                let _ = f.seek(SeekFrom::Start(0));
                let _ = f.write_all(&self.oid_counter.to_le_bytes());
            }
        }
        oid
    }
}

// ─────────────────────────────────────────────────────────────
// 8. ERROR TYPES
// ─────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum CatalogError {
    DatabaseNotFound(String),
    DatabaseAlreadyExists(String),
    TableNotFound(String),
    TableAlreadyExists(String),
    ColumnNotFound(String),
    TypeNotFound(String),
    IndexNotFound(String),
    ConstraintNotFound(String),
    AlreadyHasPrimaryKey,
    ReferencedKeyMissing,
    ColumnCountMismatch,
    TypeMismatch { column: String },
    ForeignKeyDependency(String),
    InvalidOperation(String),
    IoError(std::io::Error),
}

impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DatabaseNotFound(s)   => write!(f, "Database '{}' not found", s),
            Self::DatabaseAlreadyExists(s) => write!(f, "Database '{}' already exists", s),
            Self::TableNotFound(s)      => write!(f, "Table '{}' not found", s),
            Self::TableAlreadyExists(s) => write!(f, "Table '{}' already exists", s),
            Self::ColumnNotFound(s)     => write!(f, "Column '{}' not found", s),
            Self::TypeNotFound(s)       => write!(f, "Type '{}' not found", s),
            Self::IndexNotFound(s)      => write!(f, "Index '{}' not found", s),
            Self::ConstraintNotFound(s) => write!(f, "Constraint '{}' not found", s),
            Self::AlreadyHasPrimaryKey  => write!(f, "Table already has a primary key"),
            Self::ReferencedKeyMissing  => write!(f, "Referenced columns are not covered by a PK or UNIQUE constraint"),
            Self::ColumnCountMismatch   => write!(f, "Referencing and referenced column counts differ"),
            Self::TypeMismatch { column } => write!(f, "Type mismatch on column '{}'", column),
            Self::ForeignKeyDependency(s) => write!(f, "Cannot drop: table has foreign key dependents ({})", s),
            Self::InvalidOperation(s)   => write!(f, "Invalid operation: {}", s),
            Self::IoError(e)            => write!(f, "I/O error: {}", e),
        }
    }
}

impl From<std::io::Error> for CatalogError {
    fn from(e: std::io::Error) -> Self { CatalogError::IoError(e) }
}
