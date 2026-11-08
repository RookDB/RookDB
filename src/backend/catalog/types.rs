//! Defines core catalog data structures used to represent databases,
//! tables, and columns in memory and on disk.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::types::{DataType, DataValue};

fn default_nullable() -> bool {
    true
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct Constraints {
    pub not_null: bool,
    pub unique: bool,
    pub default: Option<DataValue>,
    pub check: Option<String>,
}

/// Represents a column within a table.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    #[serde(default = "default_nullable")]
    pub nullable: bool,
    #[serde(default)]
    pub constraints: Constraints,
}

impl Column {
    pub fn new(name: String, data_type: DataType) -> Self {
        Self {
            name,
            data_type,
            nullable: true,
            constraints: Constraints::default(),
        }
    }
}

/// Represents a table schema.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Table {
    pub columns: Vec<Column>,
}

/// A stored view definition.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ViewDef {
    /// The SELECT query that defines the view, serialised as a JSON string.
    /// Stored as text so we can round-trip through the catalog.
    pub query_json: String,
}

/// Represents a database containing multiple tables.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Database {
    pub tables: HashMap<String, Table>,
    /// Named views stored in this database.
    ///
    /// Populated by the DDL stage.
    #[serde(default)]
    pub views: HashMap<String, ViewDef>,
}

/// Represents the top-level catalog holding all databases.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Catalog {
    pub databases: HashMap<String, Database>,
    /// The engine of the data directory this catalog describes.
    #[serde(skip, default = "crate::backend::engine::Engine::current_dir")]
    engine: std::sync::Arc<crate::backend::engine::Engine>,
    /// The engine's metadata generation when this catalog was made. A
    /// catalog loaded from the system tables is made before they are read,
    /// so it is at least as new as this.
    #[serde(skip)]
    generation: u64,
}

impl Catalog {
    /// An empty catalog for the data directory of `engine`.
    pub fn new(engine: std::sync::Arc<crate::backend::engine::Engine>) -> Self {
        Catalog {
            databases: HashMap::new(),
            generation: engine.metadata_generation(),
            engine,
        }
    }

    /// `databases` in the data directory of `engine`.
    pub fn with_databases(
        engine: std::sync::Arc<crate::backend::engine::Engine>,
        databases: HashMap<String, Database>,
    ) -> Self {
        Catalog {
            databases,
            generation: engine.metadata_generation(),
            engine,
        }
    }

    /// The engine of this catalog's data directory.
    pub fn engine(&self) -> &std::sync::Arc<crate::backend::engine::Engine> {
        &self.engine
    }

    /// The engine's metadata generation when this catalog was made.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
}

/// Catalogs are equal when they describe the same schema.
impl PartialEq for Catalog {
    fn eq(&self, other: &Self) -> bool {
        self.databases == other.databases
    }
}

impl Eq for Catalog {}
