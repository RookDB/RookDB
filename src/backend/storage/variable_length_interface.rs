//! Trait-based interface for persistent variable-length tuple storage.
//!
//! Provides the [`PersistentVariableLengthStore`] trait, which abstracts the
//! four CRUD operations (insert / read_all / update / delete) over a single
//! table, and the concrete [`TableStore`] implementation that delegates to the
//! free functions in [`variable_length_api`].
//!
//! # Re-export
//! `storage/mod.rs` re-exports `PersistentVariableLengthStore` at the
//! `storage_manager::storage` level.
//!
//! # Example
//! ```rust,ignore
//! use storage_manager::storage::PersistentVariableLengthStore;
//! use storage_manager::storage::variable_length_interface::TableStore;
//! use storage_manager::catalog::data_type::{DataType, Value};
//!
//! let schema = vec![
//!     ("id".to_string(), DataType::Int32),
//!     ("data".to_string(), DataType::Blob),
//! ];
//! let mut store = TableStore::new("mydb", "mytable", schema);
//! let result = store.insert(&[Value::Int32(1), Value::Blob(vec![0xDE, 0xAD])]).unwrap();
//! println!("Inserted {} bytes.", result.tuple_bytes);
//! ```

use std::io;

use crate::backend::catalog::data_type::{DataType, Value};
use crate::backend::catalog::types::Catalog;
use crate::backend::storage::variable_length_api::{
    DeleteResult, InsertResult, TupleRecord, UpdateResult,
    delete_tuple_api, insert_tuple_api, read_tuple_by_location_api, read_tuples_api,
    update_tuple_api,
};

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// A storage interface for a single table that supports CRUD operations on
/// tuples containing variable-length BLOB and ARRAY columns (with TOAST).
///
/// Implementations must handle:
/// - Automatic TOAST promotion for oversized values (> 8 KiB)
/// - Schema-aware TOAST cleanup on update / delete
/// - Transparent detoasting on read
pub trait PersistentVariableLengthStore {
    /// Insert a new tuple into the table.
    ///
    /// Column values must be provided in schema order.
    /// Oversized BLOB / ARRAY values are automatically pushed to TOAST storage.
    fn insert(&mut self, values: &[Value]) -> io::Result<InsertResult>;

    /// Scan and decode every live tuple in the table.
    ///
    /// TOAST-backed values are transparently fetched and reconstructed.
    fn read_all(&self, catalog: &Catalog) -> io::Result<Vec<TupleRecord>>;

    /// Replace the tuple identified by `(page_num, slot_index)`.
    ///
    /// Old TOAST values are freed; new ones are created if `new_values`
    /// contains oversized columns.
    fn update(
        &mut self,
        page_num: u32,
        slot_index: u32,
        new_values: &[Value],
    ) -> io::Result<UpdateResult>;

    /// Delete the tuple at `(page_num, slot_index)`.
    ///
    /// Associated TOAST values are freed automatically.
    fn delete(&mut self, page_num: u32, slot_index: u32) -> io::Result<DeleteResult>;

    /// Fetch a single tuple by its heap location.
    ///
    /// Returns `None` if no live tuple exists at `(page_num, slot_index)`.
    fn get(
        &self,
        catalog: &Catalog,
        page_num: u32,
        slot_index: u32,
    ) -> io::Result<Option<TupleRecord>>;
}

// ---------------------------------------------------------------------------
// Concrete implementation: TableStore
// ---------------------------------------------------------------------------

/// Concrete [`PersistentVariableLengthStore`] bound to a specific table.
///
/// Holds the target database/table names and the column schema, then
/// delegates all operations to the free functions in [`variable_length_api`].
pub struct TableStore {
    /// Database name, matching the directory under `database/base/`
    pub db: String,
    /// Table name, matching the `.dat` file under the database directory
    pub table: String,
    /// Column schema: `(column_name, DataType)` pairs in declaration order
    pub schema: Vec<(String, DataType)>,
}

impl TableStore {
    /// Create a new `TableStore` bound to `db.table` with the given schema.
    ///
    /// The underlying `.dat` file must already exist (created by `init_table`).
    pub fn new(db: impl Into<String>, table: impl Into<String>, schema: Vec<(String, DataType)>) -> Self {
        TableStore {
            db: db.into(),
            table: table.into(),
            schema,
        }
    }

    /// Convenience accessor for the database name.
    pub fn db_name(&self) -> &str {
        &self.db
    }

    /// Convenience accessor for the table name.
    pub fn table_name(&self) -> &str {
        &self.table
    }

    /// Return the number of columns defined by this store's schema.
    pub fn column_count(&self) -> usize {
        self.schema.len()
    }
}

impl PersistentVariableLengthStore for TableStore {
    fn insert(&mut self, values: &[Value]) -> io::Result<InsertResult> {
        insert_tuple_api(&self.db, &self.table, values, &self.schema)
    }

    fn read_all(&self, catalog: &Catalog) -> io::Result<Vec<TupleRecord>> {
        read_tuples_api(catalog, &self.db, &self.table, &self.schema)
    }

    fn update(
        &mut self,
        page_num: u32,
        slot_index: u32,
        new_values: &[Value],
    ) -> io::Result<UpdateResult> {
        update_tuple_api(
            &self.db,
            &self.table,
            page_num,
            slot_index,
            new_values,
            &self.schema,
        )
    }

    fn delete(&mut self, page_num: u32, slot_index: u32) -> io::Result<DeleteResult> {
        delete_tuple_api(&self.db, &self.table, page_num, slot_index, &self.schema)
    }

    fn get(
        &self,
        catalog: &Catalog,
        page_num: u32,
        slot_index: u32,
    ) -> io::Result<Option<TupleRecord>> {
        read_tuple_by_location_api(
            catalog,
            &self.db,
            &self.table,
            &self.schema,
            page_num,
            slot_index,
        )
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::data_type::{DataType, Value};

    #[test]
    fn test_table_store_construction() {
        let schema = vec![
            ("id".to_string(), DataType::Int32),
            ("payload".to_string(), DataType::Blob),
        ];
        let store = TableStore::new("test", "table1", schema);
        assert_eq!(store.db_name(), "test");
        assert_eq!(store.table_name(), "table1");
        assert_eq!(store.column_count(), 2);
    }

    #[test]
    fn test_table_store_schema_types() {
        let schema = vec![
            ("id".to_string(), DataType::Int32),
            ("active".to_string(), DataType::Boolean),
            ("name".to_string(), DataType::Text),
            ("data".to_string(), DataType::Blob),
            (
                "tags".to_string(),
                DataType::Array {
                    element_type: Box::new(DataType::Text),
                },
            ),
        ];
        let store = TableStore::new("db", "tbl", schema);
        assert_eq!(store.column_count(), 5);

        // Variable-length columns
        let var_count = store
            .schema
            .iter()
            .filter(|(_, dt)| dt.is_variable_length())
            .count();
        assert_eq!(var_count, 3, "Text, Blob, Array<Text> are variable-length");
    }

    #[test]
    fn test_table_store_insert_int_blob() {
        let schema = vec![
            ("id".to_string(), DataType::Int32),
            ("blob_val".to_string(), DataType::Blob),
        ];

        let mut store = TableStore::new("test", "table1", schema);
        
        let blob_data = 0x1ab2341ce23u64.to_be_bytes().to_vec();
        let values = &[
            Value::Int32(96),
            Value::Blob(blob_data),
        ];
        
        // Use the interface to store the value
        let _result = store.insert(values).unwrap();
    }

    #[test]
    fn test_table_store_read_tuple() {
        let schema = vec![
            ("id".to_string(), DataType::Int32),
            ("data".to_string(), DataType::Blob),
        ];

        let store = TableStore::new("test", "table1", schema);
        let catalog = crate::backend::catalog::load_catalog();
        let results = store.read_all(&catalog).unwrap();

        for tuple in &results {
             println!("Page: {}, Slot: {}, Values: {:?}", tuple.page_num, tuple.slot_index, tuple.values);
        
            // assert_eq!(tuple.values.len(), 2);
        }
    }
}
