//! Index management – create and drop B-tree / Hash index metadata.
//!
//! Full B-tree traversal is left for the indexing milestone; this module
//! manages the *metadata* in pg_index and creates a stub index file on disk.

use std::fs;
use std::path::Path;

use crate::catalog::types::{Catalog, CatalogError, Index, IndexType};
use crate::catalog::page_manager::{CatalogPageManager, CAT_INDEX, CAT_CONSTRAINT};
use crate::catalog::serialize::{deserialize_constraint_tuple, deserialize_index_tuple, serialize_index_tuple};
use crate::layout::{INDEX_DIR_TEMPLATE, INDEX_FILE_TEMPLATE};

// ─────────────────────────────────────────────────────────────
// Helper: resolve db_name for a table OID
// ─────────────────────────────────────────────────────────────

fn db_name_for_table(catalog: &Catalog, table_oid: u32) -> Option<String> {
    for db in catalog.databases.values() {
        for t in db.tables.values() {
            if t.table_oid == table_oid {
                return Some(db.db_name.clone());
            }
        }
    }
    None
}

// ─────────────────────────────────────────────────────────────
// create_index
// ─────────────────────────────────────────────────────────────

/// Create a B-tree index on `column_oids` for `table_oid`.
///
/// Steps:
///  1. Validate that column OIDs belong to the table.
///  2. Generate an index name if not provided.
///  3. Create stub index file on disk.
///  4. Insert a record into pg_index.
///  5. Update the in-memory table.indexes list.
///
/// Returns the new `index_oid`.
pub fn create_index(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    table_oid: u32,
    column_oids: Vec<u32>,
    is_unique: bool,
    is_primary: bool,
    index_name: Option<String>,
) -> Result<u32, CatalogError> {
    let db_name = db_name_for_table(catalog, table_oid)
        .ok_or_else(|| CatalogError::TableNotFound(table_oid.to_string()))?;

    let name = index_name.unwrap_or_else(|| {
        let col_part = column_oids.iter().map(|o| o.to_string()).collect::<Vec<_>>().join("_");
        format!("idx_{}_{}", table_oid, col_part)
    });

    // Ensure indexes directory exists
    let idx_dir = INDEX_DIR_TEMPLATE.replace("{database}", &db_name);
    if !Path::new(&idx_dir).exists() {
        fs::create_dir_all(&idx_dir)?;
    }

    // Create stub index file
    let idx_file = INDEX_FILE_TEMPLATE
        .replace("{database}", &db_name)
        .replace("{index}", &name);
    if !Path::new(&idx_file).exists() {
        fs::write(&idx_file, b"")?;   // empty placeholder; B-tree populated later
    }

    let index_oid = catalog.alloc_oid();
    let index = Index {
        index_oid,
        index_name: name,
        table_oid,
        index_type: IndexType::BTree,
        column_oids: column_oids.clone(),
        is_unique,
        is_primary,
        index_pages: 0,
    };

    // Persist to pg_index
    let bytes = serialize_index_tuple(&index);
    pm.insert_catalog_tuple(CAT_INDEX, bytes)?;

    // Update in-memory table
    for db in catalog.databases.values_mut() {
        for table in db.tables.values_mut() {
            if table.table_oid == table_oid {
                table.indexes.push(index_oid);
                break;
            }
        }
    }
    catalog.cache.invalidate_indexes(table_oid);

    Ok(index_oid)
}

// ─────────────────────────────────────────────────────────────
// drop_index
// ─────────────────────────────────────────────────────────────

/// Drop an index by `index_oid`.
///
/// Refuses if any constraint still references the index.
pub fn drop_index(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    index_oid: u32,
) -> Result<(), CatalogError> {
    // Find the exact (page_num, slot_id) via the proper page-scan predicate.
    let result = pm.find_catalog_tuple(CAT_INDEX, |b| {
        deserialize_index_tuple(b)
            .map(|idx| idx.index_oid == index_oid)
            .unwrap_or(false)
    })?;

    let (pn, slot, raw) = result.ok_or_else(|| CatalogError::IndexNotFound(index_oid.to_string()))?;
    let index = deserialize_index_tuple(&raw).map_err(CatalogError::IoError)?;

    // Check that no constraint references this index
    let constraints = pm.scan_catalog(CAT_CONSTRAINT)?;
    for t in &constraints {
        let c = deserialize_constraint_tuple(t).map_err(CatalogError::IoError)?;
        let references = match &c.metadata {
            crate::catalog::types::ConstraintMetadata::PrimaryKey { index_oid: ioid } => *ioid == index_oid,
            crate::catalog::types::ConstraintMetadata::Unique { index_oid: ioid }     => *ioid == index_oid,
            _ => false,
        };
        if references {
            return Err(CatalogError::ForeignKeyDependency(c.constraint_name.clone()));
        }
    }

    // Remove index file from disk (best-effort)
    let db_name = db_name_for_table(catalog, index.table_oid).unwrap_or_default();
    let idx_file = INDEX_FILE_TEMPLATE
        .replace("{database}", &db_name)
        .replace("{index}", &index.index_name);
    let _ = fs::remove_file(&idx_file);

    // Logical delete in pg_index using the real (page_num, slot_id)
    pm.delete_catalog_tuple(CAT_INDEX, pn, slot)?;

    // Update in-memory table
    for db in catalog.databases.values_mut() {
        for table in db.tables.values_mut() {
            if table.table_oid == index.table_oid {
                table.indexes.retain(|&oid| oid != index_oid);
                break;
            }
        }
    }
    catalog.cache.invalidate_indexes(index.table_oid);

    Ok(())
}

// ─────────────────────────────────────────────────────────────
// get_indexes_for_table
// ─────────────────────────────────────────────────────────────

/// Return all Index records that apply to `table_oid`.
pub fn get_indexes_for_table(
    pm: &CatalogPageManager,
    table_oid: u32,
) -> Result<Vec<Index>, CatalogError> {
    let tuples = pm.scan_catalog(CAT_INDEX)?;
    tuples.iter()
        .map(|t| deserialize_index_tuple(t).map_err(CatalogError::IoError))
        .filter(|r| r.as_ref().map(|idx: &Index| idx.table_oid == table_oid).unwrap_or(true))
        .collect()
}
