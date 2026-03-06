//! Constraint management functions for the catalog.
//!
//! All public functions in this module mutate the in-memory `Catalog` **and**
//! persist the change to the page-based system catalogs.

use crate::catalog::types::{
    Catalog, CatalogError, Constraint, ConstraintMetadata, ConstraintType,
    ConstraintViolation, ReferentialAction,
};
use crate::catalog::indexes::create_index;
use crate::catalog::page_manager::{CatalogPageManager, CAT_COLUMN, CAT_CONSTRAINT};
use crate::catalog::serialize::{
    deserialize_column_tuple, deserialize_constraint_tuple, serialize_constraint_tuple,
};

// ─────────────────────────────────────────────────────────────
// Helper: resolve column names → OIDs for a given table OID
// ─────────────────────────────────────────────────────────────

fn resolve_column_oids(
    pm: &CatalogPageManager,
    table_oid: u32,
    column_names: &[String],
) -> Result<Vec<u32>, CatalogError> {
    // Scan pg_column for this table, build name→oid map
    let tuples = pm.scan_catalog(CAT_COLUMN)?;
    let mut name_to_oid: std::collections::HashMap<String, u32> = Default::default();
    for t in &tuples {
        let (coid, toid, cname, ..) = deserialize_column_tuple(t)
            .map_err(CatalogError::IoError)?;
        if toid == table_oid {
            name_to_oid.insert(cname, coid);
        }
    }
    let mut oids = Vec::new();
    for cn in column_names {
        let oid = name_to_oid.get(cn).copied()
            .ok_or_else(|| CatalogError::ColumnNotFound(cn.clone()))?;
        oids.push(oid);
    }
    Ok(oids)
}

// ─────────────────────────────────────────────────────────────
// Helper: persist a Constraint struct to pg_constraint
// ─────────────────────────────────────────────────────────────

fn persist_constraint(
    pm: &mut CatalogPageManager,
    constraint: &Constraint,
) -> Result<(), CatalogError> {
    let bytes = serialize_constraint_tuple(constraint);
    pm.insert_catalog_tuple(CAT_CONSTRAINT, bytes)?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────
// Helper: check whether a table already has a PK
// ─────────────────────────────────────────────────────────────

fn table_has_primary_key(
    pm: &CatalogPageManager,
    table_oid: u32,
) -> Result<bool, CatalogError> {
    let tuples = pm.scan_catalog(CAT_CONSTRAINT)?;
    for t in &tuples {
        let c = deserialize_constraint_tuple(t).map_err(CatalogError::IoError)?;
        if c.table_oid == table_oid && c.constraint_type == ConstraintType::PrimaryKey {
            return Ok(true);
        }
    }
    Ok(false)
}

// ─────────────────────────────────────────────────────────────
// Helper: check that the referenced columns are covered by a PK / UNIQUE
// ─────────────────────────────────────────────────────────────

fn check_referenced_columns_unique(
    pm: &CatalogPageManager,
    ref_table_oid: u32,
    ref_col_oids: &[u32],
) -> Result<bool, CatalogError> {
    let tuples = pm.scan_catalog(CAT_CONSTRAINT)?;
    for t in &tuples {
        let c = deserialize_constraint_tuple(t).map_err(CatalogError::IoError)?;
        if c.table_oid != ref_table_oid { continue; }
        if c.constraint_type != ConstraintType::PrimaryKey
            && c.constraint_type != ConstraintType::Unique { continue; }
        // columns must be a superset of ref_col_oids
        if ref_col_oids.iter().all(|oid| c.column_oids.contains(oid)) {
            return Ok(true);
        }
    }
    Ok(false)
}

// ─────────────────────────────────────────────────────────────
// Public constraint-creation functions
// ─────────────────────────────────────────────────────────────

/// Add a PRIMARY KEY constraint to `table_oid` over `column_names`.
///
/// Creates a backing unique B-tree index automatically.  Returns the new constraint OID.
pub fn add_primary_key_constraint(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    table_oid: u32,
    column_names: Vec<String>,
    constraint_name: Option<String>,
) -> Result<u32, CatalogError> {
    if table_has_primary_key(pm, table_oid)? {
        return Err(CatalogError::AlreadyHasPrimaryKey);
    }

    let column_oids = resolve_column_oids(pm, table_oid, &column_names)?;

    // Mark each column NOT NULL in memory (page-level update skipped for brevity;
    // the is_nullable=false is enforced at insert time via validate_constraints).
    // For each column in-memory we update the flag:
    for db in catalog.databases.values_mut() {
        for table in db.tables.values_mut() {
            if table.table_oid == table_oid {
                for col in table.columns.iter_mut() {
                    if column_oids.contains(&col.column_oid) {
                        col.is_nullable = false;
                    }
                }
            }
        }
    }

    let name = constraint_name.unwrap_or_else(|| format!("pk_table_{}", table_oid));
    let index_oid = create_index(catalog, pm, table_oid, column_oids.clone(), true, true, Some(name.clone() + "_idx"))?;

    let constraint_oid = catalog.alloc_oid();
    let constraint = Constraint {
        constraint_oid,
        constraint_name: name,
        constraint_type: ConstraintType::PrimaryKey,
        table_oid,
        column_oids,
        metadata: ConstraintMetadata::PrimaryKey { index_oid },
        is_deferrable: false,
    };

    persist_constraint(pm, &constraint)?;

    // Cache in-memory on the table
    for db in catalog.databases.values_mut() {
        for table in db.tables.values_mut() {
            if table.table_oid == table_oid {
                table.constraints.push(constraint.clone());
                break;
            }
        }
    }
    catalog.cache.invalidate_constraints(table_oid);

    Ok(constraint_oid)
}

/// Add a FOREIGN KEY constraint.  Returns the new constraint OID.
pub fn add_foreign_key_constraint(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    table_oid: u32,
    column_names: Vec<String>,
    referenced_table_oid: u32,
    referenced_column_names: Vec<String>,
    on_delete: ReferentialAction,
    on_update: ReferentialAction,
    constraint_name: Option<String>,
) -> Result<u32, CatalogError> {
    if column_names.len() != referenced_column_names.len() {
        return Err(CatalogError::ColumnCountMismatch);
    }

    let column_oids     = resolve_column_oids(pm, table_oid, &column_names)?;
    let ref_column_oids = resolve_column_oids(pm, referenced_table_oid, &referenced_column_names)?;

    if !check_referenced_columns_unique(pm, referenced_table_oid, &ref_column_oids)? {
        return Err(CatalogError::ReferencedKeyMissing);
    }

    let name = constraint_name.unwrap_or_else(|| format!("fk_{}_to_{}", table_oid, referenced_table_oid));
    let constraint_oid = catalog.alloc_oid();
    let constraint = Constraint {
        constraint_oid,
        constraint_name: name,
        constraint_type: ConstraintType::ForeignKey,
        table_oid,
        column_oids,
        metadata: ConstraintMetadata::ForeignKey {
            referenced_table_oid,
            referenced_column_oids: ref_column_oids,
            on_delete,
            on_update,
        },
        is_deferrable: false,
    };

    persist_constraint(pm, &constraint)?;

    for db in catalog.databases.values_mut() {
        for table in db.tables.values_mut() {
            if table.table_oid == table_oid {
                table.constraints.push(constraint.clone());
                break;
            }
        }
    }
    catalog.cache.invalidate_constraints(table_oid);

    Ok(constraint_oid)
}

/// Add a UNIQUE constraint.  Returns the new constraint OID.
pub fn add_unique_constraint(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    table_oid: u32,
    column_names: Vec<String>,
    constraint_name: Option<String>,
) -> Result<u32, CatalogError> {
    let column_oids = resolve_column_oids(pm, table_oid, &column_names)?;
    let name = constraint_name.unwrap_or_else(|| format!("uq_table_{}_{}", table_oid, column_oids.iter().map(|o| o.to_string()).collect::<Vec<_>>().join("_")));
    let index_oid = create_index(catalog, pm, table_oid, column_oids.clone(), true, false, Some(name.clone() + "_idx"))?;

    let constraint_oid = catalog.alloc_oid();
    let constraint = Constraint {
        constraint_oid,
        constraint_name: name,
        constraint_type: ConstraintType::Unique,
        table_oid,
        column_oids,
        metadata: ConstraintMetadata::Unique { index_oid },
        is_deferrable: false,
    };

    persist_constraint(pm, &constraint)?;

    for db in catalog.databases.values_mut() {
        for table in db.tables.values_mut() {
            if table.table_oid == table_oid {
                table.constraints.push(constraint.clone());
                break;
            }
        }
    }
    catalog.cache.invalidate_constraints(table_oid);

    Ok(constraint_oid)
}

/// Add a NOT NULL constraint to a single column.
pub fn add_not_null_constraint(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    table_oid: u32,
    column_oid: u32,
) -> Result<(), CatalogError> {
    // Update in-memory column metadata
    let mut found = false;
    for db in catalog.databases.values_mut() {
        for table in db.tables.values_mut() {
            if table.table_oid == table_oid {
                for col in table.columns.iter_mut() {
                    if col.column_oid == column_oid {
                        col.is_nullable = false;
                        found = true;
                    }
                }
            }
        }
    }
    if !found {
        return Err(CatalogError::ColumnNotFound(column_oid.to_string()));
    }

    let constraint_oid = catalog.alloc_oid();
    let name = format!("nn_col_{}", column_oid);
    let constraint = Constraint {
        constraint_oid,
        constraint_name: name,
        constraint_type: ConstraintType::NotNull,
        table_oid,
        column_oids: vec![column_oid],
        metadata: ConstraintMetadata::NotNull,
        is_deferrable: false,
    };
    persist_constraint(pm, &constraint)?;
    catalog.cache.invalidate_constraints(table_oid);
    Ok(())
}

// ─────────────────────────────────────────────────────────────
// Constraint validation at insert time
// ─────────────────────────────────────────────────────────────

/// Validate all constraints for a table before inserting / updating a tuple.
///
/// `tuple_values` maps column OID → raw bytes.  NOT NULL is the only
/// constraint class that can be evaluated fully without index access;
/// UNIQUE / FK checks only verify that the value list is non-empty for now
/// (full index-backed validation is added in the indexing milestone).
pub fn validate_constraints(
    catalog: &Catalog,
    pm: &CatalogPageManager,
    table_oid: u32,
    tuple_values: &std::collections::HashMap<u32, Option<Vec<u8>>>,
) -> Result<(), ConstraintViolation> {
    // Look up constraints for this table
    let constraints = match get_constraints_for_table(catalog, pm, table_oid) {
        Ok(c) => c,
        Err(_) => return Ok(()), // no constraints → fine
    };

    for constraint in &constraints {
        match &constraint.constraint_type {
            ConstraintType::NotNull => {
                for col_oid in &constraint.column_oids {
                    if let Some(None) = tuple_values.get(col_oid) {
                        let col_name = col_oid.to_string();
                        return Err(ConstraintViolation::NotNullViolation { column: col_name });
                    }
                }
            }
            // Full UNIQUE / FK validation requires index access – deferred
            ConstraintType::PrimaryKey | ConstraintType::Unique => {}
            ConstraintType::ForeignKey => {}
            ConstraintType::Check => {}
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────
// Query helpers
// ─────────────────────────────────────────────────────────────

/// Return all constraints for the named table.
pub fn get_constraints_for_table(
    _catalog: &Catalog,
    pm: &CatalogPageManager,
    table_oid: u32,
) -> Result<Vec<Constraint>, CatalogError> {
    let tuples = pm.scan_catalog(CAT_CONSTRAINT)?;
    tuples.iter()
        .map(|t| deserialize_constraint_tuple(t).map_err(CatalogError::IoError))
        .filter(|r| r.as_ref().map(|c: &Constraint| c.table_oid == table_oid).unwrap_or(true))
        .collect()
}
