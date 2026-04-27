//! Constraint management functions for the catalog.
//!
//! All public functions in this module mutate the in-memory `Catalog` **and**
//! persist the change to the page-based system catalogs.

use crate::buffer_manager::BufferManager;
use crate::catalog::types::{
    Catalog, CatalogError, Constraint, ConstraintMetadata, ConstraintType, ConstraintViolation,
    ReferentialAction,
};
use crate::catalog::indexes::{create_index, get_indexes_for_table};
use crate::catalog::page_manager::{CatalogPageManager, CAT_COLUMN, CAT_CONSTRAINT};
use crate::catalog::serialize::{
    deserialize_column_tuple, serialize_column_tuple, deserialize_constraint_tuple, serialize_constraint_tuple, deserialize_database_tuple, deserialize_table_tuple
};

// ─────────────────────────────────────────────────────────────
// Helper: resolve db_name for a table OID
// ─────────────────────────────────────────────────────────────

fn db_name_for_table(pm: &CatalogPageManager, bm: &mut BufferManager, table_oid: u32) -> Option<String> {
    if let Ok(tables) = pm.scan_catalog(bm, crate::catalog::page_manager::CAT_TABLE) {
        let mut target_db_oid = None;
        for t in &tables {
            if let Ok((toid, _, db_oid, ..)) = deserialize_table_tuple(t) {
                if toid == table_oid {
                    target_db_oid = Some(db_oid);
                    break;
                }
            }
        }
        if let Some(db_oid) = target_db_oid {
            if let Ok(dbs) = pm.scan_catalog(bm, crate::catalog::page_manager::CAT_DATABASE) {
                for d in &dbs {
                    if let Ok((doid, name, ..)) = deserialize_database_tuple(d) {
                        if doid == db_oid {
                            return Some(name);
                        }
                    }
                }
            }
        }
    }
    None
}

// ─────────────────────────────────────────────────────────────
// Helper: resolve column names → OIDs for a given table OID
// ─────────────────────────────────────────────────────────────

fn resolve_column_oids(
    pm: &CatalogPageManager,
    bm: &mut BufferManager,
    table_oid: u32,
    column_names: &[String],
) -> Result<Vec<u32>, CatalogError> {
    let tuples = pm.scan_catalog(bm, CAT_COLUMN)?;
    let mut name_to_oid: std::collections::HashMap<String, u32> = Default::default();
    for t in &tuples {
        let (coid, toid, cname, ..) = deserialize_column_tuple(t).map_err(CatalogError::IoError)?;
        if toid == table_oid {
            name_to_oid.insert(cname, coid);
        }
    }
    let mut oids = Vec::new();
    for cn in column_names {
        let oid = name_to_oid
            .get(cn)
            .copied()
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
    bm: &mut BufferManager,
    constraint: &Constraint,
) -> Result<(), CatalogError> {
    let bytes = serialize_constraint_tuple(constraint);
    pm.insert_catalog_tuple(bm, CAT_CONSTRAINT, bytes)?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────
// Helper: check whether a table already has a PK
// ─────────────────────────────────────────────────────────────

fn table_has_primary_key(
    pm: &CatalogPageManager,
    bm: &mut BufferManager,
    table_oid: u32,
) -> Result<bool, CatalogError> {
    let tuples = pm.scan_catalog(bm, CAT_CONSTRAINT)?;
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
    bm: &mut BufferManager,
    ref_table_oid: u32,
    ref_col_oids: &[u32],
) -> Result<bool, CatalogError> {
    let tuples = pm.scan_catalog(bm, CAT_CONSTRAINT)?;
    for t in &tuples {
        let c = deserialize_constraint_tuple(t).map_err(CatalogError::IoError)?;
        if c.table_oid != ref_table_oid {
            continue;
        }
        if c.constraint_type != ConstraintType::PrimaryKey
            && c.constraint_type != ConstraintType::Unique
        {
            continue;
        }
        if ref_col_oids.iter().all(|oid| c.column_oids.contains(oid)) {
            return Ok(true);
        }
    }
    Ok(false)
}

// ─────────────────────────────────────────────────────────────
// Public constraint-creation functions
// ─────────────────────────────────────────────────────────────

fn set_columns_not_null(
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    column_oids: &[u32],
) -> Result<(), CatalogError> {
    for &coid in column_oids {
        if let Some((page_num, slot_id, tuple_bytes)) = pm.find_catalog_tuple(bm, CAT_COLUMN, |bytes| {
            if let Ok(deser) = deserialize_column_tuple(bytes) {
                deser.0 == coid
            } else {
                false
            }
        })? {
            let (c_oid, t_oid, name, pos, dt, tm, _, def_val, c_oids) =
                deserialize_column_tuple(&tuple_bytes)
                    .map_err(CatalogError::IoError)?;
            let new_bytes = serialize_column_tuple(
                c_oid, t_oid, &name, pos, &dt, tm.as_ref(), false, def_val.as_ref(), &c_oids,
            );
            pm.update_catalog_tuple(bm, CAT_COLUMN, page_num, slot_id, &new_bytes)?;
        }
    }
    Ok(())
}

pub fn add_primary_key_constraint(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    table_oid: u32,
    column_names: Vec<String>,
    constraint_name: Option<String>,
) -> Result<u32, CatalogError> {
    if table_has_primary_key(pm, bm, table_oid)? {
        return Err(CatalogError::AlreadyHasPrimaryKey);
    }

    let column_oids = resolve_column_oids(pm, bm, table_oid, &column_names)?;



    let name = constraint_name.unwrap_or_else(|| format!("pk_table_{}", table_oid));
    let index_oid = create_index(
        catalog,
        pm,
        bm,
        table_oid,
        column_oids.clone(),
        true,
        true,
        Some(name.clone() + "_idx"),
    )?;

    let constraint_oid = catalog.alloc_oid();
    let constraint = Constraint {
        constraint_oid,
        constraint_name: name,
        constraint_type: ConstraintType::PrimaryKey,
        table_oid,
        column_oids: column_oids.clone(),
        metadata: ConstraintMetadata::PrimaryKey { index_oid },
        is_deferrable: false,
    };

    persist_constraint(pm, bm, &constraint)?;
    
    set_columns_not_null(pm, bm, &column_oids)?;

    catalog.cache.invalidate_constraints(table_oid);

    Ok(constraint_oid)
}

pub fn add_foreign_key_constraint(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
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

    let column_oids = resolve_column_oids(pm, bm, table_oid, &column_names)?;
    let ref_column_oids =
        resolve_column_oids(pm, bm, referenced_table_oid, &referenced_column_names)?;

    if !check_referenced_columns_unique(pm, bm, referenced_table_oid, &ref_column_oids)? {
        return Err(CatalogError::ReferencedKeyMissing);
    }

    let name =
        constraint_name.unwrap_or_else(|| format!("fk_{}_to_{}", table_oid, referenced_table_oid));
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

    persist_constraint(pm, bm, &constraint)?;


    catalog.cache.invalidate_constraints(table_oid);

    Ok(constraint_oid)
}

pub fn add_unique_constraint(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    table_oid: u32,
    column_names: Vec<String>,
    constraint_name: Option<String>,
) -> Result<u32, CatalogError> {
    let column_oids = resolve_column_oids(pm, bm, table_oid, &column_names)?;
    let name = constraint_name.unwrap_or_else(|| {
        format!(
            "uq_table_{}_{}",
            table_oid,
            column_oids
                .iter()
                .map(|o| o.to_string())
                .collect::<Vec<_>>()
                .join("_")
        )
    });
    let index_oid = create_index(
        catalog,
        pm,
        bm,
        table_oid,
        column_oids.clone(),
        true,
        false,
        Some(name.clone() + "_idx"),
    )?;

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

    persist_constraint(pm, bm, &constraint)?;


    catalog.cache.invalidate_constraints(table_oid);

    Ok(constraint_oid)
}

pub fn add_not_null_constraint(
    catalog: &mut Catalog,
    pm: &mut CatalogPageManager,
    bm: &mut BufferManager,
    table_oid: u32,
    column_oid: u32,
) -> Result<(), CatalogError> {
    let cols = crate::catalog::catalog::get_columns(pm, bm, table_oid)?;
    if !cols.iter().any(|c| c.column_oid == column_oid) {
        return Err(CatalogError::ColumnNotFound(format!("column_oid: {}", column_oid)));
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
    persist_constraint(pm, bm, &constraint)?;
    set_columns_not_null(pm, bm, &[column_oid])?;
    catalog.cache.invalidate_constraints(table_oid);
    Ok(())
}

// ─────────────────────────────────────────────────────────────
// Constraint validation at insert time
// ─────────────────────────────────────────────────────────────

pub fn validate_constraints(
    catalog: &Catalog,
    pm: &CatalogPageManager,
    bm: &mut BufferManager,
    table_oid: u32,
    tuple_values: &std::collections::HashMap<u32, Option<Vec<u8>>>,
) -> Result<(), ConstraintViolation> {
    let constraints = match get_constraints_for_table(catalog, pm, bm, table_oid) {
        Ok(c) => c,
        Err(_) => return Ok(()),
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
            ConstraintType::PrimaryKey | ConstraintType::Unique => {
                let db_name = db_name_for_table(pm, bm, table_oid).unwrap_or_default();
                let index_oid = match &constraint.metadata {
                    ConstraintMetadata::PrimaryKey { index_oid: id } => *id,
                    ConstraintMetadata::Unique { index_oid: id } => *id,
                    _ => continue,
                };
                let indexes = get_indexes_for_table(pm, bm, table_oid).unwrap_or_default();
                if let Some(idx) = indexes.iter().find(|i| i.index_oid == index_oid) {
                    let mut key_bytes = Vec::new();
                    let mut all_nulls = true;
                    for col_oid in &constraint.column_oids {
                        if let Some(Some(val)) = tuple_values.get(col_oid) {
                            key_bytes.extend_from_slice(val);
                            all_nulls = false;
                        }
                    }
                    if !all_nulls
                        && crate::catalog::indexes::index_lookup(
                            bm,
                            &db_name,
                            &idx.index_name,
                            &key_bytes,
                        )
                        .unwrap_or(false)
                    {
                        return Err(ConstraintViolation::UniqueViolation {
                            constraint: constraint.constraint_name.clone(),
                        });
                    }
                }
            }
            ConstraintType::ForeignKey => {
                let db_name = db_name_for_table(pm, bm, table_oid).unwrap_or_default();
                if let ConstraintMetadata::ForeignKey {
                    referenced_table_oid,
                    referenced_column_oids,
                    ..
                } = &constraint.metadata
                {
                    let mut key_bytes = Vec::new();
                    let mut any_null = false;
                    for col_oid in &constraint.column_oids {
                        if let Some(Some(val)) = tuple_values.get(col_oid) {
                            key_bytes.extend_from_slice(val);
                        } else {
                            any_null = true;
                        }
                    }
                    if !any_null {
                        let ref_indexes = get_indexes_for_table(pm, bm, *referenced_table_oid)
                            .unwrap_or_default();
                        let mut found_index = None;
                        for idx in ref_indexes {
                            if idx.column_oids == *referenced_column_oids
                                && (idx.is_primary || idx.is_unique)
                            {
                                found_index = Some(idx);
                                break;
                            }
                        }
                        if let Some(idx) = found_index {
                            if !crate::catalog::indexes::index_lookup(
                                bm,
                                &db_name,
                                &idx.index_name,
                                &key_bytes,
                            )
                            .unwrap_or(false)
                            {
                                return Err(ConstraintViolation::ForeignKeyViolation {
                                    constraint: constraint.constraint_name.clone(),
                                });
                            }
                        }
                    }
                }
            }
            ConstraintType::Check => {}
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────
// Query helpers
// ─────────────────────────────────────────────────────────────

pub fn get_constraints_for_table(
    _catalog: &Catalog,
    pm: &CatalogPageManager,
    bm: &mut BufferManager,
    table_oid: u32,
) -> Result<Vec<Constraint>, CatalogError> {
    let tuples = pm.scan_catalog(bm, CAT_CONSTRAINT)?;
    tuples
        .iter()
        .map(|t| deserialize_constraint_tuple(t).map_err(CatalogError::IoError))
        .filter(|r| {
            r.as_ref()
                .map(|c: &Constraint| c.table_oid == table_oid)
                .unwrap_or(true)
        })
        .collect()
}
