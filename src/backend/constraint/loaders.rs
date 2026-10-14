//! System table data loaders for constraint metadata.
//!
//! Loads UNIQUE indexes, CHECK constraints, FOREIGN KEY definitions,
//! and table schemas from the system tables (`sys_indexes`, `sys_constraints`,
//! `sys_columns`, `sys_tables`).

use crate::backend::system_table::split_column_list;
use crate::types::{DataType, DataValue};

/// A foreign key seen from the table it references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferencingFk {
    pub child_table: String,
    /// The child's FK columns, in declaration order.
    pub child_cols: Vec<String>,
    /// The parent columns they reference, position for position.
    pub parent_cols: Vec<String>,
    pub parent_table: String,
    /// The constraint type as stored, e.g. `FOREIGN KEY ON DELETE CASCADE`.
    pub action: String,
}

/// Load FOREIGN KEY constraints where `our_table` is the **parent** (referenced table).
///
/// Scans ALL constraints in the database to find constraints where
/// `ref_table == our_table_name`.  Since `ref_table` is stored as a name string
/// (not an ID), we filter by name.
///
pub fn load_referencing_foreign_keys(
    engine: &crate::backend::engine::Engine,
    _db_name: &str,
    our_table_name: &str,
) -> Result<Vec<ReferencingFk>, String> {
    use crate::backend::system_table::{SYS_CONSTRAINTS_SCHEMA, SYS_TABLES_SCHEMA};

    let constr_path = engine.paths().system_table("constraints");
    if !constr_path.exists() {
        return Ok(Vec::new());
    }

    // Load sys_tables for the db to create a table_id → table_name mapping
    let tbl_path = engine.paths().system_table("tables");
    let mut tbl_id_to_name: std::collections::HashMap<i32, String> =
        std::collections::HashMap::new();
    if tbl_path.exists()
        && let Ok(heap) = crate::backend::cache::lease_heap(engine, &tbl_path)
    {
        for result in heap.scan() {
            if let Ok((_, _, raw_bytes)) = result
                && let Ok(decoded) =
                    crate::types::deserialize_nullable_row(SYS_TABLES_SCHEMA, &raw_bytes)
                && decoded.len() >= 3
                && let Some(Some(DataValue::Int(tid))) = decoded.first()
            {
                let name_opt = match decoded.get(2) {
                    Some(Some(DataValue::Varchar(name))) => Some(name.clone()),
                    Some(Some(DataValue::Char(name))) => Some(name.clone()),
                    _ => None,
                };
                if let Some(name) = name_opt {
                    tbl_id_to_name.insert(*tid, name);
                }
            }
        }
    }

    let heap = crate::backend::cache::lease_heap(engine, &constr_path)
        .map_err(|e| format!("Failed to open sys_constraints: {}", e))?;

    let mut referencing_fks = Vec::new();
    let schema = SYS_CONSTRAINTS_SCHEMA;

    for result in heap.scan() {
        let (_page_id, _slot_id, raw_bytes) =
            result.map_err(|e| format!("Error scanning sys_constraints: {}", e))?;
        let decoded = crate::types::deserialize_nullable_row(schema, &raw_bytes)
            .map_err(|e| format!("Error deserializing sys_constraints: {}", e))?;

        if decoded.len() < 6 {
            continue;
        }

        let constr_type = match &decoded[2] {
            Some(DataValue::Varchar(s)) => s.as_str(),
            Some(DataValue::Char(s)) => s.as_str(),
            _ => continue,
        };
        if !constr_type.to_uppercase().contains("FOREIGN KEY") {
            continue;
        }

        let ref_table = match &decoded[4] {
            Some(DataValue::Varchar(s)) => s.clone(),
            Some(DataValue::Char(s)) => s.clone(),
            _ => continue,
        };

        // Check if this FK references our table
        if !ref_table.eq_ignore_ascii_case(our_table_name) {
            continue;
        }

        // Resolve the child table name from the table_id
        let child_table_id = match &decoded[1] {
            Some(DataValue::Int(id)) => *id,
            _ => continue,
        };
        let child_table = tbl_id_to_name
            .get(&child_table_id)
            .cloned()
            .unwrap_or_else(|| format!("<table_id={}>", child_table_id));

        let child_cols = match &decoded[3] {
            Some(DataValue::Varchar(s)) | Some(DataValue::Char(s)) => split_column_list(s),
            _ => continue,
        };
        let parent_cols = match &decoded[5] {
            Some(DataValue::Varchar(s)) | Some(DataValue::Char(s)) => split_column_list(s),
            _ => continue,
        };
        if child_cols.is_empty() || child_cols.len() != parent_cols.len() {
            continue;
        }

        referencing_fks.push(ReferencingFk {
            child_table,
            child_cols,
            parent_cols,
            parent_table: ref_table,
            action: constr_type.to_string(),
        });
    }

    Ok(referencing_fks)
}

/// Load the complete column schema for a table from `sys_columns`.
///
/// Returns `Vec<(column_name, DataType)>` ordered by ordinal position.
pub(crate) fn load_table_schema(
    engine: &crate::backend::engine::Engine,
    db_name: &str,
    table_name: &str,
) -> Result<Option<Vec<(String, DataType)>>, String> {
    use crate::backend::system_table::{SYS_COLUMNS_SCHEMA, resolve_table_id};

    let (table_id, _) = match resolve_table_id(engine, db_name, table_name) {
        Ok(ids) => ids,
        Err(_) => return Ok(None),
    };

    let col_path = engine.paths().system_table("columns");
    if !col_path.exists() {
        return Ok(None);
    }

    let heap = crate::backend::cache::lease_heap(engine, &col_path)
        .map_err(|e| format!("Failed to open sys_columns: {}", e))?;

    let mut schema: Vec<(i32, String, DataType)> = Vec::new();

    for result in heap.scan() {
        let (_page_id, _slot_id, raw_bytes) =
            result.map_err(|e| format!("Error scanning sys_columns: {}", e))?;
        let decoded = crate::types::deserialize_nullable_row(SYS_COLUMNS_SCHEMA, &raw_bytes)
            .map_err(|e| format!("Error deserializing sys_columns: {}", e))?;

        if decoded.len() < 8 {
            continue;
        }

        // SYS_COLUMNS_SCHEMA: col_id, table_id, name, data_type, ordinal, nullable, has_default, default_value
        let row_table_id = match &decoded[1] {
            Some(DataValue::Int(id)) => *id,
            _ => continue,
        };
        if row_table_id != table_id {
            continue;
        }

        let col_name = match &decoded[2] {
            Some(DataValue::Varchar(n)) => n.clone(),
            Some(DataValue::Char(n)) => n.clone(),
            _ => continue,
        };
        let col_type_str = match &decoded[3] {
            Some(DataValue::Varchar(s)) => s.clone(),
            Some(DataValue::Char(s)) => s.clone(),
            _ => continue,
        };
        let ordinal = match &decoded[4] {
            Some(DataValue::Int(o)) => *o,
            _ => continue,
        };

        let col_type: DataType = col_type_str.parse().unwrap_or(DataType::Varchar(255));
        schema.push((ordinal, col_name, col_type));
    }

    schema.sort_by_key(|(ord, _, _)| *ord);
    Ok(Some(
        schema.into_iter().map(|(_, name, dt)| (name, dt)).collect(),
    ))
}
