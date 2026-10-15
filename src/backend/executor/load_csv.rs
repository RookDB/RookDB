//! Row insertion: the typed insert path and its text edges.
//!
//! [`insert_row`] takes one value per column as `Option<DataValue>` and is
//! the only way rows enter a table. Text input (CSV files, literals of a
//! cached INSERT, tests) is parsed once by [`parse_text_row`] and then goes
//! through the same path.

use std::fs::File;
use std::io::{BufRead, BufReader};

use crate::backend::error::{RookError, RookResult};
use crate::catalog::types::{Catalog, Column};
use crate::types::DataValue;
use crate::types::functions::assign_cast;
use crate::types::row::serialize_nullable_typed_row;

/// Parse a row given as text, one string per column, the way SQL literals
/// are parsed. Empty text and `NULL` (any case) are SQL NULL.
pub fn parse_text_row(columns: &[Column], values: &[&str]) -> RookResult<Vec<Option<DataValue>>> {
    if values.len() != columns.len() {
        return Err(RookError::TypeMismatch(format!(
            "expected {} values, got {}",
            columns.len(),
            values.len()
        )));
    }
    columns
        .iter()
        .zip(values)
        .map(|(col, text)| parse_text_value(col, text))
        .collect()
}

/// Parse one value given as text for `column`; see [`parse_text_row`].
pub fn parse_text_value(column: &Column, text: &str) -> RookResult<Option<DataValue>> {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        return Ok(None);
    }
    DataValue::parse_and_encode(&column.data_type, text)
        .and_then(|bytes| DataValue::from_bytes(&column.data_type, &bytes))
        .map(Some)
        .map_err(|e| e.with_context(format!("column '{}'", column.name)))
}

/// Insert one row: convert each value to its column's type, check the
/// table's constraints, write the row and add it to every index.
///
/// Nothing is written when a value or constraint is rejected. If an index
/// rejects the row after it is written, the row is deleted again and the
/// error returned. Returns the row's heap location.
pub fn insert_row(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    values: &[Option<DataValue>],
) -> RookResult<(u32, u32)> {
    InsertTarget::new(catalog, db_name, table_name)?.insert_row(catalog, values)
}

/// What an INSERT statement resolves once about its table, so a multi-row
/// statement does not repeat the catalog and metadata lookups, the schema
/// copy and the path building for every row. It borrows nothing, so an
/// operator can keep it from row to row.
pub struct InsertTarget {
    db_name: String,
    table_name: String,
    columns: Vec<Column>,
    schema: Vec<crate::types::DataType>,
    table_path: std::path::PathBuf,
    resolved: std::cell::RefCell<ResolvedIndexes>,
}

/// The table's constraints and indexes as of one metadata generation.
struct ResolvedIndexes {
    generation: u64,
    checks: crate::backend::constraint::RowChecks,
    indexes: crate::backend::executor::create_index::IndexList,
}

impl ResolvedIndexes {
    fn new(catalog: &Catalog, db_name: &str, table_name: &str) -> Self {
        let engine = catalog.engine();
        // Read first: an invalidation while resolving shows up on the next row.
        let generation = engine.metadata_generation();
        ResolvedIndexes {
            generation,
            checks: crate::backend::constraint::RowChecks::new(catalog, db_name, table_name),
            indexes: crate::backend::executor::create_index::table_index_list(
                engine, db_name, table_name,
            ),
        }
    }
}

impl InsertTarget {
    pub fn new(catalog: &Catalog, db_name: &str, table_name: &str) -> RookResult<Self> {
        let columns = table_columns(catalog, db_name, table_name)?.to_vec();
        Ok(Self {
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
            schema: columns.iter().map(|c| c.data_type.clone()).collect(),
            columns,
            table_path: catalog.engine().paths().heap(db_name, table_name),
            resolved: std::cell::RefCell::new(ResolvedIndexes::new(catalog, db_name, table_name)),
        })
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// [`insert_row`] against the resolved table.
    pub fn insert_row(
        &self,
        catalog: &Catalog,
        values: &[Option<DataValue>],
    ) -> RookResult<(u32, u32)> {
        insert_row_resolved(self, catalog, values)
    }

    /// Resolve the table's constraints and indexes again when table metadata
    /// was invalidated since: a row's checks can build an index a constraint
    /// lacks, and the row must then go into that index too.
    fn refresh(&self, catalog: &Catalog) {
        if catalog.engine().metadata_generation() != self.resolved.borrow().generation {
            *self.resolved.borrow_mut() =
                ResolvedIndexes::new(catalog, &self.db_name, &self.table_name);
        }
    }
}

fn insert_row_resolved(
    target: &InsertTarget,
    catalog: &Catalog,
    values: &[Option<DataValue>],
) -> RookResult<(u32, u32)> {
    let (db_name, table_name, columns) = (
        target.db_name.as_str(),
        target.table_name.as_str(),
        target.columns.as_slice(),
    );
    if values.len() != columns.len() {
        return Err(RookError::TypeMismatch(format!(
            "table '{}' has {} columns but {} values were supplied",
            table_name,
            columns.len(),
            values.len()
        )));
    }
    let row: Vec<Option<DataValue>> = columns
        .iter()
        .zip(values)
        .map(|(col, value)| match value {
            None => Ok(None),
            Some(v) => assign_cast(v, &col.data_type)
                .map(Some)
                .map_err(|e| RookError::TypeMismatch(format!("column '{}': {}", col.name, e))),
        })
        .collect::<RookResult<_>>()?;

    target.refresh(catalog);
    target
        .resolved
        .borrow()
        .checks
        .check(catalog, columns, &row, &[])?;
    target.refresh(catalog);
    let indexes = std::sync::Arc::clone(&target.resolved.borrow().indexes);

    let bytes = serialize_nullable_typed_row(&target.schema, &row)?;

    let (page_id, slot_id) = crate::backend::cache::with_heap(catalog.engine(), &target.table_path, |hm| {
            hm.insert_tuple(&bytes)
        })
        // A row the heap refuses (empty or larger than a page) is a TypeMismatch.
        ?;

    if let Err(e) = crate::backend::executor::create_index::insert_into_indexes(
        &indexes,
        catalog.engine(),
        &row,
        page_id,
        slot_id,
    ) {
        let cause = format!("Index update failed: {}", e);
        // An index whose file is gone is built again from the table, which
        // now holds the row, so the row stands. Any other failure leaves
        // the indexes as they were: the row is taken back out instead.
        let lost = indexes.iter().any(|(path, _)| !path.exists());
        let repaired = lost
            && crate::backend::executor::create_index::repair_indexes(
                catalog, db_name, table_name, &cause,
            )
            .is_ok();
        if !repaired {
            // A row the index cannot see would vanish from every index
            // scan: take the row back out and fail the insert.
            if let Err(undo) = crate::backend::executor::delete::delete_by_pointers(
                catalog,
                db_name,
                table_name,
                &[(page_id, slot_id)],
            ) {
                log::error!("Failed to undo insert after index error: {}", undo);
            }
            return Err(RookError::IndexMaintenance(cause));
        }
    }

    crate::backend::cache::note_modified(catalog.engine(), db_name, table_name, 1);
    Ok((page_id, slot_id))
}

/// Parse a row given as text with [`parse_text_row`] and insert it.
pub fn insert_text_row(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    values: &[&str],
) -> RookResult<(u32, u32)> {
    let columns = table_columns(catalog, db_name, table_name)?;
    let row = parse_text_row(columns, values)?;
    insert_row(catalog, db_name, table_name, &row)
}

fn table_columns<'a>(
    catalog: &'a Catalog,
    db_name: &str,
    table_name: &str,
) -> RookResult<&'a [Column]> {
    let db = catalog
        .databases
        .get(db_name)
        .ok_or_else(|| RookError::NotFound {
            entity: "Database",
            name: db_name.to_string(),
        })?;
    let table = db
        .tables
        .get(table_name)
        .ok_or_else(|| RookError::NotFound {
            entity: "Table",
            name: format!("{}.{}", db_name, table_name),
        })?;
    Ok(&table.columns)
}

/// Insert a row given as text and return its heap location.
///
/// `Ok(None)` means the row was rejected (bad value or constraint
/// violation) and nothing was written; `Err` is a storage failure or a
/// missing table.
pub fn insert_single_tuple_with_location(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    values: &[&str],
) -> RookResult<Option<(u32, u32)>> {
    match insert_text_row(catalog, db_name, table_name, values) {
        Ok(location) => Ok(Some(location)),
        Err(
            e @ (RookError::Io(_) | RookError::IndexMaintenance(_) | RookError::NotFound { .. }),
        ) => Err(e.bare_io()),
        Err(e) => {
            log::info!("Row rejected for '{}.{}': {}", db_name, table_name, e);
            Ok(None)
        }
    }
}

/// [`insert_single_tuple_with_location`] reporting only whether the row was
/// inserted.
pub fn insert_single_tuple(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    values: &[&str],
) -> RookResult<bool> {
    insert_single_tuple_with_location(catalog, db_name, table_name, values).map(|opt| opt.is_some())
}

/// Load a CSV file into a table, one row per line.
///
/// A first line that names the table's columns is skipped as a header. Each
/// row is parsed with [`parse_text_row`] and inserted with [`insert_row`];
/// rows that fail are logged and counted, and loading continues.
///
/// Returns the number of rows inserted.
pub fn load_csv(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    csv_path: &str,
) -> RookResult<u32> {
    log::info!(
        "Loading CSV '{}' into '{}.{}'",
        csv_path,
        db_name,
        table_name
    );

    let columns = table_columns(catalog, db_name, table_name)?;
    if columns.is_empty() {
        return Err(RookError::Internal("Table has no columns".to_string()));
    }

    let csv_file = File::open(csv_path)
        .map_err(|e| RookError::storage(format!("Failed to open CSV file '{}'", csv_path), e))?;
    let mut lines = BufReader::new(csv_file).lines();

    // Skip a header line if present.
    let mut first_row: Option<String> = None;
    for line in lines.by_ref() {
        let line = line.map_err(RookError::storage_bare)?;
        if line.trim().is_empty() {
            continue;
        }
        let parsed = parse_csv_line(&line);
        let is_header = parsed.len() == columns.len()
            && parsed
                .iter()
                .zip(columns.iter())
                .all(|(v, col)| col.name.eq_ignore_ascii_case(v.trim().trim_matches('"')));
        if !is_header {
            first_row = Some(line);
        }
        break;
    }

    let mut line_idx = if first_row.is_none() { 1 } else { 0 };
    let mut inserted = 0u32;
    let mut skipped = 0u32;
    let mut failed = 0u32;

    let target = InsertTarget::new(catalog, db_name, table_name)?;
    for line in first_row.into_iter().map(Ok).chain(lines) {
        line_idx += 1;
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                log::warn!("Line {}: error reading line: {}", line_idx, e);
                failed += 1;
                continue;
            }
        };
        if line.trim().is_empty() {
            skipped += 1;
            continue;
        }

        let fields = parse_csv_line(&line);
        if fields.len() != columns.len() {
            log::warn!(
                "[CSV LOADER] Line {}: Expected {} columns, found {}. Skipping row.",
                line_idx,
                columns.len(),
                fields.len()
            );
            skipped += 1;
            continue;
        }
        let refs: Vec<&str> = fields.iter().map(String::as_str).collect();

        let result =
            parse_text_row(columns, &refs).and_then(|row| target.insert_row(catalog, &row));
        match result {
            Ok(_) => inserted += 1,
            Err(e) => {
                log::warn!("[CSV LOADER] Line {}: {}", line_idx, e);
                failed += 1;
            }
        }
    }

    log::info!(
        "CSV load into '{}.{}': {} inserted, {} skipped, {} failed",
        db_name,
        table_name,
        inserted,
        skipped,
        failed
    );
    if inserted == 0 && (skipped > 0 || failed > 0) {
        log::warn!(
            "WARNING: No rows were inserted. Please check your CSV file format and data types."
        );
    }

    Ok(inserted)
}

/// RFC-4180 compliant CSV line parser.
/// Handles quoted fields, embedded commas, doubled quotes (`""`), and backslash-escaped quotes (`\"`).
pub fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let chars: Vec<char> = line.chars().collect();
    let n = chars.len();
    let mut i = 0;

    while i < n {
        // Skip leading whitespace before field
        while i < n && (chars[i] == ' ' || chars[i] == '\t') {
            i += 1;
        }
        if i >= n {
            break;
        }

        if chars[i] == '"' {
            i += 1; // consume opening quote
            let mut field = String::new();
            while i < n {
                if chars[i] == '"' {
                    if i + 1 < n && chars[i + 1] == '"' {
                        field.push('"');
                        i += 2;
                    } else {
                        // closing quote
                        i += 1;
                        break;
                    }
                } else if chars[i] == '\\' && i + 1 < n && chars[i + 1] == '"' {
                    field.push('"');
                    i += 2;
                } else {
                    field.push(chars[i]);
                    i += 1;
                }
            }
            // Skip trailing whitespace after closing quote up to comma or end
            while i < n && (chars[i] == ' ' || chars[i] == '\t') {
                i += 1;
            }
            fields.push(field);
            if i < n && chars[i] == ',' {
                i += 1; // consume delimiter
                if i == n {
                    fields.push(String::new());
                }
            }
        } else {
            let mut field = String::new();
            while i < n && chars[i] != ',' {
                field.push(chars[i]);
                i += 1;
            }
            fields.push(field.trim().to_string());
            if i < n && chars[i] == ',' {
                i += 1; // consume delimiter
                if i == n {
                    fields.push(String::new());
                }
            }
        }
    }

    fields
}
