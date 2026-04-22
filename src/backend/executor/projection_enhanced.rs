//! Enhanced Projection Engine with Benchmarking and Extended Features
//!
//! This module provides a comprehensive projection implementation with:
//! - Column reordering
//! - Variable-length data handling
//! - Benchmarking and performance metrics
//! - Filter options with error tracking
//! - Success/failure status reporting
//! - Layer-by-layer evaluation
//! - Integration with CTEs and subqueries
//!
//! Main entry point: `ProjectionEngine::execute()`

use std::collections::{HashMap, HashSet};
use std::time::Instant;
use std::fs::OpenOptions;
use std::io;

use crate::catalog::types::{Catalog, Column, DataType};
use crate::disk::read_page;
use crate::executor::expr::{eval_expr, Expr, Row};
use crate::executor::tuple_codec::decode_tuple;
use crate::executor::value::Value;
use crate::layout::TABLE_FILE_TEMPLATE;
use crate::page::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, Page};
use crate::table::page_count;
use crate::executor::projection::{OutputColumn, ProjectionInput, ProjectionItem, ResultTable};

// ─── Status and Result Types ─────────────────────────────────────────────────

/// Status of projection operation
#[derive(Debug, Clone)]
pub enum ProjectionStatus {
    Success,
    PartialSuccess { error_count: u64, warning_count: u64 },
    Failed { reason: String },
}

impl std::fmt::Display for ProjectionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProjectionStatus::Success => write!(f, "SUCCESS"),
            ProjectionStatus::PartialSuccess { error_count, warning_count } => {
                write!(f, "PARTIAL_SUCCESS (errors: {}, warnings: {})", error_count, warning_count)
            }
            ProjectionStatus::Failed { reason } => write!(f, "FAILED: {}", reason),
        }
    }
}

/// Performance metrics for projection
#[derive(Debug, Clone)]
pub struct ProjectionMetrics {
    pub rows_processed: u64,
    pub rows_filtered: u64,
    pub rows_output: u64,
    pub elapsed_ms: u128,
    pub pages_read: u64,
    pub memory_bytes: usize,
}

impl ProjectionMetrics {
    pub fn new() -> Self {
        Self {
            rows_processed: 0,
            rows_filtered: 0,
            rows_output: 0,
            elapsed_ms: 0,
            pages_read: 0,
            memory_bytes: 0,
        }
    }

    pub fn throughput_rows_per_sec(&self) -> f64 {
        if self.elapsed_ms == 0 {
            0.0
        } else {
            (self.rows_processed as f64 * 1000.0) / (self.elapsed_ms as f64)
        }
    }
}

/// Complete result of projection operation
#[derive(Debug, Clone)]
pub struct ProjectionResult {
    pub status: ProjectionStatus,
    pub data: ResultTable,
    pub metrics: ProjectionMetrics,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ProjectionResult {
    /// Print result with all details
    pub fn print_detailed(&self) {
        println!("\n=== Projection Result ===");
        println!("Status: {}", self.status);
        
        if !self.warnings.is_empty() {
            println!("\nWarnings ({}):", self.warnings.len());
            for w in &self.warnings {
                println!("  ⚠ {}", w);
            }
        }
        
        if !self.errors.is_empty() {
            println!("\nErrors ({}):", self.errors.len());
            for e in &self.errors {
                println!("  ✗ {}", e);
            }
        }

        println!("\n--- Data ---");
        self.data.print();
        
        println!("\n--- Metrics ---");
        println!("  Processed: {} rows", self.metrics.rows_processed);
        println!("  Filtered:  {} rows", self.metrics.rows_filtered);
        println!("  Output:    {} rows", self.metrics.rows_output);
        println!("  Time:      {} ms", self.metrics.elapsed_ms);
        println!("  Pages:     {} read", self.metrics.pages_read);
        println!("  Throughput: {:.0} rows/sec", self.metrics.throughput_rows_per_sec());
        println!("  Memory:    {} bytes", self.metrics.memory_bytes);
    }
}

// ─── Column Reordering ───────────────────────────────────────────────────────

/// Specification for column reordering
#[derive(Debug, Clone)]
pub struct ColumnReorderSpec {
    /// Original column indices in desired order
    pub indices: Vec<usize>,
    /// New column names (if remapping)
    pub new_names: Option<Vec<String>>,
}

impl ColumnReorderSpec {
    /// Reorder columns by indices
    pub fn by_indices(indices: Vec<usize>) -> Self {
        Self { indices, new_names: None }
    }

    /// Reorder columns by indices and rename
    pub fn by_indices_and_names(indices: Vec<usize>, new_names: Vec<String>) -> Self {
        Self { indices, new_names: Some(new_names) }
    }
}

// ─── Column Elimination ─────────────────────────────────────────────────────

/// Column elimination optimizer - only reads required columns
#[derive(Debug, Clone)]
pub struct ColumnPruner {
    /// Set of column indices that are actually needed
    pub required_columns: HashSet<usize>,
    /// Original column count for validation
    pub total_columns: usize,
}

impl ColumnPruner {
    /// Create a new column pruner
    pub fn new(total_columns: usize) -> Self {
        Self {
            required_columns: HashSet::new(),
            total_columns,
        }
    }

    /// Analyze projection items to determine required columns
    pub fn analyze_projection(&mut self, items: &[ProjectionItem]) {
        for item in items {
            match item {
                ProjectionItem::Star => {
                    // SELECT * needs all columns
                    for i in 0..self.total_columns {
                        self.required_columns.insert(i);
                    }
                }
                ProjectionItem::Expr(expr, _) => {
                    self.analyze_expression(expr);
                }
            }
        }
    }

    /// Analyze WHERE clause expression for required columns
    pub fn analyze_filter(&mut self, expr: &Option<Expr>) {
        if let Some(e) = expr {
            self.analyze_expression(e);
        }
    }

    /// Recursively analyze expression tree for column dependencies
    pub fn analyze_expression(&mut self, expr: &Expr) {
        match expr {
            Expr::Column(index) => {
                if *index < self.total_columns {
                    self.required_columns.insert(*index);
                }
            }
            // Binary operations
            Expr::Add(left, right) | Expr::Sub(left, right) | Expr::Mul(left, right) | Expr::Div(left, right) |
            Expr::Eq(left, right) | Expr::Ne(left, right) | Expr::Lt(left, right) | Expr::Le(left, right) |
            Expr::Gt(left, right) | Expr::Ge(left, right) | Expr::And(left, right) | Expr::Or(left, right) |
            Expr::Like(left, right) | Expr::NotLike(left, right) | Expr::Concat(left, right) |
            Expr::DateAdd(left, right) | Expr::DateDiff(left, right) => {
                self.analyze_expression(left);
                self.analyze_expression(right);
            }
            // Unary operations
            Expr::Neg(operand) | Expr::Not(operand) | Expr::IsNull(operand) | Expr::IsNotNull(operand) |
            Expr::Upper(operand) | Expr::Lower(operand) | Expr::Length(operand) | Expr::Trim(operand) => {
                self.analyze_expression(operand);
            }
            // Ternary operations
            Expr::Substring(expr, start, length) => {
                self.analyze_expression(expr);
                self.analyze_expression(start);
                self.analyze_expression(length);
            }
            Expr::Between(expr, min, max) => {
                self.analyze_expression(expr);
                self.analyze_expression(min);
                self.analyze_expression(max);
            }
            // Cast operation
            Expr::Cast(expr, _) => {
                self.analyze_expression(expr);
            }
            // In operations
            Expr::In(expr, values) | Expr::NotIn(expr, values) => {
                self.analyze_expression(expr);
                for value in values {
                    self.analyze_expression(value);
                }
            }
            // Literals don't need columns
            Expr::Const(_) => {}
        }
    }

    /// Get the list of required column indices (sorted)
    pub fn get_required_indices(&self) -> Vec<usize> {
        let mut indices: Vec<usize> = self.required_columns.iter().cloned().collect();
        indices.sort();
        indices
    }

    /// Check if column elimination is beneficial
    pub fn should_eliminate(&self) -> bool {
        // Only eliminate if we can skip at least 2 columns
        self.total_columns > self.required_columns.len() + 1
    }

    /// Get elimination statistics
    pub fn elimination_stats(&self) -> (usize, usize, f64) {
        let total = self.total_columns;
        let required = self.required_columns.len();
        let savings = if total > 0 { ((total - required) as f64 / total as f64) * 100.0 } else { 0.0 };
        (total, required, savings)
    }
}

// ─── Filter Information ──────────────────────────────────────────────────────

/// Filter configuration with tracking
#[derive(Debug, Clone)]
pub struct FilterConfig {
    pub predicate: Option<Expr>,
    pub track_filtered: bool,
    pub max_errors: u64,
}

impl FilterConfig {
    pub fn new(predicate: Option<Expr>) -> Self {
        Self {
            predicate,
            track_filtered: false,
            max_errors: u64::MAX,
        }
    }

    pub fn with_error_tracking(mut self, max_errors: u64) -> Self {
        self.track_filtered = true;
        self.max_errors = max_errors;
        self
    }
}

// ─── Projection Engine ───────────────────────────────────────────────────────

pub struct ProjectionEngine;

impl ProjectionEngine {
    /// Execute projection with all enhancements
    pub fn execute(
        input: ProjectionInput,
        reorder: Option<ColumnReorderSpec>,
        filter_config: Option<FilterConfig>,
    ) -> io::Result<ProjectionResult> {
        let start = Instant::now();
        let mut metrics = ProjectionMetrics::new();
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Step 1: Resolve schema
        let (schema, is_cte) = resolve_schema_enhanced(
            input.catalog,
            input.db_name,
            input.table_name,
            &input.cte_tables,
        )?;
        metrics.memory_bytes += schema.len() * std::mem::size_of::<Column>();

        // Step 2: Column Elimination Analysis
        let mut pruner = ColumnPruner::new(schema.len());
        pruner.analyze_projection(&input.items);
        pruner.analyze_filter(&input.predicate);

        let should_eliminate = pruner.should_eliminate();
        let elimination_stats = pruner.elimination_stats();

        if should_eliminate {
            warnings.push(format!("Column elimination: {} of {} columns needed ({:.1}% savings)",
                                elimination_stats.1, elimination_stats.0, elimination_stats.2));
        }

        // Step 3: Load rows (with column elimination if beneficial)
        let rows = if is_cte {
            input.cte_tables[input.table_name].rows.clone()
        } else if should_eliminate {
            // Load only required columns
            load_rows_with_column_elimination(
                input.catalog,
                input.db_name,
                input.table_name,
                &pruner.get_required_indices(),
                &mut metrics
            )?
        } else {
            load_rows_with_metrics(input.catalog, input.db_name, input.table_name, &mut metrics)?
        };
        metrics.rows_processed = rows.len() as u64;

        // Step 3: Apply filter
        let fc = filter_config.unwrap_or_else(|| FilterConfig::new(input.predicate.clone()));
        let (filtered_rows, filter_errors) = filter_rows_with_tracking(rows, &fc)?;
        metrics.rows_filtered = metrics.rows_processed - filtered_rows.len() as u64;
        errors.extend(filter_errors.iter().cloned());

        // Step 4: Evaluate projection list
        let (mut out_cols, mut out_rows) = eval_projection_list_safe(
            filtered_rows,
            &input.items,
            &schema,
            &mut errors,
        )?;
        metrics.rows_output = out_rows.len() as u64;

        // Step 5: Apply column reordering if specified
        if let Some(reorder_spec) = reorder {
            match reorder_columns(&out_cols, &out_rows, &reorder_spec) {
                Ok((new_cols, new_rows)) => {
                    out_cols = new_cols;
                    out_rows = new_rows;
                }
                Err(e) => {
                    warnings.push(format!("Column reordering failed: {}", e));
                }
            }
        }

        // Step 6: Apply DISTINCT if requested
        if input.distinct {
            let before = out_rows.len();
            out_rows = apply_distinct_safe(out_rows);
            if out_rows.len() < before {
                warnings.push(format!("Removed {} duplicate rows", before - out_rows.len()));
            }
        }

        metrics.rows_output = out_rows.len() as u64;
        metrics.elapsed_ms = start.elapsed().as_millis();

        // Determine status
        let status = if errors.is_empty() {
            if warnings.is_empty() {
                ProjectionStatus::Success
            } else {
                ProjectionStatus::PartialSuccess {
                    error_count: 0,
                    warning_count: warnings.len() as u64,
                }
            }
        } else {
            if errors.len() <= 5 {
                ProjectionStatus::PartialSuccess {
                    error_count: errors.len() as u64,
                    warning_count: warnings.len() as u64,
                }
            } else {
                ProjectionStatus::Failed {
                    reason: format!("{} errors during projection", errors.len()),
                }
            }
        };

        Ok(ProjectionResult {
            status,
            data: ResultTable { columns: out_cols, rows: out_rows },
            metrics,
            errors,
            warnings,
        })
    }

    /// Simple execution (backward compatible)
    pub fn execute_simple(input: ProjectionInput) -> io::Result<ProjectionResult> {
        Self::execute(input, None, None)
    }
}

// ─── Helper Functions ───────────────────────────────────────────────────────

fn resolve_schema_enhanced<'a>(
    catalog: &'a Catalog,
    db_name: &str,
    table_name: &str,
    cte_tables: &HashMap<String, ResultTable>,
) -> io::Result<(Vec<Column>, bool)> {
    if cte_tables.contains_key(table_name) {
        let cte = &cte_tables[table_name];
        let cols: Vec<Column> = cte.columns.iter().map(|c| Column {
            name: c.name.clone(),
            data_type: c.data_type.as_legacy_str(),
        }).collect();
        return Ok((cols, true));
    }

    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name))
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name))
    })?;
    Ok((table.columns.clone(), false))
}

fn load_rows_with_metrics(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    metrics: &mut ProjectionMetrics,
) -> io::Result<Vec<Row>> {
    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name))
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name))
    })?;
    let schema = &table.columns;

    let path = TABLE_FILE_TEMPLATE
        .replace("{database}", db_name)
        .replace("{table}", table_name);

    let mut file = OpenOptions::new().read(true).open(&path).map_err(|e| {
        io::Error::new(e.kind(), format!("Cannot open table file '{}': {}", path, e))
    })?;

    let total_pages = page_count(&mut file)?;
    metrics.pages_read = total_pages as u64;

    if total_pages <= 1 {
        return Ok(vec![]);
    }

    let mut rows = Vec::new();
    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
            let tuple_bytes = &page.data[offset..offset + length];
            let row = decode_tuple(tuple_bytes, schema);
            metrics.memory_bytes += row.len() * std::mem::size_of::<Value>();
            rows.push(row);
        }
    }

    Ok(rows)
}

/// Load rows with column elimination - only reads required columns
fn load_rows_with_column_elimination(
    catalog: &Catalog,
    db_name: &str,
    table_name: &str,
    required_indices: &[usize],
    metrics: &mut ProjectionMetrics,
) -> io::Result<Vec<Row>> {
    let db = catalog.databases.get(db_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Database '{}' not found", db_name))
    })?;
    let table = db.tables.get(table_name).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Table '{}' not found", table_name))
    })?;
    let schema = &table.columns;

    let path = TABLE_FILE_TEMPLATE
        .replace("{database}", db_name)
        .replace("{table}", table_name);

    let mut file = OpenOptions::new().read(true).open(&path).map_err(|e| {
        io::Error::new(e.kind(), format!("Cannot open table file '{}': {}", path, e))
    })?;

    let total_pages = page_count(&mut file)?;
    metrics.pages_read = total_pages as u64;

    if total_pages <= 1 {
        return Ok(vec![]);
    }

    let mut rows = Vec::new();
    for page_num in 1..total_pages {
        let mut page = Page::new();
        read_page(&mut file, &mut page, page_num)?;

        let lower = u32::from_le_bytes(page.data[0..4].try_into().unwrap());
        let num_items = (lower - PAGE_HEADER_SIZE) / ITEM_ID_SIZE;

        for i in 0..num_items {
            let base = (PAGE_HEADER_SIZE + i * ITEM_ID_SIZE) as usize;
            let offset = u32::from_le_bytes(page.data[base..base + 4].try_into().unwrap()) as usize;
            let length = u32::from_le_bytes(page.data[base + 4..base + 8].try_into().unwrap()) as usize;
            let tuple_bytes = &page.data[offset..offset + length];
            let full_row = decode_tuple(tuple_bytes, schema);

            // Only include required columns
            let mut filtered_row = Vec::new();
            for &idx in required_indices {
                if idx < full_row.len() {
                    filtered_row.push(full_row[idx].clone());
                }
            }
            metrics.memory_bytes += filtered_row.len() * std::mem::size_of::<Value>();
            rows.push(filtered_row);
        }
    }

    Ok(rows)
}

fn filter_rows_with_tracking(
    rows: Vec<Row>,
    config: &FilterConfig,
) -> io::Result<(Vec<Row>, Vec<String>)> {
    let pred = match &config.predicate {
        None => return Ok((rows, vec![])),
        Some(p) => p,
    };

    let mut output = Vec::new();
    let mut errors = Vec::new();

    for (idx, row) in rows.into_iter().enumerate() {
        match eval_expr(pred, &row) {
            Ok(Value::Bool(true)) => output.push(row),
            Ok(_) => {} // Filtered out
            Err(e) => {
                if config.track_filtered {
                    let err_msg = format!("Row {}: {}", idx, e);
                    errors.push(err_msg);
                    if errors.len() >= config.max_errors as usize {
                        break;
                    }
                }
            }
        }
    }

    Ok((output, errors))
}

fn eval_projection_list_safe(
    rows: Vec<Row>,
    items: &[ProjectionItem],
    schema: &[Column],
    errors: &mut Vec<String>,
) -> io::Result<(Vec<OutputColumn>, Vec<Row>)> {
    let out_cols: Vec<OutputColumn> = items
        .iter()
        .flat_map(|item| match item {
            ProjectionItem::Star => schema
                .iter()
                .map(|c| OutputColumn {
                    name: c.name.clone(),
                    data_type: c.parsed_type(),
                })
                .collect::<Vec<_>>(),
            ProjectionItem::Expr(_, alias) => vec![OutputColumn {
                name: alias.clone(),
                data_type: DataType::Text,
            }],
        })
        .collect();

    if rows.is_empty() {
        return Ok((out_cols, vec![]));
    }

    let num_schema_cols = schema.len();
    let mut out_rows = Vec::with_capacity(rows.len());

    for (row_idx, row) in rows.iter().enumerate() {
        let mut out_row = Vec::new();
        for item in items {
            match item {
                ProjectionItem::Star => {
                    for i in 0..num_schema_cols {
                        out_row.push(row.get(i).cloned().unwrap_or(Value::Null));
                    }
                }
                ProjectionItem::Expr(expr, _) => {
                    match eval_expr(expr, row) {
                        Ok(v) => out_row.push(v),
                        Err(e) => {
                            errors.push(format!("Row {}: Expression eval failed: {}", row_idx, e));
                            out_row.push(Value::Null);
                        }
                    }
                }
            }
        }
        out_rows.push(out_row);
    }

    Ok((out_cols, out_rows))
}

fn reorder_columns(
    cols: &[OutputColumn],
    rows: &[Row],
    spec: &ColumnReorderSpec,
) -> Result<(Vec<OutputColumn>, Vec<Row>), String> {
    // Validate indices
    for &idx in &spec.indices {
        if idx >= cols.len() {
            return Err(format!("Column index {} out of bounds (max: {})", idx, cols.len() - 1));
        }
    }

    // Reorder column metadata
    let new_cols: Vec<OutputColumn> = spec.indices
        .iter()
        .enumerate()
        .map(|(new_idx, &old_idx)| {
            let mut col = cols[old_idx].clone();
            if let Some(ref names) = spec.new_names {
                if new_idx < names.len() {
                    col.name = names[new_idx].clone();
                }
            }
            col
        })
        .collect();

    // Reorder rows
    let new_rows: Vec<Row> = rows.iter().map(|row| {
        spec.indices
            .iter()
            .map(|&idx| row.get(idx).cloned().unwrap_or(Value::Null))
            .collect()
    }).collect();

    Ok((new_cols, new_rows))
}

fn apply_distinct_safe(rows: Vec<Row>) -> Vec<Row> {
    let mut seen: HashSet<Vec<Value>> = HashSet::new();
    let mut out = Vec::new();
    for row in rows {
        if seen.insert(row.clone()) {
            out.push(row);
        }
    }
    out
}

// ─── Integration with Temporary Files ────────────────────────────────────────

/// Save projection result to temporary file
pub fn save_projection_to_temp(
    result: &ProjectionResult,
    temp_dir: Option<&str>,
) -> io::Result<String> {
    use std::time::{SystemTime, UNIX_EPOCH};
    
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    
    let temp_path = if let Some(dir) = temp_dir {
        format!("{}/projection_{}.csv", dir, timestamp)
    } else {
        format!("/tmp/projection_{}.csv", timestamp)
    };

    let mut file = std::fs::File::create(&temp_path)?;
    use std::io::Write;

    // Write header
    let headers: Vec<&str> = result.data.columns.iter().map(|c| c.name.as_str()).collect();
    writeln!(file, "{}", headers.join(","))?;

    // Write rows
    for row in &result.data.rows {
        let cells: Vec<String> = row.iter().map(|v| v.to_string()).collect();
        writeln!(file, "{}", cells.join(","))?;
    }

    Ok(temp_path)
}
