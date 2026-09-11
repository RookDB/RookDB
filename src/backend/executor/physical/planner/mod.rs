//! Physical Planner — Converts a `LogicalPlan` into a tree of `PhysicalOperator` nodes.
//!
//! The planner walks the logical plan tree bottom-up and builds the corresponding
//! physical operator tree. It uses the catalog to resolve table schemas and file
//! paths needed for operators like `SeqScan`.

use std::path::PathBuf;

use rook_ast::logical::*;

use super::tuple::ColumnInfo;
use super::expr::{Predicate, Expr, expr_from_ast};
use crate::types::datatype::DataType;
use super::operators::{
    PhysicalOperator,
    SeqScanOperator,
    CteScanOperator,
    SingleRowOperator,
    IndexScanOperator,
    IndexScanMode,
    FilterOperator,
    ProjectionOperator,
    LimitOperator,
    DistinctOperator,
    SortOperator,
    SetOpOperator,
    SetOpType as PhysicalSetOpType,
    SubqueryExecOperator,
    SubqueryType as PhysicalSubqueryType,
    InsertOperator,
    ValuesOperator,
};

use crate::backend::catalog::types::Catalog;
use crate::backend::heap::heap_manager::HeapManager;
use crate::backend::index::btree::BTree;
use self::helpers::infer_expr_type_from_ast;

pub mod helpers;
pub mod subqueries;
pub mod joins;
pub mod index_scan;
pub mod cost;

/// Converts a `LogicalPlan` into an executable `PhysicalOperator` tree.
pub struct PhysicalPlanner {
    /// The catalog for schema lookups.
    pub(crate) catalog: Catalog,
    /// Current database name.
    pub(crate) db_name: String,
}

impl PhysicalPlanner {
    pub fn new(catalog: Catalog, db_name: String) -> Self {
        Self { catalog, db_name }
    }

    /// Plan a logical query plan into a physical operator tree.
    ///
    /// Returns the root of the operator tree.
    pub fn plan(&self, logical_plan: &LogicalPlan) -> Result<Box<dyn PhysicalOperator>, String> {
        // Start with an empty CTE registry — CTEs are materialised on demand
        // as the planner descends into LogicalCte nodes.
        let mut cte_registry: std::collections::HashMap<String, Vec<super::tuple::Tuple>> =
            std::collections::HashMap::new();
        self.plan_internal(logical_plan, &mut cte_registry)
    }

    pub(crate) fn plan_internal(
        &self,
        plan: &LogicalPlan,
        cte_registry: &mut std::collections::HashMap<String, Vec<super::tuple::Tuple>>,
    ) -> Result<Box<dyn PhysicalOperator>, String> {
        match plan {
            LogicalPlan::TableScan(ts) => self.plan_table_scan(ts),

            LogicalPlan::Filter(f) => {
                // Check if the filter's child is a TableScan that can use an
                // index-accelerated scan (PointLookup or RangeLookup) instead
                // of a SeqScan + FilterOperator.
                if let LogicalPlan::TableScan(ref ts) = *f.child
                    && let Some(scan_op) = self.try_plan_index_scan_with_predicate(ts, &f.predicate)? {
                        log::info!(
                            "[Volcano] Using index-accelerated scan for table '{}' (full predicate preserved on top)",
                            ts.table
                        );
                        // IMPORTANT: Always apply the full predicate as a FilterOperator
                        // on top of the index-accelerated scan. This guarantees correctness
                        // for compound AND predicates like `indexed_col = 5 AND other > 10`
                        // where only the indexed portion is used by the index scan and the
                        // non-indexed portion must still be applied as a filter.
                        let predicate = self.make_predicate(&f.predicate, scan_op.schema())?;
                        return Ok(Box::new(FilterOperator::new(scan_op, predicate)));
                    }

                // Fall through: create a SeqScan + FilterOperator
                let child = self.plan_internal(&f.child, cte_registry)?;
                let predicate = self.make_predicate(&f.predicate, child.schema())?;
                Ok(Box::new(FilterOperator::new(child, predicate)))
            }

            LogicalPlan::Project(p) => {
                let child = self.plan_internal(&p.child, cte_registry)?;
                let child_schema = child.schema();
                let column_names: Vec<String> = child_schema.iter().map(|c| c.name.clone()).collect();
                let child_types: Vec<_> = child_schema.iter().map(|c| c.data_type.clone()).collect();

                // Convert project expressions to physical expressions
                if p.expressions.is_empty() {
                    // No explicit expressions — treat as SELECT *
                    return Ok(Box::new(ProjectionOperator::star(child)));
                }

                let mut projections = Vec::new();
                for ne in &p.expressions {
                    let (e, data_type) = self.plan_projection_expr(&ne.expr, &column_names, &child_types)?;
                    projections.push((e, ne.name.clone(), data_type));
                }

                Ok(Box::new(ProjectionOperator::new(child, projections)))
            }

            LogicalPlan::Distinct(d) => {
                let child = self.plan_internal(&d.child, cte_registry)?;
                Ok(Box::new(DistinctOperator::new(child)))
            }

            LogicalPlan::Sort(s) => {
                let child = self.plan_internal(&s.child, cte_registry)?;
                // Clone the schema immediately to avoid borrowing `child` (which will be moved later)
                let child_schema: Vec<ColumnInfo> = child.schema().to_vec();
                let column_names: Vec<String> = child_schema.iter().map(|c| c.name.clone()).collect();
                let child_types: Vec<_> = child_schema.iter().map(|c| c.data_type.clone()).collect();

                // ── Build sort keys, handling complex expressions ──────────────
                //
                // For simple column references, we look up the column index directly.
                // For complex expressions (e.g. `ORDER BY a+b`), we:
                //   1. Plan the expression as an extra physical expression
                //   2. Project it as an additional hidden column
                //   3. Sort by the hidden column's index
                //   4. Strip the hidden column after sorting
                let mut sort_keys: Vec<(usize, bool)> = Vec::new();
                let mut extra_projections: Vec<(Expr, String, DataType)> = Vec::new();
                let mut has_complex = false;

                for ob in &s.order_by {
                    match &ob.expr {
                        rook_ast::ExprNode::Column(name) => {
                            let idx = column_names.iter().position(|n| n == name)
                                .ok_or_else(|| format!("Sort column '{}' not found", name))?;
                            sort_keys.push((idx, !ob.ascending));
                        }
                        rook_ast::ExprNode::Compound(parts) => {
                            let name = parts.last().cloned().unwrap_or_default();
                            let idx = column_names.iter().position(|n| n == &name)
                                .ok_or_else(|| format!("Sort column '{}' not found", name))?;
                            sort_keys.push((idx, !ob.ascending));
                        }
                        complex_expr => {
                            // Complex sort expression: project as a hidden column
                            let (phys_expr, data_type) = self.plan_projection_expr(
                                complex_expr, &column_names, &child_types,
                            )?;
                            let col_name = format!("__sort_col_{}", extra_projections.len());
                            let sort_idx = column_names.len() + extra_projections.len();
                            sort_keys.push((sort_idx, !ob.ascending));
                            extra_projections.push((phys_expr, col_name.clone(), data_type));
                            has_complex = true;
                        }
                    }
                }

                // ── Build the operator chain ──────────────────────────────────

                // Option 1: Project all existing columns + extra sort columns
                if has_complex {
                    let mut all_projections: Vec<(Expr, String, DataType)> = child_schema
                        .iter()
                        .map(|ci| {
                            let expr = Expr::Column { table: None, column: ci.name.clone() };
                            (expr, ci.name.clone(), ci.data_type.clone())
                        })
                        .collect();
                    all_projections.extend(extra_projections);
                    let child_with_extra = Box::new(ProjectionOperator::new(child, all_projections));

                    let sort_op = SortOperator::new(child_with_extra, sort_keys);

                    // Strip the extra sort columns
                    let result_schema: Vec<String> = child_schema.iter().map(|c| c.name.clone()).collect();
                    let indices: Vec<usize> = (0..column_names.len()).collect();

                    if let Some(limit) = s.limit {
                        log::info!("[Volcano] Sort has limit={}, wrapping in LimitOperator", limit);
                        let limited = Box::new(LimitOperator::new(
                            Box::new(sort_op), limit as usize, 0,
                        ));
                        Ok(Box::new(ProjectionOperator::from_indices(
                            limited, &indices, &result_schema,
                        )?))
                    } else {
                        Ok(Box::new(ProjectionOperator::from_indices(
                            Box::new(sort_op), &indices, &result_schema,
                        )?))
                    }
                } else {
                    // Simple case: all sort keys are column references
                    let sort_op = SortOperator::new(child, sort_keys);

                    if let Some(limit) = s.limit {
                        log::info!("[Volcano] Sort has limit={}, wrapping in LimitOperator", limit);
                        Ok(Box::new(LimitOperator::new(
                            Box::new(sort_op), limit as usize, 0,
                        )))
                    } else {
                        Ok(Box::new(sort_op))
                    }
                }
            }

            LogicalPlan::Limit(l) => {
                let child = self.plan_internal(&l.child, cte_registry)?;
                Ok(Box::new(LimitOperator::new(
                    child,
                    l.limit as usize,
                    l.offset as usize,
                )))
            }

            LogicalPlan::Aggregate(a) => {
                self.plan_aggregate_with_ctes(a, cte_registry)
            }

            LogicalPlan::Join(j) => {
                self.plan_join_with_ctes(j, cte_registry)
            }

            LogicalPlan::SetOp(s) => {
                let left = self.plan_internal(&s.left, cte_registry)?;
                let right = self.plan_internal(&s.right, cte_registry)?;

                let op_type = match s.op {
                    rook_ast::logical::SetOpType::Union => PhysicalSetOpType::Union,
                    rook_ast::logical::SetOpType::Intersect => PhysicalSetOpType::Intersect,
                    rook_ast::logical::SetOpType::Except => PhysicalSetOpType::Except,
                };

                log::info!(
                    "[Volcano] Planning {:?} (all={})",
                    op_type,
                    s.all
                );

                Ok(Box::new(SetOpOperator::new(left, right, op_type, s.all)))
            }

            LogicalPlan::Subquery(sq) => {
                // For FROM-clause subqueries (derived tables), plan the inner query
                // and optionally wrap in SubqueryExecOperator in table (passthrough) mode.
                let inner = self.plan_internal(&sq.subquery, cte_registry)?;

                // If the subquery has an alias, it's a derived table in FROM clause.
                // Just return the inner plan directly — it acts as a scan of the subquery result.
                if sq.alias.is_some() {
                    return Ok(inner);
                }

                // No alias means this is likely a scalar subquery used in an expression.
                // Wrap it in SubqueryExecOperator in Scalar mode.
                log::info!("[Volcano] Wrapping subquery in Scalar SubqueryExecOperator");
                Ok(Box::new(SubqueryExecOperator::new(
                    inner,
                    PhysicalSubqueryType::Scalar,
                    None,
                )))
            }

            // ── Recursive CTE node ──────────────────────────────────────────

            LogicalPlan::RecursiveCte(rc) => {
                self.plan_recursive_cte(rc, cte_registry)
            }

            // ── CTE nodes ────────────────────────────────────────────────────

            LogicalPlan::Cte(c) => {
                // 1. Materialise the CTE inner plan into an in-memory Vec<Tuple>.
                //
                // We fully execute the inner plan now (before planning the outer
                // query) so that any CteScan leaves in the outer query can look
                // up the cached tuples in O(1) without re-executing the CTE body.
                let mut inner_op = self.plan_internal(&c.inner, cte_registry)?;
                let mut tuples = Vec::new();
                while let Some(tuple) = inner_op.next()? {
                    tuples.push(tuple);
                }
                log::info!(
                    "[Volcano] CTE '{}' materialised {} tuples",
                    c.name,
                    tuples.len()
                );

                // 2. Register the materialised tuples so CteScan leaves can find them.
                cte_registry.insert(c.name.to_ascii_lowercase(), tuples);

                // 3. Plan the outer query (which may contain CteScan leaves that
                //    reference the CTE we just registered).
                self.plan_internal(&c.outer, cte_registry)
            }

            LogicalPlan::Insert(inp) => {
                // VALUES form: the logical planner already expanded rows to
                // full table arity; feed them via a constant-producing child.
                let child: Box<dyn PhysicalOperator> = if !inp.values_rows.is_empty() {
                    let table_schema = self.resolve_table_schema(&inp.table)?;
                    let schema: Vec<ColumnInfo> = table_schema
                        .iter()
                        .map(|c| ColumnInfo {
                            name: c.name.clone(),
                            data_type: c.data_type.clone(),
                            table: Some(inp.table.clone()),
                        })
                        .collect();
                    let compiled = inp
                        .values_rows
                        .iter()
                        .map(|row| {
                            row.iter()
                                .map(|node| {
                                    expr_from_ast(node, &[]).map_err(|e| e.to_string())
                                })
                                .collect::<Result<Vec<_>, _>>()
                        })
                        .collect::<Result<Vec<_>, String>>()?;
                    Box::new(ValuesOperator::new(compiled, schema))
                } else {
                    self.plan_internal(&inp.child, cte_registry)?
                };

                log::info!(
                    "[Volcano] Planning Insert into '{}' with child operator '{}'",
                    inp.table,
                    child.name()
                );
                Ok(Box::new(InsertOperator::new(
                    child,
                    inp.table.clone(),
                    self.db_name.clone(),
                    self.catalog.clone(),
                )))
            }

            LogicalPlan::CteScan(cs) => {
                // Retrieve the pre-materialised tuples from the CTE registry.
                // For recursive CTEs, `plan_recursive_cte` updates the registry before
                // each iteration, so the operator always sees the latest working table.
                let tuples = cte_registry
                    .get(&cs.name.to_ascii_lowercase())
                    .cloned()
                    .ok_or_else(|| format!("CTE '{}' not found in registry during physical planning", cs.name))?;

                // Build the schema for CteScanOperator from the CteScan node.
                // The CTE name serves as the table qualifier so that table-qualified
                // references (e.g. `my_cte.col`) and NATURAL JOIN disambiguation work
                // correctly when a CTE is on the right side of a join.
                let cte_table_name = cs.name.clone();
                let schema: Vec<super::tuple::ColumnInfo> = cs.schema.columns.iter().map(|c| {
                    super::tuple::ColumnInfo {
                        name: c.name.clone(),
                        data_type: crate::types::datatype::DataType::Varchar(u16::MAX),
                        table: Some(cte_table_name.clone()),
                    }
                }).collect();

                log::info!(
                    "[Volcano] CteScan '{}': {} tuples available",
                    cs.name,
                    tuples.len()
                );

                Ok(Box::new(CteScanOperator::new(tuples, schema)))
            }
        }
    }
}

// ── Table scan planning ───────────────────────────────────────────────────────

impl PhysicalPlanner {
    /// Create a table scan operator for the given LogicalTableScan.
    ///
    /// If the scan targets a system table (`ts.system_table_name.is_some()`),
    /// the heap path is resolved to `database/system/{name}.dat` and the
    /// column schema for the output tuples is drawn from the system table's
    /// definition rather than the user catalog.
    ///
    /// Otherwise, if a `.idx` file exists alongside the `.dat` heap file,
    /// uses an `IndexScanOperator` with `FullScan` mode instead of a
    /// sequential scan.  This allows the B+ Tree index to drive tuple
    /// retrieval in key order.
    pub(crate) fn plan_table_scan(&self, ts: &LogicalTableScan) -> Result<Box<dyn PhysicalOperator>, String> {
        // ── System table path ─────────────────────────────────────────────
        if let Some(sys_name) = &ts.system_table_name {
            // Virtual single-row table: no heap file needed
            if sys_name == "__singlerow__" {
                return Ok(Box::new(SingleRowOperator::new()));
            }
            let heap_path = PathBuf::from(format!(
                "database/system/{}.dat",
                sys_name
            ));
            if !heap_path.exists() {
                return Err(format!("System table heap file not found: {:?}", heap_path));
            }
            // Use the system table's actual physical schema for deserialisation
            // (so INT columns are decoded as Int, BOOL as Bool, etc.) but keep
            // the INFORMATION_SCHEMA view column names for display.
            //
            // IMPORTANT: Some INFORMATION_SCHEMA views (especially COLUMNS) have
            // column names that do NOT positionally align with the physical system
            // table columns.  For example, sys_columns has physical columns
            // [col_id, table_id, name, data_type, ordinal, nullable, has_default,
            //  default_value] but the COLUMNS view exposes [table_catalog,
            //  table_schema, table_name, column_name, ordinal_position, data_type,
            //  is_nullable, column_default].  We must map each info_schema column
            //  to the correct physical column index and type.
            //
            // Use ts.schema (view column names, set by the logical planner) instead
            // of info_schema_column_schema(sys_name) because sys_name is the system
            // table name (e.g. "databases") while ts.schema holds the correct view
            // column names (e.g. "catalog_name", "schema_name" for SCHEMATA).
            let info_schema = &ts.schema;
            let sys_schema = crate::backend::system_table::system_table_schema(sys_name);
            // Map info_schema column names to physical column types + positions
            let (mut column_info, column_mapping) = self.map_info_schema_columns(sys_name, info_schema, sys_schema);
            // Set the table name for each column to allow table-qualified resolution
            // (alias takes precedence per SQL semantics).
            let sys_table_name = ts.alias.clone().unwrap_or_else(|| sys_name.to_string());
            for ci in column_info.iter_mut() {
                ci.table = Some(sys_table_name.clone());
            }
            let schema_types: Vec<DataType> = sys_schema.to_vec();
            let heap_manager = HeapManager::open(heap_path)
                .map_err(|e| format!("Failed to open system table '{}': {}", sys_name, e))?;
            return Ok(Box::new(SeqScanOperator::new_with_mapping(
                heap_manager,
                schema_types,
                column_info,
                column_mapping,
            )));
        }

        // ── Regular user table path ───────────────────────────────────────
        let table_schema = self.resolve_table_schema(&ts.table)?;
        // Stamp the ALIAS (when present) as the tuple qualifier — SQL
        // semantics: once a table is aliased, references must use the alias.
        // This also lets self-joins disambiguate two scans of one table.
        let table_name = ts.alias.clone().unwrap_or_else(|| ts.table.clone());
        let column_info: Vec<ColumnInfo> = table_schema.iter().map(|c| {
            ColumnInfo {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                table: Some(table_name.clone()),
            }
        }).collect();

        let heap_path = PathBuf::from(format!(
            "database/base/{}/{}.dat",
            self.db_name, ts.table
        ));

        if !heap_path.exists() {
            return Err(format!("Heap file not found: {:?}", heap_path));
        }

        // Check for any index files (named or legacy) alongside the heap file
        // First try loading named indexes from sys_indexes
        let named_indexes = crate::backend::executor::create_index::load_table_indexes(
            &self.db_name, &ts.table
        ).unwrap_or_default();

        if !named_indexes.is_empty() {
            // Use the first named index for the full scan
            let first_idx_name = &named_indexes[0].0;
            let index_path = PathBuf::from(format!(
                "database/base/{}/{}.{}.idx",
                self.db_name, ts.table, first_idx_name
            ));

            if index_path.exists() {
                log::info!(
                    "[Volcano] Named index '{}' file found at {:?}, using IndexScanOperator with FullScan",
                    first_idx_name, index_path
                );

                crate::backend::cache::checkpoint();
                let mut btree = BTree::open(index_path)
                    .map_err(|e| format!("Failed to open index for table '{}': {}", ts.table, e))?;

                // Set the key type from the indexed column (NOT the first table column)
                let idx_col_name = &named_indexes[0].1;
                if let Some(idx_col) = table_schema.iter().find(|c| c.name.eq_ignore_ascii_case(idx_col_name)) {
                    btree.set_key_type(idx_col.data_type.clone());
                } else if let Some(first_col) = table_schema.first() {
                    btree.set_key_type(first_col.data_type.clone());
                }

                let heap_manager = HeapManager::open(heap_path)
                    .map_err(|e| format!("Failed to open heap for table '{}': {}", ts.table, e))?;

                return Ok(Box::new(IndexScanOperator::new(
                    btree,
                    heap_manager,
                    IndexScanMode::FullScan,
                    column_info,
                )));
            }
        }

        // Fallback: check for legacy single-index file
        let legacy_index_path = PathBuf::from(format!(
            "database/base/{}/{}.idx",
            self.db_name, ts.table
        ));

        if legacy_index_path.exists() {
            log::info!(
                "[Volcano] Legacy index file found at {:?}, using IndexScanOperator with FullScan",
                legacy_index_path
            );

            crate::backend::cache::checkpoint();
            let mut btree = BTree::open(legacy_index_path)
                .map_err(|e| format!("Failed to open legacy index for table '{}': {}", ts.table, e))?;

            if let Some(first_col) = table_schema.first() {
                btree.set_key_type(first_col.data_type.clone());
            }

            let heap_manager = HeapManager::open(heap_path)
                .map_err(|e| format!("Failed to open heap for table '{}': {}", ts.table, e))?;

            return Ok(Box::new(IndexScanOperator::new(
                btree,
                heap_manager,
                IndexScanMode::FullScan,
                column_info,
            )));
        }

        log::info!(
            "[Volcano] No index file found for table '{}', using SeqScan",
            ts.table
        );

        let heap_manager = HeapManager::open(heap_path)
            .map_err(|e| format!("Failed to open heap for table '{}': {}", ts.table, e))?;

        Ok(Box::new(SeqScanOperator::new(heap_manager, column_info)))
    }
}

// ── Schema helpers ────────────────────────────────────────────────────────────

impl PhysicalPlanner {
    /// Return the column-to-physical-index mapping for a given info_schema view.
    ///
    /// Many INFORMATION_SCHEMA views expose column names that do NOT positionally
    /// align with the physical system table columns (e.g. "columns" view's
    /// `column_name` maps to physical index 2, not 3).  This function returns
    /// the correct `(view_col_idx -> phys_col_idx)` mapping so the SeqScan
    /// operator can reorder deserialised values to match view column order.
    fn info_schema_column_mapping(sys_name: &str) -> Vec<usize> {
        match sys_name.to_ascii_lowercase().as_str() {
            // sys_columns: [col_id(0), table_id(1), name(2), data_type(3),
            //               ordinal(4), nullable(5), has_default(6), default_value(7)]
            // COLUMNS view: [table_catalog, table_schema, table_name, column_name,
            //                ordinal_position, data_type, is_nullable, column_default]
            "columns" => vec![0, 1, 2, 2, 4, 3, 5, 7],

            // sys_constraints: [constraint_id(0), table_id(1), type(2),
            //                   columns(3), ref_table(4), ref_columns(5)]
            // TABLE_CONSTRAINTS view: [constraint_catalog, constraint_schema,
            //                          constraint_name, table_name,
            //                          constraint_type, constraint_columns]
            "constraints" | "table_constraints" => vec![0, 1, 2, 3, 2, 3],

            // sys_indexes: [index_id(0), table_id(1), name(2), is_unique(3),
            //               is_primary(4), columns(5)]
            // INDEXES/STATISTICS view: [table_catalog, table_schema, table_name,
            //                          index_name, unique, primary]
            "indexes" | "statistics" => vec![0, 1, 2, 2, 3, 4],

            // Default: empty = identity mapping (positions align).
            // Caller will generate identity based on sys_schema length.
            _ => Vec::new(),
        }
    }

    /// Map INFORMATION_SCHEMA view column names to the correct physical column types.
    ///
    /// Returns `(Vec<ColumnInfo>, Vec<usize>)` where:
    /// - `ColumnInfo` contains view column names with physical DataTypes
    /// - `Vec<usize>` maps each view column position to its physical column index
    ///
    /// The `SeqScanOperator` uses the physical DataTypes for correct deserialisation
    /// and applies the mapping to reorder values into view column order.
    pub(crate) fn map_info_schema_columns(
        &self,
        sys_name: &str,
        info_schema: &rook_ast::logical::ColumnSchema,
        sys_schema: &[DataType],
    ) -> (Vec<ColumnInfo>, Vec<usize>) {
        let mut mapping = Self::info_schema_column_mapping(sys_name);

        // If no explicit mapping (empty vec), use identity mapping: each view
        // column maps to the same physical position.  This is correct for views
        // like "tables", "schemata", "views" where columns align positionally.
        if mapping.is_empty() {
            mapping = (0..sys_schema.len()).collect();
        }

        let column_info: Vec<ColumnInfo> = info_schema.columns.iter().zip(mapping.iter())
            .map(|(c, &phys_idx)| {
                let dt = sys_schema.get(phys_idx)
                    .cloned()
                    .unwrap_or(DataType::Varchar(255));
                ColumnInfo {
                    name: c.name.clone(),
                    data_type: dt,
                    table: None,
                }
            })
            .collect();

        (column_info, mapping)
    }

    /// Resolve the table's schema from the catalog.
    pub(crate) fn resolve_table_schema(&self, table_name: &str) -> Result<Vec<crate::backend::catalog::types::Column>, String> {
        let db = self.catalog.databases.get(&self.db_name)
            .ok_or_else(|| format!("Database '{}' not found", self.db_name))?;
        let table = db.tables.get(table_name)
            .ok_or_else(|| format!("Table '{}' not found in database '{}'", table_name, self.db_name))?;
        Ok(table.columns.clone())
    }

    /// Build a predicate from the AST predicate node, resolving column names
    /// against the child operator's schema. Handles subqueries (EXISTS, IN)
    /// by materializing them during physical plan construction.
    pub(crate) fn make_predicate(
        &self,
        pred_node: &rook_ast::PredicateNode,
        schema: &[ColumnInfo],
    ) -> Result<Predicate, String> {
        let column_names: Vec<String> = schema.iter().map(|c| c.name.clone()).collect();
        self.build_predicate_with_subqueries(pred_node, &column_names, schema)
    }
}

// ── Projection expression planning ────────────────────────────────────────────

impl PhysicalPlanner {
    /// Plan a single projection expression, handling `ScalarSubquery` nodes
    /// directly without going through the ConstantValue round-trip.
    pub(crate) fn plan_projection_expr(
        &self,
        expr: &rook_ast::ExprNode,
        column_names: &[String],
        child_types: &[DataType],
    ) -> Result<(Expr, DataType), String> {
        match expr {
            // Top-level scalar subquery: materialize and create Expr::Constant directly
            rook_ast::ExprNode::ScalarSubquery(info) => {
                let (value, data_type) = self.materialize_scalar_subquery(&info.select)?;
                let phys_expr = match value {
                    Some(dv) => Expr::Constant(dv),
                    None => Expr::Null,
                };
                Ok((phys_expr, data_type))
            }
            // Nested subqueries inside Binary or Cast need AST replacement first
            rook_ast::ExprNode::Binary { left, op, right } => {
                let left = self.materialize_nested_subqueries(left);
                let right = self.materialize_nested_subqueries(right);
                // Now convert the materialized AST (no more subqueries inside)
                let left_expr = expr_from_ast(&left?, column_names)?;
                let right_expr = expr_from_ast(&right?, column_names)?;
                let phys_op = match op {
                    rook_ast::ArithOp::Add => Expr::Add(Box::new(left_expr), Box::new(right_expr)),
                    rook_ast::ArithOp::Sub => Expr::Sub(Box::new(left_expr), Box::new(right_expr)),
                    rook_ast::ArithOp::Mul => Expr::Mul(Box::new(left_expr), Box::new(right_expr)),
                    rook_ast::ArithOp::Div => Expr::Div(Box::new(left_expr), Box::new(right_expr)),
                };
                let data_type = infer_expr_type_from_ast(expr, child_types, column_names)?;
                Ok((phys_op, data_type))
            }
            rook_ast::ExprNode::Cast { expr: inner, data_type } => {
                let inner = self.materialize_nested_subqueries(inner)?;
                let inner_expr = expr_from_ast(&inner, column_names)?;
                let target_dt: DataType = data_type.parse()
                    .map_err(|e: String| format!("Invalid CAST target type '{}': {}", data_type, e))?;
                Ok((Expr::Cast(Box::new(inner_expr), target_dt.clone()), target_dt))
            }
            // All other expression types: use the standard converter
            _ => {
                let e = expr_from_ast(expr, column_names)?;
                let data_type = infer_expr_type_from_ast(expr, child_types, column_names)?;
                Ok((e, data_type))
            }
        }
    }
}

// ── Index-accelerated scan planning (M1 + M2) ────────────────────────────────


// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rook_ast::{PredicateNode, ExprNode, ComparisonOp, BinaryOp, ConstantValue};

    fn col(name: &str) -> ExprNode { ExprNode::Column(name.to_string()) }
    fn compound(parts: &[&str]) -> ExprNode {
        ExprNode::Compound(parts.iter().map(|s| s.to_string()).collect())
    }
    fn constant_int(v: i64) -> ExprNode { ExprNode::Constant(ConstantValue::Int(v)) }
    fn constant_float(v: f64) -> ExprNode { ExprNode::Constant(ConstantValue::Float(v)) }
    fn constant_text(s: &str) -> ExprNode { ExprNode::Constant(ConstantValue::Text(s.to_string())) }
    #[allow(dead_code)]
    fn constant_bool(b: bool) -> ExprNode { ExprNode::Constant(ConstantValue::Boolean(b)) }

    fn eq_pred(l: ExprNode, r: ExprNode) -> PredicateNode {
        PredicateNode::Compare { left: Box::new(l), op: ComparisonOp::Eq, right: Box::new(r) }
    }
    fn gt_pred(l: ExprNode, r: ExprNode) -> PredicateNode {
        PredicateNode::Compare { left: Box::new(l), op: ComparisonOp::Gt, right: Box::new(r) }
    }
    fn and_pred(l: PredicateNode, r: PredicateNode) -> PredicateNode {
        PredicateNode::BinaryOp { left: Box::new(l), op: BinaryOp::And, right: Box::new(r) }
    }
    fn not_pred(inner: PredicateNode) -> PredicateNode {
        PredicateNode::Not(Box::new(inner))
    }

    /// Create a dummy PhysicalPlanner for testing private methods.
    fn make_planner() -> PhysicalPlanner {
        use std::collections::HashMap;
        PhysicalPlanner {
            catalog: Catalog { databases: HashMap::new() },
            db_name: "test".to_string(),
        }
    }

    // ── extract_index_mode_from_predicate ─────────────────────────────────

    #[test]
    fn test_extract_point_lookup_simple_eq() {
        let planner = make_planner();
        let pred = eq_pred(col("id"), constant_int(42));
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_some(), "Equality on indexed column should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(42));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_point_lookup_constant_on_left() {
        let planner = make_planner();
        let pred = eq_pred(constant_int(99), col("age"));
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "constant = col should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(99));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_non_indexed_column_returns_none() {
        let planner = make_planner();
        let pred = eq_pred(col("name"), constant_int(1));
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_none(), "Non-indexed column should not match");
    }

    #[test]
    fn test_extract_and_predicate_first_side_matches() {
        let planner = make_planner();
        let pred = and_pred(
            eq_pred(col("id"), constant_int(5)),
            eq_pred(col("name"), constant_text("hello")),
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_some(), "First side of AND should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(5));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_and_predicate_second_side_matches() {
        let planner = make_planner();
        let pred = and_pred(
            eq_pred(col("name"), constant_text("hello")),
            eq_pred(col("id"), constant_int(10)),
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_some(), "Second side of AND should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(10));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_and_predicate_neither_side_matches() {
        let planner = make_planner();
        let pred = and_pred(
            eq_pred(col("name"), constant_text("hello")),
            eq_pred(col("age"), constant_int(30)),
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_none(), "Neither side matches indexed column 'id'");
    }

    #[test]
    fn test_extract_not_predicate_returns_none() {
        let planner = make_planner();
        let pred = not_pred(eq_pred(col("id"), constant_int(5)));
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_none(), "NOT predicate should not match");
    }

    #[test]
    fn test_extract_gt_returns_range_lookup() {
        let planner = make_planner();
        // col > 18 → RangeLookup(19, MAX) for INT
        let pred = gt_pred(col("age"), constant_int(18));
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "Range operator should now match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(19), "Gt should increment the bound");
                assert_eq!(high, crate::types::value::DataValue::Int(i32::MAX), "Gt should use MAX sentinel");
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_ge_uses_value_as_low() {
        let planner = make_planner();
        // col >= 18 → RangeLookup(18, MAX) for INT
        let pred = PredicateNode::Compare {
            left: Box::new(col("age")),
            op: ComparisonOp::Ge,
            right: Box::new(constant_int(18)),
        };
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "Ge should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(18), "Ge should NOT increment the bound");
                assert_eq!(high, crate::types::value::DataValue::Int(i32::MAX));
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_lt_returns_range_lookup() {
        let planner = make_planner();
        // col < 10 → RangeLookup(MIN, 9) for INT
        let pred = PredicateNode::Compare {
            left: Box::new(col("age")),
            op: ComparisonOp::Lt,
            right: Box::new(constant_int(10)),
        };
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "Lt should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(i32::MIN));
                assert_eq!(high, crate::types::value::DataValue::Int(9), "Lt should decrement the bound");
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_le_uses_value_as_high() {
        let planner = make_planner();
        // col <= 10 → RangeLookup(MIN, 10) for INT
        let pred = PredicateNode::Compare {
            left: Box::new(col("age")),
            op: ComparisonOp::Le,
            right: Box::new(constant_int(10)),
        };
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "Le should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(i32::MIN));
                assert_eq!(high, crate::types::value::DataValue::Int(10), "Le should NOT decrement the bound");
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_gt_float_range() {
        let planner = make_planner();
        // salary > 50000.0 → RangeLookup(nextafter(50000.0), MAX) for DOUBLE
        let pred = gt_pred(col("salary"), constant_float(50000.0));
        let mode = planner.extract_index_mode_from_predicate(&pred, "salary", &DataType::DoublePrecision);
        assert!(mode.is_some(), "Gt on float should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                match low {
                    crate::types::value::DataValue::DoublePrecision(v) => {
                        assert!(v.0 > 50000.0, "Gt on float should use next-up value");
                    }
                    _ => panic!("Expected DoublePrecision"),
                }
                match high {
                    crate::types::value::DataValue::DoublePrecision(v) => {
                        assert_eq!(v.0, f64::MAX);
                    }
                    _ => panic!("Expected DoublePrecision"),
                }
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_gt_bigint_range() {
        let planner = make_planner();
        // big_col > 1_000_000_000_000 → RangeLookup(1_000_000_000_001, MAX) for BIGINT
        let pred = gt_pred(col("big_col"), constant_int(1_000_000_000_000));
        let mode = planner.extract_index_mode_from_predicate(&pred, "big_col", &DataType::BigInt);
        assert!(mode.is_some(), "Gt on BigInt should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::BigInt(1_000_000_000_001));
                assert_eq!(high, crate::types::value::DataValue::BigInt(i64::MAX));
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_and_combines_two_range_lookups() {
        let planner = make_planner();
        // AND(col > 5, col < 10) on same column → RangeLookup(6, 9)
        let pred = and_pred(
            gt_pred(col("age"), constant_int(5)),
            PredicateNode::Compare {
                left: Box::new(col("age")),
                op: ComparisonOp::Lt,
                right: Box::new(constant_int(10)),
            },
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "AND of two ranges should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(6),
                    "Combined low should be max(6, MIN) = 6");
                assert_eq!(high, crate::types::value::DataValue::Int(9),
                    "Combined high should be min(MAX, 9) = 9");
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_and_combines_ge_and_le() {
        let planner = make_planner();
        // AND(col >= 5, col <= 10) → RangeLookup(5, 10)
        let pred = and_pred(
            PredicateNode::Compare {
                left: Box::new(col("age")),
                op: ComparisonOp::Ge,
                right: Box::new(constant_int(5)),
            },
            PredicateNode::Compare {
                left: Box::new(col("age")),
                op: ComparisonOp::Le,
                right: Box::new(constant_int(10)),
            },
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "AND of Ge+Le should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(5));
                assert_eq!(high, crate::types::value::DataValue::Int(10));
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_and_combines_eq_and_gt() {
        let planner = make_planner();
        // AND(age = 7, age > 5) → PointLookup(7) (more selective)
        let pred = and_pred(
            eq_pred(col("age"), constant_int(7)),
            gt_pred(col("age"), constant_int(5)),
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "AND of Eq+Gt should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(7),
                    "PointLookup should be preferred over RangeLookup");
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_compound_column() {
        let planner = make_planner();
        let pred = eq_pred(compound(&["users", "id"]), constant_int(42));
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_some(), "Compound column should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(42));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_varchar_equality() {
        let planner = make_planner();
        let pred = eq_pred(col("name"), constant_text("Alice"));
        let mode = planner.extract_index_mode_from_predicate(&pred, "name", &DataType::Varchar(255));
        assert!(mode.is_some(), "Varchar equality should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Varchar("Alice".to_string()));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_float_equality() {
        let planner = make_planner();
        let pred = eq_pred(col("salary"), constant_float(75000.5));
        let mode = planner.extract_index_mode_from_predicate(&pred, "salary", &DataType::DoublePrecision);
        assert!(mode.is_some(), "Float equality should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                match dv {
                    crate::types::value::DataValue::DoublePrecision(v) => {
                        assert!((v.0 - 75000.5).abs() < 0.001, "Unexpected value: {}", v.0);
                    }
                    _ => panic!("Expected DoublePrecision"),
                }
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_case_insensitive_column_match() {
        let planner = make_planner();
        // Indexed column is "ID" but predicate uses "id"
        let pred = eq_pred(col("id"), constant_int(1));
        let mode = planner.extract_index_mode_from_predicate(&pred, "ID", &DataType::Int);
        assert!(mode.is_some(), "Column matching should be case-insensitive");
    }

    // ── scan_mode_priority ────────────────────────────────────────────────

    #[test]
    fn test_scan_mode_priority_ordering() {
        let point = IndexScanMode::PointLookup(crate::types::value::DataValue::Int(0));
        let full = IndexScanMode::FullScan;

        assert_eq!(PhysicalPlanner::scan_mode_priority(&point), 0,
            "PointLookup should have highest priority (0)");
        assert_eq!(PhysicalPlanner::scan_mode_priority(&full), 2,
            "FullScan should have lowest priority (2)");
        assert!(
            PhysicalPlanner::scan_mode_priority(&point) < PhysicalPlanner::scan_mode_priority(&full),
            "PointLookup priority should be higher (lower number) than FullScan"
        );
    }

    // ── ast_constant_to_data_value ────────────────────────────────────────

    #[test]
    fn test_ast_constant_int_to_data_value() {
        let dv = PhysicalPlanner::ast_constant_to_data_value(&ConstantValue::Int(42));
        assert_eq!(dv, crate::types::value::DataValue::Int(42));
    }

    #[test]
    fn test_ast_constant_bigint_to_data_value() {
        // Larger than i32::MAX should become BigInt
        let dv = PhysicalPlanner::ast_constant_to_data_value(&ConstantValue::Int(3_000_000_000));
        assert_eq!(dv, crate::types::value::DataValue::BigInt(3_000_000_000));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_ast_constant_float_to_data_value() {
        let dv = PhysicalPlanner::ast_constant_to_data_value(&ConstantValue::Float(3.14));
        match dv {
            crate::types::value::DataValue::DoublePrecision(v) => {
                assert!((v.0 - 3.14).abs() < 0.001);
            }
            _ => panic!("Expected DoublePrecision"),
        }
    }

    #[test]
    fn test_ast_constant_text_to_data_value() {
        let dv = PhysicalPlanner::ast_constant_to_data_value(&ConstantValue::Text("hello".to_string()));
        assert_eq!(dv, crate::types::value::DataValue::Varchar("hello".to_string()));
    }

    #[test]
    fn test_ast_constant_bool_to_data_value() {
        let dv = PhysicalPlanner::ast_constant_to_data_value(&ConstantValue::Boolean(true));
        assert_eq!(dv, crate::types::value::DataValue::Bool(true));
    }

    #[test]
    fn test_ast_constant_null_to_data_value() {
        let dv = PhysicalPlanner::ast_constant_to_data_value(&ConstantValue::Null);
        assert_eq!(dv, crate::types::value::DataValue::Int(0), "Null defaults to Int(0)");
    }

    // ── try_plan_index_scan_with_predicate edge cases ─────────────────────

    #[test]
    fn test_try_index_scan_system_table_returns_none() {
        // System tables are skipped (e.g., information_schema)
        let planner = make_planner();
        let ts = LogicalTableScan {
            table: "tables".to_string(),
            alias: None,
            schema: ColumnSchema::empty(),
            system_table_name: Some("tables".to_string()),
        };
        let pred = eq_pred(col("table_name"), constant_text("users"));
        let result = planner.try_plan_index_scan_with_predicate(&ts, &pred)
            .expect("Should not error");
        assert!(result.is_none(), "System tables should not use index-accelerated scans");
    }

    #[test]
    fn test_try_index_scan_nonexistent_table_returns_none() {
        // Table not found in catalog → graceful None
        let planner = make_planner();
        let ts = LogicalTableScan {
            table: "ghost_table".to_string(),
            alias: None,
            schema: ColumnSchema::empty(),
            system_table_name: None,
        };
        let pred = eq_pred(col("id"), constant_int(1));
        let result = planner.try_plan_index_scan_with_predicate(&ts, &pred)
            .expect("Should not error");
        assert!(result.is_none(), "Non-existent table should return None gracefully");
    }

    #[test]
    fn test_try_index_scan_nonexistent_table_is_graceful() {
        // Table doesn't exist in catalog → graceful None (not panic)
        let planner = make_planner();
        let ts = LogicalTableScan {
            table: "ghost".to_string(),
            alias: None,
            schema: ColumnSchema::empty(),
            system_table_name: None,
        };
        let pred = eq_pred(col("id"), constant_int(1));
        let result = planner.try_plan_index_scan_with_predicate(&ts, &pred);
        assert!(result.is_ok(), "Should not error on non-existent table");
        assert!(result.unwrap().is_none(), "Should return None gracefully");
    }

    // ── Compound AND predicate correctness ─────────────────────────────────

    #[test]
    fn test_extract_and_predicate_preserves_non_indexed_condition() {
        // This validates that extract_index_mode_from_predicate returns the
        // indexed portion of an AND predicate. The full predicate is ALWAYS
        // applied as a FilterOperator on top of the index scan, so the
        // non-indexed portion is preserved. This test verifies we correctly
        // extract the indexed-match portion.
        let planner = make_planner();

        // AND: indexed_col = 5 AND non_indexed_col > 10
        // Only the indexed part should be returned by extract
        let pred = and_pred(
            eq_pred(col("salary"), constant_float(50000.0)),
            gt_pred(col("age"), constant_int(30)),
        );

        let mode = planner.extract_index_mode_from_predicate(&pred, "salary", &DataType::DoublePrecision);
        assert!(mode.is_some(), "Should match the indexed column part of AND");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::DoublePrecision(
                    crate::types::value::OrderedF64(50000.0)
                ), "Should extract the indexed side of the AND");
            }
            other => panic!("Expected PointLookup for indexed column, got {:?}", other),
        }

        // NOTE: The non-indexed part (age > 30) is preserved by the FilterOperator
        // that wraps the index scan in plan_internal. This test just verifies
        // that extract_index_mode_from_predicate returns the correct indexed portion.
    }

    // ── BETWEEN index range pushdown (L1) ─────────────────────────────────

    fn between_pred(expr: ExprNode, low: ExprNode, high: ExprNode) -> PredicateNode {
        PredicateNode::Between {
            expr: Box::new(expr),
            low: Box::new(low),
            high: Box::new(high),
        }
    }

    fn inlist_pred(expr: ExprNode, items: Vec<ExprNode>) -> PredicateNode {
        PredicateNode::InList {
            expr: Box::new(expr),
            list: items,
        }
    }

    #[test]
    fn test_extract_between_int_range() {
        let planner = make_planner();
        // age BETWEEN 18 AND 65 → RangeLookup(18, 65)
        let pred = between_pred(col("age"), constant_int(18), constant_int(65));
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "BETWEEN on indexed column should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(18));
                assert_eq!(high, crate::types::value::DataValue::Int(65));
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_between_float_range() {
        let planner = make_planner();
        // salary BETWEEN 30000.0 AND 80000.0 → RangeLookup(30000.0, 80000.0)
        let pred = between_pred(col("salary"), constant_float(30000.0), constant_float(80000.0));
        let mode = planner.extract_index_mode_from_predicate(&pred, "salary", &DataType::DoublePrecision);
        assert!(mode.is_some(), "BETWEEN on indexed column should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                match (low, high) {
                    (crate::types::value::DataValue::DoublePrecision(l),
                     crate::types::value::DataValue::DoublePrecision(h)) => {
                        assert!((l.0 - 30000.0).abs() < 0.001);
                        assert!((h.0 - 80000.0).abs() < 0.001);
                    }
                    _ => panic!("Expected DoublePrecision values"),
                }
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_between_non_indexed_column_returns_none() {
        let planner = make_planner();
        // BETWEEN on 'name' but index is on 'id'
        let pred = between_pred(col("name"), constant_text("A"), constant_text("Z"));
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_none(), "BETWEEN on non-indexed column should not match");
    }

    #[test]
    fn test_extract_between_varchar_range() {
        let planner = make_planner();
        // name BETWEEN 'Alice' AND 'Charlie' → RangeLookup('Alice', 'Charlie')
        let pred = between_pred(col("name"), constant_text("Alice"), constant_text("Charlie"));
        let mode = planner.extract_index_mode_from_predicate(&pred, "name", &DataType::Varchar(255));
        assert!(mode.is_some(), "BETWEEN on indexed VARCHAR column should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Varchar("Alice".to_string()));
                assert_eq!(high, crate::types::value::DataValue::Varchar("Charlie".to_string()));
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_between_compound_column() {
        let planner = make_planner();
        // users.id BETWEEN 10 AND 20 → RangeLookup(10, 20)
        let pred = between_pred(
            compound(&["users", "id"]),
            constant_int(10),
            constant_int(20),
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_some(), "BETWEEN with compound column should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(10));
                assert_eq!(high, crate::types::value::DataValue::Int(20));
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_between_expression_not_constant_returns_none() {
        let planner = make_planner();
        // age BETWEEN col+1 AND 100  (dynamic low bound) → should not match
        let dynamic_low = ExprNode::Binary {
            left: Box::new(col("min_age")),
            op: rook_ast::ArithOp::Add,
            right: Box::new(constant_int(1)),
        };
        let pred = between_pred(col("age"), dynamic_low, constant_int(100));
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_none(), "BETWEEN with dynamic low bound should not match");
    }

    #[test]
    fn test_extract_between_and_eq_compound_uses_between() {
        let planner = make_planner();
        // AND(age BETWEEN 18 AND 65, name = 'Alice') on indexed 'age'
        // Should pick BETWEEN (RangeLookup) since it matches 'age'
        let pred = and_pred(
            between_pred(col("age"), constant_int(18), constant_int(65)),
            eq_pred(col("name"), constant_text("Alice")),
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "age", &DataType::Int);
        assert!(mode.is_some(), "BETWEEN side of AND should match");
        match mode.unwrap() {
            IndexScanMode::RangeLookup(low, high) => {
                assert_eq!(low, crate::types::value::DataValue::Int(18));
                assert_eq!(high, crate::types::value::DataValue::Int(65));
            }
            other => panic!("Expected RangeLookup, got {:?}", other),
        }
    }

    // ── IN list index pushdown (L1) ───────────────────────────────────────

    #[test]
    fn test_extract_inlist_single_element_creates_point_lookup() {
        let planner = make_planner();
        // id IN (42) → PointLookup(42)
        let pred = inlist_pred(col("id"), vec![constant_int(42)]);
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_some(), "Single-element IN list on indexed column should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(42));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_inlist_multi_element_returns_none() {
        let planner = make_planner();
        // id IN (1, 2, 3) → not accelerated, falls through to SeqScan+Filter
        let pred = inlist_pred(col("id"), vec![
            constant_int(1),
            constant_int(2),
            constant_int(3),
        ]);
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_none(), "Multi-element IN list should not be directly accelerated");
    }

    #[test]
    fn test_extract_inlist_non_indexed_column_returns_none() {
        let planner = make_planner();
        // name IN ('Alice') on indexed 'id' → no match
        let pred = inlist_pred(col("name"), vec![constant_text("Alice")]);
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_none(), "IN on non-indexed column should not match");
    }

    #[test]
    fn test_extract_inlist_varchar_single() {
        let planner = make_planner();
        // name IN ('Bob') → PointLookup('Bob')
        let pred = inlist_pred(col("name"), vec![constant_text("Bob")]);
        let mode = planner.extract_index_mode_from_predicate(&pred, "name", &DataType::Varchar(255));
        assert!(mode.is_some(), "Single-element VARCHAR IN should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Varchar("Bob".to_string()));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_inlist_compound_column_single() {
        let planner = make_planner();
        // users.id IN (100) → PointLookup(100)
        let pred = inlist_pred(
            compound(&["users", "id"]),
            vec![constant_int(100)],
        );
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_some(), "Single-element IN with compound column should match");
        match mode.unwrap() {
            IndexScanMode::PointLookup(dv) => {
                assert_eq!(dv, crate::types::value::DataValue::Int(100));
            }
            other => panic!("Expected PointLookup, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_inlist_single_non_constant_returns_none() {
        let planner = make_planner();
        // id IN (some_column) → single element but not constant → no match
        let pred = inlist_pred(col("id"), vec![col("other")]);
        let mode = planner.extract_index_mode_from_predicate(&pred, "id", &DataType::Int);
        assert!(mode.is_none(), "IN with non-constant list element should not match");
    }
}
