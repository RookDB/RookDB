//! Index-accelerated scan planning — split from `planner/mod.rs`.
//!
//! Chooses between SeqScan and `IndexScanOperator` (PointLookup /
//! RangeLookup / CompositePointLookup / FullScan) by matching a filter
//! predicate against a table's indexes. When several indexes qualify the
//! most selective mode wins (see `scan_mode_priority`).

use std::path::PathBuf;

use rook_ast::logical::LogicalTableScan;

use super::super::operators::{IndexScanOperator, IndexScanMode, PhysicalOperator};
use crate::types::Comparable;
use super::super::tuple::ColumnInfo;
use super::PhysicalPlanner;
use super::cost::CostModel;
use crate::backend::heap::HeapManager;
use crate::backend::index::BTree;
use crate::types::{DataType, DataValue};

impl PhysicalPlanner {
    /// Try to create an index-accelerated scan from a Filter + TableScan pattern.
    ///
    /// Examines the filter predicate and table indexes to determine if a
    /// PointLookup or RangeLookup can replace the SeqScan + FilterOperator.
    /// When multiple indexes exist, selects the best one (dynamic index selection, M2).
    pub fn try_plan_index_scan_with_predicate(
        &self,
        ts: &LogicalTableScan,
        pred_node: &rook_ast::PredicateNode,
    ) -> Result<Option<Box<dyn PhysicalOperator>>, String> {
        // Only works for regular user tables
        if ts.system_table_name.is_some() {
            return Ok(None);
        }

        let table_schema = match self.resolve_table_schema(&ts.table) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };

        // Build column_info for the table
        let table_name = ts.table.clone();
        let column_info: Vec<ColumnInfo> = table_schema.iter().map(|c| {
            ColumnInfo {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                table: Some(table_name.clone()),
            }
        }).collect();

        let heap_path = PathBuf::from(format!(
            "database/base/{}/{}.dat", self.db_name, ts.table
        ));
        if !heap_path.exists() {
            return Ok(None);
        }

        // Fetch table and column statistics for Cost-Based Optimization (CBO)
        let (data_pages, total_tuples) = match crate::backend::cache::table_statistics(&self.db_name, &ts.table) {
            Ok(s) => (s.data_pages.max(1), s.total_tuple_count),
            Err(_) => (1, 100),
        };
        let col_stats_map = crate::backend::cache::column_statistics(&self.db_name, &ts.table);

        // Load indexes (both named and legacy), preserving composite layouts
        let named_indexes = crate::backend::executor::create_index::load_table_indexes_multi(
            &self.db_name, &ts.table,
        ).unwrap_or_default();

        // Try named indexes first (dynamic selection with cost model)
        let mut best_idx_name: Option<String> = None;
        let mut best_col_name: Option<String> = None;
        let mut best_key_types: Vec<DataType> = Vec::new();
        let mut best_mode: Option<IndexScanMode> = None;
        let mut best_cost_idx: f64 = f64::INFINITY;
        let mut best_selectivity: f64 = 1.0;
        let mut best_priority: u8 = u8::MAX;

        for (idx_name, col_names, _is_unique) in &named_indexes {
            // Composite index: equality on ALL key columns → exact lookup.
            if col_names.len() > 1 {
                if let Some(key_values) = self.extract_composite_point_key(pred_node, col_names) {
                    let mode = IndexScanMode::CompositePointLookup(key_values);
                    let priority = 0; // most selective
                    let primary_col = &col_names[0];
                    let col_type = table_schema.iter()
                        .find(|c| c.name.eq_ignore_ascii_case(primary_col))
                        .map(|c| &c.data_type)
                        .unwrap_or(&DataType::Int);
                    let sel = Self::estimate_candidate_selectivity(
                        &mode,
                        primary_col,
                        col_type,
                        col_stats_map.get(&primary_col.to_ascii_lowercase()),
                        total_tuples,
                    );
                    let cost = CostModel::cost_index_scan(data_pages, total_tuples, sel);

                    if cost < best_cost_idx || ((cost - best_cost_idx).abs() < 1e-6 && priority < best_priority) {
                        best_cost_idx = cost;
                        best_selectivity = sel;
                        best_priority = priority;
                        best_idx_name = Some(idx_name.clone());
                        best_col_name = Some(col_names.join(","));
                        best_key_types = col_names.iter()
                            .map(|cn| table_schema.iter()
                                .find(|c| c.name.eq_ignore_ascii_case(cn))
                                .map(|c| c.data_type.clone())
                                .unwrap_or(DataType::Int))
                            .collect();
                        best_mode = Some(mode);
                    }
                }
                continue;
            }

            let col_name = &col_names[0];
            // Look up the indexed column's DataType for sentinel bounds
            let col_type = table_schema.iter()
                .find(|c| c.name.eq_ignore_ascii_case(col_name))
                .map(|c| &c.data_type)
                .unwrap_or_else(|| &DataType::Int);
            if let Some(mode) = self.extract_index_mode_from_predicate(pred_node, col_name, col_type) {
                let priority = Self::scan_mode_priority(&mode);
                let sel = Self::estimate_candidate_selectivity(
                    &mode,
                    col_name,
                    col_type,
                    col_stats_map.get(&col_name.to_ascii_lowercase()),
                    total_tuples,
                );
                let cost = CostModel::cost_index_scan(data_pages, total_tuples, sel);

                if cost < best_cost_idx || ((cost - best_cost_idx).abs() < 1e-6 && priority < best_priority) {
                    best_cost_idx = cost;
                    best_selectivity = sel;
                    best_priority = priority;
                    best_idx_name = Some(idx_name.clone());
                    best_col_name = Some(col_name.clone());
                    best_key_types.clear();
                    best_mode = Some(mode);
                }
            }
        }

        // Try legacy index if no named index matched
        if best_mode.is_none() {
            let legacy_idx_path = PathBuf::from(format!(
                "database/base/{}/{}.idx", self.db_name, ts.table
            ));
            let meta_path = format!("database/base/{}/{}.idx.meta", self.db_name, ts.table);
            if legacy_idx_path.exists()
                && let Ok(meta_json) = std::fs::read_to_string(&meta_path) {
                    #[derive(serde::Deserialize)]
                    struct IndexMeta { column_name: String }
                    if let Ok(meta) = serde_json::from_str::<IndexMeta>(&meta_json) {
                        let col_type = table_schema.iter()
                            .find(|c| c.name.eq_ignore_ascii_case(&meta.column_name))
                            .map(|c| &c.data_type)
                            .unwrap_or_else(|| &DataType::Int);
                        if let Some(mode) = self.extract_index_mode_from_predicate(pred_node, &meta.column_name, col_type) {
                            let col_name = meta.column_name.clone();
                            let priority = Self::scan_mode_priority(&mode);
                            let sel = Self::estimate_candidate_selectivity(
                                &mode,
                                &col_name,
                                col_type,
                                col_stats_map.get(&col_name.to_ascii_lowercase()),
                                total_tuples,
                            );
                            let cost = CostModel::cost_index_scan(data_pages, total_tuples, sel);

                            if cost < best_cost_idx || ((cost - best_cost_idx).abs() < 1e-6 && priority < best_priority) {
                                best_cost_idx = cost;
                                best_selectivity = sel;
                                best_mode = Some(mode);
                                best_col_name = Some(col_name);
                                best_idx_name = Some(format!("idx_{}_{}", ts.table, meta.column_name));
                            }
                        }
                    }
                }
        }

        // CBO Decision: compare chosen IndexScan cost with SeqScan cost
        if best_mode.is_some() {
            let cost_seq = CostModel::cost_seq_scan(data_pages, total_tuples);
            let use_index = if total_tuples <= 50 || data_pages <= 1 {
                best_selectivity <= 0.30
            } else {
                best_cost_idx < cost_seq
            };

            if !use_index {
                log::info!(
                    "[CBO] Sequential scan chosen over index scan for table '{}' (cost_seq={:.2} vs best_cost_idx={:.2}, selectivity={:.4})",
                    ts.table, cost_seq, best_cost_idx, best_selectivity
                );
                return Ok(None);
            }
        }

        // Build the index scan operator if a matching index was found
        if let (Some(idx_name), Some(ref col_name), Some(mode)) = (best_idx_name, best_col_name, best_mode) {
            let idx_path = PathBuf::from(format!(
                "database/base/{}/{}.{}.idx", self.db_name, ts.table, idx_name
            ));
            if !idx_path.exists() {
                return Ok(None);
            }

            log::info!(
                "[Volcano] Index-accelerated scan: mode={:?}, table='{}'",
                mode, ts.table
            );

            crate::backend::cache::checkpoint();
            let mut btree = BTree::open(idx_path.clone())
                .map_err(|e| format!("Failed to open index: {}", e))?;
            // Set key type(s) from the INDEXED column(s) (NOT the first table column)
            if !best_key_types.is_empty() {
                btree.set_key_types(best_key_types.clone());
            } else if let Some(idx_col) = table_schema.iter().find(|c| c.name.eq_ignore_ascii_case(col_name.as_str())) {
                btree.set_key_type(idx_col.data_type.clone());
            } else if let Some(first_col) = table_schema.first() {
                btree.set_key_type(first_col.data_type.clone());
            }

            let heap_manager = HeapManager::open(heap_path)
                .map_err(|e| format!("Failed to open heap: {}", e))?;

            return Ok(Some(Box::new(IndexScanOperator::new(
                btree, heap_manager, mode, column_info,
            ))));
        }

        Ok(None)
    }

    /// Extract an exact composite key from an AND-conjunction of equality
    /// predicates over ALL of `col_names` (in key order).
    ///
    /// e.g. for index `(a, b)`: `WHERE a = 1 AND b = 'x'` → `[Int(1), Varchar(x)]`.
    /// Returns `None` when any key column lacks an equality predicate.
    pub(crate) fn extract_composite_point_key(
        &self,
        pred: &rook_ast::PredicateNode,
        col_names: &[String],
    ) -> Option<Vec<DataValue>> {
        // Flatten the conjunction into individual comparison predicates.
        fn flatten<'p>(pred: &'p rook_ast::PredicateNode, out: &mut Vec<&'p rook_ast::PredicateNode>) {
            match pred {
                rook_ast::PredicateNode::BinaryOp {
                    left,
                    op: rook_ast::BinaryOp::And,
                    right,
                } => {
                    flatten(left, out);
                    flatten(right, out);
                }
                other => out.push(other),
            }
        }

        let mut conjuncts = Vec::new();
        flatten(pred, &mut conjuncts);

        let mut key = Vec::with_capacity(col_names.len());
        for wanted in col_names {
            let mut found: Option<DataValue> = None;
            for c in &conjuncts {
                if let rook_ast::PredicateNode::Compare { left, op: rook_ast::ComparisonOp::Eq, right } = c {
                    let (col_expr, const_val) = match (left.as_ref(), right.as_ref()) {
                        (rook_ast::ExprNode::Column(_), rook_ast::ExprNode::Constant(cv))
                        | (rook_ast::ExprNode::Compound(_), rook_ast::ExprNode::Constant(cv))
                        | (rook_ast::ExprNode::Constant(cv), rook_ast::ExprNode::Column(_))
                        | (rook_ast::ExprNode::Constant(cv), rook_ast::ExprNode::Compound(_)) => {
                            (left.as_ref(), cv)
                        }
                        _ => continue,
                    };
                    let leaf = match col_expr {
                        rook_ast::ExprNode::Column(name) => name.as_str(),
                        rook_ast::ExprNode::Compound(parts) => parts.last()?.as_str(),
                        _ => continue,
                    };
                    if leaf.eq_ignore_ascii_case(wanted) {
                        found = Some(Self::ast_constant_to_data_value(const_val));
                        break;
                    }
                }
            }
            key.push(found?);
        }
        Some(key)
    }

    /// Priority for dynamic index selection: lower = better.
    pub(crate) fn scan_mode_priority(mode: &IndexScanMode) -> u8 {
        match mode {
            IndexScanMode::PointLookup(_) => 0,
            IndexScanMode::CompositePointLookup(_) => 0,
            IndexScanMode::RangeLookup(..) => 1,
            IndexScanMode::FullScan => 2,
        }
    }

    /// Estimate selectivity for a candidate index scan mode using column statistics or fallback heuristics.
    pub(crate) fn estimate_candidate_selectivity(
        mode: &IndexScanMode,
        _col_name: &str,
        col_type: &DataType,
        col_stats: Option<&crate::backend::statistics::ColumnStatistics>,
        total_tuples: u64,
    ) -> f64 {
        use crate::backend::statistics::column_stats::StatOp;
        match mode {
            IndexScanMode::PointLookup(val) => {
                if let Some(cs) = col_stats {
                    cs.estimate_selectivity(StatOp::Eq, val)
                } else if total_tuples > 0 {
                    (1.0 / total_tuples as f64).clamp(0.0001, 1.0)
                } else {
                    0.001
                }
            }
            IndexScanMode::CompositePointLookup(_) => {
                if let Some(cs) = col_stats {
                    if cs.distinct_count > 0 {
                        (1.0 / cs.distinct_count as f64).clamp(0.0001, 1.0)
                    } else {
                        (1.0 / cs.total_count.max(1) as f64).clamp(0.0001, 1.0)
                    }
                } else if total_tuples > 0 {
                    (1.0 / total_tuples as f64).clamp(0.0001, 1.0)
                } else {
                    0.001
                }
            }
            IndexScanMode::RangeLookup(low, high) => {
                let is_low_min = *low == DataValue::min_for_type(col_type);
                let is_high_max = *high == DataValue::max_for_type(col_type);

                if let Some(cs) = col_stats {
                    if is_low_min && is_high_max {
                        1.0
                    } else if is_low_min {
                        cs.estimate_selectivity(StatOp::Le, high)
                    } else if is_high_max {
                        cs.estimate_selectivity(StatOp::Ge, low)
                    } else {
                        cs.estimate_between(low, high)
                    }
                } else {
                    if is_low_min && is_high_max {
                        1.0
                    } else if is_low_min || is_high_max {
                        0.33
                    } else {
                        0.10
                    }
                }
            }
            IndexScanMode::FullScan => 1.0,
        }
    }

    /// Extract an IndexScanMode from a predicate if it matches an indexed column.
    ///
    /// Matches:
    ///   - `column = constant` → PointLookup
    ///   - `column >= constant` → RangeLookup(constant, MAX)
    ///   - `column > constant` → RangeLookup(constant+1, MAX)        (for ints/floats)
    ///   - `column <= constant` → RangeLookup(MIN, constant)
    ///   - `column < constant` → RangeLookup(MIN, constant-1)        (for ints/floats)
    ///   - `column BETWEEN low AND high` → RangeLookup(low, high)
    ///   - `column IN (single_constant)` → PointLookup
    ///   - AND predicates: tries both sides and combines ranges on the
    ///     same column into the tightest [max(low), min(high)] interval.
    ///
    /// Non-matched predicates and complex expressions fall through to
    /// SeqScan + FilterOperator for correct results.
    pub(crate) fn extract_index_mode_from_predicate(
        &self,
        pred: &rook_ast::PredicateNode,
        indexed_col: &str,
        col_type: &DataType,
    ) -> Option<IndexScanMode> {
        match pred {
            rook_ast::PredicateNode::Compare { left, op, right } => {
                // Extract column name and constant value from the comparison
                let (col_name, const_val) = match (left.as_ref(), right.as_ref()) {
                    // column op constant or constant op column
                    (col @ rook_ast::ExprNode::Column(_), rook_ast::ExprNode::Constant(cv))
                    | (rook_ast::ExprNode::Constant(cv), col @ rook_ast::ExprNode::Column(_)) => {
                        (col, cv)
                    }
                    // compound.column op constant or constant op compound.column
                    (col @ rook_ast::ExprNode::Compound(_), rook_ast::ExprNode::Constant(cv))
                    | (rook_ast::ExprNode::Constant(cv), col @ rook_ast::ExprNode::Compound(_)) => {
                        (col, cv)
                    }
                    _ => return None,
                };

                // Extract the leaf column name (last part for Compound)
                let leaf_name = match col_name {
                    rook_ast::ExprNode::Column(name) => name.as_str(),
                    rook_ast::ExprNode::Compound(parts) => parts.last()?.as_str(),
                    _ => return None,
                };

                // Check if the column matches the indexed column
                if !leaf_name.eq_ignore_ascii_case(indexed_col) {
                    return None;
                }

                let dv = Self::ast_constant_to_data_value(const_val);

                match op {
                    rook_ast::ComparisonOp::Eq => Some(IndexScanMode::PointLookup(dv)),
                    // ── Single range operators → RangeLookup with sentinels ──
                    rook_ast::ComparisonOp::Gt => {
                        // col > val → RangeLookup(val+1, MAX)
                        let low = dv.increment()?;
                        let high = DataValue::max_for_type(col_type);
                        Some(IndexScanMode::RangeLookup(low, high))
                    }
                    rook_ast::ComparisonOp::Ge => {
                        // col >= val → RangeLookup(val, MAX)
                        let high = DataValue::max_for_type(col_type);
                        Some(IndexScanMode::RangeLookup(dv, high))
                    }
                    rook_ast::ComparisonOp::Lt => {
                        // col < val → RangeLookup(MIN, val-1)
                        let low = DataValue::min_for_type(col_type);
                        let high = dv.decrement()?;
                        Some(IndexScanMode::RangeLookup(low, high))
                    }
                    rook_ast::ComparisonOp::Le => {
                        // col <= val → RangeLookup(MIN, val)
                        let low = DataValue::min_for_type(col_type);
                        Some(IndexScanMode::RangeLookup(low, dv))
                    }
                    _ => None,
                }
            }

            // ── BETWEEN: col BETWEEN low AND high → RangeLookup(low, high) ──
            rook_ast::PredicateNode::Between { expr, low, high } => {
                // Extract column name from the BETWEEN expression
                let leaf_name = match expr.as_ref() {
                    rook_ast::ExprNode::Column(name) => name.as_str(),
                    rook_ast::ExprNode::Compound(parts) => parts.last()?.as_str(),
                    _ => return None,
                };

                // Check if the column matches the indexed column
                if !leaf_name.eq_ignore_ascii_case(indexed_col) {
                    return None;
                }

                // Both bounds must be constants
                let low_val = match low.as_ref() {
                    rook_ast::ExprNode::Constant(cv) => Self::ast_constant_to_data_value(cv),
                    _ => return None,
                };
                let high_val = match high.as_ref() {
                    rook_ast::ExprNode::Constant(cv) => Self::ast_constant_to_data_value(cv),
                    _ => return None,
                };

                Some(IndexScanMode::RangeLookup(low_val, high_val))
            }

            // ── IN list: col IN (val)  or  col IN (v1, v2, ...) ──────────
            rook_ast::PredicateNode::InList { expr, list } => {
                // Extract column name from the IN expression
                let leaf_name = match expr.as_ref() {
                    rook_ast::ExprNode::Column(name) => name.as_str(),
                    rook_ast::ExprNode::Compound(parts) => parts.last()?.as_str(),
                    _ => return None,
                };

                // Check if the column matches the indexed column
                if !leaf_name.eq_ignore_ascii_case(indexed_col) {
                    return None;
                }

                // Single-element IN list: col IN (val) → PointLookup(val)
                if list.len() == 1
                    && let rook_ast::ExprNode::Constant(cv) = &list[0] {
                        return Some(IndexScanMode::PointLookup(
                            Self::ast_constant_to_data_value(cv),
                        ));
                    }

                // Multi-element IN lists are not directly accelerated.
                // They fall through to SeqScan + FilterOperator which
                // evaluates the IN predicate correctly.
                None
            }

            // ── AND predicates: try both sides and combine if both match ──
            rook_ast::PredicateNode::BinaryOp {
                left, op: rook_ast::BinaryOp::And, right,
            } => {
                let left_mode = self.extract_index_mode_from_predicate(left, indexed_col, col_type);
                let right_mode = self.extract_index_mode_from_predicate(right, indexed_col, col_type);

                match (left_mode, right_mode) {
                    // Both sides match on the same column → combine into tightest range
                    (Some(a), Some(b)) => Some(Self::combine_index_modes(a, b)),
                    // Only one side matches → use it (existing fall-through behaviour)
                    (Some(a), None) => Some(a),
                    (None, Some(b)) => Some(b),
                    (None, None) => None,
                }
            }
            _ => None,
        }
    }

    /// Combine two `IndexScanMode` values from an AND predicate on the same column.
    ///
    /// The goal is to produce the tightest possible range:
    /// - Two `RangeLookup`s → `RangeLookup(max(low_a, low_b), min(high_a, high_b))`
    /// - `PointLookup` + `RangeLookup` → prefer `PointLookup` (more selective)
    /// - Two `PointLookup`s → keep the first (both must be the same value under AND)
    /// - Any other combination → keep the more selective mode
    fn combine_index_modes(a: IndexScanMode, b: IndexScanMode) -> IndexScanMode {
        match (&a, &b) {
            // Two RangeLookups on the same column: intersect the intervals
            (IndexScanMode::RangeLookup(low_a, high_a), IndexScanMode::RangeLookup(low_b, high_b)) => {
                let low = if low_a.compare(low_b).ok() == Some(std::cmp::Ordering::Greater) {
                    low_a.clone()
                } else {
                    low_b.clone()
                };
                let high = if high_a.compare(high_b).ok() == Some(std::cmp::Ordering::Less) {
                    high_a.clone()
                } else {
                    high_b.clone()
                };
                // If low > high, the range is empty — still correct (no rows match)
                IndexScanMode::RangeLookup(low, high)
            }
            // PointLookup + RangeLookup (or vice versa): PointLookup is more selective
            (IndexScanMode::PointLookup(_), _) => a,
            (_, IndexScanMode::PointLookup(_)) => b,
            // RangeLookup + FullScan: RangeLookup wins
            (IndexScanMode::RangeLookup(..), _) | (_, IndexScanMode::RangeLookup(..)) => {
                match (&a, &b) {
                    (IndexScanMode::RangeLookup(..), _) => a,
                    (_, IndexScanMode::RangeLookup(..)) => b,
                    _ => unreachable!(),
                }
            }
            // Fallback: any other combination
            _ => a,
        }
    }

    /// Convert an AST ConstantValue to a DataValue.
    pub(crate) fn ast_constant_to_data_value(cv: &rook_ast::ConstantValue) -> crate::types::value::DataValue {
        match cv {
            rook_ast::ConstantValue::Null => crate::types::value::DataValue::Int(0),
            rook_ast::ConstantValue::Int(i) => {
                if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                    crate::types::value::DataValue::Int(*i as i32)
                } else {
                    crate::types::value::DataValue::BigInt(*i)
                }
            }
            rook_ast::ConstantValue::Float(f) => {
                crate::types::value::DataValue::DoublePrecision(
                    crate::types::value::OrderedF64(*f),
                )
            }
            rook_ast::ConstantValue::Text(s) => crate::types::value::DataValue::Varchar(s.clone()),
            rook_ast::ConstantValue::Boolean(b) => crate::types::value::DataValue::Bool(*b),
        }
    }
}
