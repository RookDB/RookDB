//! Index-accelerated scan planning — split from `planner/mod.rs`.
//!
//! Chooses between SeqScan and `IndexScanOperator` (PointLookup /
//! RangeLookup / CompositePointLookup / FullScan) by matching a filter
//! predicate against a table's indexes. When several indexes qualify the
//! most selective mode wins (see `scan_mode_priority`).

use std::path::PathBuf;

use rook_ast::logical::LogicalTableScan;

use super::super::operators::{IndexScanMode, IndexScanOperator, PhysicalOperator};
use super::super::tuple::ColumnInfo;
use super::PhysicalPlanner;
use crate::backend::error::{RookError, RookResult};
use crate::backend::heap::HeapManager;
use crate::types::Comparable;
use crate::types::{DataType, DataValue};

impl PhysicalPlanner {
    /// Try to create an index-accelerated scan from a Filter + TableScan pattern.
    ///
    /// Examines the filter predicate and table indexes to determine if a
    /// PointLookup or RangeLookup can replace the SeqScan + FilterOperator.
    /// When multiple indexes exist, selects the best one (dynamic index selection, M2).
    pub(crate) fn try_plan_index_scan_with_predicate(
        &self,
        ts: &LogicalTableScan,
        pred_node: &rook_ast::PredicateNode,
    ) -> RookResult<Option<Box<dyn PhysicalOperator>>> {
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
        let column_info: Vec<ColumnInfo> = table_schema
            .iter()
            .map(|c| ColumnInfo {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                table: Some(table_name.clone()),
            })
            .collect();

        let heap_path = PathBuf::from(crate::backend::paths::heap(&self.db_name, &ts.table));
        if !heap_path.exists() {
            return Ok(None);
        }

        // Load indexes (both named and legacy), preserving composite layouts
        let named_indexes = crate::backend::executor::create_index::load_table_indexes_multi(
            &self.db_name,
            &ts.table,
        )
        .unwrap_or_default();

        // Try named indexes first (dynamic selection — M2)
        let mut best_idx_name: Option<String> = None;
        let mut best_col_name: Option<String> = None;
        let mut best_col_names: Vec<String> = Vec::new();
        let mut best_key_types: Vec<DataType> = Vec::new();
        let mut best_mode: Option<IndexScanMode> = None;

        for (idx_name, col_names, _is_unique) in &named_indexes {
            // Composite index: equality on ALL key columns → exact lookup.
            if col_names.len() > 1 {
                let current_key_types: Vec<DataType> = col_names
                    .iter()
                    .map(|cn| {
                        table_schema
                            .iter()
                            .find(|c| c.name.eq_ignore_ascii_case(cn))
                            .map(|c| c.data_type.clone())
                            .unwrap_or(DataType::Int)
                    })
                    .collect();
                if let Some(key_values) =
                    self.extract_composite_point_key(pred_node, col_names, &current_key_types)
                {
                    let priority = 0; // most selective
                    let should_replace = match &best_mode {
                        Some(existing) => priority < Self::scan_mode_priority(existing),
                        None => true,
                    };
                    if should_replace {
                        best_idx_name = Some(idx_name.clone());
                        best_col_name = Some(col_names.join(","));
                        best_col_names = col_names.clone();
                        best_key_types = current_key_types;
                        best_mode = Some(IndexScanMode::CompositePointLookup(key_values));
                    }
                }
                continue;
            }

            let col_name = &col_names[0];
            // Look up the indexed column's DataType for sentinel bounds
            let col_type = table_schema
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(col_name))
                .map(|c| &c.data_type)
                .unwrap_or_else(|| &DataType::Int);
            if let Some(mode) =
                self.extract_index_mode_from_predicate(pred_node, col_name, col_type)
            {
                let priority = Self::scan_mode_priority(&mode);
                let should_replace = match &best_mode {
                    Some(existing) => priority < Self::scan_mode_priority(existing),
                    None => true,
                };
                if should_replace {
                    best_idx_name = Some(idx_name.clone());
                    best_col_name = Some(col_name.clone());
                    best_col_names = vec![col_name.clone()];
                    best_key_types.clear();
                    best_mode = Some(mode);
                }
            }
        }

        // Try legacy index if no named index matched
        if best_mode.is_none() {
            let legacy_idx_path = PathBuf::from(crate::backend::paths::legacy_index(
                &self.db_name,
                &ts.table,
            ));
            let meta_path = crate::backend::paths::legacy_index_meta(&self.db_name, &ts.table);
            if legacy_idx_path.exists()
                && let Ok(meta_json) = std::fs::read_to_string(&meta_path)
            {
                #[derive(serde::Deserialize)]
                struct IndexMeta {
                    column_name: String,
                }
                if let Ok(meta) = serde_json::from_str::<IndexMeta>(&meta_json) {
                    let col_type = table_schema
                        .iter()
                        .find(|c| c.name.eq_ignore_ascii_case(&meta.column_name))
                        .map(|c| &c.data_type)
                        .unwrap_or_else(|| &DataType::Int);
                    if let Some(mode) = self.extract_index_mode_from_predicate(
                        pred_node,
                        &meta.column_name,
                        col_type,
                    ) {
                        let col_name = meta.column_name.clone();
                        best_mode = Some(mode);
                        best_col_name = Some(col_name.clone());
                        best_col_names = vec![col_name];
                        best_idx_name = Some(format!("idx_{}_{}", ts.table, meta.column_name));
                    }
                }
            }
        }

        // Build the index scan operator if a matching index was found
        if let (Some(idx_name), Some(ref col_name), Some(mode)) =
            (best_idx_name, best_col_name, best_mode)
        {
            let idx_path = PathBuf::from(crate::backend::paths::index(
                &self.db_name,
                &ts.table,
                &idx_name,
            ));
            if !idx_path.exists() {
                return Ok(None);
            }

            log::info!(
                "[Volcano] Index-accelerated scan: mode={:?}, table='{}'",
                mode,
                ts.table
            );

            crate::backend::cache::checkpoint();
            // Key type(s) come from the INDEXED column(s) (NOT the first table
            // column). The index itself is not opened here: the scan looks keys
            // up through the cached handle, which keeps the file open and its
            // pages cached across queries.
            let key_types: Vec<DataType> = if !best_key_types.is_empty() {
                best_key_types.clone()
            } else if let Some(idx_col) = table_schema
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(col_name.as_str()))
            {
                vec![idx_col.data_type.clone()]
            } else if let Some(first_col) = table_schema.first() {
                vec![first_col.data_type.clone()]
            } else {
                vec![DataType::Int]
            };

            let heap_manager = HeapManager::open(heap_path.clone()).map_err(|e| {
                RookError::Io(e).with_context(format!(
                    "opening heap {} for table '{}'",
                    heap_path.display(),
                    ts.table
                ))
            })?;

            let indexed_cols: Vec<usize> = best_col_names
                .iter()
                .filter_map(|cn| {
                    column_info
                        .iter()
                        .position(|ci| ci.name.eq_ignore_ascii_case(cn))
                })
                .collect();

            return Ok(Some(Box::new(IndexScanOperator::with_cached_index(
                idx_path,
                key_types,
                heap_manager,
                mode,
                column_info,
                indexed_cols,
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
        key_types: &[DataType],
    ) -> Option<Vec<DataValue>> {
        // Flatten the conjunction into individual comparison predicates.
        fn flatten<'p>(
            pred: &'p rook_ast::PredicateNode,
            out: &mut Vec<&'p rook_ast::PredicateNode>,
        ) {
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
        for (col_idx, wanted) in col_names.iter().enumerate() {
            let col_type = key_types.get(col_idx).unwrap_or(&DataType::Int);
            let mut found: Option<DataValue> = None;
            for c in &conjuncts {
                if let rook_ast::PredicateNode::Compare {
                    left,
                    op: rook_ast::ComparisonOp::Eq,
                    right,
                } = c
                {
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
                        let dv = Self::ast_constant_to_data_value(const_val)?;
                        found = Self::coerce_to_key_type(dv, col_type);
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

                let dv = Self::ast_constant_to_data_value(const_val)?;
                let dv = Self::coerce_to_key_type(dv, col_type)?;

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
                    rook_ast::ExprNode::Constant(cv) => {
                        let dv = Self::ast_constant_to_data_value(cv)?;
                        Self::coerce_to_key_type(dv, col_type)?
                    }
                    _ => return None,
                };
                let high_val = match high.as_ref() {
                    rook_ast::ExprNode::Constant(cv) => {
                        let dv = Self::ast_constant_to_data_value(cv)?;
                        Self::coerce_to_key_type(dv, col_type)?
                    }
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
                    && let rook_ast::ExprNode::Constant(cv) = &list[0]
                {
                    let dv = Self::ast_constant_to_data_value(cv)?;
                    let dv = Self::coerce_to_key_type(dv, col_type)?;
                    return Some(IndexScanMode::PointLookup(dv));
                }

                // Multi-element IN lists are not directly accelerated.
                // They fall through to SeqScan + FilterOperator which
                // evaluates the IN predicate correctly.
                None
            }

            // ── AND predicates: try both sides and combine if both match ──
            rook_ast::PredicateNode::BinaryOp {
                left,
                op: rook_ast::BinaryOp::And,
                right,
            } => {
                let left_mode = self.extract_index_mode_from_predicate(left, indexed_col, col_type);
                let right_mode =
                    self.extract_index_mode_from_predicate(right, indexed_col, col_type);

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
            (
                IndexScanMode::RangeLookup(low_a, high_a),
                IndexScanMode::RangeLookup(low_b, high_b),
            ) => {
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

    /// Coerce a DataValue to the target column/key DataType (e.g. space-padding CHAR(n)).
    pub(crate) fn coerce_to_key_type(dv: DataValue, target_type: &DataType) -> Option<DataValue> {
        match target_type {
            DataType::Char(n) | DataType::Character(n) => {
                let s = match dv {
                    DataValue::Char(s) | DataValue::Varchar(s) => s,
                    _ => return None,
                };
                let width = *n as usize;
                if s.len() > width {
                    return None;
                }
                let mut padded = s;
                if padded.len() < width {
                    padded.push_str(&" ".repeat(width - padded.len()));
                }
                Some(DataValue::Char(padded))
            }
            DataType::Varchar(_) => match dv {
                DataValue::Char(s) | DataValue::Varchar(s) => Some(DataValue::Varchar(s)),
                _ => None,
            },
            DataType::SmallInt => match dv {
                DataValue::SmallInt(i) => Some(DataValue::SmallInt(i)),
                DataValue::Int(i) if i >= i16::MIN as i32 && i <= i16::MAX as i32 => {
                    Some(DataValue::SmallInt(i as i16))
                }
                DataValue::BigInt(i) if i >= i16::MIN as i64 && i <= i16::MAX as i64 => {
                    Some(DataValue::SmallInt(i as i16))
                }
                _ => None,
            },
            DataType::Int => match dv {
                DataValue::SmallInt(i) => Some(DataValue::Int(i as i32)),
                DataValue::Int(i) => Some(DataValue::Int(i)),
                DataValue::BigInt(i) if i >= i32::MIN as i64 && i <= i32::MAX as i64 => {
                    Some(DataValue::Int(i as i32))
                }
                _ => None,
            },
            DataType::BigInt => match dv {
                DataValue::SmallInt(i) => Some(DataValue::BigInt(i as i64)),
                DataValue::Int(i) => Some(DataValue::BigInt(i as i64)),
                DataValue::BigInt(i) => Some(DataValue::BigInt(i)),
                _ => None,
            },
            DataType::DoublePrecision => match dv {
                DataValue::Real(r) => Some(DataValue::DoublePrecision(
                    crate::types::value::OrderedF64(r.0 as f64),
                )),
                DataValue::DoublePrecision(d) => Some(DataValue::DoublePrecision(d)),
                DataValue::Int(i) => Some(DataValue::DoublePrecision(
                    crate::types::value::OrderedF64(i as f64),
                )),
                DataValue::BigInt(i) => Some(DataValue::DoublePrecision(
                    crate::types::value::OrderedF64(i as f64),
                )),
                _ => None,
            },
            DataType::Real => match dv {
                DataValue::Real(r) => Some(DataValue::Real(r)),
                DataValue::DoublePrecision(d) => {
                    Some(DataValue::Real(crate::types::value::OrderedF32(d.0 as f32)))
                }
                DataValue::Int(i) => {
                    Some(DataValue::Real(crate::types::value::OrderedF32(i as f32)))
                }
                _ => None,
            },
            DataType::Numeric { precision, scale } | DataType::Decimal { precision, scale } => {
                match dv {
                    DataValue::Numeric(num) => {
                        if num.scale == *scale {
                            Some(DataValue::Numeric(num))
                        } else if num.scale < *scale {
                            let diff = (*scale - num.scale) as u32;
                            let factor = 10_i128.checked_pow(diff)?;
                            let unscaled = num.unscaled.checked_mul(factor)?;
                            Some(DataValue::Numeric(crate::types::value::NumericValue {
                                unscaled,
                                scale: *scale,
                            }))
                        } else {
                            let diff = (num.scale - *scale) as u32;
                            let factor = 10_i128.checked_pow(diff)?;
                            Some(DataValue::Numeric(crate::types::value::NumericValue {
                                unscaled: num.unscaled / factor,
                                scale: *scale,
                            }))
                        }
                    }
                    DataValue::SmallInt(i) => {
                        let factor = 10_i128.checked_pow(*scale as u32)?;
                        let unscaled = (i as i128).checked_mul(factor)?;
                        Some(DataValue::Numeric(crate::types::value::NumericValue {
                            unscaled,
                            scale: *scale,
                        }))
                    }
                    DataValue::Int(i) => {
                        let factor = 10_i128.checked_pow(*scale as u32)?;
                        let unscaled = (i as i128).checked_mul(factor)?;
                        Some(DataValue::Numeric(crate::types::value::NumericValue {
                            unscaled,
                            scale: *scale,
                        }))
                    }
                    DataValue::BigInt(i) => {
                        let factor = 10_i128.checked_pow(*scale as u32)?;
                        let unscaled = (i as i128).checked_mul(factor)?;
                        Some(DataValue::Numeric(crate::types::value::NumericValue {
                            unscaled,
                            scale: *scale,
                        }))
                    }
                    DataValue::DoublePrecision(d) => {
                        let s = format!("{}", d.0);
                        crate::types::value::parse_numeric_literal(&s, *precision, *scale)
                            .or_else(|_| {
                                let s2 = format!("{:.prec$}", d.0, prec = *scale as usize);
                                crate::types::value::parse_numeric_literal(&s2, *precision, *scale)
                            })
                            .ok()
                            .map(DataValue::Numeric)
                    }
                    DataValue::Real(r) => {
                        let s = format!("{}", r.0);
                        crate::types::value::parse_numeric_literal(&s, *precision, *scale)
                            .or_else(|_| {
                                let s2 = format!("{:.prec$}", r.0, prec = *scale as usize);
                                crate::types::value::parse_numeric_literal(&s2, *precision, *scale)
                            })
                            .ok()
                            .map(DataValue::Numeric)
                    }
                    DataValue::Char(s) | DataValue::Varchar(s) => {
                        crate::types::value::parse_numeric_literal(&s, *precision, *scale)
                            .ok()
                            .map(DataValue::Numeric)
                    }
                    _ => None,
                }
            }
            DataType::Date => match dv {
                DataValue::Date(d) => Some(DataValue::Date(d)),
                DataValue::Varchar(s) | DataValue::Char(s) => {
                    let trimmed = s.trim();
                    chrono::NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
                        .ok()
                        .map(DataValue::Date)
                }
                _ => None,
            },
            DataType::Timestamp => match dv {
                DataValue::Timestamp(t) => Some(DataValue::Timestamp(t)),
                DataValue::Varchar(s) | DataValue::Char(s) => {
                    let trimmed = s.trim();
                    chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%.f")
                        .or_else(|_| {
                            chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S")
                        })
                        .or_else(|_| {
                            chrono::NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
                                .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                        })
                        .ok()
                        .map(DataValue::Timestamp)
                }
                _ => None,
            },
            DataType::Time => match dv {
                DataValue::Time(t) => Some(DataValue::Time(t)),
                DataValue::Varchar(s) | DataValue::Char(s) => {
                    let trimmed = s.trim();
                    chrono::NaiveTime::parse_from_str(trimmed, "%H:%M:%S%.f")
                        .or_else(|_| chrono::NaiveTime::parse_from_str(trimmed, "%H:%M:%S"))
                        .ok()
                        .map(DataValue::Time)
                }
                _ => None,
            },
            _ => Some(dv),
        }
    }

    /// Convert an AST ConstantValue to a DataValue (None for Null).
    pub(crate) fn ast_constant_to_data_value(
        cv: &rook_ast::ConstantValue,
    ) -> Option<crate::types::value::DataValue> {
        match cv {
            rook_ast::ConstantValue::Null => None,
            rook_ast::ConstantValue::Int(i) => {
                if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                    Some(crate::types::value::DataValue::Int(*i as i32))
                } else {
                    Some(crate::types::value::DataValue::BigInt(*i))
                }
            }
            rook_ast::ConstantValue::Float(f) => {
                Some(crate::types::value::DataValue::DoublePrecision(
                    crate::types::value::OrderedF64(*f),
                ))
            }
            rook_ast::ConstantValue::Text(s) => {
                Some(crate::types::value::DataValue::Varchar(s.clone()))
            }
            rook_ast::ConstantValue::Boolean(b) => Some(crate::types::value::DataValue::Bool(*b)),
        }
    }
}
