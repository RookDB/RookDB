//! General correlated-subquery evaluation.
//!
//! A correlated subquery is run as a complete query for each distinct tuple of
//! outer values: every outer reference is replaced by a constant and the
//! result goes through the normal planner, so SELECT lists, joins, GROUP BY,
//! aggregates, HAVING, LIMIT and arbitrary expressions over outer columns all
//! mean what they mean outside a subquery. Results are cached by outer key,
//! so outer rows that repeat a key do not re-run it.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rook_ast::{ConstantValue, ExprNode, FunctionArg, PredicateNode, SelectExpr, SelectPlan};

use super::super::engine::execute_shared;
use super::super::tuple::Tuple;
use crate::catalog::types::Catalog;
use crate::types::value::DataValue;

/// Most distinct outer keys whose results are remembered at once.
const CACHE_LIMIT: usize = 4096;

/// The values of a subquery's outer columns for one outer row.
type OuterKey = Vec<Option<DataValue>>;

/// How a correlated subquery is consumed.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SubqueryUse {
    /// Only whether it returns a row matters.
    Exists,
    /// All rows are needed (IN list, scalar value).
    Rows,
}

/// An outer column a subquery names: lower-cased qualifier and name as
/// written, plus its index in the outer tuple.
#[derive(Clone, Debug)]
pub struct OuterBinding {
    pub qualifier: Option<String>,
    pub name: String,
    pub index: usize,
}

/// A correlated subquery that is evaluated as a full plan per outer key.
pub struct CorrelatedSubquery {
    template: SelectPlan,
    bindings: Vec<OuterBinding>,
    catalog: Arc<Catalog>,
    db_name: String,
    cache: RefCell<HashMap<OuterKey, Rc<Vec<Tuple>>>>,
}

impl std::fmt::Debug for CorrelatedSubquery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CorrelatedSubquery")
            .field("bindings", &self.bindings)
            .finish_non_exhaustive()
    }
}

impl CorrelatedSubquery {
    pub fn new(
        mut template: SelectPlan,
        bindings: Vec<OuterBinding>,
        catalog: Arc<Catalog>,
        db_name: String,
        usage: SubqueryUse,
    ) -> Self {
        // EXISTS needs one row; asking for one lets the plan stop early.
        if usage == SubqueryUse::Exists && template.limit.is_none() {
            template.limit = Some(rook_ast::LimitClause {
                limit: 1,
                offset: None,
            });
        }
        Self {
            template,
            bindings,
            catalog,
            db_name,
            cache: RefCell::new(HashMap::new()),
        }
    }

    pub fn bindings(&self) -> &[OuterBinding] {
        &self.bindings
    }

    /// The subquery with every outer reference replaced by `values`
    /// (one per binding, in order).
    pub fn bound_select(&self, values: &[Option<DataValue>]) -> SelectPlan {
        let mut select = self.template.clone();
        rewrite_select(&mut select, &mut |node, _| {
            let key = ref_key(node)?;
            let i = self
                .bindings
                .iter()
                .position(|b| b.qualifier == key.0 && b.name == key.1)?;
            Some(value_to_expr(values[i].as_ref()))
        });
        select
    }

    /// The rows the subquery returns for the outer row `tuple`.
    pub fn rows(&self, tuple: &Tuple) -> Result<Rc<Vec<Tuple>>, String> {
        let key: OuterKey = self
            .bindings
            .iter()
            .map(|b| tuple.values.get(b.index).and_then(|v| v.clone()))
            .collect();
        if let Some(hit) = self.cache.borrow().get(&key) {
            return Ok(Rc::clone(hit));
        }
        let select = self.bound_select(&key);
        let query = rook_ast::QueryPlan::Select(select);
        let logical = crate::planner::plan_query(&query, &self.catalog, &self.db_name)
            .map_err(|e| format!("Correlated subquery planning error: {}", e.message))?;
        let (rows, _) = execute_shared(&logical, Arc::clone(&self.catalog), &self.db_name)
            .map_err(|e| format!("Correlated subquery error: {}", e))?;
        let rows = Rc::new(rows);
        let mut cache = self.cache.borrow_mut();
        if cache.len() >= CACHE_LIMIT {
            cache.clear();
        }
        cache.insert(key, Rc::clone(&rows));
        Ok(rows)
    }
}

/// `(qualifier, name)`, lower-cased, of a column reference node.
pub fn ref_key(node: &ExprNode) -> Option<(Option<String>, String)> {
    match node {
        ExprNode::Column(n) => Some((None, n.to_ascii_lowercase())),
        ExprNode::Compound(parts) if parts.len() >= 2 => Some((
            Some(parts[parts.len() - 2].to_ascii_lowercase()),
            parts[parts.len() - 1].to_ascii_lowercase(),
        )),
        _ => None,
    }
}

/// A value as an expression that keeps its type.
fn value_to_expr(value: Option<&DataValue>) -> ExprNode {
    let text_cast = |dv: &DataValue, text: String| ExprNode::Cast {
        expr: Box::new(ExprNode::Constant(ConstantValue::Text(text))),
        data_type: dv.data_type().to_string(),
    };
    let Some(dv) = value else {
        return ExprNode::Constant(ConstantValue::Null);
    };
    match dv {
        DataValue::SmallInt(v) => ExprNode::Constant(ConstantValue::Int(*v as i64)),
        DataValue::Int(v) => ExprNode::Constant(ConstantValue::Int(*v as i64)),
        DataValue::BigInt(v) => ExprNode::Constant(ConstantValue::Int(*v)),
        DataValue::Real(v) => ExprNode::Constant(ConstantValue::Float(v.0 as f64)),
        DataValue::DoublePrecision(v) => ExprNode::Constant(ConstantValue::Float(v.0)),
        DataValue::Bool(v) => ExprNode::Constant(ConstantValue::Boolean(*v)),
        DataValue::Char(s) | DataValue::Varchar(s) => {
            ExprNode::Constant(ConstantValue::Text(s.clone()))
        }
        DataValue::Numeric(_) => text_cast(dv, dv.to_string()),
        DataValue::Date(d) => text_cast(dv, d.format("%Y-%m-%d").to_string()),
        DataValue::Time(t) => text_cast(dv, t.format("%H:%M:%S%.6f").to_string()),
        DataValue::Timestamp(t) => text_cast(dv, t.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
        DataValue::Bit(s) => text_cast(dv, s.clone()),
    }
}

// ── AST walk ──────────────────────────────────────────────────────────────────

/// Walk every expression of `select` that can hold an outer reference (SELECT
/// list, WHERE, JOIN ... ON, GROUP BY, HAVING) and let `f` replace a node:
/// `f(node, in_having)` returning `Some(new)` swaps it and stops descending.
/// ORDER BY is left alone (its names may be output aliases) and nested
/// subqueries are not entered (they resolve names in their own scope).
pub fn rewrite_select(
    select: &mut SelectPlan,
    f: &mut dyn FnMut(&ExprNode, bool) -> Option<ExprNode>,
) {
    for p in &mut select.projections {
        match p {
            SelectExpr::UnnamedExpr(e) | SelectExpr::ExprWithAlias { expr: e, .. } => {
                rewrite_expr(e, false, f)
            }
            SelectExpr::Wildcard | SelectExpr::QualifiedWildcard(_) => {}
        }
    }
    for j in &mut select.joins {
        if let Some(c) = &mut j.condition {
            rewrite_pred(c, false, f);
        }
    }
    if let Some(s) = &mut select.selection {
        rewrite_pred(s, false, f);
    }
    for g in &mut select.group_by {
        rewrite_expr(g, false, f);
    }
    if let Some(h) = &mut select.having {
        rewrite_pred(h, true, f);
    }
}

pub fn rewrite_pred(
    node: &mut PredicateNode,
    having: bool,
    f: &mut dyn FnMut(&ExprNode, bool) -> Option<ExprNode>,
) {
    match node {
        PredicateNode::BinaryOp { left, right, .. } => {
            rewrite_pred(left, having, f);
            rewrite_pred(right, having, f);
        }
        PredicateNode::Not(p) => rewrite_pred(p, having, f),
        PredicateNode::Compare { left, right, .. }
        | PredicateNode::IsDistinctFrom { left, right } => {
            rewrite_expr(left, having, f);
            rewrite_expr(right, having, f);
        }
        PredicateNode::IsNull(e)
        | PredicateNode::IsNotNull(e)
        | PredicateNode::Like { expr: e, .. }
        | PredicateNode::IsBoolean { expr: e, .. }
        | PredicateNode::InSubquery { expr: e, .. } => rewrite_expr(e, having, f),
        PredicateNode::Between { expr, low, high } => {
            rewrite_expr(expr, having, f);
            rewrite_expr(low, having, f);
            rewrite_expr(high, having, f);
        }
        PredicateNode::InList { expr, list } => {
            rewrite_expr(expr, having, f);
            for e in list {
                rewrite_expr(e, having, f);
            }
        }
        PredicateNode::Exists(_) => {}
    }
}

pub fn rewrite_expr(
    node: &mut ExprNode,
    having: bool,
    f: &mut dyn FnMut(&ExprNode, bool) -> Option<ExprNode>,
) {
    if let Some(new) = f(node, having) {
        *node = new;
        return;
    }
    match node {
        ExprNode::Column(_)
        | ExprNode::Compound(_)
        | ExprNode::Constant(_)
        | ExprNode::ScalarSubquery(_) => {}
        ExprNode::Binary { left, right, .. }
        | ExprNode::Compare { left, right, .. }
        | ExprNode::Logical { left, right, .. } => {
            rewrite_expr(left, having, f);
            rewrite_expr(right, having, f);
        }
        ExprNode::Cast { expr, .. }
        | ExprNode::Not(expr)
        | ExprNode::IsNull(expr)
        | ExprNode::IsNotNull(expr) => rewrite_expr(expr, having, f),
        ExprNode::Function { args, .. } => {
            for a in args {
                if let FunctionArg::Expr(e) = a {
                    rewrite_expr(e, having, f);
                }
            }
        }
        ExprNode::Case {
            when_then_pairs,
            else_result,
        } => {
            for (w, t) in when_then_pairs {
                rewrite_expr(w, having, f);
                rewrite_expr(t, having, f);
            }
            if let Some(e) = else_result {
                rewrite_expr(e, having, f);
            }
        }
    }
}
