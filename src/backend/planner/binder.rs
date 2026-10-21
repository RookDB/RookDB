//! Name resolution for SELECT: the binder.
//!
//! Before a SELECT is planned, every column it names is resolved once,
//! against the relations its FROM clause opens and those of the queries
//! around it:
//!
//! - a column of this query becomes `qualifier.column`: the qualifier its
//!   rows carry at run time (the alias, else the table, CTE or view name)
//!   and the column's stored spelling;
//! - a bare name that a `USING` / `NATURAL` join merged becomes what the
//!   merged column reads: the left copy, the right copy for a RIGHT join,
//!   `COALESCE` of both for a FULL join;
//! - a column of an enclosing query becomes [`ExprNode::OuterColumn`], so
//!   whether a subquery is correlated, and with what, is in the tree;
//! - `*` and `t.*` are expanded, and a GROUP BY position becomes the output
//!   expression it names;
//! - in GROUP BY, HAVING and ORDER BY an output alias stands for its
//!   expression (ORDER BY looks at outputs first, as in SQL; the others only
//!   when no input column has the name).
//!
//! An unknown or ambiguous name is reported here, before any planning.
//! Binding a bound SELECT again changes nothing: subqueries, CTE bodies and
//! views are planned (and so bound) again on their own later.

use std::collections::HashMap;

use rook_ast::*;

use super::helpers::{expr_to_name, not_found, resolve_table_ref};
use crate::backend::error::{RookError, RookResult};
use crate::catalog::{Catalog, Database};

const CTE_PREFIX: &str = "__cte__:";

/// Output column names of the CTEs a SELECT can read, by lower-cased name.
pub type CteColumns = HashMap<String, Vec<String>>;

/// A relation of a FROM clause.
struct Relation {
    /// Lower-cased names a qualifier may use for it.
    names: Vec<String>,
    /// The qualifier its columns carry at run time.
    stamp: String,
    columns: Vec<String>,
}

impl Relation {
    fn column(&self, name: &str) -> Option<&String> {
        self.columns.iter().find(|c| c.eq_ignore_ascii_case(name))
    }

    fn reference(&self, column: &str) -> ExprNode {
        ExprNode::Compound(vec![self.stamp.clone(), column.to_string()])
    }
}

/// The names one SELECT can read.
#[derive(Default)]
struct Scope {
    relations: Vec<Relation>,
    /// Bare names a USING / NATURAL join merged, with what each reads.
    merged: Vec<(String, ExprNode)>,
    /// What `*` shows: each column with its name, in order.
    star: Vec<(ExprNode, String)>,
    /// The SELECT outputs, once bound: name and expression.
    outputs: Vec<(String, ExprNode)>,
}

impl Scope {
    /// A bare name: `Ok(None)` when no relation has it.
    fn bare(&self, name: &str) -> RookResult<Option<ExprNode>> {
        if let Some((_, e)) = self
            .merged
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
        {
            return Ok(Some(e.clone()));
        }
        let mut found = self
            .relations
            .iter()
            .flat_map(|r| r.columns.iter().map(move |c| (r, c)))
            .filter(|(_, c)| c.eq_ignore_ascii_case(name));
        match (found.next(), found.next()) {
            (None, _) => Ok(None),
            (Some((r, c)), None) => Ok(Some(r.reference(c))),
            (Some(_), Some(_)) => Err(RookError::AmbiguousColumn(name.to_string())),
        }
    }

    fn relation(&self, qualifier: &str) -> Option<&Relation> {
        let q = qualifier.to_ascii_lowercase();
        self.relations.iter().find(|r| r.names.contains(&q))
    }

    /// The output an alias names, if exactly one expression has that name.
    fn output(&self, name: &str) -> RookResult<Option<ExprNode>> {
        let mut found = self
            .outputs
            .iter()
            .filter(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, e)| e);
        let Some(first) = found.next() else {
            return Ok(None);
        };
        if found.any(|e| e != first) {
            return Err(RookError::AmbiguousColumn(name.to_string()));
        }
        Ok(Some(first.clone()))
    }
}

/// Where a name is being read: which output aliases it may also mean.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Clause {
    /// FROM, WHERE, the SELECT list: input columns only.
    Input,
    /// GROUP BY and HAVING: an input column, else an output alias.
    Grouping,
    /// ORDER BY: a bare name is an output first, else an input column.
    OrderBy,
}

/// Bind `select` (see the module documentation). `ctes` are the CTEs it
/// can read besides its own WITH list.
pub fn bind_select(
    select: &SelectPlan,
    catalog: &Catalog,
    db_name: &str,
    ctes: &CteColumns,
) -> RookResult<SelectPlan> {
    let db = catalog
        .databases
        .get(db_name)
        .ok_or_else(|| not_found("Database", db_name))?;
    let mut binder = Binder {
        catalog,
        db_name,
        db,
        scopes: Vec::new(),
        ctes: vec![ctes.clone()],
    };
    binder.select(select)
}

/// The names of a bound SELECT's output columns.
pub fn output_names(select: &SelectPlan) -> Vec<String> {
    select
        .projections
        .iter()
        .filter_map(|p| match p {
            SelectExpr::UnnamedExpr(e) => Some(expr_to_name(e)),
            SelectExpr::ExprWithAlias { alias, .. } => Some(alias.clone()),
            SelectExpr::Wildcard | SelectExpr::QualifiedWildcard(_) => None,
        })
        .collect()
}

struct Binder<'a> {
    catalog: &'a Catalog,
    db_name: &'a str,
    db: &'a Database,
    /// The SELECTs being bound, innermost last.
    scopes: Vec<Scope>,
    /// The CTEs visible, innermost WITH list last.
    ctes: Vec<CteColumns>,
}

impl Binder<'_> {
    fn select(&mut self, select: &SelectPlan) -> RookResult<SelectPlan> {
        self.ctes.push(CteColumns::new());
        self.scopes.push(Scope::default());
        let bound = self.select_in_scope(select);
        self.scopes.pop();
        self.ctes.pop();
        bound
    }

    fn scope(&mut self) -> &mut Scope {
        self.scopes.last_mut().expect("binding inside a SELECT")
    }

    fn select_in_scope(&mut self, select: &SelectPlan) -> RookResult<SelectPlan> {
        let mut out = select.clone();

        // A CTE body sees earlier CTEs but no columns of this query.
        let outer_scopes = std::mem::take(&mut self.scopes);
        let ctes = self.ctes(&mut out.ctes);
        self.scopes = outer_scopes;
        ctes?;

        // FROM and JOIN: each ON condition sees the relations joined so far.
        if let Some(first) = select.from.first() {
            let relation = self.relation(first)?;
            self.add_relation(relation, None)?;
        }
        for (i, join) in select.joins.iter().enumerate() {
            let relation = self.relation(&join.relation)?;
            self.add_relation(relation, Some(join))?;
            if let Some(cond) = &join.condition {
                out.joins[i].condition = Some(self.pred(cond, Clause::Input)?);
            }
        }

        // The SELECT list, with `*` expanded and names kept as written.
        let mut projections = Vec::with_capacity(select.projections.len());
        for item in &select.projections {
            match item {
                SelectExpr::Wildcard => {
                    for (expr, name) in self.scope().star.clone() {
                        projections.push(named(expr, name));
                    }
                }
                SelectExpr::QualifiedWildcard(q) => {
                    let scope = self.scope();
                    let relation = scope.relation(q).ok_or_else(|| missing_relation(q))?;
                    for c in &relation.columns {
                        projections.push(named(relation.reference(c), c.clone()));
                    }
                }
                SelectExpr::UnnamedExpr(e) => {
                    let bound = self.expr(e, Clause::Input)?;
                    projections.push(named(bound, expr_to_name(e)));
                }
                SelectExpr::ExprWithAlias { expr, alias } => {
                    projections.push(SelectExpr::ExprWithAlias {
                        expr: self.expr(expr, Clause::Input)?,
                        alias: alias.clone(),
                    });
                }
            }
        }
        let outputs = projections
            .iter()
            .filter_map(|p| match p {
                SelectExpr::UnnamedExpr(e) => Some((expr_to_name(e), e.clone())),
                SelectExpr::ExprWithAlias { expr, alias } => Some((alias.clone(), expr.clone())),
                SelectExpr::Wildcard | SelectExpr::QualifiedWildcard(_) => None,
            })
            .collect();
        self.scope().outputs = outputs;
        out.projections = projections;

        if let Some(pred) = &select.selection {
            out.selection = Some(self.pred(pred, Clause::Input)?);
        }

        let mut group_by = Vec::with_capacity(select.group_by.len());
        for key in &select.group_by {
            group_by.push(match key {
                ExprNode::Constant(ConstantValue::Int(pos)) => {
                    let outputs = &self.scope().outputs;
                    usize::try_from(*pos)
                        .ok()
                        .and_then(|p| p.checked_sub(1))
                        .and_then(|i| outputs.get(i))
                        .map(|(_, e)| e.clone())
                        .ok_or_else(|| {
                            RookError::InvalidQuery(format!(
                                "GROUP BY position {} is out of range (1..{})",
                                pos,
                                outputs.len()
                            ))
                        })?
                }
                other => self.expr(other, Clause::Grouping)?,
            });
        }
        out.group_by = group_by;

        if let Some(pred) = &select.having {
            out.having = Some(self.pred(pred, Clause::Grouping)?);
        }
        for (ob, key) in out.order_by.iter_mut().zip(&select.order_by) {
            ob.expr = match &key.expr {
                // A position is resolved against the output by the planner.
                e @ ExprNode::Constant(_) => e.clone(),
                e => self.expr(e, Clause::OrderBy)?,
            };
        }
        Ok(out)
    }

    /// Bind a WITH list in order, making each CTE visible to the next.
    fn ctes(&mut self, ctes: &mut [CteDef]) -> RookResult<()> {
        for cte in ctes {
            *cte.query = self.select(&cte.query)?;
            let columns = output_names(&cte.query);
            self.ctes
                .last_mut()
                .expect("a CTE frame")
                .insert(cte.name.to_ascii_lowercase(), columns);
            if let Some(term) = &cte.recursive_term {
                cte.recursive_term = Some(Box::new(self.select(term)?));
            }
        }
        Ok(())
    }

    /// Add a FROM relation to the scope; `join` is the clause joining it.
    fn add_relation(&mut self, relation: Relation, join: Option<&JoinClause>) -> RookResult<()> {
        let scope = self.scope();
        if let Some(dup) = relation
            .names
            .iter()
            .find(|n| scope.relations.iter().any(|r| r.names.contains(n)))
        {
            return Err(RookError::InvalidQuery(format!(
                "table name '{}' specified more than once",
                dup
            )));
        }
        let right: Vec<(ExprNode, String)> = relation
            .columns
            .iter()
            .map(|c| (relation.reference(c), c.clone()))
            .collect();
        let Some(join) = join else {
            scope.star = right;
            scope.relations.push(relation);
            return Ok(());
        };
        let left = std::mem::take(&mut scope.star);
        let has = |items: &[(ExprNode, String)], n: &str| {
            items.iter().any(|(_, name)| name.eq_ignore_ascii_case(n))
        };
        let shared: Vec<String> = match join.join_type {
            JoinType::Natural => left
                .iter()
                .filter(|(_, n)| has(&right, n))
                .map(|(_, n)| n.clone())
                .collect(),
            _ => {
                for n in &join.using {
                    if !has(&left, n) || !has(&right, n) {
                        return Err(RookError::NotFound {
                            entity: "Column",
                            name: format!("{} (in USING)", n),
                        });
                    }
                }
                join.using.clone()
            }
        };
        let find = |items: &[(ExprNode, String)], n: &str| {
            items
                .iter()
                .find(|(_, name)| name.eq_ignore_ascii_case(n))
                .map(|(e, _)| e.clone())
        };
        let mut star = Vec::new();
        for n in &shared {
            let (Some(l), Some(r)) = (find(&left, n), find(&right, n)) else {
                continue;
            };
            let expr = match join.join_type {
                JoinType::Right => r,
                JoinType::Full => ExprNode::Function {
                    name: "COALESCE".to_string(),
                    args: vec![
                        FunctionArg::Expr(Box::new(l)),
                        FunctionArg::Expr(Box::new(r)),
                    ],
                    distinct: false,
                },
                _ => l,
            };
            scope.merged.retain(|(m, _)| !m.eq_ignore_ascii_case(n));
            scope.merged.push((n.clone(), expr.clone()));
            star.push((expr, n.clone()));
        }
        let is_shared = |name: &str| shared.iter().any(|n| n.eq_ignore_ascii_case(name));
        star.extend(left.into_iter().filter(|(_, n)| !is_shared(n)));
        star.extend(right.into_iter().filter(|(_, n)| !is_shared(n)));
        scope.star = star;
        scope.relations.push(relation);
        Ok(())
    }

    /// What a FROM item is: its names, run-time qualifier and columns.
    fn relation(&self, tref: &TableRef) -> RookResult<Relation> {
        let alias = tref.alias.as_ref();
        let named = |stamp: String, columns: Vec<String>| Relation {
            names: vec![stamp.to_ascii_lowercase()],
            stamp,
            columns,
        };
        let cte_name = tref.name.strip_prefix(CTE_PREFIX);
        let raw = cte_name.unwrap_or(&tref.name);
        let cte = self
            .ctes
            .iter()
            .rev()
            .find_map(|frame| frame.get(&raw.to_ascii_lowercase()));
        if let Some(columns) = cte {
            let stamp = alias.cloned().unwrap_or_else(|| raw.to_string());
            return Ok(named(stamp, columns.clone()));
        }
        if cte_name.is_some() {
            return Err(not_found("CTE", raw));
        }
        if let Some(view) = tref.name.strip_prefix("information_schema.") {
            let view = view.to_ascii_lowercase();
            let columns = crate::backend::system_table::info_schema_column_schema(&view)
                .columns
                .into_iter()
                .map(|c| c.name)
                .collect();
            let system_table = super::helpers::info_schema_table(&view);
            return Ok(match alias {
                Some(a) => named(a.clone(), columns),
                None => Relation {
                    names: vec![view.clone(), system_table.to_string()],
                    stamp: system_table.to_string(),
                    columns,
                },
            });
        }
        if let Some(view) = self.db.views.get(&tref.name) {
            let query: SelectPlan = serde_json::from_str(&view.query_json).map_err(|e| {
                RookError::Internal(format!("Failed to deserialise view '{}': {}", tref.name, e))
            })?;
            let bound = bind_select(&query, self.catalog, self.db_name, &CteColumns::new())?;
            let stamp = alias.cloned().unwrap_or_else(|| tref.name.clone());
            return Ok(named(stamp, output_names(&bound)));
        }
        let (table, alias) = resolve_table_ref(tref, self.db)?;
        let columns = self.db.tables[&table]
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();
        Ok(named(alias.unwrap_or(table), columns))
    }

    fn expr(&mut self, expr: &ExprNode, clause: Clause) -> RookResult<ExprNode> {
        match expr {
            ExprNode::Column(name) => self.column(None, name, clause),
            ExprNode::Compound(parts) => match parts.as_slice() {
                [] => Err(RookError::Internal("Empty compound identifier".to_string())),
                [name] => self.column(None, name, clause),
                [.., q, name] => self.column(Some(q), name, clause),
            },
            ExprNode::ScalarSubquery(info) => Ok(ExprNode::ScalarSubquery(SubqueryInfo {
                select: Box::new(self.select(&info.select)?),
            })),
            ExprNode::Predicate(p) => Ok(ExprNode::Predicate(Box::new(self.pred(p, clause)?))),
            other => {
                let mut error = None;
                let bound = other.map_children(&mut |child| match self.expr(child, clause) {
                    Ok(e) => e,
                    Err(e) => {
                        error.get_or_insert(e);
                        child.clone()
                    }
                });
                error.map_or(Ok(bound), Err)
            }
        }
    }

    fn pred(&mut self, pred: &PredicateNode, clause: Clause) -> RookResult<PredicateNode> {
        Ok(match pred {
            PredicateNode::BinaryOp { left, op, right } => PredicateNode::BinaryOp {
                left: Box::new(self.pred(left, clause)?),
                op: *op,
                right: Box::new(self.pred(right, clause)?),
            },
            PredicateNode::Not(p) => PredicateNode::Not(Box::new(self.pred(p, clause)?)),
            PredicateNode::Exists(info) => PredicateNode::Exists(SubqueryInfo {
                select: Box::new(self.select(&info.select)?),
            }),
            PredicateNode::InSubquery {
                expr,
                subquery,
                negated,
            } => PredicateNode::InSubquery {
                expr: Box::new(self.expr(expr, clause)?),
                subquery: SubqueryInfo {
                    select: Box::new(self.select(&subquery.select)?),
                },
                negated: *negated,
            },
            other => {
                let mut error = None;
                let bound = other.map_exprs(&mut |e| match self.expr(e, clause) {
                    Ok(e) => e,
                    Err(err) => {
                        error.get_or_insert(err);
                        e.clone()
                    }
                });
                return error.map_or(Ok(bound), Err);
            }
        })
    }

    /// Resolve a column reference, innermost query first.
    fn column(
        &self,
        qualifier: Option<&String>,
        name: &str,
        clause: Clause,
    ) -> RookResult<ExprNode> {
        let written = || match qualifier {
            Some(q) => format!("{}.{}", q, name),
            None => name.to_string(),
        };
        for (levels, scope) in self.scopes.iter().rev().enumerate() {
            let here = levels == 0;
            if here
                && qualifier.is_none()
                && clause == Clause::OrderBy
                && let Some(e) = scope.output(name)?
            {
                return Ok(e);
            }
            let found = match qualifier {
                Some(q) => match scope.relation(q) {
                    Some(r) => Some(r.column(name).map(|c| r.reference(c)).ok_or_else(|| {
                        RookError::NotFound {
                            entity: "Column",
                            name: written(),
                        }
                    })?),
                    None => None,
                },
                None => scope.bare(name)?,
            };
            if let Some(e) = found {
                return Ok(if here { e } else { outer(&e, levels) });
            }
            if here
                && qualifier.is_none()
                && clause == Clause::Grouping
                && let Some(e) = scope.output(name)?
            {
                return Ok(e);
            }
        }
        Err(match qualifier {
            Some(q) => missing_relation(q),
            None => RookError::NotFound {
                entity: "Column",
                name: written(),
            },
        })
    }
}

/// `expr` read from a query `levels` out: its columns become outer columns.
fn outer(expr: &ExprNode, levels: usize) -> ExprNode {
    match expr {
        ExprNode::Compound(parts) if parts.len() == 2 => ExprNode::OuterColumn(OuterRef {
            levels,
            table: Some(parts[0].clone()),
            column: parts[1].clone(),
        }),
        other => other.map_children(&mut |e| outer(e, levels)),
    }
}

/// A SELECT item for `expr`, keeping `name` as its output name.
fn named(expr: ExprNode, name: String) -> SelectExpr {
    if expr_to_name(&expr) == name {
        SelectExpr::UnnamedExpr(expr)
    } else {
        SelectExpr::ExprWithAlias { expr, alias: name }
    }
}

fn missing_relation(qualifier: &str) -> RookError {
    RookError::InvalidQuery(format!(
        "missing FROM-clause entry for table '{}'",
        qualifier
    ))
}
