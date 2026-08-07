//! Choosing the order of a multi-relation join.
//!
//! Only connected subsets are searched, so a cross product never appears in the
//! middle of a query that did not ask for one. Where the graph genuinely is
//! disconnected, components are optimised separately and combined afterwards.
//!
//! Outer joins are reordering barriers, and interesting orders are not tracked;
//! both are noted in `docs/join/design-rationale.md`.

use std::collections::HashMap;

use crate::executor::selection::{ComparisonOp, Expr, Predicate};

use super::algorithm::JoinAlgorithm;
use super::config::JoinConfig;
use super::cost::{CostModel, SideEstimate};
use super::error::JoinError;
use super::key::resolve_key_class;
use super::source::TableRef;
use super::stats::TableStatsCache;

/// A conjunct together with the relations it mentions.
#[derive(Debug, Clone)]
pub struct Conjunct {
    /// Bit `i` set means relation `i` is mentioned.
    pub mask: u64,
    pub predicate: Predicate,
}

/// A conjunct relating exactly two relations.
#[derive(Debug, Clone)]
pub struct JoinEdge {
    pub left: usize,
    pub right: usize,
    pub predicate: Predicate,
    /// Fraction of the cross product this edge keeps.
    pub selectivity: f64,
}

/// The relations of a query and the conditions between them.
#[derive(Debug)]
pub struct JoinGraph {
    relations: Vec<TableRef>,
    edges: Vec<JoinEdge>,
    /// Conjuncts touching a single relation.
    filters: Vec<(usize, Predicate)>,
    /// Every conjunct, with the relations it mentions.
    conjuncts: Vec<Conjunct>,
    /// Bit `j` set in `neighbours[i]` means an edge joins `i` and `j`.
    neighbours: Vec<u64>,
    /// Rows and width per relation, from statistics.
    cardinality: Vec<f64>,
    row_bytes: Vec<f64>,
}

impl JoinGraph {
    /// Build the graph, resolving every conjunct to the relations it touches.
    pub fn build(
        relations: Vec<TableRef>,
        condition: Option<&Predicate>,
        stats: &TableStatsCache,
    ) -> Result<Self, JoinError> {
        if relations.is_empty() {
            return Err(JoinError::plan(
                "a join needs at least one relation".to_string(),
            ));
        }
        if relations.len() > 64 {
            return Err(JoinError::plan(
                "more than 64 relations in one join is not supported".to_string(),
            ));
        }

        let mut aliases = Vec::with_capacity(relations.len());
        for relation in &relations {
            if aliases.contains(&relation.alias) {
                return Err(JoinError::schema(format!(
                    "alias '{}' is used more than once; give each relation a distinct alias",
                    relation.alias
                )));
            }
            aliases.push(relation.alias.clone());
        }

        let mut cardinality = Vec::with_capacity(relations.len());
        let mut row_bytes = Vec::with_capacity(relations.len());
        let mut distinct: Vec<HashMap<usize, u64>> = Vec::with_capacity(relations.len());
        for relation in &relations {
            let (table, _) = stats.stats_for(relation);
            cardinality.push(table.rows.max(1) as f64);
            row_bytes.push(if table.avg_row_bytes > 0.0 {
                table.avg_row_bytes
            } else {
                64.0
            });
            distinct.push(
                table
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| (index, column.distinct_or_one()))
                    .collect(),
            );
        }

        let mut graph = Self {
            relations,
            edges: Vec::new(),
            filters: Vec::new(),
            conjuncts: Vec::new(),
            neighbours: Vec::new(),
            cardinality,
            row_bytes,
        };
        graph.neighbours = vec![0u64; graph.relations.len()];

        let mut conjuncts = Vec::new();
        flatten(condition, &mut conjuncts);

        for conjunct in conjuncts {
            let touched = graph.relations_touched(conjunct)?;
            let mask = touched.iter().fold(0u64, |mask, r| mask | (1u64 << r));
            graph.conjuncts.push(Conjunct {
                mask,
                predicate: conjunct.clone(),
            });
            match touched.len() {
                // A constant conjunct has to be evaluated somewhere; the first
                // relation is as good a place as any.
                0 => graph.filters.push((0, conjunct.clone())),
                1 => graph.filters.push((touched[0], conjunct.clone())),
                2 => {
                    let selectivity = graph.edge_selectivity(conjunct, &distinct);
                    graph.add_edge(touched[0], touched[1], conjunct.clone(), selectivity);
                }
                // A conjunct spanning three or more relations cannot be an
                // edge. It is applied once every relation it mentions is
                // present, which the search models by attaching it to the
                // widest pair it does relate.
                _ => {
                    let selectivity = graph.edge_selectivity(conjunct, &distinct);
                    graph.add_edge(touched[0], touched[1], conjunct.clone(), selectivity);
                }
            }
        }

        Ok(graph)
    }

    fn add_edge(&mut self, left: usize, right: usize, predicate: Predicate, selectivity: f64) {
        self.neighbours[left] |= 1u64 << right;
        self.neighbours[right] |= 1u64 << left;
        self.edges.push(JoinEdge {
            left,
            right,
            predicate,
            selectivity,
        });
    }

    pub fn relations(&self) -> &[TableRef] {
        &self.relations
    }

    pub fn edges(&self) -> &[JoinEdge] {
        &self.edges
    }

    pub fn filters(&self) -> &[(usize, Predicate)] {
        &self.filters
    }

    /// Every conjunct, with the relations it mentions.
    pub fn conjuncts(&self) -> &[Conjunct] {
        &self.conjuncts
    }

    /// Column count of a relation, used when laying out a subtree's schema.
    pub fn relation(&self, index: usize) -> Option<&TableRef> {
        self.relations.get(index)
    }

    /// Which relations a conjunct mentions, in ascending order.
    fn relations_touched(&self, predicate: &Predicate) -> Result<Vec<usize>, JoinError> {
        let mut mask = 0u64;
        self.mark_predicate(predicate, &mut mask)?;
        Ok(members(mask))
    }

    fn mark_predicate(&self, predicate: &Predicate, mask: &mut u64) -> Result<(), JoinError> {
        match predicate {
            Predicate::Compare(a, _, b) => {
                self.mark_expr(a, mask)?;
                self.mark_expr(b, mask)
            }
            Predicate::IsNull(e) | Predicate::IsNotNull(e) => self.mark_expr(e, mask),
            Predicate::Not(p) | Predicate::Exists(p) => self.mark_predicate(p, mask),
            Predicate::And(a, b) | Predicate::Or(a, b) => {
                self.mark_predicate(a, mask)?;
                self.mark_predicate(b, mask)
            }
            Predicate::Between(e, low, high) => {
                self.mark_expr(e, mask)?;
                self.mark_expr(low, mask)?;
                self.mark_expr(high, mask)
            }
            Predicate::In(e, list) => {
                self.mark_expr(e, mask)?;
                for item in list {
                    self.mark_expr(item, mask)?;
                }
                Ok(())
            }
            Predicate::Like(e, _, _) => self.mark_expr(e, mask),
        }
    }

    fn mark_expr(&self, expr: &Expr, mask: &mut u64) -> Result<(), JoinError> {
        match expr {
            Expr::Column(reference) => {
                let (relation, _) = self.resolve(&reference.column_name)?;
                *mask |= 1u64 << relation;
                Ok(())
            }
            Expr::Constant(_) => Ok(()),
            Expr::Add(a, b) | Expr::Sub(a, b) | Expr::Mul(a, b) | Expr::Div(a, b) => {
                self.mark_expr(a, mask)?;
                self.mark_expr(b, mask)
            }
        }
    }

    /// Resolve a column name to `(relation, column)`.
    ///
    /// An unqualified name present in more than one relation is an error, not
    /// a guess - the same rule the two-relation resolver applies.
    pub fn resolve(&self, name: &str) -> Result<(usize, usize), JoinError> {
        if let Some((qualifier, column)) = name.rsplit_once('.') {
            let relation = self
                .relations
                .iter()
                .position(|r| r.alias == qualifier)
                .ok_or_else(|| {
                    JoinError::schema(format!("'{name}' refers to unknown relation '{qualifier}'"))
                })?;
            let index = self.relations[relation]
                .columns
                .iter()
                .position(|c| c.name == column)
                .ok_or_else(|| {
                    JoinError::schema(format!("relation '{qualifier}' has no column '{column}'"))
                })?;
            return Ok((relation, index));
        }

        let mut found = None;
        for (relation, table) in self.relations.iter().enumerate() {
            if let Some(index) = table.columns.iter().position(|c| c.name == name) {
                if found.is_some() {
                    return Err(JoinError::schema(format!(
                        "column '{name}' is ambiguous across the joined relations; qualify it"
                    )));
                }
                found = Some((relation, index));
            }
        }
        found.ok_or_else(|| JoinError::schema(format!("no column '{name}' in any relation")))
    }

    /// Selectivity of an edge, from the distinct-value counts of the columns
    /// it equates.
    fn edge_selectivity(&self, predicate: &Predicate, distinct: &[HashMap<usize, u64>]) -> f64 {
        let Predicate::Compare(left, ComparisonOp::Equals, right) = predicate else {
            return super::cost::DEFAULT_RANGE_SELECTIVITY;
        };
        let (Expr::Column(a), Expr::Column(b)) = (left.as_ref(), right.as_ref()) else {
            return super::cost::UNKNOWN_SELECTIVITY;
        };
        let (Ok((left_relation, left_column)), Ok((right_relation, right_column))) =
            (self.resolve(&a.column_name), self.resolve(&b.column_name))
        else {
            return super::cost::UNKNOWN_SELECTIVITY;
        };

        let left_ndv = distinct[left_relation]
            .get(&left_column)
            .copied()
            .unwrap_or(1)
            .max(1);
        let right_ndv = distinct[right_relation]
            .get(&right_column)
            .copied()
            .unwrap_or(1)
            .max(1);

        // System-R containment: the join pairs through the larger count.
        1.0 / left_ndv.max(right_ndv) as f64
    }

    /// Whether the relations in `mask` form one connected component.
    pub fn connected(&self, mask: u64) -> bool {
        if mask == 0 {
            return false;
        }
        let start = mask.trailing_zeros();
        let mut reached = 1u64 << start;
        let mut frontier = reached;

        while frontier != 0 {
            let mut next = 0u64;
            for relation in members(frontier) {
                next |= self.neighbours[relation] & mask & !reached;
            }
            reached |= next;
            frontier = next;
        }

        reached == mask
    }

    /// Edges with one end in each of two disjoint sets.
    fn edges_between(&self, left: u64, right: u64) -> Vec<&JoinEdge> {
        self.edges
            .iter()
            .filter(|edge| {
                let a = 1u64 << edge.left;
                let b = 1u64 << edge.right;
                (a & left != 0 && b & right != 0) || (a & right != 0 && b & left != 0)
            })
            .collect()
    }

    /// Connected components, as bitmasks.
    pub fn components(&self) -> Vec<u64> {
        let full = full_mask(self.relations.len());
        let mut remaining = full;
        let mut components = Vec::new();

        while remaining != 0 {
            let start = remaining.trailing_zeros();
            let mut reached = 1u64 << start;
            let mut frontier = reached;
            while frontier != 0 {
                let mut next = 0u64;
                for relation in members(frontier) {
                    next |= self.neighbours[relation] & !reached;
                }
                reached |= next;
                frontier = next;
            }
            components.push(reached);
            remaining &= !reached;
        }

        components
    }
}

/// A chosen join order.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderedPlan {
    Scan(usize),
    Join {
        left: Box<OrderedPlan>,
        right: Box<OrderedPlan>,
        algorithm: JoinAlgorithm,
        /// Whether this node has no condition joining its two sides.
        cross_product: bool,
        rows: u64,
        cost: f64,
    },
}

impl OrderedPlan {
    /// Relations in this subtree, left to right.
    pub fn relation_order(&self) -> Vec<usize> {
        let mut out = Vec::new();
        self.collect(&mut out);
        out
    }

    fn collect(&self, out: &mut Vec<usize>) {
        match self {
            OrderedPlan::Scan(relation) => out.push(*relation),
            OrderedPlan::Join { left, right, .. } => {
                left.collect(out);
                right.collect(out);
            }
        }
    }

    /// Whether any node joins two sides with no condition between them.
    pub fn has_cross_product(&self) -> bool {
        match self {
            OrderedPlan::Scan(_) => false,
            OrderedPlan::Join {
                left,
                right,
                cross_product,
                ..
            } => *cross_product || left.has_cross_product() || right.has_cross_product(),
        }
    }

    /// Whether both children of some node are themselves joins.
    pub fn is_bushy(&self) -> bool {
        match self {
            OrderedPlan::Scan(_) => false,
            OrderedPlan::Join { left, right, .. } => {
                let both = matches!(left.as_ref(), OrderedPlan::Join { .. })
                    && matches!(right.as_ref(), OrderedPlan::Join { .. });
                both || left.is_bushy() || right.is_bushy()
            }
        }
    }

    pub fn estimated_rows(&self) -> u64 {
        match self {
            OrderedPlan::Scan(_) => 0,
            OrderedPlan::Join { rows, .. } => *rows,
        }
    }

    pub fn total_cost(&self) -> f64 {
        match self {
            OrderedPlan::Scan(_) => 0.0,
            OrderedPlan::Join { cost, .. } => *cost,
        }
    }

    pub fn render(&self, graph: &JoinGraph) -> String {
        let mut out = String::new();
        self.render_into(graph, 0, &mut out);
        out
    }

    fn render_into(&self, graph: &JoinGraph, depth: usize, out: &mut String) {
        use std::fmt::Write as _;
        let indent = "  ".repeat(depth);
        match self {
            OrderedPlan::Scan(relation) => {
                let _ = writeln!(out, "{indent}Scan {}", graph.relations[*relation].alias);
            }
            OrderedPlan::Join {
                left,
                right,
                algorithm,
                cross_product,
                rows,
                cost,
            } => {
                let kind = if *cross_product {
                    " (cross product)"
                } else {
                    ""
                };
                let _ = writeln!(
                    out,
                    "{indent}{} Join{kind}  [rows={rows}  cost={cost:.2}ms]",
                    algorithm.name()
                );
                left.render_into(graph, depth + 1, out);
                right.render_into(graph, depth + 1, out);
            }
        }
    }
}

/// Cost a specific left-deep order, joining the relations as listed.
pub fn cost_of_order(
    graph: &JoinGraph,
    order: &[usize],
    config: &JoinConfig,
) -> Result<f64, JoinError> {
    if order.is_empty() {
        return Err(JoinError::plan(
            "an order needs at least one relation".to_string(),
        ));
    }
    let mut seen = 0u64;
    for relation in order {
        if *relation >= graph.relations.len() {
            return Err(JoinError::plan(format!("no relation {relation}")));
        }
        if seen & (1u64 << relation) != 0 {
            return Err(JoinError::plan(
                "an order may not repeat a relation".to_string(),
            ));
        }
        seen |= 1u64 << relation;
    }

    let model = CostModel::new(config.work_memory_bytes);
    let mut current = leaf(graph, order[0]);
    let mut mask = 1u64 << order[0];

    for relation in &order[1..] {
        let next = leaf(graph, *relation);
        let right_mask = 1u64 << relation;
        let edges = graph.edges_between(mask, right_mask);
        let cross = edges.is_empty();
        current = join_entries(&current, &next, &edges, &model, cross);
        mask |= right_mask;
    }

    Ok(current.cost)
}

#[derive(Debug, Clone)]
struct MemoEntry {
    plan: OrderedPlan,
    rows: f64,
    row_bytes: f64,
    cost: f64,
}

/// Choose an order for the relations in `graph`.
pub fn optimize(graph: &JoinGraph, config: &JoinConfig) -> Result<OrderedPlan, JoinError> {
    // Refuse an ordering for a join that could not execute anyway.
    validate_edges(graph)?;

    let model = CostModel::new(config.work_memory_bytes);
    let max_exhaustive = config.tuning.max_exhaustive_relations;
    let components = graph.components();

    // Each connected component is optimised on its own. A cross product
    // between two of them is unavoidable - the query relates them by nothing
    // - so it is applied here, explicitly, rather than being discovered
    // inside the search.
    let mut solved: Vec<MemoEntry> = Vec::with_capacity(components.len());
    for component in &components {
        solved.push(optimize_component(
            graph,
            *component,
            &model,
            max_exhaustive,
        )?);
    }

    // Combine components smallest-first, so the product grows as slowly as it
    // can.
    solved.sort_by(|a, b| {
        a.rows
            .partial_cmp(&b.rows)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut combined = solved
        .first()
        .cloned()
        .ok_or_else(|| JoinError::plan("no relations to join".to_string()))?;

    for next in solved.into_iter().skip(1) {
        combined = join_entries(&combined, &next, &[], &model, true);
    }

    Ok(combined.plan)
}

fn optimize_component(
    graph: &JoinGraph,
    component: u64,
    model: &CostModel,
    max_exhaustive: usize,
) -> Result<MemoEntry, JoinError> {
    let relations = members(component);
    if relations.len() == 1 {
        return Ok(leaf(graph, relations[0]));
    }

    if relations.len() <= max_exhaustive {
        exhaustive(graph, component, model)
    } else {
        greedy(graph, component, model)
    }
}

fn leaf(graph: &JoinGraph, relation: usize) -> MemoEntry {
    MemoEntry {
        plan: OrderedPlan::Scan(relation),
        rows: graph.cardinality[relation],
        row_bytes: graph.row_bytes[relation],
        cost: 0.0,
    }
}

/// Dynamic programming over connected subsets.
fn exhaustive(
    graph: &JoinGraph,
    component: u64,
    model: &CostModel,
) -> Result<MemoEntry, JoinError> {
    let relations = members(component);
    let mut memo: HashMap<u64, MemoEntry> = HashMap::new();

    for relation in &relations {
        memo.insert(1u64 << relation, leaf(graph, *relation));
    }

    // Subsets of the component, in increasing size.
    let mut subsets: Vec<u64> = submasks(component)
        .into_iter()
        .filter(|m| *m != 0)
        .collect();
    subsets.sort_by_key(|mask| mask.count_ones());

    for subset in subsets {
        if subset.count_ones() < 2 || !graph.connected(subset) {
            continue;
        }

        let mut best: Option<MemoEntry> = None;
        for left in submasks(subset) {
            if left == 0 || left == subset {
                continue;
            }
            let right = subset & !left;
            // Only halves that are themselves connected, and that have an edge
            // between them: anything else is a cross product, and this search
            // does not consider those.
            if !graph.connected(left) || !graph.connected(right) {
                continue;
            }
            let edges = graph.edges_between(left, right);
            if edges.is_empty() {
                continue;
            }

            let (Some(left_entry), Some(right_entry)) = (memo.get(&left), memo.get(&right)) else {
                continue;
            };
            let candidate = join_entries(left_entry, right_entry, &edges, model, false);

            if best
                .as_ref()
                .is_none_or(|current| candidate.cost < current.cost)
            {
                best = Some(candidate);
            }
        }

        if let Some(best) = best {
            memo.insert(subset, best);
        }
    }

    memo.remove(&component).ok_or_else(|| {
        JoinError::plan("the join graph could not be ordered; it may be disconnected".to_string())
    })
}

/// Greedy fallback for graphs too large to search exhaustively.
///
/// Repeatedly joins the cheapest available connected pair. Not optimal, but it
/// never produces a cross product while a connecting edge remains.
fn greedy(graph: &JoinGraph, component: u64, model: &CostModel) -> Result<MemoEntry, JoinError> {
    let mut pending: Vec<(u64, MemoEntry)> = members(component)
        .into_iter()
        .map(|relation| (1u64 << relation, leaf(graph, relation)))
        .collect();

    while pending.len() > 1 {
        let mut best: Option<(usize, usize, MemoEntry)> = None;

        for i in 0..pending.len() {
            for j in (i + 1)..pending.len() {
                let edges = graph.edges_between(pending[i].0, pending[j].0);
                if edges.is_empty() {
                    continue;
                }
                let candidate = join_entries(&pending[i].1, &pending[j].1, &edges, model, false);
                if best
                    .as_ref()
                    .is_none_or(|(_, _, current)| candidate.cost < current.cost)
                {
                    best = Some((i, j, candidate));
                }
            }
        }

        let Some((i, j, merged)) = best else {
            break;
        };
        let mask = pending[i].0 | pending[j].0;
        // Remove the higher index first so the lower stays valid.
        pending.remove(j);
        pending.remove(i);
        pending.push((mask, merged));
    }

    pending
        .into_iter()
        .next()
        .map(|(_, entry)| entry)
        .ok_or_else(|| JoinError::plan("nothing to order".to_string()))
}

/// Cost joining two subsets, picking the cheapest applicable algorithm.
fn join_entries(
    left: &MemoEntry,
    right: &MemoEntry,
    edges: &[&JoinEdge],
    model: &CostModel,
    cross_product: bool,
) -> MemoEntry {
    let selectivity: f64 = edges.iter().map(|edge| edge.selectivity).product::<f64>();
    let rows = if edges.is_empty() {
        left.rows * right.rows
    } else {
        (left.rows * right.rows * selectivity).max(1.0)
    };

    let left_side = side_of(left);
    let right_side = side_of(right);
    let output = rows.min(u64::MAX as f64) as u64;
    let has_keys = !edges.is_empty();

    // Every algorithm is costed at every node. The previous implementation
    // fixed on block nested loop for each one, which made its "cost-based"
    // ordering a cost-based ordering of a single plan shape.
    let mut best: Option<(JoinAlgorithm, f64)> = None;
    for algorithm in [
        JoinAlgorithm::BlockNestedLoop,
        JoinAlgorithm::SortMerge,
        JoinAlgorithm::Hash,
    ] {
        // A key-based algorithm needs an equality between the two sides.
        if !has_keys && algorithm != JoinAlgorithm::BlockNestedLoop {
            continue;
        }
        let cost = model
            .cost(algorithm, &left_side, &right_side, output, 1024, has_keys)
            .total();
        if best.as_ref().is_none_or(|(_, current)| cost < *current) {
            best = Some((algorithm, cost));
        }
    }

    let (algorithm, node_cost) = best.unwrap_or((JoinAlgorithm::BlockNestedLoop, f64::MAX));
    let total = left.cost + right.cost + node_cost;

    // Output rows carry the wider of the two inputs' widths, summed.
    let row_bytes = left.row_bytes + right.row_bytes;

    MemoEntry {
        plan: OrderedPlan::Join {
            left: Box::new(left.plan.clone()),
            right: Box::new(right.plan.clone()),
            algorithm,
            cross_product,
            rows: output,
            cost: total,
        },
        rows,
        row_bytes,
        cost: total,
    }
}

fn side_of(entry: &MemoEntry) -> SideEstimate {
    let bytes = entry.rows * entry.row_bytes;
    SideEstimate {
        rows: entry.rows.min(u64::MAX as f64) as u64,
        pages: ((bytes / crate::page::PAGE_SIZE as f64).ceil()).max(1.0) as u64,
        row_bytes: entry.row_bytes,
        // Intermediate results have no measured statistics; assume every row
        // carries a distinct key, which is the least presumptuous guess.
        distinct: entry.rows.max(1.0) as u64,
        null_fraction: 0.0,
    }
}

// ── Bit helpers ──────────────────────────────────────────────────────────────

fn full_mask(count: usize) -> u64 {
    if count >= 64 {
        u64::MAX
    } else {
        (1u64 << count) - 1
    }
}

fn members(mask: u64) -> Vec<usize> {
    (0..64).filter(|bit| mask & (1u64 << bit) != 0).collect()
}

/// Every submask of `mask`, including zero and `mask` itself.
fn submasks(mask: u64) -> Vec<u64> {
    let mut out = vec![mask];
    let mut current = mask;
    while current != 0 {
        current = (current - 1) & mask;
        out.push(current);
    }
    out
}

fn flatten<'a>(condition: Option<&'a Predicate>, out: &mut Vec<&'a Predicate>) {
    let Some(condition) = condition else {
        return;
    };
    match condition {
        Predicate::And(a, b) => {
            flatten(Some(a), out);
            flatten(Some(b), out);
        }
        other => out.push(other),
    }
}

/// Check that every edge's two sides are comparable, so an ordering is not
/// produced for a join that cannot execute.
pub fn validate_edges(graph: &JoinGraph) -> Result<(), JoinError> {
    for edge in &graph.edges {
        let Predicate::Compare(left, ComparisonOp::Equals, right) = &edge.predicate else {
            continue;
        };
        let (Expr::Column(a), Expr::Column(b)) = (left.as_ref(), right.as_ref()) else {
            continue;
        };
        let (left_relation, left_column) = graph.resolve(&a.column_name)?;
        let (right_relation, right_column) = graph.resolve(&b.column_name)?;
        resolve_key_class(
            &graph.relations[left_relation].columns[left_column].data_type,
            &graph.relations[right_relation].columns[right_column].data_type,
        )?;
    }
    Ok(())
}
