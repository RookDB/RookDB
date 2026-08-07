//! Executing a chosen multi-relation join order.
//!
//! A conjunct is applied at the lowest node whose subtree mentions every
//! relation it names. Each node names its inputs' columns by position and
//! rewrites those conjuncts to match, which reuses the two-relation machinery
//! rather than growing a second copy.
//!
//! Only the right input of a node is materialised; the left spine streams.

use std::sync::Arc;

use crate::executor::selection::{ColumnReference, Expr, Predicate, TriValue};
use crate::types::value::DataValue;

use super::super::algorithm::{JoinAlgorithm, JoinRequest, JoinType, spec_for};
use super::super::config::JoinConfig;
use super::super::error::JoinError;
use super::super::memory::MemoryAccountant;
use super::super::order::{JoinGraph, OrderedPlan};
use super::super::plan::build_operator_with;
use super::super::predicate::{JoinPredicate, SideResolver, split_conjuncts};
use super::super::row::RowCodec;
use super::super::schema::{OutputSchema, RelationSchema};
use super::super::source::{BufferSource, RowSource, TableSource};
use super::super::spill::{RowBufferBuilder, SpillScope};
use super::{MatchEvaluator, RowStream};

/// One executed subtree: its rows, and the shape of them.
struct Subtree {
    source: Box<dyn RowSource>,
    schema: Arc<OutputSchema>,
    /// Relations in this subtree, in output column order.
    relations: Vec<usize>,
}

/// Run a chosen order.
///
/// Every node is an inner join: outer joins are reordering barriers, so a
/// reordered block contains none.
pub fn execute_ordered(
    graph: &JoinGraph,
    plan: &OrderedPlan,
    config: &JoinConfig,
) -> Result<Box<dyn RowStream>, JoinError> {
    let mut applied = vec![false; graph.conjuncts().len()];
    let subtree = build(graph, plan, config, &mut applied)?;
    let covered = mask_of(&subtree.relations);

    // A single relation has no join node to evaluate a condition at, and a
    // plan can leave a conjunct unapplied for the same reason.
    let mut leftover = Vec::new();
    for (index, conjunct) in graph.conjuncts().iter().enumerate() {
        if applied[index] {
            continue;
        }
        if conjunct.mask & !covered != 0 {
            return Err(JoinError::plan(format!(
                "a join condition mentions relations {:#b} that the plan never combines",
                conjunct.mask
            )));
        }
        let names = synthetic_names(&subtree, "l");
        leftover.push(rewrite(graph, &conjunct.predicate, &names, &[])?);
        applied[index] = true;
    }

    // The optimiser is free to join the relations in any order, but the
    // output must not depend on which order it chose - a caller asked for `a
    // JOIN b` and expects a's columns first.
    let mut stream = subtree.source.open()?;

    if let Some(condition) = combine(leftover) {
        let relation = RelationSchema::new("l", positional_columns(&subtree.schema, "l"));
        let empty = RelationSchema::new("r", Vec::new());
        let width = relation.len();
        let resolver = SideResolver::new(&relation, &empty)?;
        let split = split_conjuncts(Some(&condition), &resolver, JoinType::FullOuter)?;
        if let Some(residual) = split.residual {
            stream = Box::new(Filter {
                inner: stream,
                codec: RowCodec::for_schema(&subtree.schema),
                predicate: JoinPredicate::new(residual, width),
                scratch: Vec::new(),
            });
        }
    }

    let declared: Vec<usize> = (0..graph.relations().len()).collect();
    if subtree.relations == declared {
        return Ok(stream);
    }
    reorder(graph, stream, &subtree)
}

/// Applies a condition that no join node could evaluate.
struct Filter {
    inner: Box<dyn RowStream>,
    codec: RowCodec,
    predicate: JoinPredicate,
    scratch: Vec<Option<DataValue>>,
}

impl Iterator for Filter {
    type Item = Result<Vec<u8>, JoinError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let row = match self.inner.next()? {
                Ok(row) => row,
                Err(e) => return Some(Err(e)),
            };
            if let Err(e) = self.codec.decode_into(&row, &mut self.scratch) {
                return Some(Err(e));
            }
            match self.predicate.evaluate(&self.scratch, &[]) {
                // Only a definite match passes; UNKNOWN does not.
                Ok(TriValue::True) => return Some(Ok(row)),
                Ok(_) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl RowStream for Filter {
    fn schema(&self) -> &Arc<OutputSchema> {
        self.inner.schema()
    }

    fn stats(&self) -> super::ExecStats {
        self.inner.stats()
    }
}

/// Permute a subtree's columns back into the declared relation order.
fn reorder(
    graph: &JoinGraph,
    inner: Box<dyn RowStream>,
    subtree: &Subtree,
) -> Result<Box<dyn RowStream>, JoinError> {
    // Where each relation's columns begin in the subtree's output.
    let mut offset = 0usize;
    let mut starts = vec![0usize; graph.relations().len()];
    for relation in &subtree.relations {
        starts[*relation] = offset;
        offset += graph
            .relation(*relation)
            .map(|table| table.columns.len())
            .unwrap_or(0);
    }

    let mut mapping = Vec::with_capacity(offset);
    let mut columns = Vec::with_capacity(offset);
    for relation in 0..graph.relations().len() {
        let Some(table) = graph.relation(relation) else {
            continue;
        };
        for column in 0..table.columns.len() {
            mapping.push(starts[relation] + column);
            let source = subtree
                .schema
                .columns
                .get(starts[relation] + column)
                .ok_or_else(|| {
                    JoinError::plan(
                        "internal: a column is missing from the plan output".to_string(),
                    )
                })?;
            columns.push(source.clone());
        }
    }

    let schema = Arc::new(OutputSchema::from_output_columns(columns));
    Ok(Box::new(Reorder {
        input_codec: RowCodec::for_schema(&subtree.schema),
        output_codec: RowCodec::for_schema(&schema),
        schema,
        inner,
        mapping,
        scratch: Vec::new(),
        permuted: Vec::new(),
    }))
}

/// Rewrites each row into the declared column order.
struct Reorder {
    inner: Box<dyn RowStream>,
    input_codec: RowCodec,
    output_codec: RowCodec,
    schema: Arc<OutputSchema>,
    /// Output position -> input position.
    mapping: Vec<usize>,
    scratch: Vec<Option<DataValue>>,
    permuted: Vec<Option<DataValue>>,
}

impl Iterator for Reorder {
    type Item = Result<Vec<u8>, JoinError>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = match self.inner.next()? {
            Ok(row) => row,
            Err(e) => return Some(Err(e)),
        };

        if let Err(e) = self.input_codec.decode_into(&row, &mut self.scratch) {
            return Some(Err(e));
        }

        self.permuted.clear();
        for source in &self.mapping {
            self.permuted
                .push(self.scratch.get(*source).cloned().flatten());
        }

        Some(self.output_codec.encode(&self.permuted))
    }
}

impl RowStream for Reorder {
    fn schema(&self) -> &Arc<OutputSchema> {
        &self.schema
    }

    fn stats(&self) -> super::ExecStats {
        self.inner.stats()
    }
}

fn build(
    graph: &JoinGraph,
    node: &OrderedPlan,
    config: &JoinConfig,
    applied: &mut [bool],
) -> Result<Subtree, JoinError> {
    match node {
        OrderedPlan::Scan(relation) => scan(graph, *relation),
        OrderedPlan::Join { left, right, .. } => {
            let left = build(graph, left, config, applied)?;
            let right = build(graph, right, config, applied)?;
            join(graph, left, right, config, applied)
        }
    }
}

/// A leaf relation.
fn scan(graph: &JoinGraph, relation: usize) -> Result<Subtree, JoinError> {
    let table = graph
        .relation(relation)
        .ok_or_else(|| JoinError::plan(format!("no relation {relation}")))?;

    let schema = Arc::new(OutputSchema::left_only(&table.relation_schema()));
    let source = TableSource::new(table)?;

    Ok(Subtree {
        source: Box::new(source),
        schema,
        relations: vec![relation],
    })
}

/// Join two subtrees, applying every conjunct they now jointly satisfy.
fn join(
    graph: &JoinGraph,
    left: Subtree,
    right: Subtree,
    config: &JoinConfig,
    applied: &mut [bool],
) -> Result<Subtree, JoinError> {
    let covered = mask_of(&left.relations) | mask_of(&right.relations);

    // Where each leaf column ended up in each subtree's output.
    let left_names = synthetic_names(&left, "l");
    let right_names = synthetic_names(&right, "r");

    // Conjuncts whose relations are all present, and not yet applied.
    let mut conjuncts = Vec::new();
    for (index, conjunct) in graph.conjuncts().iter().enumerate() {
        if applied[index] || conjunct.mask & !covered != 0 {
            continue;
        }
        conjuncts.push(rewrite(
            graph,
            &conjunct.predicate,
            &left_names,
            &right_names,
        )?);
        applied[index] = true;
    }

    let condition = combine(conjuncts);

    // Two synthetic relations, named by position, so the two-relation
    // resolver can do the work.
    let left_relation = RelationSchema::new("l", positional_columns(&left.schema, "l"));
    let right_relation = RelationSchema::new("r", positional_columns(&right.schema, "r"));
    let resolver = SideResolver::new(&left_relation, &right_relation)?;
    // Split as if nothing may be pushed down: an intermediate result has no
    // scan to push a filter into, so every conjunct has to land in the keys or
    // the residual and be evaluated by the operator.
    let split = split_conjuncts(condition.as_ref(), &resolver, JoinType::FullOuter)?;

    let request = JoinRequest {
        join_type: JoinType::Inner,
        keys: &split.keys,
        has_residual: split.residual.is_some(),
        has_inner_index: false,
    };

    // Prefer a hash join when there is an equality; otherwise a nested loop is
    // the only thing that can evaluate the condition at all.
    let algorithm = if split.keys.is_empty() {
        JoinAlgorithm::BlockNestedLoop
    } else {
        JoinAlgorithm::Hash
    };
    let spec = spec_for(algorithm).validate(&request)?;

    let schema = Arc::new(OutputSchema::join_of(&left.schema, &right.schema));
    let residual = split
        .residual
        .clone()
        .map(|predicate| JoinPredicate::new(predicate, left_relation.len()));
    let evaluator = MatchEvaluator::new(split.keys.clone(), residual);

    let outer = left.source.open()?;
    let stream = build_operator_with(
        &spec,
        evaluator,
        outer,
        right.source,
        Arc::clone(&schema),
        None,
        config,
        1024,
        None,
    )?;

    // Materialise this node so it can serve as the inner side of the next one.
    // The left spine streams; only this is held.
    let source = materialise(stream, Arc::clone(&schema), config)?;

    let mut relations = left.relations;
    relations.extend(right.relations);

    Ok(Subtree {
        source,
        schema,
        relations,
    })
}

/// Collect a stream into a re-openable source, spilling if it outgrows the
/// budget.
fn materialise(
    mut stream: Box<dyn RowStream>,
    schema: Arc<OutputSchema>,
    config: &JoinConfig,
) -> Result<Box<dyn RowSource>, JoinError> {
    let scope = SpillScope::create(&config.spill_root)?;
    let budget = MemoryAccountant::new(config.work_memory_bytes);
    let mut builder = RowBufferBuilder::new(&scope, "intermediate", schema.fingerprint);

    while let Some(row) = stream.next() {
        builder.push(&row?, &budget)?;
    }

    let buffer = builder.finish(&budget)?;
    Ok(Box::new(BufferSource::new(buffer, schema)))
}

fn mask_of(relations: &[usize]) -> u64 {
    relations
        .iter()
        .fold(0u64, |mask, relation| mask | (1u64 << relation))
}

/// Map each leaf column of a subtree to the synthetic name it will be given.
fn synthetic_names(subtree: &Subtree, prefix: &str) -> Vec<(String, String)> {
    subtree
        .schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.qualified_name.clone(), format!("{prefix}{index}")))
        .collect()
}

fn positional_columns(schema: &OutputSchema, prefix: &str) -> Vec<crate::catalog::Column> {
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let mut synthetic =
                crate::catalog::Column::new(format!("{prefix}{index}"), column.data_type.clone());
            synthetic.nullable = column.nullable;
            synthetic
        })
        .collect()
}

/// Rewrite a conjunct's column references onto the synthetic names.
fn rewrite(
    graph: &JoinGraph,
    predicate: &Predicate,
    left: &[(String, String)],
    right: &[(String, String)],
) -> Result<Predicate, JoinError> {
    let lookup = |name: &str| -> Result<String, JoinError> {
        // Resolve through the graph so an unqualified name is handled the same
        // way it was when the graph was built.
        let (relation, column) = graph.resolve(name)?;
        let table = graph
            .relation(relation)
            .ok_or_else(|| JoinError::plan(format!("no relation {relation}")))?;
        let qualified = format!(
            "{}.{}",
            table.alias,
            table
                .columns
                .get(column)
                .map(|c| c.name.as_str())
                .unwrap_or_default()
        );

        left.iter()
            .chain(right.iter())
            .find(|(original, _)| *original == qualified)
            .map(|(_, synthetic)| synthetic.clone())
            .ok_or_else(|| {
                JoinError::schema(format!(
                    "'{qualified}' is not available at this point in the plan"
                ))
            })
    };

    rewrite_predicate(predicate, &lookup)
}

fn rewrite_predicate(
    predicate: &Predicate,
    lookup: &dyn Fn(&str) -> Result<String, JoinError>,
) -> Result<Predicate, JoinError> {
    Ok(match predicate {
        Predicate::Compare(a, op, b) => Predicate::Compare(
            Box::new(rewrite_expr(a, lookup)?),
            *op,
            Box::new(rewrite_expr(b, lookup)?),
        ),
        Predicate::IsNull(e) => Predicate::IsNull(Box::new(rewrite_expr(e, lookup)?)),
        Predicate::IsNotNull(e) => Predicate::IsNotNull(Box::new(rewrite_expr(e, lookup)?)),
        Predicate::Not(p) => Predicate::Not(Box::new(rewrite_predicate(p, lookup)?)),
        Predicate::Exists(p) => Predicate::Exists(Box::new(rewrite_predicate(p, lookup)?)),
        Predicate::And(a, b) => Predicate::And(
            Box::new(rewrite_predicate(a, lookup)?),
            Box::new(rewrite_predicate(b, lookup)?),
        ),
        Predicate::Or(a, b) => Predicate::Or(
            Box::new(rewrite_predicate(a, lookup)?),
            Box::new(rewrite_predicate(b, lookup)?),
        ),
        Predicate::Between(e, low, high) => Predicate::Between(
            Box::new(rewrite_expr(e, lookup)?),
            Box::new(rewrite_expr(low, lookup)?),
            Box::new(rewrite_expr(high, lookup)?),
        ),
        Predicate::In(e, list) => {
            let mut items = Vec::with_capacity(list.len());
            for item in list {
                items.push(rewrite_expr(item, lookup)?);
            }
            Predicate::In(Box::new(rewrite_expr(e, lookup)?), items)
        }
        Predicate::Like(e, pattern, compiled) => Predicate::Like(
            Box::new(rewrite_expr(e, lookup)?),
            pattern.clone(),
            compiled.clone(),
        ),
    })
}

fn rewrite_expr(
    expr: &Expr,
    lookup: &dyn Fn(&str) -> Result<String, JoinError>,
) -> Result<Expr, JoinError> {
    Ok(match expr {
        Expr::Column(reference) => {
            Expr::Column(ColumnReference::new(lookup(&reference.column_name)?))
        }
        Expr::Constant(constant) => Expr::Constant(constant.clone()),
        Expr::Add(a, b) => Expr::Add(
            Box::new(rewrite_expr(a, lookup)?),
            Box::new(rewrite_expr(b, lookup)?),
        ),
        Expr::Sub(a, b) => Expr::Sub(
            Box::new(rewrite_expr(a, lookup)?),
            Box::new(rewrite_expr(b, lookup)?),
        ),
        Expr::Mul(a, b) => Expr::Mul(
            Box::new(rewrite_expr(a, lookup)?),
            Box::new(rewrite_expr(b, lookup)?),
        ),
        Expr::Div(a, b) => Expr::Div(
            Box::new(rewrite_expr(a, lookup)?),
            Box::new(rewrite_expr(b, lookup)?),
        ),
    })
}

fn combine(mut parts: Vec<Predicate>) -> Option<Predicate> {
    let first = parts.pop()?;
    Some(parts.into_iter().fold(first, Predicate::and))
}
