//! Re-openable row sources.
//!
//! Nested-loop joins rescan their inner input once per outer block, so an
//! input has to be more than a one-shot iterator. A [`RowSource`] can be
//! opened repeatedly; each [`RowSource::open`] yields a fresh, independent
//! stream.
//!
//! A source is described by a [`TableRef`] - a path, an alias, and a column
//! list. The join subsystem never reads the catalog itself; the CLI resolves a
//! catalog entry into a `TableRef` and hands it over. That is what keeps joins
//! testable against scratch files with no global state.

use std::path::PathBuf;
use std::sync::Arc;

use crate::catalog::Column;
use crate::executor::selection::{SelectionExecutor, TriValue};
use crate::heap::HeapManager;
use crate::heap::heap_manager::HeapScanIterator;

use super::error::JoinError;
use super::exec::{ExecStats, RowStream, StatsHandle, new_stats};
use super::schema::{OutputSchema, RelationSchema};

/// Everything the join subsystem needs to read one relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    /// Name this relation is qualified by in the join condition.
    pub alias: String,
    /// Heap file backing the relation.
    pub path: PathBuf,
    pub columns: Vec<Column>,
}

impl TableRef {
    pub fn new(alias: impl Into<String>, path: PathBuf, columns: Vec<Column>) -> Self {
        Self {
            alias: alias.into(),
            path,
            columns,
        }
    }

    pub fn relation_schema(&self) -> RelationSchema {
        RelationSchema::new(self.alias.clone(), self.columns.clone())
    }
}

/// An input that can be scanned more than once.
pub trait RowSource {
    fn schema(&self) -> &Arc<OutputSchema>;
    fn open(&self) -> Result<Box<dyn RowStream>, JoinError>;
    /// Rows produced by the most recent complete scan, when known. Used by the
    /// planner only as a hint; correctness never depends on it.
    fn estimated_rows(&self) -> Option<u64>;
}

/// A base-table scan, optionally with a filter pushed into it.
pub struct TableSource {
    manager: HeapManager,
    schema: Arc<OutputSchema>,
    filter: Option<Arc<SelectionExecutor>>,
}

impl TableSource {
    /// Opens the heap file once; every [`RowSource::open`] then produces an
    /// independent scan over it without reopening.
    pub fn new(table: &TableRef) -> Result<Self, JoinError> {
        Self::with_filter(table, None)
    }

    pub fn with_filter(
        table: &TableRef,
        filter: Option<SelectionExecutor>,
    ) -> Result<Self, JoinError> {
        let manager = HeapManager::open(table.path.clone()).map_err(|e| {
            JoinError::Io(format!(
                "cannot open relation '{}' at {}: {e}",
                table.alias,
                table.path.display()
            ))
        })?;

        Ok(Self {
            manager,
            schema: Arc::new(OutputSchema::left_only(&table.relation_schema())),
            filter: filter.map(Arc::new),
        })
    }
}

impl RowSource for TableSource {
    fn schema(&self) -> &Arc<OutputSchema> {
        &self.schema
    }

    fn open(&self) -> Result<Box<dyn RowStream>, JoinError> {
        Ok(Box::new(TableScan {
            inner: self.manager.scan(),
            schema: Arc::clone(&self.schema),
            filter: self.filter.clone(),
            stats: new_stats(),
        }))
    }

    fn estimated_rows(&self) -> Option<u64> {
        // Maintained by the heap on insert and delete, so it is exact and
        // costs nothing to read - unlike counting slots, which over-counts
        // rows that have been deleted but not yet compacted away.
        Some(self.manager.header.total_tuples)
    }
}

struct TableScan {
    inner: HeapScanIterator,
    schema: Arc<OutputSchema>,
    filter: Option<Arc<SelectionExecutor>>,
    stats: StatsHandle,
}

impl Iterator for TableScan {
    type Item = Result<Vec<u8>, JoinError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let row = match self.inner.next()? {
                Ok((_page, _slot, data)) => data,
                Err(e) => return Some(Err(JoinError::Io(e.to_string()))),
            };

            if let Some(filter) = &self.filter {
                match filter.evaluate_tuple(&row) {
                    // A filter keeps only definite matches: UNKNOWN is not a
                    // match, matching WHERE semantics everywhere else.
                    Ok(TriValue::True) => {}
                    Ok(_) => continue,
                    Err(e) => return Some(Err(JoinError::schema(e))),
                }
            }

            self.stats.borrow_mut().rows_out += 1;
            return Some(Ok(row));
        }
    }
}

impl RowStream for TableScan {
    fn schema(&self) -> &Arc<OutputSchema> {
        &self.schema
    }

    fn stats(&self) -> ExecStats {
        self.stats.borrow().clone()
    }
}
