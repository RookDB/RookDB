//! SQL sessions: execute one parsed statement at a time against a catalog.
//!
//! A [`Session`] holds the catalog and the current database. [`Session::execute`]
//! runs a [`QueryPlan`] and returns what a client should show: result rows for
//! queries, and the lines describing what the statement did. Nothing here
//! prints; front ends decide how to present a [`StatementResult`].
//!
//! Errors are [`RookError`]s whose `Display` is the full line to show the
//! user, e.g. `Update failed: ...`.

mod alter;
mod ddl;
pub(crate) use ddl::rebuild_indexes;
mod dml;
mod names;

use rook_ast::QueryPlan;

use crate::backend::error::{RookError, RookResult};
use crate::backend::executor::physical::explain::PlanDescription;
use crate::backend::executor::physical::tuple::{ColumnInfo, Tuple};
use std::sync::Arc;

use crate::catalog::Catalog;

/// What one statement produced.
#[derive(Debug, Default)]
pub struct StatementResult {
    /// Result rows of a query, with their schema.
    pub rows: Option<(Vec<ColumnInfo>, Vec<Tuple>)>,
    /// Lines describing what the statement did, warnings included, in the
    /// order they happened. Shown after `rows`.
    pub messages: Vec<String>,
    /// The physical plan, for `EXPLAIN`: the operator tree and the planner's
    /// decisions as data. `rows` holds the same plan as text.
    pub plan: Option<PlanDescription>,
}

impl StatementResult {
    fn message(line: impl Into<String>) -> Self {
        StatementResult {
            messages: vec![line.into()],
            ..Default::default()
        }
    }

    fn lines(messages: Vec<String>) -> Self {
        StatementResult {
            messages,
            ..Default::default()
        }
    }
}

/// Shown when a statement needs a database and none is selected.
pub const NO_DATABASE: &str = "No database selected. Use 'USE <database>' first.";

/// A connection's state: the catalog and the database `USE` selected.
///
/// Queries share the catalog through the `Arc`; DDL changes it in place
/// (`Arc::make_mut`), which copies only while a query still holds it.
pub struct Session {
    pub catalog: Arc<Catalog>,
    pub current_db: Option<String>,
}

/// Statements that may write the system tables.
fn changes_catalog(plan: &QueryPlan) -> bool {
    matches!(
        plan,
        QueryPlan::CreateDatabase(_)
            | QueryPlan::DropDatabase(_)
            | QueryPlan::CreateTable(_)
            | QueryPlan::CreateTableAsSelect(_)
            | QueryPlan::DropTable(_)
            | QueryPlan::AlterTable(_)
            | QueryPlan::CreateIndex(_)
            | QueryPlan::DropIndex(_)
            | QueryPlan::CreateView(_)
            | QueryPlan::DropView(_)
            | QueryPlan::Truncate(_)
    )
}

impl Session {
    pub fn new(catalog: Catalog) -> Self {
        Session {
            catalog: Arc::new(catalog),
            current_db: None,
        }
    }

    /// Execute one parsed statement.
    ///
    /// A statement that changes the catalog runs as one catalog change: if
    /// it fails, the system tables and the in-memory catalog go back to how
    /// they were before it.
    pub fn execute(&mut self, plan: QueryPlan) -> RookResult<StatementResult> {
        let plan = names::resolve_table_names(plan, &self.catalog, &self.current_db);
        if !changes_catalog(&plan) {
            return self.execute_resolved(&plan);
        }
        let engine = Arc::clone(self.catalog.engine());
        let change = crate::backend::system_table::begin_catalog_change(&engine)?;
        let result = self.execute_resolved(&plan);
        let result = match result {
            Ok(done) => change.commit().map(|()| done),
            Err(e) => {
                change.rollback();
                Err(e)
            }
        };
        if result.is_err() {
            // The in-memory catalog may hold part of the change.
            self.catalog = Arc::new(crate::catalog::try_load_catalog_in(&engine).map_err(|e| {
                e.with_context("Failed to reload the catalog after a failed statement")
            })?);
            if let Some(db) = &self.current_db
                && !self.catalog.databases.contains_key(db)
            {
                self.current_db = None;
            }
        }
        result
    }

    fn execute_resolved(&mut self, plan: &QueryPlan) -> RookResult<StatementResult> {
        match plan {
            QueryPlan::ShowDatabases => Ok(ddl::show_databases(&self.catalog)),
            QueryPlan::CreateDatabase(p) => {
                ddl::create_database(Arc::make_mut(&mut self.catalog), p)
            }
            QueryPlan::DropDatabase(p) => ddl::drop_database(self, p),
            QueryPlan::UseDatabase(name) => Ok(ddl::use_database(self, name)),
            QueryPlan::ShowTables => Ok(ddl::show_tables(&self.catalog, self.db()?)),
            QueryPlan::CreateTable(p) => {
                let db = self.db()?.to_string();
                ddl::create_table(Arc::make_mut(&mut self.catalog), &db, p)
            }
            QueryPlan::Insert(_) => dml::insert(&self.catalog, self.db()?, plan),
            QueryPlan::Select(_) | QueryPlan::SetOperation(_) => {
                dml::query(&self.catalog, self.db()?, plan)
            }
            QueryPlan::Update(p) => dml::update(&self.catalog, self.db()?, p),
            QueryPlan::Delete(p) => dml::delete(&self.catalog, self.db()?, p),
            QueryPlan::Explain(p) => dml::explain(&self.catalog, self.db()?, p),
            QueryPlan::Vacuum(p) => ddl::vacuum(&self.catalog, self.db()?, &p.table),
            QueryPlan::Analyze(p) => ddl::analyze(&self.catalog, self.db()?, p.table.as_deref()),
            QueryPlan::CreateIndex(p) => ddl::create_index(&self.catalog, self.db()?, p),
            QueryPlan::Unknown(msg) => Err(RookError::Internal(format!(
                "Unsupported SQL statement: {}",
                msg
            ))),
            // Statements whose failures the shell reports as `Error: ...`.
            _ => {
                let db = self.db()?.to_string();
                self.execute_ddl(&db, plan)
                    .map_err(|e| e.bare_io().with_context("Error"))
            }
        }
    }

    fn execute_ddl(&mut self, db: &str, plan: &QueryPlan) -> RookResult<StatementResult> {
        let catalog = Arc::make_mut(&mut self.catalog);
        match plan {
            QueryPlan::DropTable(p) => {
                ddl::drop_table(catalog, db, &p.table, p.if_exists, p.cascade)
            }
            QueryPlan::AlterTable(p) => alter::alter_table(catalog, db, p),
            QueryPlan::CreateView(p) => ddl::create_view(catalog, db, p),
            QueryPlan::DropView(p) => ddl::drop_view(catalog, db, &p.name, p.if_exists),
            QueryPlan::Truncate(p) => ddl::truncate(catalog, db, &p.table),
            QueryPlan::CreateTableAsSelect(p) => ddl::create_table_as_select(catalog, db, p),
            QueryPlan::DropIndex(p) => {
                ddl::drop_index(catalog, db, &p.index_name, &p.table_name, p.if_exists)
            }
            other => Err(RookError::Internal(format!(
                "Unsupported statement: {}",
                other.statement_type()
            ))),
        }
    }

    /// The selected database, or the error a statement that needs one gets.
    fn db(&self) -> RookResult<&str> {
        self.current_db
            .as_deref()
            .ok_or_else(|| RookError::Internal(NO_DATABASE.to_string()))
    }
}
