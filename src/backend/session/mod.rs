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
}

impl StatementResult {
    fn message(line: impl Into<String>) -> Self {
        StatementResult {
            rows: None,
            messages: vec![line.into()],
        }
    }

    fn lines(messages: Vec<String>) -> Self {
        StatementResult {
            rows: None,
            messages,
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

impl Session {
    pub fn new(catalog: Catalog) -> Self {
        Session {
            catalog: Arc::new(catalog),
            current_db: None,
        }
    }

    /// Execute one parsed statement.
    pub fn execute(&mut self, plan: QueryPlan) -> RookResult<StatementResult> {
        let plan = names::resolve_table_names(plan, &self.catalog, &self.current_db);
        match &plan {
            QueryPlan::ShowDatabases => Ok(ddl::show_databases(&self.catalog)),
            QueryPlan::CreateDatabase(p) => {
                Ok(ddl::create_database(Arc::make_mut(&mut self.catalog), p))
            }
            QueryPlan::DropDatabase(p) => ddl::drop_database(self, p),
            QueryPlan::UseDatabase(name) => Ok(ddl::use_database(self, name)),
            QueryPlan::ShowTables => Ok(ddl::show_tables(&self.catalog, self.db()?)),
            QueryPlan::CreateTable(p) => {
                let db = self.db()?.to_string();
                ddl::create_table(Arc::make_mut(&mut self.catalog), &db, p)
            }
            QueryPlan::Insert(_) => dml::insert(&self.catalog, self.db()?, &plan),
            QueryPlan::Select(_) | QueryPlan::SetOperation(_) => {
                dml::query(&self.catalog, self.db()?, &plan)
            }
            QueryPlan::Update(p) => dml::update(&self.catalog, self.db()?, p),
            QueryPlan::Delete(p) => dml::delete(&self.catalog, self.db()?, p),
            QueryPlan::Vacuum(p) => ddl::vacuum(&self.catalog, self.db()?, &p.table),
            QueryPlan::CreateIndex(p) => ddl::create_index(&self.catalog, self.db()?, p),
            QueryPlan::Unknown(msg) => Err(RookError::Internal(format!(
                "Unsupported SQL statement: {}",
                msg
            ))),
            // Statements whose failures the shell reports as `Error: ...`.
            _ => {
                let db = self.db()?.to_string();
                self.execute_ddl(&db, &plan)
                    .map_err(|e| RookError::Internal(e.to_string()).with_context("Error"))
            }
        }
    }

    fn execute_ddl(&mut self, db: &str, plan: &QueryPlan) -> std::io::Result<StatementResult> {
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
            other => Err(std::io::Error::other(format!(
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
