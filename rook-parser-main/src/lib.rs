use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

mod models;
mod utils;

use crate::utils::build_query_summary;

pub fn parse_sql(sql: &str) -> Result<String, String> {
    let dialect = GenericDialect {};

    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| e.to_string())?;

    let statement = statements
        .first()
        .ok_or("No SQL statement found")?;

    let summary = build_query_summary(statement);

    serde_json::to_string_pretty(&summary)
        .map_err(|e| e.to_string())
}