use serde::Serialize;

#[derive(Debug, Serialize)]
pub enum Category {
    DDL,
    DML,
    DQL,
    UNKNOWN,
}

#[derive(Debug, Serialize)]
pub enum StatementType {
    Select,
    Insert,
    CreateTable,
    CreateDatabase,
    ShowTables,
    ShowDatabases,
    USEDatabase,
    Unknown,
}

#[derive(Debug, Serialize)]
pub struct QuerySummary {
    pub category: Category,

    #[serde(rename = "type")]
    pub stmt_type: StatementType,

    pub params: Params,
}

/* ---------------- PARAM STRUCTS ---------------- */

#[derive(Debug, Serialize)]
pub struct SelectParams {
    pub tables: Vec<String>,
    pub columns: Vec<String>,
    pub joins: Vec<String>,
    pub filters: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct InsertParams {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<String>>,
    pub row_count: usize,
}

#[derive(Debug, Serialize)]
pub struct CreateDatabaseParams {
    pub database: String,
    pub if_not_exists: bool,
}

#[derive(Debug, Serialize)]
pub struct ColumnParam {
    pub name: String,
    pub data_type: String,
    pub constraints: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct TableConstraintParam {
    pub definition: String,
}

#[derive(Debug, Serialize)]
pub struct CreateTableParams {
    pub table: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnParam>,
    pub constraints: Vec<TableConstraintParam>,
}

#[derive(Debug, Serialize)]
pub struct ShowTablesParams;

#[derive(Debug, Serialize)]
pub struct ShowDatabasesParams;

#[derive(Debug, Serialize)]
pub struct UseDatabaseParams {
    pub database: String,
}


#[derive(Debug, Serialize)]
pub struct UnknownParams;

/* ---------------- PARAM ENUM ---------------- */

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Params {
    Select(SelectParams),
    Insert(InsertParams),
    CreateTable(CreateTableParams),
    CreateDatabase(CreateDatabaseParams),
    ShowTables(ShowTablesParams),
    ShowDatabases(ShowDatabasesParams),
    UseDatabase(UseDatabaseParams),
    Unknown(UnknownParams),
}