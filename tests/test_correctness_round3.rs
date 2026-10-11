//! Round-3 scratch verification tests for correctness issues found in a
//! third manual deep-dive (index scans, scalar functions, SELECT *
//! expansion, INSERT INTO ... SELECT roundtrip). Diagnostic probes like the
//! round-1/round-2 suites — each asserts the SQL-correct behaviour so a
//! failure pinpoints the defect.
//!
//! Findings under test:
//!   G. `SELECT *` over a table with an index uses `IndexScan(FullScan)`,
//!      which reads only B+tree entries. Rows whose indexed column is NULL
//!      are never indexed → they VANISH from every unfiltered scan.
//!   H. `POSITION(sub IN s)` returns a BYTE offset (str::find) while
//!      LENGTH counts characters → wrong result for multibyte text.
//!   I. `SELECT *` expands to name-based projections; with duplicate column
//!      names across a join, one of the duplicated columns is resolved to
//!      the wrong side.
//!   J. INSERT INTO ... SELECT roundtrips values through Display strings;
//!      a varchar containing a single quote corrupts the row (values shift
//!      / insert fails).

mod common;

use common::load_catalog;
use std::path::PathBuf;

use rook_ast::QueryPlan;

use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::backend::executor::physical::tuple::Tuple;
use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::executor::create_index::create_index;
use storage_manager::executor::load_csv::insert_single_tuple;
use storage_manager::planner::plan_query;
use storage_manager::types::datatype::DataType;
use storage_manager::types::value::DataValue;

/// A workspace for one test; see `common::TestWorkspace`.
struct TestWorkspace(#[allow(dead_code)] common::TestWorkspace);

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        Self(common::TestWorkspace::new("cr3", tag))
    }
}

fn col(name: &str, ty: DataType, nullable: bool) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty,
        nullable,
        constraints: Constraints::default(),
    }
}

fn run_select(
    catalog: &storage_manager::catalog::types::Catalog,
    db: &str,
    sql: &str,
) -> Vec<Tuple> {
    let select = match rook_parser::parse_sql(sql) {
        Ok(QueryPlan::Select(s)) => s,
        other => panic!("parse failed for {:?}: {:?}", sql, other.err()),
    };
    let logical = plan_query(&QueryPlan::Select(select), catalog, db).expect("plan failed");
    execute_plan_collect(&logical, catalog, db).expect("execution failed")
}

fn fmt_rows(tuples: &[Tuple]) -> Vec<Vec<String>> {
    tuples
        .iter()
        .map(|t| {
            t.values
                .iter()
                .map(|v| {
                    v.as_ref()
                        .map(|d| format!("{}", d))
                        .unwrap_or_else(|| "NULL".into())
                })
                .collect()
        })
        .collect()
}

// ── Finding G: IndexScan(FullScan) drops rows whose indexed column is NULL ──
//
// plan_table_scan replaces the SeqScan with IndexScanOperator::FullScan
// whenever ANY index exists. FullScan walks the B+tree leaf chain, and rows
// with NULL key components are deliberately not indexed
// (update_index_on_insert / vacuum rebuild both skip NULL keys). So a plain
// `SELECT *` silently loses every NULL-key row.

#[test]
fn fullscan_index_drops_null_key_rows() {
    let _ws = TestWorkspace::new("fullscan");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "testdb"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "testdb",
        "g_t",
        vec![
            col("id", DataType::Int, false),
            col("v", DataType::Int, true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "testdb", "g_t", &["1", "10"]).unwrap());
    assert!(insert_single_tuple(&catalog, "testdb", "g_t", &["2", "20"]).unwrap());
    // Row 3 has NULL in the indexed column → never enters the B+tree.
    assert!(insert_single_tuple(&catalog, "testdb", "g_t", &["3", "NULL"]).unwrap());

    create_index(&catalog, "testdb", "g_t", "idx_v", &["v".to_string()]).expect("create index");

    // Unfiltered scan must return ALL rows regardless of index presence.
    let tuples = run_select(&catalog, "testdb", "SELECT * FROM g_t");
    assert_eq!(
        tuples.len(),
        3,
        "BUG G CONFIRMED: SELECT * over an indexed table returned {} rows; the NULL-key row(s) vanished",
        tuples.len()
    );
    let rows = fmt_rows(&tuples);
    assert!(
        rows.iter().any(|r| r[0] == "3"),
        "row id=3 (v NULL) missing from unfiltered scan"
    );

    // Control: COUNT(*) must count every row too.
    let tuples = run_select(&catalog, "testdb", "SELECT COUNT(*) FROM g_t");
    assert_eq!(
        tuples[0].values[0],
        Some(DataValue::BigInt(3)),
        "COUNT(*) over indexed table must see all rows"
    );
}

/// Control for finding G: a table WITHOUT an index must return all rows —
/// isolates the defect to the FullScan path, not the heap or insert path.
#[test]
fn seqscan_no_index_returns_all_rows() {
    let _ws = TestWorkspace::new("noidx");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "testdb"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "testdb",
        "g_n",
        vec![
            col("id", DataType::Int, false),
            col("v", DataType::Int, true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "testdb", "g_n", &["1", "10"]).unwrap());
    assert!(insert_single_tuple(&catalog, "testdb", "g_n", &["2", "NULL"]).unwrap());

    let tuples = run_select(&catalog, "testdb", "SELECT * FROM g_n");
    assert_eq!(tuples.len(), 2, "unindexed SELECT * must return all rows");
}

// ── Finding H: POSITION returns byte offset, not character position ─────────

#[test]
fn position_returns_char_offset_for_multibyte() {
    let _ws = TestWorkspace::new("pos");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "testdb"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "testdb",
        "p_t",
        vec![
            col("id", DataType::Int, false),
            col("s", DataType::Varchar(40), true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    // 'héllo wörld': 'é' is 2 bytes in UTF-8, so byte-position of "wörld" is 8
    // but the SQL CHARACTER position is 7.
    assert!(insert_single_tuple(&catalog, "testdb", "p_t", &["1", "'héllo wörld'"]).unwrap());

    let tuples = run_select(&catalog, "testdb", "SELECT POSITION('wörld' IN s) FROM p_t");
    assert_eq!(
        tuples[0].values[0],
        Some(DataValue::Int(7)),
        "BUG H CONFIRMED: POSITION returned byte offset {} instead of char position 7",
        tuples[0].values[0]
            .as_ref()
            .map(|v| format!("{}", v))
            .unwrap_or_default()
    );
}

// ── Finding I: SELECT * over a join with duplicate column names ─────────────
//
// ProjectionOperator::star projects by column NAME. When both sides of a join
// expose the same column name (e.g. both have `id`), unqualified name
// resolution matches the FIRST occurrence, so one side's `id` column shows
// the other side's values.

#[test]
fn select_star_join_duplicate_column_names() {
    let _ws = TestWorkspace::new("stardup");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "testdb"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "testdb",
        "a",
        vec![
            col("id", DataType::Int, false),
            col("aval", DataType::Int, true),
        ],
    );
    create_table(
        &mut catalog,
        "testdb",
        "b",
        vec![
            col("id", DataType::Int, false),
            col("bval", DataType::Int, true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "testdb", "a", &["1", "100"]).unwrap());
    assert!(insert_single_tuple(&catalog, "testdb", "b", &["2", "100"]).unwrap());

    // SELECT * must show a.id = 1 (left) and b.id = 2 (right) in the two id
    // columns. Name-based star projection resolves BOTH `id` columns to the
    // first occurrence.
    let tuples = run_select(
        &catalog,
        "testdb",
        "SELECT * FROM a JOIN b ON a.aval = b.bval",
    );
    assert_eq!(tuples.len(), 1, "one matching join row");
    let row = fmt_rows(&tuples).remove(0);
    assert_eq!(
        row,
        vec![
            "1".to_string(),
            "100".to_string(),
            "2".to_string(),
            "100".to_string()
        ],
        "BUG I CONFIRMED: SELECT * over join with duplicate 'id' columns produced {:?}",
        row
    );
}

// ── Finding J: INSERT INTO ... SELECT corrupts quotes / shifts values ───────
//
// InsertOperator serialises child tuple values with DataValue's Display
// (Varchar → 'text') and feeds the strings to insert_single_tuple, which
// re-parses them. A stored single quote re-enters the SQL-ish value parser
// unescaped, so 'O'Brien' is mis-tokenised: the remainder of the row shifts
// across columns or the row is rejected.

#[test]
fn insert_select_roundtrip_quoted_string() {
    let _ws = TestWorkspace::new("insq");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "testdb"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "testdb",
        "src",
        vec![
            col("id", DataType::Int, false),
            col("name", DataType::Varchar(30), true),
        ],
    );
    create_table(
        &mut catalog,
        "testdb",
        "dst",
        vec![
            col("id", DataType::Int, false),
            col("name", DataType::Varchar(30), true),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "testdb", "src", &["1", "'O'Brien'"]).unwrap());

    // INSERT INTO dst SELECT * FROM src — goes through InsertOperator's
    // Display→reparse roundtrip.
    let insert = match rook_parser::parse_sql("INSERT INTO dst SELECT * FROM src") {
        Ok(p) => p,
        other => panic!("parse failed: {:?}", other.err()),
    };
    let logical = plan_query(&insert, &catalog, "testdb").expect("plan failed");
    execute_plan_collect(&logical, &catalog, "testdb").expect("execution failed");

    // Read back what actually landed in dst.
    use storage_manager::heap::HeapManager;
    use storage_manager::types::row::deserialize_nullable_row;
    let path: PathBuf = common::path("database/base/testdb/dst.dat");
    let heap = HeapManager::open(path).expect("open heap");
    let t = catalog.databases.get("testdb").unwrap().tables["dst"].clone();
    let schema: Vec<DataType> = t.columns.iter().map(|c| c.data_type.clone()).collect();

    let mut rows: Vec<Vec<Option<DataValue>>> = Vec::new();
    for result in heap.scan() {
        let Ok((_p, _s, raw)) = result else { continue };
        if let Ok(decoded) = deserialize_nullable_row(&schema, &raw) {
            rows.push(decoded);
        }
    }

    assert_eq!(rows.len(), 1, "exactly one row must be inserted into dst");
    assert_eq!(
        rows[0],
        vec![
            Some(DataValue::Int(1)),
            Some(DataValue::Varchar("O'Brien".to_string())),
        ],
        "BUG J CONFIRMED: INSERT INTO ... SELECT corrupted the quoted value into {:?}",
        rows[0]
    );
}
