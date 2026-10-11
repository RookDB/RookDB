//! Scratch verification tests for correctness issues found in a manual
//! deep-dive of the codebase. These are NOT permanent tests — they probe
//! specific suspected defects to confirm/refute them.

use std::path::PathBuf;
use std::sync::Mutex;

use rook_parser::parse_set_clause;
use storage_manager::backend::executor::row_select::{parse_where_text, select_matching_pointers};
use storage_manager::catalog::types::{Catalog, Column, Constraints};
use storage_manager::catalog::{create_database, create_table, init_catalog, load_catalog};
use storage_manager::executor::load_csv::insert_single_tuple;
use storage_manager::types::datatype::DataType;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

struct TestWorkspace {
    prev_cwd: PathBuf,
    path: PathBuf,
}

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        let prev_cwd = std::env::current_dir().expect("read cwd");
        let path = prev_cwd.join(format!("database_ws_p{}_ddv_{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(path.join("base")).expect("create workspace");
        std::env::set_current_dir(&path).expect("chdir into workspace");
        storage_manager::backend::executor::row_select::register_where_parser(
            rook_parser::parse_where_text,
        );
        storage_manager::backend::cache::register_check_parser(rook_parser::parse_check_expr);
        Self { prev_cwd, path }
    }
}

impl Drop for TestWorkspace {
    fn drop(&mut self) {
        if std::env::set_current_dir(&self.prev_cwd).is_ok() {
            let _ = std::fs::remove_dir_all(&self.path);
        }
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

fn create_test_table(catalog: &mut Catalog, table: &str, cols: Vec<Column>) {
    create_table(catalog, "testdb", table, cols);
    assert!(
        catalog
            .databases
            .get("testdb")
            .unwrap()
            .tables
            .contains_key(table),
        "table '{}' was not created",
        table
    );
}

// ── Bug candidate 1: WHERE `col = NULL` / index scan on `<>` ─────────────────
// The index planner maps ConstantValue::Null → Int(0) in
// ast_constant_to_data_value, so `WHERE indexed_col = NULL` becomes a
// PointLookup(0) and can return row(s) with value 0 instead of zero rows.

#[test]
fn verify_index_scan_null_eq() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("idxnull");
    init_catalog();
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "testdb"), "create db");
    let mut catalog = load_catalog();

    create_test_table(
        &mut catalog,
        "t_null",
        vec![
            col("id", DataType::Int, false),
            col("v", DataType::Int, true),
        ],
    );

    // Insert a row with v = 0 and a row with v = NULL
    assert!(insert_single_tuple(&catalog, "testdb", "t_null", &["1", "0"]).unwrap());
    assert!(insert_single_tuple(&catalog, "testdb", "t_null", &["2", "NULL"]).unwrap());

    storage_manager::executor::create_index::create_index(
        &catalog,
        "testdb",
        "t_null",
        "idx_v",
        &["v".to_string()],
    )
    .expect("create index");

    // WHERE v = NULL must match zero rows (SQL semantics)
    let sel = parse_where_text("v = NULL").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "testdb", "t_null", sel).expect("select");
    assert_eq!(
        ptrs.len(),
        0,
        "BUG CONFIRMED: `v = NULL` matched {} rows (SQL says 0)",
        ptrs.len()
    );
}

// ── Bug candidate 2: CHAR padding through index scan ─────────────────────────

#[test]
fn verify_char_index_padding() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("charidx");
    init_catalog();
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "testdb"), "create db");
    let mut catalog = load_catalog();

    create_test_table(
        &mut catalog,
        "c_t",
        vec![
            col("id", DataType::Int, false),
            col("code", DataType::Char(8), true),
        ],
    );

    assert!(insert_single_tuple(&catalog, "testdb", "c_t", &["1", "'abc'"]).unwrap());
    assert!(insert_single_tuple(&catalog, "testdb", "c_t", &["2", "'xyz'"]).unwrap());

    storage_manager::executor::create_index::create_index(
        &catalog,
        "testdb",
        "c_t",
        "idx_code",
        &["code".to_string()],
    )
    .expect("create index");

    // SQL: CHAR comparison ignores trailing blanks → 'abc' = 'abc     ' is TRUE.
    let sel = parse_where_text("code = 'abc'").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "testdb", "c_t", sel).expect("select");
    assert_eq!(ptrs.len(), 1, "expected the one 'abc' row");
}

// ── Bug candidate 4a: UPDATE `text_col = text_col - n` mixes units:
// s.len() is BYTES but chars().take() counts CHARS → wrong result for
// multibyte text ('héllo' - 2 should drop 2 chars → "hél"; actual drops
// bytes: take(6-2=4 chars) → "héll").
//
// ── Bug candidate 4b (worse): decode_tuple maps SQL NULL → Text("NULL")
// and the Text arithmetic branch never checks for it, so arithmetic on a
// NULL text column corrupts NULL into the literal string "NUL"/"NULL1".

fn read_text_column(catalog: &Catalog, table: &str, column: &str) -> Vec<String> {
    use storage_manager::heap::HeapManager;
    use storage_manager::types::row::deserialize_nullable_row;

    let path: std::path::PathBuf = format!("database/base/testdb/{}.dat", table).into();
    let heap = HeapManager::open(path).expect("open heap");
    let t = catalog
        .databases
        .get("testdb")
        .unwrap()
        .tables
        .get(table)
        .unwrap();
    let schema: Vec<DataType> = t.columns.iter().map(|c| c.data_type.clone()).collect();
    let pos = t
        .columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(column))
        .unwrap();

    let mut out = Vec::new();
    for result in heap.scan() {
        let Ok((_page, _slot, raw)) = result else {
            continue;
        };
        let Ok(decoded) = deserialize_nullable_row(&schema, &raw) else {
            continue;
        };
        match decoded.get(pos) {
            Some(Some(storage_manager::types::value::DataValue::Varchar(s))) => out.push(s.clone()),
            Some(Some(storage_manager::types::value::DataValue::Char(s))) => out.push(s.clone()),
            Some(Some(v)) => out.push(format!("{:?}", v)),
            _ => out.push("NULL".to_string()),
        }
    }
    out
}

fn run_text_arith(tag: &str, rows: &[&str]) -> Result<Vec<String>, String> {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new(tag);
    init_catalog();
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "testdb"), "create db");
    let mut catalog = load_catalog();

    create_test_table(
        &mut catalog,
        "u_t",
        vec![
            col("id", DataType::Int, false),
            col("note", DataType::Varchar(50), true),
        ],
    );

    for (i, row) in rows.iter().enumerate() {
        assert!(
            insert_single_tuple(&catalog, "testdb", "u_t", &[&(i + 1).to_string(), row]).unwrap()
        );
    }

    let assignments = parse_set_clause("note = note - 2").expect("parse set");
    let sel = parse_where_text("id >= 1").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "testdb", "u_t", sel).expect("select");
    assert_eq!(ptrs.len(), rows.len());
    storage_manager::executor::update::update_by_pointers(
        &catalog,
        "testdb",
        "u_t",
        &ptrs,
        &assignments,
    )
    .map_err(|e| e.to_string())?;

    Ok(read_text_column(&catalog, "u_t", "note"))
}

/// Former BUG 4a: SET used its own arithmetic, where text minus an integer
/// dropped trailing characters (and miscounted multi-byte ones). SET now
/// uses the same evaluator as SELECT, which has no text-minus-integer
/// operator, so the statement fails instead.
#[test]
fn verify_update_text_minus_int_is_rejected() {
    let err = run_text_arith("updsub1", &["'héllo'"]).expect_err("text - int must fail");
    assert!(err.contains("Arithmetic not supported"), "got: {}", err);
}

/// Bug 4b candidate: NULL text column corrupted by arithmetic.
#[test]
fn verify_update_text_arith_on_null() {
    let values = run_text_arith("updsub2", &["NULL"]).expect("update");
    assert_eq!(
        values[0], "NULL",
        "BUG 4b: NULL text corrupted by arithmetic — got {:?}",
        values[0]
    );
}

// ── Bug candidate 4c/4d: the UPDATE decode→encode roundtrip corrupts data.
// decode_tuple maps BigInt/Double/Bool/Date/Timestamp to
// Text(format!("{:?}")) e.g. "BigInt(5)", "Date(2024-01-15)" —
// encode_tuple then fails to parse those strings and silently NULLs the
// column. Also, a varchar holding the literal text "NULL" decodes to
// Text("NULL") which encode_tuple treats as SQL NULL.

#[test]
fn verify_update_roundtrip_corruption() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("updrt");
    init_catalog();
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "testdb"), "create db");
    let mut catalog = load_catalog();

    create_test_table(
        &mut catalog,
        "r_t",
        vec![
            col("id", DataType::Int, false),
            col("big", DataType::BigInt, true),
            col("d", DataType::DoublePrecision, true),
            col("day", DataType::Date, true),
            col("tag", DataType::Varchar(20), true),
        ],
    );

    // Row 1: normal typed values; Row 2: varchar containing literal "NULL"
    assert!(
        insert_single_tuple(
            &catalog,
            "testdb",
            "r_t",
            &["1", "123456789012", "2.5", "'2024-01-15'", "'ok'"]
        )
        .unwrap()
    );
    assert!(
        insert_single_tuple(
            &catalog,
            "testdb",
            "r_t",
            &["2", "42", "1.0", "'2024-06-01'", "'NULL'"]
        )
        .unwrap()
    );

    // Touch every row with a no-op arithmetic UPDATE on the Int column
    // (using `id = id - 0`: NOTE that `tag = tag` is itself buggy — it is
    // parsed as the literal string "tag", see verify_update_self_reference.)
    let assignments = parse_set_clause("id = id - 0").expect("parse set");
    let sel = parse_where_text("id >= 1").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "testdb", "r_t", sel).expect("select");
    assert_eq!(ptrs.len(), 2);
    storage_manager::executor::update::update_by_pointers(
        &catalog,
        "testdb",
        "r_t",
        &ptrs,
        &assignments,
    )
    .expect("update");

    use storage_manager::heap::HeapManager;
    use storage_manager::types::row::deserialize_nullable_row;
    use storage_manager::types::value::DataValue;

    let path: std::path::PathBuf = "database/base/testdb/r_t.dat".into();
    let heap = HeapManager::open(path).expect("open heap");
    let t = &catalog.databases.get("testdb").unwrap().tables["r_t"];
    let schema: Vec<DataType> = t.columns.iter().map(|c| c.data_type.clone()).collect();

    let mut rows: Vec<Vec<Option<DataValue>>> = Vec::new();
    for result in heap.scan() {
        let Ok((_p, _s, raw)) = result else { continue };
        let Ok(decoded) = deserialize_nullable_row(&schema, &raw) else {
            continue;
        };
        rows.push(decoded);
    }
    rows.sort_by_key(|r| match &r[0] {
        Some(DataValue::Int(i)) => *i,
        _ => i32::MAX,
    });

    let mut corruptions: Vec<String> = Vec::new();

    // Row 1 must be untouched
    if rows[0][1] != Some(DataValue::BigInt(123456789012)) {
        corruptions.push(format!(
            "BUG 4d: BIGINT destroyed by UPDATE roundtrip — got {:?}",
            rows[0][1]
        ));
    }
    if !matches!(rows[0][2], Some(DataValue::DoublePrecision(_))) {
        corruptions.push(format!(
            "BUG 4d: DOUBLE destroyed by UPDATE roundtrip — got {:?}",
            rows[0][2]
        ));
    }
    if !matches!(rows[0][3], Some(DataValue::Date(_))) {
        corruptions.push(format!(
            "BUG 4d: DATE destroyed by UPDATE roundtrip — got {:?}",
            rows[0][3]
        ));
    }

    // Row 2's varchar containing the literal text "NULL" must survive
    if rows[1][4] != Some(DataValue::Varchar("NULL".to_string())) {
        corruptions.push(format!(
            "BUG 4c: literal text 'NULL' corrupted by UPDATE roundtrip — got {:?}",
            rows[1][4]
        ));
    }

    assert!(
        corruptions.is_empty(),
        "roundtrip corruption:\n{}",
        corruptions.join("\n")
    );
}

/// Bug 4e candidate: `SET col = col` parses the RHS column reference as a
/// string literal → the column gets overwritten with its own name as text.
#[test]
fn verify_update_self_reference() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("updself");
    init_catalog();
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "testdb"), "create db");
    let mut catalog = load_catalog();

    create_test_table(
        &mut catalog,
        "s_t",
        vec![
            col("id", DataType::Int, false),
            col("tag", DataType::Varchar(20), true),
        ],
    );
    assert!(insert_single_tuple(&catalog, "testdb", "s_t", &["1", "'hello'"]).unwrap());

    let assignments = parse_set_clause("tag = tag").expect("parse set");
    let sel = parse_where_text("id = 1").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "testdb", "s_t", sel).expect("select");
    storage_manager::executor::update::update_by_pointers(
        &catalog,
        "testdb",
        "s_t",
        &ptrs,
        &assignments,
    )
    .expect("update");

    let values = read_text_column(&catalog, "s_t", "tag");
    assert_eq!(
        values[0], "hello",
        "BUG 4e: `SET tag = tag` overwrote the column with a literal — got {:?}",
        values[0]
    );
}

// ── Bug candidate 12: duplicate keys straddling B+ tree splits ───────────────

#[test]
fn verify_duplicate_keys_straddle_free() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("dupkeys");
    init_catalog();
    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "testdb"), "create db");
    let mut catalog = load_catalog();

    create_test_table(
        &mut catalog,
        "d_t",
        vec![
            col("id", DataType::Int, false),
            col("v", DataType::Int, true),
        ],
    );

    // Insert many duplicate keys to force index splits
    for i in 0..500u32 {
        let v = (i % 5).to_string();
        let id = i.to_string();
        assert!(insert_single_tuple(&catalog, "testdb", "d_t", &[&id, &v]).unwrap());
    }

    // Build the index AFTER inserting duplicates (bulk build over
    // duplicate-heavy data, then a point lookup through it).
    storage_manager::executor::create_index::create_index(
        &catalog,
        "testdb",
        "d_t",
        "idx_v",
        &["v".to_string()],
    )
    .expect("create index");

    let sel = parse_where_text("v = 3").expect("parse where");
    let ptrs = select_matching_pointers(&catalog, "testdb", "d_t", sel).expect("select");
    assert_eq!(ptrs.len(), 100, "duplicate-heavy point lookup count");
}
