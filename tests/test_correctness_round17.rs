//! Round-17 verification tests for correctness issues found in a seventeenth
//! manual deep-dive (DML integrity: constraints on write paths). Each test
//! asserts the SQL-correct behaviour so a failure pinpoints the defect.
//!
//! Findings under test:
//!
//!   AH. A composite UNIQUE index crashes every later INSERT on the table.
//!       After `CREATE UNIQUE INDEX uq ON t(a, b)`, any subsequent INSERT
//!       panics with `key arity must match index arity`
//!       (btree/mod.rs:607). Root cause: the metadata loader explodes the
//!       composite unique index into per-column pairs
//!       (`for c in &cols { unique_indexes.push((name, c)) }`,
//!       cache.rs:336), and `check_unique_insert_meta`
//!       (constraint/validation.rs:94-103) opens the 2-column B+Tree but
//!       configures it with `set_key_type` (single type) and probes with a
//!       ONE-element key vector. The BTree's arity assertion fires and the
//!       process aborts. UNIQUE on a single column works (control).
//!
//!   AI. UPDATE bypasses the VARCHAR length limit and corrupts the row.
//!       `apply_assignments_typed` maps a text literal to
//!       `DataValue::Varchar(s.clone())` with no length check
//!       (executor/update.rs:158), while the INSERT path rejects
//!       over-length values. `UPDATE t SET name = 'toolong'` (limit 5)
//!       reports the row updated and stores 7 bytes; the row then fails to
//!       deserialize — `SELECT` errors with `VARCHAR payload length 7
//!       exceeds declared limit 5`, permanently unreadable data.
//!
//!   AJ. A batch UPDATE can leave the table violating UNIQUE.
//!       `update_by_pointers` validates each row with
//!       `validate_row_update(.., exclude = self)` against the OLD heap,
//!       never against the other rows updated in the same statement.
//!       `UPDATE t SET tag = 999 WHERE id >= 2` (two rows, both matching)
//!       reports 2 rows updated and stores tag=999 twice. Single-row
//!       collision with an untouched row IS correctly rejected (control:
//!       count 0, data unchanged).
//!
//!   AK. Multi-row INSERT is not atomic on constraint violation.
//!       `INSERT INTO t VALUES (2), (3), (1)` with UNIQUE(id) errors —
//!       but rows 2 and 3 remain inserted (the InsertOperator streams rows
//!       through insert_single_tuple one at a time; the violation on the
//!       last row aborts the statement without unwinding earlier rows).
//!       SQL statement atomicity requires the table to be unchanged after
//!       a failed multi-row INSERT.
//!
//! Controls (expected to PASS) document the working behaviour: single-column
//! UNIQUE enforcement on INSERT and UPDATE (including self-update and
//! delete-then-reinsert), VARCHAR length enforcement on INSERT, CHAR
//! blank-padded comparison, INSERT..SELECT string round-trip (quotes and
//! spaces), and single-row UPDATE collision rejection.

use std::path::PathBuf;
use std::sync::Mutex;

use rook_parser::parse_set_clause;
use storage_manager::backend::executor::create_index::create_index_with_flags;
use storage_manager::backend::executor::row_select::{parse_where_text, select_matching_pointers};
use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, load_catalog, save_catalog};
use storage_manager::executor::insert_single_tuple;
use storage_manager::executor::update::update_by_pointers;
use storage_manager::planner::plan_query;
use storage_manager::types::datatype::DataType;

static TEST_MUTEX: Mutex<()> = Mutex::new(());

struct TestWorkspace {
    prev_cwd: PathBuf,
    path: PathBuf,
}

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        let prev_cwd = std::env::current_dir().expect("read cwd");
        let path = prev_cwd.join(format!("database_ws_p{}_cr17_{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(path.join("base")).expect("create workspace");
        std::env::set_current_dir(&path).expect("chdir into workspace");
        storage_manager::backend::executor::row_select::register_where_parser(
            rook_parser::parse_where_text,
        );
        storage_manager::backend::cache::register_check_parser(rook_parser::parse_check_expr);
        storage_manager::catalog::init_catalog();
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

fn uniq_col(name: &str, ty: DataType) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty,
        nullable: true,
        constraints: Constraints {
            unique: true,
            ..Default::default()
        },
    }
}

fn try_select(
    catalog: &storage_manager::catalog::types::Catalog,
    db: &str,
    sql: &str,
) -> Result<Vec<Vec<String>>, String> {
    let plan = rook_parser::parse_sql(sql).map_err(|e| e.to_string())?;
    let logical = plan_query(&plan, catalog, db).map_err(|e| e.to_string())?;
    let tuples = storage_manager::backend::executor::physical::engine::execute_plan_collect(
        &logical, catalog, db,
    )
    .map_err(|e| e.to_string())?;
    Ok(tuples
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
        .collect())
}

fn do_update(db: &str, table: &str, pred: &str, set_clause: &str) -> Result<usize, String> {
    let catalog = load_catalog();
    let assignments = parse_set_clause(set_clause).expect("parse set clause");
    let sel = parse_where_text(pred).expect("parse where");
    let ptrs = select_matching_pointers(&catalog, db, table, sel).expect("select pointers");
    // A rejected UPDATE fails the statement; it updated no rows.
    update_by_pointers(&catalog, db, table, &ptrs, &assignments)
        .map(|u| u.updated_count)
        .or(Ok(0))
}

// ── Finding AH: composite UNIQUE index crashes INSERT ────────────────────────

#[test]
fn ah_insert_with_composite_unique_index_does_not_crash() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("ah_comp");

    let db = "db17ah";
    {
        let mut catalog = load_catalog();
        assert!(create_database(&mut catalog, db), "create db");
        let mut catalog = load_catalog();
        create_table(
            &mut catalog,
            db,
            "t",
            vec![
                col("a", DataType::Int, true),
                col("b", DataType::Int, true),
                col("v", DataType::Varchar(10), true),
            ],
        );
        save_catalog(&catalog).unwrap();
    }

    let catalog = load_catalog();
    assert!(
        insert_single_tuple(&catalog, db, "t", &["1", "2", "'x'"]).unwrap(),
        "seed row"
    );

    // Control: single-column UNIQUE index keeps working.
    let single = create_index_with_flags(&catalog, db, "t", "uq_v", &["v".into()], true, false);
    assert!(single.is_ok(), "control: single-col unique index creation");
    let ins_single = insert_single_tuple(&load_catalog(), db, "t", &["9", "9", "'unique9'"]);
    assert!(
        matches!(ins_single, Ok(true)),
        "control: INSERT after single-col unique index"
    );

    // The finding: composite UNIQUE index.
    let res = create_index_with_flags(
        &load_catalog(),
        db,
        "t",
        "uq_ab",
        &["a".into(), "b".into()],
        true,
        false,
    );
    assert!(
        res.is_ok(),
        "composite unique index creation must succeed: {:?}",
        res
    );

    // A tuple distinct in the composite key must be insertable — this used
    // to panic inside check_unique_insert_meta (key arity 1 vs index arity 2).
    let result = insert_single_tuple(&load_catalog(), db, "t", &["1", "3", "'y'"]);
    assert!(
        matches!(result, Ok(true)),
        "BUG AH CONFIRMED: INSERT on a table with a composite UNIQUE index \
         panicked (key arity must match index arity) or returned {:?} — \
         check_unique_insert_meta explodes the composite index into \
         per-column pairs and probes the 2-column BTree with a 1-element key",
        result
    );

    // And a true composite duplicate must be rejected.
    let dup = insert_single_tuple(&load_catalog(), db, "t", &["1", "3", "'dup'"]);
    assert!(
        matches!(dup, Ok(false)),
        "composite UNIQUE must reject a duplicate (a,b) tuple — got {:?}",
        dup
    );
}

// ── Finding AI: UPDATE bypasses VARCHAR length and corrupts the row ─────────

#[test]
fn ai_update_respects_varchar_length_limit() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("ai_vc");

    let db = "db17ai";
    {
        let mut catalog = load_catalog();
        assert!(create_database(&mut catalog, db), "create db");
        let mut catalog = load_catalog();
        create_table(
            &mut catalog,
            db,
            "t",
            vec![
                col("id", DataType::Int, true),
                uniq_col("name", DataType::Varchar(5)),
            ],
        );
        save_catalog(&catalog).unwrap();
    }

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, db, "t", &["1", "'abc'"]).unwrap());

    // Control: INSERT over-length is rejected.
    let ins = insert_single_tuple(&load_catalog(), db, "t", &["2", "'toolong'"]);
    assert!(
        matches!(ins, Ok(false)),
        "control: over-length INSERT must be rejected"
    );

    // The finding: UPDATE over-length is applied and corrupts the row.
    let upd = do_update(db, "t", "id = 1", "name = 'toolong'");
    assert_eq!(
        upd,
        Ok(0),
        "BUG AI CONFIRMED: over-length UPDATE reported {:?} updated rows — \
         apply_assignments_typed stores DataValue::Varchar without checking \
         the declared limit, producing a row that can no longer be decoded",
        upd
    );

    // The row must still be readable and unchanged.
    let scan = try_select(&load_catalog(), db, "SELECT name FROM t WHERE id = 1");
    assert_eq!(
        scan,
        Ok(vec![vec!["'abc'".to_string()]]),
        "the row must remain readable with its original value after the \
         rejected UPDATE — got {:?} (the historical failure mode was \
         deserialization error 'VARCHAR payload length 7 exceeds declared limit 5')",
        scan
    );
}

// ── Finding AJ: batch UPDATE can violate UNIQUE ──────────────────────────────

#[test]
fn aj_batch_update_cannot_create_unique_violation() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("aj_batch");

    let db = "db17aj";
    {
        let mut catalog = load_catalog();
        assert!(create_database(&mut catalog, db), "create db");
        let mut catalog = load_catalog();
        create_table(
            &mut catalog,
            db,
            "t",
            vec![
                col("id", DataType::Int, true),
                uniq_col("tag", DataType::Int),
            ],
        );
        save_catalog(&catalog).unwrap();
    }

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, db, "t", &["1", "100"]).unwrap());
    assert!(insert_single_tuple(&load_catalog(), db, "t", &["2", "200"]).unwrap());
    assert!(insert_single_tuple(&load_catalog(), db, "t", &["3", "300"]).unwrap());

    // Control: single-row UPDATE colliding with an untouched row is rejected.
    let single = do_update(db, "t", "id = 1", "tag = 200");
    assert_eq!(
        single,
        Ok(0),
        "control: colliding single-row UPDATE rejected"
    );
    let scan0 = try_select(&load_catalog(), db, "SELECT id, tag FROM t ORDER BY id");
    assert_eq!(
        scan0,
        Ok(vec![
            vec!["1".to_string(), "100".to_string()],
            vec!["2".to_string(), "200".to_string()],
            vec!["3".to_string(), "300".to_string()],
        ]),
        "control: data unchanged after rejected UPDATE"
    );

    // The finding: the SAME target value for BOTH matching rows of a batch
    // UPDATE passes per-row validation (each row is checked against the old
    // heap, not against its batch siblings) and lands tag=999 twice.
    let batch = do_update(db, "t", "id >= 2", "tag = 999");
    assert_eq!(
        batch,
        Ok(0),
        "BUG AJ CONFIRMED: batch UPDATE setting both rows to the same value \
         reported {:?} updated rows — validate_row_update checks each row \
         against the OLD heap (excluding only itself), so rows updated in \
         the same statement never see each other's new values and the table \
         ends up violating UNIQUE",
        batch
    );

    let scan = try_select(&load_catalog(), db, "SELECT id, tag FROM t ORDER BY id");
    assert_eq!(
        scan,
        Ok(vec![
            vec!["1".to_string(), "100".to_string()],
            vec!["2".to_string(), "200".to_string()],
            vec!["3".to_string(), "300".to_string()],
        ]),
        "table must be unchanged after the rejected batch UPDATE — got {:?}",
        scan
    );
}

// ── Finding AK: multi-row INSERT is not atomic on violation ──────────────────

#[test]
fn ak_multirow_insert_atomicity_on_unique_violation() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("ak_atom");

    let db = "db17ak";
    {
        let mut catalog = load_catalog();
        assert!(create_database(&mut catalog, db), "create db");
        let mut catalog = load_catalog();
        create_table(&mut catalog, db, "t", vec![uniq_col("id", DataType::Int)]);
        save_catalog(&catalog).unwrap();
    }

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, db, "t", &["1"]).unwrap());

    // Row 3 of the VALUES list violates UNIQUE(id). The statement must fail
    // WITHOUT leaving rows 2 and 3 (statement atomicity).
    let result = try_select(&load_catalog(), db, "INSERT INTO t VALUES (2), (3), (1)");
    assert!(
        result.is_err(),
        "the INSERT must error on the violating row"
    );

    let scan = try_select(&load_catalog(), db, "SELECT id FROM t ORDER BY id");
    assert_eq!(
        scan,
        Ok(vec![vec!["1".to_string()]]),
        "BUG AK CONFIRMED: after the failed multi-row INSERT the table \
         contains {:?} — rows preceding the violating row were already \
         streamed through insert_single_tuple and were not rolled back; \
         SQL statement atomicity requires the table to be unchanged",
        scan
    );
}

// ── Regression guards (expected to PASS) ─────────────────────────────────────

#[test]
fn x1_unique_lifecycle_and_char_padding_still_work() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("x1_guard");

    let db = "db17x1";
    {
        let mut catalog = load_catalog();
        assert!(create_database(&mut catalog, db), "create db");
        let mut catalog = load_catalog();
        create_table(
            &mut catalog,
            db,
            "t",
            vec![
                col("id", DataType::Int, true),
                uniq_col("tag", DataType::Int),
            ],
        );
        save_catalog(&catalog).unwrap();
    }

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, db, "t", &["1", "100"]).unwrap());
    assert!(insert_single_tuple(&load_catalog(), db, "t", &["2", "200"]).unwrap());

    // Duplicate INSERT rejected.
    assert!(
        matches!(
            insert_single_tuple(&load_catalog(), db, "t", &["3", "100"]),
            Ok(false)
        ),
        "control: duplicate unique INSERT rejected"
    );

    // Self-value UPDATE accepted.
    assert_eq!(
        do_update(db, "t", "id = 1", "tag = 100"),
        Ok(1),
        "control: setting a row's unique column to its own value succeeds"
    );

    // Delete then reinsert the same value: accepted.
    let sel = parse_where_text("id = 1").unwrap();
    let ptrs = select_matching_pointers(&load_catalog(), db, "t", sel).unwrap();
    assert_eq!(ptrs.len(), 1, "control: one live pointer");
    storage_manager::backend::executor::delete::delete_by_pointers(&load_catalog(), db, "t", &ptrs)
        .expect("delete");
    assert!(
        matches!(
            insert_single_tuple(&load_catalog(), db, "t", &["4", "100"]),
            Ok(true)
        ),
        "control: reinserting a value whose previous holder was deleted"
    );

    // CHAR blank-padding semantics on UNIQUE.
    let db2 = "db17x1c";
    {
        let mut catalog = load_catalog();
        assert!(create_database(&mut catalog, db2), "create db2");
        let mut catalog = load_catalog();
        create_table(
            &mut catalog,
            db2,
            "c",
            vec![
                col("id", DataType::Int, true),
                uniq_col("code", DataType::Char(4)),
            ],
        );
        save_catalog(&catalog).unwrap();
    }
    assert!(insert_single_tuple(&load_catalog(), db2, "c", &["1", "'ab'"]).unwrap());
    assert!(
        matches!(
            insert_single_tuple(&load_catalog(), db2, "c", &["2", "'ab  '"]),
            Ok(false)
        ),
        "control: CHAR padding — 'ab' and 'ab  ' are the same value"
    );
}

#[test]
fn x2_insert_select_string_roundtrip_still_work() {
    let _guard = TEST_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    let _ws = TestWorkspace::new("x2_roundtrip");

    let db = "db17x2";
    {
        let mut catalog = load_catalog();
        assert!(create_database(&mut catalog, db), "create db");
        let mut catalog = load_catalog();
        create_table(
            &mut catalog,
            db,
            "src",
            vec![
                col("id", DataType::Int, true),
                col("name", DataType::Varchar(30), true),
            ],
        );
        create_table(
            &mut catalog,
            db,
            "dst",
            vec![
                col("id", DataType::Int, true),
                col("name", DataType::Varchar(30), true),
            ],
        );
        save_catalog(&catalog).unwrap();
    }

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, db, "src", &["1", "'O''Brien'"]).unwrap());
    assert!(insert_single_tuple(&load_catalog(), db, "src", &["2", "'  spaced  '"]).unwrap());
    assert!(insert_single_tuple(&load_catalog(), db, "src", &["3", "'plain'"]).unwrap());

    let ins = try_select(
        &load_catalog(),
        db,
        "INSERT INTO dst SELECT id, name FROM src ORDER BY id",
    );
    assert!(ins.is_ok(), "INSERT..SELECT works: {:?}", ins);

    let src = try_select(&load_catalog(), db, "SELECT id, name FROM src ORDER BY id").unwrap();
    let dst = try_select(&load_catalog(), db, "SELECT id, name FROM dst ORDER BY id").unwrap();
    assert_eq!(
        src, dst,
        "INSERT..SELECT must preserve string content exactly"
    );
    assert_eq!(
        dst[0][1],
        "'O''Brien'".to_string(),
        "embedded quote preserved"
    );
    assert_eq!(
        dst[1][1],
        "'  spaced  '".to_string(),
        "leading/trailing spaces preserved"
    );
}
