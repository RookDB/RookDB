//! Round-13 scratch verification tests for correctness issues found in a
//! thirteenth manual deep-dive. Diagnostic probes like the round-1..12
//! suites — each asserts the SQL-correct behaviour so a failure pinpoints
//! the defect.
//!
//! Findings under test:
//!
//!   T. Cross-scale NUMERIC comparison PANICS (engine crash, not an error).
//!      `Comparable::compare` for `Numeric(a) vs Numeric(b)` with
//!      `a.scale > b.scale` computes `b.unscaled * 10^(a.scale-b.scale)`
//!      with plain `*` (comparison.rs:242, and the mirrored branch at
//!      :246). With the range-scan sentinels `min_for_type`/`max_for_type`
//!      produce for NUMERIC — `Numeric{±i128::MAX, scale: 0}` — the
//!      multiply overflows i128 and panics ("attempt to multiply with
//!      overflow"). Reachable end-to-end: `WHERE n > 100` on an INDEXED
//!      NUMERIC(p, s>0) column crashes the whole query because
//!      `extract_index_mode_from_predicate` builds
//!      RangeLookup(min_for_type, ...) sentinels with scale 0. Debug
//!      builds panic; the comparison should use checked math and return a
//!      clean error or saturate.
//!
//!   U. Index-scan probe keys are not coerced to NUMERIC.
//!      `ast_constant_to_data_value` maps float literals to
//!      DoublePrecision and int literals to Int, and
//!      `coerce_to_key_type` (index_scan.rs:465) has no NUMERIC branch —
//!      its `_ => Some(dv)` passes mistyped probe values through. This
//!      breaks EVERY equality lookup on an indexed NUMERIC column with a
//!      float or int literal (multi-leaf trees).
//!      `BTree::encode_key_for` then encodes the probe with the probe's
//!      own `to_bytes()` (f64 LE / i32 LE), and multi-leaf tree
//!      navigation (`find_leaf`/`cmp_encoded_keys`) decodes those bytes
//!      with the declared key type NUMERIC → BCD decode errors:
//!        WHERE n = 1250.50 → "NUMERIC payload has invalid sign nibble"
//!        WHERE n = 1250    → "NUMERIC(10, 2) requires 6 bytes"
//!      The whole query fails, while the identical predicates on the
//!      UNINDEXED column work (comparison.rs has Numeric↔Double and
//!      Numeric↔Int promotions). Single-leaf trees work by accident:
//!      navigation never decodes the probe, and the leaf scan compares
//!      decoded stored keys against the raw probe value via Comparable.
//!
//!   U2. The U gap is scale-independent: even NUMERIC(10, 0) indexed
//!       lookups with an INT literal fail ('NUMERIC(10, 0) requires 6
//!       bytes') because the probe encodes as a 4-byte i32 blob while the
//!       tree stores (p+1).div_ceil(2)-byte BCD keys.
//!
//!   V. String literals are never coerced to temporal types in
//!      predicates. `WHERE d = '2024-01-05'` on a DATE column — indexed
//!      or not — fails with "Cannot compare DATE with VARCHAR"
//!      (same for TIMESTAMP). `constant_from_ast` (expr/convert.rs) maps
//!      all text to Varchar and `Comparable` has no Varchar↔Date
//!      promotion, while the INSERT path accepts the identical literal
//!      (`parse_and_encode` strips quotes and parses DATE/TIMESTAMP
//!      strings). Mainstream SQL (PostgreSQL/MySQL) implicitly casts the
//!      literal; the engine's own write path already does.
//!
//! Controls (expected to PASS) document the working unindexed /
//! same-type / single-leaf behaviour for each scenario so the
//! divergences cannot be explained away as engine limits.

mod common;

use common::load_catalog;

use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::executor::create_index::create_index;
use storage_manager::executor::insert_single_tuple;
use storage_manager::planner::plan_query;
use storage_manager::types::datatype::DataType;
use storage_manager::types::value::{DataValue, NumericValue};

/// A workspace for one test; see `common::TestWorkspace`.
struct TestWorkspace(#[allow(dead_code)] common::TestWorkspace);

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        Self(common::TestWorkspace::new("cr13", tag))
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

fn make_table(db: &str, table: &str, columns: Vec<Column>) {
    let mut catalog = load_catalog();
    create_table(&mut catalog, db, table, columns);
    save_catalog(&catalog).unwrap();
}

/// Bulk-insert rows reusing one catalog handle (keeps the multi-leaf
/// scenario fast).
fn bulk_insert(db: &str, table: &str, rows: &[String]) {
    let catalog = load_catalog();
    for row in rows {
        let vals: Vec<&str> = vec![row.as_str()];
        assert!(
            insert_single_tuple(&catalog, db, table, &vals).unwrap(),
            "insert of {:?} failed",
            row
        );
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

// ── Finding T: cross-scale NUMERIC comparison panics ─────────────────────────

#[test]
fn t1_cross_scale_numeric_compare_must_not_panic() {
    // Minimal reproducer: the exact sentinel/value pair the index-scan
    // range path produces for NUMERIC(p, s>0).
    let sentinel_max = DataValue::Numeric(NumericValue {
        unscaled: i128::MAX,
        scale: 0,
    });
    let sentinel_min = DataValue::Numeric(NumericValue {
        unscaled: i128::MIN,
        scale: 0,
    });
    let stored = DataValue::Numeric(NumericValue {
        unscaled: 10050,
        scale: 2,
    });

    use storage_manager::types::Comparable;
    let r = std::panic::catch_unwind(|| stored.compare(&sentinel_max));
    assert!(
        r.is_ok(),
        "BUG T CONFIRMED: comparing Numeric(10050, scale 2) with the \
         max_for_type sentinel Numeric(i128::MAX, scale 0) PANICS with \
         'attempt to multiply with overflow' — comparison.rs:242 computes \
         b.unscaled * 10^(a.scale-b.scale) with plain `*`; it must use \
         checked math and produce a clean ordering or error"
    );
    let _ = r.unwrap();

    let r2 = std::panic::catch_unwind(|| {
        use storage_manager::types::Comparable;
        let _ = stored.compare(&sentinel_min);
    });
    assert!(
        r2.is_ok(),
        "BUG T (min sentinel variant): Numeric(10050, scale 2) vs \
         Numeric(i128::MIN, scale 0) also overflows the scale-up multiply"
    );
}

#[test]
fn t2_range_predicate_on_indexed_numeric_must_not_crash() {
    // End-to-end: the index planner builds RangeLookup sentinels from
    // min_for_type/max_for_type (scale 0 extremes) for NUMERIC columns;
    // the B+ Tree then compares them against stored scale-2 keys.
    let _ws = TestWorkspace::new("t2_range_num");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db13t2"), "create db");
    make_table(
        "db13t2",
        "acc",
        vec![col(
            "n",
            DataType::Numeric {
                precision: 10,
                scale: 2,
            },
            true,
        )],
    );
    bulk_insert(
        "db13t2",
        "acc",
        &["100.50".to_string(), "200.00".to_string()],
    );
    let catalog = load_catalog();
    create_index(&catalog, "db13t2", "acc", "ix_n", &["n".to_string()]).expect("create index");
    let catalog = load_catalog();

    // Control: the same range over the UNINDEXED column executes.
    let unindexed = try_select(&catalog, "db13t2", "SELECT COUNT(*) FROM acc WHERE n > 100")
        .expect("control: unindexed range executes");

    // The indexed range must not panic the engine.
    let indexed = std::panic::catch_unwind(|| {
        let catalog = load_catalog();
        try_select(&catalog, "db13t2", "SELECT COUNT(*) FROM acc WHERE n > 100")
    });

    match indexed {
        Ok(Ok(rows)) => assert_eq!(rows, unindexed, "indexed and unindexed range must agree"),
        Ok(Err(e)) => panic!(
            "BUG T CONFIRMED (graceful-error variant): indexed range predicate \
             returned a hard error instead of executing like the unindexed \
             control: {}",
            e
        ),
        Err(_) => panic!(
            "BUG T CONFIRMED: `WHERE n > 100` on an indexed NUMERIC(10,2) \
             column PANICS — the RangeLookup sentinels (Numeric{{±i128::MAX, \
             scale 0}}) overflow the cross-scale multiply in \
             Comparable::compare and crash the engine"
        ),
    }
}

// ── Finding U: indexed NUMERIC probe keys not coerced ────────────────────────

#[test]
fn u_indexed_numeric_point_lookup_with_numeric_literals() {
    let _ws = TestWorkspace::new("u_idx_num");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db13u"), "create db");
    make_table(
        "db13u",
        "acc",
        vec![col(
            "n",
            DataType::Numeric {
                precision: 10,
                scale: 2,
            },
            true,
        )],
    );

    // Enough rows to span multiple B+ Tree leaves — single-leaf trees never
    // decode the probe key during navigation, masking the bug.
    let rows: Vec<String> = (0..600).map(|i| format!("{}.50", 1000 + i)).collect();
    bulk_insert("db13u", "acc", &rows);

    let catalog = load_catalog();

    // Control: unindexed point lookups with BOTH literal forms work.
    let unindexed_float = try_select(
        &catalog,
        "db13u",
        "SELECT COUNT(*) FROM acc WHERE n = 1250.50",
    )
    .expect("control: unindexed float-literal lookup");
    assert_eq!(
        unindexed_float,
        vec![vec!["1".to_string()]],
        "control: unindexed float literal matches"
    );
    let unindexed_int = try_select(&catalog, "db13u", "SELECT COUNT(*) FROM acc WHERE n = 1250")
        .expect("control: unindexed int-literal lookup");
    assert_eq!(
        unindexed_int,
        vec![vec!["0".to_string()]],
        "control: unindexed int literal (1250) matches no 1250.50 row"
    );

    // Build the index and repeat the same lookups.
    create_index(&catalog, "db13u", "acc", "ix_n", &["n".to_string()]).expect("create index");
    let catalog = load_catalog();

    let float_hit = try_select(
        &catalog,
        "db13u",
        "SELECT COUNT(*) FROM acc WHERE n = 1250.50",
    );
    assert_eq!(
        float_hit,
        Ok(vec![vec!["1".to_string()]]),
        "BUG U CONFIRMED: indexed point lookup `WHERE n = 1250.50` returned {:?} — \
         coerce_to_key_type has no NUMERIC branch, the DoublePrecision probe is \
         encoded with f64 to_bytes() and multi-leaf navigation fails to decode \
         it as NUMERIC BCD ('invalid sign nibble'); the unindexed control works",
        float_hit
    );

    let int_probe = try_select(&catalog, "db13u", "SELECT COUNT(*) FROM acc WHERE n = 1250");
    assert!(
        int_probe.is_ok(),
        "BUG U CONFIRMED: indexed point lookup with an INT literal returned {:?} — \
         the Int probe's 4-byte to_bytes() blob is decoded as NUMERIC(10,2) BCD \
         ('requires 6 bytes') during tree navigation",
        int_probe
    );
}

// ── Finding V: string literals never coerced to temporal types ───────────────

#[test]
fn v_string_literal_vs_date_column() {
    let _ws = TestWorkspace::new("v_date_literal");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db13v"), "create db");
    make_table("db13v", "ev", vec![col("d", DataType::Date, true)]);

    // The engine's WRITE path accepts the string literal for a DATE column...
    bulk_insert(
        "db13v",
        "ev",
        &["2024-01-05".to_string(), "2024-02-10".to_string()],
    );
    let catalog = load_catalog();

    // ...but the READ path rejects the identical literal in a predicate.
    let eq = try_select(
        &catalog,
        "db13v",
        "SELECT COUNT(*) FROM ev WHERE d = '2024-01-05'",
    );
    assert_eq!(
        eq,
        Ok(vec![vec!["1".to_string()]]),
        "BUG V CONFIRMED: `WHERE d = '2024-01-05'` on a DATE column returned {:?} — \
         constant_from_ast maps the literal to Varchar and Comparable has no \
         Varchar↔Date promotion, so every date-literal predicate fails with \
         'Cannot compare DATE with VARCHAR' although the INSERT path accepts \
         the same literal",
        eq
    );

    let gt = try_select(
        &catalog,
        "db13v",
        "SELECT COUNT(*) FROM ev WHERE d > '2024-01-31'",
    );
    assert_eq!(
        gt,
        Ok(vec![vec!["1".to_string()]]),
        "BUG V CONFIRMED: `WHERE d > '2024-01-31'` on a DATE column returned {:?} — \
         same missing string→temporal coercion",
        gt
    );
}

#[test]
fn v2_string_literal_vs_timestamp_column() {
    let _ws = TestWorkspace::new("v2_ts_literal");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db13v2"), "create db");
    make_table("db13v2", "ev", vec![col("t", DataType::Timestamp, true)]);

    bulk_insert("db13v2", "ev", &["2024-01-05 10:00:00".to_string()]);
    let catalog = load_catalog();

    let eq = try_select(
        &catalog,
        "db13v2",
        "SELECT COUNT(*) FROM ev WHERE t = '2024-01-05 10:00:00'",
    );
    assert_eq!(
        eq,
        Ok(vec![vec!["1".to_string()]]),
        "BUG V2 CONFIRMED: `WHERE t = '2024-01-05 10:00:00'` on a TIMESTAMP column \
         returned {:?} — string literals are never coerced to temporal types \
         ('Cannot compare TIMESTAMP with VARCHAR')",
        eq
    );
}

// ── Finding U2: scale-0 variant — INT literal probe on indexed NUMERIC ──────

#[test]
fn u2_indexed_numeric_scale0_int_literal_probe_fails_too() {
    // The mistyped-probe bug is not limited to scale > 0: NUMERIC(p, 0)
    // encodes BCD keys with (p+1).div_ceil(2) bytes, and the INT probe's
    // 4-byte to_bytes() blob fails that decode during multi-leaf navigation.
    let _ws = TestWorkspace::new("u2_idx_num_s0");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db13u2"), "create db");
    make_table(
        "db13u2",
        "t",
        vec![col(
            "n",
            DataType::Numeric {
                precision: 10,
                scale: 0,
            },
            true,
        )],
    );

    let rows: Vec<String> = (0..600).map(|i| format!("{}", 1000 + i)).collect();
    bulk_insert("db13u2", "t", &rows);
    let catalog = load_catalog();

    // Control: the unindexed lookup with the same literal works.
    let unindexed = try_select(&catalog, "db13u2", "SELECT COUNT(*) FROM t WHERE n = 1250")
        .expect("control: unindexed int-literal lookup");
    assert_eq!(unindexed, vec![vec!["1".to_string()]]);

    create_index(&catalog, "db13u2", "t", "ix_n", &["n".to_string()]).expect("create index");
    let catalog = load_catalog();

    let hit = try_select(&catalog, "db13u2", "SELECT COUNT(*) FROM t WHERE n = 1250");
    assert_eq!(
        hit,
        Ok(vec![vec!["1".to_string()]]),
        "BUG U2 CONFIRMED: indexed point lookup `WHERE n = 1250` on a \
         NUMERIC(10,0) column returned {:?} ('NUMERIC(10, 0) requires 6 \
         bytes') — same mistyped-probe encoding gap as finding U; the \
         unindexed control matches 1 row",
        hit
    );
}

// ── Regression guards ────────────────────────────────────────────────────────

#[test]
fn x1_indexed_int_point_lookup_still_works() {
    // Guard: the well-typed index path (INT column, INT literal) is intact
    // and spans multiple leaves.
    let _ws = TestWorkspace::new("x1_idx_int");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db13x1"), "create db");
    make_table("db13x1", "t", vec![col("n", DataType::Int, true)]);

    let rows: Vec<String> = (0..600).map(|i| format!("{}", 1000 + i)).collect();
    bulk_insert("db13x1", "t", &rows);
    let catalog = load_catalog();
    create_index(&catalog, "db13x1", "t", "ix_n", &["n".to_string()]).expect("create index");
    let catalog = load_catalog();

    let hit = try_select(&catalog, "db13x1", "SELECT COUNT(*) FROM t WHERE n = 1250")
        .expect("indexed INT lookup must work");
    assert_eq!(
        hit,
        vec![vec!["1"]],
        "guard: typed index probe still matches"
    );
}

#[test]
fn x3_unindexed_cross_type_numeric_predicates_still_work() {
    // Guard: the seq-scan path has full cross-type NUMERIC promotion.
    let _ws = TestWorkspace::new("x3_unindexed_num");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db13x3"), "create db");
    make_table(
        "db13x3",
        "acc",
        vec![col(
            "n",
            DataType::Numeric {
                precision: 10,
                scale: 2,
            },
            true,
        )],
    );
    bulk_insert(
        "db13x3",
        "acc",
        &["100.50".to_string(), "200.00".to_string()],
    );
    let catalog = load_catalog();

    let a = try_select(
        &catalog,
        "db13x3",
        "SELECT COUNT(*) FROM acc WHERE n = 100.50",
    )
    .expect("float literal on unindexed NUMERIC");
    assert_eq!(a, vec![vec!["1".to_string()]]);
    let b = try_select(&catalog, "db13x3", "SELECT COUNT(*) FROM acc WHERE n > 100")
        .expect("int literal range on unindexed NUMERIC");
    assert_eq!(b, vec![vec!["2".to_string()]]);
}
