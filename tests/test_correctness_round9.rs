//! Round-9 scratch verification tests for correctness issues found in a
//! ninth manual deep-dive (FK value decoding, SMALLINT ROUND overflow,
//! NUMERIC index key codec). Diagnostic probes like the round-1..8 suites —
//! each asserts the SQL-correct behaviour so a failure pinpoints the defect.
//!
//! Findings under test:
//!   A. `decode_tuple` (delete.rs) formats every non-INT/non-string column
//!      via `format!("{:?}", other)` (Rust **Debug**), so a BIGINT FK value
//!      round-trips as the literal string `"BigInt(7)"`. Every FK path
//!      (`validate_row_delete`, `propagate_update_to_children`, and
//!      `values_to_column_values` in update.rs) flows the parent key through
//!      `ColumnValue`, so RESTRICT/CASCADE/SET-NULL actions compare
//!      `"BigInt(7)"` against the child's actual `7` and find no match:
//!      DELETE/UPDATE on the parent succeeds despite live child rows.
//!      INT and VARCHAR keys are unaffected (dedicated arms), which is why
//!      the round-1..8 FK suites never tripped it.
//!   B. `functions::round(SmallInt, negative places)` computes in i64 and
//!      casts back with `as i16`, silently wrapping:
//!      `ROUND(32767::SMALLINT, -1)` → `-32766` instead of an error (or a
//!      widened INT), contradicting the engine's checked-overflow contract.
//!   C. B+ Tree key codec asymmetry for NUMERIC: `encode_key` (codec.rs)
//!      stores `DataValue::to_bytes()` — for `Numeric` the raw
//!      `[i128 LE][scale byte]` tuple (17 bytes) — while `decode_key` reads
//!      with `DataValue::from_bytes()`, which expects packed BCD of
//!      `ceil((precision+1)/2)` bytes (6 bytes for NUMERIC(10,2)). Every
//!      key comparison on a NUMERIC index therefore errors: an indexed
//!      NUMERIC column makes point/range lookups fail where SeqScan would
//!      return the row.
//!
//! Non-findings verified while probing (kept as regression guards):
//!   N1. Int ↔ DoublePrecision comparisons are supported by `Comparable`
//!       (comparison.rs has explicit arms), so index-scan sentinels built
//!       from float constants compare fine against INT tree keys.
//!   N2. NOT NULL accepts quoted empty strings (`''` reaches the check as a
//!       two-character literal, never as `""`).
//!   N3. `normalize_bit_literal`'s inverted quote-branch endings produce
//!       quote-bearing garbage for pathological input, but `validate_bit`
//!       rejects it downstream, so no user-visible corruption.

mod common;

use common::load_catalog;

use rook_parser::parse_set_clause;
use storage_manager::backend::executor::create_index::create_index;
use storage_manager::backend::executor::delete::delete_by_pointers;
use storage_manager::backend::executor::load_csv::insert_single_tuple;
use storage_manager::backend::executor::update::update_by_pointers;
use storage_manager::backend::system_table::insert_constraint_metadata;
use storage_manager::catalog::types::{Column, Constraints};
use storage_manager::catalog::{create_database, create_table, save_catalog};
use storage_manager::planner::plan_query;
use storage_manager::types::Comparable;
use storage_manager::types::datatype::DataType;
use storage_manager::types::functions::round;
use storage_manager::types::row::{deserialize_nullable_row, serialize_nullable_typed_row};
use storage_manager::types::value::{DataValue, NumericValue, OrderedF64};

/// A workspace for one test; see `common::TestWorkspace`.
struct TestWorkspace(#[allow(dead_code)] common::TestWorkspace);

impl TestWorkspace {
    fn new(tag: &str) -> Self {
        Self(common::TestWorkspace::new("cr9", tag))
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
) -> Vec<Vec<String>> {
    let plan = rook_parser::parse_sql(sql).expect("parse failed");
    let logical = plan_query(&plan, catalog, db).expect("plan failed");
    let tuples = storage_manager::backend::executor::physical::engine::execute_plan_collect(
        &logical, catalog, db,
    )
    .expect("execution failed");
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

// ── Finding A: FK paths decode BIGINT/NUMERIC/... via Debug formatting ──────
//
// delete.rs `decode_tuple`:
//   Some(other) => ColumnValue::Text(format!("{:?}", other)),
// A BIGINT parent key `7` becomes the TEXT `"BigInt(7)"`. `validate_row_delete`
// then asks whether any child row references `"BigInt(7)"` — none does — so a
// RESTRICT delete wrongly succeeds.

#[test]
fn a_fk_restrict_blocks_delete_of_bigint_parent_key() {
    let _ws = TestWorkspace::new("a_bigint_fk");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db9"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db9",
        "parent",
        vec![col("id", DataType::BigInt, false)],
    );
    save_catalog(&catalog).unwrap();
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db9",
        "child",
        vec![col("pid", DataType::BigInt, true)],
    );
    save_catalog(&catalog).unwrap();

    insert_constraint_metadata(
        &common::engine(),
        "db9",
        "child",
        "FOREIGN KEY",
        "pid",
        Some("parent"),
        Some("id"),
    )
    .expect("insert FK metadata");

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "db9", "parent", &["7"]).unwrap());
    assert!(insert_single_tuple(&catalog, "db9", "child", &["7"]).unwrap());

    // Sanity: BIGINT 7 survives a serialize/deserialize round-trip intact —
    // the corruption happens later, in decode_tuple's Debug arm.
    let schema = vec![DataType::BigInt];
    let bytes = serialize_nullable_typed_row(&schema, &[Some(DataValue::BigInt(7))]).unwrap();
    let decoded = deserialize_nullable_row(&schema, &bytes).unwrap();
    assert_eq!(decoded, vec![Some(DataValue::BigInt(7))], "test setup");

    // Freshly created table → the parent row lives at page 1, slot 0.
    let result = delete_by_pointers(&catalog, "db9", "parent", &[(1, 0)]);
    // A RESTRICT violation fails the statement; no row may be deleted.
    let deleted = match result {
        Ok(r) => r.deleted_count,
        Err(e) => {
            assert!(
                e.to_string().contains("FOREIGN KEY"),
                "unexpected error: {e}"
            );
            0
        }
    };

    assert_eq!(
        deleted, 0,
        "BUG A CONFIRMED: RESTRICT delete succeeded although child.pid = 7 \
         references parent.id = 7 — the parent key was decoded as 'BigInt(7)' \
         via format!(\"{{:?}}\") and never matched the child's 7"
    );
}

#[test]
fn a_fk_restrict_blocks_delete_of_numeric_parent_key() {
    let _ws = TestWorkspace::new("a_numeric_fk");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db9"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db9",
        "parent",
        vec![col(
            "id",
            DataType::Numeric {
                precision: 10,
                scale: 2,
            },
            false,
        )],
    );
    save_catalog(&catalog).unwrap();
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db9",
        "child",
        vec![col(
            "pid",
            DataType::Numeric {
                precision: 10,
                scale: 2,
            },
            true,
        )],
    );
    save_catalog(&catalog).unwrap();

    insert_constraint_metadata(
        &common::engine(),
        "db9",
        "child",
        "FOREIGN KEY",
        "pid",
        Some("parent"),
        Some("id"),
    )
    .expect("insert FK metadata");

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "db9", "parent", &["5.50"]).unwrap());
    assert!(insert_single_tuple(&catalog, "db9", "child", &["5.50"]).unwrap());

    let result = delete_by_pointers(&catalog, "db9", "parent", &[(1, 0)]);
    // A RESTRICT violation fails the statement; no row may be deleted.
    let deleted = match result {
        Ok(r) => r.deleted_count,
        Err(e) => {
            assert!(
                e.to_string().contains("FOREIGN KEY"),
                "unexpected error: {e}"
            );
            0
        }
    };

    assert_eq!(
        deleted, 0,
        "BUG A CONFIRMED: RESTRICT delete succeeded although child references \
         the NUMERIC parent key — decoded as 'Numeric(NumericValue {{ unscaled: \
         550, scale: 2 }})' via format!(\"{{:?}}\")"
    );
}

#[test]
fn control_a_fk_restrict_still_blocks_int_parent_key() {
    // Control: the Int arm formats correctly, so RESTRICT works for INT keys.
    let _ws = TestWorkspace::new("a_int_control");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db9"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db9",
        "parent",
        vec![col("id", DataType::Int, false)],
    );
    save_catalog(&catalog).unwrap();
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db9",
        "child",
        vec![col("pid", DataType::Int, true)],
    );
    save_catalog(&catalog).unwrap();

    insert_constraint_metadata(
        &common::engine(),
        "db9",
        "child",
        "FOREIGN KEY",
        "pid",
        Some("parent"),
        Some("id"),
    )
    .expect("insert FK metadata");

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "db9", "parent", &["7"]).unwrap());
    assert!(insert_single_tuple(&catalog, "db9", "child", &["7"]).unwrap());

    let result = delete_by_pointers(&catalog, "db9", "parent", &[(1, 0)]);
    // A RESTRICT violation fails the statement; no row may be deleted.
    let deleted = match result {
        Ok(r) => r.deleted_count,
        Err(e) => {
            assert!(
                e.to_string().contains("FOREIGN KEY"),
                "unexpected error: {e}"
            );
            0
        }
    };

    assert_eq!(
        deleted, 0,
        "control: RESTRICT must block deleting an INT parent key with a live child"
    );
}

#[test]
fn a_update_restrict_blocks_bigint_parent_key_change() {
    // Same defect on the UPDATE path: propagate_update_to_children gets its
    // old value through values_to_column_values (same Debug arm).
    let _ws = TestWorkspace::new("a_bigint_fk_update");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db9"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db9",
        "parent",
        vec![col("id", DataType::BigInt, false)],
    );
    save_catalog(&catalog).unwrap();
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db9",
        "child",
        vec![col("pid", DataType::BigInt, true)],
    );
    save_catalog(&catalog).unwrap();

    insert_constraint_metadata(
        &common::engine(),
        "db9",
        "child",
        "FOREIGN KEY",
        "pid",
        Some("parent"),
        Some("id"),
    )
    .expect("insert FK metadata");

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "db9", "parent", &["7"]).unwrap());
    assert!(insert_single_tuple(&catalog, "db9", "child", &["7"]).unwrap());

    // UPDATE parent SET id = 9 WHERE id = 7 (row at page 1, slot 0)
    let assignments = parse_set_clause("id = 9").expect("parse set");
    // A RESTRICT violation fails the statement; no row may be updated.
    let updated = update_by_pointers(&catalog, "db9", "parent", &[(1, 0)], &assignments)
        .map(|r| r.updated_count)
        .unwrap_or(0);

    assert_eq!(
        updated, 0,
        "BUG A CONFIRMED: RESTRICT update succeeded although child.pid = 7 \
         references parent.id = 7 (old value decoded as 'BigInt(7)')"
    );
}

// ── Finding B: ROUND(SMALLINT, negative places) wraps silently ──────────────
//
// round(&DataValue::SmallInt(v), places) computes round_int(v as i64, places)
// then casts back with `as i16`. For places = -1 the rounding can exceed the
// SMALLINT range (32767 → 32770), and `as i16` wraps to -32766.

#[test]
fn b_round_smallint_negative_places_does_not_wrap() {
    let out = round(&DataValue::SmallInt(32767), -1);
    match out {
        Ok(v) => assert_ne!(
            v,
            DataValue::SmallInt(-32766),
            "BUG B CONFIRMED: ROUND(32767::SMALLINT, -1) wrapped via `as i16` to {} \
             instead of erroring or widening",
            v
        ),
        Err(e) => {
            /* acceptable: explicit overflow error */
            let _ = e;
        }
    }
}

/// Control: in-range negative-places rounding is half-away-from-zero.
#[test]
fn control_b_round_int_negative_places() {
    assert_eq!(
        round(&DataValue::Int(123), -1).unwrap(),
        DataValue::Int(120)
    );
    assert_eq!(
        round(&DataValue::Int(125), -1).unwrap(),
        DataValue::Int(130)
    );
    assert_eq!(
        round(&DataValue::Int(-125), -1).unwrap(),
        DataValue::Int(-130)
    );
}

// ── Finding C: NUMERIC index keys encode raw, decode as BCD ─────────────────
//
// codec.rs:  encode_key → DataValue::to_bytes()  → Numeric emits raw
//            [i128 LE][scale byte] = 17 bytes.
//            decode_key → DataValue::from_bytes(NUMERIC(10,2)) expects
//            ceil((10+1)/2) = 6 packed-BCD bytes → Err on every tree key.
//
// Primitive-level probe first, then an end-to-end query through a NUMERIC
// index that must return the matching row.

#[test]
fn c_numeric_index_key_roundtrip_via_codec_primitives() {
    // What the index stores (encode_key → to_bytes):
    let key = DataValue::Numeric(NumericValue {
        unscaled: 550,
        scale: 2,
    });
    let encoded = key.to_bytes();
    assert_eq!(
        encoded.len(),
        17,
        "test premise: Numeric::to_bytes is 17 raw bytes"
    );

    // What every tree comparison must do (decode_key → from_bytes with the
    // column type), per cmp_encoded_keys / cmp_encoded_vs_values:
    let decoded = DataValue::from_bytes(
        &DataType::Numeric {
            precision: 10,
            scale: 2,
        },
        &encoded,
    );
    match decoded {
        Ok(dv) => assert_eq!(
            dv.compare(&key).unwrap(),
            std::cmp::Ordering::Equal,
            "BUG C CONFIRMED: NUMERIC index key round-trip changed the value"
        ),
        Err(e) => panic!(
            "BUG C CONFIRMED: a NUMERIC index key encoded by encode_key/to_bytes \
             cannot be decoded by decode_key/from_bytes (17 raw bytes written vs \
             6 BCD bytes expected): {}",
            e
        ),
    }
}

/// Control: INT keys encode and decode consistently (raw LE both ways).
#[test]
fn control_c_int_index_key_roundtrip() {
    let key = DataValue::Int(42);
    let encoded = key.to_bytes();
    let decoded = DataValue::from_bytes(&DataType::Int, &encoded).unwrap();
    assert_eq!(decoded.compare(&key).unwrap(), std::cmp::Ordering::Equal);
}

/// End-to-end: an index over a NUMERIC column must be buildable and usable.
/// The symptom is deterministic and two-stage:
///   1. `create_index` re-reads every key it just wrote; the 17-byte raw
///      payloads fail BCD decoding, so the build itself aborts with
///      "NUMERIC payload has invalid sign nibble".
///   2. Any search against a (partially) built NUMERIC tree hits the same
///      codec mismatch inside `cmp_encoded_keys`.
///
/// The same predicate without an index (SeqScan) returns the row.
#[test]
fn c_numeric_index_point_lookup_returns_row() {
    let _ws = TestWorkspace::new("c_numeric_index");

    let mut catalog = load_catalog();
    assert!(create_database(&mut catalog, "db9"), "create db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "db9",
        "items",
        vec![
            col("id", DataType::Int, false),
            col(
                "price",
                DataType::Numeric {
                    precision: 10,
                    scale: 2,
                },
                true,
            ),
        ],
    );
    save_catalog(&catalog).unwrap();
    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "db9", "items", &["1", "5.50"]).unwrap());
    assert!(insert_single_tuple(&catalog, "db9", "items", &["2", "7.25"]).unwrap());

    // Control first: SeqScan path answers the same predicate correctly.
    // (No index yet → no IndexScan in the plan.)
    let rows_no_idx = run_select(&catalog, "db9", "SELECT id FROM items WHERE price = 5.50");
    assert_eq!(
        rows_no_idx,
        vec![vec!["1".to_string()]],
        "control: the predicate is answerable without an index"
    );

    let built = create_index(
        &catalog,
        "db9",
        "items",
        "idx_items_price",
        &["price".to_string()],
    );
    if let Err(e) = built {
        let msg = format!("{}", e);
        assert!(
            msg.contains("sign nibble") || msg.contains("NUMERIC"),
            "BUG C CONFIRMED: CREATE INDEX over a NUMERIC column failed with an \
             unexpected error: {}",
            msg
        );
        panic!(
            "BUG C CONFIRMED: CREATE INDEX over a NUMERIC column cannot even be \
             built — BTree::insert_keys re-reads the just-encoded 17-byte raw \
             payload via decode_key (6 BCD bytes expected) and aborts: {}",
            msg
        );
    }

    let rows = run_select(&catalog, "db9", "SELECT id FROM items WHERE price = 5.50");
    assert_eq!(
        rows,
        vec![vec!["1".to_string()]],
        "BUG C CONFIRMED: point lookup through the NUMERIC index returned {:?} \
         (want [id=1]) — encode_key stores 17 raw bytes while decode_key \
         demands 6 BCD bytes, so tree comparisons error",
        rows
    );
}

// ── Regression guards for non-findings ──────────────────────────────────────

/// N1: float constants vs INT keys compare fine, so index-scan range
/// sentinels built from float literals are not a defect.
#[test]
fn n1_int_vs_double_precision_is_comparable() {
    let ord = DataValue::Int(2).compare(&DataValue::DoublePrecision(OrderedF64(1.5)));
    assert_eq!(ord.unwrap(), std::cmp::Ordering::Greater);
}

/// N3: pathological BIT literals with embedded quotes never reach storage —
/// validation rejects whatever the normalize branches mangle.
#[test]
fn n3_bit_literal_with_embedded_quote_is_rejected() {
    let r = DataValue::parse_and_encode(&DataType::Bit(4), "B'10\"10'");
    assert!(
        r.is_err(),
        "embedded-quote BIT literal must be rejected, got {:?}",
        r
    );
}
