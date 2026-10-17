//! An aggregate's declared output type and the value it produces agree (G11).
//!
//! SUM follows PostgreSQL: SMALLINT/INT sum to BIGINT, BIGINT to NUMERIC,
//! REAL to REAL, DOUBLE PRECISION to DOUBLE PRECISION, NUMERIC to NUMERIC.
//! SUM over BIGINT used to be declared BIGINT and return a NUMERIC once the
//! total passed 64 bits; SUM over REAL was declared REAL and returned a
//! DOUBLE PRECISION.

mod common;

use storage_manager::backend::session::Session;
use storage_manager::types::DataValue;
use storage_manager::types::datatype::DataType;
use storage_manager::types::value::{NumericValue, OrderedF32, OrderedF64};

fn run(s: &mut Session, sql: &str) -> (Vec<DataType>, Vec<Vec<Option<DataValue>>>) {
    let result = s
        .execute(rook_parser::parse_sql(sql).unwrap())
        .unwrap_or_else(|e| panic!("{}: {}", sql, e));
    result
        .rows
        .map(|(schema, rows)| {
            (
                schema.into_iter().map(|c| c.data_type).collect(),
                rows.into_iter().map(|t| t.values).collect(),
            )
        })
        .unwrap_or_default()
}

fn session(tag: &str) -> (common::TestWorkspace, Session) {
    let ws = common::TestWorkspace::new("aggtypes", tag);
    let mut s = Session::new(common::load_catalog());
    for sql in [
        "CREATE DATABASE ag",
        "USE ag",
        "CREATE TABLE a (id INT, s SMALLINT, b BIGINT, r REAL, f DOUBLE PRECISION, n NUMERIC(10,2))",
        "INSERT INTO a VALUES (1, 1, 9223372036854775807, 0.1, 2.5, 1.10)",
        "INSERT INTO a VALUES (0, 2, 9223372036854775807, 0.1, 0.25, 2.20)",
    ] {
        run(&mut s, sql);
    }
    (ws, s)
}

fn numeric(unscaled: i128, scale: u8) -> Option<DataValue> {
    Some(DataValue::numeric(NumericValue { unscaled, scale }))
}

/// Whether `value` is of the variant a column declared `declared` holds.
fn holds(declared: &DataType, value: &Option<DataValue>) -> bool {
    matches!(
        (declared, value),
        (_, None)
            | (DataType::Int, Some(DataValue::Int(_)))
            | (DataType::BigInt, Some(DataValue::BigInt(_)))
            | (DataType::Real, Some(DataValue::Real(_)))
            | (
                DataType::DoublePrecision,
                Some(DataValue::DoublePrecision(_))
            )
            | (DataType::Numeric { .. }, Some(DataValue::Numeric(_)))
    )
}

#[test]
fn sum_is_declared_with_the_type_it_returns() {
    let (_ws, mut s) = session("sum");
    let (types, rows) = run(
        &mut s,
        "SELECT SUM(s), SUM(id), SUM(b), SUM(r), SUM(f), SUM(n) FROM a",
    );
    let big = DataType::Numeric {
        precision: 38,
        scale: 0,
    };
    assert_eq!(
        types,
        [
            DataType::BigInt,
            DataType::BigInt,
            big,
            DataType::Real,
            DataType::DoublePrecision,
            DataType::Numeric {
                precision: 38,
                scale: 2
            },
        ]
    );
    assert_eq!(
        rows,
        [[
            Some(DataValue::BigInt(3)),
            Some(DataValue::BigInt(1)),
            // Two i64::MAX: past 64 bits, and exact.
            numeric(18446744073709551614, 0),
            Some(DataValue::Real(OrderedF32(0.2))),
            Some(DataValue::DoublePrecision(OrderedF64(2.75))),
            numeric(330, 2),
        ]]
    );
}

#[test]
fn sum_over_bigint_is_numeric_inside_the_64_bit_range_too() {
    let (_ws, mut s) = session("small");
    run(&mut s, "CREATE TABLE small (b BIGINT)");
    run(&mut s, "INSERT INTO small VALUES (40), (2)");
    let (types, rows) = run(&mut s, "SELECT SUM(b) FROM small");
    assert!(holds(&types[0], &rows[0][0]), "{:?} {:?}", types, rows);
    assert_eq!(rows, [[numeric(42, 0)]]);

    // It still assigns to a BIGINT column when it fits ...
    run(&mut s, "CREATE TABLE t (b BIGINT)");
    run(&mut s, "INSERT INTO t SELECT SUM(b) FROM small");
    assert_eq!(
        run(&mut s, "SELECT b FROM t").1,
        [[Some(DataValue::BigInt(42))]]
    );
    // ... and is rejected, not stored, when it does not.
    let overflow = s.execute(rook_parser::parse_sql("INSERT INTO t SELECT SUM(b) FROM a").unwrap());
    assert!(overflow.is_err());
    assert_eq!(
        run(&mut s, "SELECT COUNT(*) FROM t").1,
        [[Some(DataValue::BigInt(1))]]
    );
}

#[test]
fn expressions_over_aggregates_keep_type_and_value_together() {
    let (_ws, mut s) = session("exprs");
    for sql in [
        "SELECT SUM(b) + 1 FROM a",
        "SELECT SUM(b - 1) FROM a",
        "SELECT SUM(n * 2) FROM a",
        "SELECT SUM(id * 2), SUM(s + id) FROM a",
        "SELECT SUM(id + 0.5), SUM(r * 2) FROM a",
        "SELECT AVG(s), AVG(id), AVG(b), AVG(r), AVG(f), AVG(n) FROM a",
        "SELECT SUM(id), SUM(b), SUM(r) FROM a WHERE id > 100",
        "SELECT id, SUM(b), SUM(r) FROM a GROUP BY id ORDER BY id",
    ] {
        let (types, rows) = run(&mut s, sql);
        for row in &rows {
            for (declared, value) in types.iter().zip(row) {
                assert!(
                    holds(declared, value),
                    "{}: {:?} declared {:?}",
                    sql,
                    value,
                    declared
                );
            }
        }
    }
    assert_eq!(
        run(&mut s, "SELECT SUM(b) + 1 FROM a").1,
        [[numeric(18446744073709551615, 0)]]
    );
    // AVG is DOUBLE PRECISION for every input type.
    let (types, _) = run(&mut s, "SELECT AVG(id), AVG(b), AVG(r), AVG(n) FROM a");
    assert!(types.iter().all(|t| *t == DataType::DoublePrecision));
}
