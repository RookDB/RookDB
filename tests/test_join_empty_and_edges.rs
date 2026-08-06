//! Degenerate inputs: empty relations, single rows, and relations whose join
//! keys are entirely NULL.
//!
//! These are the shapes where outer-join bookkeeping tends to go wrong, and
//! where an operator is most likely to index past the end of something.

#[path = "join_common/mod.rs"]
mod common;

use common::{TempDb, assert_rows_eq, col, collect_rows, eq};
use storage_manager::join::{JoinBuilder, JoinType};
use storage_manager::types::{DataType, DataValue};

fn int(value: i32) -> Option<DataValue> {
    Some(DataValue::Int(value))
}

const ALL_TYPES: [JoinType; 7] = [
    JoinType::Inner,
    JoinType::LeftOuter,
    JoinType::RightOuter,
    JoinType::FullOuter,
    JoinType::Cross,
    JoinType::Semi,
    JoinType::Anti,
];

/// Every join type, every block size, against every combination of empty and
/// non-empty inputs. Nothing here may panic, and the row counts are fixed by
/// the definition of each join.
#[test]
fn empty_inputs_are_handled_for_every_join_type() {
    let db = TempDb::new();

    for (left_rows, right_rows) in [(0, 0), (0, 3), (3, 0)] {
        let mut left = db.create_table(
            &format!("l{left_rows}_{right_rows}"),
            &[("id", DataType::Int)],
        );
        for i in 0..left_rows {
            left.insert(vec![int(i)]);
        }
        left.flush();

        let mut right = db.create_table(
            &format!("r{left_rows}_{right_rows}"),
            &[("id", DataType::Int)],
        );
        for i in 0..right_rows {
            right.insert(vec![int(i)]);
        }
        right.flush();

        for join_type in ALL_TYPES {
            for block_rows in [1, 1024] {
                let mut builder = JoinBuilder::new(left.table_ref(), right.table_ref(), join_type)
                    .with_block_rows(block_rows);
                if join_type != JoinType::Cross {
                    // Each fixture gets a distinct table name, so the aliases
                    // have to be read back rather than assumed.
                    builder = builder.with_condition(eq(
                        col(&format!("{}.id", left.table_ref().alias)),
                        col(&format!("{}.id", right.table_ref().alias)),
                    ));
                }

                let stream = builder.execute();
                let rows = match stream {
                    Ok(stream) => collect_rows(stream).expect("runs"),
                    Err(e) => panic!("{join_type:?} failed to plan: {e}"),
                };

                let expected = match join_type {
                    JoinType::Cross => left_rows as usize * right_rows as usize,
                    JoinType::Inner | JoinType::Semi => 0,
                    JoinType::Anti | JoinType::LeftOuter => left_rows as usize,
                    JoinType::RightOuter => right_rows as usize,
                    JoinType::FullOuter => (left_rows + right_rows) as usize,
                };
                assert_eq!(
                    rows.len(),
                    expected,
                    "{join_type:?} on {left_rows}x{right_rows} with block {block_rows}"
                );
            }
        }
    }
}

/// A relation whose every key is NULL matches nothing, so an INNER join is
/// empty while LEFT keeps all its rows.
#[test]
fn all_null_keys_match_nothing() {
    let db = TempDb::new();

    let mut left = db.create_table("l", &[("id", DataType::Int), ("k", DataType::Int)]);
    left.insert_all(vec![vec![int(1), None], vec![int(2), None]]);
    left.flush();

    let mut right = db.create_table("r", &[("k", DataType::Int)]);
    right.insert_all(vec![vec![None], vec![int(1)]]);
    right.flush();

    let run = |join_type: JoinType| {
        collect_rows(
            JoinBuilder::new(left.table_ref(), right.table_ref(), join_type)
                .with_condition(eq(col("l.k"), col("r.k")))
                .execute()
                .expect("plans"),
        )
        .expect("runs")
    };

    assert!(run(JoinType::Inner).is_empty());
    assert!(run(JoinType::Semi).is_empty());
    assert_eq!(run(JoinType::Anti).len(), 2);
    assert_eq!(run(JoinType::LeftOuter).len(), 2);
    assert_eq!(run(JoinType::RightOuter).len(), 2);
    assert_eq!(run(JoinType::FullOuter).len(), 4);
}

/// One row on each side, matching and not matching.
#[test]
fn single_row_relations_join_correctly() {
    let db = TempDb::new();

    let mut left = db.create_table("l", &[("k", DataType::Int)]);
    left.insert(vec![int(7)]);
    left.flush();

    let mut matching = db.create_table("r", &[("k", DataType::Int)]);
    matching.insert(vec![int(7)]);
    matching.flush();

    let mut other = db.create_table("r2", &[("k", DataType::Int)]);
    other.insert(vec![int(8)]);
    other.flush();

    let inner_matching = collect_rows(
        JoinBuilder::new(left.table_ref(), matching.table_ref(), JoinType::Inner)
            .with_condition(eq(col("l.k"), col("r.k")))
            .execute()
            .expect("plans"),
    )
    .expect("runs");
    assert_eq!(inner_matching, vec![vec![int(7), int(7)]]);

    let inner_other = collect_rows(
        JoinBuilder::new(left.table_ref(), other.table_ref(), JoinType::Inner)
            .with_condition(eq(col("l.k"), col("r2.k")))
            .execute()
            .expect("plans"),
    )
    .expect("runs");
    assert!(inner_other.is_empty());

    let full_other = collect_rows(
        JoinBuilder::new(left.table_ref(), other.table_ref(), JoinType::FullOuter)
            .with_condition(eq(col("l.k"), col("r2.k")))
            .execute()
            .expect("plans"),
    )
    .expect("runs");
    assert_rows_eq(
        &full_other,
        &[vec![int(7), None], vec![None, int(8)]],
        "full outer over two unmatched single rows",
    );
}

/// A join whose key column is a variable-length type still works, including
/// with empty strings - the row codec's trickiest case.
#[test]
fn varchar_keys_including_empty_strings_join() {
    let db = TempDb::new();

    let text = |value: &str| Some(DataValue::Varchar(value.to_string()));

    let mut left = db.create_table("l", &[("k", DataType::Varchar(8))]);
    left.insert_all(vec![vec![text("")], vec![text("a")], vec![None]]);
    left.flush();

    let mut right = db.create_table("r", &[("k", DataType::Varchar(8))]);
    right.insert_all(vec![vec![text("")], vec![text("b")]]);
    right.flush();

    let rows = collect_rows(
        JoinBuilder::new(left.table_ref(), right.table_ref(), JoinType::Inner)
            .with_condition(eq(col("l.k"), col("r.k")))
            .execute()
            .expect("plans"),
    )
    .expect("runs");

    assert_eq!(
        rows,
        vec![vec![text(""), text("")]],
        "the empty string is a value and must join to itself"
    );
}
