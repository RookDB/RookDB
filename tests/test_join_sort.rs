//! External sort on join keys.
//!
//! The properties that matter: the output is genuinely ordered by key, the
//! multiset of rows is preserved however many times they were spilled and
//! merged, and NULL-keyed rows never enter the merge at all.
//!
//! Ordering comes from `JoinKey` bytes, which are a total order by
//! construction. The previous implementation compared values and fell back to
//! `Ordering::Equal` when two were incomparable, which breaks the contract
//! `BinaryHeap` relies on and leaves the merge reading runs that are not
//! sorted.

#[path = "join_common/mod.rs"]
mod common;

use common::{Rng, TempDb, seed_from_env};
use storage_manager::join::{
    JoinError, KeyClass, KeyColumn, KeySide, KeySpec, MemoryAccountant, RowCodec, SortOutput,
    SpillScope, sort_rows,
};
use storage_manager::types::{DataType, DataValue};

const FINGERPRINT: u64 = 0x1234_5678_9ABC_DEF0;

fn schema() -> Vec<DataType> {
    vec![DataType::Int, DataType::Varchar(24)]
}

fn key_spec() -> KeySpec {
    KeySpec::new(vec![KeyColumn {
        left_index: 0,
        right_index: 0,
        class: KeyClass::Integer,
    }])
}

/// Serialize `(key, payload)` rows; a `None` key becomes a NULL.
fn encode(rows: &[(Option<i32>, &str)]) -> Vec<Vec<u8>> {
    let codec = RowCodec::new(schema());
    rows.iter()
        .map(|(key, payload)| {
            codec
                .encode(&[
                    key.map(DataValue::Int),
                    Some(DataValue::Varchar((*payload).to_string())),
                ])
                .expect("encode")
        })
        .collect()
}

fn run_sort(rows: &[Vec<u8>], budget_bytes: u64, db: &TempDb) -> SortOutput {
    let codec = RowCodec::new(schema());
    let keys = key_spec();
    let budget = MemoryAccountant::new(budget_bytes);
    let scope = SpillScope::create(db.path()).expect("scope");

    let mut input = rows.iter().cloned().map(Ok::<Vec<u8>, JoinError>);
    sort_rows(
        &mut input,
        &codec,
        &keys,
        KeySide::Left,
        &budget,
        &scope,
        "side",
        FINGERPRINT,
    )
    .expect("sort should succeed")
}

/// Decode the sorted output, asserting it is non-decreasing by key.
fn drain_sorted(output: &mut SortOutput) -> Vec<(i32, String)> {
    let codec = RowCodec::new(schema());
    let mut previous: Option<Vec<u8>> = None;
    let mut decoded = Vec::new();

    while let Some(item) = output.rows.next() {
        let (key, row) = item.expect("read");

        if let Some(previous) = &previous {
            assert!(
                previous.as_slice() <= key.as_bytes(),
                "sort produced a descending step"
            );
        }
        previous = Some(key.as_bytes().to_vec());

        let values = codec.decode(&row).expect("decode");
        let Some(DataValue::Int(k)) = values[0].clone() else {
            panic!("a sorted row must have a non-NULL key: {values:?}");
        };
        let Some(DataValue::Varchar(payload)) = values[1].clone() else {
            panic!("payload should be present");
        };
        decoded.push((k, payload));
    }

    decoded
}

fn drain_nulls(output: &SortOutput) -> Vec<String> {
    let codec = RowCodec::new(schema());
    output
        .null_keyed
        .reader()
        .expect("reader")
        .map(|row| {
            let values = codec.decode(&row.expect("read")).expect("decode");
            assert!(values[0].is_none(), "this buffer is for NULL keys only");
            match values[1].clone() {
                Some(DataValue::Varchar(payload)) => payload,
                other => panic!("unexpected payload {other:?}"),
            }
        })
        .collect()
}

// ── In-memory sorting ────────────────────────────────────────────────────────

#[test]
fn a_small_input_sorts_in_memory() {
    let db = TempDb::new();
    let rows = encode(&[
        (Some(5), "e"),
        (Some(1), "a"),
        (Some(3), "c"),
        (Some(2), "b"),
        (Some(4), "d"),
    ]);

    let mut output = run_sort(&rows, 1024 * 1024, &db);
    assert_eq!(output.stats.runs, 0, "nothing should have spilled");
    assert_eq!(output.stats.sorted_rows, 5);

    let sorted = drain_sorted(&mut output);
    assert_eq!(
        sorted,
        vec![
            (1, "a".into()),
            (2, "b".into()),
            (3, "c".into()),
            (4, "d".into()),
            (5, "e".into()),
        ]
    );
}

/// Negative keys must sort below positive ones - the sign-flip in the key
/// encoding is what makes byte order agree with numeric order.
#[test]
fn negative_keys_sort_below_positive_ones() {
    let db = TempDb::new();
    let rows = encode(&[(Some(3), "c"), (Some(-5), "a"), (Some(0), "b")]);

    let mut output = run_sort(&rows, 1024 * 1024, &db);
    let sorted = drain_sorted(&mut output);
    assert_eq!(
        sorted.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
        vec![-5, 0, 3]
    );
}

/// Rows sharing a key are all kept - a sort must not deduplicate.
#[test]
fn duplicate_keys_are_all_preserved() {
    let db = TempDb::new();
    let rows = encode(&[
        (Some(1), "a"),
        (Some(1), "b"),
        (Some(1), "c"),
        (Some(2), "d"),
    ]);

    let mut output = run_sort(&rows, 1024 * 1024, &db);
    let sorted = drain_sorted(&mut output);

    assert_eq!(sorted.len(), 4);
    assert_eq!(sorted.iter().filter(|(k, _)| *k == 1).count(), 3);
}

// ── NULL keys ────────────────────────────────────────────────────────────────

/// A NULL key cannot match anything, so it must not enter the merge - but an
/// outer join still needs the row, so it is kept aside and re-readable.
#[test]
fn null_keys_are_set_aside_not_sorted() {
    let db = TempDb::new();
    let rows = encode(&[
        (Some(2), "b"),
        (None, "null-1"),
        (Some(1), "a"),
        (None, "null-2"),
    ]);

    let mut output = run_sort(&rows, 1024 * 1024, &db);
    assert_eq!(output.stats.null_keyed_rows, 2);
    assert_eq!(output.stats.sorted_rows, 2);

    let sorted = drain_sorted(&mut output);
    assert_eq!(sorted, vec![(1, "a".into()), (2, "b".into())]);

    let nulls = drain_nulls(&output);
    assert_eq!(nulls, vec!["null-1".to_string(), "null-2".to_string()]);

    // The buffer is re-readable, which is what an outer join needs.
    assert_eq!(drain_nulls(&output).len(), 2);
}

#[test]
fn an_all_null_input_produces_no_sorted_rows() {
    let db = TempDb::new();
    let rows = encode(&[(None, "x"), (None, "y")]);

    let mut output = run_sort(&rows, 1024 * 1024, &db);
    assert!(drain_sorted(&mut output).is_empty());
    assert_eq!(output.null_keyed.len(), 2);
}

#[test]
fn an_empty_input_sorts_to_nothing() {
    let db = TempDb::new();
    let mut output = run_sort(&[], 1024 * 1024, &db);

    assert!(drain_sorted(&mut output).is_empty());
    assert!(output.null_keyed.is_empty());
    assert_eq!(output.stats.runs, 0);
}

// ── Spilling and merging ─────────────────────────────────────────────────────

/// A budget too small to hold the input forces run generation, and the merged
/// result must be identical to the in-memory one.
#[test]
fn spilling_produces_the_same_order_as_sorting_in_memory() {
    let seed = seed_from_env(0x5EED_3001);
    println!("test_join_sort::spill seed = {seed}");
    let mut rng = Rng::new(seed);

    let pairs: Vec<(Option<i32>, String)> = (0..600)
        .map(|i| {
            let key = if rng.chance(1, 10) {
                None
            } else {
                Some(rng.range_i64(-500, 500) as i32)
            };
            (key, format!("payload-{i}"))
        })
        .collect();
    let borrowed: Vec<(Option<i32>, &str)> = pairs
        .iter()
        .map(|(key, payload)| (*key, payload.as_str()))
        .collect();
    let rows = encode(&borrowed);

    let db = TempDb::new();
    let mut resident = run_sort(&rows, 8 * 1024 * 1024, &db);
    assert_eq!(resident.stats.runs, 0, "the large budget should not spill");
    let expected = drain_sorted(&mut resident);

    let db = TempDb::new();
    let mut spilled = run_sort(&rows, 4096, &db);
    assert!(
        spilled.stats.runs > 1,
        "a 4 KiB budget must produce several runs, got {}",
        spilled.stats.runs
    );
    assert!(spilled.stats.spilled_bytes > 0);
    let actual = drain_sorted(&mut spilled);

    assert_eq!(actual, expected, "spilling changed the sorted result");
    assert_eq!(
        spilled.stats.null_keyed_rows,
        resident.stats.null_keyed_rows
    );
    assert_eq!(drain_nulls(&spilled).len(), drain_nulls(&resident).len());
}

/// With more runs than the merge fan-in, the sort must merge in several
/// passes - the loop the old implementation never exercised in a test.
#[test]
fn many_runs_are_merged_in_multiple_passes() {
    let db = TempDb::new();

    let pairs: Vec<(Option<i32>, String)> = (0..1200)
        .map(|i| (Some(((i * 7919) % 1000) as i32), format!("p{i}")))
        .collect();
    let borrowed: Vec<(Option<i32>, &str)> = pairs
        .iter()
        .map(|(key, payload)| (*key, payload.as_str()))
        .collect();
    let rows = encode(&borrowed);

    // A tiny budget gives a fan-in of two, so a dozen runs need several
    // passes to collapse.
    let mut output = run_sort(&rows, 1024, &db);

    assert!(
        output.stats.runs > 4,
        "expected many runs, got {}",
        output.stats.runs
    );
    assert!(
        output.stats.merge_passes > 1,
        "expected a multi-pass merge, got {} pass(es) over {} runs",
        output.stats.merge_passes,
        output.stats.runs
    );

    let sorted = drain_sorted(&mut output);
    assert_eq!(sorted.len(), 1200, "no rows may be lost across passes");

    // Every input row appears exactly once.
    let mut payloads: Vec<String> = sorted.into_iter().map(|(_, p)| p).collect();
    payloads.sort();
    let mut expected: Vec<String> = pairs.into_iter().map(|(_, p)| p).collect();
    expected.sort();
    assert_eq!(payloads, expected);
}

/// Spilling must not lose the rows already held in memory when the budget runs
/// out mid-stream.
#[test]
fn every_row_survives_a_spill() {
    let db = TempDb::new();
    let pairs: Vec<(Option<i32>, String)> =
        (0..300).map(|i| (Some(i), format!("row{i}"))).collect();
    let borrowed: Vec<(Option<i32>, &str)> = pairs
        .iter()
        .map(|(key, payload)| (*key, payload.as_str()))
        .collect();

    let mut output = run_sort(&encode(&borrowed), 2048, &db);
    assert!(output.stats.runs > 0);

    let sorted = drain_sorted(&mut output);
    assert_eq!(sorted.len(), 300);
    assert_eq!(
        sorted.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
        (0..300).collect::<Vec<_>>(),
        "keys must come out in ascending order with none missing"
    );
}
