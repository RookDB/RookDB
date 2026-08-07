//! Statistics: distinct-value estimation, histograms, caching and ANALYZE.
//!
//! The previous implementation never populated statistics at all, so its
//! "cost-based" planner saw a hardcoded 100 pages and 10 000 rows for every
//! table in the database. What matters here is that the numbers are real, that
//! they are measured in the same equivalence classes the join matches on, and
//! that stale ones are detected rather than trusted.

#[path = "join_common/mod.rs"]
mod common;

use common::TempDb;
use storage_manager::join::key::{KeyClass, encode_value};
use storage_manager::join::stats::histogram::ReservoirSampler;
use storage_manager::join::stats::hll::HyperLogLog;
use storage_manager::join::{
    StatsConfidence, TableStatsCache, analyze_table, load_stats, save_stats,
};
use storage_manager::types::{DataType, DataValue};

fn int(value: i32) -> Option<DataValue> {
    Some(DataValue::Int(value))
}

fn text(value: &str) -> Option<DataValue> {
    Some(DataValue::Varchar(value.to_string()))
}

// ── HyperLogLog ──────────────────────────────────────────────────────────────

fn encoded_int(value: i64) -> Vec<u8> {
    encode_value(KeyClass::Integer, &DataValue::BigInt(value)).expect("encode")
}

/// Accuracy across four orders of magnitude. The small-range correction is
/// what makes the low end usable - without it a fifty-value sketch is wildly
/// wrong, and a cost model that cannot be checked on small fixtures cannot be
/// checked at all.
#[test]
fn distinct_estimates_are_accurate_across_scales() {
    for distinct in [1u64, 10, 100, 1_000, 10_000, 100_000] {
        let mut sketch = HyperLogLog::new();
        for value in 0..distinct {
            sketch.add(&encoded_int(value as i64));
        }

        let estimate = sketch.estimate() as f64;
        let actual = distinct as f64;
        let error = (estimate - actual).abs() / actual;

        // Standard error at 4096 registers is about 1.6%; allow generous
        // headroom so this is not a flaky test, but tight enough to catch a
        // broken estimator.
        assert!(
            error < 0.10,
            "{distinct} distinct values estimated as {estimate} ({:.1}% off)",
            error * 100.0
        );
    }
}

/// Repeats must not inflate the estimate.
#[test]
fn repeated_values_are_counted_once() {
    let mut sketch = HyperLogLog::new();
    for _ in 0..10_000 {
        for value in 0..50u64 {
            sketch.add(&encoded_int(value as i64));
        }
    }
    let estimate = sketch.estimate();
    assert!(
        (estimate as i64 - 50).abs() < 10,
        "500 000 additions of 50 values estimated as {estimate}"
    );
}

#[test]
fn an_empty_sketch_estimates_zero() {
    assert_eq!(HyperLogLog::new().estimate(), 0);
}

#[test]
fn merging_sketches_unions_their_values() {
    let mut first = HyperLogLog::new();
    let mut second = HyperLogLog::new();
    for value in 0..500u64 {
        first.add(&encoded_int(value as i64));
    }
    for value in 400..900u64 {
        second.add(&encoded_int(value as i64));
    }

    first.merge(&second);
    let estimate = first.estimate() as f64;
    // The union is 0..900.
    assert!(
        (estimate - 900.0).abs() / 900.0 < 0.10,
        "union estimated as {estimate}"
    );
}

// ── Histograms ───────────────────────────────────────────────────────────────

#[test]
fn histogram_boundaries_are_monotone() {
    let mut sampler = ReservoirSampler::new();
    for value in 0..5_000i64 {
        sampler.add(&encoded_int(value));
    }
    let histogram = sampler.finish().expect("enough rows for a histogram");

    assert_eq!(histogram.buckets(), 64);
    for pair in histogram.bounds.windows(2) {
        assert!(
            pair[0] <= pair[1],
            "bucket boundaries must be non-decreasing"
        );
    }
    assert_eq!(histogram.rows_represented, 5_000);
}

/// Too few rows means no histogram: boundaries drawn from a handful of values
/// would be worse than admitting ignorance.
#[test]
fn too_few_rows_yield_no_histogram() {
    let mut sampler = ReservoirSampler::new();
    for value in 0..10i64 {
        sampler.add(&encoded_int(value));
    }
    assert!(sampler.finish().is_none());
}

#[test]
fn the_fraction_below_a_value_grows_with_the_value() {
    let mut sampler = ReservoirSampler::new();
    for value in 0..5_000i64 {
        sampler.add(&encoded_int(value));
    }
    let histogram = sampler.finish().expect("histogram");

    let low = histogram.fraction_at_or_below(&encoded_int(500));
    let middle = histogram.fraction_at_or_below(&encoded_int(2_500));
    let high = histogram.fraction_at_or_below(&encoded_int(4_500));

    assert!(low < middle, "{low} should be below {middle}");
    assert!(middle < high, "{middle} should be below {high}");
    assert!((0.0..=1.0).contains(&low));
    assert!((0.0..=1.0).contains(&high));
}

/// Two disjoint ranges: everything in the lower one is below everything in the
/// upper one.
#[test]
fn convolving_disjoint_histograms_is_decisive() {
    let mut low = ReservoirSampler::new();
    let mut high = ReservoirSampler::new();
    for value in 0..2_000i64 {
        low.add(&encoded_int(value));
        high.add(&encoded_int(value + 1_000_000));
    }
    let low = low.finish().expect("histogram");
    let high = high.finish().expect("histogram");

    assert!(
        low.fraction_less_than(&high) > 0.99,
        "every low value is below every high one"
    );
    assert!(
        high.fraction_less_than(&low) < 0.01,
        "and none the other way"
    );
}

/// Sampling is seeded, so ANALYZE is reproducible and so is any plan derived
/// from it.
#[test]
fn sampling_is_deterministic() {
    let build = || {
        let mut sampler = ReservoirSampler::new();
        for value in 0..50_000i64 {
            sampler.add(&encoded_int(value.wrapping_mul(7919)));
        }
        sampler.finish().expect("histogram")
    };

    assert_eq!(build(), build(), "two identical runs must agree exactly");
}

// ── ANALYZE over a real table ────────────────────────────────────────────────

#[test]
fn analyze_measures_nulls_distinct_values_and_extremes() {
    let db = TempDb::new();
    let mut table = db.create_table(
        "t",
        &[("k", DataType::Int), ("name", DataType::Varchar(16))],
    );

    // 100 rows: `k` cycles through 20 values, 10 rows have a NULL name.
    for i in 0..100 {
        let name = if i % 10 == 0 {
            None
        } else {
            text(&format!("name-{}", i % 25))
        };
        table.insert(vec![int(i % 20), name]);
    }
    table.flush();

    let stats = analyze_table(&table.table_ref()).expect("analyze");

    assert_eq!(stats.rows, 100);
    assert_eq!(stats.columns.len(), 2);

    let key = &stats.columns[0];
    assert_eq!(key.name, "k");
    assert_eq!(key.null_fraction, 0.0);
    assert!(
        (key.distinct as i64 - 20).abs() <= 2,
        "k holds 20 distinct values; estimated {}",
        key.distinct
    );
    assert_eq!(
        key.min,
        Some(encode_value(KeyClass::Integer, &DataValue::Int(0)).expect("encode"))
    );
    assert_eq!(
        key.max,
        Some(encode_value(KeyClass::Integer, &DataValue::Int(19)).expect("encode"))
    );

    let name = &stats.columns[1];
    assert!(
        (name.null_fraction - 0.10).abs() < 1e-9,
        "null fraction should be exact, got {}",
        name.null_fraction
    );
    assert!(
        (name.distinct as i64 - 25).abs() <= 2,
        "25 distinct non-NULL names; estimated {}",
        name.distinct
    );
}

/// Distinct values are counted in join key encoding, so values the join treats
/// as equal count once. CHAR ignores trailing whitespace; VARCHAR does not.
#[test]
fn distinct_counts_use_join_equality_not_byte_equality() {
    let db = TempDb::new();
    let mut table = db.create_table(
        "t",
        &[
            ("padded", DataType::Char(8)),
            ("exact", DataType::Varchar(8)),
        ],
    );

    for _ in 0..20 {
        table.insert(vec![
            Some(DataValue::Char("ab".to_string())),
            Some(DataValue::Varchar("ab".to_string())),
        ]);
        table.insert(vec![
            Some(DataValue::Char("ab   ".to_string())),
            Some(DataValue::Varchar("ab   ".to_string())),
        ]);
    }
    table.flush();

    let stats = analyze_table(&table.table_ref()).expect("analyze");

    assert_eq!(
        stats.columns[0].distinct, 1,
        "CHAR ignores trailing spaces, so these are one value to a join"
    );
    assert_eq!(
        stats.columns[1].distinct, 2,
        "VARCHAR keeps them, so they are two values"
    );
}

/// Row counts come from the heap's own counter, which is decremented on
/// delete. The engine's page-statistics function counts slot entries and does
/// not skip dead ones - recorded here so a future upstream fix is noticed.
#[test]
fn analyze_counts_live_rows_not_slot_entries() {
    let db = TempDb::new();
    let mut table = db.create_table("t", &[("k", DataType::Int)]);
    for i in 0..100 {
        table.insert(vec![int(i)]);
    }
    table.flush();

    assert_eq!(
        analyze_table(&table.table_ref()).expect("analyze").rows,
        100
    );

    let deleted = table.delete_first(40);
    assert_eq!(deleted, 40);

    let stats = analyze_table(&table.table_ref()).expect("analyze");
    assert_eq!(stats.rows, 60, "a deleted row is not a live row");

    // Distinct values are estimated, so this is a tolerance and not an
    // equality: asserting an estimator is exact would be a test that breaks on
    // any change to the data.
    let distinct = stats.columns[0].distinct as i64;
    assert!(
        (distinct - 60).abs() <= 3,
        "a deleted row is not a distinct value either; estimated {distinct} of 60"
    );

    // Characterisation of the engine's own counter.
    let mut file = std::fs::File::open(&table.table_ref().path).expect("open");
    let upstream = storage_manager::statistics::collect_table_statistics_from_file(&mut file)
        .expect("collect");
    assert_eq!(
        upstream.total_tuple_count, 100,
        "if this changes, upstream now skips dead slots and the workaround can be revisited"
    );
}

#[test]
fn analyzing_an_empty_table_is_valid() {
    let db = TempDb::new();
    let mut table = db.create_table("t", &[("k", DataType::Int)]);
    table.flush();

    let stats = analyze_table(&table.table_ref()).expect("analyze");
    assert_eq!(stats.rows, 0);
    assert_eq!(stats.columns[0].distinct, 0);
    assert_eq!(stats.columns[0].min, None);
    assert!(stats.columns[0].histogram.is_none());
}

/// Two runs over unchanged data must produce identical statistics.
#[test]
fn analyze_is_reproducible() {
    let db = TempDb::new();
    let mut table = db.create_table("t", &[("k", DataType::Int)]);
    for i in 0..2_000 {
        table.insert(vec![int(i % 500)]);
    }
    table.flush();

    let first = analyze_table(&table.table_ref()).expect("analyze");
    let second = analyze_table(&table.table_ref()).expect("analyze");
    assert_eq!(first, second);
}

// ── Persistence and confidence ───────────────────────────────────────────────

#[test]
fn statistics_round_trip_through_the_sidecar() {
    let db = TempDb::new();
    let mut table = db.create_table("t", &[("k", DataType::Int)]);
    for i in 0..300 {
        table.insert(vec![int(i % 40)]);
    }
    table.flush();

    let stats = analyze_table(&table.table_ref()).expect("analyze");
    let path = save_stats(&table.table_ref(), &stats).expect("save");
    assert!(path.exists());

    let loaded = load_stats(&table.table_ref()).expect("load");
    assert_eq!(loaded, stats);
}

/// Without a sidecar the planner still knows exact row and page counts, and
/// says so.
#[test]
fn an_unanalyzed_table_reports_header_only_confidence() {
    let db = TempDb::new();
    let mut table = db.create_table("t", &[("k", DataType::Int)]);
    for i in 0..250 {
        table.insert(vec![int(i)]);
    }
    table.flush();

    let cache = TableStatsCache::new();
    let (stats, confidence) = cache.stats_for(&table.table_ref());

    assert_eq!(confidence, StatsConfidence::HeaderOnly);
    assert_eq!(confidence.label(), "header-only");
    assert_eq!(stats.rows, 250, "cardinality is exact even unanalyzed");
    assert!(stats.data_pages > 0);
    assert!(stats.avg_row_bytes > 0.0);
    assert!(
        stats.columns[0].histogram.is_none(),
        "no histogram without ANALYZE"
    );
}

#[test]
fn an_analyzed_table_reports_analyzed_confidence() {
    let db = TempDb::new();
    let mut table = db.create_table("t", &[("k", DataType::Int)]);
    for i in 0..300 {
        table.insert(vec![int(i % 30)]);
    }
    table.flush();

    let stats = analyze_table(&table.table_ref()).expect("analyze");
    save_stats(&table.table_ref(), &stats).expect("save");

    let cache = TableStatsCache::new();
    let (loaded, confidence) = cache.stats_for(&table.table_ref());

    assert_eq!(confidence, StatsConfidence::Analyzed);
    assert!((loaded.columns[0].distinct as i64 - 30).abs() <= 2);
    assert!(loaded.columns[0].histogram.is_some());
}

/// Statistics written before the table changed must not be used. Degrading is
/// correct; planning from numbers that no longer describe the data is not.
#[test]
fn a_stale_sidecar_is_ignored() {
    let db = TempDb::new();
    let mut table = db.create_table("t", &[("k", DataType::Int)]);
    for i in 0..200 {
        table.insert(vec![int(i % 20)]);
    }
    table.flush();

    let stats = analyze_table(&table.table_ref()).expect("analyze");
    save_stats(&table.table_ref(), &stats).expect("save");

    let cache = TableStatsCache::new();
    assert_eq!(
        cache.stats_for(&table.table_ref()).1,
        StatsConfidence::Analyzed
    );

    // Change the table behind the statistics' back.
    for i in 0..200 {
        table.insert(vec![int(1000 + i)]);
    }
    table.flush();

    let fresh = TableStatsCache::new();
    let (degraded, confidence) = fresh.stats_for(&table.table_ref());
    assert_eq!(
        confidence,
        StatsConfidence::HeaderOnly,
        "the sidecar no longer describes this table"
    );
    assert_eq!(degraded.rows, 400, "but the row count is still exact");
}

/// The cache must not re-read a relation whose stamp has not moved.
#[test]
fn the_cache_reuses_unchanged_statistics() {
    let db = TempDb::new();
    let mut table = db.create_table("t", &[("k", DataType::Int)]);
    for i in 0..100 {
        table.insert(vec![int(i)]);
    }
    table.flush();

    let cache = TableStatsCache::new();
    let (first, _) = cache.stats_for(&table.table_ref());
    let (second, _) = cache.stats_for(&table.table_ref());

    assert!(
        std::rc::Rc::ptr_eq(&first, &second),
        "the second lookup should hand back the cached value"
    );

    cache.invalidate(&table.table_ref());
    let (third, _) = cache.stats_for(&table.table_ref());
    assert!(
        !std::rc::Rc::ptr_eq(&first, &third),
        "invalidation should re-read"
    );
    assert_eq!(third.rows, first.rows);
}

/// A unique column has exactly as many distinct values as rows; that is known
/// from the catalog without measuring anything.
#[test]
fn a_unique_constraint_is_used_when_unanalyzed() {
    use storage_manager::catalog::Column;

    let db = TempDb::new();
    let mut table = db.create_table("t", &[("k", DataType::Int), ("v", DataType::Int)]);
    for i in 0..400 {
        table.insert(vec![int(i), int(i % 3)]);
    }
    table.flush();

    let mut reference = table.table_ref();
    let mut unique = Column::new("k".to_string(), DataType::Int);
    unique.constraints.unique = true;
    reference.columns[0] = unique;

    let cache = TableStatsCache::new();
    let (stats, confidence) = cache.stats_for(&reference);

    assert_eq!(confidence, StatsConfidence::HeaderOnly);
    assert_eq!(
        stats.columns[0].distinct, 400,
        "a unique column has one distinct value per row"
    );
    assert!(
        stats.columns[1].distinct < 400,
        "an ordinary column falls back to a sub-linear guess"
    );
}
