//! Randomized model test for the B+ tree.
//!
//! Runs long random sequences of inserts and deletes against a
//! `BTreeMap<key, Vec<tid>>` and checks every read path against it: point
//! lookups, all-duplicates lookups, range scans and the full scan. The key
//! sets are chosen so duplicates of one key span several leaves and
//! variable-length keys force byte-balanced splits.

use std::collections::BTreeMap;
use std::path::PathBuf;

use storage_manager::backend::index::BTree;
use storage_manager::backend::index::btree::MAX_ENCODED_KEY_SIZE;
use storage_manager::types::DataType;
use storage_manager::types::value::DataValue;

type Tid = (u32, u32);

fn lcg(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state >> 33
}

fn index_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "rookdb_btree_model_{}_{}.idx",
        std::process::id(),
        name
    ))
}

fn remove_index(path: &PathBuf) {
    std::fs::remove_file(path).ok();
    std::fs::remove_file(format!("{}.root", path.display())).ok();
}

/// Compare every read path of `tree` with `model` for all keys in `domain`.
fn check<K: Ord + Clone + std::fmt::Debug>(
    tree: &mut BTree,
    model: &BTreeMap<K, Vec<Tid>>,
    domain: &[K],
    values: &impl Fn(&K) -> Vec<DataValue>,
    seed: &mut u64,
) {
    let all: Vec<Tid> = model.values().flatten().copied().collect();
    assert_eq!(tree.scan_all().unwrap(), all, "full scan");

    for key in domain {
        let expected = model.get(key).cloned().unwrap_or_default();
        let probe = values(key);
        // Duplicates come back in insertion order, from every leaf they span.
        assert_eq!(tree.search_all_keys(&probe).unwrap(), expected, "{:?}", key);
        assert_eq!(
            tree.search_keys(&probe).unwrap(),
            expected.first().copied(),
            "{:?}",
            key
        );
        assert_eq!(
            tree.search_range_keys(&probe, &probe).unwrap(),
            expected,
            "{:?}",
            key
        );
    }

    for _ in 0..12 {
        let a = &domain[lcg(seed) as usize % domain.len()];
        let b = &domain[lcg(seed) as usize % domain.len()];
        let expected: Vec<Tid> = if a <= b {
            model
                .range(a.clone()..=b.clone())
                .flat_map(|(_, tids)| tids.iter().copied())
                .collect()
        } else {
            Vec::new() // an inverted range is empty
        };
        assert_eq!(
            tree.search_range_keys(&values(a), &values(b)).unwrap(),
            expected,
            "range {:?}..={:?}",
            a,
            b
        );
    }
}

/// Apply `ops` random operations: `insert_percent` of them inserts, the rest
/// deletes (of a live entry when there is one, plus one that must miss).
fn run_model<K: Ord + Clone + std::fmt::Debug>(
    name: &str,
    key_types: Vec<DataType>,
    domain: Vec<K>,
    values: impl Fn(&K) -> Vec<DataValue>,
    ops: usize,
    insert_percent: u64,
) {
    let path = index_path(name);
    let mut tree = BTree::create_composite(path.clone(), key_types.clone()).unwrap();
    let mut model: BTreeMap<K, Vec<Tid>> = BTreeMap::new();
    // Every live entry, for picking a random one to delete.
    let mut live: Vec<(usize, Tid)> = Vec::new();
    let mut seed = 0x00C0_FFEE ^ ops as u64;
    let mut next_tid = 0u32;

    for step in 0..ops {
        // Phases: mostly inserts, then mostly deletes, then inserts again, so
        // leaves fill, drain and refill.
        let insert_percent = match (step * 3) / ops {
            1 => 100 - insert_percent,
            _ => insert_percent,
        };
        if live.is_empty() || lcg(&mut seed) % 100 < insert_percent {
            let k = lcg(&mut seed) as usize % domain.len();
            let tid = (next_tid / 50 + 1, next_tid % 50);
            next_tid += 1;
            tree.insert_keys(&values(&domain[k]), tid.0, tid.1).unwrap();
            model.entry(domain[k].clone()).or_default().push(tid);
            live.push((k, tid));
        } else {
            let (k, tid) = live.swap_remove(lcg(&mut seed) as usize % live.len());
            let key = values(&domain[k]);
            assert!(tree.delete_keys(&key, tid.0, tid.1).unwrap());
            // The entry is gone: the same delete finds nothing.
            assert!(!tree.delete_keys(&key, tid.0, tid.1).unwrap());
            let tids = model.get_mut(&domain[k]).unwrap();
            tids.retain(|t| *t != tid);
            if tids.is_empty() {
                model.remove(&domain[k]);
            }
        }

        if step % (ops / 8) == ops / 8 - 1 {
            check(&mut tree, &model, &domain, &values, &mut seed);
        }
        // In-place edits must reach the file: reopen from disk twice.
        if step == ops / 2 || step == ops - 1 {
            drop(tree);
            tree = BTree::open(path.clone()).unwrap();
            tree.set_key_types(key_types.clone());
            check(&mut tree, &model, &domain, &values, &mut seed);
        }
    }
    assert!(tree.total_pages() > 3, "{} must exercise splits", name);

    // Drain everything that is left.
    for (k, tid) in live {
        assert!(tree.delete_keys(&values(&domain[k]), tid.0, tid.1).unwrap());
    }
    assert!(tree.scan_all().unwrap().is_empty());
    for key in &domain {
        assert_eq!(tree.search_keys(&values(key)).unwrap(), None);
    }

    drop(tree);
    remove_index(&path);
}

#[test]
fn model_int_keys_with_heavy_duplicates() {
    // 7 distinct keys: each ends up with far more entries than one leaf holds.
    let domain: Vec<i32> = vec![-40, -1, 0, 1, 7, 1000, i32::MAX];
    run_model(
        "int_dups",
        vec![DataType::Int],
        domain,
        |k| vec![DataValue::Int(*k)],
        12_000,
        75,
    );
}

#[test]
fn model_int_keys_mostly_unique() {
    let domain: Vec<i32> = (0..4000).map(|i| i * 3 - 6000).collect();
    run_model(
        "int_unique",
        vec![DataType::Int],
        domain,
        |k| vec![DataValue::Int(*k)],
        9_000,
        70,
    );
}

#[test]
fn model_bigint_keys_with_duplicates() {
    let domain: Vec<i64> = (0..300).map(|i| (i - 150) * 5_000_000_000).collect();
    run_model(
        "bigint",
        vec![DataType::BigInt],
        domain,
        |k| vec![DataValue::BigInt(*k)],
        9_000,
        70,
    );
}

/// Strings of very different lengths, a few close to the key size limit.
fn varchar_domain() -> Vec<String> {
    let mut seed = 0xFEED_u64;
    let mut domain: Vec<String> = (0..160)
        .map(|i| {
            let len = match i % 10 {
                0 => 900 + lcg(&mut seed) as usize % 1700,
                1..=3 => 60 + lcg(&mut seed) as usize % 300,
                _ => lcg(&mut seed) as usize % 24,
            };
            let fill = (b'a' + (lcg(&mut seed) % 26) as u8) as char;
            format!("{:03}{}", (i * 37) % 160, fill.to_string().repeat(len))
        })
        .collect();
    // The longest key the index accepts.
    domain.push("z".repeat(MAX_ENCODED_KEY_SIZE - 2));
    domain.push(String::new());
    domain
}

#[test]
fn model_varchar_keys_of_variable_length_with_duplicates() {
    run_model(
        "varchar",
        vec![DataType::Varchar(3000)],
        varchar_domain(),
        |k| vec![DataValue::Varchar(k.clone())],
        6_000,
        70,
    );
}

#[test]
fn model_composite_keys_with_duplicates() {
    let strings = varchar_domain();
    let domain: Vec<(i32, String)> = (0..120)
        .map(|i| (i % 9 - 4, strings[(i * 7) as usize % 40 + 4].clone()))
        .collect();
    run_model(
        "composite",
        vec![DataType::Int, DataType::Varchar(3000)],
        domain,
        |(n, s)| vec![DataValue::Int(*n), DataValue::Varchar(s.clone())],
        6_000,
        70,
    );
}

#[test]
fn oversized_key_is_rejected_and_changes_nothing() {
    let path = index_path("oversized");
    let mut tree = BTree::create(path.clone(), DataType::Varchar(4000)).unwrap();
    tree.insert(&DataValue::Varchar("a".into()), 1, 1).unwrap();

    let too_long = DataValue::Varchar("x".repeat(MAX_ENCODED_KEY_SIZE - 1));
    assert!(tree.insert(&too_long, 1, 2).is_err());
    assert_eq!(tree.scan_all().unwrap(), vec![(1, 1)]);
    assert_eq!(tree.search(&too_long).unwrap(), None);

    drop(tree);
    remove_index(&path);
}
