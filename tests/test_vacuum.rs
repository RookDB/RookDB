//! Integration tests for VACUUM / space reclamation
//! (ANALYSIS.md Tier 2 #7).
//!
//! DELETE only soft-deletes slots; VACUUM must physically reclaim them,
//! keep every surviving row readable at its NEW slot position, rebuild
//! indexes whose stored row pointers were invalidated by renumbering, and
//! reset the dead-tuple counter.

mod common;

use rook_ast::QueryPlan;
use storage_manager::backend::executor::vacuum::vacuum_table;
use storage_manager::catalog::{
    Catalog, Column, create_database, create_table, load_catalog, save_catalog,
};
use storage_manager::insert_single_tuple;
use storage_manager::types::DataType;

/// Parse a WHERE string with the real SQL grammar, select matching rows on
/// the Volcano engine, then delete them by pointer.
fn exec_delete(
    catalog: &Catalog,
    db: &str,
    table: &str,
    where_text: &str,
) -> storage_manager::backend::executor::DeleteResult {
    let selection = storage_manager::backend::executor::row_select::parse_where_text(where_text)
        .expect("parse WHERE");
    let pointers = storage_manager::backend::executor::row_select::select_matching_pointers(
        catalog, db, table, selection,
    )
    .expect("select pointers");
    storage_manager::executor::delete_by_pointers(catalog, db, table, &pointers)
        .expect("delete_by_pointers")
}

fn col(name: &str, ty: DataType) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty,
        nullable: true,
        constraints: Default::default(),
    }
}

fn setup(db: &str) -> Catalog {
    let mut catalog = load_catalog();
    create_database(&mut catalog, db);
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        db,
        "events",
        vec![
            col("id", DataType::Int),
            col("label", DataType::Varchar(60)),
        ],
    );
    save_catalog(&catalog).unwrap();
    load_catalog()
}

/// Read the raw dead_tuple_count header field.
fn dead_count(db: &str, table: &str) -> u32 {
    use std::fs::OpenOptions;
    let path = format!("database/base/{}/{}.dat", db, table);
    // The counter is kept in the pooled header page; write it out first.
    storage_manager::backend::cache::checkpoint();
    let mut f = OpenOptions::new().read(true).open(&path).unwrap();
    storage_manager::backend::table::read_dead_tuple_count(&mut f).unwrap()
}

#[test]
fn parser_accepts_vacuum_statement() {
    match rook_parser::parse_sql("VACUUM events;") {
        Ok(QueryPlan::Vacuum(p)) => assert_eq!(p.table, "events"),
        other => panic!(
            "expected Vacuum plan, got {:?}",
            other.map(|p| p.statement_type().to_string())
        ),
    }
    match rook_parser::parse_sql("VACUUM TABLE my_t") {
        Ok(QueryPlan::Vacuum(p)) => assert_eq!(p.table, "my_t"),
        other => panic!(
            "expected Vacuum plan, got {:?}",
            other.map(|p| p.statement_type().to_string())
        ),
    }
    assert!(rook_parser::parse_sql("VACUUM ../evil").is_err());
    assert!(rook_parser::parse_sql("VACUUM").is_err());
}

#[test]
fn vacuum_reclaims_dead_tuples_and_preserves_live_rows() {
    let _ws = common::TestWorkspace::new("vacuum", "reclaim");
    let catalog = setup("vdb");

    // Insert 40 rows.
    for i in 0..40 {
        insert_single_tuple(
            &catalog,
            "vdb",
            "events",
            &[&i.to_string(), &format!("row{}", i)],
        )
        .unwrap();
    }

    // Delete ids 0..10 and 20..30.
    let result = exec_delete(
        &catalog,
        "vdb",
        "events",
        "id >= 0 AND id < 10 OR id >= 20 AND id < 30",
    );
    assert_eq!(result.deleted_count, 20);

    assert!(
        dead_count("vdb", "events") >= 20,
        "dead counter must track deletes"
    );

    // VACUUM.
    let stats = vacuum_table(&catalog, "vdb", "events").expect("vacuum failed");
    assert_eq!(stats.pages_compacted, 1);
    assert!(stats.dead_tuples_before >= 20);
    assert_eq!(dead_count("vdb", "events"), 0, "counter must reset");

    // Exactly the odd rows survive, readable via fresh heap scans.
    let heap = storage_manager::heap::HeapManager::open(std::path::PathBuf::from(
        "database/base/vdb/events.dat",
    ))
    .unwrap();
    let schema = vec![DataType::Int, DataType::Varchar(60)];
    let mut live_ids = Vec::new();
    for r in heap.scan() {
        if let Ok((_, _, raw)) = r
            && let Ok(row) = storage_manager::types::deserialize_nullable_row(&schema, &raw)
            && let Some(Some(storage_manager::types::DataValue::Int(id))) = row.first()
        {
            live_ids.push(*id);
        }
    }
    live_ids.sort();
    let expected: Vec<i32> = (0..40)
        .filter(|i| !((0..10).contains(i) || (20..30).contains(i)))
        .collect();
    assert_eq!(live_ids, expected, "only live rows must survive VACUUM");
}

#[test]
fn vacuum_rebuilds_indexes_for_renumbered_slots() {
    let _ws = common::TestWorkspace::new("vacuum", "indexrb");
    let catalog = setup("idb");

    for i in 0..30 {
        insert_single_tuple(
            &catalog,
            "idb",
            "events",
            &[&i.to_string(), &format!("row{}", i)],
        )
        .unwrap();
    }

    // Build an index over `id`.
    let n = storage_manager::executor::create_index::create_index(
        &catalog,
        "idb",
        "events",
        "idx_events_id",
        &["id".to_string()],
    )
    .expect("create_index failed");
    assert_eq!(n, 30);

    // Delete a contiguous middle chunk so surviving slots renumber.
    let result = exec_delete(&catalog, "idb", "events", "id >= 10 AND id < 20");
    assert_eq!(result.deleted_count, 10);

    let stats = vacuum_table(&catalog, "idb", "events").unwrap();
    assert_eq!(stats.indexes_rebuilt, 1, "the index must be rebuilt");

    // The rebuilt index must resolve every surviving id to a LIVE row with
    // matching contents.
    let idx_path = std::path::PathBuf::from("database/base/idb/events.idx_events_id.idx");
    let mut btree = storage_manager::backend::index::btree::BTree::open(idx_path).unwrap();
    btree.set_key_type(DataType::Int);

    let mut heap = storage_manager::heap::HeapManager::open(std::path::PathBuf::from(
        "database/base/idb/events.dat",
    ))
    .unwrap();
    for id in [0i32, 5, 9, 20, 25, 29] {
        let tid = btree
            .search(&storage_manager::types::DataValue::Int(id))
            .expect("btree search")
            .unwrap_or_else(|| panic!("index missing id {}", id));
        let raw = heap.get_tuple(tid.0, tid.1).unwrap();
        let row = storage_manager::types::deserialize_nullable_row(
            &[DataType::Int, DataType::Varchar(60)],
            &raw,
        )
        .unwrap();
        match &row[0] {
            Some(storage_manager::types::DataValue::Int(v)) => {
                assert_eq!(*v, id, "index pointer must land on the row with id {}", id);
            }
            other => panic!("unexpected row {:?} for id {}", other, id),
        }
    }

    // Deleted ids must NOT resolve through the rebuilt index.
    for id in [10i32, 15, 19] {
        assert!(
            btree
                .search(&storage_manager::types::DataValue::Int(id))
                .unwrap()
                .is_none(),
            "deleted id {} must not be in the rebuilt index",
            id
        );
    }
}

#[test]
fn vacuum_enables_space_reuse_without_page_growth() {
    let _ws = common::TestWorkspace::new("vacuum", "reuse");
    let catalog = setup("sdb");

    for i in 0..25 {
        insert_single_tuple(
            &catalog,
            "sdb",
            "events",
            &[&i.to_string(), &format!("row{}", i)],
        )
        .unwrap();
    }

    fn page_count_of(db: &str, t: &str) -> u32 {
        use std::fs::OpenOptions;
        let mut f = OpenOptions::new()
            .read(true)
            .open(format!("database/base/{}/{}.dat", db, t))
            .unwrap();
        storage_manager::backend::table::page_count(&mut f).unwrap()
    }

    // Delete everything.
    let result = exec_delete(&catalog, "sdb", "events", "id >= 0");
    assert_eq!(result.deleted_count, 25);

    vacuum_table(&catalog, "sdb", "events").unwrap();
    let pages_before = page_count_of("sdb", "events");

    // Refill the same number of rows: pages must not grow.
    let fresh_catalog = load_catalog();
    for i in 100..125 {
        insert_single_tuple(
            &fresh_catalog,
            "sdb",
            "events",
            &[&i.to_string(), &format!("n{}", i)],
        )
        .unwrap();
    }
    let pages_after = page_count_of("sdb", "events");
    assert!(
        pages_after <= pages_before,
        "VACUUM must enable space reuse: {} → {} pages",
        pages_before,
        pages_after
    );
}

#[test]
fn vacuum_is_safe_on_clean_and_missing_tables() {
    let _ws = common::TestWorkspace::new("vacuum", "safe");
    let catalog = setup("cdb");

    // Clean table → zero pages compacted.
    insert_single_tuple(&catalog, "cdb", "events", &["1", "only"]).unwrap();
    let stats = vacuum_table(&catalog, "cdb", "events").unwrap();
    assert_eq!(stats.pages_compacted, 0);
    assert_eq!(stats.dead_tuples_before, 0);

    // Unknown table → error, not a crash.
    assert!(vacuum_table(&catalog, "cdb", "nope").is_err());
    assert!(vacuum_table(&catalog, "ghost_db", "events").is_err());
}

#[test]
fn vacuum_reclaims_space_left_by_reused_slots() {
    let _ws = common::TestWorkspace::new("vacuum", "reused");
    let catalog = setup("rdb");

    for i in 0..10 {
        insert_single_tuple(&catalog, "rdb", "events", &[&i.to_string(), "old_row"]).unwrap();
    }
    assert_eq!(
        exec_delete(&catalog, "rdb", "events", "id < 3").deleted_count,
        3
    );
    // These inserts take over the three deleted slots, so no slot is left
    // flagged as deleted, but the deleted rows' bytes are still on the page.
    for i in 10..13 {
        insert_single_tuple(&catalog, "rdb", "events", &[&i.to_string(), "new_row"]).unwrap();
    }

    let path = std::path::PathBuf::from("database/base/rdb/events.dat");
    let free_before = storage_manager::statistics::collect_heap_statistics(
        &storage_manager::backend::engine::Engine::current_dir(),
        &path,
    )
    .unwrap()
    .total_free_bytes;

    let stats = vacuum_table(&catalog, "rdb", "events").expect("vacuum failed");
    assert_eq!(
        stats.pages_compacted, 1,
        "the page with stale bytes must be compacted"
    );

    let after = storage_manager::statistics::collect_heap_statistics(
        &storage_manager::backend::engine::Engine::current_dir(),
        &path,
    )
    .unwrap();
    assert!(
        after.total_free_bytes > free_before,
        "free space must grow: {} -> {}",
        free_before,
        after.total_free_bytes
    );
    assert_eq!(after.total_tuple_count, 10);
}
