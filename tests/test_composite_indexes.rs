//! Integration tests for composite (multi-column) indexes
//! (ANALYSIS.md Tier 2 #8).
//!
//! Coverage: CREATE INDEX with multiple columns builds a composite-key
//! B+ Tree; equality on ALL key columns uses the index for point lookups;
//! DML keeps the index coherent; the index survives a catalog round-trip.

mod common;

use common::TestWorkspace;
use common::load_catalog;
use rook_ast::QueryPlan;
use storage_manager::backend::executor::physical::engine::execute_plan_collect;
use storage_manager::backend::executor::physical::tuple::Tuple;
use storage_manager::catalog::{Catalog, Column, create_database, create_table, save_catalog};
use storage_manager::insert_single_tuple;
use storage_manager::types::DataType;

fn col(name: &str, ty: DataType) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty,
        nullable: true,
        constraints: Default::default(),
    }
}

/// Parse a WHERE string with the real SQL grammar, select matching rows on
/// the Volcano engine, then delete them by pointer.
fn exec_delete(
    catalog: &Catalog,
    db: &str,
    table: &str,
    where_text: &str,
) -> storage_manager::executor::DeleteResult {
    let selection = storage_manager::backend::executor::row_select::parse_where_text(where_text)
        .expect("parse WHERE");
    let pointers = storage_manager::backend::executor::row_select::select_matching_pointers(
        catalog, db, table, selection,
    )
    .expect("select pointers");
    storage_manager::executor::delete_by_pointers(catalog, db, table, &pointers)
        .expect("delete_by_pointers")
}
fn setup(db: &str) -> Catalog {
    let mut catalog = load_catalog();
    create_database(&mut catalog, db);
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        db,
        "orders",
        vec![
            col("cust_id", DataType::Int),
            col("region", DataType::Varchar(30)),
            col("amount", DataType::Int),
            col("memo", DataType::Varchar(60)),
        ],
    );
    save_catalog(&catalog).unwrap();
    load_catalog()
}

fn run_select(catalog: &Catalog, db: &str, sql: &str) -> Vec<Vec<String>> {
    let select = match rook_parser::parse_sql(sql) {
        Ok(QueryPlan::Select(select)) => select,
        other => panic!(
            "expected Select plan, got {:?}",
            other.map(|p| p.statement_type().to_string())
        ),
    };
    let logical = storage_manager::planner::plan_query(&QueryPlan::Select(select), catalog, db)
        .expect("logical planning failed");
    let tuples: Vec<Tuple> =
        execute_plan_collect(&logical, catalog, db).expect("physical execution failed");
    tuples
        .iter()
        .map(|t| {
            t.values
                .iter()
                .map(|v| match v {
                    Some(dv) => format!("{}", dv),
                    None => "NULL".to_string(),
                })
                .collect()
        })
        .collect()
}

#[test]
fn btree_composite_keys_sort_and_lookup() {
    use storage_manager::backend::index::btree::BTree;
    use storage_manager::types::{DataValue, OrderedF64};

    let path = std::env::temp_dir().join(format!(
        "rookdb_comp_p{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    let idx_path = path.join("comp.idx");

    let mut tree =
        BTree::create_composite(idx_path.clone(), vec![DataType::Int, DataType::Varchar(20)])
            .unwrap();

    // Insert (int, varchar) pairs — deliberately out of order.
    let rows: Vec<(i32, &str, u32)> = vec![
        (5, "b", 1),
        (1, "z", 2),
        (5, "a", 3),
        (2, "m", 4),
        (1, "a", 5),
        (3, "x", 6),
    ];
    for (i, tag, slot) in &rows {
        tree.insert_keys(
            &[DataValue::Int(*i), DataValue::Varchar(tag.to_string())],
            9,
            *slot,
        )
        .unwrap();
    }
    tree.sync().unwrap();

    // Exact composite lookup finds only the matching entry.
    let hit = tree
        .search_keys(&[DataValue::Int(5), DataValue::Varchar("a".into())])
        .unwrap();
    assert_eq!(hit, Some((9, 3)));

    // Wrong second component → miss even though first matches.
    assert!(
        tree.search_keys(&[DataValue::Int(5), DataValue::Varchar("q".into())])
            .unwrap()
            .is_none()
    );

    // scan_all returns entries in composite key order: (1,a),(1,z),(2,m),(3,x),(5,a),(5,b)
    let tids = tree.scan_all().unwrap();
    assert_eq!(tids.len(), 6);
    let order: Vec<u32> = tids.into_iter().map(|(_, s)| s).collect();
    // slots sorted by composite key: (1,a)=5, (1,z)=2, (2,m)=4, (3,x)=6, (5,a)=3, (5,b)=1
    assert_eq!(order, vec![5, 2, 4, 6, 3, 1]);

    // Persistence across reopen (types must be re-set by caller).
    drop(tree);
    let mut reopened = BTree::open(idx_path).unwrap();
    reopened.set_key_types(vec![DataType::Int, DataType::Varchar(20)]);
    assert_eq!(
        reopened
            .search_keys(&[DataValue::Int(1), DataValue::Varchar("z".into())])
            .unwrap(),
        Some((9, 2))
    );

    std::fs::remove_dir_all(&path).ok();
    let _ = OrderedF64(0.0); // keep import used if assertions change
}

#[test]
fn create_and_query_composite_index() {
    let _ws = TestWorkspace::new("compidx", "create");
    let mut catalog = setup("cdb");

    for row in [
        ["1", "west", "100", ""],
        ["1", "east", "200", ""],
        ["2", "west", "300", ""],
        ["2", "east", "400", ""],
        ["3", "west", "500", ""],
    ] {
        insert_single_tuple(&catalog, "cdb", "orders", &row).unwrap();
    }

    let n = storage_manager::executor::create_index::create_index(
        &catalog,
        "cdb",
        "orders",
        "idx_cust_region",
        &["cust_id".to_string(), "region".to_string()],
    )
    .expect("composite create_index failed");
    assert_eq!(n, 5);

    // Full equality across both key columns → exact single-row lookup.
    catalog = load_catalog();
    let out = run_select(
        &catalog,
        "cdb",
        "SELECT amount FROM orders WHERE cust_id = 2 AND region = 'west'",
    );
    assert_eq!(out, vec![vec!["300"]]);

    // Equality on only ONE key column cannot use the composite index —
    // results must still be CORRECT via the seq-scan fallback.
    let out = run_select(
        &catalog,
        "cdb",
        "SELECT amount FROM orders WHERE cust_id = 1 ORDER BY amount",
    );
    assert_eq!(out, vec![vec!["100"], vec!["200"]]);
}

#[test]
fn composite_index_tracks_dml() {
    let _ws = TestWorkspace::new("compidx", "dml");
    let catalog = setup("ddb");

    for row in [
        ["1", "a", "10", ""],
        ["1", "b", "20", ""],
        ["2", "c", "30", ""],
    ] {
        insert_single_tuple(&catalog, "ddb", "orders", &row).unwrap();
    }
    storage_manager::executor::create_index::create_index(
        &catalog,
        "ddb",
        "orders",
        "idx_cr",
        &["cust_id".to_string(), "region".to_string()],
    )
    .unwrap();

    // INSERT after creation must be indexed.
    insert_single_tuple(&load_catalog(), "ddb", "orders", &["1", "c", "40", ""]).unwrap();

    // DELETE removes the entry from the index too.
    let result = exec_delete(
        &load_catalog(),
        "ddb",
        "orders",
        "cust_id = 1 AND region = 'b'",
    );
    assert_eq!(result.deleted_count, 1);

    let catalog = load_catalog();
    // The deleted combination is gone; the inserted one is present.
    assert!(
        run_select(
            &catalog,
            "ddb",
            "SELECT amount FROM orders WHERE cust_id = 1 AND region = 'b'"
        )
        .is_empty()
    );
    assert_eq!(
        run_select(
            &catalog,
            "ddb",
            "SELECT amount FROM orders WHERE cust_id = 1 AND region = 'c'"
        ),
        vec![vec!["40"]]
    );

    // Direct index inspection: exactly the surviving combos resolve.
    let idx_path = common::path("database/base/ddb/orders.idx_cr.idx");
    let mut btree = storage_manager::backend::index::btree::BTree::open(idx_path).unwrap();
    btree.set_key_types(vec![DataType::Int, DataType::Varchar(30)]);
    for (c, r, should_exist) in [
        (1, "a", true),
        (1, "b", false),
        (2, "c", true),
        (1, "c", true),
    ] {
        let hit = btree
            .contains_keys(&[
                storage_manager::types::DataValue::Int(c),
                storage_manager::types::DataValue::Varchar(r.to_string()),
            ])
            .unwrap();
        assert_eq!(hit, should_exist, "combo ({},{}) existence mismatch", c, r);
    }
}

#[test]
fn composite_index_survives_vacuum_rebuild() {
    let _ws = TestWorkspace::new("compidx", "vacuum");
    let catalog = setup("vdb");

    for i in 0..12 {
        assert!(
            insert_single_tuple(
                &catalog,
                "vdb",
                "orders",
                &[
                    &(i % 3).to_string(),
                    &format!("r{}", i % 2),
                    &(i * 10).to_string(),
                    ""
                ],
            )
            .unwrap(),
            "insert of row {} failed",
            i
        );
    }
    storage_manager::executor::create_index::create_index(
        &catalog,
        "vdb",
        "orders",
        "idx_cv",
        &["cust_id".to_string(), "region".to_string()],
    )
    .unwrap();

    // Delete a slice to force compaction + rebuild.
    exec_delete(&catalog, "vdb", "orders", "amount < 60");

    let stats = storage_manager::backend::executor::vacuum::vacuum_table(&catalog, "vdb", "orders")
        .expect("vacuum");
    assert_eq!(stats.indexes_rebuilt, 1);

    // Post-rebuild: queries over the composite index stay exact.
    // Surviving rows are i=6..11; only i=6 has cust_id=0 AND region='r0'.
    let catalog = load_catalog();
    assert_eq!(
        run_select(
            &catalog,
            "vdb",
            "SELECT amount FROM orders WHERE cust_id = 0 AND region = 'r0'"
        ),
        vec![vec!["60"]]
    );
    assert_eq!(
        run_select(
            &catalog,
            "vdb",
            "SELECT amount FROM orders WHERE cust_id = 1 AND region = 'r0'"
        ),
        vec![vec!["100"]]
    );
}
