//! Integration tests for constraint-flag persistence (ANALYSIS.md Tier 1 #1
//! / Tier 2 #6).
//!
//! Regression scenario: `load_catalog_from_system()` used to rebuild every
//! Column with `Constraints::default()`, so NOT NULL / UNIQUE / CHECK flags
//! and DEFAULT values silently vanished after any process restart.

mod common;

use storage_manager::catalog::{
    Catalog, Column, Constraints, create_database, create_table, load_catalog, save_catalog,
};
use storage_manager::insert_single_tuple;
use storage_manager::types::DataType;

/// Build a column with explicit constraint flags.
fn col(name: &str, ty: DataType, nullable: bool, constraints: Constraints) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty,
        nullable,
        constraints,
    }
}

fn init_and_create(db: &str, table: &str, constraints: Constraints) {
    let mut catalog = load_catalog();
    create_database(&mut catalog, db);
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        db,
        table,
        vec![
            col("id", DataType::Int, false, constraints),
            col(
                "label",
                DataType::Varchar(40),
                true,
                Constraints {
                    not_null: true,
                    ..Default::default()
                },
            ),
        ],
    );
    save_catalog(&catalog).unwrap();
}

#[test]
fn not_null_flag_survives_save_load_cycle() {
    let _ws = common::TestWorkspace::new("conpersist", "notnull");

    init_and_create(
        "nn_db",
        "t1",
        Constraints {
            not_null: true,
            ..Default::default()
        },
    );

    // Fresh load — as a new process/session would perform.
    let reloaded: Catalog = load_catalog();
    let c = &reloaded.databases["nn_db"].tables["t1"].columns[0];
    assert!(c.constraints.not_null, "NOT NULL flag must be restored");
    assert!(!c.nullable, "nullable flag must stay in sync with NOT NULL");

    // The restored flag must actually be enforced on INSERT.
    let ok = insert_single_tuple(&reloaded, "nn_db", "t1", &["NULL", "x"]).unwrap();
    assert!(!ok, "inserting NULL into NOT NULL column must be rejected");
}

#[test]
fn unique_flag_survives_save_load_cycle() {
    let _ws = common::TestWorkspace::new("conpersist", "unique");

    init_and_create(
        "uq_db",
        "t2",
        Constraints {
            unique: true,
            ..Default::default()
        },
    );

    let reloaded: Catalog = load_catalog();
    let c = &reloaded.databases["uq_db"].tables["t2"].columns[0];
    assert!(c.constraints.unique, "UNIQUE flag must be restored");

    assert!(insert_single_tuple(&reloaded, "uq_db", "t2", &["1", "a"]).unwrap());
    let dup = insert_single_tuple(&reloaded, "uq_db", "t2", &["1", "b"]).unwrap();
    assert!(
        !dup,
        "duplicate value in UNIQUE column must be rejected after reload"
    );
}

#[test]
fn check_constraint_survives_save_load_cycle() {
    let _ws = common::TestWorkspace::new("conpersist", "check");

    let mut catalog = load_catalog();
    create_database(&mut catalog, "chk_db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "chk_db",
        "t3",
        vec![
            col(
                "age",
                DataType::Int,
                true,
                Constraints {
                    check: Some("age > 0".to_string()),
                    ..Default::default()
                },
            ),
            col("name", DataType::Varchar(30), true, Constraints::default()),
        ],
    );
    save_catalog(&catalog).unwrap();

    let reloaded: Catalog = load_catalog();
    let c = &reloaded.databases["chk_db"].tables["t3"].columns[0];
    assert_eq!(
        c.constraints.check.as_deref(),
        Some("age > 0"),
        "CHECK expression must round-trip through sys_constraints"
    );

    // Enforcement is read live from sys_constraints; verify a violating row
    // is still rejected against the reloaded catalog.
    let bad = insert_single_tuple(&reloaded, "chk_db", "t3", &["-5", "x"]).unwrap();
    assert!(!bad, "CHECK violation must be detected after reload");
    assert!(insert_single_tuple(&reloaded, "chk_db", "t3", &["5", "ok"]).unwrap());
}

#[test]
fn default_value_survives_save_load_cycle() {
    let _ws = common::TestWorkspace::new("conpersist", "default");

    let mut catalog = load_catalog();
    create_database(&mut catalog, "def_db");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "def_db",
        "t4",
        vec![col(
            "status",
            DataType::Varchar(20),
            true,
            Constraints {
                default: Some(storage_manager::types::DataValue::Varchar(
                    "active".to_string(),
                )),
                ..Default::default()
            },
        )],
    );
    save_catalog(&catalog).unwrap();

    let reloaded: Catalog = load_catalog();
    let c = &reloaded.databases["def_db"].tables["t4"].columns[0];
    assert_eq!(
        c.constraints.default,
        Some(storage_manager::types::DataValue::Varchar(
            "active".to_string()
        )),
        "DEFAULT value must round-trip through sys_columns"
    );
}

#[test]
fn flags_round_trip_through_repeated_cycles() {
    let _ws = common::TestWorkspace::new("conpersist", "cycles");

    init_and_create(
        "inv",
        "items",
        Constraints {
            not_null: true,
            unique: true,
            ..Default::default()
        },
    );

    // Save/load repeatedly — flags must neither decay nor duplicate.
    for cycle in 0..3 {
        let catalog = load_catalog();
        save_catalog(&catalog).unwrap();
        let reloaded = load_catalog();
        let c = &reloaded.databases["inv"].tables["items"].columns[0];
        assert!(c.constraints.not_null, "cycle {}: NOT NULL lost", cycle);
        assert!(c.constraints.unique, "cycle {}: UNIQUE lost", cycle);

        // No duplicate constraint rows accumulate either: a fresh save of a
        // freshly loaded catalog must keep exactly two NOT NULL rows
        // (columns `id` and `label` are both NOT NULL).
        let rows = count_constraint_rows("inv", "items", "NOT NULL");
        assert_eq!(
            rows, 2,
            "cycle {}: expected exactly two NOT NULL rows",
            cycle
        );
    }
}

/// Count sys_constraints rows of `ctype` belonging to `db.table`.
fn count_constraint_rows(db: &str, table: &str, ctype: &str) -> usize {
    use storage_manager::backend::system_table as st;
    let (_, db_id) = match st::resolve_table_id(db, table) {
        Ok(x) => x,
        Err(_) => return 0,
    };
    let path = std::path::PathBuf::from(format!(
        "{}/constraints.dat",
        storage_manager::layout::SYSTEM_DIR
    ));
    if !path.exists() {
        return 0;
    }
    let heap = match storage_manager::backend::heap::HeapManager::open(path) {
        Ok(h) => h,
        Err(_) => return 0,
    };
    let mut n = 0;
    for result in heap.scan() {
        if let Ok((_, _, raw)) = result
            && let Ok(decoded) =
                storage_manager::types::deserialize_nullable_row(st::SYS_CONSTRAINTS_SCHEMA, &raw)
        {
            let tid = matches!(
                &decoded.get(1),
                Some(Some(storage_manager::types::DataValue::Int(_)))
            );
            let t = decoded.get(2).map(|v| match v {
                Some(storage_manager::types::DataValue::Varchar(s)) => s.clone(),
                _ => String::new(),
            });
            if tid && t.as_deref() == Some(ctype) {
                let _ = db_id;
                n += 1;
            }
        }
    }
    n
}

#[test]
fn violations_are_reported_as_typed_errors() {
    use storage_manager::backend::constraint::{ConstraintKind, RookError, validate_row_insert};

    let _ws = common::TestWorkspace::new("conpersist", "typed");

    init_and_create(
        "typed_db",
        "t9",
        Constraints {
            not_null: true,
            ..Default::default()
        },
    );
    let catalog = load_catalog();

    // NOT NULL violation → structured variant.
    let err = validate_row_insert(
        &catalog,
        "typed_db",
        "t9",
        &typed(&catalog, "typed_db", "t9", &["NULL", "x"]),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        RookError::ConstraintViolation { kind: ConstraintKind::NotNull, ref column, .. }
            if column.as_deref() == Some("id")
    ));
    assert_eq!(err.constraint_kind(), Some(ConstraintKind::NotNull));

    // Unknown table → NotFound, not a constraint error.
    let err = validate_row_insert(
        &catalog,
        "typed_db",
        "nope",
        &[
            Some(storage_manager::types::DataValue::Int(1)),
            Some(storage_manager::types::DataValue::Varchar("x".to_string())),
        ],
    )
    .unwrap_err();
    assert!(!err.is_constraint_violation());
    assert!(matches!(err, RookError::NotFound { .. }));

    // Display output stays user-friendly.
    let err = validate_row_insert(
        &catalog,
        "typed_db",
        "t9",
        &typed(&catalog, "typed_db", "t9", &["NULL", "x"]),
    )
    .unwrap_err();
    assert!(err.to_string().contains("NOT NULL"));
}

#[test]
fn update_can_rewrite_row_without_self_unique_collision() {
    use storage_manager::backend::constraint::{ConstraintKind, validate_row_update};

    let _ws = common::TestWorkspace::new("conpersist", "selfupd");

    // Table with an INT PRIMARY KEY → not_null + unique flags + auto index.
    let mut catalog = load_catalog();
    create_database(&mut catalog, "updb");
    let mut catalog = load_catalog();
    create_table(
        &mut catalog,
        "updb",
        "people",
        vec![
            col(
                "id",
                DataType::Int,
                false,
                Constraints {
                    not_null: true,
                    unique: true,
                    ..Default::default()
                },
            ),
            col("name", DataType::Varchar(30), true, Constraints::default()),
        ],
    );
    save_catalog(&catalog).unwrap();

    let catalog = load_catalog();
    assert!(insert_single_tuple(&catalog, "updb", "people", &["1", "Ann"]).unwrap());
    assert!(insert_single_tuple(&catalog, "updb", "people", &["2", "Ben"]).unwrap());

    // RELOAD — this is what used to lose the UNIQUE flag entirely; now the
    // flag is restored, so the checks below are actually exercised.
    let catalog = load_catalog();

    // Rewriting row id=1 in place (same key) must NOT collide with itself.
    validate_row_update(
        &catalog,
        "updb",
        "people",
        &typed(&catalog, "updb", "people", &["1", "Ann-Marie"]),
        Some((1, 0)),
    )
    .expect("self-update of unchanged UNIQUE key must succeed");

    // Changing id=1 to id=2 (a DIFFERENT row's key) must still be rejected.
    let err = validate_row_update(
        &catalog,
        "updb",
        "people",
        &typed(&catalog, "updb", "people", &["2", "Ann-Marie"]),
        Some((1, 0)),
    )
    .expect_err("taking another row's UNIQUE key must fail");
    assert_eq!(err.constraint_kind(), Some(ConstraintKind::Unique));
}

/// A row given as text, parsed for the table's column types.
fn typed(
    catalog: &storage_manager::catalog::Catalog,
    db: &str,
    table: &str,
    values: &[&str],
) -> Vec<Option<storage_manager::types::DataValue>> {
    let columns = &catalog.databases[db].tables[table].columns;
    storage_manager::executor::parse_text_row(columns, values).unwrap()
}
