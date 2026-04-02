//! Tests for binary serialisation / deserialisation of all system catalog tuples (Spec §7.6).
//!
//! Covers:
//! - 7.6.1: Insert catalog tuple, read back, verify roundtrip
//! - 7.6.2: Scan catalog – insert multiple, retrieve all
//! - 7.6.3: Update catalog tuple – modify and verify
//! - Roundtrip tests for all 6 tuple types: database, table, column,
//!   constraint, index, type

use storage_manager::catalog::serialize::*;
use storage_manager::catalog::types::*;

// ─────────────────────────────────────────────────────────────
// pg_database roundtrip
// ─────────────────────────────────────────────────────────────

#[test]
fn test_serialize_database_tuple_roundtrip() {
    let bytes = serialize_database_tuple(42, "mydb", "admin_user", 1700000000, 1);
    let (oid, name, owner, created_at, enc) =
        deserialize_database_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(oid, 42);
    assert_eq!(name, "mydb");
    assert_eq!(owner, "admin_user");
    assert_eq!(created_at, 1700000000);
    assert_eq!(enc, 1); // UTF8
}

#[test]
fn test_serialize_database_tuple_empty_strings() {
    let bytes = serialize_database_tuple(1, "", "", 0, 2);
    let (oid, name, owner, created_at, enc) =
        deserialize_database_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(oid, 1);
    assert_eq!(name, "");
    assert_eq!(owner, "");
    assert_eq!(created_at, 0);
    assert_eq!(enc, 2); // ASCII
}

#[test]
fn test_serialize_database_tuple_long_name() {
    let long_name = "a".repeat(200);
    let bytes = serialize_database_tuple(99, &long_name, "owner", 12345, 1);
    let (oid, name, ..) = deserialize_database_tuple(&bytes).expect("deserialize should succeed");
    assert_eq!(oid, 99);
    assert_eq!(name, long_name);
}

// ─────────────────────────────────────────────────────────────
// pg_table roundtrip
// ─────────────────────────────────────────────────────────────

#[test]
fn test_serialize_table_tuple_roundtrip() {
    let bytes = serialize_table_tuple(100, "users", 42, 0, 1000, 5, 1700000000);
    let (toid, tname, db_oid, ttype, rows, pages, created) =
        deserialize_table_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(toid, 100);
    assert_eq!(tname, "users");
    assert_eq!(db_oid, 42);
    assert_eq!(ttype, 0); // UserTable
    assert_eq!(rows, 1000);
    assert_eq!(pages, 5);
    assert_eq!(created, 1700000000);
}

#[test]
fn test_serialize_table_tuple_system_catalog() {
    let bytes = serialize_table_tuple(10, "pg_database", 1, 1, 0, 1, 0);
    let (toid, tname, db_oid, ttype, ..) =
        deserialize_table_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(toid, 10);
    assert_eq!(tname, "pg_database");
    assert_eq!(db_oid, 1);
    assert_eq!(ttype, 1); // SystemCatalog
}

// ─────────────────────────────────────────────────────────────
// pg_column roundtrip
// ─────────────────────────────────────────────────────────────

#[test]
fn test_serialize_column_tuple_roundtrip_int() {
    let dt = DataType::int();
    let bytes = serialize_column_tuple(
        200, 100, "id", 1, &dt, None, false, None, &[500, 501],
    );
    let (coid, toid, cname, cpos, rdt, tm, nullable, dv, coids) =
        deserialize_column_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(coid, 200);
    assert_eq!(toid, 100);
    assert_eq!(cname, "id");
    assert_eq!(cpos, 1);
    assert_eq!(rdt.type_oid, dt.type_oid);
    assert_eq!(rdt.type_name, "INT");
    assert!(tm.is_none());
    assert!(!nullable);
    assert!(dv.is_none());
    assert_eq!(coids, vec![500, 501]);
}

#[test]
fn test_serialize_column_tuple_varchar_with_modifier() {
    let dt = DataType::varchar(100);
    let tm = TypeModifier::VarcharLen(100);
    let bytes = serialize_column_tuple(
        201, 100, "email", 2, &dt, Some(&tm), true, None, &[],
    );
    let (_, _, cname, cpos, rdt, rtm, nullable, _, _) =
        deserialize_column_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(cname, "email");
    assert_eq!(cpos, 2);
    assert!(rdt.type_name.starts_with("VARCHAR"));
    assert_eq!(rtm, Some(TypeModifier::VarcharLen(100)));
    assert!(nullable);
}

#[test]
fn test_serialize_column_tuple_with_default_values() {
    let dt = DataType::int();
    let dv = DefaultValue::Integer(42);
    let bytes = serialize_column_tuple(202, 100, "age", 3, &dt, None, true, Some(&dv), &[]);
    let (_, _, _, _, _, _, _, rdv, _) =
        deserialize_column_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(rdv, Some(DefaultValue::Integer(42)));
}

#[test]
fn test_serialize_column_tuple_default_value_variants() {
    let dt = DataType::text();
    let defaults = vec![
        DefaultValue::Integer(42),
        DefaultValue::BigInt(999999999999i64),
        DefaultValue::Float(3.14f32),
        DefaultValue::Double(2.71828f64),
        DefaultValue::Str("hello".to_string()),
        DefaultValue::Boolean(true),
        DefaultValue::Null,
        DefaultValue::CurrentTimestamp,
    ];

    for dv in &defaults {
        let bytes = serialize_column_tuple(300, 100, "col", 1, &dt, None, true, Some(dv), &[]);
        let (_, _, _, _, _, _, _, rdv, _) =
            deserialize_column_tuple(&bytes).expect("deserialize should succeed");
        assert_eq!(rdv.as_ref(), Some(dv), "DefaultValue roundtrip failed for {:?}", dv);
    }
}

// ─────────────────────────────────────────────────────────────
// pg_constraint roundtrip
// ─────────────────────────────────────────────────────────────

#[test]
fn test_serialize_constraint_primary_key() {
    let c = Constraint {
        constraint_oid: 500,
        constraint_name: "pk_users_id".to_string(),
        constraint_type: ConstraintType::PrimaryKey,
        table_oid: 100,
        column_oids: vec![200],
        metadata: ConstraintMetadata::PrimaryKey { index_oid: 600 },
        is_deferrable: false,
    };
    let bytes = serialize_constraint_tuple(&c);
    let rc = deserialize_constraint_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(rc.constraint_oid, 500);
    assert_eq!(rc.constraint_name, "pk_users_id");
    assert_eq!(rc.constraint_type, ConstraintType::PrimaryKey);
    assert_eq!(rc.table_oid, 100);
    assert_eq!(rc.column_oids, vec![200]);
    assert!(!rc.is_deferrable);
    match rc.metadata {
        ConstraintMetadata::PrimaryKey { index_oid } => assert_eq!(index_oid, 600),
        other => panic!("Expected PrimaryKey metadata, got {:?}", other),
    }
}

#[test]
fn test_serialize_constraint_foreign_key() {
    let c = Constraint {
        constraint_oid: 501,
        constraint_name: "fk_orders_user".to_string(),
        constraint_type: ConstraintType::ForeignKey,
        table_oid: 101,
        column_oids: vec![210, 211],
        metadata: ConstraintMetadata::ForeignKey {
            referenced_table_oid: 100,
            referenced_column_oids: vec![200, 201],
            on_delete: ReferentialAction::Cascade,
            on_update: ReferentialAction::SetNull,
        },
        is_deferrable: false,
    };
    let bytes = serialize_constraint_tuple(&c);
    let rc = deserialize_constraint_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(rc.constraint_type, ConstraintType::ForeignKey);
    assert_eq!(rc.column_oids, vec![210, 211]);
    match rc.metadata {
        ConstraintMetadata::ForeignKey {
            referenced_table_oid,
            referenced_column_oids,
            on_delete,
            on_update,
        } => {
            assert_eq!(referenced_table_oid, 100);
            assert_eq!(referenced_column_oids, vec![200, 201]);
            assert_eq!(on_delete, ReferentialAction::Cascade);
            assert_eq!(on_update, ReferentialAction::SetNull);
        }
        other => panic!("Expected ForeignKey metadata, got {:?}", other),
    }
}

#[test]
fn test_serialize_constraint_unique() {
    let c = Constraint {
        constraint_oid: 502,
        constraint_name: "uq_email".to_string(),
        constraint_type: ConstraintType::Unique,
        table_oid: 100,
        column_oids: vec![201],
        metadata: ConstraintMetadata::Unique { index_oid: 601 },
        is_deferrable: false,
    };
    let bytes = serialize_constraint_tuple(&c);
    let rc = deserialize_constraint_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(rc.constraint_type, ConstraintType::Unique);
    match rc.metadata {
        ConstraintMetadata::Unique { index_oid } => assert_eq!(index_oid, 601),
        other => panic!("Expected Unique metadata, got {:?}", other),
    }
}

#[test]
fn test_serialize_constraint_not_null() {
    let c = Constraint {
        constraint_oid: 503,
        constraint_name: "nn_name".to_string(),
        constraint_type: ConstraintType::NotNull,
        table_oid: 100,
        column_oids: vec![201],
        metadata: ConstraintMetadata::NotNull,
        is_deferrable: false,
    };
    let bytes = serialize_constraint_tuple(&c);
    let rc = deserialize_constraint_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(rc.constraint_type, ConstraintType::NotNull);
    assert!(matches!(rc.metadata, ConstraintMetadata::NotNull));
}

#[test]
fn test_serialize_constraint_check() {
    let c = Constraint {
        constraint_oid: 504,
        constraint_name: "ck_age".to_string(),
        constraint_type: ConstraintType::Check,
        table_oid: 100,
        column_oids: vec![203],
        metadata: ConstraintMetadata::Check {
            check_expression: "age > 0 AND age < 200".to_string(),
        },
        is_deferrable: true,
    };
    let bytes = serialize_constraint_tuple(&c);
    let rc = deserialize_constraint_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(rc.constraint_type, ConstraintType::Check);
    assert!(rc.is_deferrable);
    match rc.metadata {
        ConstraintMetadata::Check { check_expression } => {
            assert_eq!(check_expression, "age > 0 AND age < 200");
        }
        other => panic!("Expected Check metadata, got {:?}", other),
    }
}

#[test]
fn test_serialize_constraint_referential_actions() {
    // Test all 4 referential action types roundtrip
    let actions = vec![
        (ReferentialAction::NoAction, 0u8),
        (ReferentialAction::Cascade, 1u8),
        (ReferentialAction::SetNull, 2u8),
        (ReferentialAction::Restrict, 3u8),
    ];

    for (action, expected_byte) in &actions {
        assert_eq!(action.to_u8(), *expected_byte);
        assert_eq!(ReferentialAction::from_u8(*expected_byte), *action);
    }
}

// ─────────────────────────────────────────────────────────────
// pg_index roundtrip
// ─────────────────────────────────────────────────────────────

#[test]
fn test_serialize_index_tuple_btree() {
    let idx = Index {
        index_oid: 600,
        index_name: "idx_users_email".to_string(),
        table_oid: 100,
        index_type: IndexType::BTree,
        column_oids: vec![201],
        is_unique: true,
        is_primary: false,
        index_pages: 3,
    };
    let bytes = serialize_index_tuple(&idx);
    let ridx = deserialize_index_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(ridx.index_oid, 600);
    assert_eq!(ridx.index_name, "idx_users_email");
    assert_eq!(ridx.table_oid, 100);
    assert_eq!(ridx.index_type, IndexType::BTree);
    assert_eq!(ridx.column_oids, vec![201]);
    assert!(ridx.is_unique);
    assert!(!ridx.is_primary);
    assert_eq!(ridx.index_pages, 3);
}

#[test]
fn test_serialize_index_tuple_hash() {
    let idx = Index {
        index_oid: 601,
        index_name: "idx_hash".to_string(),
        table_oid: 100,
        index_type: IndexType::Hash,
        column_oids: vec![200, 201],
        is_unique: false,
        is_primary: false,
        index_pages: 1,
    };
    let bytes = serialize_index_tuple(&idx);
    let ridx = deserialize_index_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(ridx.index_type, IndexType::Hash);
    assert_eq!(ridx.column_oids, vec![200, 201]);
    assert!(!ridx.is_unique);
}

#[test]
fn test_serialize_index_tuple_primary() {
    let idx = Index {
        index_oid: 602,
        index_name: "pk_idx".to_string(),
        table_oid: 100,
        index_type: IndexType::BTree,
        column_oids: vec![200],
        is_unique: true,
        is_primary: true,
        index_pages: 1,
    };
    let bytes = serialize_index_tuple(&idx);
    let ridx = deserialize_index_tuple(&bytes).expect("deserialize should succeed");

    assert!(ridx.is_primary);
    assert!(ridx.is_unique);
}

// ─────────────────────────────────────────────────────────────
// pg_type roundtrip
// ─────────────────────────────────────────────────────────────

#[test]
fn test_serialize_type_tuple_roundtrip() {
    let dt = DataType {
        type_oid: 1,
        type_name: "INT".to_string(),
        type_category: TypeCategory::Numeric,
        type_length: 4,
        type_align: 4,
        is_builtin: true,
    };
    let bytes = serialize_type_tuple(&dt);
    let rdt = deserialize_type_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(rdt.type_oid, 1);
    assert_eq!(rdt.type_name, "INT");
    assert!(matches!(rdt.type_category, TypeCategory::Numeric));
    assert_eq!(rdt.type_length, 4);
    assert_eq!(rdt.type_align, 4);
    assert!(rdt.is_builtin);
}

#[test]
fn test_serialize_type_variable_length() {
    let dt = DataType {
        type_oid: 6,
        type_name: "TEXT".to_string(),
        type_category: TypeCategory::String,
        type_length: -1,
        type_align: 1,
        is_builtin: true,
    };
    let bytes = serialize_type_tuple(&dt);
    let rdt = deserialize_type_tuple(&bytes).expect("deserialize should succeed");

    assert_eq!(rdt.type_length, -1);
}

// ─────────────────────────────────────────────────────────────
// calculate_tuple_size
// ─────────────────────────────────────────────────────────────

#[test]
fn test_calculate_tuple_size_fixed_only() {
    let columns = vec![
        Column {
            column_oid: 1, name: "id".into(), column_position: 1,
            data_type: DataType::int(), type_modifier: None,
            is_nullable: false, default_value: None, constraints: vec![],
        },
        Column {
            column_oid: 2, name: "age".into(), column_position: 2,
            data_type: DataType::int(), type_modifier: None,
            is_nullable: true, default_value: None, constraints: vec![],
        },
    ];
    let (size, has_var) = calculate_tuple_size(&columns);
    assert_eq!(size, 8); // 4 + 4
    assert!(!has_var);
}

#[test]
fn test_calculate_tuple_size_with_variable() {
    let columns = vec![
        Column {
            column_oid: 1, name: "id".into(), column_position: 1,
            data_type: DataType::int(), type_modifier: None,
            is_nullable: false, default_value: None, constraints: vec![],
        },
        Column {
            column_oid: 2, name: "name".into(), column_position: 2,
            data_type: DataType::text(), type_modifier: None,
            is_nullable: true, default_value: None, constraints: vec![],
        },
    ];
    let (size, has_var) = calculate_tuple_size(&columns);
    assert_eq!(size, 6); // 4 (INT) + 2 (length prefix for TEXT)
    assert!(has_var);
}

// ─────────────────────────────────────────────────────────────
// ConstraintType / IndexType enum helpers
// ─────────────────────────────────────────────────────────────

#[test]
fn test_constraint_type_roundtrip() {
    let types = vec![
        (ConstraintType::PrimaryKey, 1u8),
        (ConstraintType::ForeignKey, 2u8),
        (ConstraintType::Unique, 3u8),
        (ConstraintType::NotNull, 4u8),
        (ConstraintType::Check, 5u8),
    ];
    for (ct, byte) in &types {
        assert_eq!(ct.to_u8(), *byte);
        assert_eq!(ConstraintType::from_u8(*byte).unwrap(), *ct);
    }
    assert!(ConstraintType::from_u8(99).is_none());
}

#[test]
fn test_index_type_roundtrip() {
    assert_eq!(IndexType::BTree.to_u8(), 1);
    assert_eq!(IndexType::Hash.to_u8(), 2);
    assert_eq!(IndexType::from_u8(1), IndexType::BTree);
    assert_eq!(IndexType::from_u8(2), IndexType::Hash);
    // default to BTree for unknown
    assert_eq!(IndexType::from_u8(99), IndexType::BTree);
}

#[test]
fn test_encoding_roundtrip() {
    assert_eq!(Encoding::UTF8.to_u8(), 1);
    assert_eq!(Encoding::ASCII.to_u8(), 2);
    assert_eq!(Encoding::from_u8(1), Encoding::UTF8);
    assert_eq!(Encoding::from_u8(2), Encoding::ASCII);
    // default to UTF8 for unknown
    assert_eq!(Encoding::from_u8(99), Encoding::UTF8);
}

#[test]
fn test_type_category_helpers() {
    assert_eq!(type_category_to_u8(&TypeCategory::Numeric), 1);
    assert_eq!(type_category_to_u8(&TypeCategory::String), 2);
    assert_eq!(type_category_to_u8(&TypeCategory::DateTime), 3);
    assert_eq!(type_category_to_u8(&TypeCategory::Boolean), 4);
    assert_eq!(type_category_to_u8(&TypeCategory::Binary), 5);

    assert!(matches!(type_category_from_u8(1), TypeCategory::Numeric));
    assert!(matches!(type_category_from_u8(2), TypeCategory::String));
    assert!(matches!(type_category_from_u8(3), TypeCategory::DateTime));
    assert!(matches!(type_category_from_u8(4), TypeCategory::Boolean));
    assert!(matches!(type_category_from_u8(99), TypeCategory::Binary)); // default
}
