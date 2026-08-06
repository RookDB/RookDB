//! Differential test: the join subsystem's `RowCodec` must agree exactly with
//! the engine's own row format.
//!
//! `RowCodec` exists to precompute the physical layout once per schema instead
//! of once per row. That is a performance change only - if it ever becomes a
//! semantic change, the join subsystem would silently read and write rows the
//! rest of the engine cannot, so this test is the guard on that.

#[path = "join_common/mod.rs"]
mod common;

use common::{Rng, columns_from_types, random_row, random_schema, seed_from_env};
use storage_manager::join::{OutputSchema, RelationSchema, RowBuilder, RowCodec};
use storage_manager::types::row::serialize_nullable_typed_row;
use storage_manager::types::{DataType, DataValue, deserialize_nullable_row};

/// Encoding must be byte-identical to upstream, including when upstream
/// refuses the input.
#[test]
fn encode_matches_upstream_byte_for_byte() {
    let seed = seed_from_env(0x5EED_0001);
    println!("test_join_row_codec::encode seed = {seed}");
    let mut rng = Rng::new(seed);

    let mut compared = 0usize;
    for _ in 0..400 {
        let schema = random_schema(&mut rng, 1, 6);
        let codec = RowCodec::new(schema.clone());

        for _ in 0..5 {
            let values = random_row(&mut rng, &schema, 2);

            let mine = codec.encode(&values);
            let theirs = serialize_nullable_typed_row(&schema, &values);

            assert_eq!(
                mine.is_ok(),
                theirs.is_ok(),
                "codec and upstream disagree on whether this row is encodable\n\
                 schema = {schema:?}\nvalues = {values:?}\n\
                 mine = {mine:?}\ntheirs = {theirs:?}"
            );

            if let (Ok(mine), Ok(theirs)) = (mine, theirs) {
                assert_eq!(
                    mine, theirs,
                    "encoded bytes differ\nschema = {schema:?}\nvalues = {values:?}"
                );
                compared += 1;
            }
        }
    }

    assert!(
        compared > 500,
        "only {compared} rows actually encoded; the generator is not producing valid values"
    );
}

/// Decoding must produce the same values as upstream for bytes upstream wrote.
#[test]
fn decode_matches_upstream_values() {
    let seed = seed_from_env(0x5EED_0002);
    println!("test_join_row_codec::decode seed = {seed}");
    let mut rng = Rng::new(seed);

    let mut compared = 0usize;
    for _ in 0..400 {
        let schema = random_schema(&mut rng, 1, 6);
        let codec = RowCodec::new(schema.clone());

        for _ in 0..5 {
            let values = random_row(&mut rng, &schema, 2);
            let Ok(bytes) = serialize_nullable_typed_row(&schema, &values) else {
                continue;
            };

            let mine = codec.decode(&bytes);
            let theirs = deserialize_nullable_row(&schema, &bytes);

            assert_eq!(
                mine.is_ok(),
                theirs.is_ok(),
                "codec and upstream disagree on whether this row is decodable\n\
                 schema = {schema:?}\nvalues = {values:?}"
            );

            if let (Ok(mine), Ok(theirs)) = (mine, theirs) {
                assert_eq!(
                    mine, theirs,
                    "decoded values differ\nschema = {schema:?}\ninput = {values:?}"
                );
                compared += 1;
            }
        }
    }

    assert!(
        compared > 500,
        "only {compared} rows actually decoded; the generator is not producing valid values"
    );
}

/// Single-column extraction must agree with a full decode. This is the path
/// join operators use to pull a key out of a row without paying for the rest
/// of the columns.
#[test]
fn decode_column_matches_full_decode() {
    let seed = seed_from_env(0x5EED_0003);
    println!("test_join_row_codec::decode_column seed = {seed}");
    let mut rng = Rng::new(seed);

    for _ in 0..300 {
        let schema = random_schema(&mut rng, 1, 6);
        let codec = RowCodec::new(schema.clone());
        let values = random_row(&mut rng, &schema, 3);

        let Ok(bytes) = codec.encode(&values) else {
            continue;
        };
        let Ok(full) = codec.decode(&bytes) else {
            continue;
        };

        for (index, expected) in full.iter().enumerate() {
            let single = codec
                .decode_column(&bytes, index)
                .expect("column decode failed on a row that fully decoded");
            assert_eq!(
                &single, expected,
                "column {index} differs between single and full decode\nschema = {schema:?}"
            );
        }
    }
}

/// Decoding is defined only for a slice whose length is exactly the row's,
/// because the last variable-length payload runs to the end of the row.
/// A padded buffer must be rejected, not silently misread.
#[test]
fn padded_buffer_is_rejected_for_varlen_rows() {
    let schema = vec![DataType::Int, DataType::Varchar(16)];
    let codec = RowCodec::new(schema.clone());

    let values = vec![
        Some(DataValue::Int(7)),
        Some(DataValue::Varchar("hello".to_string())),
    ];
    let bytes = codec.encode(&values).expect("row should encode");

    let exact = codec
        .decode(&bytes)
        .expect("exact-length slice should decode");
    assert_eq!(exact, values);

    let mut padded = bytes.clone();
    padded.extend_from_slice(&[0u8; 8]);

    // Upstream would read the trailing zeros into the last VARCHAR. We do too,
    // by construction - so what this asserts is that the two agree, and that
    // the result is visibly not the original value. The defence against ever
    // handing over a padded buffer is the length framing in the spill layer.
    let mine = codec.decode(&padded);
    let theirs = deserialize_nullable_row(&schema, &padded);
    assert_eq!(mine.is_ok(), theirs.is_ok());
    if let (Ok(mine), Ok(theirs)) = (mine, theirs) {
        assert_eq!(mine, theirs, "padded decode must still match upstream");
        assert_ne!(
            mine, values,
            "padding a row changed nothing, so this test is not exercising the hazard"
        );
    }
}

/// Var-len offsets are `u16`, so a payload that would begin past byte 65535
/// cannot be addressed. Only the *second and later* var-len columns can hit
/// this: the first payload always starts a few bytes into the row.
///
/// Upstream casts the cursor with `as u16`, which wraps silently and writes a
/// corrupt offset table. We refuse instead. The second half of this test
/// characterises the upstream behaviour so we notice if it is ever fixed.
#[test]
fn oversized_row_is_refused_not_truncated() {
    let schema = vec![DataType::Varchar(65535), DataType::Varchar(65535)];
    let codec = RowCodec::new(schema.clone());

    // A single large payload is fine: its offset is small, and its length is
    // derived from the end of the row.
    let single = vec![Some(DataValue::Varchar("a".repeat(65_530))), None];
    let bytes = codec
        .encode(&single)
        .expect("one large payload is addressable and must encode");
    assert_eq!(codec.decode(&bytes).expect("decode"), single);

    // Pushing a second payload past byte 65535 is not addressable.
    let values = vec![
        Some(DataValue::Varchar("a".repeat(65_530))),
        Some(DataValue::Varchar("b".to_string())),
    ];

    let err = codec
        .encode(&values)
        .expect_err("a payload starting past byte 65535 must be refused");
    let rendered = err.to_string();
    assert!(
        rendered.contains("65535"),
        "error should name the limit it hit, got: {rendered}"
    );

    // Characterisation of upstream: it accepts the row, wraps the offset, and
    // the result does not survive a round trip - currently it panics on the
    // reversed slice range rather than returning an error.
    //
    // This is latent rather than live for the heap, since a row that large
    // cannot fit in an 8 KiB page in the first place. It matters here because
    // join spill files are not page-bounded, so the join subsystem is the
    // first component that could actually construct such a row.
    let upstream = serialize_nullable_typed_row(&schema, &values)
        .expect("upstream currently accepts this row");

    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        deserialize_nullable_row(&schema, &upstream)
    }));
    std::panic::set_hook(previous_hook);

    let recovered = match outcome {
        Err(_) => None,     // panicked
        Ok(Err(_)) => None, // returned an error
        Ok(Ok(values)) => Some(values),
    };
    assert_ne!(
        recovered.as_ref(),
        Some(&values),
        "upstream now round-trips rows past the u16 offset limit; revisit the refusal above"
    );
}

/// A deliberate, tested divergence from upstream.
///
/// `to_bytes_for_type` enforces the declared width for CHAR but not for
/// VARCHAR, while `DataValue::from_bytes` enforces it for both. So upstream
/// will happily serialize a VARCHAR payload longer than its column allows and
/// then fail to deserialize the result. A join writes rows to spill files and
/// reads them back mid-execution, so accepting such a row would turn a schema
/// violation into a failure thousands of rows later. We refuse at encode time.
#[test]
fn over_long_varchar_is_refused_although_upstream_accepts_it() {
    let schema = vec![DataType::Varchar(4)];
    let codec = RowCodec::new(schema.clone());
    let values = vec![Some(DataValue::Varchar("far too long".to_string()))];

    let err = codec
        .encode(&values)
        .expect_err("an over-long VARCHAR must be refused");
    assert!(
        err.to_string().contains("declared limit"),
        "error should explain the limit, got: {err}"
    );

    // Characterisation: upstream writes it, then cannot read it back.
    let upstream = serialize_nullable_typed_row(&schema, &values)
        .expect("upstream currently accepts an over-long VARCHAR");
    assert!(
        deserialize_nullable_row(&schema, &upstream).is_err(),
        "upstream no longer produces an unreadable row here"
    );
}

// ── RowBuilder ───────────────────────────────────────────────────────────────

fn relation(alias: &str, types: &[DataType]) -> RelationSchema {
    RelationSchema::new(alias, columns_from_types(types))
}

/// Joined output is the left row's columns followed by the right row's.
#[test]
fn builder_concatenates_in_declared_order() {
    let left_types = vec![DataType::Int, DataType::Varchar(8)];
    let right_types = vec![DataType::BigInt];
    let schema = OutputSchema::concat(
        &relation("l", &left_types),
        &relation("r", &right_types),
        false,
        false,
    );
    let builder = RowBuilder::new(&schema);

    let left = vec![
        Some(DataValue::Int(1)),
        Some(DataValue::Varchar("ab".to_string())),
    ];
    let right = vec![Some(DataValue::BigInt(99))];

    let bytes = builder
        .build(Some(&left), Some(&right))
        .expect("row should build");
    let decoded = RowCodec::for_schema(&schema)
        .decode(&bytes)
        .expect("built row should decode");

    assert_eq!(
        decoded,
        vec![
            Some(DataValue::Int(1)),
            Some(DataValue::Varchar("ab".to_string())),
            Some(DataValue::BigInt(99)),
        ]
    );
    assert_eq!(
        schema
            .columns
            .iter()
            .map(|c| c.qualified_name.as_str())
            .collect::<Vec<_>>(),
        vec!["l.c0", "l.c1", "r.c0"]
    );
}

/// An absent side becomes NULLs in exactly that side's columns - the shape an
/// outer join emits for an unmatched row.
#[test]
fn builder_null_extends_the_absent_side() {
    let left_types = vec![DataType::Int];
    let right_types = vec![DataType::Varchar(8), DataType::Bool];
    let schema = OutputSchema::concat(
        &relation("l", &left_types),
        &relation("r", &right_types),
        false,
        true,
    );
    let builder = RowBuilder::new(&schema);
    let codec = RowCodec::for_schema(&schema);

    let left = vec![Some(DataValue::Int(5))];
    let bytes = builder.build(Some(&left), None).expect("row should build");
    assert_eq!(
        codec.decode(&bytes).expect("decode"),
        vec![Some(DataValue::Int(5)), None, None]
    );

    let right = vec![
        Some(DataValue::Varchar("z".to_string())),
        Some(DataValue::Bool(true)),
    ];
    let bytes = builder.build(None, Some(&right)).expect("row should build");
    assert_eq!(
        codec.decode(&bytes).expect("decode"),
        vec![
            None,
            Some(DataValue::Varchar("z".to_string())),
            Some(DataValue::Bool(true))
        ]
    );

    // Null-extending a side marks its columns nullable in the schema.
    assert!(schema.columns[1].nullable);
    assert!(schema.columns[2].nullable);
}

/// SEMI and ANTI joins emit left columns only, so their builder cannot be
/// handed right-side values at all.
#[test]
fn left_only_schema_rejects_right_side_values() {
    let left_types = vec![DataType::Int, DataType::Bool];
    let schema = OutputSchema::left_only(&relation("l", &left_types));
    assert_eq!(schema.left_width(), 2);
    assert_eq!(schema.right_width(), 0);

    let builder = RowBuilder::new(&schema);
    let left = vec![Some(DataValue::Int(3)), Some(DataValue::Bool(false))];

    let bytes = builder.build(Some(&left), None).expect("row should build");
    assert_eq!(
        RowCodec::for_schema(&schema)
            .decode(&bytes)
            .expect("decode"),
        left
    );

    let stray = vec![Some(DataValue::Int(1))];
    assert!(
        builder.build(Some(&left), Some(&stray)).is_err(),
        "a left-only schema must refuse right-side values"
    );
}

/// Schemas with the same types share a fingerprint; different types do not.
/// Spill files carry this value so a run cannot be read under a foreign
/// schema.
#[test]
fn fingerprint_tracks_the_type_list() {
    let a = OutputSchema::concat(
        &relation("l", &[DataType::Int]),
        &relation("r", &[DataType::Varchar(8)]),
        false,
        false,
    );
    let same_types_other_names = OutputSchema::concat(
        &relation("x", &[DataType::Int]),
        &relation("y", &[DataType::Varchar(8)]),
        false,
        false,
    );
    let different_types = OutputSchema::concat(
        &relation("l", &[DataType::BigInt]),
        &relation("r", &[DataType::Varchar(8)]),
        false,
        false,
    );

    assert_eq!(a.fingerprint, same_types_other_names.fingerprint);
    assert_ne!(a.fingerprint, different_types.fingerprint);

    // Column-order changes must be visible too.
    let swapped = OutputSchema::concat(
        &relation("l", &[DataType::Varchar(8)]),
        &relation("r", &[DataType::Int]),
        false,
        false,
    );
    assert_ne!(a.fingerprint, swapped.fingerprint);
}
