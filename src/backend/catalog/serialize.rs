//! Binary serialisation / deserialisation for system-catalog tuples.
//!
//! Each system catalog record is stored as a variable-length byte slice inside
//! a slotted data page.  The format for every catalog is documented below.
//!
//! Variable-length strings are stored as:  `[u16 len (LE)] [bytes …]`
//! Arrays are stored as: `[u16 count (LE)] [element × count]`

use std::io::{self, Cursor, Read};

use crate::catalog::types::{
    Constraint, ConstraintMetadata, ConstraintType, DataType, DefaultValue, Index, IndexType,
    ReferentialAction, TypeCategory, TypeModifier,
};

// ─────────────────────────────────────────────────────────────
// Low-level helpers
// ─────────────────────────────────────────────────────────────

fn write_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}
fn write_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
fn write_i16(buf: &mut Vec<u8>, v: i16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len().min(u16::MAX as usize) as u16;
    write_u16(buf, len);
    buf.extend_from_slice(&bytes[..len as usize]);
}

fn write_u32_arr(buf: &mut Vec<u8>, arr: &[u32]) {
    write_u16(buf, arr.len() as u16);
    for &v in arr {
        write_u32(buf, v);
    }
}

fn read_u8(c: &mut Cursor<&[u8]>) -> io::Result<u8> {
    let mut b = [0u8; 1];
    c.read_exact(&mut b)?;
    Ok(b[0])
}
fn read_u16(c: &mut Cursor<&[u8]>) -> io::Result<u16> {
    let mut b = [0u8; 2];
    c.read_exact(&mut b)?;
    Ok(u16::from_le_bytes(b))
}
fn read_u32(c: &mut Cursor<&[u8]>) -> io::Result<u32> {
    let mut b = [0u8; 4];
    c.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}
fn read_u64(c: &mut Cursor<&[u8]>) -> io::Result<u64> {
    let mut b = [0u8; 8];
    c.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}
fn read_i16(c: &mut Cursor<&[u8]>) -> io::Result<i16> {
    let mut b = [0u8; 2];
    c.read_exact(&mut b)?;
    Ok(i16::from_le_bytes(b))
}

fn read_str(c: &mut Cursor<&[u8]>) -> io::Result<String> {
    let len = read_u16(c)? as usize;
    let mut bytes = vec![0u8; len];
    c.read_exact(&mut bytes)?;
    String::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn read_u32_arr(c: &mut Cursor<&[u8]>) -> io::Result<Vec<u32>> {
    let count = read_u16(c)? as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(read_u32(c)?);
    }
    Ok(out)
}

// ─────────────────────────────────────────────────────────────
// pg_database
// ─────────────────────────────────────────────────────────────
// Layout: db_oid(4) | db_name | db_owner | created_at(8) | encoding(1)

pub fn serialize_database_tuple(
    db_oid: u32,
    db_name: &str,
    db_owner: &str,
    created_at: u64,
    encoding: u8,
) -> Vec<u8> {
    let mut buf = Vec::new();
    write_u32(&mut buf, db_oid);
    write_str(&mut buf, db_name);
    write_str(&mut buf, db_owner);
    write_u64(&mut buf, created_at);
    write_u8(&mut buf, encoding);
    buf
}

pub fn deserialize_database_tuple(bytes: &[u8]) -> io::Result<(u32, String, String, u64, u8)> {
    let mut c = Cursor::new(bytes);
    let oid = read_u32(&mut c)?;
    let name = read_str(&mut c)?;
    let owner = read_str(&mut c)?;
    let created = read_u64(&mut c)?;
    let encoding = read_u8(&mut c)?;
    Ok((oid, name, owner, created, encoding))
}

// ─────────────────────────────────────────────────────────────
// pg_table
// ─────────────────────────────────────────────────────────────
// Layout: table_oid(4) | table_name | db_oid(4) | table_type(1)
//         | row_count(8) | page_count(4) | created_at(8)

pub fn serialize_table_tuple(
    table_oid: u32,
    table_name: &str,
    db_oid: u32,
    table_type: u8,
    row_count: u64,
    page_count: u32,
    created_at: u64,
) -> Vec<u8> {
    let mut buf = Vec::new();
    write_u32(&mut buf, table_oid);
    write_str(&mut buf, table_name);
    write_u32(&mut buf, db_oid);
    write_u8(&mut buf, table_type);
    write_u64(&mut buf, row_count);
    write_u32(&mut buf, page_count);
    write_u64(&mut buf, created_at);
    buf
}

pub fn deserialize_table_tuple(bytes: &[u8]) -> io::Result<(u32, String, u32, u8, u64, u32, u64)> {
    let mut c = Cursor::new(bytes);
    Ok((
        read_u32(&mut c)?,
        read_str(&mut c)?,
        read_u32(&mut c)?,
        read_u8(&mut c)?,
        read_u64(&mut c)?,
        read_u32(&mut c)?,
        read_u64(&mut c)?,
    ))
}

// ─────────────────────────────────────────────────────────────
// pg_column
// ─────────────────────────────────────────────────────────────
// Layout: column_oid(4) | table_oid(4) | column_name | column_pos(2)
//         | type_oid(4) | type_length(2i) | type_align(1)
//         | type_category(1) | type_name | type_mod_flag(1) [type_mod_data]
//         | is_nullable(1) | has_default(1) [default_tag(1) default_data]
//         | num_constraints(2) | constraint_oid[*]

pub fn serialize_column_tuple(
    column_oid: u32,
    table_oid: u32,
    column_name: &str,
    column_pos: u16,
    dt: &DataType,
    type_modifier: Option<&TypeModifier>,
    is_nullable: bool,
    default_value: Option<&DefaultValue>,
    constraint_oids: &[u32],
) -> Vec<u8> {
    let mut buf = Vec::new();
    write_u32(&mut buf, column_oid);
    write_u32(&mut buf, table_oid);
    write_str(&mut buf, column_name);
    write_u16(&mut buf, column_pos);
    // DataType inline
    write_u32(&mut buf, dt.type_oid);
    write_i16(&mut buf, dt.type_length);
    write_u8(&mut buf, dt.type_align);
    write_u8(&mut buf, type_category_to_u8(&dt.type_category));
    write_str(&mut buf, &dt.type_name);
    // TypeModifier (flag byte + optional payload)
    match type_modifier {
        None => write_u8(&mut buf, 0),
        Some(TypeModifier::VarcharLen(n)) => {
            write_u8(&mut buf, 1);
            write_u16(&mut buf, *n);
        }
        Some(TypeModifier::Precision { precision, scale }) => {
            write_u8(&mut buf, 2);
            write_u8(&mut buf, *precision);
            write_u8(&mut buf, *scale);
        }
    }
    write_u8(&mut buf, is_nullable as u8);
    // DefaultValue
    match default_value {
        None => write_u8(&mut buf, 0),
        Some(dv) => {
            write_u8(&mut buf, 1);
            serialize_default_value(&mut buf, dv);
        }
    }
    write_u32_arr(&mut buf, constraint_oids);
    buf
}

pub fn deserialize_column_tuple(
    bytes: &[u8],
) -> io::Result<(
    u32,
    u32,
    String,
    u16,
    DataType,
    Option<TypeModifier>,
    bool,
    Option<DefaultValue>,
    Vec<u32>,
)> {
    let mut c = Cursor::new(bytes);
    let column_oid = read_u32(&mut c)?;
    let table_oid = read_u32(&mut c)?;
    let column_name = read_str(&mut c)?;
    let column_pos = read_u16(&mut c)?;
    // DataType inline
    let type_oid = read_u32(&mut c)?;
    let type_length = read_i16(&mut c)?;
    let type_align = read_u8(&mut c)?;
    let type_category = type_category_from_u8(read_u8(&mut c)?);
    let type_name = read_str(&mut c)?;
    let dt = DataType {
        type_oid,
        type_name,
        type_category,
        type_length,
        type_align,
        is_builtin: true,
    };
    // TypeModifier
    let tm_flag = read_u8(&mut c)?;
    let type_modifier = match tm_flag {
        1 => Some(TypeModifier::VarcharLen(read_u16(&mut c)?)),
        2 => {
            let p = read_u8(&mut c)?;
            let s = read_u8(&mut c)?;
            Some(TypeModifier::Precision {
                precision: p,
                scale: s,
            })
        }
        _ => None,
    };
    let is_nullable = read_u8(&mut c)? != 0;
    // DefaultValue
    let has_default = read_u8(&mut c)? != 0;
    let default_value = if has_default {
        Some(deserialize_default_value(&mut c)?)
    } else {
        None
    };
    let constraint_oids = read_u32_arr(&mut c)?;
    Ok((
        column_oid,
        table_oid,
        column_name,
        column_pos,
        dt,
        type_modifier,
        is_nullable,
        default_value,
        constraint_oids,
    ))
}

// ─────────────────────────────────────────────────────────────
// pg_constraint
// ─────────────────────────────────────────────────────────────
// Layout: constraint_oid(4) | constraint_name | constraint_type(1)
//         | table_oid(4) | column_oids[] | is_deferrable(1)
//         | <type-specific payload>

pub fn serialize_constraint_tuple(c: &Constraint) -> Vec<u8> {
    let mut buf = Vec::new();
    write_u32(&mut buf, c.constraint_oid);
    write_str(&mut buf, &c.constraint_name);
    write_u8(&mut buf, c.constraint_type.to_u8());
    write_u32(&mut buf, c.table_oid);
    write_u32_arr(&mut buf, &c.column_oids);
    write_u8(&mut buf, c.is_deferrable as u8);
    // Type-specific metadata
    match &c.metadata {
        ConstraintMetadata::PrimaryKey { index_oid } => {
            write_u32(&mut buf, *index_oid);
        }
        ConstraintMetadata::ForeignKey {
            referenced_table_oid,
            referenced_column_oids,
            on_delete,
            on_update,
        } => {
            write_u32(&mut buf, *referenced_table_oid);
            write_u32_arr(&mut buf, referenced_column_oids);
            write_u8(&mut buf, on_delete.to_u8());
            write_u8(&mut buf, on_update.to_u8());
        }
        ConstraintMetadata::Unique { index_oid } => {
            write_u32(&mut buf, *index_oid);
        }
        ConstraintMetadata::NotNull => {}
        ConstraintMetadata::Check { check_expression } => {
            write_str(&mut buf, check_expression);
        }
    }
    buf
}

pub fn deserialize_constraint_tuple(bytes: &[u8]) -> io::Result<Constraint> {
    let mut cur = Cursor::new(bytes);
    let constraint_oid = read_u32(&mut cur)?;
    let constraint_name = read_str(&mut cur)?;
    let ct_byte = read_u8(&mut cur)?;
    let constraint_type = ConstraintType::from_u8(ct_byte).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown constraint type {}", ct_byte),
        )
    })?;
    let table_oid = read_u32(&mut cur)?;
    let column_oids = read_u32_arr(&mut cur)?;
    let is_deferrable = read_u8(&mut cur)? != 0;
    let metadata = match constraint_type {
        ConstraintType::PrimaryKey => {
            let index_oid = read_u32(&mut cur)?;
            ConstraintMetadata::PrimaryKey { index_oid }
        }
        ConstraintType::ForeignKey => {
            let referenced_table_oid = read_u32(&mut cur)?;
            let referenced_column_oids = read_u32_arr(&mut cur)?;
            let on_delete = ReferentialAction::from_u8(read_u8(&mut cur)?);
            let on_update = ReferentialAction::from_u8(read_u8(&mut cur)?);
            ConstraintMetadata::ForeignKey {
                referenced_table_oid,
                referenced_column_oids,
                on_delete,
                on_update,
            }
        }
        ConstraintType::Unique => {
            let index_oid = read_u32(&mut cur)?;
            ConstraintMetadata::Unique { index_oid }
        }
        ConstraintType::NotNull => ConstraintMetadata::NotNull,
        ConstraintType::Check => {
            let expr = read_str(&mut cur)?;
            ConstraintMetadata::Check {
                check_expression: expr,
            }
        }
    };
    Ok(Constraint {
        constraint_oid,
        constraint_name,
        constraint_type,
        table_oid,
        column_oids,
        metadata,
        is_deferrable,
    })
}

// ─────────────────────────────────────────────────────────────
// pg_index
// ─────────────────────────────────────────────────────────────
// Layout: index_oid(4) | index_name | table_oid(4) | index_type(1)
//         | column_oids[] | is_unique(1) | is_primary(1) | index_pages(4)

pub fn serialize_index_tuple(idx: &Index) -> Vec<u8> {
    let mut buf = Vec::new();
    write_u32(&mut buf, idx.index_oid);
    write_str(&mut buf, &idx.index_name);
    write_u32(&mut buf, idx.table_oid);
    write_u8(&mut buf, idx.index_type.to_u8());
    write_u32_arr(&mut buf, &idx.column_oids);
    write_u8(&mut buf, idx.is_unique as u8);
    write_u8(&mut buf, idx.is_primary as u8);
    write_u32(&mut buf, idx.index_pages);
    buf
}

pub fn deserialize_index_tuple(bytes: &[u8]) -> io::Result<Index> {
    let mut c = Cursor::new(bytes);
    let index_oid = read_u32(&mut c)?;
    let index_name = read_str(&mut c)?;
    let table_oid = read_u32(&mut c)?;
    let index_type = IndexType::from_u8(read_u8(&mut c)?);
    let column_oids = read_u32_arr(&mut c)?;
    let is_unique = read_u8(&mut c)? != 0;
    let is_primary = read_u8(&mut c)? != 0;
    let index_pages = read_u32(&mut c)?;
    Ok(Index {
        index_oid,
        index_name,
        table_oid,
        index_type,
        column_oids,
        is_unique,
        is_primary,
        index_pages,
    })
}

// ─────────────────────────────────────────────────────────────
// pg_type
// ─────────────────────────────────────────────────────────────
// Layout: type_oid(4) | type_name | type_category(1) | type_length(2i)
//         | type_align(1) | is_builtin(1)

pub fn serialize_type_tuple(dt: &DataType) -> Vec<u8> {
    let mut buf = Vec::new();
    write_u32(&mut buf, dt.type_oid);
    write_str(&mut buf, &dt.type_name);
    write_u8(&mut buf, type_category_to_u8(&dt.type_category));
    write_i16(&mut buf, dt.type_length);
    write_u8(&mut buf, dt.type_align);
    write_u8(&mut buf, dt.is_builtin as u8);
    buf
}

pub fn deserialize_type_tuple(bytes: &[u8]) -> io::Result<DataType> {
    let mut c = Cursor::new(bytes);
    let type_oid = read_u32(&mut c)?;
    let type_name = read_str(&mut c)?;
    let type_category = type_category_from_u8(read_u8(&mut c)?);
    let type_length = read_i16(&mut c)?;
    let type_align = read_u8(&mut c)?;
    let is_builtin = read_u8(&mut c)? != 0;
    Ok(DataType {
        type_oid,
        type_name,
        type_category,
        type_length,
        type_align,
        is_builtin,
    })
}

// ─────────────────────────────────────────────────────────────
// DefaultValue (used inside pg_column)
// ─────────────────────────────────────────────────────────────
// tag(1): 1=Integer 2=BigInt 3=Float 4=Double 5=Str 6=Boolean 7=Null 8=CurrentTimestamp

fn serialize_default_value(buf: &mut Vec<u8>, dv: &DefaultValue) {
    match dv {
        DefaultValue::Integer(v) => {
            write_u8(buf, 1);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        DefaultValue::BigInt(v) => {
            write_u8(buf, 2);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        DefaultValue::Float(v) => {
            write_u8(buf, 3);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        DefaultValue::Double(v) => {
            write_u8(buf, 4);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        DefaultValue::Str(s) => {
            write_u8(buf, 5);
            write_str(buf, s);
        }
        DefaultValue::Boolean(b) => {
            write_u8(buf, 6);
            write_u8(buf, *b as u8);
        }
        DefaultValue::Null => {
            write_u8(buf, 7);
        }
        DefaultValue::CurrentTimestamp => {
            write_u8(buf, 8);
        }
    }
}

fn deserialize_default_value(c: &mut Cursor<&[u8]>) -> io::Result<DefaultValue> {
    let tag = read_u8(c)?;
    match tag {
        1 => {
            let mut b = [0u8; 4];
            c.read_exact(&mut b)?;
            Ok(DefaultValue::Integer(i32::from_le_bytes(b)))
        }
        2 => {
            let mut b = [0u8; 8];
            c.read_exact(&mut b)?;
            Ok(DefaultValue::BigInt(i64::from_le_bytes(b)))
        }
        3 => {
            let mut b = [0u8; 4];
            c.read_exact(&mut b)?;
            Ok(DefaultValue::Float(f32::from_le_bytes(b)))
        }
        4 => {
            let mut b = [0u8; 8];
            c.read_exact(&mut b)?;
            Ok(DefaultValue::Double(f64::from_le_bytes(b)))
        }
        5 => Ok(DefaultValue::Str(read_str(c)?)),
        6 => Ok(DefaultValue::Boolean(read_u8(c)? != 0)),
        7 => Ok(DefaultValue::Null),
        8 => Ok(DefaultValue::CurrentTimestamp),
        v => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown default tag {}", v),
        )),
    }
}

// ─────────────────────────────────────────────────────────────
// TypeCategory helpers
// ─────────────────────────────────────────────────────────────

pub fn type_category_to_u8(tc: &TypeCategory) -> u8 {
    match tc {
        TypeCategory::Numeric => 1,
        TypeCategory::String => 2,
        TypeCategory::DateTime => 3,
        TypeCategory::Boolean => 4,
        TypeCategory::Binary => 5,
    }
}

pub fn type_category_from_u8(v: u8) -> TypeCategory {
    match v {
        1 => TypeCategory::Numeric,
        2 => TypeCategory::String,
        3 => TypeCategory::DateTime,
        4 => TypeCategory::Boolean,
        _ => TypeCategory::Binary,
    }
}

// ─────────────────────────────────────────────────────────────
// Tuple size estimator
// ─────────────────────────────────────────────────────────────

/// Calculate the approximate fixed and variable portions of a tuple's size.
/// Returns `(fixed_bytes, has_variable_fields)`.
pub fn calculate_tuple_size(columns: &[crate::catalog::types::Column]) -> (usize, bool) {
    let mut fixed = 0usize;
    let mut has_variable = false;
    for col in columns {
        if col.data_type.type_length > 0 {
            fixed += col.data_type.type_length as usize;
        } else {
            has_variable = true;
            fixed += 2; // 2-byte length prefix
        }
    }
    (fixed, has_variable)
}
