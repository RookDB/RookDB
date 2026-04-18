//! Set operations: UNION, INTERSECT, EXCEPT.

use std::collections::HashSet;
use std::io;

use crate::executor::projection::ResultTable;
use crate::executor::value::Value;

fn schema_compatible(a: &ResultTable, b: &ResultTable) -> bool {
    a.columns.len() == b.columns.len()
}

fn rows_to_set(rows: &[Vec<Value>]) -> HashSet<Vec<Value>> {
    rows.iter().cloned().collect()
}

/// UNION [ALL].  `all=true` preserves duplicates (UNION ALL).
pub fn union(a: ResultTable, b: ResultTable, all: bool) -> io::Result<ResultTable> {
    if !schema_compatible(&a, &b) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "UNION: column count mismatch ({} vs {})",
                a.columns.len(),
                b.columns.len()
            ),
        ));
    }
    let columns = a.columns.clone();
    let rows = if all {
        let mut r = a.rows;
        r.extend(b.rows);
        r
    } else {
        let mut seen: HashSet<Vec<Value>> = HashSet::new();
        let mut out = Vec::new();
        for row in a.rows.into_iter().chain(b.rows.into_iter()) {
            if seen.insert(row.clone()) {
                out.push(row);
            }
        }
        out
    };
    Ok(ResultTable { columns, rows })
}

/// INTERSECT [ALL].
pub fn intersect(a: ResultTable, b: ResultTable, all: bool) -> io::Result<ResultTable> {
    if !schema_compatible(&a, &b) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "INTERSECT: column count mismatch ({} vs {})",
                a.columns.len(),
                b.columns.len()
            ),
        ));
    }
    let columns = a.columns.clone();
    let rows = if all {
        // INTERSECT ALL: for each row in a, include it as many times as it appears in b.
        let mut b_remaining: Vec<Vec<Value>> = b.rows;
        let mut out = Vec::new();
        for row in a.rows {
            if let Some(pos) = b_remaining.iter().position(|r| r == &row) {
                b_remaining.remove(pos);
                out.push(row);
            }
        }
        out
    } else {
        let b_set = rows_to_set(&b.rows);
        let mut seen: HashSet<Vec<Value>> = HashSet::new();
        let mut out = Vec::new();
        for row in a.rows {
            if b_set.contains(&row) && seen.insert(row.clone()) {
                out.push(row);
            }
        }
        out
    };
    Ok(ResultTable { columns, rows })
}

/// EXCEPT [ALL].
pub fn except(a: ResultTable, b: ResultTable, all: bool) -> io::Result<ResultTable> {
    if !schema_compatible(&a, &b) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "EXCEPT: column count mismatch ({} vs {})",
                a.columns.len(),
                b.columns.len()
            ),
        ));
    }
    let columns = a.columns.clone();
    let rows = if all {
        // EXCEPT ALL: remove each occurrence in b from a.
        let mut b_remaining: Vec<Vec<Value>> = b.rows;
        let mut out = Vec::new();
        for row in a.rows {
            if let Some(pos) = b_remaining.iter().position(|r| r == &row) {
                b_remaining.remove(pos);
            } else {
                out.push(row);
            }
        }
        out
    } else {
        let b_set = rows_to_set(&b.rows);
        let mut seen: HashSet<Vec<Value>> = HashSet::new();
        let mut out = Vec::new();
        for row in a.rows {
            if !b_set.contains(&row) && seen.insert(row.clone()) {
                out.push(row);
            }
        }
        out
    };
    Ok(ResultTable { columns, rows })
}
